package collect

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/Halcyonic-01/Chronicle/internal/event"
)

// seenRetention bounds how long a log line's fingerprint is remembered for
// deduplication. Each poll only looks back 30 seconds, so a few minutes of
// memory is ample — and without an upper bound this map grows for the lifetime
// of the process, once per distinct log line.
const seenRetention = 5 * time.Minute

type LokiCollector struct {
	BaseCollector
	client  *http.Client
	baseURL string
	query   string
	seen    map[string]time.Time
}

func NewLokiCollector(out chan<- event.Event) *LokiCollector {
	return &LokiCollector{
		BaseCollector: BaseCollector{Out: out},
		client:        &http.Client{Timeout: 10 * time.Second},
		baseURL:       strings.TrimRight(valueOr(os.Getenv("LOKI_URL"), "http://localhost:3100"), "/"),
		query:         valueOr(os.Getenv("LOKI_QUERY"), `{namespace=~".+"}`),
		seen:          make(map[string]time.Time),
	}
}

// forgetStaleFingerprints keeps the dedupe set proportional to recent log
// volume rather than to total uptime.
func (l *LokiCollector) forgetStaleFingerprints(now time.Time) {
	for id, at := range l.seen {
		if now.Sub(at) > seenRetention {
			delete(l.seen, id)
		}
	}
}

func (l *LokiCollector) Run(ctx context.Context) error {
	ticker := time.NewTicker(15 * time.Second)
	defer ticker.Stop()
	// Query a recent window immediately so startup does not wait for a tick.
	if err := l.poll(ctx); err != nil { /* Loki may not be ready yet. */
	}
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if err := l.poll(ctx); err != nil {
				continue
			} // tolerate transient Loki failures
		}
	}
}

type lokiResponse struct {
	Data struct {
		Result []struct {
			Stream map[string]string `json:"stream"`
			Values [][]string        `json:"values"`
		} `json:"result"`
	} `json:"data"`
}

func (l *LokiCollector) poll(ctx context.Context) error {
	end := time.Now()
	start := end.Add(-30 * time.Second)
	l.forgetStaleFingerprints(end)
	u, err := url.Parse(l.baseURL + "/loki/api/v1/query_range")
	if err != nil {
		return err
	}
	q := u.Query()
	q.Set("query", l.query)
	q.Set("start", fmt.Sprintf("%d", start.UnixNano()))
	q.Set("end", fmt.Sprintf("%d", end.UnixNano()))
	q.Set("limit", "500")
	u.RawQuery = q.Encode()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return err
	}
	resp, err := l.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		_, _ = io.Copy(io.Discard, resp.Body)
		return fmt.Errorf("loki returned %s", resp.Status)
	}
	var result lokiResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return err
	}
	for _, stream := range result.Data.Result {
		for _, value := range stream.Values {
			if len(value) != 2 {
				continue
			}
			l.emitLog(stream.Stream, value[0], value[1])
		}
	}
	return nil
}

func (l *LokiCollector) emitLog(labels map[string]string, timestamp, line string) {
	level := strings.ToLower(labels["level"] + " " + line)
	if !strings.Contains(level, "error") && !strings.Contains(level, "fatal") && !strings.Contains(level, "panic") && !strings.Contains(level, "exception") {
		return
	}
	hash := sha256.Sum256([]byte(strings.Join([]string{timestamp, line, labels["namespace"], labels["pod"]}, "\x00")))
	id := hex.EncodeToString(hash[:])
	if _, ok := l.seen[id]; ok {
		return
	}
	l.seen[id] = time.Now()
	nanos, err := strconv.ParseInt(timestamp, 10, 64)
	occurred := time.Unix(0, nanos).UTC()
	if err != nil {
		occurred = time.Now().UTC()
	}
	namespace := valueOr(labels["namespace"], "default")
	entity := valueOr(labels["pod"], valueOr(labels["container"], "unknown"))
	l.Emit(event.Event{Source: "loki", OccurredAt: occurred, Namespace: namespace, EntityKind: "Pod", EntityName: entity, Type: "log_error", Severity: "warning", Title: strings.TrimSpace(line), Payload: mustJSON(map[string]any{"labels": labels, "line": line})})
}

func valueOr(value, fallback string) string {
	if value == "" {
		return fallback
	}
	return value
}
