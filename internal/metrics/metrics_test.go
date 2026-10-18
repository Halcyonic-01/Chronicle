package metrics

import (
	"context"
	"io"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"
)

func TestServeExposesChroniclesMetricsOnItsOwnAddress(t *testing.T) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := l.Addr().String()
	l.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go Serve(ctx, addr)
	EventsDuplicate.Inc()

	var body string
	for i := 0; i < 50 && body == ""; i++ {
		if resp, err := http.Get("http://" + addr + "/metrics"); err == nil {
			raw, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			body = string(raw)
		} else {
			time.Sleep(50 * time.Millisecond)
		}
	}
	for _, want := range []string{"chronicle_events_duplicate_total", "chronicle_leader", "go_goroutines"} {
		if !strings.Contains(body, want) {
			t.Errorf("/metrics lacks %s", want)
		}
	}
}
