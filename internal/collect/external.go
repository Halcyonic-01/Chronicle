package collect

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/Halcyonic-01/Chronicle/internal/event"
)

type ArgoCollector struct {
	BaseCollector
	client  *http.Client
	baseURL string
	token   string
	seen    map[string]string
}

func NewArgoCollectorFromEnv(out chan<- event.Event) *ArgoCollector {
	baseURL := strings.TrimRight(os.Getenv("ARGOCD_URL"), "/")
	if baseURL == "" {
		return nil
	}
	return &ArgoCollector{BaseCollector: BaseCollector{Out: out}, client: &http.Client{Timeout: 10 * time.Second}, baseURL: baseURL, token: os.Getenv("ARGOCD_TOKEN"), seen: make(map[string]string)}
}

func (a *ArgoCollector) Run(ctx context.Context) error {
	interval := durationFromEnv("ARGOCD_POLL_INTERVAL", 30*time.Second)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		if err := a.poll(ctx); err != nil && ctx.Err() != nil {
			return ctx.Err()
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func (a *ArgoCollector) poll(ctx context.Context) error {
	var response struct {
		Items []struct {
			Metadata struct {
				Name      string `json:"name"`
				Namespace string `json:"namespace"`
			} `json:"metadata"`
			Status struct {
				Sync struct {
					Status   string `json:"status"`
					Revision string `json:"revision"`
				} `json:"sync"`
				Health struct {
					Status string `json:"status"`
				} `json:"health"`
			} `json:"status"`
		} `json:"items"`
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, a.baseURL+"/api/v1/applications", nil)
	if err != nil {
		return err
	}
	if a.token != "" {
		req.Header.Set("Authorization", "Bearer "+a.token)
	}
	resp, err := a.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		return fmt.Errorf("argocd returned %s", resp.Status)
	}
	if err := json.NewDecoder(resp.Body).Decode(&response); err != nil {
		return err
	}
	for _, app := range response.Items {
		name := app.Metadata.Name
		if name == "" {
			continue
		}
		key := name + "/sync"
		revision := app.Status.Sync.Revision
		if revision != "" && a.seen[key] != revision {
			a.seen[key] = revision
			a.Emit(event.Event{Source: "argocd", EntityKind: "Application", EntityName: name, Namespace: valueOr(app.Metadata.Namespace, "argocd"), Type: "deploy", Severity: "info", Title: fmt.Sprintf("%s synced revision %s", name, shortRevision(revision)), Payload: mustJSON(map[string]any{"revision": revision, "sync_status": app.Status.Sync.Status})})
		}
		healthKey := name + "/health"
		health := app.Status.Health.Status
		if health != "" && health != "Healthy" && a.seen[healthKey] != health {
			a.seen[healthKey] = health
			a.Emit(event.Event{Source: "argocd", EntityKind: "Application", EntityName: name, Namespace: valueOr(app.Metadata.Namespace, "argocd"), Type: "application_unhealthy", Severity: "warning", Title: fmt.Sprintf("%s health is %s", name, health), Payload: mustJSON(map[string]any{"health": health})})
		}
	}
	return nil
}

type TerraformCollector struct {
	BaseCollector
	client  *http.Client
	baseURL string
	token   string
	org     string
	seen    map[string]string
}

func NewTerraformCollectorFromEnv(out chan<- event.Event) *TerraformCollector {
	baseURL := strings.TrimRight(os.Getenv("TERRAFORM_URL"), "/")
	if baseURL == "" {
		return nil
	}
	return &TerraformCollector{BaseCollector: BaseCollector{Out: out}, client: &http.Client{Timeout: 10 * time.Second}, baseURL: baseURL, token: os.Getenv("TERRAFORM_TOKEN"), org: os.Getenv("TERRAFORM_ORG"), seen: make(map[string]string)}
}

func (t *TerraformCollector) Run(ctx context.Context) error {
	interval := durationFromEnv("TERRAFORM_POLL_INTERVAL", 30*time.Second)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		if err := t.poll(ctx); err != nil && ctx.Err() != nil {
			return ctx.Err()
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func (t *TerraformCollector) poll(ctx context.Context) error {
	endpoint := t.baseURL + "/api/v2/runs?page[size]=20"
	if t.org != "" {
		endpoint += "&filter[organization][name]=" + t.org
	}
	var response struct {
		Data []struct {
			ID         string `json:"id"`
			Attributes struct {
				Status    string `json:"status"`
				CreatedAt string `json:"created-at"`
				Message   string `json:"message"`
			} `json:"attributes"`
		} `json:"data"`
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return err
	}
	if t.token != "" {
		req.Header.Set("Authorization", "Bearer "+t.token)
		req.Header.Set("Content-Type", "application/vnd.api+json")
	}
	resp, err := t.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		return fmt.Errorf("terraform returned %s", resp.Status)
	}
	if err := json.NewDecoder(resp.Body).Decode(&response); err != nil {
		return err
	}
	for _, run := range response.Data {
		if run.ID == "" || run.Attributes.Status == "" || t.seen[run.ID] == run.Attributes.Status {
			continue
		}
		t.seen[run.ID] = run.Attributes.Status
		severity := "info"
		if run.Attributes.Status == "errored" || run.Attributes.Status == "canceled" {
			severity = "warning"
		}
		t.Emit(event.Event{Source: "terraform", EntityKind: "Run", EntityName: run.ID, Namespace: "terraform", Type: "terraform_run", Severity: severity, Title: fmt.Sprintf("Terraform run %s: %s", run.ID, run.Attributes.Status), Payload: mustJSON(map[string]any{"status": run.Attributes.Status, "created_at": run.Attributes.CreatedAt, "message": run.Attributes.Message})})
	}
	return nil
}

func durationFromEnv(name string, fallback time.Duration) time.Duration {
	if raw := os.Getenv(name); raw != "" {
		if parsed, err := time.ParseDuration(raw); err == nil && parsed > 0 {
			return parsed
		}
	}
	return fallback
}

func shortRevision(revision string) string {
	if len(revision) > 12 {
		return revision[:12]
	}
	return revision
}
