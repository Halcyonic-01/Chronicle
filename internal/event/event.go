package event

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/tidwall/gjson"
)

// Event is the single shape every source is flattened into.
type Event struct {
	ID string `json:"id"`

	// --- TIME: three different clocks, and they disagree. ---
	OccurredAt time.Time `json:"occurred_at"` // when source says it happened
	IngestedAt time.Time `json:"ingested_at"` // when WE received it (authoritative)

	// --- WHERE: which thing in the infrastructure is this about? ---
	Source     string `json:"source"`      // "k8s" | "github" | "prometheus"
	Namespace  string `json:"namespace"`   // "default", "monitoring"
	EntityKind string `json:"entity_kind"` // "Pod" | "Service" | "Deployment"
	EntityName string `json:"entity_name"` // "api-7d9f8-x2k1"

	// --- WHAT: what kind of thing happened? ---
	Type     string `json:"type"`     // "deploy" | "restart" | "oom_kill"
	Severity string `json:"severity"` // "info" | "warning" | "critical"
	Title    string `json:"title"`    // human-readable one-liner

	// --- EXTRA: source-specific detail, kept as raw JSON ---
	Payload json.RawMessage `json:"payload"`

	// --- LINKING: how this event connects to others ---
	TraceID        string `json:"trace_id"`        // from OpenTelemetry, if present
	CorrelationKey string `json:"correlation_key"` // ties related events
}

// CorrelationKey ties related events (e.g., github deploy, argo sync, pod restart) together.
func CorrelationKey(e Event) string {
	// Best case: everything from one deploy shares a commit SHA.
	if sha := gjson.GetBytes(e.Payload, "commit_sha").String(); len(sha) >= 12 {
		return "sha:" + sha[:12]
	}

	// Fallback: same deployment, same 30-second bucket.
	bucket := e.IngestedAt.Truncate(30 * time.Second).Unix()
	return fmt.Sprintf("%s/%s@%d", e.Namespace, ownerOf(e), bucket)
}

func ownerOf(e Event) string {
	// If the payload contains the owner reference (added by the k8s collector), use it
	if owner := gjson.GetBytes(e.Payload, "owner").String(); owner != "" {
		return owner
	}
	return e.EntityName
}
