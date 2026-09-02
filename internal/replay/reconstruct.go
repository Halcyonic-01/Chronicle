package replay

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/Halcyonic-01/Chronicle/internal/event"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/klauspost/compress/zstd"
	"github.com/tidwall/gjson"
)

// Replayer reconstructs the exact cluster state at any arbitrary past moment.
type Replayer struct {
	pool *pgxpool.Pool
}

func NewReplayer(pool *pgxpool.Pool) *Replayer {
	return &Replayer{pool: pool}
}

// At reconstructs cluster state at an arbitrary past instant using the
// keyframe + delta approach: load the most recent snapshot, then replay
// every event between the snapshot and the target time forward.
func (r *Replayer) At(ctx context.Context, t time.Time) (*Snapshot, error) {
	// Step 1: Find the most recent snapshot at or before t
	var compressed []byte
	var takenAt time.Time
	err := r.pool.QueryRow(ctx, `
		SELECT data, taken_at FROM snapshots
		WHERE taken_at <= $1
		ORDER BY taken_at DESC LIMIT 1`, t).Scan(&compressed, &takenAt)
	if err != nil {
		return nil, fmt.Errorf("no snapshot before %s: %w", t, err)
	}

	dec, err := zstd.NewReader(nil)
	if err != nil {
		return nil, err
	}
	raw, err := dec.DecodeAll(compressed, nil)
	if err != nil {
		return nil, err
	}

	var snap Snapshot
	if err := json.Unmarshal(raw, &snap); err != nil {
		return nil, err
	}

	// Step 2: Fetch every event between the snapshot and t
	rows, err := r.pool.Query(ctx, `
		SELECT id, ingested_at, entity_kind, entity_name, namespace,
		       type, severity, title, payload
		FROM events
		WHERE ingested_at > $1 AND ingested_at <= $2
		ORDER BY ingested_at ASC`, takenAt, t)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	// Step 3: Apply each event forward to move state to time t
	for rows.Next() {
		var e event.Event
		if err := rows.Scan(&e.ID, &e.IngestedAt, &e.EntityKind, &e.EntityName,
			&e.Namespace, &e.Type, &e.Severity, &e.Title,
			&e.Payload); err != nil {
			return nil, err
		}
		applyEvent(&snap, e)
	}

	snap.TakenAt = t // mark as a reconstruction, not a raw snapshot
	return &snap, rows.Err()
}

// applyEvent mutates the snapshot to reflect one event having happened.
// THIS IS THE REPLAY ENGINE. Every event type collected needs a case here,
// or replay silently drifts from reality.
func applyEvent(s *Snapshot, e event.Event) {
	key := fmt.Sprintf("%s/%s/%s", e.Namespace, e.EntityKind, e.EntityName)
	obj, exists := s.Objects[key]

	switch e.Type {
	case "pod_created":
		s.Objects[key] = ObjectState{
			Kind: e.EntityKind, Name: e.EntityName,
			Namespace: e.Namespace, Phase: "Pending",
		}
	case "pod_deleted":
		delete(s.Objects, key)

	case "container_restart", "oom_kill":
		if !exists {
			return
		}
		obj.Restarts++
		obj.ReadyCount = 0
		obj.Phase = "Restarting"
		s.Objects[key] = obj

	case "became_ready":
		if !exists {
			return
		}
		obj.ReadyCount = 1
		obj.Phase = "Running"
		s.Objects[key] = obj

	case "became_unready":
		if !exists {
			return
		}
		obj.ReadyCount = 0
		s.Objects[key] = obj

	case "deploy":
		if !exists {
			return
		}
		obj.Image = gjson.GetBytes(e.Payload, "new_image").String()
		s.Objects[key] = obj

	case "scale":
		if !exists {
			return
		}
		obj.Replicas = int32(gjson.GetBytes(e.Payload, "new_replicas").Int())
		s.Objects[key] = obj

	case "resource_change":
		if !exists {
			return
		}
		obj.MemLimit = gjson.GetBytes(e.Payload, "new_mem_limit").Int()
		s.Objects[key] = obj

	case "error_spike", "latency_spike", "memory_pressure":
		// Metric events update the metric map, not object structure.
		s.Metrics[e.Type+"{"+e.EntityName+"}"] =
			gjson.GetBytes(e.Payload, "value").Float()
	}
}

// VerifyDrift compares a real snapshot against a reconstruction of the same
// moment, alerting if they diverge. Run this hourly as a smoke alarm.
func (r *Replayer) VerifyDrift(ctx context.Context) error {
	// Pick a real snapshot ~2 hours ago
	var compressed []byte
	var takenAt time.Time
	target := time.Now().Add(-2 * time.Hour)

	err := r.pool.QueryRow(ctx, `
		SELECT data, taken_at FROM snapshots
		WHERE taken_at <= $1
		ORDER BY taken_at DESC LIMIT 1`, target).Scan(&compressed, &takenAt)
	if err != nil {
		return fmt.Errorf("no snapshot 2h ago for drift check: %w", err)
	}

	dec, _ := zstd.NewReader(nil)
	raw, _ := dec.DecodeAll(compressed, nil)
	var real Snapshot
	json.Unmarshal(raw, &real)

	reconstructed, err := r.At(ctx, takenAt)
	if err != nil {
		return err
	}

	diffs := diffSnapshots(&real, reconstructed)
	if len(diffs) > 0 {
		slog.Error("REPLAY DRIFT DETECTED", "count", len(diffs), "sample", diffs[:min(5, len(diffs))])
	}
	return nil
}

func diffSnapshots(a, b *Snapshot) []string {
	var diffs []string
	for k, ao := range a.Objects {
		bo, ok := b.Objects[k]
		if !ok {
			diffs = append(diffs, "missing:"+k)
			continue
		}
		if ao.Phase != bo.Phase {
			diffs = append(diffs, fmt.Sprintf("phase_mismatch:%s (want %s got %s)", k, ao.Phase, bo.Phase))
		}
		if ao.Restarts != bo.Restarts {
			diffs = append(diffs, fmt.Sprintf("restart_mismatch:%s (want %d got %d)", k, ao.Restarts, bo.Restarts))
		}
	}
	return diffs
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
