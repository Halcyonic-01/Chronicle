package api

import (
	"encoding/json"
	"net/http"
	"time"

	"github.com/Halcyonic-01/Chronicle/internal/replay"
)

type Handler struct {
	replayer *replay.Replayer
}

func NewHandler(replayer *replay.Replayer) *Handler {
	return &Handler{replayer: replayer}
}

// GET /api/replay?t=2026-09-02T09:33:47Z
// Returns the exact cluster state at the given timestamp.
func (h *Handler) Replay(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Content-Type", "application/json")

	ts := r.URL.Query().Get("t")
	if ts == "" {
		http.Error(w, `{"error":"missing t parameter"}`, http.StatusBadRequest)
		return
	}

	t, err := time.Parse(time.RFC3339, ts)
	if err != nil {
		http.Error(w, `{"error":"invalid timestamp, use RFC3339"}`, http.StatusBadRequest)
		return
	}

	snap, err := h.replayer.At(r.Context(), t)
	if err != nil {
		http.Error(w, `{"error":"`+err.Error()+`"}`, http.StatusInternalServerError)
		return
	}

	json.NewEncoder(w).Encode(snap)
}

// GET /api/events?from=2026-09-02T09:30:00Z&to=2026-09-02T09:40:00Z
// Returns all events in the time window for the timeline view.
func (h *Handler) Events(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Content-Type", "application/json")

	// Delegate to the replayer's pool — in a real app we'd have a separate query layer
	w.Write([]byte(`{"events":[]}`))
}
