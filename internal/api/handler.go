package api

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/Halcyonic-01/Chronicle/internal/event"
	"github.com/Halcyonic-01/Chronicle/internal/graph"
	"github.com/Halcyonic-01/Chronicle/internal/heal"
	"github.com/Halcyonic-01/Chronicle/internal/rca"
	"github.com/Halcyonic-01/Chronicle/internal/replay"
	"github.com/Halcyonic-01/Chronicle/internal/store"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

type Handler struct {
	replayer *replay.Replayer
	analyzer *rca.Analyzer
	rcaDB    *rca.PostgresEventSource
	healer   *heal.Engine
	graph    *store.GraphStore
	actions  heal.ActionStore
	k8s      kubernetes.Interface
}

func NewHandler(replayer *replay.Replayer, analyzer *rca.Analyzer, rcaDB *rca.PostgresEventSource, healer *heal.Engine, graphStore *store.GraphStore, actionStore heal.ActionStore, k8sClient kubernetes.Interface) *Handler {
	return &Handler{replayer: replayer, analyzer: analyzer, rcaDB: rcaDB, healer: healer, graph: graphStore, actions: actionStore, k8s: k8sClient}
}

type analyzeResponse struct {
	*rca.Result
	Action *heal.Action `json:"action,omitempty"`
}

// POST /api/analyze?event_id=123
func (h *Handler) Analyze(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Content-Type", "application/json")

	if r.Method != http.MethodPost {
		http.Error(w, `{"error":"use POST"}`, http.StatusMethodNotAllowed)
		return
	}

	eventID := r.URL.Query().Get("event_id")
	if eventID == "" {
		http.Error(w, `{"error":"missing event_id"}`, http.StatusBadRequest)
		return
	}

	evt, err := h.rcaDB.GetEvent(r.Context(), eventID)
	if err != nil {
		http.Error(w, `{"error":"event not found"}`, http.StatusNotFound)
		return
	}

	res, err := h.analyzer.Analyze(r.Context(), *evt)
	if err != nil {
		http.Error(w, `{"error":"`+err.Error()+`"}`, http.StatusInternalServerError)
		return
	}

	var action *heal.Action
	if h.healer != nil {
		action, err = h.healer.Evaluate(r.Context(), res)
		if err != nil {
			http.Error(w, `{"error":"`+err.Error()+`"}`, http.StatusInternalServerError)
			return
		}
	}

	json.NewEncoder(w).Encode(analyzeResponse{Result: res, Action: action})
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
	if h.graph != nil {
		if edges, graphErr := h.graph.At(r.Context(), t); graphErr == nil {
			snap.Edges = edges
		}
	}

	json.NewEncoder(w).Encode(snap)
}

// GET /api/events?from=2026-09-02T09:30:00Z&to=2026-09-02T09:40:00Z
// Returns all events in the time window for the timeline view.
func (h *Handler) Events(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Content-Type", "application/json")

	if r.Method != http.MethodGet {
		http.Error(w, `{"error":"use GET"}`, http.StatusMethodNotAllowed)
		return
	}
	to := time.Now().UTC()
	from := to.Add(-24 * time.Hour)
	if value := r.URL.Query().Get("from"); value != "" {
		parsed, err := time.Parse(time.RFC3339, value)
		if err != nil {
			http.Error(w, `{"error":"invalid from timestamp, use RFC3339"}`, http.StatusBadRequest)
			return
		}
		from = parsed
	}
	if value := r.URL.Query().Get("to"); value != "" {
		parsed, err := time.Parse(time.RFC3339, value)
		if err != nil {
			http.Error(w, `{"error":"invalid to timestamp, use RFC3339"}`, http.StatusBadRequest)
			return
		}
		to = parsed
	}
	limit := 100
	if value := r.URL.Query().Get("limit"); value != "" {
		if _, err := fmt.Sscanf(value, "%d", &limit); err != nil {
			http.Error(w, `{"error":"invalid limit"}`, http.StatusBadRequest)
			return
		}
	}
	items, err := h.rcaDB.RecentEvents(r.Context(), from, to, limit)
	if err != nil {
		http.Error(w, `{"error":"failed to load events"}`, http.StatusInternalServerError)
		return
	}
	if items == nil {
		items = []event.Event{}
	}
	json.NewEncoder(w).Encode(map[string]any{"events": items, "from": from, "to": to})
}

type graphResponse struct {
	Nodes []graph.Node `json:"nodes"`
	Edges []graph.Edge `json:"edges"`
}

// GET /api/graph returns the current dependency graph used by RCA.
// Pass ?at=<RFC3339> to query the temporal graph at a historical instant.
func (h *Handler) Graph(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Content-Type", "application/json")
	if h.graph == nil {
		http.Error(w, `{"error":"graph store unavailable"}`, http.StatusServiceUnavailable)
		return
	}
	var edges []graph.Edge
	var err error
	historical := false
	if raw := r.URL.Query().Get("at"); raw != "" {
		historical = true
		at, parseErr := time.Parse(time.RFC3339, raw)
		if parseErr != nil {
			http.Error(w, `{"error":"invalid at timestamp, use RFC3339"}`, http.StatusBadRequest)
			return
		}
		edges, err = h.graph.At(r.Context(), at)
	} else {
		edges, err = h.graph.Current(r.Context())
	}
	if err != nil {
		http.Error(w, `{"error":"failed to load graph"}`, http.StatusInternalServerError)
		return
	}
	seen := map[string]graph.Node{}
	for _, edge := range edges {
		seen[edge.From.Key()] = edge.From
		seen[edge.To.Key()] = edge.To
	}
	// Include live resources even when they currently have no dependency edge.
	// Otherwise isolated services silently disappear from the graph UI.
	if h.k8s != nil && !historical {
		if items, err := h.k8s.CoreV1().Services("").List(r.Context(), metav1.ListOptions{}); err == nil {
			for _, item := range items.Items {
				n := graph.Node{Kind: "Service", Name: item.Name, Namespace: item.Namespace}
				seen[n.Key()] = n
			}
		}
		if items, err := h.k8s.AppsV1().Deployments("").List(r.Context(), metav1.ListOptions{}); err == nil {
			for _, item := range items.Items {
				n := graph.Node{Kind: "Deployment", Name: item.Name, Namespace: item.Namespace}
				seen[n.Key()] = n
			}
		}
		if items, err := h.k8s.CoreV1().Pods("").List(r.Context(), metav1.ListOptions{}); err == nil {
			for _, item := range items.Items {
				n := graph.Node{Kind: "Pod", Name: item.Name, Namespace: item.Namespace}
				seen[n.Key()] = n
			}
		}
		if items, err := h.k8s.AppsV1().StatefulSets("").List(r.Context(), metav1.ListOptions{}); err == nil {
			for _, item := range items.Items {
				n := graph.Node{Kind: "StatefulSet", Name: item.Name, Namespace: item.Namespace}
				seen[n.Key()] = n
			}
		}
		if items, err := h.k8s.CoreV1().ConfigMaps("").List(r.Context(), metav1.ListOptions{}); err == nil {
			for _, item := range items.Items {
				n := graph.Node{Kind: "ConfigMap", Name: item.Name, Namespace: item.Namespace}
				seen[n.Key()] = n
			}
		}
		if items, err := h.k8s.CoreV1().Secrets("").List(r.Context(), metav1.ListOptions{}); err == nil {
			for _, item := range items.Items {
				n := graph.Node{Kind: "Secret", Name: item.Name, Namespace: item.Namespace}
				seen[n.Key()] = n
			}
		}
		if items, err := h.k8s.CoreV1().PersistentVolumeClaims("").List(r.Context(), metav1.ListOptions{}); err == nil {
			for _, item := range items.Items {
				n := graph.Node{Kind: "PersistentVolumeClaim", Name: item.Name, Namespace: item.Namespace}
				seen[n.Key()] = n
			}
		}
		if items, err := h.k8s.CoreV1().Nodes().List(r.Context(), metav1.ListOptions{}); err == nil {
			for _, item := range items.Items {
				n := graph.Node{Kind: "Node", Name: item.Name}
				seen[n.Key()] = n
			}
		}
		if items, err := h.k8s.NetworkingV1().Ingresses("").List(r.Context(), metav1.ListOptions{}); err == nil {
			for _, item := range items.Items {
				n := graph.Node{Kind: "Ingress", Name: item.Name, Namespace: item.Namespace}
				seen[n.Key()] = n
			}
		}
	}
	nodes := make([]graph.Node, 0, len(seen))
	for _, node := range seen {
		nodes = append(nodes, node)
	}
	if nodes == nil {
		nodes = []graph.Node{}
	}
	if edges == nil {
		edges = []graph.Edge{}
	}
	json.NewEncoder(w).Encode(graphResponse{Nodes: nodes, Edges: edges})
}

func (h *Handler) HealingActions(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Content-Type", "application/json")
	if h.actions == nil {
		http.Error(w, `{"error":"healing store unavailable"}`, http.StatusServiceUnavailable)
		return
	}
	actions, err := h.actions.ListActions(r.Context(), 100)
	if err != nil {
		http.Error(w, `{"error":"failed to load healing actions"}`, http.StatusInternalServerError)
		return
	}
	if actions == nil {
		actions = []heal.Action{}
	}
	json.NewEncoder(w).Encode(map[string]any{"actions": actions})
}

// POST /api/heal/actions/{id}/approve or /deny. This changes review state only;
// it never executes Kubernetes changes while actions are dry-run.
func (h *Handler) DecideHealingAction(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Content-Type", "application/json")
	if h.actions == nil {
		http.Error(w, `{"error":"healing store unavailable"}`, http.StatusServiceUnavailable)
		return
	}
	parts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
	if len(parts) != 5 || parts[0] != "api" || parts[1] != "heal" || parts[2] != "actions" || (parts[4] != "approve" && parts[4] != "deny") {
		http.Error(w, `{"error":"invalid healing action path"}`, http.StatusBadRequest)
		return
	}
	var input struct {
		By     string `json:"by"`
		Reason string `json:"reason"`
	}
	if r.Body != nil {
		_ = json.NewDecoder(r.Body).Decode(&input)
	}
	if strings.TrimSpace(input.By) == "" {
		input.By = "local-reviewer"
	}
	action, err := h.actions.DecideAction(r.Context(), parts[3], parts[4] == "approve", input.By, input.Reason)
	if err != nil {
		http.Error(w, `{"error":"action is no longer pending or was not found"}`, http.StatusConflict)
		return
	}
	json.NewEncoder(w).Encode(action)
}
