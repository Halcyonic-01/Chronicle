package api

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/Halcyonic-01/Chronicle/internal/event"
	"github.com/Halcyonic-01/Chronicle/internal/graph"
	"github.com/Halcyonic-01/Chronicle/internal/heal"
	"github.com/Halcyonic-01/Chronicle/internal/rca"
	"github.com/Halcyonic-01/Chronicle/internal/replay"
	"github.com/Halcyonic-01/Chronicle/internal/store"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

type Handler struct {
	replayer  *replay.Replayer
	analyzer  *rca.Analyzer
	rcaDB     *rca.PostgresEventSource
	healer    *heal.Engine
	graph     *store.GraphStore
	actions   heal.ActionStore
	k8s       kubernetes.Interface
	execution *heal.Controller
	recent    *store.RecentCache
}

func NewHandler(replayer *replay.Replayer, analyzer *rca.Analyzer, rcaDB *rca.PostgresEventSource, healer *heal.Engine, graphStore *store.GraphStore, actionStore heal.ActionStore, k8sClient kubernetes.Interface, execution ...*heal.Controller) *Handler {
	var controller *heal.Controller
	if len(execution) > 0 {
		controller = execution[0]
	}
	return &Handler{replayer: replayer, analyzer: analyzer, rcaDB: rcaDB, healer: healer, graph: graphStore, actions: actionStore, k8s: k8sClient, execution: controller}
}

// WithRecentCache serves the console's default event view from Redis instead of
// paging the events table on every poll. It is optional: without it, and
// whenever the cache cannot satisfy a request, the handler reads PostgreSQL.
func (h *Handler) WithRecentCache(cache *store.RecentCache) *Handler {
	h.recent = cache
	return h
}

// cachedEvents returns the requested page from Redis, or nil when the cache
// cannot answer it: a paged or narrowed request, a cache miss, or a window
// deeper than the cache retains.
func (h *Handler) cachedEvents(ctx context.Context, from, to time.Time, limit, offset int) []event.Event {
	if h.recent == nil || offset != 0 || limit > h.recent.Limit() {
		return nil
	}
	events, err := h.recent.Recent(ctx, from, to, limit)
	if err != nil {
		slog.Warn("recent event cache unavailable; falling back to PostgreSQL", "err", err)
		return nil
	}
	// Fewer events than asked for may mean the cache is merely cold, so it is
	// only authoritative once it has filled the page.
	if len(events) < limit {
		return nil
	}
	return events
}

// allowedOrigin is read once at startup. Chronicle's console is served from
// the same origin as the API, so the default is to send no CORS header at all
// rather than the wildcard that previously let any page on any origin read
// every incident, event and healing decision.
var allowedOrigin = os.Getenv("CHRONICLE_ALLOWED_ORIGIN")

func writeJSONHeaders(w http.ResponseWriter) {
	if allowedOrigin != "" {
		w.Header().Set("Access-Control-Allow-Origin", allowedOrigin)
		w.Header().Set("Vary", "Origin")
	}
	w.Header().Set("Content-Type", "application/json")
}

// RequireAPIToken guards the API when CHRONICLE_API_TOKEN is set. Chronicle's
// Service is ClusterIP and the token is unset by default, which keeps local
// development and the test scripts working; set it whenever the API is reachable
// beyond the cluster.
func RequireAPIToken(next http.Handler) http.Handler {
	token := os.Getenv("CHRONICLE_API_TOKEN")
	if token == "" {
		return next
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		presented := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
		if subtle.ConstantTimeCompare([]byte(presented), []byte(token)) != 1 {
			writeJSONHeaders(w)
			http.Error(w, `{"error":"API authentication required"}`, http.StatusUnauthorized)
			return
		}
		next.ServeHTTP(w, r)
	})
}

type analyzeResponse struct {
	*rca.Result
	Action *heal.Action `json:"action,omitempty"`
}

// POST /api/analyze?event_id=123
func (h *Handler) Analyze(w http.ResponseWriter, r *http.Request) {
	writeJSONHeaders(w)

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
	writeJSONHeaders(w)

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
	// Only an explicit current-time request may overlay live Kubernetes health.
	// Slider and historical requests remain pure event-store reconstruction.
	if h.k8s != nil && r.URL.Query().Get("live") == "1" && isCurrentReplayTime(t) {
		h.overlayLiveReplayState(r.Context(), snap)
	}
	if h.graph != nil {
		if edges, graphErr := h.graph.At(r.Context(), t); graphErr == nil {
			snap.Edges = edges
		}
	}

	json.NewEncoder(w).Encode(snap)
}

func isCurrentReplayTime(t time.Time) bool {
	age := time.Since(t)
	return age >= -time.Minute && age <= 2*time.Minute
}

// overlayLiveReplayState replaces the health fields for Pods and Deployments
// with their current Kubernetes status. The historical snapshot remains the
// source for all other fields and for all non-current replay points.
func (h *Handler) overlayLiveReplayState(ctx context.Context, snap *replay.Snapshot) {
	if snap == nil || h.k8s == nil {
		return
	}

	if pods, err := h.k8s.CoreV1().Pods("").List(ctx, metav1.ListOptions{}); err == nil {
		for _, pod := range pods.Items {
			key := fmt.Sprintf("%s/Pod/%s", pod.Namespace, pod.Name)
			obj, ok := snap.Objects[key]
			if !ok {
				continue
			}

			ready := false
			for _, condition := range pod.Status.Conditions {
				if condition.Type == corev1.PodReady && condition.Status == corev1.ConditionTrue {
					ready = true
					break
				}
			}
			var restarts int32
			for _, status := range append(pod.Status.InitContainerStatuses, pod.Status.ContainerStatuses...) {
				restarts += status.RestartCount
			}

			obj.ReadyCount = boolInt32(ready)
			obj.Restarts = restarts
			obj.StatusReason = pod.Status.Reason
			obj.StatusMessage = pod.Status.Message
			switch {
			case pod.Status.Phase == corev1.PodFailed:
				obj.Phase = "Failed"
			case ready:
				obj.Phase = "Running"
			default:
				obj.Phase = "Pending"
			}
			snap.Objects[key] = obj
		}
	}

	if deployments, err := h.k8s.AppsV1().Deployments("").List(ctx, metav1.ListOptions{}); err == nil {
		for _, deployment := range deployments.Items {
			key := fmt.Sprintf("%s/Deployment/%s", deployment.Namespace, deployment.Name)
			obj, ok := snap.Objects[key]
			if !ok {
				continue
			}

			desired := int32(1)
			if deployment.Spec.Replicas != nil {
				desired = *deployment.Spec.Replicas
			}
			obj.Replicas = desired
			obj.ReadyCount = deployment.Status.ReadyReplicas
			if deployment.Status.ReadyReplicas >= desired {
				obj.Phase = "Running"
				obj.StatusReason = ""
				obj.StatusMessage = ""
			} else {
				obj.Phase = "Pending"
				obj.StatusReason = "NotReady"
				obj.StatusMessage = fmt.Sprintf("%d/%d replicas ready", deployment.Status.ReadyReplicas, desired)
			}
			snap.Objects[key] = obj
		}
	}
}

// GET /api/events?from=2026-09-02T09:30:00Z&to=2026-09-02T09:40:00Z
// Returns all events in the time window for the timeline view.
func (h *Handler) Events(w http.ResponseWriter, r *http.Request) {
	writeJSONHeaders(w)

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
	offset := 0
	if value := r.URL.Query().Get("offset"); value != "" {
		if _, err := fmt.Sscanf(value, "%d", &offset); err != nil || offset < 0 {
			http.Error(w, `{"error":"invalid offset"}`, http.StatusBadRequest)
			return
		}
	}
	items := h.cachedEvents(r.Context(), from, to, limit, offset)
	if items == nil {
		loaded, err := h.rcaDB.RecentEvents(r.Context(), from, to, limit, offset)
		if err != nil {
			http.Error(w, `{"error":"failed to load events"}`, http.StatusInternalServerError)
			return
		}
		items = loaded
	}
	total, err := h.rcaDB.CountEvents(r.Context(), from, to)
	if err != nil {
		http.Error(w, `{"error":"failed to count events"}`, http.StatusInternalServerError)
		return
	}
	if items == nil {
		items = []event.Event{}
	}
	json.NewEncoder(w).Encode(map[string]any{"events": items, "total": total, "offset": offset, "from": from, "to": to})
}

type graphResponse struct {
	Nodes []graph.Node `json:"nodes"`
	Edges []graph.Edge `json:"edges"`
}

type postureResource struct {
	Kind      string `json:"kind"`
	Namespace string `json:"namespace"`
	Name      string `json:"name"`
	Status    string `json:"status"`
	Message   string `json:"message"`
	Ready     int32  `json:"ready"`
	Desired   int32  `json:"desired"`
	Restarts  int32  `json:"restarts"`
}

type postureResponse struct {
	Resources []postureResource `json:"resources"`
	Summary   struct {
		Total    int `json:"total"`
		Healthy  int `json:"healthy"`
		Warning  int `json:"warning"`
		Critical int `json:"critical"`
	} `json:"summary"`
}

// Posture returns live Kubernetes health, deliberately separate from the
// historical event stream. A resource is healthy only when its current
// Kubernetes status says it is ready.
func (h *Handler) Posture(w http.ResponseWriter, r *http.Request) {
	writeJSONHeaders(w)
	if h.k8s == nil {
		http.Error(w, `{"error":"kubernetes client unavailable"}`, http.StatusServiceUnavailable)
		return
	}

	result := postureResponse{Resources: []postureResource{}}
	if pods, err := h.k8s.CoreV1().Pods("").List(r.Context(), metav1.ListOptions{}); err == nil {
		for _, pod := range pods.Items {
			ready := false
			for _, condition := range pod.Status.Conditions {
				if condition.Type == corev1.PodReady && condition.Status == corev1.ConditionTrue {
					ready = true
					break
				}
			}
			var restarts int32
			for _, status := range append(pod.Status.InitContainerStatuses, pod.Status.ContainerStatuses...) {
				restarts += status.RestartCount
			}
			status := "healthy"
			message := string(pod.Status.Phase)
			if pod.Status.Phase == corev1.PodFailed {
				status = "critical"
				message = "Pod failed"
			} else if pod.Status.Phase == corev1.PodPending || !ready {
				status = "warning"
				if !ready {
					message = "Pod is not ready"
				}
			}
			result.Resources = append(result.Resources, postureResource{Kind: "Pod", Namespace: pod.Namespace, Name: pod.Name, Status: status, Message: message, Ready: boolInt32(ready), Desired: 1, Restarts: restarts})
		}
	}
	if deployments, err := h.k8s.AppsV1().Deployments("").List(r.Context(), metav1.ListOptions{}); err == nil {
		for _, deployment := range deployments.Items {
			desired := int32(1)
			if deployment.Spec.Replicas != nil {
				desired = *deployment.Spec.Replicas
			}
			status := "healthy"
			message := fmt.Sprintf("%d/%d replicas ready", deployment.Status.ReadyReplicas, desired)
			if deployment.Status.ReadyReplicas < desired {
				status = "warning"
			}
			result.Resources = append(result.Resources, postureResource{Kind: "Deployment", Namespace: deployment.Namespace, Name: deployment.Name, Status: status, Message: message, Ready: deployment.Status.ReadyReplicas, Desired: desired})
		}
	}
	sort.Slice(result.Resources, func(i, j int) bool {
		rank := map[string]int{"critical": 0, "warning": 1, "healthy": 2}
		if rank[result.Resources[i].Status] != rank[result.Resources[j].Status] {
			return rank[result.Resources[i].Status] < rank[result.Resources[j].Status]
		}
		return result.Resources[i].Namespace+"/"+result.Resources[i].Name < result.Resources[j].Namespace+"/"+result.Resources[j].Name
	})
	for _, resource := range result.Resources {
		result.Summary.Total++
		switch resource.Status {
		case "critical":
			result.Summary.Critical++
		case "warning":
			result.Summary.Warning++
		default:
			result.Summary.Healthy++
		}
	}
	json.NewEncoder(w).Encode(result)
}

func boolInt32(value bool) int32 {
	if value {
		return 1
	}
	return 0
}

// GET /api/graph returns the current dependency graph used by RCA.
// Pass ?at=<RFC3339> to query the temporal graph at a historical instant.
func (h *Handler) Graph(w http.ResponseWriter, r *http.Request) {
	writeJSONHeaders(w)
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
	writeJSONHeaders(w)
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

// POST /api/heal/actions/{id}/approve or /deny. Approval is authenticated;
// live execution additionally requires the controller's safety gates.
func (h *Handler) DecideHealingAction(w http.ResponseWriter, r *http.Request) {
	writeJSONHeaders(w)
	if r.Method != http.MethodPost {
		http.Error(w, `{"error":"use POST"}`, http.StatusMethodNotAllowed)
		return
	}
	if !heal.ApprovalAuthorized(r) {
		http.Error(w, `{"error":"approval authentication required"}`, http.StatusUnauthorized)
		return
	}
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
	if approved := parts[4] == "approve"; approved && h.execution != nil && h.execution.Policy.LiveEnabled {
		_, _ = h.execution.ExecuteApproved(r.Context(), action)
	}
	json.NewEncoder(w).Encode(action)
}
