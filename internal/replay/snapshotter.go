package replay

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/Halcyonic-01/Chronicle/internal/graph"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/klauspost/compress/zstd"
	"github.com/oklog/ulid/v2"
	"github.com/prometheus/client_golang/api"
	v1 "github.com/prometheus/client_golang/api/prometheus/v1"
	"github.com/prometheus/common/model"
	"golang.org/x/sync/errgroup"
	appsv1 "k8s.io/api/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

// snapshotMetrics are the PromQL queries we sample into every snapshot.
var snapshotMetrics = map[string]string{
	"cpu_usage":    `sum by (pod) (rate(container_cpu_usage_seconds_total[2m]))`,
	"memory_usage": `sum by (pod) (container_memory_working_set_bytes)`,
	"error_rate":   `sum by (pod) (rate(http_requests_total{status=~"5.."}[2m]))`,
}

// Snapshotter takes periodic full snapshots of the cluster state.
type Snapshotter struct {
	client kubernetes.Interface
	prom   v1.API
	pool   *pgxpool.Pool
	graph  *graph.Graph
}

func NewSnapshotter(client kubernetes.Interface, promClient api.Client, pool *pgxpool.Pool, g *graph.Graph) *Snapshotter {
	return &Snapshotter{
		client: client,
		prom:   v1.NewAPI(promClient),
		pool:   pool,
		graph:  g,
	}
}

// Run takes a snapshot every 5 minutes, continuously.
func (s *Snapshotter) Run(ctx context.Context) error {
	ticker := time.NewTicker(5 * time.Minute)
	defer ticker.Stop()

	// Take one immediately on startup
	if _, err := s.Take(ctx); err != nil {
		fmt.Printf("initial snapshot failed: %v\n", err)
	}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if _, err := s.Take(ctx); err != nil {
				fmt.Printf("snapshot failed: %v\n", err)
			}
		}
	}
}

// Take captures a complete point-in-time snapshot of the cluster.
// It fetches all resource types in parallel to avoid internal inconsistency.
func (s *Snapshotter) Take(ctx context.Context) (*Snapshot, error) {
	snap := &Snapshot{
		ID:      ulid.Make().String(),
		TakenAt: time.Now().UTC(),
		Objects: make(map[string]ObjectState),
		Metrics: make(map[string]float64),
	}

	g, gctx := errgroup.WithContext(ctx)
	var mu sync.Mutex

	// Fetch Pods
	g.Go(func() error {
		pods, err := s.client.CoreV1().Pods("").List(gctx, metav1.ListOptions{})
		if err != nil {
			return err
		}
		mu.Lock()
		defer mu.Unlock()
		for _, p := range pods.Items {
			key := fmt.Sprintf("%s/Pod/%s", p.Namespace, p.Name)
			snap.Objects[key] = podToState(p)
		}
		return nil
	})

	// Fetch Deployments
	g.Go(func() error {
		deps, err := s.client.AppsV1().Deployments("").List(gctx, metav1.ListOptions{})
		if err != nil {
			return err
		}
		mu.Lock()
		defer mu.Unlock()
		for _, d := range deps.Items {
			key := fmt.Sprintf("%s/Deployment/%s", d.Namespace, d.Name)
			snap.Objects[key] = deploymentToState(d)
		}
		return nil
	})

	// Fetch Services
	g.Go(func() error {
		svcs, err := s.client.CoreV1().Services("").List(gctx, metav1.ListOptions{})
		if err != nil {
			return err
		}
		mu.Lock()
		defer mu.Unlock()
		for _, svc := range svcs.Items {
			key := fmt.Sprintf("%s/Service/%s", svc.Namespace, svc.Name)
			snap.Objects[key] = ObjectState{
				Kind:      "Service",
				Name:      svc.Name,
				Namespace: svc.Namespace,
				Labels:    svc.Labels,
				Phase:     "Active",
			}
		}
		return nil
	})

	// Fetch Nodes
	g.Go(func() error {
		nodes, err := s.client.CoreV1().Nodes().List(gctx, metav1.ListOptions{})
		if err != nil {
			return err
		}
		mu.Lock()
		defer mu.Unlock()
		for _, n := range nodes.Items {
			phase := "Ready"
			for _, c := range n.Status.Conditions {
				if c.Type == "Ready" && c.Status != "True" {
					phase = "NotReady"
				}
			}
			snap.Objects["cluster/Node/"+n.Name] = ObjectState{
				Kind:  "Node",
				Name:  n.Name,
				Phase: phase,
			}
		}
		return nil
	})

	// Sample metrics (best-effort — failures don't abort the snapshot)
	g.Go(func() error {
		for name, query := range snapshotMetrics {
			result, _, err := s.prom.Query(gctx, query, snap.TakenAt)
			if err != nil {
				continue
			}
			vec, ok := result.(model.Vector)
			if !ok {
				continue
			}
			mu.Lock()
			for _, sample := range vec {
				k := name + "{" + string(sample.Metric["pod"]) + "}"
				snap.Metrics[k] = float64(sample.Value)
			}
			mu.Unlock()
		}
		return nil
	})

	if err := g.Wait(); err != nil {
		return nil, err
	}

	snap.Edges = s.graph.CurrentEdges()
	return snap, s.store(ctx, snap)
}

func deploymentToState(d appsv1.Deployment) ObjectState {
	var image string
	if len(d.Spec.Template.Spec.Containers) > 0 {
		image = d.Spec.Template.Spec.Containers[0].Image
	}
	ready := int32(0)
	if d.Status.ReadyReplicas > 0 {
		ready = d.Status.ReadyReplicas
	}
	replicas := int32(1)
	if d.Spec.Replicas != nil {
		replicas = *d.Spec.Replicas
	}
	return ObjectState{
		Kind:       "Deployment",
		Name:       d.Name,
		Namespace:  d.Namespace,
		Image:      image,
		Replicas:   replicas,
		ReadyCount: ready,
		Phase:      "Running",
		Labels:     d.Labels,
	}
}

// store compresses and writes the snapshot to Postgres.
func (s *Snapshotter) store(ctx context.Context, snap *Snapshot) error {
	raw, err := json.Marshal(snap)
	if err != nil {
		return err
	}

	enc, err := zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedDefault))
	if err != nil {
		return err
	}
	compressed := enc.EncodeAll(raw, nil)

	_, err = s.pool.Exec(ctx,
		`INSERT INTO snapshots (id, taken_at, data, obj_count) VALUES ($1, $2, $3, $4)`,
		snap.ID, snap.TakenAt, compressed, len(snap.Objects),
	)
	return err
}
