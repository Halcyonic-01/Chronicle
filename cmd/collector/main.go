package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/google/go-github/v60/github"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/api"
	"golang.org/x/sync/errgroup"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"

	"k8s.io/client-go/tools/clientcmd"

	"net/http"

	chronicleapi "github.com/Halcyonic-01/Chronicle/internal/api"
	"github.com/Halcyonic-01/Chronicle/internal/collect"
	"github.com/Halcyonic-01/Chronicle/internal/event"
	"github.com/Halcyonic-01/Chronicle/internal/graph"
	"github.com/Halcyonic-01/Chronicle/internal/replay"
	"github.com/Halcyonic-01/Chronicle/internal/store"
)

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	events := make(chan event.Event, 10_000) // buffered: absorbs bursts

	// errgroup: if any collector dies, we learn about it and shut down cleanly.
	g, gctx := errgroup.WithContext(ctx)

	// Init K8s client
	k8sClient, err := newK8sClient()
	if err != nil {
		slog.Error("failed to init k8s client", "err", err)
		os.Exit(1)
	}

	// Init GitHub client
	ghClient := github.NewClient(nil)

	inCluster := os.Getenv("KUBERNETES_SERVICE_HOST") != ""

	// Init Prometheus client
	promURL := os.Getenv("PROMETHEUS_URL")
	if promURL == "" {
		if inCluster {
			promURL = "http://monitoring-kube-prometheus-prometheus.monitoring.svc.cluster.local:9090"
		} else {
			promURL = "http://localhost:9090"
		}
	}
	promClient, err := api.NewClient(api.Config{Address: promURL})
	if err != nil {
		slog.Error("failed to init prom client", "err", err)
		os.Exit(1)
	}

	// Init Postgres Pool
	pgURL := os.Getenv("POSTGRES_URL")
	if pgURL == "" {
		if inCluster {
			pgURL = "postgres://postgres:postgres@postgres-postgresql.chronicle.svc.cluster.local:5432/postgres?sslmode=disable"
		} else {
			pgURL = "postgres://postgres:postgres@localhost:5433/postgres?sslmode=disable"
		}
	}
	pool, err := pgxpool.New(ctx, pgURL)
	if err != nil {
		slog.Error("failed to connect to postgres", "err", err)
		os.Exit(1)
	}
	defer pool.Close()

	g.Go(func() error { return collect.NewK8sCollector(k8sClient, events).Run(gctx) })
	g.Go(func() error {
		return collect.NewGitHubCollector(ghClient, "Halcyonic-01", "Chronicle", events).Run(gctx)
	})
	g.Go(func() error { return collect.NewPromCollector(promClient, events).Run(gctx) })
	g.Go(func() error { return collect.NewLokiCollector(events).Run(gctx) })
	g.Go(func() error { return store.NewWriter(pool).Run(gctx, events) })

	// Phase 3: Snapshotter — takes a full cluster snapshot every 5 minutes.
	inMemGraph := graph.New()
	snapshotter := replay.NewSnapshotter(k8sClient, promClient, pool, inMemGraph)
	g.Go(func() error { return snapshotter.Run(gctx) })

	// Phase 3: HTTP API server — serves the replay endpoint to the dashboard.
	replayer := replay.NewReplayer(pool)
	apiHandler := chronicleapi.NewHandler(replayer)
	mux := http.NewServeMux()
	mux.HandleFunc("/api/replay", apiHandler.Replay)
	mux.HandleFunc("/api/events", apiHandler.Events)
	g.Go(func() error {
		slog.Info("Chronicle API listening", "addr", ":8181")
		srv := &http.Server{Addr: ":8181", Handler: mux}
		go func() {
			<-gctx.Done()
			srv.Close()
		}()
		if err := srv.ListenAndServe(); err != http.ErrServerClosed {
			return err
		}
		return nil
	})

	// Graph sync loop
	g.Go(func() error {
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()
		meshBuilder := graph.NewMeshBuilder(promClient)
		graphStore := store.NewGraphStore(pool)

		// Run once immediately
		syncGraph := func() {
			var allEdges []graph.Edge
			if pods, err := k8sClient.CoreV1().Pods("").List(gctx, metav1.ListOptions{}); err == nil {
				if svcs, err := k8sClient.CoreV1().Services("").List(gctx, metav1.ListOptions{}); err == nil {
					allEdges = append(allEdges, graph.BuildServiceEdges(svcs.Items, pods.Items)...)
					knownSvcs := make(map[string]bool)
					for _, s := range svcs.Items {
						knownSvcs[s.Name] = true
					}
					for _, p := range pods.Items {
						allEdges = append(allEdges, graph.InferCallEdges(p, knownSvcs)...)
					}
				}
			} else {
				slog.Warn("failed to fetch k8s resources for graph", "err", err)
			}

			if runtimeEdges, err := meshBuilder.RuntimeEdges(gctx); err == nil {
				allEdges = append(allEdges, runtimeEdges...)
			}

			if len(allEdges) > 0 {
				// Deduplicate edges to avoid primary key violations
				deduped := make([]graph.Edge, 0, len(allEdges))
				seen := make(map[string]bool)
				for _, e := range allEdges {
					key := fmt.Sprintf("%s|%s|%s", e.From.Key(), e.To.Key(), e.Kind)
					if !seen[key] {
						seen[key] = true
						deduped = append(deduped, e)
					}
				}

				if err := graphStore.Sync(gctx, deduped); err != nil {
					slog.Error("failed to sync graph to postgres", "err", err)
				}
			}
		}

		syncGraph() // first run

		for {
			select {
			case <-gctx.Done():
				return gctx.Err()
			case <-ticker.C:
				syncGraph()
			}
		}
	})

	slog.Info("Chronicle collectors started")

	if err := g.Wait(); err != nil && !errors.Is(err, context.Canceled) {
		slog.Error("collector failed", "err", err)
		os.Exit(1)
	}
}

func newK8sClient() (kubernetes.Interface, error) {
	config, err := rest.InClusterConfig()
	if err != nil {
		// fallback to local kubeconfig for out-of-cluster dev
		kubeconfig := os.Getenv("KUBECONFIG")
		if kubeconfig == "" {
			kubeconfig = os.Getenv("HOME") + "/.kube/config"
		}
		config, err = clientcmd.BuildConfigFromFlags("", kubeconfig)
		if err != nil {
			return nil, err
		}
	}
	return kubernetes.NewForConfig(config)
}
