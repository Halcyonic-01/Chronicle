package main

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/google/go-github/v60/github"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/api"
	"golang.org/x/sync/errgroup"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"

	"github.com/Halcyonic-01/Chronicle/internal/collect"
	"github.com/Halcyonic-01/Chronicle/internal/event"
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
