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
	"k8s.io/client-go/tools/leaderelection"
	"k8s.io/client-go/tools/leaderelection/resourcelock"

	"net/http"

	chronicleapi "github.com/Halcyonic-01/Chronicle/internal/api"
	"github.com/Halcyonic-01/Chronicle/internal/collect"
	"github.com/Halcyonic-01/Chronicle/internal/event"
	"github.com/Halcyonic-01/Chronicle/internal/graph"
	"github.com/Halcyonic-01/Chronicle/internal/heal"
	"github.com/Halcyonic-01/Chronicle/internal/rca"
	"github.com/Halcyonic-01/Chronicle/internal/replay"
	"github.com/Halcyonic-01/Chronicle/internal/store"
	chronicleweb "github.com/Halcyonic-01/Chronicle/internal/web"
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
	if token := os.Getenv("GITHUB_TOKEN"); token != "" {
		ghClient = github.NewTokenClient(ctx, token)
	}
	githubOwner := valueOrEnv("GITHUB_OWNER", "Halcyonic-01")
	githubRepo := valueOrEnv("GITHUB_REPO", "Chronicle")

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

	var eventBus *store.KafkaBus
	if brokers := os.Getenv("KAFKA_BROKERS"); brokers != "" {
		eventBus = store.NewKafkaBus(brokers, valueOrEnv("KAFKA_TOPIC", "chronicle.events"), valueOrEnv("KAFKA_GROUP", "chronicle-writer"))
		defer eventBus.Close()
	}
	var recentCache *store.RecentCache
	if address := os.Getenv("REDIS_ADDR"); address != "" {
		recentCache = store.NewRecentCache(address, os.Getenv("REDIS_PASSWORD"), 0)
		defer recentCache.Close()
	}
	collectorEvents := events
	if eventBus != nil {
		collectorEvents = make(chan event.Event, 10_000)
	}

	// Initialize Graph dependencies
	inMemGraph := graph.New()
	meshBuilder := graph.NewMeshBuilder(promClient)
	graphStore := store.NewGraphStore(pool)

	syncGraph := func(syncCtx context.Context) {
		var allEdges []graph.Edge
		graphReady := false
		pods, podErr := k8sClient.CoreV1().Pods("").List(syncCtx, metav1.ListOptions{})
		svcs, svcErr := k8sClient.CoreV1().Services("").List(syncCtx, metav1.ListOptions{})
		if podErr == nil && svcErr == nil {
			graphReady = true
			allEdges = append(allEdges, graph.BuildServiceEdges(svcs.Items, pods.Items)...)
			allEdges = append(allEdges, graph.BuildOwnerEdges(pods.Items)...)
			allEdges = append(allEdges, graph.BuildReferenceEdges(pods.Items)...)
			knownSvcs := make(map[string]bool)
			for _, s := range svcs.Items {
				knownSvcs[s.Namespace+"/"+s.Name] = true
			}
			for _, p := range pods.Items {
				allEdges = append(allEdges, graph.InferCallEdges(p, knownSvcs)...)
			}
		} else {
			slog.Warn("failed to fetch k8s resources for graph", "pod_err", podErr, "service_err", svcErr)
		}
		if ingresses, err := k8sClient.NetworkingV1().Ingresses("").List(syncCtx, metav1.ListOptions{}); err == nil {
			allEdges = append(allEdges, graph.BuildIngressEdges(ingresses.Items)...)
		} else {
			graphReady = false
			slog.Warn("failed to fetch ingresses for graph", "err", err)
		}

		if runtimeEdges, err := meshBuilder.RuntimeEdges(syncCtx); err == nil {
			allEdges = append(allEdges, runtimeEdges...)
		}

		if graphReady {
			deduped := make([]graph.Edge, 0, len(allEdges))
			seen := make(map[string]bool)
			for _, e := range allEdges {
				key := fmt.Sprintf("%s|%s|%s", e.From.Key(), e.To.Key(), e.Kind)
				if !seen[key] {
					seen[key] = true
					deduped = append(deduped, e)
				}
			}

			inMemGraph.SetEdges(deduped)
			if err := graphStore.Sync(syncCtx, deduped); err != nil {
				slog.Error("failed to sync graph to postgres", "err", err)
			}
		}
	}

	// Phase 3: Snapshotter — takes a full cluster snapshot every 5 minutes.
	snapshotter := replay.NewSnapshotter(k8sClient, promClient, pool, inMemGraph)

	// Collectors, graph sync, snapshots, and the event writer are singleton
	// workloads. Only the pod holding the Kubernetes Lease runs them.
	runLeaderWorkloads := func(leaderCtx context.Context) {
		if err := func() error {
			syncGraph(leaderCtx)
			leaderGroup, leaderCtx := errgroup.WithContext(leaderCtx)
			leaderGroup.Go(func() error { return collect.NewK8sCollector(k8sClient, collectorEvents).Run(leaderCtx) })
			leaderGroup.Go(func() error {
				return collect.NewGitHubCollector(ghClient, githubOwner, githubRepo, collectorEvents).Run(leaderCtx)
			})
			leaderGroup.Go(func() error { return collect.NewPromCollector(promClient, collectorEvents).Run(leaderCtx) })
			leaderGroup.Go(func() error { return collect.NewLokiCollector(collectorEvents).Run(leaderCtx) })
			if argocd := collect.NewArgoCollectorFromEnv(collectorEvents); argocd != nil {
				leaderGroup.Go(func() error { return argocd.Run(leaderCtx) })
			}
			if terraform := collect.NewTerraformCollectorFromEnv(collectorEvents); terraform != nil {
				leaderGroup.Go(func() error { return terraform.Run(leaderCtx) })
			}
			if eventBus != nil {
				leaderGroup.Go(func() error {
					for {
						select {
						case <-leaderCtx.Done():
							return leaderCtx.Err()
						case e := <-collectorEvents:
							if err := eventBus.Publish(leaderCtx, e); err != nil {
								slog.Warn("Kafka publish failed; retrying", "err", err)
								select {
								case <-leaderCtx.Done():
									return leaderCtx.Err()
								case <-time.After(2 * time.Second):
								}
							}
						}
					}
				})
				leaderGroup.Go(func() error {
					for {
						if err := eventBus.Consume(leaderCtx, events); err != nil {
							if errors.Is(err, context.Canceled) {
								return err
							}
							slog.Warn("Kafka consume failed; retrying", "err", err)
							select {
							case <-leaderCtx.Done():
								return leaderCtx.Err()
							case <-time.After(2 * time.Second):
							}
							continue
						}
					}
				})
			}
			leaderGroup.Go(func() error { return store.NewWriter(pool, recentCache).Run(leaderCtx, events) })
			leaderGroup.Go(func() error { return snapshotter.Run(leaderCtx) })
			leaderGroup.Go(func() error {
				ticker := time.NewTicker(30 * time.Second)
				defer ticker.Stop()
				for {
					select {
					case <-leaderCtx.Done():
						return leaderCtx.Err()
					case <-ticker.C:
						syncGraph(leaderCtx)
					}
				}
			})
			return leaderGroup.Wait()
		}(); err != nil && !errors.Is(err, context.Canceled) {
			slog.Error("leader workloads stopped", "err", err)
		}
	}

	// Phase 3/4: HTTP API server — serves the replay and RCA endpoints
	replayer := replay.NewReplayer(pool)

	// Phase 4: RCA Analyzer
	rcaDB := rca.NewPostgresEventSource(pool)
	narrator := rca.NewOpenAICompatibleNarratorFromEnv()
	analyzer := &rca.Analyzer{
		Events:   rcaDB,
		Graph:    &rca.ReplayerGraphSource{Replayer: replayer, Historical: graphStore},
		Narrator: narrator,
		MaxHops:  3,
	}
	healStore := heal.NewPostgresAuditStore(pool)
	healer := heal.NewEngine(healStore)
	healController := heal.NewController(healStore, heal.NewKubernetesExecutor(k8sClient))

	apiHandler := chronicleapi.NewHandler(replayer, analyzer, rcaDB, healer, graphStore, healStore, k8sClient, healController)
	mux := http.NewServeMux()
	mux.Handle("/", chronicleweb.Handler())
	mux.HandleFunc("/api/replay", apiHandler.Replay)
	mux.HandleFunc("/api/events", apiHandler.Events)
	mux.HandleFunc("/api/analyze", apiHandler.Analyze)
	mux.HandleFunc("/api/graph", apiHandler.Graph)
	mux.HandleFunc("/api/heal/actions", apiHandler.HealingActions)
	mux.HandleFunc("/api/heal/actions/", apiHandler.DecideHealingAction)
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

	identity, err := os.Hostname()
	if err != nil || identity == "" {
		identity = fmt.Sprintf("chronicle-%d", os.Getpid())
	}
	if podName := os.Getenv("POD_NAME"); podName != "" {
		identity = podName
	}
	lockNamespace := valueOrEnv("LEADER_ELECTION_NAMESPACE", "chronicle")
	lockName := valueOrEnv("LEADER_ELECTION_NAME", "chronicle-leader")
	lock, err := resourcelock.New(
		resourcelock.LeasesResourceLock,
		lockNamespace,
		lockName,
		k8sClient.CoreV1(),
		k8sClient.CoordinationV1(),
		resourcelock.ResourceLockConfig{Identity: identity},
	)
	if err != nil {
		slog.Error("failed to create leader election lock", "err", err)
		os.Exit(1)
	}
	g.Go(func() error {
		slog.Info("waiting for Chronicle leader lease", "identity", identity, "namespace", lockNamespace, "name", lockName)
		leaderelection.RunOrDie(gctx, leaderelection.LeaderElectionConfig{
			Lock:            lock,
			LeaseDuration:   15 * time.Second,
			RenewDeadline:   10 * time.Second,
			RetryPeriod:     2 * time.Second,
			ReleaseOnCancel: true,
			Callbacks: leaderelection.LeaderCallbacks{
				OnStartedLeading: func(leaderCtx context.Context) {
					slog.Info("Chronicle became leader", "identity", identity)
					runLeaderWorkloads(leaderCtx)
				},
				OnStoppedLeading: func() {
					slog.Error("Chronicle lost leader lease", "identity", identity)
					cancel()
				},
				OnNewLeader: func(newLeader string) {
					slog.Info("Chronicle observed leader", "identity", newLeader)
				},
			},
		})
		return nil
	})

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

func valueOrEnv(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
}
