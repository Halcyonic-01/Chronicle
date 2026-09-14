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
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/informers"
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

	// The Kafka reader is NOT created here. A reader with a group ID joins the
	// consumer group as soon as it exists, so building one on every replica
	// lets a follower be assigned the partition it will never read from —
	// stranding the whole event stream. Only the leader may hold it, so the bus
	// is built per leadership term below.
	kafkaBrokers := os.Getenv("KAFKA_BROKERS")

	var recentCache *store.RecentCache
	if address := os.Getenv("REDIS_ADDR"); address != "" {
		recentCache = store.NewRecentCache(address, os.Getenv("REDIS_PASSWORD"), 0)
		defer recentCache.Close()
	}
	collectorEvents := events
	if kafkaBrokers != "" {
		collectorEvents = make(chan event.Event, 10_000)
	}
	eventAcknowledgements := make(chan string, 10_000)

	// Initialize Graph dependencies
	inMemGraph := graph.New()
	meshBuilder := graph.NewMeshBuilder(promClient)
	graphStore := store.NewGraphStore(pool)

	// Phase 3: Snapshotter — takes a full cluster snapshot every 5 minutes.
	snapshotter := replay.NewSnapshotter(k8sClient, promClient, pool, inMemGraph)

	// Collectors, graph sync, snapshots, and the event writer are singleton
	// workloads. Only the pod holding the Kubernetes Lease runs them.
	runLeaderWorkloads := func(leaderCtx context.Context) {
		if err := func() error {
			// Created and closed with the leadership term: a demoted replica
			// must leave the consumer group rather than sit on the partition.
			var eventBus *store.KafkaBus
			if kafkaBrokers != "" {
				eventBus = store.NewKafkaBus(kafkaBrokers, valueOrEnv("KAFKA_TOPIC", "chronicle.events"), valueOrEnv("KAFKA_GROUP", "chronicle-writer"))
				defer eventBus.Close()
			}
			// Topology is read from a shared informer cache rather than re-listing every
			// pod in the cluster every 30 seconds. On a cluster of any size that LIST is
			// tens of megabytes of API traffic per sync; a watch-backed cache is
			// incremental and already local. The factory is built per leadership term
			// because informers cannot be restarted once their stop channel closes.
			graphFactory := informers.NewSharedInformerFactory(k8sClient, 10*time.Minute)
			podLister := graphFactory.Core().V1().Pods().Lister()
			serviceLister := graphFactory.Core().V1().Services().Lister()
			deploymentLister := graphFactory.Apps().V1().Deployments().Lister()
			ingressLister := graphFactory.Networking().V1().Ingresses().Lister()

			syncGraph := func(syncCtx context.Context) {
				var allEdges []graph.Edge
				graphReady := false
				podRefs, podErr := podLister.List(labels.Everything())
				serviceRefs, svcErr := serviceLister.List(labels.Everything())
				if podErr == nil && svcErr == nil && len(podRefs) > 0 {
					graphReady = true
					pods := make([]corev1.Pod, 0, len(podRefs))
					for _, p := range podRefs {
						pods = append(pods, *p)
					}
					svcs := make([]corev1.Service, 0, len(serviceRefs))
					for _, s := range serviceRefs {
						svcs = append(svcs, *s)
					}
					allEdges = append(allEdges, graph.BuildServiceEdges(svcs, pods)...)
					allEdges = append(allEdges, graph.BuildOwnerEdges(pods)...)
					allEdges = append(allEdges, graph.BuildReferenceEdges(pods)...)
					knownSvcs := make(map[string]bool)
					for _, s := range svcs {
						knownSvcs[s.Namespace+"/"+s.Name] = true
					}
					for _, p := range pods {
						allEdges = append(allEdges, graph.InferCallEdges(p, knownSvcs)...)
					}
				} else {
					slog.Warn("failed to read k8s cache for graph", "pod_err", podErr, "service_err", svcErr)
				}
				// Argo CD stamps an instance label on what it deploys; that label is the
				// only thing tying an Application event to a node in this graph.
				if deploymentRefs, err := deploymentLister.List(labels.Everything()); err == nil {
					deployments := make([]appsv1.Deployment, 0, len(deploymentRefs))
					for _, d := range deploymentRefs {
						deployments = append(deployments, *d)
					}
					allEdges = append(allEdges, graph.BuildArgoEdges(deployments)...)
				} else {
					slog.Warn("failed to read deployments for graph", "err", err)
				}
				if ingressRefs, err := ingressLister.List(labels.Everything()); err == nil {
					ingresses := make([]networkingv1.Ingress, 0, len(ingressRefs))
					for _, i := range ingressRefs {
						ingresses = append(ingresses, *i)
					}
					allEdges = append(allEdges, graph.BuildIngressEdges(ingresses)...)
				} else {
					graphReady = false
					slog.Warn("failed to read ingresses for graph", "err", err)
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

			graphFactory.Start(leaderCtx.Done())
			// WaitForCacheSync with no informers returns true immediately; the
			// factory's own method is the one that actually waits.
			for informer, synced := range graphFactory.WaitForCacheSync(leaderCtx.Done()) {
				if !synced {
					return fmt.Errorf("informer cache for %v did not sync", informer)
				}
			}
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
							for {
								if err := eventBus.Publish(leaderCtx, e); err == nil {
									break
								} else {
									slog.Warn("Kafka publish failed; retrying", "err", err)
								}
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
						if err := eventBus.Consume(leaderCtx, events, eventAcknowledgements); err != nil {
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
			writerAcks := (chan<- string)(nil)
			if eventBus != nil {
				writerAcks = eventAcknowledgements
			}
			leaderGroup.Go(func() error { return store.NewWriter(pool, recentCache).Run(leaderCtx, events, writerAcks) })
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
			// Closed edge versions are kept for the same year the snapshot
			// retention policy covers, so historical graph queries and replay
			// stay answerable over the same period.
			leaderGroup.Go(func() error {
				ticker := time.NewTicker(time.Hour)
				defer ticker.Stop()
				for {
					select {
					case <-leaderCtx.Done():
						return leaderCtx.Err()
					case <-ticker.C:
						if err := graphStore.Prune(leaderCtx, time.Now().UTC().AddDate(-1, 0, 0)); err != nil {
							slog.Error("failed to prune graph edge history", "err", err)
						}
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
		// One service hop costs two graph hops, because calls are modelled as
		// Pod -> Service -> Pod. A budget of 3 therefore reaches barely one
		// service away; 5 reaches the service behind the one that broke, which
		// is where root causes usually live. Distance already damps the score
		// (x0.33 at five hops), so the extra reach cannot dominate a ranking.
		MaxHops: 5,
	}
	healStore := heal.NewPostgresAuditStore(pool)
	healer := heal.NewEngine(healStore)
	healController := heal.NewController(healStore, heal.NewKubernetesExecutor(k8sClient))

	apiHandler := chronicleapi.NewHandler(replayer, analyzer, rcaDB, healer, graphStore, healStore, k8sClient, healController).
		WithRecentCache(recentCache)

	// The API surface is gated as one unit so a new endpoint cannot be added
	// outside the authentication check by accident.
	apiMux := http.NewServeMux()
	apiMux.HandleFunc("/api/replay", apiHandler.Replay)
	apiMux.HandleFunc("/api/events", apiHandler.Events)
	apiMux.HandleFunc("/api/posture", apiHandler.Posture)
	apiMux.HandleFunc("/api/analyze", apiHandler.Analyze)
	apiMux.HandleFunc("/api/graph", apiHandler.Graph)
	apiMux.HandleFunc("/api/heal/actions", apiHandler.HealingActions)
	apiMux.HandleFunc("/api/heal/actions/", apiHandler.DecideHealingAction)

	mux := http.NewServeMux()
	mux.Handle("/", chronicleweb.Handler())
	mux.Handle("/api/", chronicleapi.RequireAPIToken(apiMux))
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
		config := leaderelection.LeaderElectionConfig{
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
					// Losing the lease must not take the pod down with it: this
					// replica still serves the API and console, and a transient
					// renewal failure should only cost it the singleton
					// workloads until it wins the lease back.
					slog.Warn("Chronicle lost leader lease", "identity", identity)
				},
				OnNewLeader: func(newLeader string) {
					slog.Info("Chronicle observed leader", "identity", newLeader)
				},
			},
		}
		for {
			leaderelection.RunOrDie(gctx, config)
			if gctx.Err() != nil {
				return gctx.Err()
			}
			slog.Info("re-entering Chronicle leader election", "identity", identity)
			select {
			case <-gctx.Done():
				return gctx.Err()
			case <-time.After(2 * time.Second):
			}
		}
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
