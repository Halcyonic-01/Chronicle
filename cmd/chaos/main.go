// Command chaos injects real faults into the local kind cluster, one at a time,
// and scores Chronicle's analysis of each incident they cause.
//
//	chaos list                      the experiment catalog
//	chaos plan   [flags]            the order a run would use, touching nothing
//	chaos run    [flags]            inject, observe, score, restore
//	chaos report FILE               summarise a run's log
//
// See internal/chaos/README.md.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"text/tabwriter"
	"time"

	promapi "github.com/prometheus/client_golang/api"
	v1 "github.com/prometheus/client_golang/api/prometheus/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"

	"github.com/Halcyonic-01/Chronicle/internal/chaos"
	"github.com/Halcyonic-01/Chronicle/internal/rca/benchdata/eval"
)

func main() {
	if len(os.Args) < 2 {
		usage()
	}
	var err error
	switch os.Args[1] {
	case "list":
		list()
	case "plan":
		err = plan(os.Args[2:])
	case "run":
		err = run(os.Args[2:])
	case "report":
		err = report(os.Args[2:])
	default:
		usage()
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "chaos:", err)
		os.Exit(1)
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, "usage: chaos list | plan [flags] | run [flags] | report FILE   (chaos run -h for flags)")
	os.Exit(2)
}

func list() {
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "EXPERIMENT\tCATEGORY\tHYPOTHESIS")
	for _, e := range chaos.Catalog() {
		fmt.Fprintf(w, "%s\t%s\t%s\n", e.Name, e.Category, e.Hypothesis)
	}
	w.Flush()
}

type options struct {
	only, api, prom, namespace, kubeContext, allowContext, out string
	trials, extra                                              int
	seed                                                       int64
	skipMetrics                                                bool
	cfg                                                        chaos.Config
}

func parse(name string, args []string) (*options, []chaos.Planned, error) {
	o := &options{cfg: chaos.DefaultConfig()}
	fs := flag.NewFlagSet(name, flag.ExitOnError)
	fs.StringVar(&o.only, "only", "", "comma-separated experiment names (default: all)")
	fs.IntVar(&o.trials, "trials", 1, "runs of each experiment")
	fs.Int64Var(&o.seed, "seed", time.Now().UnixNano(), "seed for the random order; reuse it to repeat a run")
	fs.StringVar(&o.api, "api", "http://localhost:8181", "Chronicle API")
	fs.StringVar(&o.prom, "prom", "http://localhost:9090", "Prometheus, for the steady-state check")
	fs.BoolVar(&o.skipMetrics, "skip-metrics", false, "judge health by rollouts alone (blind to errors and latency)")
	fs.StringVar(&o.namespace, "namespace", "default", "the victim application's namespace")
	fs.StringVar(&o.kubeContext, "context", "", "kube context (default: the current one; only kind-* is allowed)")
	fs.StringVar(&o.allowContext, "allow-context", "", "allow this one non-kind context, deliberately")
	fs.StringVar(&o.out, "out", "", "JSONL log (default: chaos-results/run-<time>.jsonl)")
	fs.DurationVar(&o.cfg.DetectTimeout, "detect", o.cfg.DetectTimeout, "how long Chronicle has to raise a signal")
	fs.DurationVar(&o.cfg.Settle, "settle", o.cfg.Settle, "wait after the symptom before asking for the analysis")
	fs.DurationVar(&o.cfg.SteadyTimeout, "steady", o.cfg.SteadyTimeout, "longest wait for the steady state")
	fs.DurationVar(&o.cfg.Cooldown, "cooldown", o.cfg.Cooldown, "how long the steady state must hold before each fault (more than Chronicle's 60s flap window)")
	fs.IntVar(&o.cfg.ExtraSymptoms, "extra", o.cfg.ExtraSymptoms, "other symptoms analysed per incident")
	_ = fs.Parse(args)

	var names []string
	if o.only != "" {
		names = strings.Split(o.only, ",")
	}
	exps, err := chaos.Select(chaos.Catalog(), names)
	if err != nil {
		return nil, nil, err
	}
	return o, chaos.Plan(exps, o.trials, o.seed), nil
}

func plan(args []string) error {
	o, planned, err := parse("plan", args)
	if err != nil {
		return err
	}
	fmt.Printf("seed %d, %d experiment runs (nothing is touched)\n", o.seed, len(planned))
	for i, p := range planned {
		fmt.Printf("%2d. %s (trial %d) [%s]\n    %s\n", i+1, p.Experiment.Name, p.Trial, p.Experiment.Category, p.Experiment.Hypothesis)
		for _, a := range p.Experiment.Setup {
			fmt.Printf("      setup:   %s\n", a.Describe())
		}
		for _, a := range p.Experiment.Inject {
			fmt.Printf("      inject:  %s\n", a.Describe())
		}
	}
	return nil
}

func run(args []string) error {
	o, planned, err := parse("run", args)
	if err != nil {
		return err
	}
	rules := clientcmd.NewDefaultClientConfigLoadingRules()
	kube := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(rules, &clientcmd.ConfigOverrides{CurrentContext: o.kubeContext})
	raw, err := kube.RawConfig()
	if err != nil {
		return err
	}
	if o.kubeContext == "" {
		o.kubeContext = raw.CurrentContext
	}
	if err := chaos.CheckContext(o.kubeContext, o.allowContext); err != nil {
		return err
	}
	restConfig, err := kube.ClientConfig()
	if err != nil {
		return err
	}
	client, err := kubernetes.NewForConfig(restConfig)
	if err != nil {
		return err
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	cluster := chaos.NewCluster(client, o.namespace, o.kubeContext)
	api := chaos.NewChronicle(o.api, os.Getenv("CHRONICLE_API_TOKEN"))
	if err := api.Reachable(ctx); err != nil {
		return fmt.Errorf("Chronicle API at %s is not reachable (port-forward it first): %w", o.api, err)
	}
	steady := &chaos.Steady{Cluster: cluster}
	if !o.skipMetrics {
		pc, err := promapi.NewClient(promapi.Config{Address: o.prom})
		if err != nil {
			return err
		}
		steady.Prom = v1.NewAPI(pc)
		if _, _, err := steady.Prom.Query(ctx, "up", time.Now()); err != nil {
			return fmt.Errorf("Prometheus at %s is not reachable; without it health is judged by rollouts alone (pass -skip-metrics to accept that): %w", o.prom, err)
		}
	}

	if o.out == "" {
		o.out = filepath.Join("chaos-results", "run-"+time.Now().UTC().Format("20060102T150405Z")+".jsonl")
	}
	if err := os.MkdirAll(filepath.Dir(o.out), 0o755); err != nil {
		return err
	}
	file, err := os.OpenFile(o.out, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	defer file.Close()

	info := chaos.RunInfo{Seed: o.seed, Context: o.kubeContext, Namespace: o.namespace, API: o.api, Commit: commit(),
		Config: map[string]string{"detect": o.cfg.DetectTimeout.String(), "settle": o.cfg.Settle.String(), "steady": o.cfg.SteadyTimeout.String(),
			"cooldown": o.cfg.Cooldown.String(), "extra_symptoms": fmt.Sprint(o.cfg.ExtraSymptoms)}}
	if !o.skipMetrics {
		info.Prometheus = o.prom
	}
	if hash, err := eval.SourceHash("."); err == nil {
		info.RCASourceHash = hash
	}
	for _, p := range planned {
		info.Order = append(info.Order, fmt.Sprintf("%s#%d", p.Experiment.Name, p.Trial))
	}
	fmt.Printf("chaos run: %d experiment runs against %s/%s, seed %d, log %s\n", len(planned), o.kubeContext, o.namespace, o.seed, o.out)
	fmt.Println("the deployed Chronicle is assumed to be built from this tree (make deploy-chronicle); Ctrl-C restores and stops")
	runner := &chaos.Runner{Cluster: cluster, Chronicle: api, Steady: steady, Config: o.cfg, Out: file, Log: os.Stdout}
	runErr := runner.Run(ctx, planned, info)
	if err := file.Sync(); err != nil {
		return err
	}
	if summary, err := os.Open(o.out); err == nil {
		records, _ := chaos.ReadRecords(summary)
		summary.Close()
		fmt.Println()
		fmt.Print(chaos.Report(records))
	}
	return runErr
}

func report(args []string) error {
	if len(args) != 1 {
		return fmt.Errorf("usage: chaos report FILE")
	}
	f, err := os.Open(args[0])
	if err != nil {
		return err
	}
	defer f.Close()
	records, err := chaos.ReadRecords(f)
	if err != nil {
		return err
	}
	fmt.Print(chaos.Report(records))
	return nil
}

// commit names the checked-out tree, marked when it has uncommitted changes.
func commit() string {
	out, err := exec.Command("git", "rev-parse", "--short", "HEAD").Output()
	if err != nil {
		return ""
	}
	id := strings.TrimSpace(string(out))
	if dirty, err := exec.Command("git", "status", "--porcelain").Output(); err == nil && len(strings.TrimSpace(string(dirty))) > 0 {
		id += "+dirty"
	}
	return id
}
