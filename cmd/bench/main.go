// Command bench runs benchmarks comparing direct postgres, pglink, and pgbouncer.
//
// This is a thin CLI wrapper around the pkg/e2e orchestrator which handles:
// - Building pglink from current or specified worktree
// - Starting/stopping pglink and pgbouncer processes
// - Running `go test -bench` against pkg/benchmarks
// - Collecting results in Go benchmark format for benchstat
//
// Usage:
//
//	bin/bench                     # Run default benchmarks
//	bin/bench -cases select1      # Run only SELECT 1 benchmark
//	bin/bench -duration 30s       # Run each benchmark for 30 seconds
//	bin/bench -observable         # Enable observability (traces/metrics to Grafana)
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/justjake/pglink/pkg/e2e"
)

func main() {
	// Define flags
	duration := flag.Duration("duration", 15*time.Second, "duration of each benchmark measurement")
	warmup := flag.Duration("warmup", 5*time.Second, "warmup duration before measuring")
	rounds := flag.Int("rounds", 2, "number of rounds per target")
	count := flag.Int("count", 1, "iterations per benchmark for go test -count")
	cpu := flag.Int("cpu", 100, "number of parallel workers (go test -cpu flag)")
	outputDir := flag.String("output", "", "output directory for results (default: out/benchmarks/<timestamp>)")
	cases := flag.String("cases", "", "comma-separated list of cases to run (empty = all)")
	simpleQuery := flag.Bool("simple-query", false, "use simple query protocol instead of extended")
	observable := flag.Bool("observable", true, "enable observability integration (traces/metrics)")
	checkObservable := flag.Bool("check-observable", false, "verify traces and metrics were recorded (defaults to -observable value)")
	seed := flag.Int64("seed", 0, "random seed for workload generation (0 = time-based)")
	pglinkPoolMaxConns := flag.Int("pglink-pool-max-conns", 0, "backend pool max connections for pglink (0 = use -cpu value)")

	// A/B test flags
	aLabel := flag.String("a-label", "", "label for A variant (empty = 'pglink')")
	aWorktree := flag.String("a-worktree", "", "path to worktree for A variant (empty = current)")
	aArgs := flag.String("a-args", "", "extra CLI arguments for A variant pglink")
	aEnv := flag.String("a-env", "", "extra environment variables for A variant (KEY=VAL,KEY2=VAL2)")
	aGOMAXPROCS := flag.Int("a-gomaxprocs", 0, "GOMAXPROCS for A variant (0 = default)")

	bLabel := flag.String("b-label", "", "label for B variant (empty = no B variant)")
	bWorktree := flag.String("b-worktree", "", "path to worktree for B variant")
	bArgs := flag.String("b-args", "", "extra CLI arguments for B variant pglink")
	bEnv := flag.String("b-env", "", "extra environment variables for B variant (KEY=VAL,KEY2=VAL2)")
	bGOMAXPROCS := flag.Int("b-gomaxprocs", 0, "GOMAXPROCS for B variant (0 = default)")

	// Target flags
	includeDirect := flag.Bool("direct", true, "include direct postgres benchmark")
	includePgbouncer := flag.Bool("pgbouncer", true, "include pgbouncer benchmark")
	includePglink := flag.Bool("pglink", true, "include pglink benchmark")
	includeMitmProxy := flag.Bool("mitm-proxy", false, "include mitm-proxy benchmark")

	// Debug flags
	debug := flag.Bool("debug", false, "enable debug logging for spawned pglink processes")

	// Profiling flags
	pprof := flag.Bool("pprof", false, "collect CPU and memory profiles from pglink targets")
	profileDuration := flag.Duration("profile-duration", 30*time.Second, "duration for CPU profile collection")

	// pgbench flags
	pgbenchScale := flag.Int("pgbench-scale", 10,
		"pgbench scale factor for table initialization (-s flag). "+
			"Scale 1 = 100K rows (~16MB), 10 = 1M rows (~160MB), 100 = 10M rows (~1.6GB). "+
			"Higher values increase lock contention on pgbench_branches/tellers tables.")

	flag.Parse()

	// Default -check-observable to -observable if not explicitly set
	checkObservableExplicit := false
	for _, arg := range os.Args[1:] {
		if strings.HasPrefix(arg, "-check-observable") {
			checkObservableExplicit = true
			break
		}
	}
	if !checkObservableExplicit && *observable {
		*checkObservable = true
	}

	// Set up logger
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))

	// Build config
	cfg := e2e.BenchSuiteConfig{
		Duration:           *duration,
		Warmup:             *warmup,
		Rounds:             *rounds,
		Count:              *count,
		CPU:                *cpu,
		OutputDir:          *outputDir,
		SimpleQueryMode:    *simpleQuery,
		Observable:         *observable,
		CheckObservable:    *checkObservable,
		Seed:               *seed,
		PglinkPoolMaxConns: *pglinkPoolMaxConns,
		Pprof:              *pprof,
		ProfileDuration:    *profileDuration,
	}

	// Parse cases
	if *cases != "" {
		cfg.Cases = strings.Split(*cases, ",")
	}

	// Build targets
	cfg.Targets = []e2e.TargetConfig{}

	// Add direct postgres target if requested
	if *includeDirect {
		cfg.Targets = append(cfg.Targets, e2e.TargetConfig{
			Name:       "direct",
			Type:       e2e.TargetTypeDirect,
			ConnString: "postgres://app:app_password@localhost:15432/uno?sslmode=disable",
		})
	}

	// Add pglink A variant (if enabled)
	if *includePglink {
		aTargetName := "pglink"
		if *aLabel != "" {
			aTargetName = fmt.Sprintf("pglink-%s", *aLabel)
		}
		aTarget := e2e.TargetConfig{
			Name:       aTargetName,
			Type:       e2e.TargetTypePglink,
			Port:       16432,
			GOMAXPROCS: *aGOMAXPROCS,
		}
		if *aWorktree != "" {
			aTarget.BinaryPath = "" // Will be built by orchestrator
		}
		if *aArgs != "" {
			aTarget.ExtraArgs = strings.Split(*aArgs, " ")
		}
		if *aEnv != "" {
			aTarget.ExtraEnv = strings.Split(*aEnv, ",")
		}
		if *debug {
			aTarget.ExtraArgs = append(aTarget.ExtraArgs, "-log-level", "debug")
		}
		cfg.Targets = append(cfg.Targets, aTarget)
	}

	// Add pglink B variant if specified (requires pglink to be enabled)
	if *bLabel != "" && *includePglink {
		bTarget := e2e.TargetConfig{
			Name:       fmt.Sprintf("pglink-%s", *bLabel),
			Type:       e2e.TargetTypePglink,
			Port:       16433,
			GOMAXPROCS: *bGOMAXPROCS,
		}
		if *bWorktree != "" {
			bTarget.BinaryPath = "" // Will be built by orchestrator
		}
		if *bArgs != "" {
			bTarget.ExtraArgs = strings.Split(*bArgs, " ")
		}
		if *bEnv != "" {
			bTarget.ExtraEnv = strings.Split(*bEnv, ",")
		}
		if *debug {
			bTarget.ExtraArgs = append(bTarget.ExtraArgs, "-log-level", "debug")
		}
		cfg.Targets = append(cfg.Targets, bTarget)

		// Set up A/B test config
		cfg.ABTest = &e2e.ABTestConfig{
			A: e2e.TargetVariant{
				Label:      *aLabel,
				Worktree:   *aWorktree,
				ExtraArgs:  strings.Split(*aArgs, " "),
				ExtraEnv:   strings.Split(*aEnv, ","),
				GOMAXPROCS: *aGOMAXPROCS,
			},
			B: e2e.TargetVariant{
				Label:      *bLabel,
				Worktree:   *bWorktree,
				ExtraArgs:  bTarget.ExtraArgs,
				ExtraEnv:   bTarget.ExtraEnv,
				GOMAXPROCS: *bGOMAXPROCS,
			},
		}
	}

	// Add pgbouncer target if requested
	if *includePgbouncer {
		cfg.Targets = append(cfg.Targets, e2e.TargetConfig{
			Name: "pgbouncer",
			Type: e2e.TargetTypePgbouncer,
			Port: 16433, // Use 16433 to avoid conflicts with docker's 6432
		})
	}

	// Add mitm-proxy target if requested
	if *includeMitmProxy {
		cfg.Targets = append(cfg.Targets, e2e.TargetConfig{
			Name: "mitm-proxy",
			Type: e2e.TargetTypeMitmProxy,
			Port: 16434,
		})
	}

	// Create orchestrator
	orchestrator, err := e2e.NewOrchestrator(cfg, logger)
	if err != nil {
		logger.Error("failed to create orchestrator", "error", err)
		os.Exit(1)
	}

	// Set up runners based on requested cases
	runners := buildRunners(cfg.Cases, *pgbenchScale)
	orchestrator.SetRunners(runners)

	// Set up context with signal handling
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		sig := <-sigChan
		logger.Info("received shutdown signal", "signal", sig)
		cancel()
	}()

	// Run benchmarks
	logger.Info("starting benchmark suite",
		"targets", len(cfg.Targets),
		"cases", cfg.Cases,
		"duration", cfg.Duration,
		"rounds", cfg.Rounds)

	results, err := orchestrator.Run(ctx)
	if err != nil {
		logger.Error("benchmark failed", "error", err)
		os.Exit(1)
	}

	// Print summary
	logger.Info("benchmark complete",
		"output_dir", orchestrator.OutputDir(),
		"execution_id", orchestrator.ExecutionID())

	for _, tr := range results.Results {
		logger.Info("target results",
			"target", tr.Target,
			"rounds", len(tr.Rounds),
			"metrics", len(tr.Metrics))
	}

	fmt.Printf("\nResults written to: %s\n", orchestrator.OutputDir())
	fmt.Printf("Use 'benchstat %s/bench.*.txt' to analyze results\n", orchestrator.OutputDir())
}

// buildRunners creates the appropriate benchmark runners based on the requested cases.
// If no cases are specified (empty slice), returns just the Go runner which runs all Go cases.
// If specific cases are specified, creates runners for each type that has matching cases.
//
// Note: pgbench clients, threads, and protocol are derived from standard flags:
//   - clients: from -cpu flag (via BenchRunConfig.CPU)
//   - threads: from GOMAXPROCS (automatically detected)
//   - protocol: from -simple-query flag (via BenchRunConfig.SimpleQueryMode)
func buildRunners(cases []string, pgbenchScale int) []e2e.BenchRunner {
	goRunner := e2e.NewGoTestRunner()
	pgbenchRunner := e2e.NewPgbenchRunner()

	// Configure pgbench-specific settings
	pgbenchRunner.ScaleFactor = pgbenchScale
	// Set backend connection string for pgbench init (must be direct to postgres with admin user)
	pgbenchRunner.BackendConnString = "postgres://postgres:postgres@localhost:15432/uno?sslmode=disable"

	// If no cases specified, just use Go runner (default behavior)
	if len(cases) == 0 {
		return []e2e.BenchRunner{goRunner}
	}

	// Check which runners have matching cases
	var runners []e2e.BenchRunner

	if e2e.HasCasesForRunner(cases, goRunner) {
		runners = append(runners, goRunner)
	}

	if e2e.HasCasesForRunner(cases, pgbenchRunner) {
		runners = append(runners, pgbenchRunner)
	}

	// If no cases matched any runner, return Go runner as fallback
	if len(runners) == 0 {
		return []e2e.BenchRunner{goRunner}
	}

	return runners
}
