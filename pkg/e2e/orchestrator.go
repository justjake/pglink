package e2e

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/prometheus/common/expfmt"

	"github.com/justjake/pglink/pkg/config"
	"github.com/justjake/pglink/pkg/config/pgbouncer"
)

// Orchestrator manages the benchmark lifecycle including:
// - Starting/stopping pglink, pgbouncer, and docker containers
// - Managing output directories and git info capture
// - Running benchmarks via BenchRunner
type Orchestrator struct {
	// Config is the benchmark suite configuration.
	Config BenchSuiteConfig

	// Runner is the benchmark execution backend.
	Runner BenchRunner

	// Logger for orchestrator messages.
	Logger *slog.Logger

	// Internal state
	mainWorktreePath string
	currentWorktree  string
	portOffset       int
	executionID      string
	outputDir        string
	runnerGitInfo    *RunnerGitInfo
	processes        map[string]*exec.Cmd // Running processes by target name
	metricsPorts     map[string]int       // Metrics ports by target name (for observable mode)
}

// NewOrchestrator creates a new Orchestrator with the given configuration.
func NewOrchestrator(cfg BenchSuiteConfig, logger *slog.Logger) (*Orchestrator, error) {
	// Determine worktree paths
	currentWorktree, err := CurrentWorktreePath()
	if err != nil {
		return nil, fmt.Errorf("failed to determine current worktree: %w", err)
	}

	mainWorktree, err := MainWorktreePath(currentWorktree)
	if err != nil {
		return nil, fmt.Errorf("failed to determine main worktree: %w", err)
	}

	portOffset := WorktreePortOffset(currentWorktree)

	return &Orchestrator{
		Config:           cfg,
		Runner:           DefaultBenchRunner(),
		Logger:           logger,
		mainWorktreePath: mainWorktree,
		currentWorktree:  currentWorktree,
		portOffset:       portOffset,
		processes:        make(map[string]*exec.Cmd),
		metricsPorts:     make(map[string]int),
	}, nil
}

// Run executes the benchmark suite.
func (o *Orchestrator) Run(ctx context.Context) (*BenchmarkResults, error) {
	// Generate execution ID
	o.executionID = generateExecutionID()
	o.Logger.Info("starting benchmark run",
		"execution_id", o.executionID,
		"worktree", WorktreeName(o.currentWorktree),
		"port_offset", o.portOffset,
	)

	// Initialize output directory
	if err := o.initOutputDir(); err != nil {
		return nil, fmt.Errorf("failed to init output dir: %w", err)
	}

	// Update latest symlink immediately so users can monitor progress
	if err := o.updateLatestSymlink(); err != nil {
		o.Logger.Warn("failed to update latest symlink", "error", err)
	}

	// Capture runner git info
	if err := o.captureRunnerGitInfo(); err != nil {
		return nil, fmt.Errorf("failed to capture git info: %w", err)
	}

	// Build results structure
	results := &BenchmarkResults{
		ExecutionID: o.executionID,
		Timestamp:   time.Now(),
		Runner:      *o.runnerGitInfo,
		Config:      o.Config,
		Results:     make([]TargetResult, 0, len(o.Config.Targets)),
	}

	// Ensure docker containers are running (from main worktree)
	if err := o.ensureDockerContainers(ctx); err != nil {
		return nil, fmt.Errorf("failed to start docker containers: %w", err)
	}

	// Estimate total time
	numTargets := len(o.Config.Targets)
	numRounds := o.Config.Rounds
	estTimePerRound := o.Config.Duration + o.Config.Warmup + 30*time.Second // Add buffer for setup
	estTotalTime := time.Duration(numTargets*numRounds) * estTimePerRound
	o.Logger.Info("estimated total time",
		"targets", numTargets,
		"rounds", numRounds,
		"est_per_round", estTimePerRound,
		"est_total", estTotalTime.Round(time.Second))

	// Run benchmarks for each target
	for i, target := range o.Config.Targets {
		o.Logger.Info("starting target",
			"target", target.Name,
			"progress", fmt.Sprintf("%d/%d", i+1, numTargets))

		targetResult, err := o.runTarget(ctx, target)
		if err != nil {
			o.Logger.Error("target benchmark failed", "target", target.Name, "error", err)
			// Continue with other targets
		}
		if targetResult != nil {
			results.Results = append(results.Results, *targetResult)
		}
	}

	// Check observability if enabled
	if o.Config.CheckObservable {
		o.Logger.Info("checking observability data...")
		checkResult, err := o.checkObservability(ctx, results)
		if err != nil {
			o.Logger.Error("observability check failed", "error", err)
		}
		results.ObservabilityCheck = checkResult
	}

	// Write results.json
	if err := o.writeResults(results); err != nil {
		o.Logger.Error("failed to write results", "error", err)
	}

	// Generate BENCHMARK.md report
	if err := o.generateBenchmarkReport(results); err != nil {
		o.Logger.Warn("failed to generate benchmark report", "error", err)
	}

	return results, nil
}

// runTarget runs benchmarks for a single target.
func (o *Orchestrator) runTarget(ctx context.Context, target TargetConfig) (*TargetResult, error) {
	o.Logger.Info("running benchmarks for target", "target", target.Name, "type", target.Type)

	// Apply port offset
	if target.Port != 0 {
		target.Port = ApplyPortOffset(target.Port, o.portOffset)
	}
	if target.MetricsPort != 0 {
		target.MetricsPort = ApplyPortOffset(target.MetricsPort, o.portOffset)
	}

	// Generate connection string if not provided
	if target.ConnString == "" {
		switch target.Type {
		case TargetTypePglink, TargetTypePgbouncer:
			// Connect through the proxy to alpha_uno database
			target.ConnString = fmt.Sprintf("postgres://app:app_password@localhost:%d/alpha_uno?sslmode=disable", target.Port)
		}
	}

	// Start target process (if not direct connection)
	needsStop := false
	if target.Type != TargetTypeDirect {
		if err := o.startTargetProcess(ctx, &target); err != nil {
			return nil, fmt.Errorf("failed to start %s: %w", target.Name, err)
		}
		needsStop = true
	}

	// Build result structure
	result := &TargetResult{
		Target:     target.Name,
		Git:        target.Git,
		BinaryPath: target.BinaryPath,
		Rounds:     make([]RoundResult, 0, o.Config.Rounds),
	}

	// Create bench.txt for this target
	benchFile, err := os.Create(filepath.Join(o.outputDir, fmt.Sprintf("bench.%s.txt", target.Name)))
	if err != nil {
		return nil, fmt.Errorf("failed to create bench file: %w", err)
	}
	defer func() {
		if err := benchFile.Close(); err != nil {
			o.Logger.Warn("failed to close bench file", "error", err)
		}
	}()

	// Run rounds
	for round := 1; round <= o.Config.Rounds; round++ {
		o.Logger.Info("running round", "target", target.Name, "round", round, "total", o.Config.Rounds)

		runCfg := BenchRunConfig{
			Duration:        o.Config.Duration,
			Warmup:          o.Config.Warmup,
			CPU:             o.Config.CPU,
			SimpleQueryMode: o.Config.SimpleQueryMode,
			Seed:            o.Config.Seed,
			Cases:           o.Config.Cases,
			Target:          target,
			ConnString:      target.ConnString,
			RunID:           o.executionID,
			Round:           round,
			TotalRounds:     o.Config.Rounds,
			Timestamp:       time.Now(),
			Count:           o.Config.Count,
			Timeout:         o.Config.Duration + 5*time.Minute, // Add buffer for warmup and teardown
			OutputDir:       o.outputDir,
		}

		runResult, err := o.Runner.Run(ctx, runCfg)
		if err != nil {
			o.Logger.Error("benchmark run failed", "target", target.Name, "round", round, "error", err)
		}

		roundResult := RoundResult{
			Round: round,
		}

		if runResult != nil {
			roundResult.Duration = runResult.Duration
			roundResult.Output = string(runResult.Output)

			// Append to bench file
			if _, err := benchFile.Write(runResult.Output); err != nil {
				o.Logger.Warn("failed to write benchmark output", "error", err)
			}
			if _, err := benchFile.WriteString("\n"); err != nil {
				o.Logger.Warn("failed to write newline", "error", err)
			}

			// Collect metrics
			result.Metrics = append(result.Metrics, runResult.Metrics...)
		}

		result.Rounds = append(result.Rounds, roundResult)
	}

	// Scrape metrics before stopping the target (if observable mode)
	// Note: pglink uses prometheus client library for metrics, not OTEL metrics.
	// Push metrics won't work until we migrate to OTEL. For now, scrape directly.
	if needsStop && o.Config.Observable {
		if scraped, err := o.scrapeMetricsEndpoint(ctx); err != nil {
			o.Logger.Warn("failed to scrape metrics", "target", target.Name, "error", err)
		} else {
			result.ScrapedMetrics = scraped
			o.Logger.Info("scraped metrics from target",
				"target", target.Name,
				"families", len(scraped.MetricNames),
				"samples", scraped.SampleCount)
		}
	}

	// Stop target process
	if needsStop {
		o.stopTargetProcess(target.Name)
	}

	return result, nil
}

// startTargetProcess starts a pglink or pgbouncer process.
func (o *Orchestrator) startTargetProcess(ctx context.Context, target *TargetConfig) error {
	switch target.Type {
	case TargetTypePglink:
		return o.startPglink(ctx, target)
	case TargetTypePgbouncer:
		return o.startPgbouncer(ctx, target)
	default:
		return fmt.Errorf("unknown target type: %s", target.Type)
	}
}

// startPglink starts a pglink process.
func (o *Orchestrator) startPglink(ctx context.Context, target *TargetConfig) error {
	binaryPath := target.BinaryPath
	if binaryPath == "" {
		binaryPath = filepath.Join(o.currentWorktree, "out", "pglink")
	}

	// Ensure binary exists
	if _, err := os.Stat(binaryPath); os.IsNotExist(err) {
		return fmt.Errorf("pglink binary not found at %s", binaryPath)
	}

	// Generate benchmark config and write as pglink JSON
	cfg, err := o.benchmarkConfig()
	if err != nil {
		return fmt.Errorf("failed to create benchmark config: %w", err)
	}

	configPath, err := o.writePglinkConfig(cfg, target)
	if err != nil {
		return fmt.Errorf("failed to write pglink config: %w", err)
	}

	o.Logger.Info("generated pglink config", "path", configPath, "pool_max_conns", cfg.Databases["alpha_uno"].Backend.PoolMaxConns)

	// Build args
	args := []string{
		"-config", configPath,
	}

	if target.Port != 0 {
		args = append(args, "-listen-addr", fmt.Sprintf(":%d", target.Port))
	}

	if target.MessageBufferBytes != "" {
		args = append(args, "-message-buffer-bytes", target.MessageBufferBytes)
	}

	// Add observability flags if enabled
	if o.Config.Observable {
		// Enable OTEL with minimal mode (no SQL parsing)
		args = append(args, "-otel", "minimal")
		// Send traces to Tempo via OTLP gRPC (port 14317 mapped to container's 4317)
		args = append(args, "-otel-endpoint", "localhost:14317")
		// Add execution_id and target as OTEL attributes for filtering
		args = append(args, "-otel-attrs", fmt.Sprintf("bench.execution_id=%s,bench.target=%s", o.executionID, target.Name))

		// Push metrics to Prometheus via OTLP (port 19090)
		args = append(args, "-prometheus-push", "localhost:19090")
		args = append(args, "-prometheus-attrs", fmt.Sprintf("bench_execution_id=%s,bench_target=%s", o.executionID, target.Name))

		// Push logs to Loki via OTLP (port 13100)
		args = append(args, "-otel-logs")
		args = append(args, "-otel-logs-endpoint", "localhost:13100")

		// Also enable metrics scraping endpoint for backup verification
		metricsPort := target.Port + 3000 // e.g., 16432 -> 19432
		if target.MetricsPort != 0 {
			metricsPort = target.MetricsPort
		}
		args = append(args, "-prometheus-listen", fmt.Sprintf(":%d", metricsPort))
		// Record metrics port for backup scraping
		o.metricsPorts[target.Name] = metricsPort
	}

	args = append(args, target.ExtraArgs...)

	// Create log file - use "default" suffix for the default "pglink" target
	logSuffix := target.Name
	if target.Name == "pglink" {
		logSuffix = "default"
	}
	logFile, err := os.Create(filepath.Join(o.outputDir, fmt.Sprintf("pglink.%s.log", logSuffix)))
	if err != nil {
		return fmt.Errorf("failed to create log file: %w", err)
	}

	cmd := exec.CommandContext(ctx, binaryPath, args...)
	cmd.Stdout = logFile
	cmd.Stderr = logFile

	// Set up environment
	env := os.Environ()
	if target.GOMAXPROCS > 0 {
		env = append(env, fmt.Sprintf("GOMAXPROCS=%d", target.GOMAXPROCS))
	}
	// For observability, enable insecure mode for local OTLP connection
	if o.Config.Observable {
		env = append(env, "OTEL_EXPORTER_OTLP_INSECURE=true")
	}
	env = append(env, target.ExtraEnv...)
	cmd.Env = env

	if err := cmd.Start(); err != nil {
		_ = logFile.Close()
		return fmt.Errorf("failed to start pglink: %w", err)
	}

	o.processes[target.Name] = cmd
	o.Logger.Info("started pglink", "target", target.Name, "pid", cmd.Process.Pid, "port", target.Port)

	// Wait for pglink to be ready (simple sleep for now)
	time.Sleep(2 * time.Second)

	return nil
}

// benchmarkConfig creates a benchmark-optimized config from pglink.json.
// This is the single source of truth for both pglink and pgbouncer benchmark configs.
func (o *Orchestrator) benchmarkConfig() (*config.Config, error) {
	// Read the original config from the current worktree (not main)
	// so that worktree-specific config changes are used
	srcPath := filepath.Join(o.currentWorktree, "pglink.json")
	cfg, err := config.ReadConfigFile(srcPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read config: %w", err)
	}

	// Determine pool size: use explicit setting, or fall back to CPU (parallelism)
	poolMaxConns := o.Config.PglinkPoolMaxConns
	if poolMaxConns == 0 {
		poolMaxConns = o.Config.CPU
	}

	// Set pool_max_conns for each database to handle high parallelism
	for _, dbCfg := range cfg.Databases {
		dbCfg.Backend.PoolMaxConns = int32(poolMaxConns)
		dbCfg.Backend.PoolMinIdleConns = nil
		// Set pool acquire timeout to match pgbouncer's default query_wait_timeout (120s)
		// pglink's default is only 1s which causes spurious failures under high contention
		longTimeout := 120000 // 120 seconds, same as pgbouncer default
		dbCfg.PoolAcquireTimeoutMilliseconds = &longTimeout
	}

	// Disable TLS for benchmarks (connecting via localhost)
	cfg.TLS = &config.JsonTLSConfig{
		SSLMode: config.SSLModeDisable,
	}

	return cfg, nil
}

// writePglinkConfig writes the config as pglink JSON format.
func (o *Orchestrator) writePglinkConfig(cfg *config.Config, target *TargetConfig) (string, error) {
	data, err := json.MarshalIndent(cfg, "", "  ")
	if err != nil {
		return "", fmt.Errorf("failed to marshal config: %w", err)
	}

	configPath := filepath.Join(o.outputDir, fmt.Sprintf("pglink.%s.json", target.Name))
	if err := os.WriteFile(configPath, data, 0644); err != nil {
		return "", fmt.Errorf("failed to write config: %w", err)
	}

	return configPath, nil
}

// writePgbouncerConfig writes the config as pgbouncer INI format.
func (o *Orchestrator) writePgbouncerConfig(ctx context.Context, cfg *config.Config, target *TargetConfig) (string, error) {
	// Create secrets cache (nil client is fine since our test config uses insecure: values)
	secrets := config.NewSecretCache(nil)

	// Generate pgbouncer config using transaction mode (same as pglink)
	opts := pgbouncer.Options{
		PoolMode: pgbouncer.PoolModeTransaction,
	}
	pgbCfg, err := pgbouncer.GenerateConfigWithOptions(ctx, cfg, secrets, target.Port, opts)
	if err != nil {
		return "", fmt.Errorf("failed to generate pgbouncer config: %w", err)
	}

	// Write config to output directory
	configDir := filepath.Join(o.outputDir, fmt.Sprintf("pgbouncer.%s", target.Name))
	if err := pgbCfg.WriteToDir(configDir); err != nil {
		return "", fmt.Errorf("failed to write pgbouncer config: %w", err)
	}

	return configDir, nil
}

// startPgbouncer starts a pgbouncer process.
func (o *Orchestrator) startPgbouncer(ctx context.Context, target *TargetConfig) error {
	// Generate benchmark config (same as pglink) and write as pgbouncer INI
	cfg, err := o.benchmarkConfig()
	if err != nil {
		return fmt.Errorf("failed to create benchmark config: %w", err)
	}

	configDir, err := o.writePgbouncerConfig(ctx, cfg, target)
	if err != nil {
		return fmt.Errorf("failed to write pgbouncer config: %w", err)
	}

	o.Logger.Info("generated pgbouncer config", "path", configDir, "pool_max_conns", cfg.Databases["alpha_uno"].Backend.PoolMaxConns)

	// Create log file
	logFile, err := os.Create(filepath.Join(o.outputDir, fmt.Sprintf("pgbouncer.%s.log", target.Name)))
	if err != nil {
		return fmt.Errorf("failed to create log file: %w", err)
	}

	// Start pgbouncer
	configPath := filepath.Join(configDir, "pgbouncer.ini")
	cmd := exec.CommandContext(ctx, "pgbouncer", configPath)
	cmd.Dir = configDir // Run from config dir so relative paths work
	cmd.Stdout = logFile
	cmd.Stderr = logFile

	if err := cmd.Start(); err != nil {
		_ = logFile.Close()
		return fmt.Errorf("failed to start pgbouncer: %w", err)
	}

	o.processes[target.Name] = cmd
	o.Logger.Info("started pgbouncer", "target", target.Name, "pid", cmd.Process.Pid, "port", target.Port)

	// Wait for pgbouncer to be ready
	time.Sleep(2 * time.Second)

	return nil
}

// stopTargetProcess stops a running target process gracefully.
// It sends SIGTERM first to allow trace flushing, then SIGKILL if needed.
func (o *Orchestrator) stopTargetProcess(name string) {
	cmd, ok := o.processes[name]
	if !ok {
		return
	}

	o.Logger.Info("stopping process", "target", name)

	if cmd.Process == nil {
		delete(o.processes, name)
		return
	}

	// Send SIGTERM for graceful shutdown (allows trace flushing)
	if err := cmd.Process.Signal(syscall.SIGTERM); err != nil {
		o.Logger.Warn("failed to send SIGTERM", "target", name, "error", err)
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
		delete(o.processes, name)
		return
	}

	// Wait for graceful shutdown with timeout
	done := make(chan error, 1)
	go func() {
		done <- cmd.Wait()
	}()

	select {
	case <-done:
		o.Logger.Info("process stopped gracefully", "target", name)
	case <-time.After(5 * time.Second):
		o.Logger.Warn("process did not stop gracefully, sending SIGKILL", "target", name)
		_ = cmd.Process.Kill()
		<-done // Wait for kill to complete
	}

	delete(o.processes, name)
}

// ensureDockerContainers ensures required docker containers are running.
func (o *Orchestrator) ensureDockerContainers(ctx context.Context) error {
	// Build docker compose command
	args := []string{"compose", "up", "-d"}

	// Include observability profile if enabled
	if o.Config.Observable {
		args = []string{"compose", "--profile", "observability", "up", "-d", "--wait"}
	}

	cmd := exec.CommandContext(ctx, "docker", args...)
	cmd.Dir = o.mainWorktreePath

	output, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("docker compose up failed: %w\nOutput: %s", err, string(output))
	}

	o.Logger.Info("docker containers running",
		"observable", o.Config.Observable,
		"output", string(output))
	return nil
}

// initOutputDir creates the output directory for this benchmark run.
func (o *Orchestrator) initOutputDir() error {
	timestamp := time.Now().Format("2006-01-02T15-04-05")
	dirName := fmt.Sprintf("%s-%s", timestamp, o.executionID[:8])

	baseDir := o.Config.OutputDir
	if baseDir == "" {
		baseDir = filepath.Join(o.currentWorktree, "out", "benchmarks")
	}

	o.outputDir = filepath.Join(baseDir, dirName)

	if err := os.MkdirAll(o.outputDir, 0755); err != nil {
		return fmt.Errorf("failed to create output dir: %w", err)
	}

	o.Logger.Info("created output directory", "path", o.outputDir)
	return nil
}

// captureRunnerGitInfo captures git metadata for the benchmark runner.
func (o *Orchestrator) captureRunnerGitInfo() error {
	git, err := GetGitMetadata(o.currentWorktree)
	if err != nil {
		return err
	}

	diff, _ := GetGitDiff(o.currentWorktree)
	status, _ := GetGitStatus(o.currentWorktree)

	o.runnerGitInfo = &RunnerGitInfo{
		Git:       git,
		GitDiff:   diff,
		GitStatus: status,
	}

	// Write git info to files (best effort - don't fail on these)
	_ = writeFile(filepath.Join(o.outputDir, "git-sha"), git.SHA)
	_ = writeFile(filepath.Join(o.outputDir, "git-branch"), git.Branch)
	_ = writeFile(filepath.Join(o.outputDir, "git-diff"), diff)
	_ = writeFile(filepath.Join(o.outputDir, "git-status"), status)

	return nil
}

// writeResults writes the results.json file.
func (o *Orchestrator) writeResults(results *BenchmarkResults) error {
	data, err := json.MarshalIndent(results, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(o.outputDir, "results.json"), data, 0644)
}

// updateLatestSymlink updates the "latest" symlink to point to this run.
func (o *Orchestrator) updateLatestSymlink() error {
	latestPath := filepath.Join(filepath.Dir(o.outputDir), "latest")

	// Remove existing symlink (ignore error if doesn't exist)
	_ = os.Remove(latestPath)

	// Create new symlink (relative path)
	return os.Symlink(filepath.Base(o.outputDir), latestPath)
}

// OutputDir returns the path to the output directory for this run.
func (o *Orchestrator) OutputDir() string {
	return o.outputDir
}

// ExecutionID returns the unique execution ID for this run.
func (o *Orchestrator) ExecutionID() string {
	return o.executionID
}

// generateExecutionID generates a unique execution ID.
func generateExecutionID() string {
	b := make([]byte, 8)
	_, _ = rand.Read(b) // crypto/rand.Read always succeeds on modern systems
	return hex.EncodeToString(b)
}

// writeFile is a helper to write a string to a file.
func writeFile(path, content string) error {
	return os.WriteFile(path, []byte(strings.TrimSpace(content)+"\n"), 0644)
}

// CopyFile copies a file from src to dst.
func CopyFile(src, dst string) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer func() { _ = in.Close() }()

	out, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer func() { _ = out.Close() }()

	_, err = io.Copy(out, in)
	return err
}

// generateBenchmarkReport creates a BENCHMARK.md file summarizing the results.
func (o *Orchestrator) generateBenchmarkReport(results *BenchmarkResults) error {
	var b strings.Builder

	// Write header
	b.WriteString("# Benchmark Results\n\n")
	fmt.Fprintf(&b, "**Execution ID:** `%s`\n\n", o.executionID)
	fmt.Fprintf(&b, "**Timestamp:** %s\n\n", results.Timestamp.Format(time.RFC3339))
	fmt.Fprintf(&b, "**Git SHA:** `%s`\n\n", o.runnerGitInfo.Git.SHA[:12])
	fmt.Fprintf(&b, "**Branch:** `%s`\n\n", o.runnerGitInfo.Git.Branch)

	// Write configuration summary
	b.WriteString("## Configuration\n\n")
	b.WriteString("| Setting | Value |\n")
	b.WriteString("|---------|-------|\n")
	fmt.Fprintf(&b, "| Duration | %s |\n", o.Config.Duration)
	fmt.Fprintf(&b, "| Warmup | %s |\n", o.Config.Warmup)
	fmt.Fprintf(&b, "| Rounds | %d |\n", o.Config.Rounds)
	fmt.Fprintf(&b, "| CPU (parallelism) | %d |\n", o.Config.CPU)
	fmt.Fprintf(&b, "| Targets | %d |\n", len(o.Config.Targets))
	b.WriteString("\n")

	// List targets
	b.WriteString("### Targets\n\n")
	for _, t := range o.Config.Targets {
		fmt.Fprintf(&b, "- **%s** (%s)\n", t.Name, t.Type)
	}
	b.WriteString("\n")

	// Run benchstat comparison if we have multiple targets
	if len(results.Results) >= 2 {
		b.WriteString("## Benchstat Comparison\n\n")
		benchstatOutput := o.runBenchstat()
		if benchstatOutput != "" {
			fmt.Fprintf(&b, "```\n%s```\n\n", benchstatOutput)
		} else {
			b.WriteString("_benchstat not available or failed to run_\n\n")
		}
	}

	// Write individual target results
	b.WriteString("## Results by Target\n\n")
	for _, tr := range results.Results {
		fmt.Fprintf(&b, "### %s\n\n", tr.Target)
		if tr.Git != nil {
			fmt.Fprintf(&b, "- Git SHA: `%s`\n", tr.Git.SHA[:12])
			fmt.Fprintf(&b, "- Branch: `%s`\n", tr.Git.Branch)
		}
		fmt.Fprintf(&b, "- Rounds completed: %d\n", len(tr.Rounds))
		fmt.Fprintf(&b, "- Metrics collected: %d\n", len(tr.Metrics))
		b.WriteString("\n")

		// Show key metrics if available
		if len(tr.Metrics) > 0 {
			b.WriteString("| Benchmark | Iterations | ns/op | MB/s |\n")
			b.WriteString("|-----------|------------|-------|------|\n")
			for _, m := range tr.Metrics {
				mbps := ""
				if m.MBPerSec > 0 {
					mbps = fmt.Sprintf("%.2f", m.MBPerSec)
				}
				fmt.Fprintf(&b, "| %s | %d | %.0f | %s |\n", m.Name, m.Iterations, m.NsPerOp, mbps)
			}
			b.WriteString("\n")
		}
	}

	// Write file listing
	b.WriteString("## Output Files\n\n")
	b.WriteString("| File | Description |\n")
	b.WriteString("|------|-------------|\n")

	files, _ := os.ReadDir(o.outputDir)
	for _, file := range files {
		desc := describeOutputFile(file.Name())
		fmt.Fprintf(&b, "| `%s` | %s |\n", file.Name(), desc)
	}

	// Write to file
	reportPath := filepath.Join(o.outputDir, "BENCHMARK.md")
	if err := os.WriteFile(reportPath, []byte(b.String()), 0644); err != nil {
		return fmt.Errorf("failed to write report file: %w", err)
	}

	o.Logger.Info("generated benchmark report", "path", reportPath)
	return nil
}

// runBenchstat runs benchstat to compare benchmark results.
// It uses bin/tool to ensure mise environment is activated.
func (o *Orchestrator) runBenchstat() string {
	// Find all bench.*.txt files
	pattern := filepath.Join(o.outputDir, "bench.*.txt")
	files, err := filepath.Glob(pattern)
	if err != nil || len(files) < 2 {
		return ""
	}

	// Build benchstat command with labeled inputs
	// Format: benchstat name1=file1 name2=file2
	args := []string{"benchstat"}
	for _, file := range files {
		// Extract target name from filename (bench.TARGET.txt)
		base := filepath.Base(file)
		name := strings.TrimPrefix(base, "bench.")
		name = strings.TrimSuffix(name, ".txt")
		args = append(args, fmt.Sprintf("%s=%s", name, file))
	}

	// Use bin/tool to run benchstat with mise environment
	toolPath := filepath.Join(o.currentWorktree, "bin", "tool")
	cmd := exec.Command(toolPath, args...)
	output, err := cmd.CombinedOutput()
	if err != nil {
		o.Logger.Warn("benchstat failed", "error", err, "output", string(output))
		return ""
	}

	return string(output)
}

// describeOutputFile returns a human-readable description for a benchmark output file.
func describeOutputFile(filename string) string {
	// Exact matches
	descriptions := map[string]string{
		"BENCHMARK.md": "This benchmark report",
		"results.json": "Full benchmark results in JSON format (for programmatic analysis)",
		"git-sha":      "Git commit SHA of the worktree running the benchmark",
		"git-branch":   "Git branch name of the benchmark runner",
		"git-diff":     "Output of `git diff` showing uncommitted changes",
		"git-status":   "Output of `git status --porcelain`",
	}

	if desc, ok := descriptions[filename]; ok {
		return desc
	}

	// Pattern matches
	switch {
	case strings.HasPrefix(filename, "bench.") && strings.HasSuffix(filename, ".txt"):
		target := strings.TrimSuffix(strings.TrimPrefix(filename, "bench."), ".txt")
		return fmt.Sprintf("Go benchmark output for target `%s` (benchstat compatible)", target)

	case strings.HasPrefix(filename, "pglink.") && strings.HasSuffix(filename, ".log"):
		target := strings.TrimSuffix(strings.TrimPrefix(filename, "pglink."), ".log")
		return fmt.Sprintf("pglink stdout/stderr logs for target `%s`", target)

	case strings.HasPrefix(filename, "pgbouncer.") && strings.HasSuffix(filename, ".log"):
		target := strings.TrimSuffix(strings.TrimPrefix(filename, "pgbouncer."), ".log")
		return fmt.Sprintf("pgbouncer logs for target `%s`", target)

	case strings.HasPrefix(filename, "postgres.") && strings.HasSuffix(filename, ".log"):
		target := strings.TrimSuffix(strings.TrimPrefix(filename, "postgres."), ".log")
		return fmt.Sprintf("PostgreSQL container logs for `%s`", target)

	case strings.HasSuffix(filename, ".trace"):
		return "Flight recorder trace file (can be viewed with go tool trace)"

	case strings.HasSuffix(filename, ".pprof"):
		return "CPU/memory profile (can be viewed with go tool pprof)"

	default:
		return "Benchmark artifact"
	}
}

// checkObservability verifies that observability data was recorded to the stack.
// This is a STRICT check - it fails if traces, metrics, or logs are missing.
func (o *Orchestrator) checkObservability(ctx context.Context, results *BenchmarkResults) (*ObservabilityCheckResult, error) {
	result := &ObservabilityCheckResult{
		Passed: true,
		Errors: []string{},
	}

	// Give time for all data to be pushed/flushed
	o.Logger.Info("waiting for observability data to flush...")
	time.Sleep(5 * time.Second)

	// 1. Check Tempo for traces - REQUIRED
	tracesResult, err := o.checkTempo(ctx)
	if err != nil {
		result.Errors = append(result.Errors, fmt.Sprintf("tempo check failed: %v", err))
		result.Passed = false
	} else {
		result.Traces = tracesResult
		if !tracesResult.Found {
			result.Errors = append(result.Errors, "no traces found in Tempo for service 'pglink'")
			result.Passed = false
		}
	}

	// 2. Check Prometheus for pushed metrics - REQUIRED
	metricsResult, err := o.checkPrometheus(ctx)
	if err != nil {
		result.Errors = append(result.Errors, fmt.Sprintf("prometheus check failed: %v", err))
		result.Passed = false
	} else {
		result.Metrics = metricsResult
		if !metricsResult.Found {
			result.Errors = append(result.Errors, "no pglink metrics found in Prometheus")
			result.Passed = false
		}
	}

	// 3. Check Loki for pushed logs - REQUIRED
	logsResult, err := o.checkLoki(ctx)
	if err != nil {
		result.Errors = append(result.Errors, fmt.Sprintf("loki check failed: %v", err))
		result.Passed = false
	} else {
		result.Logs = logsResult
		if !logsResult.Found {
			result.Errors = append(result.Errors, "no pglink logs found in Loki")
			result.Passed = false
		}
	}

	// Log summary
	if result.Passed {
		o.Logger.Info("observability check PASSED",
			"traces", result.Traces.TraceCount,
			"metrics", len(result.Metrics.MetricNames),
			"logs", result.Logs.LogCount)
	} else {
		o.Logger.Error("observability check FAILED",
			"errors", result.Errors,
			"traces_found", result.Traces != nil && result.Traces.Found,
			"metrics_found", result.Metrics != nil && result.Metrics.Found,
			"logs_found", result.Logs != nil && result.Logs.Found)
	}

	return result, nil
}

// checkTempo queries Tempo to verify traces were recorded.
func (o *Orchestrator) checkTempo(ctx context.Context) (*TracesCheckResult, error) {
	result := &TracesCheckResult{
		Found:        false,
		ServiceNames: []string{},
	}

	// Tempo search API: GET /api/search?service.name=pglink&limit=10
	tempoURL := "http://localhost:13200/api/search"
	params := url.Values{}
	params.Set("service.name", "pglink")
	params.Set("limit", "100")

	reqURL := fmt.Sprintf("%s?%s", tempoURL, params.Encode())
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, reqURL, nil)
	if err != nil {
		return result, fmt.Errorf("failed to create request: %w", err)
	}

	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return result, fmt.Errorf("tempo request failed: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return result, fmt.Errorf("tempo returned status %d: %s", resp.StatusCode, string(body))
	}

	// Parse response
	var searchResp tempoSearchResponse
	if err := json.NewDecoder(resp.Body).Decode(&searchResp); err != nil {
		return result, fmt.Errorf("failed to decode tempo response: %w", err)
	}

	result.TraceCount = len(searchResp.Traces)

	// Collect service names from traces
	serviceSet := make(map[string]bool)
	for _, trace := range searchResp.Traces {
		if trace.RootServiceName != "" {
			serviceSet[trace.RootServiceName] = true
		}
	}

	for svc := range serviceSet {
		result.ServiceNames = append(result.ServiceNames, svc)
	}

	// Use inspectedTraces as an approximation for total spans if available
	if searchResp.Metrics != nil {
		result.SpanCount = searchResp.Metrics.InspectedTraces
	}

	result.Found = result.TraceCount > 0

	o.Logger.Info("tempo check complete",
		"traces", result.TraceCount,
		"inspected", result.SpanCount,
		"services", result.ServiceNames)

	return result, nil
}

// tempoSearchResponse is the response structure from Tempo's search API.
type tempoSearchResponse struct {
	Traces  []tempoTrace  `json:"traces"`
	Metrics *tempoMetrics `json:"metrics,omitempty"`
}

type tempoTrace struct {
	TraceID         string `json:"traceID"`
	RootServiceName string `json:"rootServiceName"`
	RootTraceName   string `json:"rootTraceName"`
	DurationMs      int    `json:"durationMs,omitempty"`
}

type tempoMetrics struct {
	InspectedTraces int `json:"inspectedTraces"`
}

// checkPrometheus queries Prometheus to verify metrics were pushed via remote write.
func (o *Orchestrator) checkPrometheus(ctx context.Context) (*MetricsCheckResult, error) {
	result := &MetricsCheckResult{
		Found:       false,
		MetricNames: []string{},
		Source:      "prometheus:19090",
	}

	// Query Prometheus for pglink metrics using the series API
	// We look for any metrics with the bench_execution_id label matching our run
	// Note: We don't use time constraints since the execution_id is unique enough
	promURL := "http://localhost:19090/api/v1/series"
	params := url.Values{}
	// Match any metric with our execution_id label
	params.Set("match[]", fmt.Sprintf("{bench_execution_id=\"%s\"}", o.executionID))

	reqURL := fmt.Sprintf("%s?%s", promURL, params.Encode())
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, reqURL, nil)
	if err != nil {
		return result, fmt.Errorf("failed to create request: %w", err)
	}

	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return result, fmt.Errorf("prometheus request failed: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return result, fmt.Errorf("prometheus returned status %d: %s", resp.StatusCode, string(body))
	}

	// Parse response
	var promResp prometheusSeriesResponse
	if err := json.NewDecoder(resp.Body).Decode(&promResp); err != nil {
		return result, fmt.Errorf("failed to decode prometheus response: %w", err)
	}

	if promResp.Status != "success" {
		return result, fmt.Errorf("prometheus query failed: status=%s", promResp.Status)
	}

	// Collect unique metric names
	metricSet := make(map[string]bool)
	for _, series := range promResp.Data {
		if name, ok := series["__name__"]; ok {
			metricSet[name] = true
		}
	}

	for name := range metricSet {
		result.MetricNames = append(result.MetricNames, name)
	}

	result.SampleCount = len(promResp.Data)
	result.Found = len(result.MetricNames) > 0

	o.Logger.Info("prometheus check complete",
		"metrics", len(result.MetricNames),
		"samples", result.SampleCount)

	return result, nil
}

// prometheusSeriesResponse is the response from Prometheus series API.
type prometheusSeriesResponse struct {
	Status string              `json:"status"`
	Data   []map[string]string `json:"data"`
}

// scrapeMetricsEndpoint scrapes pglink's /metrics endpoint directly to verify metrics are being generated.
// This is a fallback when Prometheus doesn't have pushed metrics (because pglink uses prometheus client lib).
func (o *Orchestrator) scrapeMetricsEndpoint(ctx context.Context) (*MetricsCheckResult, error) {
	result := &MetricsCheckResult{
		Found:       false,
		MetricNames: []string{},
		Source:      "pglink:/metrics",
	}

	// Find a pglink target's metrics port
	var metricsPort int
	for _, t := range o.Config.Targets {
		if t.Type == TargetTypePglink {
			if port, ok := o.metricsPorts[t.Name]; ok {
				metricsPort = port
				break
			}
		}
	}

	if metricsPort == 0 {
		return result, fmt.Errorf("no pglink metrics port found")
	}

	endpoint := fmt.Sprintf("http://localhost:%d/metrics", metricsPort)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return result, fmt.Errorf("failed to create request: %w", err)
	}

	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return result, fmt.Errorf("failed to scrape %s: %w", endpoint, err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return result, fmt.Errorf("metrics endpoint returned %d: %s", resp.StatusCode, string(body))
	}

	// Read and parse metrics text format
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return result, fmt.Errorf("failed to read response: %w", err)
	}

	// Simple parsing: count lines starting with "pglink_"
	lines := strings.Split(string(body), "\n")
	metricSet := make(map[string]bool)
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, "pglink_") && !strings.HasPrefix(line, "#") {
			// Extract metric name (up to first space or brace)
			name := line
			if idx := strings.IndexAny(name, " {"); idx > 0 {
				name = name[:idx]
			}
			metricSet[name] = true
			result.SampleCount++
		}
	}

	for name := range metricSet {
		result.MetricNames = append(result.MetricNames, name)
	}

	result.Found = len(result.MetricNames) > 0
	result.Source = endpoint

	o.Logger.Info("scraped metrics directly from pglink",
		"endpoint", endpoint,
		"metrics", len(result.MetricNames),
		"samples", result.SampleCount)

	return result, nil
}

// checkLoki queries Loki to verify logs were pushed.
func (o *Orchestrator) checkLoki(ctx context.Context) (*LogsCheckResult, error) {
	result := &LogsCheckResult{
		Found:  false,
		Source: "loki:13100",
	}

	// Query Loki for pglink logs using label query
	// Use LogQL to query for service_name="pglink"
	lokiURL := "http://localhost:13100/loki/api/v1/query_range"
	params := url.Values{}
	params.Set("query", "{service_name=\"pglink\"}")
	params.Set("limit", "100")
	// Query last 5 minutes
	now := time.Now()
	params.Set("start", fmt.Sprintf("%d", now.Add(-5*time.Minute).UnixNano()))
	params.Set("end", fmt.Sprintf("%d", now.UnixNano()))

	reqURL := fmt.Sprintf("%s?%s", lokiURL, params.Encode())
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, reqURL, nil)
	if err != nil {
		return result, fmt.Errorf("failed to create request: %w", err)
	}

	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return result, fmt.Errorf("loki request failed: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return result, fmt.Errorf("loki returned status %d: %s", resp.StatusCode, string(body))
	}

	// Parse response
	var lokiResp lokiQueryResponse
	if err := json.NewDecoder(resp.Body).Decode(&lokiResp); err != nil {
		return result, fmt.Errorf("failed to decode loki response: %w", err)
	}

	if lokiResp.Status != "success" {
		return result, fmt.Errorf("loki query failed: status=%s", lokiResp.Status)
	}

	// Count streams and entries
	result.StreamCount = len(lokiResp.Data.Result)
	for _, stream := range lokiResp.Data.Result {
		result.LogCount += len(stream.Values)
	}

	result.Found = result.LogCount > 0

	o.Logger.Info("loki check complete",
		"streams", result.StreamCount,
		"logs", result.LogCount)

	return result, nil
}

// lokiQueryResponse is the response from Loki query_range API.
type lokiQueryResponse struct {
	Status string `json:"status"`
	Data   struct {
		ResultType string `json:"resultType"`
		Result     []struct {
			Stream map[string]string `json:"stream"`
			Values [][]string        `json:"values"` // Each value is [timestamp, log_line]
		} `json:"result"`
	} `json:"data"`
}
