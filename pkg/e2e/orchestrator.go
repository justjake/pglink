package e2e

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"
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
	if target.Type != TargetTypeDirect {
		if err := o.startTargetProcess(ctx, &target); err != nil {
			return nil, fmt.Errorf("failed to start %s: %w", target.Name, err)
		}
		defer o.stopTargetProcess(target.Name)
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

	// Build args
	args := []string{
		"-config", filepath.Join(o.mainWorktreePath, "pglink.json"),
	}

	if target.Port != 0 {
		args = append(args, "-listen-addr", fmt.Sprintf(":%d", target.Port))
	}

	if target.MessageBufferBytes != "" {
		args = append(args, "-message-buffer-bytes", target.MessageBufferBytes)
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

// startPgbouncer starts a pgbouncer process.
func (o *Orchestrator) startPgbouncer(ctx context.Context, target *TargetConfig) error {
	// TODO: Implement pgbouncer start
	return fmt.Errorf("pgbouncer support not yet implemented")
}

// stopTargetProcess stops a running target process.
func (o *Orchestrator) stopTargetProcess(name string) {
	cmd, ok := o.processes[name]
	if !ok {
		return
	}

	o.Logger.Info("stopping process", "target", name)

	if cmd.Process != nil {
		_ = cmd.Process.Kill() // Ignore error - process may have already exited
		_ = cmd.Wait()         // Ignore error - we just want to clean up
	}

	delete(o.processes, name)
}

// ensureDockerContainers ensures required docker containers are running.
func (o *Orchestrator) ensureDockerContainers(ctx context.Context) error {
	// Run docker compose up from main worktree
	cmd := exec.CommandContext(ctx, "docker", "compose", "up", "-d")
	cmd.Dir = o.mainWorktreePath

	output, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("docker compose up failed: %w\nOutput: %s", err, string(output))
	}

	o.Logger.Info("docker containers running", "output", string(output))
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
