package e2e

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/justjake/pglink/pkg/benchmarks"
)

// GoTestRunner runs benchmarks using `go test -bench`.
type GoTestRunner struct {
	// GoPath is the path to the go binary. If empty, uses "go" from PATH.
	GoPath string
}

// NewGoTestRunner creates a new GoTestRunner.
func NewGoTestRunner() *GoTestRunner {
	return &GoTestRunner{}
}

// Name returns the runner name.
func (r *GoTestRunner) Name() string {
	return "go-test"
}

// SupportedCases returns the benchmark cases supported by the Go test runner.
func (r *GoTestRunner) SupportedCases() []string {
	return []string{"select1", "copy_in", "copy_out", "mixed"}
}

// Run executes benchmarks using go test -bench.
func (r *GoTestRunner) Run(ctx context.Context, cfg BenchRunConfig) (*BenchRunResult, error) {
	goPath := r.GoPath
	if goPath == "" {
		goPath = "go"
	}

	// Create a timeout context to prevent hangs
	timeout := cfg.RunTimeout()
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	// Build the go test command
	args := []string{
		"test",
		"-v", // Verbose output to show progress
		"-bench=.",
		"-benchmem",
		"-run=^$", // Skip unit tests, only run benchmarks
	}

	// Add -cpu flag for parallelism (controls GOMAXPROCS and number of parallel workers)
	if cfg.CPU > 0 {
		args = append(args, fmt.Sprintf("-cpu=%d", cfg.CPU))
	}

	// Add count flag
	if cfg.Count > 0 {
		args = append(args, fmt.Sprintf("-count=%d", cfg.Count))
	}

	// Add timeout flag
	if cfg.Timeout > 0 {
		args = append(args, fmt.Sprintf("-timeout=%s", cfg.Timeout))
	}

	// Add benchmark filter if specific cases are requested
	if len(cfg.Cases) > 0 {
		// Build regex pattern for specific cases
		pattern := buildCasePattern(cfg.Cases)
		args = append(args, fmt.Sprintf("-bench=%s", pattern))
	}

	// Add the package path
	pkgPath := cfg.BenchPackagePath
	if pkgPath == "" {
		pkgPath = "./pkg/benchmarks/..."
	}
	args = append(args, pkgPath)

	cmd := exec.CommandContext(ctx, goPath, args...)

	// Set up environment
	env := os.Environ()

	// Build benchmark config and serialize to environment variables
	benchCfg := benchmarks.BenchConfig{
		ConnString:  cfg.ConnString,
		Target:      cfg.Target.Name,
		Duration:    cfg.Duration,
		Warmup:      cfg.Warmup,
		Seed:        cfg.Seed,
		RunID:       cfg.RunID,
		Round:       cfg.Round,
		TotalRounds: cfg.TotalRounds,
	}
	if cfg.SimpleQueryMode {
		benchCfg.Protocol = "simple"
	}

	env = append(env, benchCfg.ToEnv()...)

	cmd.Env = env

	// Open output files for this run
	casesStr := strings.Join(cfg.Cases, ",")
	if casesStr == "" {
		casesStr = "all"
	}
	outputs, err := OpenProcessOutputs(cfg.OutputDir, "go-test", cfg.Target.Name, cfg.Round, casesStr)
	if err != nil {
		return nil, fmt.Errorf("failed to open output files: %w", err)
	}
	defer func() { _ = outputs.Close() }()

	// Set up pipes to stream output in real-time
	stdoutPipe, err := cmd.StdoutPipe()
	if err != nil {
		return nil, fmt.Errorf("failed to create stdout pipe: %w", err)
	}
	stderrPipe, err := cmd.StderrPipe()
	if err != nil {
		return nil, fmt.Errorf("failed to create stderr pipe: %w", err)
	}

	// Start the command
	startTime := time.Now()
	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("failed to start go test: %w", err)
	}

	// Stream output in real-time while capturing it
	var stdout, stderr bytes.Buffer
	var wg sync.WaitGroup
	wg.Add(2)

	// Build writers for stdout: buffer (for parsing), os.Stderr (visibility), and file
	stdoutWriters := []io.Writer{&stdout, os.Stderr}
	if outputs.Stdout != nil {
		stdoutWriters = append(stdoutWriters, outputs.Stdout)
	}
	stdoutWriter := io.MultiWriter(stdoutWriters...)

	// Build writers for stderr: buffer (for error messages), os.Stderr (visibility), and file
	stderrWriters := []io.Writer{&stderr, os.Stderr}
	if outputs.Stderr != nil {
		stderrWriters = append(stderrWriters, outputs.Stderr)
	}
	stderrWriter := io.MultiWriter(stderrWriters...)

	// Stream stdout
	go func() {
		defer wg.Done()
		_, _ = io.Copy(stdoutWriter, stdoutPipe)
	}()

	// Stream stderr
	go func() {
		defer wg.Done()
		_, _ = io.Copy(stderrWriter, stderrPipe)
	}()

	// Wait for output streaming to complete
	wg.Wait()

	// Wait for command to finish
	err = cmd.Wait()
	duration := time.Since(startTime)

	result := &BenchRunResult{
		Output:   stdout.Bytes(),
		Stderr:   stderr.Bytes(),
		Duration: duration,
	}

	// Get exit code
	if cmd.ProcessState != nil {
		result.ExitCode = cmd.ProcessState.ExitCode()
	}

	if err != nil {
		// Check if this was a timeout - return partial results with error
		if ctx.Err() != nil {
			result.Metrics = parseBenchmarkOutput(stdout.Bytes())
			result.Error = fmt.Errorf("go test timed out after %v: %w", timeout, ctx.Err())
			return result, result.Error
		}
		result.Error = fmt.Errorf("go test failed: %w\nstderr: %s", err, stderr.String())
	}

	// Parse benchmark metrics from output
	result.Metrics = parseBenchmarkOutput(stdout.Bytes())

	return result, nil
}

// buildCasePattern builds a regex pattern for specific benchmark cases.
func buildCasePattern(cases []string) string {
	if len(cases) == 0 {
		return "."
	}

	// Map case names to benchmark names
	benchNames := make([]string, 0, len(cases))
	for _, c := range cases {
		switch strings.ToLower(c) {
		case "select1":
			benchNames = append(benchNames, "BenchmarkSelect1")
		case "mixed":
			benchNames = append(benchNames, "BenchmarkMixed")
		case "copy_in", "copyin":
			benchNames = append(benchNames, "BenchmarkCopyIn")
		case "copy_out", "copyout":
			benchNames = append(benchNames, "BenchmarkCopyOut")
		default:
			// Use as-is if it looks like a benchmark name
			if strings.HasPrefix(c, "Benchmark") {
				benchNames = append(benchNames, c)
			} else {
				benchNames = append(benchNames, "Benchmark"+c)
			}
		}
	}

	return "^(" + strings.Join(benchNames, "|") + ")"
}

// Regex patterns for parsing benchmark output
var (
	// Matches the benchmark name and iterations at the start of the line
	benchStartRegex = regexp.MustCompile(`^(Benchmark\S+)\s+(\d+)\s+`)
	// Individual metric patterns
	nsPerOpRegex     = regexp.MustCompile(`([\d.]+)\s+ns/op`)
	mbPerSecRegex    = regexp.MustCompile(`([\d.]+)\s+MB/s`)
	opsPerSecRegex   = regexp.MustCompile(`([\d.]+)\s+ops/s`)
	qpsRegex         = regexp.MustCompile(`([\d.]+)\s+qps`)
	bytesPerOpRegex  = regexp.MustCompile(`(\d+)\s+B/op`)
	allocsPerOpRegex = regexp.MustCompile(`(\d+)\s+allocs/op`)
)

// parseBenchmarkOutput extracts BenchMetric from go test -bench output.
func parseBenchmarkOutput(output []byte) []BenchMetric {
	var metrics []BenchMetric

	scanner := bufio.NewScanner(bytes.NewReader(output))
	for scanner.Scan() {
		line := scanner.Text()

		// Check if line starts with a benchmark name and iterations
		startMatch := benchStartRegex.FindStringSubmatch(line)
		if startMatch == nil {
			continue
		}

		metric := BenchMetric{
			Name: startMatch[1],
		}

		// Parse iterations
		if n, err := strconv.ParseInt(startMatch[2], 10, 64); err == nil {
			metric.Iterations = n
		}

		// Parse ns/op
		if m := nsPerOpRegex.FindStringSubmatch(line); m != nil {
			if ns, err := strconv.ParseFloat(m[1], 64); err == nil {
				metric.NsPerOp = ns
			}
		}

		// Parse MB/s (throughput)
		if m := mbPerSecRegex.FindStringSubmatch(line); m != nil {
			if mb, err := strconv.ParseFloat(m[1], 64); err == nil {
				metric.MBPerSec = mb
			}
		}

		// Parse ops/s (custom metric)
		if m := opsPerSecRegex.FindStringSubmatch(line); m != nil {
			if ops, err := strconv.ParseFloat(m[1], 64); err == nil {
				metric.OpsPerSec = ops
			}
		}

		// Parse qps (custom metric)
		if m := qpsRegex.FindStringSubmatch(line); m != nil {
			if qps, err := strconv.ParseFloat(m[1], 64); err == nil {
				metric.QPS = qps
			}
		}

		// Parse B/op
		if m := bytesPerOpRegex.FindStringSubmatch(line); m != nil {
			if b, err := strconv.ParseInt(m[1], 10, 64); err == nil {
				metric.BytesPerOp = b
			}
		}

		// Parse allocs/op
		if m := allocsPerOpRegex.FindStringSubmatch(line); m != nil {
			if a, err := strconv.ParseInt(m[1], 10, 64); err == nil {
				metric.AllocsPerOp = a
			}
		}

		metrics = append(metrics, metric)
	}

	return metrics
}
