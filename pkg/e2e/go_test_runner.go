package e2e

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"regexp"
	"strconv"
	"strings"
	"time"
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

// Run executes benchmarks using go test -bench.
func (r *GoTestRunner) Run(ctx context.Context, cfg BenchRunConfig) (*BenchRunResult, error) {
	goPath := r.GoPath
	if goPath == "" {
		goPath = "go"
	}

	// Build the go test command
	args := []string{
		"test",
		"-bench=.",
		"-benchmem",
		"-run=^$", // Skip unit tests, only run benchmarks
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

	// Add benchmark configuration via environment variables
	env = append(env,
		fmt.Sprintf("BENCH_CONN_STRING=%s", cfg.ConnString),
		fmt.Sprintf("BENCH_TARGET=%s", cfg.Target.Name),
		fmt.Sprintf("BENCH_MAX_CONNS=%d", cfg.MaxConns),
		fmt.Sprintf("BENCH_CONCURRENCY=%d", cfg.Concurrency),
		fmt.Sprintf("BENCH_DURATION=%s", cfg.Duration),
		fmt.Sprintf("BENCH_WARMUP=%s", cfg.Warmup),
	)

	if cfg.SimpleQueryMode {
		env = append(env, "BENCH_SIMPLE_QUERY=true")
	}

	if cfg.Seed != 0 {
		env = append(env, fmt.Sprintf("BENCH_SEED=%d", cfg.Seed))
	}

	// Add run metadata
	env = append(env,
		fmt.Sprintf("BENCH_RUN_ID=%s", cfg.RunID),
		fmt.Sprintf("BENCH_ROUND=%d", cfg.Round),
		fmt.Sprintf("BENCH_TOTAL_ROUNDS=%d", cfg.TotalRounds),
	)

	cmd.Env = env

	// Capture output
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	// Run the benchmark
	startTime := time.Now()
	err := cmd.Run()
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
	// Matches lines like: BenchmarkSelect1/target=pglink-16    50000    25000 ns/op    1234 B/op    12 allocs/op
	benchLineRegex = regexp.MustCompile(`^(Benchmark\S+)\s+(\d+)\s+([\d.]+)\s+ns/op(?:\s+([\d.]+)\s+B/op)?(?:\s+(\d+)\s+allocs/op)?(?:\s+([\d.]+)\s+MB/s)?`)
)

// parseBenchmarkOutput extracts BenchMetric from go test -bench output.
func parseBenchmarkOutput(output []byte) []BenchMetric {
	var metrics []BenchMetric

	scanner := bufio.NewScanner(bytes.NewReader(output))
	for scanner.Scan() {
		line := scanner.Text()
		matches := benchLineRegex.FindStringSubmatch(line)
		if matches == nil {
			continue
		}

		metric := BenchMetric{
			Name: matches[1],
		}

		// Parse iterations
		if n, err := strconv.ParseInt(matches[2], 10, 64); err == nil {
			metric.Iterations = n
		}

		// Parse ns/op
		if ns, err := strconv.ParseFloat(matches[3], 64); err == nil {
			metric.NsPerOp = ns
		}

		// Parse B/op (optional)
		if len(matches) > 4 && matches[4] != "" {
			if b, err := strconv.ParseInt(matches[4], 10, 64); err == nil {
				metric.BytesPerOp = b
			}
		}

		// Parse allocs/op (optional)
		if len(matches) > 5 && matches[5] != "" {
			if a, err := strconv.ParseInt(matches[5], 10, 64); err == nil {
				metric.AllocsPerOp = a
			}
		}

		// Parse MB/s (optional, for throughput benchmarks)
		if len(matches) > 6 && matches[6] != "" {
			if mb, err := strconv.ParseFloat(matches[6], 64); err == nil {
				metric.MBPerSec = mb
			}
		}

		metrics = append(metrics, metric)
	}

	return metrics
}
