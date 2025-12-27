package e2e

import (
	"context"
)

// BenchRunner executes benchmarks and returns results in Go benchmark format.
// This interface allows different benchmark backends (go test -bench, pgbench, etc.)
// to be used interchangeably.
type BenchRunner interface {
	// Run executes benchmarks for the given target configuration.
	// Returns results that can be written in Go benchmark format.
	Run(ctx context.Context, cfg BenchRunConfig) (*BenchRunResult, error)

	// Name returns the name of this runner (e.g., "go-test", "pgbench").
	Name() string
}

// BenchRunnerFactory creates BenchRunner instances.
type BenchRunnerFactory func() BenchRunner

// DefaultBenchRunner returns the default BenchRunner implementation (GoTestRunner).
func DefaultBenchRunner() BenchRunner {
	return NewGoTestRunner()
}
