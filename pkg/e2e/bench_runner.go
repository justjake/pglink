package e2e

import (
	"context"
	"strings"
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

	// SupportedCases returns the list of benchmark case names this runner supports.
	// Cases are matched case-insensitively.
	SupportedCases() []string
}

// BenchRunnerFactory creates BenchRunner instances.
type BenchRunnerFactory func() BenchRunner

// DefaultBenchRunner returns the default BenchRunner implementation (GoTestRunner).
func DefaultBenchRunner() BenchRunner {
	return NewGoTestRunner()
}

// FilterCasesForRunner returns the subset of cases that are supported by the given runner.
// If cases is empty, returns all supported cases (run everything).
// Returns nil if no cases match.
func FilterCasesForRunner(cases []string, runner BenchRunner) []string {
	supported := runner.SupportedCases()

	// If no cases specified, return all supported cases (default "run all" behavior)
	if len(cases) == 0 {
		return supported
	}

	supportedSet := make(map[string]bool, len(supported))
	for _, c := range supported {
		supportedSet[strings.ToLower(c)] = true
	}

	var filtered []string
	for _, c := range cases {
		if supportedSet[strings.ToLower(c)] {
			filtered = append(filtered, c)
		}
	}
	return filtered
}

// HasCasesForRunner returns true if any of the given cases are supported by the runner.
func HasCasesForRunner(cases []string, runner BenchRunner) bool {
	return len(FilterCasesForRunner(cases, runner)) > 0
}
