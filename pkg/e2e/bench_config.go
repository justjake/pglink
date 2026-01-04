package e2e

import (
	"time"
)

// BenchSuiteConfig is the top-level configuration for a benchmark suite.
type BenchSuiteConfig struct {
	// Measurement settings
	Duration time.Duration // Duration of each benchmark measurement (default: 15s)
	Warmup   time.Duration // Warmup duration before measuring (default: 5s)
	Rounds   int           // Number of rounds per target (default: 2)
	Count    int           // Iterations per benchmark for go test -count (default: 1)

	// Concurrency settings (controls -cpu flag for go test)
	CPU int // Number of parallel workers / GOMAXPROCS for benchmarks (default: 100)

	// Output settings
	OutputDir string // Output directory for results

	// Benchmark selection
	Cases []string // Cases to run (empty = all): select1, mixed, copy_in, copy_out

	// Protocol settings
	SimpleQueryMode bool // Use simple query protocol instead of extended

	// Workload settings
	Seed int64 // Random seed for workload generation (0 = time-based)

	// Observability
	Observable      bool // Enable observability integration
	CheckObservable bool // Verify observability data was recorded after benchmark
	FlightRecorder  bool // Enable flight recorder for pglink targets

	// Profiling
	Pprof           bool          // Enable pprof profiling for pglink targets
	ProfileDuration time.Duration // Duration for CPU profile collection (default: 30s)

	// Pglink pool settings
	PglinkPoolMaxConns int // Backend pool max connections (0 = use CPU setting)

	// Targets to benchmark
	Targets []TargetConfig

	// A/B test configuration (if set, only runs A vs B)
	ABTest *ABTestConfig
}

// DefaultBenchSuiteConfig returns a BenchSuiteConfig with sensible defaults.
func DefaultBenchSuiteConfig() BenchSuiteConfig {
	return BenchSuiteConfig{
		Duration:  15 * time.Second,
		Warmup:    5 * time.Second,
		Rounds:    2,
		Count:     1,
		CPU:       100,
		OutputDir: "out/benchmarks",
	}
}

// TargetType identifies the type of benchmark target.
type TargetType string

const (
	// TargetTypeDirect connects directly to PostgreSQL (baseline).
	TargetTypeDirect TargetType = "direct"
	// TargetTypePglink connects through pglink proxy.
	TargetTypePglink TargetType = "pglink"
	// TargetTypePgbouncer connects through pgbouncer (comparison).
	TargetTypePgbouncer TargetType = "pgbouncer"
	// TargetTypeMitmProxy connects through mitm-proxy (simple test proxy).
	TargetTypeMitmProxy TargetType = "mitm-proxy"
)

// TargetConfig configures a single benchmark target.
type TargetConfig struct {
	// Name is the display name (e.g., "pglink", "direct", "pgbouncer")
	Name string
	// Type identifies the target type
	Type TargetType
	// ConnString is the database connection string
	ConnString string

	// Process configuration (for pglink/pgbouncer targets)
	BinaryPath         string   // Path to binary (default: ./out/pglink)
	Worktree           string   // Path to worktree to build from (empty = current directory)
	Port               int      // Listen port
	MetricsPort        int      // Prometheus metrics port
	ExtraArgs          []string // Extra CLI arguments
	ExtraEnv           []string // Extra environment variables
	MessageBufferBytes string   // Ring buffer size (e.g., "16KiB")
	GOMAXPROCS         int      // GOMAXPROCS setting (0 = default)

	// Git metadata for this target (populated during setup)
	Git *GitMetadata
}

// ABTestConfig configures an A/B comparison test.
type ABTestConfig struct {
	A TargetVariant
	B TargetVariant
}

// TargetVariant configures one side of an A/B test.
type TargetVariant struct {
	// Label is the display label (required)
	Label string
	// BinaryPath is the path to pglink binary (default: ./out/pglink)
	BinaryPath string
	// Worktree is the path to worktree to build from
	// If set, builds to runner's out/worktrees/<label>/pglink
	Worktree string
	// ExtraArgs are extra CLI arguments for pglink
	ExtraArgs []string
	// ExtraEnv are extra environment variables
	ExtraEnv []string
	// GOMAXPROCS setting (0 = default)
	GOMAXPROCS int
}

// CompareConfig configures comparison against another git ref/binary.
type CompareConfig struct {
	// Ref is the git ref (branch/tag/commit) to compare against
	Ref string
	// Worktree is the path to existing git worktree
	Worktree string
	// Binary is the path to pre-built binary
	Binary string
	// Label is the display label (default: derived from ref/path)
	Label string
	// GOMAXPROCS for comparison target
	GOMAXPROCS int
}

// BenchRunConfig configures a single benchmark run against one target.
// This is passed to BenchRunner and includes both suite-level and target-level config.
type BenchRunConfig struct {
	// Suite-level config (inherited from BenchSuiteConfig)
	Duration        time.Duration
	Warmup          time.Duration
	CPU             int // Number of parallel workers (-cpu flag)
	SimpleQueryMode bool
	Seed            int64
	Cases           []string

	// Target-specific config
	Target     TargetConfig
	ConnString string // Resolved connection string for this target

	// Run metadata
	RunID       string    // Unique identifier for this run
	Round       int       // Current round number (1-indexed)
	TotalRounds int       // Total rounds
	Timestamp   time.Time // Start time of this run

	// Go test specific
	Count   int           // -count flag value
	Timeout time.Duration // -timeout flag value

	// Paths
	OutputDir        string // Directory for benchmark output
	BenchPackagePath string // Path to benchmark package (default: ./pkg/benchmarks)
}

// BenchRunResult contains benchmark execution results.
type BenchRunResult struct {
	// Output is the raw output in Go benchmark format (for benchstat)
	Output []byte

	// Metrics contains parsed benchmark metrics for programmatic access
	Metrics []BenchMetric

	// Execution metadata
	Duration time.Duration // Actual execution time
	ExitCode int           // Process exit code (0 = success)
	Stderr   []byte        // Captured stderr

	// Error contains any execution error
	Error error
}

// BenchMetric represents a single benchmark measurement.
type BenchMetric struct {
	// Name is the full benchmark name (e.g., "BenchmarkSelect1/target=pglink")
	Name string
	// Iterations is the number of iterations (b.N)
	Iterations int64
	// NsPerOp is nanoseconds per operation
	NsPerOp float64
	// BytesPerOp is bytes allocated per operation (from -benchmem)
	BytesPerOp int64
	// AllocsPerOp is allocations per operation (from -benchmem)
	AllocsPerOp int64
	// MBPerSec is throughput for COPY benchmarks (0 if not applicable)
	MBPerSec float64
	// OpsPerSec is operations per second (custom metric from benchmarks)
	OpsPerSec float64
	// QPS is queries per second (custom metric from benchmarks)
	QPS float64
}

// RunnerGitInfo captures git metadata for the benchmark runner itself.
type RunnerGitInfo struct {
	Git       *GitMetadata `json:"git"`
	GitDiff   string       `json:"git_diff,omitempty"`
	GitStatus string       `json:"git_status,omitempty"`
}

// BenchmarkResults is the top-level structure for results.json.
type BenchmarkResults struct {
	ExecutionID        string                    `json:"execution_id"`
	Timestamp          time.Time                 `json:"timestamp"`
	Runner             RunnerGitInfo             `json:"runner"`
	Config             BenchSuiteConfig          `json:"config"`
	Results            []TargetResult            `json:"results"`
	ObservabilityCheck *ObservabilityCheckResult `json:"observability_check,omitempty"`
}

// ObservabilityCheckResult contains results of observability verification.
type ObservabilityCheckResult struct {
	// Passed is true if all checks passed.
	Passed bool `json:"passed"`
	// Traces contains trace verification results.
	Traces *TracesCheckResult `json:"traces,omitempty"`
	// Metrics contains metrics verification results.
	Metrics *MetricsCheckResult `json:"metrics,omitempty"`
	// Logs contains log verification results.
	Logs *LogsCheckResult `json:"logs,omitempty"`
	// Errors contains any errors encountered during verification.
	Errors []string `json:"errors,omitempty"`
}

// TracesCheckResult contains results from checking Tempo for traces.
type TracesCheckResult struct {
	// Found is true if traces were found.
	Found bool `json:"found"`
	// TraceCount is the number of traces found.
	TraceCount int `json:"trace_count"`
	// SpanCount is the total number of spans found.
	SpanCount int `json:"span_count"`
	// ServiceNames lists the service names found.
	ServiceNames []string `json:"service_names,omitempty"`
}

// MetricsCheckResult contains results from checking metrics.
type MetricsCheckResult struct {
	// Found is true if metrics were found.
	Found bool `json:"found"`
	// MetricNames lists the pglink metrics found.
	MetricNames []string `json:"metric_names,omitempty"`
	// SampleCount is the number of metric samples found.
	SampleCount int `json:"sample_count"`
	// Source indicates where metrics were found (e.g., "prometheus:19090")
	Source string `json:"source,omitempty"`
}

// LogsCheckResult contains results from checking Loki for logs.
type LogsCheckResult struct {
	// Found is true if logs were found.
	Found bool `json:"found"`
	// LogCount is the number of log entries found.
	LogCount int `json:"log_count"`
	// StreamCount is the number of log streams found.
	StreamCount int `json:"stream_count"`
	// Source indicates where logs were found (e.g., "loki:13100")
	Source string `json:"source,omitempty"`
}

// ScrapedMetrics is an alias for MetricsCheckResult for backwards compatibility.
// It holds metrics scraped directly from a pglink instance's /metrics endpoint.
type ScrapedMetrics = MetricsCheckResult

// TargetResult contains results for a single target.
type TargetResult struct {
	Target         string          `json:"target"`
	Git            *GitMetadata    `json:"git,omitempty"`
	BinaryPath     string          `json:"binary_path,omitempty"`
	Metrics        []BenchMetric   `json:"metrics"`
	Rounds         []RoundResult   `json:"rounds"`
	ScrapedMetrics *ScrapedMetrics `json:"scraped_metrics,omitempty"`
}

// RoundResult contains results for a single round.
type RoundResult struct {
	Round    int           `json:"round"`
	Duration time.Duration `json:"duration"`
	Output   string        `json:"output,omitempty"` // Raw benchmark output
}

// RunTimeout calculates a timeout for a benchmark run based on duration and warmup.
// Adds 20% buffer and ensures minimum 30s timeout.
func (cfg BenchRunConfig) RunTimeout() time.Duration {
	timeout := cfg.Duration + cfg.Warmup
	timeout += timeout / 5 // 20% buffer
	if timeout < 30*time.Second {
		timeout = 30 * time.Second
	}
	return timeout
}
