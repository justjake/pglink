// Command bench runs comprehensive benchmarks comparing direct postgres, pglink, and pgbouncer.
//
// The benchmark runs each configuration for a fixed duration, collects metrics, then exits.
// It does NOT wait for in-flight queries to complete after the duration expires.
package main

import (
	"context"
	"encoding/json"
	"encoding/pem"
	"flag"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"math/rand"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"text/template"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/justjake/pglink/pkg/config"
	"github.com/justjake/pglink/pkg/config/pgbouncer"
)

// TaskResult is returned by each task execution to report work done.
type TaskResult struct {
	// Queries is the number of queries executed in this task.
	Queries int
	// Bytes is the number of bytes transferred (for COPY benchmarks).
	Bytes int64
}

// BenchmarkCase defines a single benchmark case (e.g., "Mixed Workload", "COPY OUT")
type BenchmarkCase struct {
	// Name is a short identifier for this benchmark
	Name string

	// Title is the markdown title for this benchmark section
	Title string

	// Description is optional markdown description
	Description string

	// SetUp runs once before all targets for this case
	SetUp func(ctx context.Context, s *BenchmarkSuite) error

	// TearDown runs once after all targets for this case
	TearDown func(ctx context.Context, s *BenchmarkSuite) error
}

// BenchmarkTarget defines a single target configuration (becomes a row in the table)
type BenchmarkTarget struct {
	// Name is the target name shown in the table (e.g., "direct", "pgbouncer", "pglink")
	Name string

	// SetUp runs before benchmarking this target
	SetUp func(ctx context.Context, s *BenchmarkSuite) error

	// TearDown runs after benchmarking this target
	TearDown func(ctx context.Context, s *BenchmarkSuite) error

	// Task runs a single iteration of the benchmark task.
	// Returns TaskResult with query count and bytes transferred.
	Task func(ctx context.Context, s *BenchmarkSuite, pool *pgxpool.Pool, bc *BenchmarkCase) (TaskResult, error)

	// ConnString returns the connection string for this target
	ConnString func(s *BenchmarkSuite) string
}

// BenchmarkResult holds results from a single benchmark run
type BenchmarkResult struct {
	CaseTitle     string        `json:"case_title"`
	TargetName    string        `json:"target_name"`
	MaxConns      int           `json:"max_conns"`
	Concurrency   int           `json:"concurrency"`
	TotalTasks    int64         `json:"total_tasks"`
	TotalQueries  int64         `json:"total_queries"`
	TotalBytes    int64         `json:"total_bytes"`
	SuccessCount  int64         `json:"success_count"`
	ErrorCount    int64         `json:"error_count"`
	ErrorRate     float64       `json:"error_rate"`
	Duration      time.Duration `json:"duration"`
	QueriesPerSec float64       `json:"queries_per_sec"`
	BytesPerSec   float64       `json:"bytes_per_sec"`
	MBPerSec      float64       `json:"mb_per_sec"`
	AvgLatencyUs  float64       `json:"avg_latency_us"`
	MinLatencyUs  float64       `json:"min_latency_us"`
	MaxLatencyUs  float64       `json:"max_latency_us"`
	P50LatencyUs  float64       `json:"p50_latency_us"`
	P95LatencyUs  float64       `json:"p95_latency_us"`
	P99LatencyUs  float64       `json:"p99_latency_us"`
	SampleErrors  []string      `json:"sample_errors,omitempty"`
	TraceFile     string        `json:"trace_file,omitempty"`
	Timestamp     time.Time     `json:"timestamp"`
}

// RunConfig defines the benchmark run parameters
type RunConfig struct {
	Duration       time.Duration
	Warmup         time.Duration
	MaxConns       int
	Concurrency    int
	Rounds         int
	FlightRecorder bool
	FullTargets    bool // Include all target variants (TLS, session pooling, multiple GOMAXPROCS)
	Seed           int64

	// A/B comparison config (optional)
	Compare          *CompareConfig // Comparison target config
	CompareGomaxprocs int           // GOMAXPROCS for comparison target
}

// BenchmarkSuite manages the benchmark execution
type BenchmarkSuite struct {
	runCfg     RunConfig
	projectDir string
	outputDir  string
	results    []BenchmarkResult
	targets    []*BenchmarkTarget

	// Process management
	pglinkCmd    *exec.Cmd
	pgbouncerCmd *exec.Cmd

	// Ports
	directPort       int
	pglinkPort       int
	pgbouncerPort    int
	comparePort      int // Port for comparison pglink instance

	// Current pglink config (set during target setup)
	currentPglinkConfig *config.Config

	// Flight recorder
	pglinkTempDir string
	lastTraceFile string

	// TLS cert/key paths
	tlsCertPath string
	tlsKeyPath  string

	// Random source for deterministic workload generation
	rng *rand.Rand

	// Comparison target info (populated during setup)
	compareBinaryPath string
	compareCommitHash string
	compareLabel      string
}

// NewBenchmarkSuite creates a new benchmark suite
func NewBenchmarkSuite(cfg RunConfig, projectDir, outputDir string) *BenchmarkSuite {
	return &BenchmarkSuite{
		runCfg:        cfg,
		projectDir:    projectDir,
		outputDir:     outputDir,
		directPort:    15432,
		pglinkPort:    16432,
		pgbouncerPort: 16433,
		comparePort:   16434,
		rng:           rand.New(rand.NewSource(cfg.Seed)),
	}
}

// getCurrentCommit returns the short commit hash of HEAD
func (s *BenchmarkSuite) getCurrentCommit() string {
	cmd := exec.Command("git", "rev-parse", "--short", "HEAD")
	cmd.Dir = s.projectDir
	out, err := cmd.Output()
	if err != nil {
		return "unknown"
	}
	return strings.TrimSpace(string(out))
}

// ============================================================================
// Target Factory Functions
// ============================================================================

// makeDirectTarget creates a target for direct postgres connection
func makeDirectTarget() *BenchmarkTarget {
	return &BenchmarkTarget{
		Name: "direct",
		ConnString: func(s *BenchmarkSuite) string {
			return fmt.Sprintf("postgres://app:app_password@localhost:%d/uno?sslmode=disable", s.directPort)
		},
	}
}

// makePgbouncerTarget creates a target for pgbouncer (transaction pooling, no TLS)
func makePgbouncerTarget() *BenchmarkTarget {
	return &BenchmarkTarget{
		Name: "pgbouncer",
		SetUp: func(ctx context.Context, s *BenchmarkSuite) error {
			return s.startPgbouncerWithOpts(ctx, s.currentPglinkConfig,
				pgbouncerOpts{poolMode: pgbouncer.PoolModeTransaction, useTLS: false})
		},
		TearDown: func(ctx context.Context, s *BenchmarkSuite) error {
			s.stopPgbouncer()
			return nil
		},
		ConnString: func(s *BenchmarkSuite) string {
			return fmt.Sprintf("postgres://app:app_password@localhost:%d/alpha_uno?sslmode=disable", s.pgbouncerPort)
		},
	}
}

// makePglinkTarget creates a target for pglink with specified GOMAXPROCS
func makePglinkTarget(gomaxprocs int) *BenchmarkTarget {
	return makePglinkTargetWithLabel(gomaxprocs, "")
}

// makePglinkTargetWithLabel creates a target for pglink with specified GOMAXPROCS and optional commit label
func makePglinkTargetWithLabel(gomaxprocs int, commitLabel string) *BenchmarkTarget {
	var name string
	if commitLabel != "" {
		name = fmt.Sprintf("pglink (current @ %s)", commitLabel)
	} else if gomaxprocs > 0 {
		name = fmt.Sprintf("pglink (GOMAXPROCS=%d)", gomaxprocs)
	} else {
		name = "pglink"
	}
	return &BenchmarkTarget{
		Name: name,
		SetUp: func(ctx context.Context, s *BenchmarkSuite) error {
			return s.startPglinkWithOpts(ctx, s.currentPglinkConfig,
				pglinkOpts{gomaxprocs: gomaxprocs, useTLS: false})
		},
		TearDown: func(ctx context.Context, s *BenchmarkSuite) error {
			s.stopPglink()
			return nil
		},
		ConnString: func(s *BenchmarkSuite) string {
			return fmt.Sprintf("postgres://app:app_password@localhost:%d/alpha_uno?sslmode=disable", s.pglinkPort)
		},
	}
}

// Extended targets (used when -full-targets is set)

func makePgbouncerTLSTarget() *BenchmarkTarget {
	return &BenchmarkTarget{
		Name: "pgbouncer (tls)",
		SetUp: func(ctx context.Context, s *BenchmarkSuite) error {
			return s.startPgbouncerWithOpts(ctx, s.currentPglinkConfig,
				pgbouncerOpts{poolMode: pgbouncer.PoolModeTransaction, useTLS: true})
		},
		TearDown: func(ctx context.Context, s *BenchmarkSuite) error {
			s.stopPgbouncer()
			return nil
		},
		ConnString: func(s *BenchmarkSuite) string {
			return fmt.Sprintf("postgres://app:app_password@localhost:%d/alpha_uno?sslmode=require", s.pgbouncerPort)
		},
	}
}

func makePgbouncerSessionTarget() *BenchmarkTarget {
	return &BenchmarkTarget{
		Name: "pgbouncer (session)",
		SetUp: func(ctx context.Context, s *BenchmarkSuite) error {
			return s.startPgbouncerWithOpts(ctx, s.currentPglinkConfig,
				pgbouncerOpts{poolMode: pgbouncer.PoolModeSession, useTLS: false})
		},
		TearDown: func(ctx context.Context, s *BenchmarkSuite) error {
			s.stopPgbouncer()
			return nil
		},
		ConnString: func(s *BenchmarkSuite) string {
			return fmt.Sprintf("postgres://app:app_password@localhost:%d/alpha_uno?sslmode=disable", s.pgbouncerPort)
		},
	}
}

func makePglinkTLSTarget(gomaxprocs int) *BenchmarkTarget {
	return &BenchmarkTarget{
		Name: fmt.Sprintf("pglink (tls, GOMAXPROCS=%d)", gomaxprocs),
		SetUp: func(ctx context.Context, s *BenchmarkSuite) error {
			return s.startPglinkWithOpts(ctx, s.currentPglinkConfig,
				pglinkOpts{gomaxprocs: gomaxprocs, useTLS: true})
		},
		TearDown: func(ctx context.Context, s *BenchmarkSuite) error {
			s.stopPglink()
			return nil
		},
		ConnString: func(s *BenchmarkSuite) string {
			return fmt.Sprintf("postgres://app:app_password@localhost:%d/alpha_uno?sslmode=require", s.pglinkPort)
		},
	}
}

// buildTargets creates the target list based on configuration
func (s *BenchmarkSuite) buildTargets() []*BenchmarkTarget {
	var targets []*BenchmarkTarget

	// Get current commit for labeling when comparison is enabled
	currentCommit := ""
	if s.compareBinaryPath != "" {
		currentCommit = s.getCurrentCommit()
	}

	if s.runCfg.FullTargets {
		targets = []*BenchmarkTarget{
			makeDirectTarget(),
			makePgbouncerTarget(),
			makePgbouncerTLSTarget(),
			makePgbouncerSessionTarget(),
			makePglinkTargetWithLabel(4, currentCommit),
			makePglinkTargetWithLabel(8, currentCommit),
			makePglinkTargetWithLabel(16, currentCommit),
			makePglinkTLSTarget(16),
		}
	} else {
		// Default: minimal set
		targets = []*BenchmarkTarget{
			makeDirectTarget(),
			makePgbouncerTarget(),
			makePglinkTargetWithLabel(16, currentCommit),
		}
	}

	// Add comparison target if configured
	if s.compareBinaryPath != "" {
		targets = append(targets, makePglinkCompareTarget(
			s.compareBinaryPath,
			s.comparePort,
			s.runCfg.CompareGomaxprocs,
			s.compareLabel,
		))
	}

	return targets
}

// ============================================================================
// A/B Comparison Targets
// ============================================================================

// CompareConfig describes a comparison target for A/B testing
type CompareConfig struct {
	// One of these must be set:
	Ref        string // Git ref (branch, tag, commit) - auto-creates worktree
	Worktree   string // Path to existing worktree
	BinaryPath string // Path to pre-built binary

	// Optional:
	Label string // Custom label (default: derived from ref/path)
}

// WorktreeManager handles git worktree creation and cleanup
type WorktreeManager struct {
	projectDir  string
	worktreeDir string   // out/worktrees
	created     []string // Worktrees we created (for cleanup)
}

// NewWorktreeManager creates a new WorktreeManager
func NewWorktreeManager(projectDir string) *WorktreeManager {
	return &WorktreeManager{
		projectDir:  projectDir,
		worktreeDir: filepath.Join(projectDir, "out", "worktrees"),
	}
}

// ResolveRef resolves a git ref to a full commit hash
func (m *WorktreeManager) ResolveRef(ref string) (commitHash string, err error) {
	cmd := exec.Command("git", "rev-parse", ref)
	cmd.Dir = m.projectDir
	out, err := cmd.Output()
	if err != nil {
		return "", fmt.Errorf("failed to resolve ref %q: %w", ref, err)
	}
	return strings.TrimSpace(string(out)), nil
}

// ShortHash returns the first 7 characters of a commit hash
func ShortHash(commitHash string) string {
	if len(commitHash) > 7 {
		return commitHash[:7]
	}
	return commitHash
}

// GetCurrentCommit returns the current HEAD commit hash
func (m *WorktreeManager) GetCurrentCommit() (string, error) {
	return m.ResolveRef("HEAD")
}

// CachedBinaryPath returns the path where a cached binary for a commit would be stored
func (m *WorktreeManager) CachedBinaryPath(commitHash string) string {
	return filepath.Join(m.projectDir, "out", fmt.Sprintf("pglink-%s", ShortHash(commitHash)))
}

// CreateWorktree creates a git worktree for the given ref and returns the path and commit hash
func (m *WorktreeManager) CreateWorktree(ref string) (worktreePath string, commitHash string, err error) {
	// Resolve ref to commit hash
	commitHash, err = m.ResolveRef(ref)
	if err != nil {
		return "", "", err
	}

	// Create worktree directory
	if err := os.MkdirAll(m.worktreeDir, 0755); err != nil {
		return "", "", fmt.Errorf("failed to create worktree dir: %w", err)
	}

	// Create unique worktree path
	worktreePath = filepath.Join(m.worktreeDir, ShortHash(commitHash))

	// Check if worktree already exists
	if _, err := os.Stat(worktreePath); err == nil {
		// Worktree exists, remove it first
		removeCmd := exec.Command("git", "worktree", "remove", "--force", worktreePath)
		removeCmd.Dir = m.projectDir
		_ = removeCmd.Run() // Ignore errors
	}

	// Create worktree
	cmd := exec.Command("git", "worktree", "add", "--detach", worktreePath, commitHash)
	cmd.Dir = m.projectDir
	if out, err := cmd.CombinedOutput(); err != nil {
		return "", "", fmt.Errorf("failed to create worktree: %w\n%s", err, out)
	}

	m.created = append(m.created, worktreePath)
	return worktreePath, commitHash, nil
}

// BuildInWorktree builds pglink in the worktree and copies the binary to the output path
func (m *WorktreeManager) BuildInWorktree(worktreePath string, outputBinary string) error {
	// Check if bin/build exists
	buildScript := filepath.Join(worktreePath, "bin", "build")
	if _, err := os.Stat(buildScript); os.IsNotExist(err) {
		return fmt.Errorf("bin/build not found in worktree at %s - use --compare-binary for old code", worktreePath)
	}

	// Run bin/build
	cmd := exec.Command(buildScript)
	cmd.Dir = worktreePath
	if out, err := cmd.CombinedOutput(); err != nil {
		return fmt.Errorf("build failed in worktree: %w\n%s", err, out)
	}

	// Copy the built binary to the output path
	srcBinary := filepath.Join(worktreePath, "out", "pglink")
	if _, err := os.Stat(srcBinary); os.IsNotExist(err) {
		return fmt.Errorf("build did not produce out/pglink in worktree")
	}

	// Copy file
	src, err := os.Open(srcBinary)
	if err != nil {
		return fmt.Errorf("failed to open built binary: %w", err)
	}
	defer src.Close()

	dst, err := os.OpenFile(outputBinary, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0755)
	if err != nil {
		return fmt.Errorf("failed to create output binary: %w", err)
	}
	defer dst.Close()

	if _, err := io.Copy(dst, src); err != nil {
		return fmt.Errorf("failed to copy binary: %w", err)
	}

	return nil
}

// Cleanup removes all worktrees we created
func (m *WorktreeManager) Cleanup() {
	for _, path := range m.created {
		cmd := exec.Command("git", "worktree", "remove", "--force", path)
		cmd.Dir = m.projectDir
		_ = cmd.Run() // Ignore errors
	}
	m.created = nil
}

// PrepareCompareTarget prepares a comparison target and returns the binary path, commit hash, and label
func (s *BenchmarkSuite) PrepareCompareTarget(cfg CompareConfig) (binaryPath, commitHash, label string, err error) {
	wm := NewWorktreeManager(s.projectDir)
	defer wm.Cleanup()

	switch {
	case cfg.BinaryPath != "":
		// Pre-built binary - just use it directly
		if _, err := os.Stat(cfg.BinaryPath); os.IsNotExist(err) {
			return "", "", "", fmt.Errorf("binary not found: %s", cfg.BinaryPath)
		}
		binaryPath = cfg.BinaryPath
		label = cfg.Label
		if label == "" {
			label = filepath.Base(cfg.BinaryPath)
		}
		return binaryPath, "", label, nil

	case cfg.Worktree != "":
		// Existing worktree - build from it
		if _, err := os.Stat(cfg.Worktree); os.IsNotExist(err) {
			return "", "", "", fmt.Errorf("worktree not found: %s", cfg.Worktree)
		}

		// Get commit hash from worktree
		cmd := exec.Command("git", "rev-parse", "HEAD")
		cmd.Dir = cfg.Worktree
		out, err := cmd.Output()
		if err != nil {
			return "", "", "", fmt.Errorf("failed to get commit from worktree: %w", err)
		}
		commitHash = strings.TrimSpace(string(out))

		// Check if cached binary exists
		cachedPath := wm.CachedBinaryPath(commitHash)
		if _, err := os.Stat(cachedPath); err == nil {
			log.Printf("Using cached binary for %s: %s", ShortHash(commitHash), cachedPath)
			binaryPath = cachedPath
		} else {
			// Build from worktree
			log.Printf("Building from worktree %s (commit %s)...", cfg.Worktree, ShortHash(commitHash))
			if err := wm.BuildInWorktree(cfg.Worktree, cachedPath); err != nil {
				return "", "", "", err
			}
			binaryPath = cachedPath
		}

		label = cfg.Label
		if label == "" {
			label = ShortHash(commitHash)
		}
		return binaryPath, commitHash, label, nil

	case cfg.Ref != "":
		// Git ref - resolve, create worktree, build
		commitHash, err = wm.ResolveRef(cfg.Ref)
		if err != nil {
			return "", "", "", err
		}

		// Check if cached binary exists
		cachedPath := wm.CachedBinaryPath(commitHash)
		if _, err := os.Stat(cachedPath); err == nil {
			log.Printf("Using cached binary for %s (%s): %s", cfg.Ref, ShortHash(commitHash), cachedPath)
			binaryPath = cachedPath
		} else {
			// Create worktree and build
			log.Printf("Creating worktree for %s (%s)...", cfg.Ref, ShortHash(commitHash))
			worktreePath, _, err := wm.CreateWorktree(cfg.Ref)
			if err != nil {
				return "", "", "", err
			}

			log.Printf("Building in worktree...")
			if err := wm.BuildInWorktree(worktreePath, cachedPath); err != nil {
				return "", "", "", err
			}
			binaryPath = cachedPath
		}

		label = cfg.Label
		if label == "" {
			// Use ref name if it's a branch/tag, otherwise use short hash
			if cfg.Ref != commitHash && !strings.HasPrefix(cfg.Ref, commitHash[:7]) {
				label = fmt.Sprintf("%s @ %s", cfg.Ref, ShortHash(commitHash))
			} else {
				label = ShortHash(commitHash)
			}
		}
		return binaryPath, commitHash, label, nil

	default:
		return "", "", "", fmt.Errorf("CompareConfig must have Ref, Worktree, or BinaryPath set")
	}
}

// makePglinkCompareTarget creates a benchmark target for a comparison pglink binary
func makePglinkCompareTarget(binaryPath string, port int, gomaxprocs int, label string) *BenchmarkTarget {
	return &BenchmarkTarget{
		Name: fmt.Sprintf("pglink (%s)", label),
		SetUp: func(ctx context.Context, s *BenchmarkSuite) error {
			return s.startPglinkWithOpts(ctx, s.currentPglinkConfig,
				pglinkOpts{
					gomaxprocs: gomaxprocs,
					useTLS:     false,
					binaryPath: binaryPath,
					port:       port,
					label:      label,
				})
		},
		TearDown: func(ctx context.Context, s *BenchmarkSuite) error {
			s.stopPglink()
			return nil
		},
		ConnString: func(s *BenchmarkSuite) string {
			return fmt.Sprintf("postgres://app:app_password@localhost:%d/alpha_uno?sslmode=disable", port)
		},
	}
}

// ============================================================================
// Mixed Workload Generation
// ============================================================================

// WorkloadQuery represents a single query in the mixed workload
type WorkloadQuery struct {
	SQL        string
	IsWrite    bool
	InTxn      bool // Part of a multi-statement transaction
	TxnStart   bool // BEGIN
	TxnEnd     bool // COMMIT
	ExpectRows int  // Expected row count (0 for writes)
}

// generateMixedWorkload creates a deterministic workload with various query types.
// Uses only SELECT queries with generated data to avoid needing DDL permissions.
// Includes explicit transactions of varying lengths.
func generateMixedWorkload(rng *rand.Rand, count int) []WorkloadQuery {
	queries := make([]WorkloadQuery, 0, count)

	i := 0
	for i < count {
		r := rng.Float64()

		if r < 0.5 {
			// 50%: Simple SELECT with varying row counts
			rows := rng.Intn(100) + 1
			queries = append(queries, WorkloadQuery{
				SQL:        fmt.Sprintf("SELECT generate_series(1, %d)", rows),
				IsWrite:    false,
				ExpectRows: rows,
			})
			i++
		} else if r < 0.7 {
			// 20%: SELECT with computation (simulates more complex queries)
			n := rng.Intn(1000) + 100
			queries = append(queries, WorkloadQuery{
				SQL:        fmt.Sprintf("SELECT i, i*2, i::text FROM generate_series(1, %d) AS i", n),
				IsWrite:    false,
				ExpectRows: n,
			})
			i++
		} else if r < 0.85 {
			// 15%: Read transaction (2-5 queries)
			txnLen := rng.Intn(4) + 2
			if i+txnLen+2 > count {
				txnLen = count - i - 2
				if txnLen < 1 {
					continue
				}
			}
			queries = append(queries, WorkloadQuery{SQL: "BEGIN", TxnStart: true, InTxn: true})
			i++
			for j := 0; j < txnLen; j++ {
				rows := rng.Intn(50) + 1
				queries = append(queries, WorkloadQuery{
					SQL:        fmt.Sprintf("SELECT generate_series(1, %d)", rows),
					IsWrite:    false,
					InTxn:      true,
					ExpectRows: rows,
				})
				i++
			}
			queries = append(queries, WorkloadQuery{SQL: "COMMIT", TxnEnd: true, InTxn: true})
			i++
		} else {
			// 15%: Longer transaction with mixed query sizes
			txnLen := rng.Intn(5) + 3
			if i+txnLen+2 > count {
				txnLen = count - i - 2
				if txnLen < 1 {
					continue
				}
			}
			queries = append(queries, WorkloadQuery{SQL: "BEGIN", TxnStart: true, InTxn: true})
			i++
			for j := 0; j < txnLen; j++ {
				rows := rng.Intn(200) + 10
				queries = append(queries, WorkloadQuery{
					SQL:        fmt.Sprintf("SELECT i, md5(i::text) FROM generate_series(1, %d) AS i", rows),
					IsWrite:    false,
					InTxn:      true,
					ExpectRows: rows,
				})
				i++
			}
			queries = append(queries, WorkloadQuery{SQL: "COMMIT", TxnEnd: true, InTxn: true})
			i++
		}
	}

	return queries
}

// ============================================================================
// Benchmark Cases
// ============================================================================

// COPY benchmark constants
const (
	copyRowCount      = 1000   // Rows per COPY operation
	copyBatchSize     = 5      // Number of COPY operations per task (to amortize overhead)
	mixedWorkloadSize = 100000 // Total queries in the mixed workload
	mixedBatchSize    = 20     // Queries per task for mixed workload
)

// Pre-generated workload and COPY data (set during suite setup)
var (
	mixedWorkload []WorkloadQuery
	copyData      []byte
	copyDataSize  int64
)

func (s *BenchmarkSuite) buildBenchmarkCases() []*BenchmarkCase {
	return []*BenchmarkCase{
		{
			Name:        "mixed",
			Title:       "Mixed Workload",
			Description: fmt.Sprintf("Various SELECT queries with transactions. %d queries/task, deterministic (seed=%d)", mixedBatchSize, s.runCfg.Seed),
		},
		{
			Name:        "copy_out",
			Title:       fmt.Sprintf("COPY OUT (%d rows × %d per task)", copyRowCount, copyBatchSize),
			Description: "COPY TO STDOUT benchmark using generated data",
		},
		{
			Name:        "copy_in",
			Title:       fmt.Sprintf("COPY IN (%d rows × %d per task)", copyRowCount, copyBatchSize),
			Description: "COPY FROM STDIN benchmark using temp tables",
		},
	}
}

// getTaskForCase returns the appropriate task function for a benchmark case
func getTaskForCase(bc *BenchmarkCase) func(ctx context.Context, s *BenchmarkSuite, pool *pgxpool.Pool, bc *BenchmarkCase) (TaskResult, error) {
	switch bc.Name {
	case "mixed":
		return taskMixedWorkload
	case "copy_out":
		return taskCopyOut
	case "copy_in":
		return taskCopyIn
	default:
		return taskMixedWorkload
	}
}

// taskMixedWorkload executes a batch of queries from the pre-generated workload
func taskMixedWorkload(ctx context.Context, s *BenchmarkSuite, pool *pgxpool.Pool, bc *BenchmarkCase) (TaskResult, error) {
	// Get a random starting point in the workload
	startIdx := s.rng.Intn(len(mixedWorkload) - mixedBatchSize)

	conn, err := pool.Acquire(ctx)
	if err != nil {
		return TaskResult{}, err
	}
	defer conn.Release()

	queriesExecuted := 0
	for i := 0; i < mixedBatchSize && startIdx+i < len(mixedWorkload); i++ {
		q := mixedWorkload[startIdx+i]

		if q.TxnStart || q.TxnEnd {
			_, err = conn.Exec(ctx, q.SQL)
		} else if q.ExpectRows > 0 {
			var rows pgx.Rows
			rows, err = conn.Query(ctx, q.SQL)
			if err != nil {
				return TaskResult{Queries: queriesExecuted}, err
			}
			for rows.Next() {
			}
			rows.Close()
			if err = rows.Err(); err != nil {
				return TaskResult{Queries: queriesExecuted}, err
			}
		} else {
			_, err = conn.Exec(ctx, q.SQL)
		}
		if err != nil {
			return TaskResult{Queries: queriesExecuted}, err
		}
		queriesExecuted++
	}

	return TaskResult{Queries: queriesExecuted}, nil
}

// taskCopyOut executes multiple COPY TO STDOUT operations
func taskCopyOut(ctx context.Context, s *BenchmarkSuite, pool *pgxpool.Pool, bc *BenchmarkCase) (TaskResult, error) {
	conn, err := pool.Acquire(ctx)
	if err != nil {
		return TaskResult{}, err
	}
	defer conn.Release()

	var totalBytes int64
	query := fmt.Sprintf(`COPY (
		SELECT i AS id, 'row_' || i::text AS name, i * 10 AS value
		FROM generate_series(1, %d) AS i
	) TO STDOUT`, copyRowCount)

	for i := 0; i < copyBatchSize; i++ {
		// Use a counting writer to track bytes
		cw := &countingWriter{}
		_, err = conn.Conn().PgConn().CopyTo(ctx, cw, query)
		if err != nil {
			return TaskResult{Queries: i, Bytes: totalBytes}, err
		}
		totalBytes += cw.count
	}

	return TaskResult{Queries: copyBatchSize, Bytes: totalBytes}, nil
}

// taskCopyIn executes multiple COPY FROM STDIN operations
func taskCopyIn(ctx context.Context, s *BenchmarkSuite, pool *pgxpool.Pool, bc *BenchmarkCase) (TaskResult, error) {
	conn, err := pool.Acquire(ctx)
	if err != nil {
		return TaskResult{}, err
	}
	defer conn.Release()

	// Create temp table for this session
	_, err = conn.Exec(ctx, `
		CREATE TEMP TABLE IF NOT EXISTS bench_copy_temp (
			id INTEGER,
			name TEXT,
			value INTEGER
		) ON COMMIT DELETE ROWS
	`)
	if err != nil {
		return TaskResult{}, err
	}

	var totalBytes int64
	for i := 0; i < copyBatchSize; i++ {
		tx, err := conn.Begin(ctx)
		if err != nil {
			return TaskResult{Queries: i, Bytes: totalBytes}, err
		}

		_, err = tx.Conn().PgConn().CopyFrom(ctx, strings.NewReader(string(copyData)), "COPY bench_copy_temp FROM STDIN")
		if err != nil {
			_ = tx.Rollback(ctx)
			return TaskResult{Queries: i, Bytes: totalBytes}, err
		}

		if err = tx.Commit(ctx); err != nil {
			return TaskResult{Queries: i, Bytes: totalBytes}, err
		}
		totalBytes += copyDataSize
	}

	return TaskResult{Queries: copyBatchSize, Bytes: totalBytes}, nil
}

// countingWriter counts bytes written to it
type countingWriter struct {
	count int64
}

func (w *countingWriter) Write(p []byte) (int, error) {
	w.count += int64(len(p))
	return len(p), nil
}

// formatLatency formats microseconds in human-readable form
func formatLatency(us float64) string {
	if us >= 1000000 {
		return fmt.Sprintf("%.2fs", us/1000000)
	} else if us >= 1000 {
		return fmt.Sprintf("%.1fms", us/1000)
	}
	return fmt.Sprintf("%.0fμs", us)
}

// ============================================================================
// Setup and Teardown
// ============================================================================

type pgbouncerOpts struct {
	poolMode pgbouncer.PoolMode
	useTLS   bool
}

type pglinkOpts struct {
	gomaxprocs int
	useTLS     bool
	binaryPath string // Path to pglink binary (empty = default out/pglink)
	port       int    // Port to use (0 = default s.pglinkPort)
	label      string // Label for this target (e.g., "current @ abc123")
}

func (s *BenchmarkSuite) generateCopyData() {
	var buf strings.Builder
	for i := 1; i <= copyRowCount; i++ {
		buf.WriteString(fmt.Sprintf("%d\trow_%d\t%d\n", i, i, i*10))
	}
	copyData = []byte(buf.String())
	copyDataSize = int64(len(copyData))
}

func (s *BenchmarkSuite) generateMixedWorkload() {
	// Use a seeded RNG for reproducibility
	rng := rand.New(rand.NewSource(s.runCfg.Seed))
	mixedWorkload = generateMixedWorkload(rng, mixedWorkloadSize)
	log.Printf("Generated mixed workload: %d queries (seed=%d)", len(mixedWorkload), s.runCfg.Seed)

	// Count query types for logging
	var selects, txnStarts int
	for _, q := range mixedWorkload {
		if q.TxnStart {
			txnStarts++
		} else if !q.TxnEnd {
			selects++
		}
	}
	log.Printf("  SELECT queries: %d, Transactions: %d", selects, txnStarts)
}

func (s *BenchmarkSuite) ensureDockerCompose(ctx context.Context) error {
	cmd := exec.CommandContext(ctx, "docker-compose", "up", "-d", "--wait")
	cmd.Dir = s.projectDir
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	return cmd.Run()
}

func (s *BenchmarkSuite) generatePglinkConfig() (*config.Config, error) {
	configPath := filepath.Join(s.projectDir, "pglink.json")
	cfg, err := config.ReadConfigFile(configPath)
	if err != nil {
		return nil, err
	}
	for _, dbCfg := range cfg.Databases {
		dbCfg.Backend.PoolMaxConns = int32(s.runCfg.MaxConns)
	}
	return cfg, nil
}

func (s *BenchmarkSuite) startPglinkWithOpts(ctx context.Context, cfg *config.Config, opts pglinkOpts) error {
	tmpDir, err := os.MkdirTemp("", "pglink-bench-*")
	if err != nil {
		return err
	}
	s.pglinkTempDir = tmpDir

	var args []string
	if s.runCfg.FlightRecorder {
		tracesDir := filepath.Join(tmpDir, "traces")
		if err := os.MkdirAll(tracesDir, 0755); err != nil {
			_ = os.RemoveAll(tmpDir)
			return fmt.Errorf("failed to create traces dir: %w", err)
		}
		minAge := s.runCfg.Duration + s.runCfg.Warmup + 30*time.Second
		args = append(args,
			"-flight-recorder", tracesDir,
			"-flight-recorder-min-age", minAge.String(),
			"-flight-recorder-max-bytes", "52428800",
		)
	}

	// Determine port
	port := opts.port
	if port == 0 {
		port = s.pglinkPort
	}

	cfgCopy := *cfg
	// Update listen address with the target port
	cfgCopy.Listen = config.ListenAddr(fmt.Sprintf(":%d", port))

	if opts.useTLS {
		certPath := "server.crt"
		keyPath := "server.key"
		cfgCopy.TLS = &config.JsonTLSConfig{
			SSLMode:            config.SSLModeAllow,
			CertPath:           certPath,
			CertPrivateKeyPath: keyPath,
			GenerateCert:       true,
		}
		s.tlsCertPath = filepath.Join(tmpDir, certPath)
		s.tlsKeyPath = filepath.Join(tmpDir, keyPath)
	} else {
		cfgCopy.TLS = &config.JsonTLSConfig{
			SSLMode: config.SSLModeDisable,
		}
		s.tlsCertPath = ""
		s.tlsKeyPath = ""
	}

	configPath := filepath.Join(tmpDir, "pglink.json")
	configData, err := json.MarshalIndent(&cfgCopy, "", "  ")
	if err != nil {
		_ = os.RemoveAll(tmpDir)
		return err
	}
	if err := os.WriteFile(configPath, configData, 0644); err != nil {
		_ = os.RemoveAll(tmpDir)
		return err
	}

	// Determine binary path
	pglinkBinary := opts.binaryPath
	if pglinkBinary == "" {
		pglinkBinary = filepath.Join(s.projectDir, "out", "pglink")
		if _, err := os.Stat(pglinkBinary); os.IsNotExist(err) {
			buildCmd := exec.CommandContext(ctx, filepath.Join(s.projectDir, "bin", "build"))
			buildCmd.Dir = s.projectDir
			if err := buildCmd.Run(); err != nil {
				_ = os.RemoveAll(tmpDir)
				return fmt.Errorf("failed to build pglink: %w", err)
			}
		}
	}

	cmdArgs := append([]string{"-config", configPath}, args...)
	s.pglinkCmd = exec.Command(pglinkBinary, cmdArgs...)
	s.pglinkCmd.Env = append(os.Environ(), fmt.Sprintf("GOMAXPROCS=%d", opts.gomaxprocs))
	s.pglinkCmd.Dir = tmpDir

	if err := s.pglinkCmd.Start(); err != nil {
		_ = os.RemoveAll(tmpDir)
		return fmt.Errorf("failed to start pglink: %w", err)
	}

	return s.waitForPort(ctx, port, 10*time.Second)
}

func (s *BenchmarkSuite) stopPglink() {
	s.lastTraceFile = ""

	if s.pglinkCmd != nil && s.pglinkCmd.Process != nil {
		if s.runCfg.FlightRecorder {
			_ = s.pglinkCmd.Process.Signal(syscall.SIGUSR1)
			time.Sleep(500 * time.Millisecond)
		}

		_ = s.pglinkCmd.Process.Signal(syscall.SIGTERM)
		done := make(chan error, 1)
		go func() { done <- s.pglinkCmd.Wait() }()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			_ = s.pglinkCmd.Process.Kill()
		}
		s.pglinkCmd = nil
	}

	if s.runCfg.FlightRecorder && s.pglinkTempDir != "" {
		tracesDir := filepath.Join(s.pglinkTempDir, "traces")
		files, err := os.ReadDir(tracesDir)
		if err == nil && len(files) > 0 {
			var latestFile string
			var latestTime time.Time
			for _, f := range files {
				if strings.HasSuffix(f.Name(), ".trace") {
					info, err := f.Info()
					if err == nil && info.ModTime().After(latestTime) {
						latestTime = info.ModTime()
						latestFile = f.Name()
					}
				}
			}
			if latestFile != "" {
				srcPath := filepath.Join(tracesDir, latestFile)
				destDir := filepath.Join(s.outputDir, "traces")
				_ = os.MkdirAll(destDir, 0755)
				destPath := filepath.Join(destDir, latestFile)

				if src, err := os.Open(srcPath); err == nil {
					if dst, err := os.Create(destPath); err == nil {
						_, _ = io.Copy(dst, src)
						_ = dst.Close()
						s.lastTraceFile = destPath
						log.Printf("    Trace captured: %s", destPath)
					}
					_ = src.Close()
				}
			}
		}
		_ = os.RemoveAll(s.pglinkTempDir)
		s.pglinkTempDir = ""
	}
}

func (s *BenchmarkSuite) startPgbouncerWithOpts(ctx context.Context, cfg *config.Config, opts pgbouncerOpts) error {
	tmpDir, err := os.MkdirTemp("", "pgbouncer-bench-*")
	if err != nil {
		return err
	}

	pgbOpts := pgbouncer.Options{
		PoolMode: opts.poolMode,
	}

	if opts.useTLS {
		certPath := filepath.Join(tmpDir, "server.crt")
		keyPath := filepath.Join(tmpDir, "server.key")
		if err := generateSelfSignedCert(certPath, keyPath); err != nil {
			_ = os.RemoveAll(tmpDir)
			return fmt.Errorf("failed to generate TLS cert for pgbouncer: %w", err)
		}
		pgbOpts.TLS = &pgbouncer.TLSConfig{
			CertPath: certPath,
			KeyPath:  keyPath,
		}
	}

	secrets := config.NewSecretCache(nil)
	pgbCfg, err := pgbouncer.GenerateConfigWithOptions(ctx, cfg, secrets, s.pgbouncerPort, pgbOpts)
	if err != nil {
		_ = os.RemoveAll(tmpDir)
		return fmt.Errorf("failed to generate pgbouncer config: %w", err)
	}
	if err := pgbCfg.WriteToDir(tmpDir); err != nil {
		_ = os.RemoveAll(tmpDir)
		return fmt.Errorf("failed to write pgbouncer config: %w", err)
	}

	s.pgbouncerCmd = exec.Command("pgbouncer", filepath.Join(tmpDir, "pgbouncer.ini"))
	s.pgbouncerCmd.Dir = tmpDir

	if err := s.pgbouncerCmd.Start(); err != nil {
		_ = os.RemoveAll(tmpDir)
		return fmt.Errorf("failed to start pgbouncer: %w", err)
	}

	return s.waitForPort(ctx, s.pgbouncerPort, 10*time.Second)
}

func generateSelfSignedCert(certPath, keyPath string) (err error) {
	cert, err := config.GenerateSelfSignedCertForBenchmark()
	if err != nil {
		return err
	}

	certFile, err := os.Create(certPath)
	if err != nil {
		return err
	}
	defer func() {
		if cerr := certFile.Close(); cerr != nil && err == nil {
			err = cerr
		}
	}()

	for _, certBytes := range cert.Certificate {
		if err := pem.Encode(certFile, &pem.Block{Type: "CERTIFICATE", Bytes: certBytes}); err != nil {
			return err
		}
	}

	keyFile, err := os.OpenFile(keyPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0600)
	if err != nil {
		return err
	}
	defer func() {
		if kerr := keyFile.Close(); kerr != nil && err == nil {
			err = kerr
		}
	}()

	privKeyBytes, err := config.MarshalPrivateKeyForBenchmark(cert.PrivateKey)
	if err != nil {
		return err
	}

	return pem.Encode(keyFile, &pem.Block{Type: "EC PRIVATE KEY", Bytes: privKeyBytes})
}

func (s *BenchmarkSuite) stopPgbouncer() {
	if s.pgbouncerCmd != nil && s.pgbouncerCmd.Process != nil {
		_ = s.pgbouncerCmd.Process.Signal(syscall.SIGTERM)
		done := make(chan error, 1)
		go func() { done <- s.pgbouncerCmd.Wait() }()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			_ = s.pgbouncerCmd.Process.Kill()
		}
		s.pgbouncerCmd = nil
	}
}

func (s *BenchmarkSuite) waitForPort(ctx context.Context, port int, timeout time.Duration) error {
	connStr := fmt.Sprintf("postgres://app:app_password@localhost:%d/alpha_uno?sslmode=disable&connect_timeout=1", port)
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		conn, err := pgx.Connect(ctx, connStr)
		if err == nil {
			_ = conn.Close(ctx)
			// Give the server time to clean up the connection
			time.Sleep(500 * time.Millisecond)
			return nil
		}
		time.Sleep(100 * time.Millisecond)
	}
	return fmt.Errorf("timeout waiting for port %d", port)
}

// ============================================================================
// Benchmark Runner
// ============================================================================

func (s *BenchmarkSuite) Run(ctx context.Context) error {
	if err := os.MkdirAll(s.outputDir, 0755); err != nil {
		return fmt.Errorf("failed to create output dir: %w", err)
	}

	log.Println("Ensuring docker-compose is running...")
	if err := s.ensureDockerCompose(ctx); err != nil {
		return fmt.Errorf("docker-compose setup failed: %w", err)
	}

	log.Println("Generating workloads...")
	s.generateCopyData()
	s.generateMixedWorkload()

	cfg, err := s.generatePglinkConfig()
	if err != nil {
		return fmt.Errorf("failed to generate pglink config: %w", err)
	}
	s.currentPglinkConfig = cfg

	// Prepare comparison target if configured
	if s.runCfg.Compare != nil {
		log.Println("Preparing comparison target...")
		binaryPath, commitHash, label, err := s.PrepareCompareTarget(*s.runCfg.Compare)
		if err != nil {
			return fmt.Errorf("failed to prepare comparison target: %w", err)
		}
		s.compareBinaryPath = binaryPath
		s.compareCommitHash = commitHash
		s.compareLabel = label
		log.Printf("Comparison target ready: %s", label)
	}

	s.targets = s.buildTargets()

	cases := s.buildBenchmarkCases()
	rounds := s.runCfg.Rounds
	if rounds < 1 {
		rounds = 1
	}

	log.Printf("Running %d benchmark cases × %d targets × %d rounds", len(cases), len(s.targets), rounds)
	log.Printf("Each round: %s warmup + %s measurement", s.runCfg.Warmup, s.runCfg.Duration)
	log.Printf("Pool: max_conns=%d, concurrency=%d", s.runCfg.MaxConns, s.runCfg.Concurrency)

	for _, bc := range cases {
		log.Printf("\n=== %s ===", bc.Title)

		// Case setup
		if bc.SetUp != nil {
			if err := bc.SetUp(ctx, s); err != nil {
				log.Printf("Warning: case setup failed: %v", err)
				continue
			}
		}

		// Get task function for this case
		taskFn := getTaskForCase(bc)

		// Collect results from all rounds
		targetResults := make(map[string][]BenchmarkResult)

		for round := 1; round <= rounds; round++ {
			// Shuffle targets for this round
			shuffledTargets := make([]*BenchmarkTarget, len(s.targets))
			copy(shuffledTargets, s.targets)
			rand.Shuffle(len(shuffledTargets), func(i, j int) {
				shuffledTargets[i], shuffledTargets[j] = shuffledTargets[j], shuffledTargets[i]
			})

			log.Printf("  Round %d/%d", round, rounds)

			for _, target := range shuffledTargets {
				result, err := s.runTarget(ctx, bc, target, taskFn)
				if err != nil {
					log.Printf("    %s: SETUP ERROR: %v", target.Name, err)
					continue
				}
				targetResults[target.Name] = append(targetResults[target.Name], result)

				// Format output based on whether we have bytes
				errPct := result.ErrorRate * 100
				errStr := ""
				if errPct > 0 {
					errStr = fmt.Sprintf(", err=%.1f%%", errPct)
				}

				if result.TotalBytes > 0 {
					log.Printf("    %s: %.0f qps, %.1f MB/s, p50=%s, p99=%s%s",
						target.Name, result.QueriesPerSec, result.MBPerSec,
						formatLatency(result.P50LatencyUs), formatLatency(result.P99LatencyUs), errStr)
				} else {
					log.Printf("    %s: %.0f qps, p50=%s, p99=%s%s",
						target.Name, result.QueriesPerSec,
						formatLatency(result.P50LatencyUs), formatLatency(result.P99LatencyUs), errStr)
				}

				// Log sample errors if error rate is high
				if errPct >= 10 && len(result.SampleErrors) > 0 {
					log.Printf("      Sample errors: %v", result.SampleErrors[:min(3, len(result.SampleErrors))])
				}

				// Warn on very high error rate
				if errPct >= 50 {
					log.Printf("    ⚠️  HIGH ERROR RATE: %.1f%% - this may indicate a bug", errPct)
				}
			}
		}

		// Aggregate results across rounds
		for _, target := range s.targets {
			results := targetResults[target.Name]
			if len(results) == 0 {
				continue
			}
			aggregated := aggregateResults(results, bc.Title)
			s.results = append(s.results, aggregated)
		}

		// Case teardown
		if bc.TearDown != nil {
			if err := bc.TearDown(ctx, s); err != nil {
				log.Printf("Warning: case teardown failed: %v", err)
			}
		}
	}

	return s.writeResults()
}

func aggregateResults(results []BenchmarkResult, caseTitle string) BenchmarkResult {
	if len(results) == 0 {
		return BenchmarkResult{}
	}
	if len(results) == 1 {
		return results[0]
	}

	agg := BenchmarkResult{
		CaseTitle:   caseTitle,
		TargetName:  results[0].TargetName,
		MaxConns:    results[0].MaxConns,
		Concurrency: results[0].Concurrency,
		Timestamp:   results[0].Timestamp,
	}

	var totalTasks, totalQueries, totalBytes, successCount, errorCount int64
	var totalDuration time.Duration
	var allErrors []string

	for _, r := range results {
		totalTasks += r.TotalTasks
		totalQueries += r.TotalQueries
		totalBytes += r.TotalBytes
		successCount += r.SuccessCount
		errorCount += r.ErrorCount
		totalDuration += r.Duration
		allErrors = append(allErrors, r.SampleErrors...)
		if r.TraceFile != "" {
			agg.TraceFile = r.TraceFile
		}
	}

	agg.TotalTasks = totalTasks
	agg.TotalQueries = totalQueries
	agg.TotalBytes = totalBytes
	agg.SuccessCount = successCount
	agg.ErrorCount = errorCount
	agg.Duration = totalDuration

	if totalTasks > 0 {
		agg.ErrorRate = float64(errorCount) / float64(totalTasks)
		agg.QueriesPerSec = float64(totalQueries) / totalDuration.Seconds()
	}
	if totalBytes > 0 {
		agg.BytesPerSec = float64(totalBytes) / totalDuration.Seconds()
		agg.MBPerSec = agg.BytesPerSec / (1024 * 1024)
	}

	// Average latency percentiles across rounds
	var sumAvg, sumMin, sumMax, sumP50, sumP95, sumP99 float64
	validCount := 0
	for _, r := range results {
		if r.AvgLatencyUs > 0 {
			sumAvg += r.AvgLatencyUs
			sumMin += r.MinLatencyUs
			sumMax += r.MaxLatencyUs
			sumP50 += r.P50LatencyUs
			sumP95 += r.P95LatencyUs
			sumP99 += r.P99LatencyUs
			validCount++
		}
	}
	if validCount > 0 {
		agg.AvgLatencyUs = sumAvg / float64(validCount)
		agg.MinLatencyUs = sumMin / float64(validCount)
		agg.MaxLatencyUs = sumMax / float64(validCount)
		agg.P50LatencyUs = sumP50 / float64(validCount)
		agg.P95LatencyUs = sumP95 / float64(validCount)
		agg.P99LatencyUs = sumP99 / float64(validCount)
	}

	// Deduplicate sample errors
	seen := make(map[string]bool)
	for _, e := range allErrors {
		if !seen[e] && len(agg.SampleErrors) < 10 {
			seen[e] = true
			agg.SampleErrors = append(agg.SampleErrors, e)
		}
	}

	return agg
}

func (s *BenchmarkSuite) runTarget(
	ctx context.Context,
	bc *BenchmarkCase,
	target *BenchmarkTarget,
	taskFn func(ctx context.Context, s *BenchmarkSuite, pool *pgxpool.Pool, bc *BenchmarkCase) (TaskResult, error),
) (result BenchmarkResult, err error) {
	result = BenchmarkResult{
		CaseTitle:   bc.Title,
		TargetName:  target.Name,
		MaxConns:    s.runCfg.MaxConns,
		Concurrency: s.runCfg.Concurrency,
		Timestamp:   time.Now(),
	}

	// Assign the task function to target temporarily
	target.Task = func(ctx context.Context, s *BenchmarkSuite, pool *pgxpool.Pool, bc *BenchmarkCase) (TaskResult, error) {
		return taskFn(ctx, s, pool, bc)
	}

	// Target setup
	if target.SetUp != nil {
		if err := target.SetUp(ctx, s); err != nil {
			return result, fmt.Errorf("target setup failed: %w", err)
		}
	}

	defer func() {
		if target.TearDown != nil {
			_ = target.TearDown(ctx, s)
		}
		if s.lastTraceFile != "" {
			result.TraceFile = s.lastTraceFile
		}
	}()

	// Create connection pool
	var pool *pgxpool.Pool
	if target.ConnString != nil {
		connStr := target.ConnString(s)
		poolConfig, err := pgxpool.ParseConfig(connStr)
		if err != nil {
			return result, err
		}
		poolConfig.MaxConns = int32(s.runCfg.Concurrency)
		poolConfig.MinConns = int32(s.runCfg.Concurrency)
		// Use default extended protocol (prepare + execute)
		poolConfig.ConnConfig.DefaultQueryExecMode = pgx.QueryExecModeExec

		pool, err = pgxpool.NewWithConfig(ctx, poolConfig)
		if err != nil {
			return result, err
		}
		defer pool.Close()
	}

	// Warmup
	warmupCtx, warmupCancel := context.WithTimeout(ctx, s.runCfg.Warmup)
	s.runLoadForDuration(warmupCtx, pool, target, bc, nil)
	warmupCancel()

	// Benchmark
	var successCount, errorCount, totalQueries, totalBytes atomic.Int64
	latencies := make([]int64, 0, 100000)
	latenciesMu := sync.Mutex{}
	errorsMu := sync.Mutex{}
	var sampleErrors []string

	benchCtx, benchCancel := context.WithTimeout(ctx, s.runCfg.Duration)
	start := time.Now()

	s.runLoadForDuration(benchCtx, pool, target, bc,
		func(tr TaskResult, latency time.Duration, err error) {
			latenciesMu.Lock()
			latencies = append(latencies, latency.Nanoseconds())
			latenciesMu.Unlock()

			// Always count partial progress from the task
			totalQueries.Add(int64(tr.Queries))
			totalBytes.Add(tr.Bytes)

			if err != nil {
				errorCount.Add(1)
				errorsMu.Lock()
				if len(sampleErrors) < 10 {
					sampleErrors = append(sampleErrors, err.Error())
				}
				errorsMu.Unlock()
			} else {
				successCount.Add(1)
			}
		},
	)

	benchCancel()
	elapsed := time.Since(start)

	// Calculate results
	result.TotalTasks = successCount.Load() + errorCount.Load()
	result.TotalQueries = totalQueries.Load()
	result.TotalBytes = totalBytes.Load()
	result.SuccessCount = successCount.Load()
	result.ErrorCount = errorCount.Load()
	result.Duration = elapsed
	result.SampleErrors = sampleErrors

	if result.TotalTasks > 0 {
		result.ErrorRate = float64(result.ErrorCount) / float64(result.TotalTasks)
		result.QueriesPerSec = float64(result.TotalQueries) / elapsed.Seconds()
	}
	if result.TotalBytes > 0 {
		result.BytesPerSec = float64(result.TotalBytes) / elapsed.Seconds()
		result.MBPerSec = result.BytesPerSec / (1024 * 1024)
	}

	if len(latencies) > 0 {
		sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
		var total int64
		for _, l := range latencies {
			total += l
		}
		result.AvgLatencyUs = float64(total) / float64(len(latencies)) / 1000.0
		result.MinLatencyUs = float64(latencies[0]) / 1000.0
		result.MaxLatencyUs = float64(latencies[len(latencies)-1]) / 1000.0
		result.P50LatencyUs = float64(latencies[len(latencies)*50/100]) / 1000.0
		result.P95LatencyUs = float64(latencies[len(latencies)*95/100]) / 1000.0
		result.P99LatencyUs = float64(latencies[len(latencies)*99/100]) / 1000.0
	}

	return result, nil
}

func (s *BenchmarkSuite) runLoadForDuration(
	ctx context.Context,
	pool *pgxpool.Pool,
	target *BenchmarkTarget,
	bc *BenchmarkCase,
	onComplete func(tr TaskResult, latency time.Duration, err error),
) {
	concurrency := s.runCfg.Concurrency

	var wg sync.WaitGroup
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				default:
					queryStart := time.Now()
					queryCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
					tr, err := target.Task(queryCtx, s, pool, bc)
					cancel()
					latency := time.Since(queryStart)

					select {
					case <-ctx.Done():
						return
					default:
						if onComplete != nil {
							onComplete(tr, latency, err)
						}
					}
				}
			}
		}()
	}

	<-ctx.Done()

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(100 * time.Millisecond):
	}
}

// ============================================================================
// Results Output
// ============================================================================

func (s *BenchmarkSuite) writeResults() error {
	// Write JSON
	jsonPath := filepath.Join(s.outputDir, "results.json")
	jsonData, err := json.MarshalIndent(s.results, "", "  ")
	if err != nil {
		return err
	}
	if err := os.WriteFile(jsonPath, jsonData, 0644); err != nil {
		return err
	}
	log.Printf("Wrote results to %s", jsonPath)

	// Write markdown
	mdPath := filepath.Join(s.projectDir, "BENCHMARKS.md")
	if err := s.writeMarkdownReport(mdPath); err != nil {
		return err
	}
	log.Printf("Wrote report to %s", mdPath)

	return nil
}

func (s *BenchmarkSuite) writeMarkdownReport(path string) error {
	tmpl := `# pglink Benchmarks

Generated: {{.Timestamp}}

## Configuration

- **Rounds per target:** {{.Rounds}} (shuffled order to mitigate ordering effects)
- **Duration per round:** {{.Duration}} measurement + {{.Warmup}} warmup
- **Pool:** max_conns={{.MaxConns}}, concurrency={{.Concurrency}}
- **Workload seed:** {{.Seed}} (for reproducibility)

## Results
{{range .Cases}}
### {{.Title}}
{{if .Description}}
{{.Description}}
{{end}}
| Target | QPS | MB/s | Error Rate | P50 (μs) | P99 (μs) |
|--------|-----|------|------------|----------|----------|
{{- $baseline := index .Results 0}}
{{range $i, $r := .Results}}
| {{$r.TargetName}} | {{fmtQPS $r.QueriesPerSec $baseline.QueriesPerSec}} | {{fmtMBs $r.MBPerSec $baseline.MBPerSec}} | {{fmtErr $r.ErrorRate}} | {{fmtLatency $r.P50LatencyUs $baseline.P50LatencyUs}} | {{fmtLatency $r.P99LatencyUs $baseline.P99LatencyUs}} |
{{- end}}
{{end}}
## Analysis

High error rates typically indicate pool exhaustion (concurrency > max_conns) or backend overload.

{{if .ErrorSamples}}
### Sample Errors

{{range .ErrorSamples}}
- ` + "`{{.}}`" + `
{{end}}
{{end}}

## Test Environment

- Go version: {{.GoVersion}}
- OS: {{.OS}}/{{.Arch}}
- CPUs: {{.NumCPU}}
`

	funcMap := template.FuncMap{
		"fmtQPS": func(val, baseline float64) string {
			if val == 0 {
				return "0 ❌"
			}
			if baseline <= 0 || val == baseline {
				return fmt.Sprintf("%.0f", val)
			}
			ratio := val / baseline
			if ratio >= 0.98 {
				return fmt.Sprintf("%.0f", val)
			}
			slowdown := baseline / val
			if slowdown >= 2 {
				return fmt.Sprintf("%.0f (%.1fx slower)", val, slowdown)
			}
			pctSlower := (1 - ratio) * 100
			return fmt.Sprintf("%.0f (%.0f%% slower)", val, pctSlower)
		},
		"fmtMBs": func(val, baseline float64) string {
			if val <= 0 {
				return "-"
			}
			if baseline <= 0 || val == baseline {
				return fmt.Sprintf("%.1f", val)
			}
			ratio := val / baseline
			if ratio >= 0.98 {
				return fmt.Sprintf("%.1f", val)
			}
			slowdown := baseline / val
			if slowdown >= 2 {
				return fmt.Sprintf("%.1f (%.1fx slower)", val, slowdown)
			}
			pctSlower := (1 - ratio) * 100
			return fmt.Sprintf("%.1f (%.0f%% slower)", val, pctSlower)
		},
		// fmtLatency formats microseconds in human-readable form
		"fmtLatency": func(val, baseline float64) string {
			format := func(us float64) string {
				if us >= 1000000 {
					return fmt.Sprintf("%.2fs", us/1000000)
				} else if us >= 1000 {
					return fmt.Sprintf("%.1fms", us/1000)
				}
				return fmt.Sprintf("%.0fμs", us)
			}
			base := format(val)
			if baseline <= 0 || val == baseline {
				return base
			}
			ratio := val / baseline
			if ratio <= 1.02 {
				return base
			}
			if ratio >= 2 {
				return fmt.Sprintf("%s (%.1fx)", base, ratio)
			}
			pctSlower := (ratio - 1) * 100
			return fmt.Sprintf("%s (+%.0f%%)", base, pctSlower)
		},
		"fmtErr": func(rate float64) string {
			pct := rate * 100
			if pct == 0 {
				return "0%"
			}
			if pct < 0.01 {
				return "<0.01%"
			}
			if pct >= 50 {
				return fmt.Sprintf("**%.1f%%**", pct) // Bold for high error rates
			}
			return fmt.Sprintf("%.2f%%", pct)
		},
	}

	t, err := template.New("report").Funcs(funcMap).Parse(tmpl)
	if err != nil {
		return err
	}

	// Group results by case title
	caseMap := make(map[string][]BenchmarkResult)
	var caseOrder []string
	var errorSamples []string
	seen := make(map[string]bool)

	for _, r := range s.results {
		if _, exists := caseMap[r.CaseTitle]; !exists {
			caseOrder = append(caseOrder, r.CaseTitle)
		}
		caseMap[r.CaseTitle] = append(caseMap[r.CaseTitle], r)
		for _, e := range r.SampleErrors {
			if !seen[e] && len(errorSamples) < 20 {
				seen[e] = true
				errorSamples = append(errorSamples, e)
			}
		}
	}

	type CaseData struct {
		Title       string
		Description string
		Results     []BenchmarkResult
	}

	var cases []CaseData
	for _, title := range caseOrder {
		results := caseMap[title]
		cases = append(cases, CaseData{
			Title:   title,
			Results: results,
		})
	}

	data := struct {
		Timestamp    string
		Rounds       int
		Duration     string
		Warmup       string
		MaxConns     int
		Concurrency  int
		Seed         int64
		Cases        []CaseData
		ErrorSamples []string
		GoVersion    string
		OS           string
		Arch         string
		NumCPU       int
	}{
		Timestamp:    time.Now().Format(time.RFC3339),
		Rounds:       s.runCfg.Rounds,
		Duration:     s.runCfg.Duration.String(),
		Warmup:       s.runCfg.Warmup.String(),
		MaxConns:     s.runCfg.MaxConns,
		Concurrency:  s.runCfg.Concurrency,
		Seed:         s.runCfg.Seed,
		Cases:        cases,
		ErrorSamples: errorSamples,
		GoVersion:    runtime.Version(),
		OS:           runtime.GOOS,
		Arch:         runtime.GOARCH,
		NumCPU:       runtime.NumCPU(),
	}

	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()

	return t.Execute(f, data)
}

// ============================================================================
// Main
// ============================================================================

func main() {
	duration := flag.Duration("duration", 15*time.Second, "Duration of each benchmark measurement")
	warmup := flag.Duration("warmup", 5*time.Second, "Warmup duration before measuring")
	rounds := flag.Int("rounds", 2, "Number of rounds per target (mitigates ordering effects)")
	outputDir := flag.String("output", "out/benchmark", "Output directory for results")
	maxConns := flag.Int("max-conns", 100, "Max backend connections in pool")
	concurrency := flag.Int("concurrency", 100, "Number of concurrent workers")
	fullTargets := flag.Bool("full-targets", false, "Include all target variants (TLS, session pooling, GOMAXPROCS 4/8/16)")
	flightRecorder := flag.Bool("flight-recorder", false, "Enable flight recorder for pglink targets")
	seed := flag.Int64("seed", 0, "Random seed for workload generation (0 = use time-based seed)")

	// A/B comparison flags (mutually exclusive)
	compareRef := flag.String("compare-ref", "", "Git ref (branch/tag/commit) to compare against. Builds and caches binary.")
	compareWorktree := flag.String("compare-worktree", "", "Path to existing git worktree to compare against")
	compareBinary := flag.String("compare-binary", "", "Path to pre-built pglink binary to compare against")
	compareLabel := flag.String("compare-label", "", "Label for comparison target (default: derived from ref/path)")
	compareGomaxprocs := flag.Int("compare-gomaxprocs", 16, "GOMAXPROCS for comparison target")

	flag.Parse()

	// Generate deterministic seed if not provided
	if *seed == 0 {
		h := fnv.New64a()
		_, _ = h.Write([]byte(time.Now().Format("2006-01-02")))
		*seed = int64(h.Sum64())
	}

	// Validate comparison flags (mutually exclusive)
	compareCount := 0
	if *compareRef != "" {
		compareCount++
	}
	if *compareWorktree != "" {
		compareCount++
	}
	if *compareBinary != "" {
		compareCount++
	}
	if compareCount > 1 {
		log.Fatal("Only one of -compare-ref, -compare-worktree, or -compare-binary can be specified")
	}

	// Build comparison config if specified
	var compareCfg *CompareConfig
	if compareCount > 0 {
		compareCfg = &CompareConfig{
			Ref:        *compareRef,
			Worktree:   *compareWorktree,
			BinaryPath: *compareBinary,
			Label:      *compareLabel,
		}
	}

	cfg := RunConfig{
		Duration:          *duration,
		Warmup:            *warmup,
		Rounds:            *rounds,
		MaxConns:          *maxConns,
		Concurrency:       *concurrency,
		FullTargets:       *fullTargets,
		FlightRecorder:    *flightRecorder,
		Seed:              *seed,
		Compare:           compareCfg,
		CompareGomaxprocs: *compareGomaxprocs,
	}

	projectDir, err := os.Getwd()
	if err != nil {
		log.Fatalf("Failed to get working directory: %v", err)
	}

	suite := NewBenchmarkSuite(cfg, projectDir, *outputDir)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigCh
		log.Println("Received signal, shutting down...")
		cancel()
	}()

	if err := suite.Run(ctx); err != nil {
		log.Fatalf("Benchmark failed: %v", err)
	}

	log.Println("Benchmarks complete!")
}
