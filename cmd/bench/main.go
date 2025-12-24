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
	"io"
	"log"
	"math/rand"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
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

// BenchmarkCase defines a single benchmark case (e.g., "SELECT 1", "COPY OUT")
type BenchmarkCase struct {
	// Title is the markdown title for this benchmark section
	Title string

	// Description is optional markdown description
	Description string

	// SetUp runs once before all targets for this case
	SetUp func(ctx context.Context, s *BenchmarkSuite) error

	// TearDown runs once after all targets for this case
	TearDown func(ctx context.Context, s *BenchmarkSuite) error

	// Targets are the different configurations to benchmark
	Targets []*BenchmarkTarget
}

// BenchmarkTarget defines a single target configuration (becomes a row in the table)
type BenchmarkTarget struct {
	// Name is the target name shown in the table (e.g., "direct", "pglink (GOMAXPROCS=4)")
	Name string

	// SetUp runs before benchmarking this target
	SetUp func(ctx context.Context, s *BenchmarkSuite) error

	// TearDown runs after benchmarking this target
	TearDown func(ctx context.Context, s *BenchmarkSuite) error

	// Task runs a single iteration of the benchmark task
	// It receives a connection pool (may be nil for non-pool benchmarks like psql)
	Task func(ctx context.Context, s *BenchmarkSuite, pool *pgxpool.Pool) error

	// ConnString returns the connection string for this target
	ConnString func(s *BenchmarkSuite) string

	// MaxConns is the max connections for the pool
	MaxConns int

	// Concurrency is the number of concurrent workers
	Concurrency int
}

// BenchmarkResult holds results from a single benchmark run
type BenchmarkResult struct {
	CaseTitle     string        `json:"case_title"`
	TargetName    string        `json:"target_name"`
	MaxConns      int           `json:"max_conns"`
	Concurrency   int           `json:"concurrency"`
	TotalQueries  int64         `json:"total_queries"`
	SuccessCount  int64         `json:"success_count"`
	ErrorCount    int64         `json:"error_count"`
	ErrorRate     float64       `json:"error_rate"`
	Duration      time.Duration `json:"duration"`
	QueriesPerSec float64       `json:"queries_per_sec"`
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
	MaxConns       []int
	Concurrency    []int
	Rounds         int // Number of rounds to run each target (mitigates ordering effects)
	FlightRecorder bool
}

// BenchmarkSuite manages the benchmark execution
type BenchmarkSuite struct {
	runCfg     RunConfig
	projectDir string
	outputDir  string
	results    []BenchmarkResult

	// Pre-generated COPY data for consistent benchmarks
	copyData     []byte
	copyDataFile string

	// Process management
	pglinkCmd    *exec.Cmd
	pgbouncerCmd *exec.Cmd

	// Ports
	directPort    int
	pglinkPort    int
	pgbouncerPort int

	// Current pglink config (set during target setup)
	currentPglinkConfig *config.Config

	// Flight recorder
	pglinkTempDir string // Temp dir for pglink config (also contains traces)
	lastTraceFile string // Path to last captured trace file

	// TLS cert/key paths (set when pglink starts with TLS enabled)
	tlsCertPath string
	tlsKeyPath  string
}

// COPY benchmark constants
const copyRowCount = 1000

// NewBenchmarkSuite creates a new benchmark suite
func NewBenchmarkSuite(cfg RunConfig, projectDir, outputDir string) *BenchmarkSuite {
	return &BenchmarkSuite{
		runCfg:        cfg,
		projectDir:    projectDir,
		outputDir:     outputDir,
		directPort:    15432,
		pglinkPort:    16432,
		pgbouncerPort: 16433,
	}
}

// ============================================================================
// Target Factory Functions
// ============================================================================

// makeDirectTarget creates a target for direct postgres connection
func makeDirectTarget(name string, maxConns, concurrency int, task func(ctx context.Context, s *BenchmarkSuite, pool *pgxpool.Pool) error) *BenchmarkTarget {
	return &BenchmarkTarget{
		Name:        name,
		MaxConns:    maxConns,
		Concurrency: concurrency,
		ConnString: func(s *BenchmarkSuite) string {
			return fmt.Sprintf("postgres://app:app_password@localhost:%d/uno?sslmode=disable", s.directPort)
		},
		Task: task,
	}
}

// pgbouncerOpts configures pgbouncer target behavior.
type pgbouncerOpts struct {
	poolMode pgbouncer.PoolMode
	useTLS   bool
}

// makePgbouncerTarget creates a target for pgbouncer connection with options.
func makePgbouncerTarget(name string, maxConns, concurrency int, opts pgbouncerOpts, task func(ctx context.Context, s *BenchmarkSuite, pool *pgxpool.Pool) error) *BenchmarkTarget {
	return &BenchmarkTarget{
		Name:        name,
		MaxConns:    maxConns,
		Concurrency: concurrency,
		SetUp: func(ctx context.Context, s *BenchmarkSuite) error {
			return s.startPgbouncerWithOpts(ctx, s.currentPglinkConfig, opts)
		},
		TearDown: func(ctx context.Context, s *BenchmarkSuite) error {
			s.stopPgbouncer()
			return nil
		},
		ConnString: func(s *BenchmarkSuite) string {
			sslmode := "disable"
			if opts.useTLS {
				sslmode = "require"
			}
			return fmt.Sprintf("postgres://app:app_password@localhost:%d/alpha_uno?sslmode=%s", s.pgbouncerPort, sslmode)
		},
		Task: task,
	}
}

// pglinkOpts configures pglink target behavior.
type pglinkOpts struct {
	gomaxprocs int
	useTLS     bool
}

// makePglinkTarget creates a target for pglink connection with options.
func makePglinkTarget(name string, maxConns, concurrency int, opts pglinkOpts, task func(ctx context.Context, s *BenchmarkSuite, pool *pgxpool.Pool) error) *BenchmarkTarget {
	displayName := name
	if opts.gomaxprocs > 0 {
		displayName = fmt.Sprintf("%s (GOMAXPROCS=%d)", name, opts.gomaxprocs)
	}
	return &BenchmarkTarget{
		Name:        displayName,
		MaxConns:    maxConns,
		Concurrency: concurrency,
		SetUp: func(ctx context.Context, s *BenchmarkSuite) error {
			return s.startPglinkWithOpts(ctx, s.currentPglinkConfig, opts)
		},
		TearDown: func(ctx context.Context, s *BenchmarkSuite) error {
			s.stopPglink()
			return nil
		},
		ConnString: func(s *BenchmarkSuite) string {
			sslmode := "disable"
			if opts.useTLS {
				sslmode = "require"
			}
			return fmt.Sprintf("postgres://app:app_password@localhost:%d/alpha_uno?sslmode=%s", s.pglinkPort, sslmode)
		},
		Task: task,
	}
}

// ============================================================================
// Task Implementations
// ============================================================================

// taskSelect1 runs SELECT 1
func taskSelect1(ctx context.Context, s *BenchmarkSuite, pool *pgxpool.Pool) error {
	var result int
	return pool.QueryRow(ctx, "SELECT 1").Scan(&result)
}

// taskSelectRows runs SELECT with 100 rows
func taskSelectRows(ctx context.Context, s *BenchmarkSuite, pool *pgxpool.Pool) error {
	rows, err := pool.Query(ctx, "SELECT generate_series(1, 100)")
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
	}
	return rows.Err()
}

// taskTransaction runs BEGIN/SELECT/COMMIT
func taskTransaction(ctx context.Context, s *BenchmarkSuite, pool *pgxpool.Pool) error {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return err
	}
	var result int
	if err := tx.QueryRow(ctx, "SELECT 1").Scan(&result); err != nil {
		_ = tx.Rollback(ctx)
		return err
	}
	return tx.Commit(ctx)
}

// taskCopyOut runs COPY TO STDOUT using a query-generated dataset.
// This avoids table locking by generating data on the fly.
func taskCopyOut(ctx context.Context, s *BenchmarkSuite, pool *pgxpool.Pool) error {
	conn, err := pool.Acquire(ctx)
	if err != nil {
		return err
	}
	defer conn.Release()

	// Generate data on the fly - no table access, no locking
	query := fmt.Sprintf(`COPY (
		SELECT i AS id, 'row_' || i::text AS name, i * 10 AS value
		FROM generate_series(1, %d) AS i
	) TO STDOUT`, copyRowCount)

	_, err = conn.Conn().PgConn().CopyTo(ctx, io.Discard, query)
	return err
}

// taskCopyIn runs COPY FROM STDIN to a temp table.
// Each connection creates its own temp table, avoiding cross-connection locking.
func taskCopyIn(ctx context.Context, s *BenchmarkSuite, pool *pgxpool.Pool) error {
	conn, err := pool.Acquire(ctx)
	if err != nil {
		return err
	}
	defer conn.Release()

	// Create a temp table for this session if it doesn't exist.
	// Temp tables are session-local, so no locking between connections.
	_, err = conn.Exec(ctx, `
		CREATE TEMP TABLE IF NOT EXISTS bench_copy_temp (
			id INTEGER,
			name TEXT,
			value INTEGER
		) ON COMMIT DELETE ROWS
	`)
	if err != nil {
		return err
	}

	// Use a transaction - ON COMMIT DELETE ROWS cleans up automatically
	tx, err := conn.Begin(ctx)
	if err != nil {
		return err
	}

	_, err = tx.Conn().PgConn().CopyFrom(ctx, strings.NewReader(string(s.copyData)), "COPY bench_copy_temp FROM STDIN")
	if err != nil {
		_ = tx.Rollback(ctx)
		return err
	}

	// Commit triggers ON COMMIT DELETE ROWS, cleaning up without TRUNCATE
	return tx.Commit(ctx)
}

// makePsqlCopyOutTask creates a psql COPY OUT task for a given connection getter.
// Uses a query-generated dataset to avoid table locking.
func makePsqlCopyOutTask(getConnParams func(s *BenchmarkSuite) (host string, port int, dbname, user, password string)) func(ctx context.Context, s *BenchmarkSuite, pool *pgxpool.Pool) error {
	return func(ctx context.Context, s *BenchmarkSuite, pool *pgxpool.Pool) error {
		host, port, dbname, user, password := getConnParams(s)

		// Generate data on the fly with COPY (SELECT ...) - no table access, no locking
		query := fmt.Sprintf(`COPY (SELECT i AS id, 'row_' || i::text AS name, i * 10 AS value FROM generate_series(1, %d) AS i) TO STDOUT`, copyRowCount)

		cmd := exec.CommandContext(ctx, "psql",
			"-h", host,
			"-p", fmt.Sprintf("%d", port),
			"-d", dbname,
			"-U", user,
			"-c", query,
		)
		cmd.Env = append(os.Environ(), fmt.Sprintf("PGPASSWORD=%s", password))
		output, err := cmd.CombinedOutput()
		if err != nil {
			return fmt.Errorf("psql copy out failed: %w: %s", err, string(output))
		}
		return nil
	}
}

// makePsqlCopyInTask creates a psql COPY IN task for a given connection getter.
// Uses a temp table with ON COMMIT DELETE ROWS to avoid locking.
func makePsqlCopyInTask(getConnParams func(s *BenchmarkSuite) (host string, port int, dbname, user, password string)) func(ctx context.Context, s *BenchmarkSuite, pool *pgxpool.Pool) error {
	return func(ctx context.Context, s *BenchmarkSuite, pool *pgxpool.Pool) error {
		host, port, dbname, user, password := getConnParams(s)

		// Create temp table, COPY into it within a transaction, then commit.
		// ON COMMIT DELETE ROWS cleans up automatically without TRUNCATE locking.
		query := fmt.Sprintf(`
			CREATE TEMP TABLE IF NOT EXISTS bench_copy_temp (
				id INTEGER, name TEXT, value INTEGER
			) ON COMMIT DELETE ROWS;
			BEGIN;
			\copy bench_copy_temp FROM '%s'
			COMMIT;
		`, s.copyDataFile)

		cmd := exec.CommandContext(ctx, "psql",
			"-h", host,
			"-p", fmt.Sprintf("%d", port),
			"-d", dbname,
			"-U", user,
			"-c", query,
		)
		cmd.Env = append(os.Environ(), fmt.Sprintf("PGPASSWORD=%s", password))
		output, err := cmd.CombinedOutput()
		if err != nil {
			return fmt.Errorf("psql copy in failed: %w: %s", err, string(output))
		}

		return nil
	}
}

// ============================================================================
// Benchmark Case Builders
// ============================================================================

// buildStandardTargets creates the standard set of targets:
// - direct
// - pgbouncer (tx pooling)
// - pgbouncer (tx pooling) (tls)
// - pgbouncer (session pooling)
// - pglink (GOMAXPROCS=4)
// - pglink (GOMAXPROCS=8)
// - pglink (GOMAXPROCS=16)
// - pglink (tls) (GOMAXPROCS=16)
func (s *BenchmarkSuite) buildStandardTargets(maxConns, concurrency int, task func(ctx context.Context, s *BenchmarkSuite, pool *pgxpool.Pool) error) []*BenchmarkTarget {
	targets := []*BenchmarkTarget{
		// Direct PostgreSQL connection
		makeDirectTarget("direct", maxConns, concurrency, task),

		// pgbouncer variants
		makePgbouncerTarget("pgbouncer (tx pooling)", maxConns, concurrency,
			pgbouncerOpts{poolMode: pgbouncer.PoolModeTransaction, useTLS: false}, task),
		makePgbouncerTarget("pgbouncer (tx pooling) (tls)", maxConns, concurrency,
			pgbouncerOpts{poolMode: pgbouncer.PoolModeTransaction, useTLS: true}, task),
		makePgbouncerTarget("pgbouncer (session pooling)", maxConns, concurrency,
			pgbouncerOpts{poolMode: pgbouncer.PoolModeSession, useTLS: false}, task),

		// pglink variants - GOMAXPROCS 4, 8, 16
		makePglinkTarget("pglink", maxConns, concurrency,
			pglinkOpts{gomaxprocs: 4, useTLS: false}, task),
		makePglinkTarget("pglink", maxConns, concurrency,
			pglinkOpts{gomaxprocs: 8, useTLS: false}, task),
		makePglinkTarget("pglink", maxConns, concurrency,
			pglinkOpts{gomaxprocs: 16, useTLS: false}, task),

		// pglink with TLS - only GOMAXPROCS=16
		makePglinkTarget("pglink (tls)", maxConns, concurrency,
			pglinkOpts{gomaxprocs: 16, useTLS: true}, task),
	}

	return targets
}

// buildPsqlTargets creates psql-based targets (subset for psql-specific tests)
func (s *BenchmarkSuite) buildPsqlTargets(concurrency int, taskMaker func(getConnParams func(s *BenchmarkSuite) (string, int, string, string, string)) func(ctx context.Context, s *BenchmarkSuite, pool *pgxpool.Pool) error) []*BenchmarkTarget {
	directParams := func(s *BenchmarkSuite) (string, int, string, string, string) {
		return "localhost", s.directPort, "uno", "app", "app_password"
	}
	pgbouncerParams := func(s *BenchmarkSuite) (string, int, string, string, string) {
		return "localhost", s.pgbouncerPort, "alpha_uno", "app", "app_password"
	}
	pglinkParams := func(s *BenchmarkSuite) (string, int, string, string, string) {
		return "localhost", s.pglinkPort, "alpha_uno", "app", "app_password"
	}

	targets := []*BenchmarkTarget{
		{
			Name:        "direct (psql)",
			Concurrency: concurrency,
			Task:        taskMaker(directParams),
		},
		{
			Name:        "pgbouncer (tx pooling) (psql)",
			Concurrency: concurrency,
			SetUp: func(ctx context.Context, s *BenchmarkSuite) error {
				return s.startPgbouncerWithOpts(ctx, s.currentPglinkConfig,
					pgbouncerOpts{poolMode: pgbouncer.PoolModeTransaction, useTLS: false})
			},
			TearDown: func(ctx context.Context, s *BenchmarkSuite) error {
				s.stopPgbouncer()
				return nil
			},
			Task: taskMaker(pgbouncerParams),
		},
		{
			Name:        "pglink (psql) (GOMAXPROCS=16)",
			Concurrency: concurrency,
			SetUp: func(ctx context.Context, s *BenchmarkSuite) error {
				return s.startPglinkWithOpts(ctx, s.currentPglinkConfig,
					pglinkOpts{gomaxprocs: 16, useTLS: false})
			},
			TearDown: func(ctx context.Context, s *BenchmarkSuite) error {
				s.stopPglink()
				return nil
			},
			Task: taskMaker(pglinkParams),
		},
	}

	return targets
}

// buildAllBenchmarkCases creates all benchmark cases for the given parameters
func (s *BenchmarkSuite) buildAllBenchmarkCases() []*BenchmarkCase {
	var cases []*BenchmarkCase

	for _, maxConns := range s.runCfg.MaxConns {
		for _, concurrency := range s.runCfg.Concurrency {
			// Skip if concurrency is too high for max_conns
			if concurrency > maxConns*3 {
				continue
			}

			mc := maxConns
			cc := concurrency

			// SELECT 1
			cases = append(cases, &BenchmarkCase{
				Title:       fmt.Sprintf("SELECT 1 (max_conns=%d, concurrency=%d)", mc, cc),
				Description: "Pure proxy overhead benchmark",
				Targets:     s.buildStandardTargets(mc, cc, taskSelect1),
			})

			// SELECT with rows
			cases = append(cases, &BenchmarkCase{
				Title:       fmt.Sprintf("SELECT 100 Rows (max_conns=%d, concurrency=%d)", mc, cc),
				Description: "Data transfer benchmark",
				Targets:     s.buildStandardTargets(mc, cc, taskSelectRows),
			})

			// Transactions
			cases = append(cases, &BenchmarkCase{
				Title:       fmt.Sprintf("Transaction (max_conns=%d, concurrency=%d)", mc, cc),
				Description: "BEGIN/SELECT/COMMIT benchmark",
				Targets:     s.buildStandardTargets(mc, cc, taskTransaction),
			})

			// COPY OUT (pgx) - uses generated data, no table access
			cases = append(cases, &BenchmarkCase{
				Title:       fmt.Sprintf("COPY OUT pgx (%d rows, max_conns=%d, concurrency=%d)", copyRowCount, mc, cc),
				Description: "COPY TO STDOUT benchmark using pgx with generated data",
				Targets:     s.buildStandardTargets(mc, cc, taskCopyOut),
			})

			// COPY IN (pgx) - uses temp tables per connection
			cases = append(cases, &BenchmarkCase{
				Title:       fmt.Sprintf("COPY IN pgx (%d rows, max_conns=%d, concurrency=%d)", copyRowCount, mc, cc),
				Description: "COPY FROM STDIN benchmark using pgx with temp tables",
				Targets:     s.buildStandardTargets(mc, cc, taskCopyIn),
			})

			// COPY OUT (psql) - uses generated data, no table access
			cases = append(cases, &BenchmarkCase{
				Title:       fmt.Sprintf("COPY OUT psql (%d rows, concurrency=%d)", copyRowCount, cc),
				Description: "COPY TO STDOUT benchmark using psql CLI with generated data",
				Targets:     s.buildPsqlTargets(cc, makePsqlCopyOutTask),
			})

			// COPY IN (psql) - uses temp tables per connection
			cases = append(cases, &BenchmarkCase{
				Title:       fmt.Sprintf("COPY IN psql (%d rows, concurrency=%d)", copyRowCount, cc),
				Description: "COPY FROM STDIN benchmark using psql CLI with temp tables",
				Targets:     s.buildPsqlTargets(cc, makePsqlCopyInTask),
			})
		}
	}

	return cases
}

// ============================================================================
// Setup and Teardown
// ============================================================================

func (s *BenchmarkSuite) generateCopyData() error {
	var buf strings.Builder
	for i := 1; i <= copyRowCount; i++ {
		buf.WriteString(fmt.Sprintf("%d\trow_%d\t%d\n", i, i, i*10))
	}
	s.copyData = []byte(buf.String())

	// Write to temp file for psql
	tmpFile, err := os.CreateTemp("", "bench_copy_*.csv")
	if err != nil {
		return err
	}
	defer func() { _ = tmpFile.Close() }()
	if _, err := tmpFile.Write(s.copyData); err != nil {
		_ = os.Remove(tmpFile.Name())
		return err
	}
	s.copyDataFile = tmpFile.Name()

	return nil
}

func (s *BenchmarkSuite) ensureDockerCompose(ctx context.Context) error {
	cmd := exec.CommandContext(ctx, "docker-compose", "up", "-d", "--wait")
	cmd.Dir = s.projectDir
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	return cmd.Run()
}

func (s *BenchmarkSuite) generatePglinkConfig(maxConns int) (*config.Config, error) {
	configPath := filepath.Join(s.projectDir, "pglink.json")
	cfg, err := config.ReadConfigFile(configPath)
	if err != nil {
		return nil, err
	}
	for _, dbCfg := range cfg.Databases {
		dbCfg.Backend.PoolMaxConns = int32(maxConns)
	}
	return cfg, nil
}

func (s *BenchmarkSuite) startPglinkWithOpts(ctx context.Context, cfg *config.Config, opts pglinkOpts) error {
	tmpDir, err := os.MkdirTemp("", "pglink-bench-*")
	if err != nil {
		return err
	}
	s.pglinkTempDir = tmpDir

	// Configure flight recorder if enabled
	var args []string
	if s.runCfg.FlightRecorder {
		tracesDir := filepath.Join(tmpDir, "traces")
		if err := os.MkdirAll(tracesDir, 0755); err != nil {
			_ = os.RemoveAll(tmpDir)
			return fmt.Errorf("failed to create traces dir: %w", err)
		}
		// Use CLI flags to enable flight recorder
		// Set min_age to benchmark duration + warmup + buffer
		minAge := s.runCfg.Duration + s.runCfg.Warmup + 30*time.Second
		args = append(args,
			"-flight-recorder", tracesDir,
			"-flight-recorder-min-age", minAge.String(),
			"-flight-recorder-max-bytes", "52428800", // 50MB for benchmarks
		)
	}

	// Make a copy of config to modify TLS settings
	cfgCopy := *cfg

	// Configure TLS based on opts
	if opts.useTLS {
		// Enable TLS with generated cert, write to files so pgbouncer can use them
		certPath := "server.crt"
		keyPath := "server.key"
		cfgCopy.TLS = &config.JsonTLSConfig{
			SSLMode:            config.SSLModeAllow,
			CertPath:           certPath,
			CertPrivateKeyPath: keyPath,
			GenerateCert:       true,
		}
		// Store absolute paths for pgbouncer to use
		s.tlsCertPath = filepath.Join(tmpDir, certPath)
		s.tlsKeyPath = filepath.Join(tmpDir, keyPath)
	} else {
		// Explicitly disable TLS
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

	pglinkBinary := filepath.Join(s.projectDir, "out", "pglink")
	if _, err := os.Stat(pglinkBinary); os.IsNotExist(err) {
		buildCmd := exec.CommandContext(ctx, filepath.Join(s.projectDir, "bin", "build"))
		buildCmd.Dir = s.projectDir
		if err := buildCmd.Run(); err != nil {
			_ = os.RemoveAll(tmpDir)
			return fmt.Errorf("failed to build pglink: %w", err)
		}
	}

	// Build command with config and optional flight recorder flags
	cmdArgs := append([]string{"-config", configPath}, args...)
	s.pglinkCmd = exec.Command(pglinkBinary, cmdArgs...)
	s.pglinkCmd.Env = append(os.Environ(), fmt.Sprintf("GOMAXPROCS=%d", opts.gomaxprocs))
	s.pglinkCmd.Dir = tmpDir

	if err := s.pglinkCmd.Start(); err != nil {
		_ = os.RemoveAll(tmpDir)
		return fmt.Errorf("failed to start pglink: %w", err)
	}

	// Wait for pglink to be ready
	return s.waitForPort(ctx, s.pglinkPort, 10*time.Second)
}

func (s *BenchmarkSuite) stopPglink() {
	s.lastTraceFile = ""

	if s.pglinkCmd != nil && s.pglinkCmd.Process != nil {
		// If flight recorder is enabled, send SIGUSR1 to trigger a snapshot before stopping
		if s.runCfg.FlightRecorder {
			_ = s.pglinkCmd.Process.Signal(syscall.SIGUSR1)
			// Give it a moment to write the snapshot
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

	// If flight recorder was enabled, find and copy the trace file
	if s.runCfg.FlightRecorder && s.pglinkTempDir != "" {
		tracesDir := filepath.Join(s.pglinkTempDir, "traces")
		files, err := os.ReadDir(tracesDir)
		if err == nil && len(files) > 0 {
			// Find the most recent trace file
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
				// Copy to output traces directory
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
		// Clean up temp dir
		_ = os.RemoveAll(s.pglinkTempDir)
		s.pglinkTempDir = ""
	}
}

func (s *BenchmarkSuite) startPgbouncerWithOpts(ctx context.Context, cfg *config.Config, opts pgbouncerOpts) error {
	tmpDir, err := os.MkdirTemp("", "pgbouncer-bench-*")
	if err != nil {
		return err
	}

	// Build pgbouncer options
	pgbOpts := pgbouncer.Options{
		PoolMode: opts.poolMode,
	}

	// Configure TLS if requested
	if opts.useTLS {
		// For pgbouncer TLS, we need to generate certs.
		// First start pglink with TLS to get the certs, then use them.
		// However, since we might not have pglink running, generate our own.
		// For simplicity, we'll generate certs directly here.
		certPath := filepath.Join(tmpDir, "server.crt")
		keyPath := filepath.Join(tmpDir, "server.key")

		// Generate self-signed cert for pgbouncer
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

// generateSelfSignedCert generates a self-signed TLS certificate.
func generateSelfSignedCert(certPath, keyPath string) (err error) {
	// Use the same cert generation as pglink's config package
	cert, err := config.GenerateSelfSignedCertForBenchmark()
	if err != nil {
		return err
	}

	// Write cert
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
		if err := writePEMBlock(certFile, "CERTIFICATE", certBytes); err != nil {
			return err
		}
	}

	// Write key
	keyFile, err := os.OpenFile(keyPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0600)
	if err != nil {
		return err
	}
	defer func() {
		if kerr := keyFile.Close(); kerr != nil && err == nil {
			err = kerr
		}
	}()

	// Get the private key bytes - we need to marshal it
	privKeyBytes, err := config.MarshalPrivateKeyForBenchmark(cert.PrivateKey)
	if err != nil {
		return err
	}

	return writePEMBlock(keyFile, "EC PRIVATE KEY", privKeyBytes)
}

func writePEMBlock(w io.Writer, blockType string, bytes []byte) error {
	block := &pem.Block{
		Type:  blockType,
		Bytes: bytes,
	}
	return pem.Encode(w, block)
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

	log.Println("Generating COPY data...")
	if err := s.generateCopyData(); err != nil {
		return fmt.Errorf("failed to generate copy data: %w", err)
	}
	defer func() { _ = os.Remove(s.copyDataFile) }()

	// Generate config for the first max_conns value (will be regenerated per case if needed)
	if len(s.runCfg.MaxConns) > 0 {
		cfg, err := s.generatePglinkConfig(s.runCfg.MaxConns[0])
		if err != nil {
			return fmt.Errorf("failed to generate pglink config: %w", err)
		}
		s.currentPglinkConfig = cfg
	}

	cases := s.buildAllBenchmarkCases()
	rounds := s.runCfg.Rounds
	if rounds < 1 {
		rounds = 1
	}

	log.Printf("Running %d benchmark cases (%d rounds each, %d targets per case)...",
		len(cases), rounds, len(cases[0].Targets))
	log.Printf("Each target runs for %s warmup + %s measurement per round",
		s.runCfg.Warmup, s.runCfg.Duration)

	for i, bc := range cases {
		log.Printf("\n=== [%d/%d] %s ===", i+1, len(cases), bc.Title)

		// Update pglink config if max_conns changed
		if len(bc.Targets) > 0 && bc.Targets[0].MaxConns > 0 {
			cfg, err := s.generatePglinkConfig(bc.Targets[0].MaxConns)
			if err != nil {
				log.Printf("Warning: failed to generate config: %v", err)
				continue
			}
			s.currentPglinkConfig = cfg
		}

		// Case setup
		if bc.SetUp != nil {
			if err := bc.SetUp(ctx, s); err != nil {
				log.Printf("Warning: case setup failed: %v", err)
				continue
			}
		}

		// Collect results from all rounds for aggregation
		// Map from target name to list of results
		targetResults := make(map[string][]BenchmarkResult)

		// Run multiple rounds with shuffled target order
		for round := 1; round <= rounds; round++ {
			// Create shuffled copy of targets for this round
			shuffledTargets := make([]*BenchmarkTarget, len(bc.Targets))
			copy(shuffledTargets, bc.Targets)
			rand.Shuffle(len(shuffledTargets), func(i, j int) {
				shuffledTargets[i], shuffledTargets[j] = shuffledTargets[j], shuffledTargets[i]
			})

			log.Printf("  Round %d/%d (order: %s)", round, rounds, targetOrder(shuffledTargets))

			for _, target := range shuffledTargets {
				result, err := s.runTarget(ctx, bc, target)
				if err != nil {
					log.Printf("    %s: ERROR: %v", target.Name, err)
					continue
				}
				targetResults[target.Name] = append(targetResults[target.Name], result)
				log.Printf("    %s: %.0f qps, p50=%.0fus, p99=%.0fus",
					target.Name, result.QueriesPerSec, result.P50LatencyUs, result.P99LatencyUs)
			}
		}

		// Aggregate results across rounds for each target
		for _, target := range bc.Targets {
			results := targetResults[target.Name]
			if len(results) == 0 {
				continue
			}
			aggregated := aggregateResults(results)
			s.results = append(s.results, aggregated)
			log.Printf("  [AGGREGATED] %s: %.0f qps (%.2f%% errors), p50=%.0fus, p99=%.0fus",
				target.Name, aggregated.QueriesPerSec, aggregated.ErrorRate*100,
				aggregated.P50LatencyUs, aggregated.P99LatencyUs)
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

// targetOrder returns a short string showing the order of targets
func targetOrder(targets []*BenchmarkTarget) string {
	names := make([]string, len(targets))
	for i, t := range targets {
		// Abbreviate target names for compact display
		name := t.Name
		if len(name) > 15 {
			name = name[:12] + "..."
		}
		names[i] = name
	}
	return strings.Join(names, " → ")
}

// aggregateResults combines multiple BenchmarkResults into one
func aggregateResults(results []BenchmarkResult) BenchmarkResult {
	if len(results) == 0 {
		return BenchmarkResult{}
	}
	if len(results) == 1 {
		return results[0]
	}

	// Use first result as template for metadata
	agg := BenchmarkResult{
		CaseTitle:   results[0].CaseTitle,
		TargetName:  results[0].TargetName,
		MaxConns:    results[0].MaxConns,
		Concurrency: results[0].Concurrency,
		Timestamp:   results[0].Timestamp,
	}

	// Sum up counts and durations
	var totalQueries, successCount, errorCount int64
	var totalDuration time.Duration
	var allErrors []string

	for _, r := range results {
		totalQueries += r.TotalQueries
		successCount += r.SuccessCount
		errorCount += r.ErrorCount
		totalDuration += r.Duration
		allErrors = append(allErrors, r.SampleErrors...)
		// Keep trace file from last result
		if r.TraceFile != "" {
			agg.TraceFile = r.TraceFile
		}
	}

	agg.TotalQueries = totalQueries
	agg.SuccessCount = successCount
	agg.ErrorCount = errorCount
	agg.Duration = totalDuration

	if totalQueries > 0 {
		agg.ErrorRate = float64(errorCount) / float64(totalQueries)
		agg.QueriesPerSec = float64(totalQueries) / totalDuration.Seconds()
	}

	// For latency percentiles, we take the average of each percentile across rounds
	// This is an approximation but avoids storing all raw latencies
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

func (s *BenchmarkSuite) runTarget(ctx context.Context, bc *BenchmarkCase, target *BenchmarkTarget) (result BenchmarkResult, err error) {
	result = BenchmarkResult{
		CaseTitle:   bc.Title,
		TargetName:  target.Name,
		MaxConns:    target.MaxConns,
		Concurrency: target.Concurrency,
		Timestamp:   time.Now(),
	}

	// Target setup
	if target.SetUp != nil {
		if err := target.SetUp(ctx, s); err != nil {
			return result, fmt.Errorf("target setup failed: %w", err)
		}
	}

	// Ensure teardown runs and capture trace file path
	defer func() {
		if target.TearDown != nil {
			_ = target.TearDown(ctx, s)
		}
		// Capture the trace file path after teardown (which calls stopPglink)
		if s.lastTraceFile != "" {
			result.TraceFile = s.lastTraceFile
		}
	}()

	// Create connection pool if we have a connection string
	var pool *pgxpool.Pool
	if target.ConnString != nil {
		connStr := target.ConnString(s)
		poolConfig, err := pgxpool.ParseConfig(connStr)
		if err != nil {
			return result, err
		}
		concurrency := target.Concurrency
		if concurrency == 0 {
			concurrency = 10
		}
		poolConfig.MaxConns = int32(concurrency)
		poolConfig.MinConns = int32(concurrency)
		poolConfig.ConnConfig.DefaultQueryExecMode = pgx.QueryExecModeDescribeExec

		pool, err = pgxpool.NewWithConfig(ctx, poolConfig)
		if err != nil {
			return result, err
		}
		defer pool.Close()
	}

	// Warmup
	warmupCtx, warmupCancel := context.WithTimeout(ctx, s.runCfg.Warmup)
	s.runLoadForDuration(warmupCtx, pool, target, nil, nil, nil)
	warmupCancel()

	// Benchmark
	var successCount, errorCount atomic.Int64
	latencies := make([]int64, 0, 100000)
	latenciesMu := sync.Mutex{}
	errorsMu := sync.Mutex{}
	var sampleErrors []string

	benchCtx, benchCancel := context.WithTimeout(ctx, s.runCfg.Duration)
	start := time.Now()

	s.runLoadForDuration(benchCtx, pool, target,
		func(latency time.Duration, err error) {
			latenciesMu.Lock()
			latencies = append(latencies, latency.Nanoseconds())
			latenciesMu.Unlock()

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
		&successCount,
		&errorCount,
	)

	benchCancel()
	elapsed := time.Since(start)

	// Calculate results
	result.TotalQueries = successCount.Load() + errorCount.Load()
	result.SuccessCount = successCount.Load()
	result.ErrorCount = errorCount.Load()
	result.Duration = elapsed
	result.SampleErrors = sampleErrors

	if result.TotalQueries > 0 {
		result.ErrorRate = float64(result.ErrorCount) / float64(result.TotalQueries)
		result.QueriesPerSec = float64(result.TotalQueries) / elapsed.Seconds()
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
	onComplete func(latency time.Duration, err error),
	successCount *atomic.Int64,
	errorCount *atomic.Int64,
) {
	concurrency := target.Concurrency
	if concurrency == 0 {
		concurrency = 10
	}

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
					queryCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
					err := target.Task(queryCtx, s, pool)
					cancel()
					latency := time.Since(queryStart)

					select {
					case <-ctx.Done():
						return
					default:
						if onComplete != nil {
							onComplete(latency, err)
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
- **Max connections tested:** {{.MaxConns}}
- **Concurrency levels:** {{.Concurrency}}
- **pglink GOMAXPROCS:** 4, 8, 16 (fixed)

## Results
{{range .Cases}}
### {{.Title}}
{{if .Description}}
{{.Description}}
{{end}}
| Target | Max Conns | Concurrency | QPS | Error Rate | P50 (μs) | P99 (μs) |
|--------|-----------|-------------|-----|------------|----------|----------|
{{range $i, $r := .Results -}}
| {{$r.TargetName}} | {{if $r.MaxConns}}{{$r.MaxConns}}{{else}}-{{end}} | {{$r.Concurrency}} | {{fmtQPS $r.QueriesPerSec $.BaselineQPS}} | {{fmtErr $r.ErrorRate}} | {{fmtLatency $r.P50LatencyUs $.BaselineP50}} | {{fmtLatency $r.P99LatencyUs $.BaselineP99}} |
{{end}}
{{end}}
## Analysis

### Error Rates Under Load

High error rates typically indicate:
- Pool exhaustion (concurrency > max_conns)
- Connection timeouts
- Backend overload

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
		"mul": func(a, b float64) float64 { return a * b },
		// fmtQPS formats QPS with relative comparison (higher is better)
		"fmtQPS": func(val, baseline float64) string {
			if baseline <= 0 || val == baseline {
				return fmt.Sprintf("%.0f", val)
			}
			ratio := val / baseline
			if ratio >= 1 {
				return fmt.Sprintf("%.0f", val) // Better or same, no annotation needed
			}
			// Slower - show how much slower
			slowdown := baseline / val
			if slowdown >= 2 {
				return fmt.Sprintf("%.0f (%.1fx slower)", val, slowdown)
			}
			pctSlower := (1 - ratio) * 100
			return fmt.Sprintf("%.0f (%.0f%% slower)", val, pctSlower)
		},
		// fmtLatency formats latency with relative comparison (lower is better)
		"fmtLatency": func(val, baseline float64) string {
			if baseline <= 0 || val == baseline {
				return fmt.Sprintf("%.0f", val)
			}
			ratio := val / baseline
			if ratio <= 1 {
				return fmt.Sprintf("%.0f", val) // Better or same, no annotation needed
			}
			// Slower - show how much slower
			if ratio >= 2 {
				return fmt.Sprintf("%.0f (%.1fx slower)", val, ratio)
			}
			pctSlower := (ratio - 1) * 100
			return fmt.Sprintf("%.0f (+%.0f%%)", val, pctSlower)
		},
		// fmtErr formats error rate
		"fmtErr": func(rate float64) string {
			pct := rate * 100
			if pct == 0 {
				return "0%"
			}
			if pct < 0.01 {
				return "<0.01%"
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
		BaselineQPS float64 // QPS of first (baseline) target
		BaselineP50 float64 // P50 latency of first (baseline) target
		BaselineP99 float64 // P99 latency of first (baseline) target
	}

	var cases []CaseData
	for _, title := range caseOrder {
		results := caseMap[title]
		cd := CaseData{
			Title:   title,
			Results: results,
		}
		// Use first result (typically "direct") as baseline for comparison
		if len(results) > 0 {
			cd.BaselineQPS = results[0].QueriesPerSec
			cd.BaselineP50 = results[0].P50LatencyUs
			cd.BaselineP99 = results[0].P99LatencyUs
		}
		cases = append(cases, cd)
	}

	data := struct {
		Timestamp    string
		Rounds       int
		Duration     string
		Warmup       string
		MaxConns     string
		Concurrency  string
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
		MaxConns:     fmt.Sprintf("%v", s.runCfg.MaxConns),
		Concurrency:  fmt.Sprintf("%v", s.runCfg.Concurrency),
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
	duration := flag.Duration("duration", 25*time.Second, "Duration of each benchmark measurement")
	warmup := flag.Duration("warmup", 5*time.Second, "Warmup duration before measuring")
	rounds := flag.Int("rounds", 3, "Number of rounds per target (mitigates ordering effects)")
	outputDir := flag.String("output", "out/benchmark", "Output directory for results")
	maxConnsStr := flag.String("max-conns", "100,500,1000", "Comma-separated max connection values")
	concurrencyStr := flag.String("concurrency", "100,200,500,600,1000,1500", "Comma-separated concurrency values")
	flightRecorder := flag.Bool("flight-recorder", false, "Enable flight recorder for pglink targets (captures trace on teardown)")
	flag.Parse()

	parseInts := func(s string) []int {
		parts := strings.Split(s, ",")
		result := make([]int, 0, len(parts))
		for _, p := range parts {
			p = strings.TrimSpace(p)
			if v, err := strconv.Atoi(p); err == nil {
				result = append(result, v)
			}
		}
		return result
	}

	cfg := RunConfig{
		MaxConns:       parseInts(*maxConnsStr),
		Concurrency:    parseInts(*concurrencyStr),
		Rounds:         *rounds,
		Duration:       *duration,
		Warmup:         *warmup,
		FlightRecorder: *flightRecorder,
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
