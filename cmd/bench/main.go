// Command bench runs comprehensive benchmarks comparing direct postgres, pglink, and pgbouncer.
//
// The benchmark runs each configuration for a fixed duration, collects metrics, then exits.
// It does NOT wait for in-flight queries to complete after the duration expires.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
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
	Timestamp     time.Time     `json:"timestamp"`
}

// RunConfig defines the benchmark run parameters
type RunConfig struct {
	Duration    time.Duration
	Warmup      time.Duration
	MaxConns    []int
	Concurrency []int
	GoMaxProcs  []int
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

// makePgbouncerTarget creates a target for pgbouncer connection
func makePgbouncerTarget(name string, maxConns, concurrency int, task func(ctx context.Context, s *BenchmarkSuite, pool *pgxpool.Pool) error) *BenchmarkTarget {
	return &BenchmarkTarget{
		Name:        name,
		MaxConns:    maxConns,
		Concurrency: concurrency,
		SetUp: func(ctx context.Context, s *BenchmarkSuite) error {
			return s.startPgbouncer(ctx, s.currentPglinkConfig)
		},
		TearDown: func(ctx context.Context, s *BenchmarkSuite) error {
			s.stopPgbouncer()
			return nil
		},
		ConnString: func(s *BenchmarkSuite) string {
			return fmt.Sprintf("postgres://app:app_password@localhost:%d/alpha_uno?sslmode=disable", s.pgbouncerPort)
		},
		Task: task,
	}
}

// makePglinkTarget creates a target for pglink connection with specified GOMAXPROCS
func makePglinkTarget(name string, maxConns, concurrency, gomaxprocs int, task func(ctx context.Context, s *BenchmarkSuite, pool *pgxpool.Pool) error) *BenchmarkTarget {
	displayName := name
	if gomaxprocs > 0 {
		displayName = fmt.Sprintf("%s (GOMAXPROCS=%d)", name, gomaxprocs)
	}
	return &BenchmarkTarget{
		Name:        displayName,
		MaxConns:    maxConns,
		Concurrency: concurrency,
		SetUp: func(ctx context.Context, s *BenchmarkSuite) error {
			return s.startPglink(ctx, s.currentPglinkConfig, gomaxprocs)
		},
		TearDown: func(ctx context.Context, s *BenchmarkSuite) error {
			s.stopPglink()
			return nil
		},
		ConnString: func(s *BenchmarkSuite) string {
			return fmt.Sprintf("postgres://app:app_password@localhost:%d/alpha_uno?sslmode=prefer", s.pglinkPort)
		},
		Task: task,
	}
}

// makePsqlTarget creates a target that uses psql CLI instead of pgx pool
func makePsqlTarget(name string, concurrency int, connParams func(s *BenchmarkSuite) (host string, port int, dbname, user, password string), task func(ctx context.Context, s *BenchmarkSuite, pool *pgxpool.Pool) error) *BenchmarkTarget {
	return &BenchmarkTarget{
		Name:        name,
		MaxConns:    0, // Not applicable for psql
		Concurrency: concurrency,
		ConnString: func(s *BenchmarkSuite) string {
			host, port, dbname, user, password := connParams(s)
			return fmt.Sprintf("postgres://%s:%s@%s:%d/%s", user, password, host, port, dbname)
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
		tx.Rollback(ctx)
		return err
	}
	return tx.Commit(ctx)
}

// taskCopyOut runs COPY TO STDOUT
func taskCopyOut(ctx context.Context, s *BenchmarkSuite, pool *pgxpool.Pool) error {
	conn, err := pool.Acquire(ctx)
	if err != nil {
		return err
	}
	defer conn.Release()
	_, err = conn.Conn().PgConn().CopyTo(ctx, io.Discard, "COPY bench_copy TO STDOUT")
	return err
}

// taskCopyIn runs COPY FROM STDIN
func taskCopyIn(ctx context.Context, s *BenchmarkSuite, pool *pgxpool.Pool) error {
	conn, err := pool.Acquire(ctx)
	if err != nil {
		return err
	}
	defer conn.Release()

	// Use a transaction so we can rollback (avoiding data accumulation)
	tx, err := conn.Begin(ctx)
	if err != nil {
		return err
	}

	_, err = tx.Conn().PgConn().CopyFrom(ctx, strings.NewReader(string(s.copyData)), "COPY bench_copy FROM STDIN")
	if err != nil {
		tx.Rollback(ctx)
		return err
	}

	// Rollback to avoid accumulating data
	return tx.Rollback(ctx)
}

// makePsqlCopyOutTask creates a psql COPY OUT task for a given connection getter
func makePsqlCopyOutTask(getConnParams func(s *BenchmarkSuite) (host string, port int, dbname, user, password string)) func(ctx context.Context, s *BenchmarkSuite, pool *pgxpool.Pool) error {
	return func(ctx context.Context, s *BenchmarkSuite, pool *pgxpool.Pool) error {
		host, port, dbname, user, password := getConnParams(s)
		cmd := exec.CommandContext(ctx, "psql",
			"-h", host,
			"-p", fmt.Sprintf("%d", port),
			"-d", dbname,
			"-U", user,
			"-c", `\copy bench_copy TO '/dev/null'`,
		)
		cmd.Env = append(os.Environ(), fmt.Sprintf("PGPASSWORD=%s", password))
		output, err := cmd.CombinedOutput()
		if err != nil {
			return fmt.Errorf("psql copy out failed: %w: %s", err, string(output))
		}
		return nil
	}
}

// makePsqlCopyInTask creates a psql COPY IN task for a given connection getter
func makePsqlCopyInTask(getConnParams func(s *BenchmarkSuite) (host string, port int, dbname, user, password string)) func(ctx context.Context, s *BenchmarkSuite, pool *pgxpool.Pool) error {
	return func(ctx context.Context, s *BenchmarkSuite, pool *pgxpool.Pool) error {
		host, port, dbname, user, password := getConnParams(s)

		// Run \copy from file and truncate in a single psql session
		// Use bench_copy as target since it already exists
		cmd := exec.CommandContext(ctx, "psql",
			"-h", host,
			"-p", fmt.Sprintf("%d", port),
			"-d", dbname,
			"-U", user,
			"-c", fmt.Sprintf(`\copy bench_copy FROM '%s'; TRUNCATE bench_copy;`, s.copyDataFile),
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

// buildStandardTargets creates the standard set of targets (direct, pgbouncer, pglink variants)
func (s *BenchmarkSuite) buildStandardTargets(maxConns, concurrency int, task func(ctx context.Context, s *BenchmarkSuite, pool *pgxpool.Pool) error) []*BenchmarkTarget {
	targets := []*BenchmarkTarget{
		makeDirectTarget("direct", maxConns, concurrency, task),
		makePgbouncerTarget("pgbouncer", maxConns, concurrency, task),
	}

	// Add pglink targets for each GOMAXPROCS value
	for _, gmp := range s.runCfg.GoMaxProcs {
		targets = append(targets, makePglinkTarget("pglink", maxConns, concurrency, gmp, task))
	}

	return targets
}

// buildPsqlTargets creates psql-based targets
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
			Name:        "pgbouncer (psql)",
			Concurrency: concurrency,
			SetUp: func(ctx context.Context, s *BenchmarkSuite) error {
				return s.startPgbouncer(ctx, s.currentPglinkConfig)
			},
			TearDown: func(ctx context.Context, s *BenchmarkSuite) error {
				s.stopPgbouncer()
				return nil
			},
			Task: taskMaker(pgbouncerParams),
		},
	}

	// Add pglink targets
	for _, gmp := range s.runCfg.GoMaxProcs {
		gmpCopy := gmp
		targets = append(targets, &BenchmarkTarget{
			Name:        fmt.Sprintf("pglink (psql, GOMAXPROCS=%d)", gmp),
			Concurrency: concurrency,
			SetUp: func(ctx context.Context, s *BenchmarkSuite) error {
				return s.startPglink(ctx, s.currentPglinkConfig, gmpCopy)
			},
			TearDown: func(ctx context.Context, s *BenchmarkSuite) error {
				s.stopPglink()
				return nil
			},
			Task: taskMaker(pglinkParams),
		})
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

			// COPY OUT (pgx)
			cases = append(cases, &BenchmarkCase{
				Title:       fmt.Sprintf("COPY OUT pgx (%d rows, max_conns=%d, concurrency=%d)", copyRowCount, mc, cc),
				Description: "COPY TO STDOUT benchmark using pgx",
				SetUp:       s.setupCopyTable,
				Targets:     s.buildStandardTargets(mc, cc, taskCopyOut),
			})

			// COPY IN (pgx)
			cases = append(cases, &BenchmarkCase{
				Title:       fmt.Sprintf("COPY IN pgx (%d rows, max_conns=%d, concurrency=%d)", copyRowCount, mc, cc),
				Description: "COPY FROM STDIN benchmark using pgx",
				SetUp:       s.setupCopyTable,
				Targets:     s.buildStandardTargets(mc, cc, taskCopyIn),
			})

			// COPY OUT (psql)
			cases = append(cases, &BenchmarkCase{
				Title:       fmt.Sprintf("COPY OUT psql (%d rows, concurrency=%d)", copyRowCount, cc),
				Description: "COPY TO STDOUT benchmark using psql CLI",
				SetUp:       s.setupCopyTable,
				Targets:     s.buildPsqlTargets(cc, makePsqlCopyOutTask),
			})

			// COPY IN (psql)
			cases = append(cases, &BenchmarkCase{
				Title:       fmt.Sprintf("COPY IN psql (%d rows, concurrency=%d)", copyRowCount, cc),
				Description: "COPY FROM STDIN benchmark using psql CLI",
				SetUp:       s.setupCopyTable,
				Targets:     s.buildPsqlTargets(cc, makePsqlCopyInTask),
			})
		}
	}

	return cases
}

// ============================================================================
// Setup and Teardown
// ============================================================================

func (s *BenchmarkSuite) setupCopyTable(ctx context.Context, _ *BenchmarkSuite) error {
	// Connect as postgres (superuser) to create the table
	connStr := fmt.Sprintf("postgres://postgres:postgres@localhost:%d/uno?sslmode=disable", s.directPort)
	conn, err := pgx.Connect(ctx, connStr)
	if err != nil {
		return fmt.Errorf("failed to connect for setup: %w", err)
	}
	defer conn.Close(ctx)

	// Create benchmark table for COPY operations and grant access to app user
	_, err = conn.Exec(ctx, `
		DROP TABLE IF EXISTS bench_copy;
		CREATE TABLE bench_copy (
			id INTEGER,
			name TEXT,
			value INTEGER
		);
		GRANT ALL ON bench_copy TO app;
	`)
	if err != nil {
		return fmt.Errorf("failed to create bench_copy table: %w", err)
	}

	// Pre-populate with data for COPY OUT benchmarks
	_, err = conn.Exec(ctx, `
		INSERT INTO bench_copy (id, name, value)
		SELECT i, 'row_' || i::text, i * 10
		FROM generate_series(1, $1) AS i;
	`, copyRowCount)
	if err != nil {
		return fmt.Errorf("failed to populate bench_copy table: %w", err)
	}

	return nil
}

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
	defer tmpFile.Close()
	if _, err := tmpFile.Write(s.copyData); err != nil {
		os.Remove(tmpFile.Name())
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

func (s *BenchmarkSuite) startPglink(ctx context.Context, cfg *config.Config, gomaxprocs int) error {
	tmpDir, err := os.MkdirTemp("", "pglink-bench-*")
	if err != nil {
		return err
	}

	configPath := filepath.Join(tmpDir, "pglink.json")
	configData, err := json.MarshalIndent(cfg, "", "  ")
	if err != nil {
		os.RemoveAll(tmpDir)
		return err
	}
	if err := os.WriteFile(configPath, configData, 0644); err != nil {
		os.RemoveAll(tmpDir)
		return err
	}

	pglinkBinary := filepath.Join(s.projectDir, "out", "pglink")
	if _, err := os.Stat(pglinkBinary); os.IsNotExist(err) {
		buildCmd := exec.CommandContext(ctx, filepath.Join(s.projectDir, "bin", "build"))
		buildCmd.Dir = s.projectDir
		if err := buildCmd.Run(); err != nil {
			os.RemoveAll(tmpDir)
			return fmt.Errorf("failed to build pglink: %w", err)
		}
	}

	s.pglinkCmd = exec.Command(pglinkBinary, "-config", configPath)
	s.pglinkCmd.Env = append(os.Environ(), fmt.Sprintf("GOMAXPROCS=%d", gomaxprocs))
	s.pglinkCmd.Dir = tmpDir

	if err := s.pglinkCmd.Start(); err != nil {
		os.RemoveAll(tmpDir)
		return fmt.Errorf("failed to start pglink: %w", err)
	}

	// Wait for pglink to be ready
	return s.waitForPort(ctx, s.pglinkPort, 10*time.Second)
}

func (s *BenchmarkSuite) stopPglink() {
	if s.pglinkCmd != nil && s.pglinkCmd.Process != nil {
		s.pglinkCmd.Process.Signal(syscall.SIGTERM)
		done := make(chan error, 1)
		go func() { done <- s.pglinkCmd.Wait() }()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			s.pglinkCmd.Process.Kill()
		}
		s.pglinkCmd = nil
	}
}

func (s *BenchmarkSuite) startPgbouncer(ctx context.Context, cfg *config.Config) error {
	tmpDir, err := os.MkdirTemp("", "pgbouncer-bench-*")
	if err != nil {
		return err
	}

	secrets := config.NewSecretCache(nil)
	pgbCfg, err := pgbouncer.GenerateConfig(ctx, cfg, secrets, s.pgbouncerPort)
	if err != nil {
		os.RemoveAll(tmpDir)
		return fmt.Errorf("failed to generate pgbouncer config: %w", err)
	}
	if err := pgbCfg.WriteToDir(tmpDir); err != nil {
		os.RemoveAll(tmpDir)
		return fmt.Errorf("failed to write pgbouncer config: %w", err)
	}

	s.pgbouncerCmd = exec.Command("pgbouncer", filepath.Join(tmpDir, "pgbouncer.ini"))
	s.pgbouncerCmd.Dir = tmpDir

	if err := s.pgbouncerCmd.Start(); err != nil {
		os.RemoveAll(tmpDir)
		return fmt.Errorf("failed to start pgbouncer: %w", err)
	}

	return s.waitForPort(ctx, s.pgbouncerPort, 10*time.Second)
}

func (s *BenchmarkSuite) stopPgbouncer() {
	if s.pgbouncerCmd != nil && s.pgbouncerCmd.Process != nil {
		s.pgbouncerCmd.Process.Signal(syscall.SIGTERM)
		done := make(chan error, 1)
		go func() { done <- s.pgbouncerCmd.Wait() }()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			s.pgbouncerCmd.Process.Kill()
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
			conn.Close(ctx)
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
	defer os.Remove(s.copyDataFile)

	// Generate config for the first max_conns value (will be regenerated per case if needed)
	if len(s.runCfg.MaxConns) > 0 {
		cfg, err := s.generatePglinkConfig(s.runCfg.MaxConns[0])
		if err != nil {
			return fmt.Errorf("failed to generate pglink config: %w", err)
		}
		s.currentPglinkConfig = cfg
	}

	cases := s.buildAllBenchmarkCases()
	log.Printf("Running %d benchmark cases...", len(cases))

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

		// Run each target
		for _, target := range bc.Targets {
			result, err := s.runTarget(ctx, bc, target)
			if err != nil {
				log.Printf("  %s: ERROR: %v", target.Name, err)
				continue
			}
			s.results = append(s.results, result)
			log.Printf("  %s: %.0f qps, %.2f%% errors, p50=%.0fus, p99=%.0fus",
				target.Name, result.QueriesPerSec, result.ErrorRate*100,
				result.P50LatencyUs, result.P99LatencyUs)
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

func (s *BenchmarkSuite) runTarget(ctx context.Context, bc *BenchmarkCase, target *BenchmarkTarget) (BenchmarkResult, error) {
	result := BenchmarkResult{
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

	// Ensure teardown runs
	defer func() {
		if target.TearDown != nil {
			target.TearDown(ctx, s)
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

- **Duration per test:** {{.Duration}}
- **Warmup:** {{.Warmup}}
- **Max connections tested:** {{.MaxConns}}
- **Concurrency levels:** {{.Concurrency}}
- **GOMAXPROCS (pglink):** {{.GoMaxProcs}}

## Results
{{range .Cases}}
### {{.Title}}
{{if .Description}}
{{.Description}}
{{end}}
| Target | Max Conns | Concurrency | QPS | Error Rate | P50 (us) | P99 (us) |
|--------|-----------|-------------|-----|------------|----------|----------|
{{range .Results -}}
| {{.TargetName}} | {{if .MaxConns}}{{.MaxConns}}{{else}}-{{end}} | {{.Concurrency}} | {{printf "%.0f" .QueriesPerSec}} | {{printf "%.2f%%" (mul .ErrorRate 100)}} | {{printf "%.0f" .P50LatencyUs}} | {{printf "%.0f" .P99LatencyUs}} |
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
		cases = append(cases, CaseData{
			Title:   title,
			Results: caseMap[title],
		})
	}

	data := struct {
		Timestamp    string
		Duration     string
		Warmup       string
		MaxConns     string
		Concurrency  string
		GoMaxProcs   string
		Cases        []CaseData
		ErrorSamples []string
		GoVersion    string
		OS           string
		Arch         string
		NumCPU       int
	}{
		Timestamp:    time.Now().Format(time.RFC3339),
		Duration:     s.runCfg.Duration.String(),
		Warmup:       s.runCfg.Warmup.String(),
		MaxConns:     fmt.Sprintf("%v", s.runCfg.MaxConns),
		Concurrency:  fmt.Sprintf("%v", s.runCfg.Concurrency),
		GoMaxProcs:   fmt.Sprintf("%v", s.runCfg.GoMaxProcs),
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
	defer f.Close()

	return t.Execute(f, data)
}

// ============================================================================
// Main
// ============================================================================

func main() {
	duration := flag.Duration("duration", 10*time.Second, "Duration of each benchmark")
	warmup := flag.Duration("warmup", 2*time.Second, "Warmup duration before measuring")
	outputDir := flag.String("output", "out/benchmark", "Output directory for results")
	maxConnsStr := flag.String("max-conns", "100,500,1000", "Comma-separated max connection values")
	concurrencyStr := flag.String("concurrency", "100,200,500,600,1000,1500", "Comma-separated concurrency values")
	gomaxprocsStr := flag.String("gomaxprocs", "1,2,4,8,16", "Comma-separated GOMAXPROCS values for pglink")
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
		MaxConns:    parseInts(*maxConnsStr),
		Concurrency: parseInts(*concurrencyStr),
		GoMaxProcs:  parseInts(*gomaxprocsStr),
		Duration:    *duration,
		Warmup:      *warmup,
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
