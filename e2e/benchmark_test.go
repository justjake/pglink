package e2e

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/justjake/pglink/pkg/config"
	"github.com/justjake/pglink/pkg/config/pgbouncer"
)

// BenchmarkTarget represents a connection target for benchmarking
type BenchmarkTarget struct {
	Name    string
	ConnStr string
}

// getBenchmarkTargets returns the list of targets to benchmark against.
// Always includes direct postgres and pglink.
// Optionally includes pgbouncer if available.
func getBenchmarkTargets(t testing.TB) []BenchmarkTarget {
	t.Helper()

	targets := []BenchmarkTarget{
		{
			Name:    "direct",
			ConnStr: "postgres://app:app_password@localhost:15432/uno?sslmode=disable",
		},
		{
			Name:    "pglink",
			ConnStr: fmt.Sprintf("postgres://app:app_password@localhost:%d/alpha_uno?sslmode=disable", DefaultPglinkPort),
		},
	}

	// Check if pgbouncer is running
	pgbouncerPort := 16433
	pgbouncerConnStr := fmt.Sprintf("postgres://app:app_password@localhost:%d/alpha_uno?sslmode=disable", pgbouncerPort)
	conn, err := pgx.Connect(context.Background(), pgbouncerConnStr)
	if err == nil {
		conn.Close(context.Background())
		targets = append(targets, BenchmarkTarget{
			Name:    "pgbouncer",
			ConnStr: pgbouncerConnStr,
		})
	} else {
		t.Logf("pgbouncer not available at port %d, skipping pgbouncer benchmarks", pgbouncerPort)
	}

	return targets
}

// newBenchPool creates a connection pool for benchmarking.
// Disables statement caching for transaction pooling compatibility.
func newBenchPool(ctx context.Context, connStr string, maxConns int32) (*pgxpool.Pool, error) {
	poolConfig, err := pgxpool.ParseConfig(connStr)
	if err != nil {
		return nil, err
	}

	poolConfig.MaxConns = maxConns
	poolConfig.MinConns = maxConns // Keep pool hot

	// Disable statement caching for transaction pooling compatibility
	poolConfig.ConnConfig.DefaultQueryExecMode = pgx.QueryExecModeDescribeExec
	poolConfig.ConnConfig.StatementCacheCapacity = 0
	poolConfig.ConnConfig.DescriptionCacheCapacity = 0

	return pgxpool.NewWithConfig(ctx, poolConfig)
}

// =============================================================================
// SELECT 1 Benchmarks - Measure pure proxy overhead
// =============================================================================

// BenchmarkSelect1_Serial measures latency of SELECT 1 queries in serial
func BenchmarkSelect1_Serial(b *testing.B) {
	// Note: benchmarks use the same harness as tests, started by TestMain
	ctx := context.Background()

	targets := getBenchmarkTargets(b)

	for _, target := range targets {
		b.Run(target.Name, func(b *testing.B) {
			pool, err := newBenchPool(ctx, target.ConnStr, 1)
			if err != nil {
				b.Fatalf("failed to create pool: %v", err)
			}
			defer pool.Close()

			// Warm up
			var result int
			if err := pool.QueryRow(ctx, "SELECT 1").Scan(&result); err != nil {
				b.Fatalf("warmup failed: %v", err)
			}

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if err := pool.QueryRow(ctx, "SELECT 1").Scan(&result); err != nil {
					b.Fatalf("query failed: %v", err)
				}
			}
		})
	}
}

// BenchmarkSelect1_Parallel measures throughput of SELECT 1 queries with parallelism
func BenchmarkSelect1_Parallel(b *testing.B) {
	ctx := context.Background()

	targets := getBenchmarkTargets(b)
	parallelism := []int{1, 2, 4, 8, 16}

	for _, target := range targets {
		for _, p := range parallelism {
			b.Run(fmt.Sprintf("%s/p%d", target.Name, p), func(b *testing.B) {
				pool, err := newBenchPool(ctx, target.ConnStr, int32(p))
				if err != nil {
					b.Fatalf("failed to create pool: %v", err)
				}
				defer pool.Close()

				// Warm up
				var result int
				if err := pool.QueryRow(ctx, "SELECT 1").Scan(&result); err != nil {
					b.Fatalf("warmup failed: %v", err)
				}

				b.ResetTimer()
				b.SetParallelism(p)
				b.RunParallel(func(pb *testing.PB) {
					var r int
					for pb.Next() {
						if err := pool.QueryRow(ctx, "SELECT 1").Scan(&r); err != nil {
							b.Errorf("query failed: %v", err)
						}
					}
				})
			})
		}
	}
}

// =============================================================================
// Real Query Benchmarks - Measure realistic workload performance
// =============================================================================

// BenchmarkSelectRows_Serial measures latency of queries returning multiple rows
func BenchmarkSelectRows_Serial(b *testing.B) {
	ctx := context.Background()

	targets := getBenchmarkTargets(b)
	rowCounts := []int{10, 100, 1000}

	for _, target := range targets {
		for _, rows := range rowCounts {
			b.Run(fmt.Sprintf("%s/rows%d", target.Name, rows), func(b *testing.B) {
				pool, err := newBenchPool(ctx, target.ConnStr, 1)
				if err != nil {
					b.Fatalf("failed to create pool: %v", err)
				}
				defer pool.Close()

				// Warm up
				pgRows, err := pool.Query(ctx, "SELECT generate_series(1, $1)", rows)
				if err != nil {
					b.Fatalf("warmup query failed: %v", err)
				}
				pgRows.Close()

				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					pgRows, err := pool.Query(ctx, "SELECT generate_series(1, $1)", rows)
					if err != nil {
						b.Fatalf("query failed: %v", err)
					}
					count := 0
					for pgRows.Next() {
						count++
					}
					pgRows.Close()
					if count != rows {
						b.Fatalf("expected %d rows, got %d", rows, count)
					}
				}
			})
		}
	}
}

// BenchmarkSelectRows_Parallel measures throughput of row-returning queries
func BenchmarkSelectRows_Parallel(b *testing.B) {
	ctx := context.Background()

	targets := getBenchmarkTargets(b)

	for _, target := range targets {
		b.Run(fmt.Sprintf("%s/rows100/p8", target.Name), func(b *testing.B) {
			pool, err := newBenchPool(ctx, target.ConnStr, 8)
			if err != nil {
				b.Fatalf("failed to create pool: %v", err)
			}
			defer pool.Close()

			b.ResetTimer()
			b.SetParallelism(8)
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					pgRows, err := pool.Query(ctx, "SELECT generate_series(1, 100)")
					if err != nil {
						b.Errorf("query failed: %v", err)
						continue
					}
					for pgRows.Next() {
					}
					pgRows.Close()
				}
			})
		})
	}
}

// BenchmarkTransaction_Serial measures latency of simple transactions
func BenchmarkTransaction_Serial(b *testing.B) {
	ctx := context.Background()

	targets := getBenchmarkTargets(b)

	for _, target := range targets {
		b.Run(target.Name, func(b *testing.B) {
			pool, err := newBenchPool(ctx, target.ConnStr, 1)
			if err != nil {
				b.Fatalf("failed to create pool: %v", err)
			}
			defer pool.Close()

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				tx, err := pool.Begin(ctx)
				if err != nil {
					b.Fatalf("begin failed: %v", err)
				}
				var result int
				if err := tx.QueryRow(ctx, "SELECT 1").Scan(&result); err != nil {
					tx.Rollback(ctx)
					b.Fatalf("query failed: %v", err)
				}
				if err := tx.Commit(ctx); err != nil {
					b.Fatalf("commit failed: %v", err)
				}
			}
		})
	}
}

// BenchmarkTransaction_Parallel measures throughput of transactions with parallelism
func BenchmarkTransaction_Parallel(b *testing.B) {
	ctx := context.Background()

	targets := getBenchmarkTargets(b)

	for _, target := range targets {
		b.Run(fmt.Sprintf("%s/p8", target.Name), func(b *testing.B) {
			pool, err := newBenchPool(ctx, target.ConnStr, 8)
			if err != nil {
				b.Fatalf("failed to create pool: %v", err)
			}
			defer pool.Close()

			b.ResetTimer()
			b.SetParallelism(8)
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					tx, err := pool.Begin(ctx)
					if err != nil {
						b.Errorf("begin failed: %v", err)
						continue
					}
					var result int
					if err := tx.QueryRow(ctx, "SELECT 1").Scan(&result); err != nil {
						tx.Rollback(ctx)
						b.Errorf("query failed: %v", err)
						continue
					}
					if err := tx.Commit(ctx); err != nil {
						b.Errorf("commit failed: %v", err)
					}
				}
			})
		})
	}
}

// =============================================================================
// Connection Benchmark - Measure connection establishment overhead
// =============================================================================

// BenchmarkConnect measures connection establishment time
func BenchmarkConnect(b *testing.B) {
	ctx := context.Background()

	targets := getBenchmarkTargets(b)

	for _, target := range targets {
		b.Run(target.Name, func(b *testing.B) {
			connConfig, err := pgx.ParseConfig(target.ConnStr)
			if err != nil {
				b.Fatalf("failed to parse config: %v", err)
			}

			// Disable statement caching
			connConfig.DefaultQueryExecMode = pgx.QueryExecModeDescribeExec
			connConfig.StatementCacheCapacity = 0
			connConfig.DescriptionCacheCapacity = 0

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				conn, err := pgx.ConnectConfig(ctx, connConfig)
				if err != nil {
					b.Fatalf("connect failed: %v", err)
				}
				conn.Close(ctx)
			}
		})
	}
}

// =============================================================================
// Latency Distribution Benchmark - For detailed latency analysis
// =============================================================================

// BenchmarkLatencyDistribution runs a fixed number of queries and reports statistics
func BenchmarkLatencyDistribution(b *testing.B) {
	ctx := context.Background()

	targets := getBenchmarkTargets(b)

	for _, target := range targets {
		b.Run(target.Name, func(b *testing.B) {
			pool, err := newBenchPool(ctx, target.ConnStr, 1)
			if err != nil {
				b.Fatalf("failed to create pool: %v", err)
			}
			defer pool.Close()

			// Collect latency samples
			latencies := make([]time.Duration, b.N)
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				start := time.Now()
				var result int
				if err := pool.QueryRow(ctx, "SELECT 1").Scan(&result); err != nil {
					b.Fatalf("query failed: %v", err)
				}
				latencies[i] = time.Since(start)
			}

			b.StopTimer()

			// Report latency statistics
			if len(latencies) > 0 {
				var total time.Duration
				min, max := latencies[0], latencies[0]
				for _, l := range latencies {
					total += l
					if l < min {
						min = l
					}
					if l > max {
						max = l
					}
				}
				avg := total / time.Duration(len(latencies))
				b.ReportMetric(float64(avg.Microseconds()), "avg_us")
				b.ReportMetric(float64(min.Microseconds()), "min_us")
				b.ReportMetric(float64(max.Microseconds()), "max_us")
			}
		})
	}
}

// =============================================================================
// PgBouncer Helper - Start pgbouncer for benchmarks
// =============================================================================

// PgBouncerProcess manages a pgbouncer process for benchmarking
type PgBouncerProcess struct {
	cmd     *exec.Cmd
	cfgDir  string
	port    int
	started bool
	mu      sync.Mutex
}

// StartPgBouncer starts a pgbouncer process using the pglink config.
// Returns nil if pgbouncer is not installed.
func StartPgBouncer(ctx context.Context, pglinkConfig *config.Config, secrets *config.SecretCache, port int) (*PgBouncerProcess, error) {
	// Check if pgbouncer is installed
	pgbouncerPath, err := exec.LookPath("pgbouncer")
	if err != nil {
		return nil, fmt.Errorf("pgbouncer not found: %w", err)
	}

	// Create temp directory for config
	cfgDir, err := os.MkdirTemp("", "pgbouncer-bench-*")
	if err != nil {
		return nil, fmt.Errorf("failed to create temp dir: %w", err)
	}

	// Generate pgbouncer config
	pgbCfg, err := pgbouncer.GenerateConfig(ctx, pglinkConfig, secrets, port)
	if err != nil {
		os.RemoveAll(cfgDir)
		return nil, fmt.Errorf("failed to generate config: %w", err)
	}

	if err := pgbCfg.WriteToDir(cfgDir); err != nil {
		os.RemoveAll(cfgDir)
		return nil, fmt.Errorf("failed to write config: %w", err)
	}

	// Start pgbouncer
	iniPath := filepath.Join(cfgDir, "pgbouncer.ini")
	cmd := exec.CommandContext(ctx, pgbouncerPath, iniPath)
	cmd.Dir = cfgDir // So it finds userlist.txt
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	if err := cmd.Start(); err != nil {
		os.RemoveAll(cfgDir)
		return nil, fmt.Errorf("failed to start pgbouncer: %w", err)
	}

	p := &PgBouncerProcess{
		cmd:     cmd,
		cfgDir:  cfgDir,
		port:    port,
		started: true,
	}

	// Wait for pgbouncer to be ready
	connStr := fmt.Sprintf("postgres://app:app_password@localhost:%d/alpha_uno?sslmode=disable", port)
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		conn, err := pgx.Connect(ctx, connStr)
		if err == nil {
			conn.Close(ctx)
			return p, nil
		}
		time.Sleep(100 * time.Millisecond)
	}

	// Failed to connect - clean up
	p.Stop()
	return nil, fmt.Errorf("pgbouncer failed to become ready")
}

// Stop stops the pgbouncer process and cleans up
func (p *PgBouncerProcess) Stop() {
	p.mu.Lock()
	defer p.mu.Unlock()

	if !p.started {
		return
	}
	p.started = false

	if p.cmd != nil && p.cmd.Process != nil {
		p.cmd.Process.Kill()
		p.cmd.Wait()
	}

	if p.cfgDir != "" {
		os.RemoveAll(p.cfgDir)
	}
}

// Port returns the port pgbouncer is listening on
func (p *PgBouncerProcess) Port() int {
	return p.port
}
