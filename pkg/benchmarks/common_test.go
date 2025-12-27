// Package benchmarks contains standard Go benchmarks for pglink performance testing.
//
// These benchmarks are designed to be run via the orchestrator (cmd/bench) which sets
// up the appropriate environment variables:
//
//   - BENCH_CONN_STRING: PostgreSQL connection string
//   - BENCH_TARGET: Target name for sub-benchmark naming
//   - BENCH_MAX_CONNS: Maximum pool connections
//   - BENCH_CONCURRENCY: Number of concurrent workers
//   - BENCH_DURATION: Duration of each measurement
//   - BENCH_WARMUP: Warmup duration before measuring
//   - BENCH_SIMPLE_QUERY: If "true", use simple query protocol
//   - BENCH_SEED: Random seed for reproducibility
package benchmarks

import (
	"context"
	"fmt"
	"log"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

var (
	// pool is the shared connection pool for all benchmarks.
	pool *pgxpool.Pool

	// benchConfig holds configuration loaded from environment.
	benchConfig struct {
		ConnString      string
		Target          string
		MaxConns        int
		Concurrency     int
		Duration        time.Duration
		Warmup          time.Duration
		SimpleQueryMode bool
		Seed            int64
		RunID           string
		Round           int
		TotalRounds     int
	}
)

func TestMain(m *testing.M) {
	// Load configuration from environment
	benchConfig.ConnString = os.Getenv("BENCH_CONN_STRING")
	if benchConfig.ConnString == "" {
		// Skip benchmarks if not configured (running outside orchestrator)
		log.Println("BENCH_CONN_STRING not set, skipping benchmarks")
		os.Exit(0)
	}

	benchConfig.Target = os.Getenv("BENCH_TARGET")
	if benchConfig.Target == "" {
		benchConfig.Target = "unknown"
	}

	if v := os.Getenv("BENCH_MAX_CONNS"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			benchConfig.MaxConns = n
		}
	}
	if benchConfig.MaxConns == 0 {
		benchConfig.MaxConns = 100
	}

	if v := os.Getenv("BENCH_CONCURRENCY"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			benchConfig.Concurrency = n
		}
	}
	if benchConfig.Concurrency == 0 {
		benchConfig.Concurrency = 100
	}

	if v := os.Getenv("BENCH_DURATION"); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			benchConfig.Duration = d
		}
	}
	if benchConfig.Duration == 0 {
		benchConfig.Duration = 15 * time.Second
	}

	if v := os.Getenv("BENCH_WARMUP"); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			benchConfig.Warmup = d
		}
	}
	if benchConfig.Warmup == 0 {
		benchConfig.Warmup = 5 * time.Second
	}

	benchConfig.SimpleQueryMode = os.Getenv("BENCH_SIMPLE_QUERY") == "true"

	if v := os.Getenv("BENCH_SEED"); v != "" {
		if n, err := strconv.ParseInt(v, 10, 64); err == nil {
			benchConfig.Seed = n
		}
	}

	benchConfig.RunID = os.Getenv("BENCH_RUN_ID")

	if v := os.Getenv("BENCH_ROUND"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			benchConfig.Round = n
		}
	}

	if v := os.Getenv("BENCH_TOTAL_ROUNDS"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			benchConfig.TotalRounds = n
		}
	}

	// Create connection pool
	ctx := context.Background()
	poolConfig, err := pgxpool.ParseConfig(benchConfig.ConnString)
	if err != nil {
		log.Fatalf("Failed to parse connection string: %v", err)
	}

	poolConfig.MaxConns = int32(benchConfig.MaxConns)

	pool, err = pgxpool.NewWithConfig(ctx, poolConfig)
	if err != nil {
		log.Fatalf("Failed to create connection pool: %v", err)
	}
	defer pool.Close()

	// Verify connection
	if err := pool.Ping(ctx); err != nil {
		log.Fatalf("Failed to ping database: %v", err)
	}

	log.Printf("Connected to database, target=%s, max_conns=%d, concurrency=%d",
		benchConfig.Target, benchConfig.MaxConns, benchConfig.Concurrency)

	// Print benchstat-compatible configuration header
	// These key: value lines are preserved by benchstat and appear in output
	printBenchConfig()

	// Run benchmarks
	os.Exit(m.Run())
}

// getPool returns the shared connection pool.
func getPool() *pgxpool.Pool {
	return pool
}

// getBenchName returns a benchmark sub-name with target and config metadata.
// Format: target=<target>/conns=<max_conns>/workers=<concurrency>
func getBenchName() string {
	return fmt.Sprintf("target=%s/conns=%d/workers=%d",
		benchConfig.Target,
		benchConfig.MaxConns,
		benchConfig.Concurrency)
}

// printBenchConfig prints benchmark configuration in benchstat-compatible format.
// These "key: value" lines appear in the output header and are preserved by benchstat.
func printBenchConfig() {
	// Print config header - benchstat preserves these
	fmt.Printf("target: %s\n", benchConfig.Target)
	fmt.Printf("duration: %s\n", benchConfig.Duration)
	fmt.Printf("warmup: %s\n", benchConfig.Warmup)
	fmt.Printf("max_conns: %d\n", benchConfig.MaxConns)
	fmt.Printf("concurrency: %d\n", benchConfig.Concurrency)
	if benchConfig.SimpleQueryMode {
		fmt.Printf("protocol: simple\n")
	} else {
		fmt.Printf("protocol: extended\n")
	}
	if benchConfig.Seed != 0 {
		fmt.Printf("seed: %d\n", benchConfig.Seed)
	}
	if benchConfig.RunID != "" {
		fmt.Printf("run_id: %s\n", benchConfig.RunID)
	}
	if benchConfig.TotalRounds > 0 {
		fmt.Printf("round: %d/%d\n", benchConfig.Round, benchConfig.TotalRounds)
	}
}
