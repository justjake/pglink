package benchmarks

import (
	"context"
	"fmt"
	"math/rand"
	"sync/atomic"
	"testing"

	"github.com/jackc/pgx/v5"
)

// BenchmarkMixed runs a mixed workload of different query types.
// This simulates a more realistic application workload with:
// - Simple SELECT queries
// - INSERT/UPDATE/DELETE operations
// - Transactions
//
// Concurrency is controlled by the -cpu flag.
// Connection management is controlled by BENCH_CONNECT_MODE (per-worker or per-op).
// workerID is used to give each parallel worker a unique RNG seed.
var workerID atomic.Int64

func BenchmarkMixed(b *testing.B) {
	b.Run(getBenchName(), func(b *testing.B) {
		benchCtx := b.Context()

		// Use seed for reproducibility
		seed := benchConfig.Seed
		if seed == 0 {
			seed = 12345 // Default seed for consistency
		}

		// Track total queries across all workers
		var totalQueries atomic.Int64

		b.RunParallel(func(pb *testing.PB) {
			// Each worker gets its own pool
			pool, err := GetTestPool(b, benchCtx)
			if err != nil {
				b.Fatalf("failed to create pool: %v", err)
			}
			defer func() {
				if err := pool.Close(); err != nil {
					b.Errorf("pool close error: %v", err)
				}
			}()

			// Per-worker RNG with unique seed
			rng := rand.New(rand.NewSource(seed + workerID.Add(1)))

			var i int
			for pb.Next() {
				op := NewOp(benchCtx, "mixed task", i)

				conn, err := pool.Acquire(op.Ctx)
				if err != nil {
					b.Fatal(op.Failed(err))
				}

				// Track queries for this iteration
				var queries int64

				// Ensure temp table exists (needed for loop mode with fresh connections)
				_, err = conn.Exec(op.Ctx, `
					CREATE TEMP TABLE IF NOT EXISTS bench_mixed (
						id SERIAL PRIMARY KEY,
						name TEXT NOT NULL,
						value INT NOT NULL,
						created_at TIMESTAMP DEFAULT NOW()
					)
				`)
				if err != nil {
					conn.Release()
					b.Fatal(op.Failed(fmt.Errorf("create table: %w", err)))
				}
				queries++

				// Ensure we have seed data (for fresh connections in loop mode)
				var count int
				if err := conn.QueryRow(op.Ctx, `SELECT COUNT(*) FROM bench_mixed`).Scan(&count); err != nil {
					conn.Release()
					b.Fatal(op.Failed(fmt.Errorf("count: %w", err)))
				}
				queries++

				if count < 100 {
					for j := count; j < 100; j++ {
						_, err = conn.Exec(op.Ctx, `INSERT INTO bench_mixed (name, value) VALUES ($1, $2)`,
							fmt.Sprintf("item_%d", j), j*10)
						if err != nil {
							conn.Release()
							b.Fatal(op.Failed(fmt.Errorf("seed: %w", err)))
						}
						queries++
					}
				}

				taskQueries, err := runMixedTask(op.Ctx, conn.Conn, rng)
				if err != nil {
					conn.Release()
					b.Fatal(op.Failed(err))
				}
				queries += taskQueries

				conn.Release()
				op.Done()
				totalQueries.Add(queries)
				i++
			}
		})
		ReportThroughputWithQueries(b, totalQueries.Load())
	})
}

// runMixedTask runs a single mixed workload task.
// Returns the number of queries executed.
func runMixedTask(ctx context.Context, conn *pgx.Conn, rng *rand.Rand) (int64, error) {
	var queries int64

	// Mix of operations: 40% SELECT, 20% INSERT, 20% UPDATE, 10% DELETE, 10% Transaction
	for j := 0; j < 20; j++ {
		op := rng.Intn(100)

		switch {
		case op < 40: // SELECT (1 query)
			rows, err := conn.Query(ctx, `SELECT id, name, value FROM bench_mixed WHERE id = $1`, rng.Intn(100)+1)
			if err != nil {
				return queries, fmt.Errorf("select: %w", err)
			}
			for rows.Next() {
				var id, value int
				var name string
				if err := rows.Scan(&id, &name, &value); err != nil {
					rows.Close()
					return queries, fmt.Errorf("scan: %w", err)
				}
			}
			rows.Close()
			if err := rows.Err(); err != nil {
				return queries, fmt.Errorf("rows: %w", err)
			}
			queries++

		case op < 60: // INSERT (1 query)
			_, err := conn.Exec(ctx, `INSERT INTO bench_mixed (name, value) VALUES ($1, $2)`,
				fmt.Sprintf("new_%d", rng.Int()), rng.Intn(1000))
			if err != nil {
				return queries, fmt.Errorf("insert: %w", err)
			}
			queries++

		case op < 80: // UPDATE (1 query)
			_, err := conn.Exec(ctx, `UPDATE bench_mixed SET value = $1 WHERE id = $2`,
				rng.Intn(1000), rng.Intn(100)+1)
			if err != nil {
				return queries, fmt.Errorf("update: %w", err)
			}
			queries++

		case op < 90: // DELETE (1 query)
			_, _ = conn.Exec(ctx, `DELETE FROM bench_mixed WHERE id > 100 AND id = $1`, rng.Intn(1000)+100)
			queries++

		default: // Transaction (4 queries: BEGIN, INSERT, UPDATE, COMMIT)
			tx, err := conn.Begin(ctx)
			if err != nil {
				return queries, fmt.Errorf("begin: %w", err)
			}
			queries++ // BEGIN

			_, err = tx.Exec(ctx, `INSERT INTO bench_mixed (name, value) VALUES ($1, $2)`,
				fmt.Sprintf("tx_%d", rng.Int()), rng.Intn(1000))
			if err != nil {
				tx.Rollback(ctx)
				queries++ // ROLLBACK
				return queries, fmt.Errorf("tx insert: %w", err)
			}
			queries++ // INSERT

			_, err = tx.Exec(ctx, `UPDATE bench_mixed SET value = value + 1 WHERE id = $1`, rng.Intn(100)+1)
			if err != nil {
				tx.Rollback(ctx)
				queries++ // ROLLBACK
				return queries, fmt.Errorf("tx update: %w", err)
			}
			queries++ // UPDATE

			if err := tx.Commit(ctx); err != nil {
				return queries, fmt.Errorf("commit: %w", err)
			}
			queries++ // COMMIT
		}
	}

	return queries, nil
}
