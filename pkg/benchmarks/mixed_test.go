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
// Connection management is controlled by BENCH_POOL_MODE (worker or loop).
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

				// Ensure we have seed data (for fresh connections in loop mode)
				var count int
				if err := conn.QueryRow(op.Ctx, `SELECT COUNT(*) FROM bench_mixed`).Scan(&count); err != nil {
					conn.Release()
					b.Fatal(op.Failed(fmt.Errorf("count: %w", err)))
				}
				if count < 100 {
					for j := count; j < 100; j++ {
						_, err = conn.Exec(op.Ctx, `INSERT INTO bench_mixed (name, value) VALUES ($1, $2)`,
							fmt.Sprintf("item_%d", j), j*10)
						if err != nil {
							conn.Release()
							b.Fatal(op.Failed(fmt.Errorf("seed: %w", err)))
						}
					}
				}

				if err := runMixedTask(op.Ctx, conn.Conn, rng); err != nil {
					conn.Release()
					b.Fatal(op.Failed(err))
				}

				conn.Release()
				op.Done()
				i++
			}
		})
	})
}

// runMixedTask runs a single mixed workload task (20 queries).
func runMixedTask(ctx context.Context, conn *pgx.Conn, rng *rand.Rand) error {
	// Mix of operations: 40% SELECT, 20% INSERT, 20% UPDATE, 10% DELETE, 10% Transaction
	for j := 0; j < 20; j++ {
		op := rng.Intn(100)

		switch {
		case op < 40: // SELECT
			rows, err := conn.Query(ctx, `SELECT id, name, value FROM bench_mixed WHERE id = $1`, rng.Intn(100)+1)
			if err != nil {
				return fmt.Errorf("select: %w", err)
			}
			for rows.Next() {
				var id, value int
				var name string
				if err := rows.Scan(&id, &name, &value); err != nil {
					rows.Close()
					return fmt.Errorf("scan: %w", err)
				}
			}
			rows.Close()
			if err := rows.Err(); err != nil {
				return fmt.Errorf("rows: %w", err)
			}

		case op < 60: // INSERT
			_, err := conn.Exec(ctx, `INSERT INTO bench_mixed (name, value) VALUES ($1, $2)`,
				fmt.Sprintf("new_%d", rng.Int()), rng.Intn(1000))
			if err != nil {
				return fmt.Errorf("insert: %w", err)
			}

		case op < 80: // UPDATE
			_, err := conn.Exec(ctx, `UPDATE bench_mixed SET value = $1 WHERE id = $2`,
				rng.Intn(1000), rng.Intn(100)+1)
			if err != nil {
				return fmt.Errorf("update: %w", err)
			}

		case op < 90: // DELETE (ignore errors - row may not exist)
			_, _ = conn.Exec(ctx, `DELETE FROM bench_mixed WHERE id > 100 AND id = $1`, rng.Intn(1000)+100)

		default: // Transaction
			tx, err := conn.Begin(ctx)
			if err != nil {
				return fmt.Errorf("begin: %w", err)
			}

			_, err = tx.Exec(ctx, `INSERT INTO bench_mixed (name, value) VALUES ($1, $2)`,
				fmt.Sprintf("tx_%d", rng.Int()), rng.Intn(1000))
			if err != nil {
				tx.Rollback(ctx)
				return fmt.Errorf("tx insert: %w", err)
			}

			_, err = tx.Exec(ctx, `UPDATE bench_mixed SET value = value + 1 WHERE id = $1`, rng.Intn(100)+1)
			if err != nil {
				tx.Rollback(ctx)
				return fmt.Errorf("tx update: %w", err)
			}

			if err := tx.Commit(ctx); err != nil {
				return fmt.Errorf("commit: %w", err)
			}
		}
	}

	return nil
}
