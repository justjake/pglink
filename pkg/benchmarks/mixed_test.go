package benchmarks

import (
	"context"
	"fmt"
	"math/rand"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
)

// BenchmarkMixed runs a mixed workload of different query types.
// This simulates a more realistic application workload with:
// - Simple SELECT queries
// - INSERT/UPDATE/DELETE operations
// - Prepared statements
// - Transactions
func BenchmarkMixed(b *testing.B) {
	target := getTarget()

	b.Run("target="+target, func(b *testing.B) {
		ctx := context.Background()
		pool := getPool()

		// Create test table
		conn, err := pool.Acquire(ctx)
		if err != nil {
			b.Fatal(err)
		}

		_, err = conn.Exec(ctx, `
			CREATE TEMP TABLE IF NOT EXISTS bench_mixed (
				id SERIAL PRIMARY KEY,
				name TEXT NOT NULL,
				value INT NOT NULL,
				created_at TIMESTAMP DEFAULT NOW()
			)
		`)
		if err != nil {
			conn.Release()
			b.Fatal(err)
		}

		// Seed with some data
		for i := 0; i < 100; i++ {
			_, err = conn.Exec(ctx, `INSERT INTO bench_mixed (name, value) VALUES ($1, $2)`,
				fmt.Sprintf("item_%d", i), i*10)
			if err != nil {
				conn.Release()
				b.Fatal(err)
			}
		}
		conn.Release()

		// Use seed for reproducibility
		seed := benchConfig.Seed
		if seed == 0 {
			seed = 12345 // Default seed for consistency
		}
		rng := rand.New(rand.NewSource(seed))

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			// Each iteration runs a "task" of 20 queries
			if err := runMixedTask(ctx, pool, rng); err != nil {
				b.Fatal(err)
			}
		}
	})
}

// runMixedTask runs a single mixed workload task.
func runMixedTask(ctx context.Context, pool *pgxpool.Pool, rng *rand.Rand) error {
	conn, err := pool.Acquire(ctx)
	if err != nil {
		return err
	}
	defer conn.Release()

	// Mix of operations:
	// 40% SELECT
	// 20% INSERT
	// 20% UPDATE
	// 10% DELETE
	// 10% Transaction

	for j := 0; j < 20; j++ {
		op := rng.Intn(100)

		switch {
		case op < 40: // SELECT
			rows, err := conn.Query(ctx, `SELECT id, name, value FROM bench_mixed WHERE id = $1`, rng.Intn(100)+1)
			if err != nil {
				return err
			}
			for rows.Next() {
				var id int
				var name string
				var value int
				if err := rows.Scan(&id, &name, &value); err != nil {
					rows.Close()
					return err
				}
			}
			rows.Close()
			if err := rows.Err(); err != nil {
				return err
			}

		case op < 60: // INSERT
			_, err := conn.Exec(ctx, `INSERT INTO bench_mixed (name, value) VALUES ($1, $2)`,
				fmt.Sprintf("new_%d", rng.Int()), rng.Intn(1000))
			if err != nil {
				return err
			}

		case op < 80: // UPDATE
			_, err := conn.Exec(ctx, `UPDATE bench_mixed SET value = $1 WHERE id = $2`,
				rng.Intn(1000), rng.Intn(100)+1)
			if err != nil {
				return err
			}

		case op < 90: // DELETE
			// Delete a random row - ignore errors (row may not exist)
			_, _ = conn.Exec(ctx, `DELETE FROM bench_mixed WHERE id > 100 AND id = $1`,
				rng.Intn(1000)+100)

		default: // Transaction
			tx, err := conn.Begin(ctx)
			if err != nil {
				return err
			}

			_, err = tx.Exec(ctx, `INSERT INTO bench_mixed (name, value) VALUES ($1, $2)`,
				fmt.Sprintf("tx_%d", rng.Int()), rng.Intn(1000))
			if err != nil {
				tx.Rollback(ctx)
				return err
			}

			_, err = tx.Exec(ctx, `UPDATE bench_mixed SET value = value + 1 WHERE id = $1`,
				rng.Intn(100)+1)
			if err != nil {
				tx.Rollback(ctx)
				return err
			}

			if err := tx.Commit(ctx); err != nil {
				return err
			}
		}
	}

	return nil
}
