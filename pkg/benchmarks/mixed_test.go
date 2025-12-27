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
	b.Run(getBenchName(), func(b *testing.B) {
		benchCtx := b.Context()
		pool := getPool()

		// Use seed for reproducibility
		seed := benchConfig.Seed
		if seed == 0 {
			seed = 12345 // Default seed for consistency
		}
		rng := rand.New(rand.NewSource(seed))

		var i int
		for b.Loop() {
			op := NewOp(benchCtx, "mixed task", i)
			if err := runMixedTask(op.Ctx, pool, rng); err != nil {
				b.Fatal(op.Failed(err))
			}
			op.Done()
			i++
		}
	})
}

// runMixedTask runs a single mixed workload task.
func runMixedTask(ctx context.Context, pool *pgxpool.Pool, rng *rand.Rand) error {
	conn, err := pool.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("acquire: %w", err)
	}
	defer conn.Release()

	// Create temp table if it doesn't exist on this connection.
	_, err = conn.Exec(ctx, `
		CREATE TEMP TABLE IF NOT EXISTS bench_mixed (
			id SERIAL PRIMARY KEY,
			name TEXT NOT NULL,
			value INT NOT NULL,
			created_at TIMESTAMP DEFAULT NOW()
		)
	`)
	if err != nil {
		return fmt.Errorf("create temp table: %w", err)
	}

	// Ensure we have some data to work with (idempotent seeding)
	var count int
	if err := conn.QueryRow(ctx, `SELECT COUNT(*) FROM bench_mixed`).Scan(&count); err != nil {
		return fmt.Errorf("count: %w", err)
	}
	if count < 100 {
		for i := count; i < 100; i++ {
			_, err = conn.Exec(ctx, `INSERT INTO bench_mixed (name, value) VALUES ($1, $2)`,
				fmt.Sprintf("item_%d", i), i*10)
			if err != nil {
				return fmt.Errorf("seed: %w", err)
			}
		}
	}

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
