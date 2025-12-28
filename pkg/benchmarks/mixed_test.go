package benchmarks

import (
	"context"
	"fmt"
	"math/rand"
	"sync/atomic"
	"testing"
	"time"

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

		// Unique table name per benchmark run to avoid collisions between worktrees
		// Use schema1 where admin has CREATE permission
		tableName := fmt.Sprintf("schema1.bench_mixed_%s", benchConfig.RunID)

		// === SETUP (runs once before parallel workers) ===
		// Use admin connection for DDL (app user doesn't have CREATE permission)
		setupCtx, setupCancel := context.WithTimeout(benchCtx, 30*time.Second)
		adminConn, err := connectAsAdmin(setupCtx)
		if err != nil {
			setupCancel()
			b.Fatalf("failed to connect as admin: %v", err)
		}

		// Create and seed in a transaction
		tx, err := adminConn.Begin(setupCtx)
		if err != nil {
			adminConn.Close(setupCtx)
			setupCancel()
			b.Fatalf("begin setup: %v", err)
		}

		_, err = tx.Exec(setupCtx, fmt.Sprintf(`
			CREATE TABLE %s (
				id SERIAL PRIMARY KEY,
				name TEXT NOT NULL,
				value INT NOT NULL,
				created_at TIMESTAMP DEFAULT NOW()
			)
		`, tableName))
		if err != nil {
			tx.Rollback(setupCtx)
			adminConn.Close(setupCtx)
			setupCancel()
			b.Fatalf("create table: %v", err)
		}

		// Grant permissions to app user
		_, err = tx.Exec(setupCtx, fmt.Sprintf(`GRANT ALL ON %s TO app`, tableName))
		if err != nil {
			tx.Rollback(setupCtx)
			adminConn.Close(setupCtx)
			setupCancel()
			b.Fatalf("grant permissions: %v", err)
		}

		// Grant sequence permissions for INSERT
		_, err = tx.Exec(setupCtx, fmt.Sprintf(`GRANT USAGE, SELECT ON SEQUENCE %s_id_seq TO app`, tableName))
		if err != nil {
			tx.Rollback(setupCtx)
			adminConn.Close(setupCtx)
			setupCancel()
			b.Fatalf("grant sequence permissions: %v", err)
		}

		for i := 0; i < 100; i++ {
			_, err = tx.Exec(setupCtx,
				fmt.Sprintf(`INSERT INTO %s (name, value) VALUES ($1, $2)`, tableName),
				fmt.Sprintf("item_%d", i), i*10)
			if err != nil {
				tx.Rollback(setupCtx)
				adminConn.Close(setupCtx)
				setupCancel()
				b.Fatalf("seed row %d: %v", i, err)
			}
		}

		if err := tx.Commit(setupCtx); err != nil {
			adminConn.Close(setupCtx)
			setupCancel()
			b.Fatalf("commit setup: %v", err)
		}
		adminConn.Close(setupCtx)
		setupCancel()

		// Track total queries across all workers
		var totalQueries atomic.Int64

		// === PARALLEL BENCHMARK ===
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

				taskQueries, err := runMixedTask(op.Ctx, conn.Conn, tableName, rng)
				if err != nil {
					conn.Release()
					b.Fatal(op.Failed(err))
				}

				conn.Release()
				op.Done()
				totalQueries.Add(taskQueries)
				i++
			}
		})
		ReportThroughputWithQueries(b, totalQueries.Load())

		// === CLEANUP (runs once after parallel workers) ===
		// Use admin connection for DDL
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
		cleanupConn, err := connectAsAdmin(cleanupCtx)
		if err == nil {
			_, _ = cleanupConn.Exec(cleanupCtx, fmt.Sprintf(`DROP TABLE IF EXISTS %s`, tableName))
			cleanupConn.Close(cleanupCtx)
		}
		cleanupCancel()
	})
}

// runMixedTask runs a single mixed workload task.
// Returns the number of queries executed.
func runMixedTask(ctx context.Context, conn *pgx.Conn, tableName string, rng *rand.Rand) (int64, error) {
	var queries int64

	// Mix of operations: 40% SELECT, 20% INSERT, 20% UPDATE, 10% DELETE, 10% Transaction
	for j := 0; j < 20; j++ {
		op := rng.Intn(100)

		switch {
		case op < 40: // SELECT (1 query)
			rows, err := conn.Query(ctx, fmt.Sprintf(`SELECT id, name, value FROM %s WHERE id = $1`, tableName), rng.Intn(100)+1)
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
			_, err := conn.Exec(ctx, fmt.Sprintf(`INSERT INTO %s (name, value) VALUES ($1, $2)`, tableName),
				fmt.Sprintf("new_%d", rng.Int()), rng.Intn(1000))
			if err != nil {
				return queries, fmt.Errorf("insert: %w", err)
			}
			queries++

		case op < 80: // UPDATE (1 query)
			_, err := conn.Exec(ctx, fmt.Sprintf(`UPDATE %s SET value = $1 WHERE id = $2`, tableName),
				rng.Intn(1000), rng.Intn(100)+1)
			if err != nil {
				return queries, fmt.Errorf("update: %w", err)
			}
			queries++

		case op < 90: // DELETE (1 query)
			_, _ = conn.Exec(ctx, fmt.Sprintf(`DELETE FROM %s WHERE id > 100 AND id = $1`, tableName), rng.Intn(1000)+100)
			queries++

		default: // Transaction (4 queries: BEGIN, INSERT, UPDATE, COMMIT)
			tx, err := conn.Begin(ctx)
			if err != nil {
				return queries, fmt.Errorf("begin: %w", err)
			}
			queries++ // BEGIN

			_, err = tx.Exec(ctx, fmt.Sprintf(`INSERT INTO %s (name, value) VALUES ($1, $2)`, tableName),
				fmt.Sprintf("tx_%d", rng.Int()), rng.Intn(1000))
			if err != nil {
				tx.Rollback(ctx)
				queries++ // ROLLBACK
				return queries, fmt.Errorf("tx insert: %w", err)
			}
			queries++ // INSERT

			_, err = tx.Exec(ctx, fmt.Sprintf(`UPDATE %s SET value = value + 1 WHERE id = $1`, tableName), rng.Intn(100)+1)
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
