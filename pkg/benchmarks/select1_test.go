package benchmarks

import (
	"testing"
)

// BenchmarkSelect1 measures the latency of a minimal SELECT 1 query.
// This is the baseline latency benchmark that measures proxy overhead
// with minimal database work.
//
// Concurrency is controlled by the -cpu flag.
// Connection management is controlled by BENCH_POOL_MODE (worker or loop).
func BenchmarkSelect1(b *testing.B) {
	b.Run(getBenchName(), func(b *testing.B) {
		benchCtx := b.Context()

		b.RunParallel(func(pb *testing.PB) {
			// Each worker gets its own pool (worker mode: 1 conn, loop mode: conn per acquire)
			pool, err := GetTestPool(b, benchCtx)
			if err != nil {
				b.Fatalf("failed to create pool: %v", err)
			}
			defer func() {
				if err := pool.Close(); err != nil {
					b.Errorf("pool close error: %v", err)
				}
			}()

			var i int
			for pb.Next() {
				op := NewOp(benchCtx, "query", i)

				conn, err := pool.Acquire(op.Ctx)
				if err != nil {
					b.Fatal(op.Failed(err))
				}

				var result int
				if err := conn.QueryRow(op.Ctx, "SELECT 1").Scan(&result); err != nil {
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

// BenchmarkConnect measures connection establishment latency.
// Each iteration creates a new connection, runs SELECT 1, and closes it.
// This tests the full connect/query/disconnect cycle.
//
// Concurrency is controlled by the -cpu flag.
// This benchmark always uses loop mode regardless of BENCH_POOL_MODE.
func BenchmarkConnect(b *testing.B) {
	b.Run(getBenchName(), func(b *testing.B) {
		benchCtx := b.Context()

		b.RunParallel(func(pb *testing.PB) {
			// Always use loop mode for this benchmark
			pool := &loopPool{connString: benchConfig.ConnString}

			var i int
			for pb.Next() {
				op := NewOp(benchCtx, "connect+query", i)

				conn, err := pool.Acquire(op.Ctx)
				if err != nil {
					b.Fatal(op.Failed(err))
				}

				var result int
				if err := conn.QueryRow(op.Ctx, "SELECT 1").Scan(&result); err != nil {
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
