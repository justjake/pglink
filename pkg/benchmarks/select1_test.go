package benchmarks

import (
	"testing"
)

// BenchmarkSelect1 measures the latency of a minimal SELECT 1 query.
// This is the baseline latency benchmark that measures proxy overhead
// with minimal database work.
func BenchmarkSelect1(b *testing.B) {
	b.Run(getBenchName(), func(b *testing.B) {
		pool := getPool()
		benchCtx := b.Context()

		var i int
		for b.Loop() {
			op := NewOp(benchCtx, "query", i)
			var result int
			if err := pool.QueryRow(op.Ctx, "SELECT 1").Scan(&result); err != nil {
				b.Fatal(op.Failed(err))
			}
			op.Done()
			i++
		}
	})
}

// BenchmarkSelect1Parallel measures SELECT 1 latency with concurrent connections.
func BenchmarkSelect1Parallel(b *testing.B) {
	b.Run(getBenchName(), func(b *testing.B) {
		pool := getPool()
		benchCtx := b.Context()

		b.RunParallel(func(pb *testing.PB) {
			var i int
			for pb.Next() {
				op := NewOp(benchCtx, "query", i)
				var result int
				if err := pool.QueryRow(op.Ctx, "SELECT 1").Scan(&result); err != nil {
					b.Fatal(op.Failed(err))
				}
				op.Done()
				i++
			}
		})
	})
}
