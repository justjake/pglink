package benchmarks

import (
	"context"
	"testing"
)

// BenchmarkSelect1 measures the latency of a minimal SELECT 1 query.
// This is the baseline latency benchmark that measures proxy overhead
// with minimal database work.
func BenchmarkSelect1(b *testing.B) {
	target := getTarget()

	b.Run("target="+target, func(b *testing.B) {
		ctx := context.Background()
		pool := getPool()

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			var result int
			if err := pool.QueryRow(ctx, "SELECT 1").Scan(&result); err != nil {
				b.Fatal(err)
			}
		}
	})
}

// BenchmarkSelect1Parallel measures SELECT 1 latency with concurrent connections.
func BenchmarkSelect1Parallel(b *testing.B) {
	target := getTarget()

	b.Run("target="+target, func(b *testing.B) {
		ctx := context.Background()
		pool := getPool()

		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				var result int
				if err := pool.QueryRow(ctx, "SELECT 1").Scan(&result); err != nil {
					b.Fatal(err)
				}
			}
		})
	})
}
