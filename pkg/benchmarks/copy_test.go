package benchmarks

import (
	"fmt"
	"io"
	"testing"
)

// countingWriter counts bytes written to it.
type countingWriter struct {
	count int64
}

func (w *countingWriter) Write(p []byte) (int, error) {
	w.count += int64(len(p))
	return len(p), nil
}

// BenchmarkCopyOut measures COPY TO STDOUT throughput.
// This benchmarks streaming large result sets through the proxy.
//
// Concurrency is controlled by the -cpu flag.
// Connection management is controlled by BENCH_CONNECT_MODE (per-worker or per-op).
func BenchmarkCopyOut(b *testing.B) {
	rows := 1000 // Number of rows per COPY

	b.Run(fmt.Sprintf("%s/rows=%d", getBenchName(), rows), func(b *testing.B) {
		benchCtx := b.Context()
		query := fmt.Sprintf(`COPY (SELECT i, 'row_' || i, i*10 FROM generate_series(1,%d) i) TO STDOUT`, rows)

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

			var i int
			for pb.Next() {
				op := NewOp(benchCtx, "copy out", i)

				conn, err := pool.Acquire(op.Ctx)
				if err != nil {
					b.Fatal(op.Failed(err))
				}

				cw := &countingWriter{}
				_, err = conn.PgConn().CopyTo(op.Ctx, cw, query)
				if err != nil {
					conn.Release()
					b.Fatal(op.Failed(err))
				}

				conn.Release()
				op.Done()
				b.SetBytes(cw.count)
				i++
			}
		})
	})
}

// BenchmarkCopyIn measures COPY FROM STDIN throughput.
// This benchmarks streaming data into the database through the proxy.
//
// Concurrency is controlled by the -cpu flag.
// Connection management is controlled by BENCH_CONNECT_MODE (per-worker or per-op).
func BenchmarkCopyIn(b *testing.B) {
	rows := 1000 // Number of rows per COPY

	b.Run(fmt.Sprintf("%s/rows=%d", getBenchName(), rows), func(b *testing.B) {
		benchCtx := b.Context()
		testData := generateCopyData(rows)
		dataLen := int64(len(testData))

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

			var i int
			for pb.Next() {
				op := NewOp(benchCtx, "copy in", i)

				conn, err := pool.Acquire(op.Ctx)
				if err != nil {
					b.Fatal(op.Failed(err))
				}

				// Create temp table (needed for loop mode where each iteration has a new connection)
				// Uses IF NOT EXISTS so it's a no-op for worker mode's persistent connection
				_, err = conn.Exec(op.Ctx, `CREATE TEMP TABLE IF NOT EXISTS bench_copy_temp (id int, name text, value int)`)
				if err != nil {
					conn.Release()
					b.Fatal(op.Failed(fmt.Errorf("create table: %w", err)))
				}

				// Truncate table (clear any data from previous iteration)
				_, err = conn.Exec(op.Ctx, `TRUNCATE bench_copy_temp`)
				if err != nil {
					conn.Release()
					b.Fatal(op.Failed(fmt.Errorf("truncate: %w", err)))
				}

				// COPY data
				reader := &repeatableReader{data: testData}
				_, err = conn.PgConn().CopyFrom(op.Ctx, reader, `COPY bench_copy_temp (id, name, value) FROM STDIN`)
				if err != nil {
					conn.Release()
					b.Fatal(op.Failed(err))
				}

				conn.Release()
				op.Done()
				b.SetBytes(dataLen)
				i++
			}
		})
	})
}

// generateCopyData generates tab-separated COPY data.
func generateCopyData(rows int) []byte {
	buf := make([]byte, 0, rows*30)
	for i := 1; i <= rows; i++ {
		row := fmt.Sprintf("%d\trow_%d\t%d\n", i, i, i*10)
		buf = append(buf, row...)
	}
	return buf
}

// repeatableReader allows reading the same data multiple times.
type repeatableReader struct {
	data []byte
	pos  int
}

func (r *repeatableReader) Read(p []byte) (int, error) {
	if r.pos >= len(r.data) {
		return 0, io.EOF
	}
	n := copy(p, r.data[r.pos:])
	r.pos += n
	return n, nil
}
