package benchmarks

import (
	"context"
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
func BenchmarkCopyOut(b *testing.B) {
	target := getTarget()
	rows := 1000 // Number of rows per COPY

	b.Run(fmt.Sprintf("target=%s/rows=%d", target, rows), func(b *testing.B) {
		ctx := context.Background()
		pool := getPool()

		query := fmt.Sprintf(`COPY (SELECT i, 'row_' || i, i*10 FROM generate_series(1,%d) i) TO STDOUT`, rows)

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			conn, err := pool.Acquire(ctx)
			if err != nil {
				b.Fatal(err)
			}

			cw := &countingWriter{}
			_, err = conn.Conn().PgConn().CopyTo(ctx, cw, query)
			conn.Release()

			if err != nil {
				b.Fatal(err)
			}

			b.SetBytes(cw.count)
		}
	})
}

// BenchmarkCopyIn measures COPY FROM STDIN throughput.
// This benchmarks streaming data into the database through the proxy.
func BenchmarkCopyIn(b *testing.B) {
	target := getTarget()
	rows := 1000 // Number of rows per COPY

	b.Run(fmt.Sprintf("target=%s/rows=%d", target, rows), func(b *testing.B) {
		ctx := context.Background()
		pool := getPool()

		// Create temp table for COPY IN
		conn, err := pool.Acquire(ctx)
		if err != nil {
			b.Fatal(err)
		}

		_, err = conn.Exec(ctx, `CREATE TEMP TABLE IF NOT EXISTS bench_copy_temp (id int, name text, value int)`)
		if err != nil {
			conn.Release()
			b.Fatal(err)
		}
		conn.Release()

		// Generate test data
		testData := generateCopyData(rows)
		dataLen := int64(len(testData))

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			conn, err := pool.Acquire(ctx)
			if err != nil {
				b.Fatal(err)
			}

			// Truncate table
			_, err = conn.Exec(ctx, `TRUNCATE bench_copy_temp`)
			if err != nil {
				conn.Release()
				b.Fatal(err)
			}

			// COPY data
			reader := &repeatableReader{data: testData}
			_, err = conn.Conn().PgConn().CopyFrom(ctx, reader, `COPY bench_copy_temp (id, name, value) FROM STDIN`)
			conn.Release()

			if err != nil {
				b.Fatal(err)
			}

			b.SetBytes(dataLen)
		}
	})
}

// generateCopyData generates tab-separated COPY data.
func generateCopyData(rows int) []byte {
	// Pre-allocate buffer (estimate ~30 bytes per row)
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

// Reset resets the reader to the beginning.
func (r *repeatableReader) Reset() {
	r.pos = 0
}
