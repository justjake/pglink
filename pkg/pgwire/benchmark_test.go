package pgwire

import (
	"bytes"
	"io"
	"testing"
)

// =============================================================================
// Benchmarks for diagnosing proxy performance overhead
// =============================================================================
//
// These benchmarks measure the overhead of various pgwire components to identify
// bottlenecks in the proxy's hot path.
//
// Run with: go test -bench=. -benchmem -benchtime=3s ./pkg/pgwire

// -----------------------------------------------------------------------------
// Ring Buffer and Cursor Benchmarks
// -----------------------------------------------------------------------------

// BenchmarkRingBuffer_WriteAndRead measures ring buffer throughput
func BenchmarkRingBuffer_WriteAndRead(b *testing.B) {
	// Create a simple Query message (type 'Q', length 9, "SELECT 1\0")
	queryMsg := []byte{'Q', 0, 0, 0, 9, 'S', 'E', 'L', 'E', 'C', 'T', ' ', '1', 0}

	ring := NewRingBuffer(RingBufferConfig{})

	// Fill the ring buffer with messages
	for i := 0; i < 1000; i++ {
		ring.rawEnd += int64(copy(ring.data[ring.rawEnd:], queryMsg))
	}

	// Parse messages
	msgsRefresh := 32
	ring.parseCompleteMessages(&msgsRefresh)
	ring.publish()

	cursor := NewClientCursor(ring)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Simulate cursor iteration (reset positions for next iteration)
		cursor.startIdx = 0
		cursor.endIdx = ring.PublishedMsgCount()
		cursor.msgIdx = -1

		for cursor.NextMsg() {
			_ = cursor.MessageType()
		}
	}
}

// BenchmarkCursor_AsClient measures flyweight parsing overhead
func BenchmarkCursor_AsClient(b *testing.B) {
	// Create a simple Query message
	queryMsg := []byte{'Q', 0, 0, 0, 9, 'S', 'E', 'L', 'E', 'C', 'T', ' ', '1', 0}

	ring := NewRingBuffer(RingBufferConfig{})

	// Fill with one message
	ring.rawEnd = int64(copy(ring.data, queryMsg))
	msgsRefresh := 32
	ring.parseCompleteMessages(&msgsRefresh)
	ring.publish()

	cursor := NewClientCursor(ring)
	cursor.startIdx = 0
	cursor.endIdx = 1
	cursor.msgIdx = 0

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = cursor.AsClient()
	}
}

// BenchmarkCursor_AsServer measures server flyweight parsing
func BenchmarkCursor_AsServer(b *testing.B) {
	// Create a ReadyForQuery message
	rfqMsg := []byte{'Z', 0, 0, 0, 5, 'I'}

	ring := NewRingBuffer(RingBufferConfig{})
	ring.rawEnd = int64(copy(ring.data, rfqMsg))
	msgsRefresh := 32
	ring.parseCompleteMessages(&msgsRefresh)
	ring.publish()

	cursor := NewServerCursor(ring)
	cursor.startIdx = 0
	cursor.endIdx = 1
	cursor.msgIdx = 0

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = cursor.AsServer()
	}
}

// BenchmarkRingRange_SetEndInclusive measures range expansion
func BenchmarkRingRange_SetEndInclusive(b *testing.B) {
	ring := NewRingBuffer(RingBufferConfig{})

	// Set up capacity
	rr := &RingRange{
		ring:     ring,
		startIdx: 0,
		endIdx:   0,
	}
	rr.capacity = &RingRange{
		ring:     ring,
		startIdx: 0,
		endIdx:   1000,
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rr.endIdx = 0 // Reset
		for j := int64(0); j < 100; j++ {
			rr.SetEndInclusive(j)
		}
	}
}

// -----------------------------------------------------------------------------
// I/O Path Benchmarks
// -----------------------------------------------------------------------------

// BenchmarkRingRange_NewReader measures reader creation overhead
func BenchmarkRingRange_NewReader(b *testing.B) {
	ring := NewRingBuffer(RingBufferConfig{})

	// Fill with messages
	queryMsg := []byte{'Q', 0, 0, 0, 9, 'S', 'E', 'L', 'E', 'C', 'T', ' ', '1', 0}
	for i := 0; i < 10; i++ {
		ring.rawEnd += int64(copy(ring.data[ring.rawEnd:], queryMsg))
	}
	msgsRefresh := 32
	ring.parseCompleteMessages(&msgsRefresh)
	ring.publish()

	rr := &RingRange{
		ring:     ring,
		startIdx: 0,
		endIdx:   10,
	}
	rr.capacity = rr

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = rr.NewReader()
	}
}

// BenchmarkIoCopy_SmallMessage measures io.Copy overhead for small messages
func BenchmarkIoCopy_SmallMessage(b *testing.B) {
	// Simulate the io.Copy path used in flushRingRange
	data := []byte{'Q', 0, 0, 0, 9, 'S', 'E', 'L', 'E', 'C', 'T', ' ', '1', 0}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = io.Copy(io.Discard, bytes.NewReader(data))
	}
}
