package pgwire

import (
	"testing"
)

// BenchmarkFlyweightAllocation tests if the flyweight pattern actually avoids allocations
func BenchmarkFlyweightAllocation(b *testing.B) {
	// Create a minimal ring buffer with a Flush message
	ring := NewRingBuffer(DefaultRingBufferConfig())

	// Manually write a Flush message (type 'H', body len 4, no body data)
	// Wire format: type(1) + length(4) = 5 bytes total, length includes itself
	data := []byte{'H', 0, 0, 0, 4} // Flush message
	copy(ring.data, data)
	ring.parsePos = 5
	ring.rawEnd = 5
	ring.offsets[0] = 0 // Message 0 starts at byte 0
	ring.publishedBytes = 5
	ring.publishedMsgs = 1

	cursor := NewClientCursor(ring)

	// Prime the cursor
	cursor.startIdx = 0
	cursor.endIdx = 1
	cursor.msgIdx = 0

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		msg, err := cursor.AsClient()
		if err != nil {
			b.Fatal(err)
		}
		// Use msg to prevent optimization
		_ = msg.PgwireMessage()
	}
}

// BenchmarkFlyweightAllocationNoInterface tests the flyweight without interface conversion
func BenchmarkFlyweightAllocationNoInterface(b *testing.B) {
	ring := NewRingBuffer(DefaultRingBufferConfig())

	data := []byte{'H', 0, 0, 0, 4}
	copy(ring.data, data)
	ring.parsePos = 5
	ring.rawEnd = 5
	ring.offsets[0] = 0
	ring.publishedBytes = 5
	ring.publishedMsgs = 1

	cursor := NewClientCursor(ring)
	cursor.startIdx = 0
	cursor.endIdx = 1
	cursor.msgIdx = 0

	fw := cursor.clientFlyweights

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		// Direct assignment without interface conversion
		fw.flush = ClientExtendedQueryFlush{source: cursor}
		// Access directly
		_ = fw.flush.source.MessageType()
	}
}
