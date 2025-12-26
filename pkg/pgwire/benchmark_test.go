package pgwire

import (
	"bytes"
	"io"
	"testing"

	"github.com/jackc/pgx/v5/pgproto3"
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
// Protocol State Update Benchmarks
// -----------------------------------------------------------------------------

// BenchmarkProtocolState_UpdateSimpleQuery measures the overhead of updating
// protocol state for a simple query message.
func BenchmarkProtocolState_UpdateSimpleQuery(b *testing.B) {
	state := NewProtocolState()

	// Create a simple query message (SELECT 1)
	msg := (*ClientSimpleQueryQuery)(ClientParsed(&pgproto3.Query{String: "SELECT 1"}))

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		state.Update(msg)
	}
}

// BenchmarkProtocolState_UpdateReadyForQuery measures server response handling
func BenchmarkProtocolState_UpdateReadyForQuery(b *testing.B) {
	state := NewProtocolState()

	msg := (*ServerResponseReadyForQuery)(ServerParsed(&pgproto3.ReadyForQuery{TxStatus: 'I'}))

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		state.Update(msg)
	}
}

// BenchmarkProtocolState_UpdateDataRow measures DataRow handling
func BenchmarkProtocolState_UpdateDataRow(b *testing.B) {
	state := NewProtocolState()

	// Create a minimal DataRow (1 column, value "1")
	msg := (*ServerResponseDataRow)(ServerParsed(&pgproto3.DataRow{Values: [][]byte{{'1'}}}))

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		state.Update(msg)
	}
}

// -----------------------------------------------------------------------------
// Message Handler Dispatch Benchmarks
// -----------------------------------------------------------------------------

// BenchmarkClientMessageHandlers_Dispatch measures handler dispatch overhead
func BenchmarkClientMessageHandlers_Dispatch(b *testing.B) {
	// Create handler like in session.go
	handlers := ClientMessageHandlers[bool]{
		SimpleQuery: nil, // nil = use default
	}

	msg := (*ClientSimpleQueryQuery)(ClientParsed(&pgproto3.Query{String: "SELECT 1"}))

	defaultHandler := func(msg ClientMessage) (bool, error) {
		return true, nil
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = handlers.HandleDefault(msg, defaultHandler)
	}
}

// BenchmarkServerMessageHandlers_Dispatch measures server handler dispatch
func BenchmarkServerMessageHandlers_Dispatch(b *testing.B) {
	handlers := ServerMessageHandlers[bool]{
		Response: nil, // nil = use default
	}

	msg := (*ServerResponseReadyForQuery)(ServerParsed(&pgproto3.ReadyForQuery{TxStatus: 'I'}))

	defaultHandler := func(msg ServerMessage) (bool, error) {
		return true, nil
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = handlers.HandleDefault(msg, defaultHandler)
	}
}

// BenchmarkClientMessageHandlers_AllocPerCall measures allocation when
// creating handlers per call (as done in ProtocolState.Update)
func BenchmarkClientMessageHandlers_AllocPerCall(b *testing.B) {
	state := NewProtocolState()
	msg := (*ClientSimpleQueryQuery)(ClientParsed(&pgproto3.Query{String: "SELECT 1"}))

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// This is what ProtocolState.UpdateForFrontentMessage does
		handlers := ClientMessageHandlers[struct{}]{
			SimpleQuery:   wrapVoid(state.UpdateForSimpleQueryMessage),
			ExtendedQuery: wrapVoid(state.UpdateForExtendedQueryMessage),
		}
		_, _ = handlers.HandleDefault(msg, func(msg ClientMessage) (struct{}, error) {
			return struct{}{}, nil
		})
	}
}

// BenchmarkClientMessageHandlers_ReuseHandlers measures dispatch with reused handlers
func BenchmarkClientMessageHandlers_ReuseHandlers(b *testing.B) {
	state := NewProtocolState()

	// Pre-allocate handlers once
	handlers := ClientMessageHandlers[struct{}]{
		SimpleQuery:   wrapVoid(state.UpdateForSimpleQueryMessage),
		ExtendedQuery: wrapVoid(state.UpdateForExtendedQueryMessage),
	}
	defaultHandler := func(msg ClientMessage) (struct{}, error) { return struct{}{}, nil }

	msg := (*ClientSimpleQueryQuery)(ClientParsed(&pgproto3.Query{String: "SELECT 1"}))

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = handlers.HandleDefault(msg, defaultHandler)
	}
}

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

// -----------------------------------------------------------------------------
// Full Path Simulation Benchmarks
// -----------------------------------------------------------------------------

// BenchmarkFullPath_SimpleQuery simulates the full hot path for a simple query
// without network I/O - just the CPU work.
func BenchmarkFullPath_SimpleQuery(b *testing.B) {
	// Create rings and cursors
	frontendRing := NewRingBuffer(RingBufferConfig{})
	backendRing := NewRingBuffer(RingBufferConfig{})

	frontendCursor := NewClientCursor(frontendRing)
	backendCursor := NewServerCursor(backendRing)

	// Create state
	clientState := NewProtocolState()
	backendState := NewProtocolState()

	// Message handlers (like in session.go)
	handlers := MessageHandlers[bool]{
		Client: ClientMessageHandlers[bool]{
			SimpleQuery: nil,
		},
		Server: ServerMessageHandlers[bool]{
			Response: nil,
		},
	}

	// Prepare messages
	queryMsg := []byte{'Q', 0, 0, 0, 9, 'S', 'E', 'L', 'E', 'C', 'T', ' ', '1', 0}
	dataRowMsg := []byte{'D', 0, 0, 0, 11, 0, 1, 0, 0, 0, 1, '1'}
	cmdCompleteMsg := []byte{'C', 0, 0, 0, 13, 'S', 'E', 'L', 'E', 'C', 'T', ' ', '1', 0}
	rfqMsg := []byte{'Z', 0, 0, 0, 5, 'I'}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Reset rings
		frontendRing.rawEnd = 0
		frontendRing.parsePos = 0
		frontendRing.localMsgCnt = 0
		frontendRing.publishedMsgs = 0
		frontendRing.publishedBytes = 0

		backendRing.rawEnd = 0
		backendRing.parsePos = 0
		backendRing.localMsgCnt = 0
		backendRing.publishedMsgs = 0
		backendRing.publishedBytes = 0

		// Simulate client sending query
		frontendRing.rawEnd = int64(copy(frontendRing.data, queryMsg))
		msgsRefresh := 32
		frontendRing.parseCompleteMessages(&msgsRefresh)
		frontendRing.publish()

		// Process frontend
		frontendCursor.startIdx = 0
		frontendCursor.endIdx = frontendRing.PublishedMsgCount()
		frontendCursor.msgIdx = -1

		var toServer RingRange
		toServer.ring = frontendRing
		toServer.capacity = &frontendCursor.RingRange

		for frontendCursor.NextMsg() {
			msg, _ := frontendCursor.AsClient()
			clientState.Update(msg)
			_, _ = handlers.Client.HandleDefault(msg, func(msg ClientMessage) (bool, error) {
				backendState.Update(msg)
				toServer.SetEndInclusive(frontendCursor.MsgIdx())
				return true, nil
			})
		}

		// Simulate backend response
		backendRing.rawEnd = int64(copy(backendRing.data, dataRowMsg))
		backendRing.rawEnd += int64(copy(backendRing.data[backendRing.rawEnd:], cmdCompleteMsg))
		backendRing.rawEnd += int64(copy(backendRing.data[backendRing.rawEnd:], rfqMsg))
		backendRing.parseCompleteMessages(&msgsRefresh)
		backendRing.publish()

		// Process backend
		backendCursor.startIdx = 0
		backendCursor.endIdx = backendRing.PublishedMsgCount()
		backendCursor.msgIdx = -1

		var toClient RingRange
		toClient.ring = backendRing
		toClient.capacity = &backendCursor.RingRange

		for backendCursor.NextMsg() {
			msg, _ := backendCursor.AsServer()
			backendState.Update(msg)
			_, _ = handlers.Server.HandleDefault(msg, func(msg ServerMessage) (bool, error) {
				clientState.Update(msg)
				toClient.SetEndInclusive(backendCursor.MsgIdx())
				return true, nil
			})
		}
	}
}

// Note: BenchmarkFullPath_WithIO was removed due to goroutine cleanup issues
// with net.Pipe() in benchmark tests. The CPU-only BenchmarkFullPath_SimpleQuery
// is sufficient for diagnosing overhead in the message processing path.
