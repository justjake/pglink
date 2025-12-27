package pgwire

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgproto3"
)

// ============================================================================
// Test Configuration
// ============================================================================

// RingTestConfig defines a ring buffer configuration for testing.
type RingTestConfig struct {
	Name          string
	MessageBytes  int64
	MessageCount  int64
	HeadroomBytes int64 // 0 means use default (8192)
}

// headroom returns the actual headroom value, defaulting if zero.
func (c RingTestConfig) headroom() int64 {
	if c.HeadroomBytes == 0 {
		return DefaultHeadroomBytes
	}
	return c.HeadroomBytes
}

// StreamingThreshold returns the minimum message size that triggers streaming.
func (c RingTestConfig) StreamingThreshold() int64 {
	return c.MessageBytes - c.headroom()
}

// NewRing creates a new RingBuffer with this configuration.
func (c RingTestConfig) NewRing() *RingBuffer {
	return NewRingBuffer(RingBufferConfig{
		MessageBytes:  c.MessageBytes,
		MessageCount:  c.MessageCount,
		HeadroomBytes: c.headroom(),
	})
}

// TestConfigs defines the buffer configurations to test against.
var TestConfigs = []RingTestConfig{
	{"16KB", 16 * 1024, 256, 0},         // 16KB data, default headroom (8KB)
	{"256KB", 256 * 1024, 4096, 0},      // 256KB data (default), default headroom (8KB)
	{"16KB-minhead", 16 * 1024, 256, 5}, // 16KB data, minimal headroom (5 bytes)
}

// forEachConfig runs a test function against all configurations.
func forEachConfig(t *testing.T, fn func(t *testing.T, cfg RingTestConfig)) {
	for _, cfg := range TestConfigs {
		t.Run(cfg.Name, func(t *testing.T) {
			fn(t, cfg)
		})
	}
}

// ============================================================================
// TestConn - Fake net.Conn for synthetic message testing
// ============================================================================

// TestConn implements net.Conn for testing the ring buffer with synthetic messages.
// Messages are encoded using pgproto3 and fed through the public interface.
type TestConn struct {
	reader      *bytes.Reader
	writeBuffer bytes.Buffer
	mu          sync.Mutex
	closed      bool
	readDelay   time.Duration // Optional delay per read for timing tests
}

// NewTestConnFromClient creates a TestConn from pgproto3 frontend messages.
func NewTestConnFromClient(msgs ...pgproto3.FrontendMessage) *TestConn {
	var buf bytes.Buffer
	for _, msg := range msgs {
		encoded, err := msg.Encode(nil)
		if err != nil {
			panic("failed to encode client message: " + err.Error())
		}
		buf.Write(encoded)
	}
	return &TestConn{reader: bytes.NewReader(buf.Bytes())}
}

// NewTestConnFromServer creates a TestConn from pgproto3 backend messages.
func NewTestConnFromServer(msgs ...pgproto3.BackendMessage) *TestConn {
	var buf bytes.Buffer
	for _, msg := range msgs {
		encoded, err := msg.Encode(nil)
		if err != nil {
			panic("failed to encode server message: " + err.Error())
		}
		buf.Write(encoded)
	}
	return &TestConn{reader: bytes.NewReader(buf.Bytes())}
}

// NewTestConnFromBytes creates a TestConn from raw bytes.
func NewTestConnFromBytes(data []byte) *TestConn {
	return &TestConn{reader: bytes.NewReader(data)}
}

// Read implements io.Reader.
func (tc *TestConn) Read(b []byte) (int, error) {
	tc.mu.Lock()
	defer tc.mu.Unlock()
	if tc.closed {
		return 0, io.EOF
	}
	if tc.readDelay > 0 {
		time.Sleep(tc.readDelay)
	}
	return tc.reader.Read(b)
}

// Write implements io.Writer.
func (tc *TestConn) Write(b []byte) (int, error) {
	tc.mu.Lock()
	defer tc.mu.Unlock()
	if tc.closed {
		return 0, errors.New("connection closed")
	}
	return tc.writeBuffer.Write(b)
}

// Close implements io.Closer.
func (tc *TestConn) Close() error {
	tc.mu.Lock()
	defer tc.mu.Unlock()
	tc.closed = true
	return nil
}

// LocalAddr implements net.Conn.
func (tc *TestConn) LocalAddr() net.Addr {
	return &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 5432}
}

// RemoteAddr implements net.Conn.
func (tc *TestConn) RemoteAddr() net.Addr {
	return &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 12345}
}

// SetDeadline implements net.Conn.
func (tc *TestConn) SetDeadline(_ time.Time) error {
	return nil
}

// SetReadDeadline implements net.Conn.
func (tc *TestConn) SetReadDeadline(_ time.Time) error {
	return nil
}

// SetWriteDeadline implements net.Conn.
func (tc *TestConn) SetWriteDeadline(_ time.Time) error {
	return nil
}

// Written returns the bytes written to the connection.
func (tc *TestConn) Written() []byte {
	tc.mu.Lock()
	defer tc.mu.Unlock()
	return tc.writeBuffer.Bytes()
}

// Compile-time verification that TestConn implements net.Conn
var _ net.Conn = (*TestConn)(nil)

// ============================================================================
// Test Helpers
// ============================================================================

// readAllMessages reads all messages from a ring buffer via cursor.
// Returns the retained messages as RawBody slices.
func readAllMessages(t *testing.T, ring *RingBuffer, isClient bool) []RawBody {
	t.Helper()

	var cursor *Cursor
	if isClient {
		cursor = NewClientCursor(ring)
	} else {
		cursor = NewServerCursor(ring)
	}

	var results []RawBody
	timeout := time.After(5 * time.Second)

	for {
		// Try to get a batch of messages
		gotBatch, err := cursor.TryNextBatch()
		if err != nil {
			// EOF or error - we're done (all messages already delivered)
			return results
		}

		if gotBatch {
			// Read all messages in this batch
			for cursor.NextMsg() {
				retained := cursor.Retain().(RawBody)
				results = append(results, retained)
			}
			continue
		}

		// No batch yet, wait for more data
		select {
		case <-timeout:
			t.Fatal("timeout reading messages from ring buffer")
		case <-cursor.Done():
			// Try one more time to get remaining messages
			if gotBatch, _ := cursor.TryNextBatch(); gotBatch {
				for cursor.NextMsg() {
					retained := cursor.Retain().(RawBody)
					results = append(results, retained)
				}
			}
			return results
		case <-cursor.Ready():
			// More data available, loop back
		}
	}
}

// feedRingBuffer feeds data from a reader into a ring buffer and waits for completion.
func feedRingBuffer(t *testing.T, ring *RingBuffer, src io.Reader) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	done := make(chan error, 1)
	go func() {
		done <- ring.ReadFrom(ctx, src)
	}()

	select {
	case err := <-done:
		if err != nil && err != io.EOF && !errors.Is(err, context.Canceled) {
			t.Fatalf("ReadFrom error: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for ReadFrom to complete")
	}
}

// setupCursorAfterFeed sets up a cursor to read published messages after feedRingBuffer.
// Now that TryNextBatch delivers all messages before returning EOF, this just calls TryNextBatch.
func setupCursorAfterFeed(t *testing.T, cursor *Cursor) {
	t.Helper()
	if _, err := cursor.TryNextBatch(); err != nil {
		t.Fatalf("TryNextBatch failed: %v", err)
	}
}

// encodeClientMessages encodes frontend messages to wire format bytes.
func encodeClientMessages(msgs ...pgproto3.FrontendMessage) []byte {
	var buf bytes.Buffer
	for _, msg := range msgs {
		encoded, err := msg.Encode(nil)
		if err != nil {
			panic("failed to encode: " + err.Error())
		}
		buf.Write(encoded)
	}
	return buf.Bytes()
}

// assertMessageType checks that a RawMessageSource has the expected type.
func assertMessageType(t *testing.T, got RawMessageSource, wantType MsgType) {
	t.Helper()
	if got.MessageType() != wantType {
		t.Errorf("MessageType = %c, want %c", got.MessageType(), wantType)
	}
}

// assertBytesEqual checks that two byte slices are equal.
func assertBytesEqual(t *testing.T, got, want []byte) {
	t.Helper()
	if !bytes.Equal(got, want) {
		t.Errorf("bytes mismatch: got %d bytes, want %d bytes", len(got), len(want))
		if len(got) < 100 && len(want) < 100 {
			t.Errorf("  got:  %x", got)
			t.Errorf("  want: %x", want)
		}
	}
}

// ============================================================================
// RingBuffer Construction Tests
// ============================================================================

func TestRingBuffer_NewDefault(t *testing.T) {
	ring := NewRingBuffer(RingBufferConfig{})

	if ring.MessageBytes != DefaultMessageBytes {
		t.Errorf("MessageBytes = %d, want %d", ring.MessageBytes, DefaultMessageBytes)
	}
	if ring.MessageCount != DefaultMessageCount {
		t.Errorf("MessageCount = %d, want %d", ring.MessageCount, DefaultMessageCount)
	}
	if ring.HeadroomBytes != DefaultHeadroomBytes {
		t.Errorf("HeadroomBytes = %d, want %d", ring.HeadroomBytes, DefaultHeadroomBytes)
	}
}

func TestRingBuffer_NewWithConfig(t *testing.T) {
	forEachConfig(t, func(t *testing.T, cfg RingTestConfig) {
		ring := cfg.NewRing()

		if ring.MessageBytes != cfg.MessageBytes {
			t.Errorf("MessageBytes = %d, want %d", ring.MessageBytes, cfg.MessageBytes)
		}
		if ring.MessageCount != cfg.MessageCount {
			t.Errorf("MessageCount = %d, want %d", ring.MessageCount, cfg.MessageCount)
		}
		if ring.HeadroomBytes != cfg.headroom() {
			t.Errorf("HeadroomBytes = %d, want %d", ring.HeadroomBytes, cfg.headroom())
		}
	})
}

func TestRingBuffer_NewWithSize_InvalidPanics(t *testing.T) {
	tests := []struct {
		name         string
		messageBytes int64
		messageCount int64
	}{
		{"non-power-of-2 bytes", 1000, 256},
		{"non-power-of-2 count", 1024, 100},
		{"zero bytes", 0, 256},  // will default, so shouldn't panic
		{"zero count", 1024, 0}, // will default, so shouldn't panic
		{"negative bytes", -1024, 256},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.messageBytes == 0 || tt.messageCount == 0 {
				// These should NOT panic (defaults are applied)
				defer func() {
					if r := recover(); r != nil {
						t.Errorf("unexpected panic for zero value: %v", r)
					}
				}()
				NewRingBuffer(RingBufferConfig{
					MessageBytes: tt.messageBytes,
					MessageCount: tt.messageCount,
				})
				return
			}

			// These SHOULD panic
			defer func() {
				if r := recover(); r == nil {
					t.Error("expected panic for invalid config")
				}
			}()
			NewRingBuffer(RingBufferConfig{
				MessageBytes: tt.messageBytes,
				MessageCount: tt.messageCount,
			})
		})
	}
}

// ============================================================================
// Single Message Tests
// ============================================================================

func TestRingBuffer_SingleClientMessage(t *testing.T) {
	forEachConfig(t, func(t *testing.T, cfg RingTestConfig) {
		ring := cfg.NewRing()
		msg := &pgproto3.Query{String: "SELECT 1"}

		conn := NewTestConnFromClient(msg)
		feedRingBuffer(t, ring, conn)

		results := readAllMessages(t, ring, true)
		if len(results) != 1 {
			t.Fatalf("got %d messages, want 1", len(results))
		}

		assertMessageType(t, results[0], MsgClientQuery)

		// Verify body matches
		encoded, _ := msg.Encode(nil)
		wantBody := encoded[5:] // Skip header
		assertBytesEqual(t, results[0].Body, wantBody)
	})
}

func TestRingBuffer_SingleServerMessage(t *testing.T) {
	forEachConfig(t, func(t *testing.T, cfg RingTestConfig) {
		ring := cfg.NewRing()
		msg := &pgproto3.ReadyForQuery{TxStatus: 'I'}

		conn := NewTestConnFromServer(msg)
		feedRingBuffer(t, ring, conn)

		results := readAllMessages(t, ring, false)
		if len(results) != 1 {
			t.Fatalf("got %d messages, want 1", len(results))
		}

		assertMessageType(t, results[0], MsgServerReadyForQuery)

		// Verify body matches
		encoded, _ := msg.Encode(nil)
		wantBody := encoded[5:] // Skip header
		assertBytesEqual(t, results[0].Body, wantBody)
	})
}

func TestRingBuffer_MessageLen(t *testing.T) {
	forEachConfig(t, func(t *testing.T, cfg RingTestConfig) {
		ring := cfg.NewRing()
		msg := &pgproto3.Query{String: "SELECT * FROM users WHERE id = 123"}

		conn := NewTestConnFromClient(msg)
		feedRingBuffer(t, ring, conn)

		cursor := NewClientCursor(ring)

		// Wait for message to be published
		published := ring.PublishedMsgCount()
		if published == 0 {
			t.Fatal("expected message to be published")
		}

		// Set up cursor range
		cursor.endIdx = published
		cursor.msgIdx = cursor.startIdx - 1

		if !cursor.NextMsg() {
			t.Fatal("expected message")
		}

		encoded, _ := msg.Encode(nil)
		wantLen := int64(len(encoded))

		if cursor.MessageLen() != wantLen {
			t.Errorf("MessageLen = %d, want %d", cursor.MessageLen(), wantLen)
		}
		if cursor.BodyLen() != int(wantLen-5) {
			t.Errorf("BodyLen = %d, want %d", cursor.BodyLen(), wantLen-5)
		}
	})
}

// ============================================================================
// Multiple Message Tests
// ============================================================================

func TestRingBuffer_MultipleMessages(t *testing.T) {
	forEachConfig(t, func(t *testing.T, cfg RingTestConfig) {
		ring := cfg.NewRing()

		msgs := []pgproto3.FrontendMessage{
			&pgproto3.Query{String: "SELECT 1"},
			&pgproto3.Query{String: "SELECT 2"},
			&pgproto3.Query{String: "SELECT 3"},
			&pgproto3.Sync{},
			&pgproto3.Terminate{},
		}

		conn := NewTestConnFromClient(msgs...)
		feedRingBuffer(t, ring, conn)

		results := readAllMessages(t, ring, true)
		if len(results) != len(msgs) {
			t.Fatalf("got %d messages, want %d", len(results), len(msgs))
		}

		// Verify each message
		expectedTypes := []MsgType{
			MsgClientQuery, MsgClientQuery, MsgClientQuery,
			MsgClientSync, MsgClientTerminate,
		}
		for i, result := range results {
			assertMessageType(t, result, expectedTypes[i])
		}
	})
}

func TestRingBuffer_MixedMessageTypes(t *testing.T) {
	forEachConfig(t, func(t *testing.T, cfg RingTestConfig) {
		ring := cfg.NewRing()

		msgs := []pgproto3.BackendMessage{
			&pgproto3.AuthenticationOk{},
			&pgproto3.ParameterStatus{Name: "server_version", Value: "15.0"},
			&pgproto3.BackendKeyData{ProcessID: 12345, SecretKey: 67890},
			&pgproto3.ReadyForQuery{TxStatus: 'I'},
		}

		conn := NewTestConnFromServer(msgs...)
		feedRingBuffer(t, ring, conn)

		results := readAllMessages(t, ring, false)
		if len(results) != len(msgs) {
			t.Fatalf("got %d messages, want %d", len(results), len(msgs))
		}

		expectedTypes := []MsgType{
			MsgServerAuth, MsgServerParameterStatus,
			MsgServerBackendKeyData, MsgServerReadyForQuery,
		}
		for i, result := range results {
			assertMessageType(t, result, expectedTypes[i])
		}
	})
}

// ============================================================================
// Wraparound Tests
// ============================================================================

func TestRingBuffer_DataWraparound(t *testing.T) {
	// Use a small buffer to force wraparound. With 256 bytes and 5 byte headroom,
	// we have 251 bytes usable. Query messages are ~10 bytes each.
	cfg := RingTestConfig{
		Name:          "small",
		MessageBytes:  256, // Small enough to force wraparound with many messages
		MessageCount:  64,
		HeadroomBytes: 5,
	}
	ring := cfg.NewRing()

	// Create many messages to force wraparound
	// Each Query "Qn" is: 5 byte header + "Qn" + null = 5 + 3 = 8 bytes
	// With 251 usable bytes, we need about 30+ messages to wrap
	var msgs []pgproto3.FrontendMessage
	for i := 0; i < 40; i++ {
		msgs = append(msgs, &pgproto3.Query{String: fmt.Sprintf("Q%d", i)})
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	conn := NewTestConnFromClient(msgs...)

	// Use StartNetConnReader so we can consume incrementally
	ring.StartNetConnReader(ctx, conn)

	cursor := NewClientCursor(ring)
	var results []RawBody

	timeout := time.After(5 * time.Second)
readLoop:
	for len(results) < len(msgs) {
		select {
		case <-timeout:
			t.Fatalf("timeout: got %d messages, want %d", len(results), len(msgs))
		case <-cursor.Done():
			// Check for remaining messages
			published := ring.PublishedMsgCount()
			if published > cursor.End() {
				cursor.endIdx = published
				cursor.msgIdx = cursor.startIdx - 1
				for cursor.NextMsg() {
					results = append(results, cursor.Retain().(RawBody))
				}
			}
			break readLoop
		case <-cursor.Ready():
		}

		// Check for new messages
		published := ring.PublishedMsgCount()
		if published > cursor.End() {
			cursor.endIdx = published
			cursor.msgIdx = cursor.startIdx - 1

			for cursor.NextMsg() {
				results = append(results, cursor.Retain().(RawBody))
			}

			// Release consumed messages to free space for writer
			ring.ReleaseThrough(cursor.endIdx)
			cursor.startIdx = cursor.endIdx
			cursor.msgIdx = cursor.startIdx - 1
		}
	}

	if len(results) != len(msgs) {
		t.Fatalf("got %d messages, want %d", len(results), len(msgs))
	}

	// Verify message contents
	for i, result := range results {
		assertMessageType(t, result, MsgClientQuery)
		encoded, _ := msgs[i].Encode(nil)
		wantBody := encoded[5:]
		assertBytesEqual(t, result.Body, wantBody)
	}
}

func TestRingBuffer_HeaderWraparound(t *testing.T) {
	// Test that messages whose 5-byte header spans the buffer boundary work correctly
	// Use a small buffer (256 bytes) and fill to force the header to wrap
	cfg := RingTestConfig{
		Name:          "small",
		MessageBytes:  256,
		MessageCount:  64,
		HeadroomBytes: 5,
	}

	ring := cfg.NewRing()

	// Create many Sync messages (5 bytes each) to fill the buffer
	// Plus a Query message that will have its header at different positions
	// as we wrap around
	var msgs []pgproto3.FrontendMessage
	for i := 0; i < 50; i++ {
		msgs = append(msgs, &pgproto3.Sync{})
	}
	// Add a Query message - its header might land at various positions
	msgs = append(msgs, &pgproto3.Query{String: "WRAP"})
	// Add more syncs to continue wrapping
	for i := 0; i < 10; i++ {
		msgs = append(msgs, &pgproto3.Sync{})
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	conn := NewTestConnFromClient(msgs...)
	ring.StartNetConnReader(ctx, conn)

	cursor := NewClientCursor(ring)
	var results []RawBody
	timeout := time.After(5 * time.Second)

readLoop:
	for len(results) < len(msgs) {
		select {
		case <-timeout:
			t.Fatalf("timeout: got %d messages, want %d", len(results), len(msgs))
		case <-cursor.Done():
			// Check for remaining messages
			published := ring.PublishedMsgCount()
			if published > cursor.End() {
				cursor.endIdx = published
				cursor.msgIdx = cursor.startIdx - 1
				for cursor.NextMsg() {
					results = append(results, cursor.Retain().(RawBody))
				}
			}
			break readLoop
		case <-cursor.Ready():
		}

		// Check for new messages
		published := ring.PublishedMsgCount()
		if published > cursor.End() {
			cursor.endIdx = published
			cursor.msgIdx = cursor.startIdx - 1

			for cursor.NextMsg() {
				results = append(results, cursor.Retain().(RawBody))
			}

			// Release consumed messages to free space
			ring.ReleaseThrough(cursor.endIdx)
			cursor.startIdx = cursor.endIdx
			cursor.msgIdx = cursor.startIdx - 1
		}
	}

	if len(results) != len(msgs) {
		t.Fatalf("got %d messages, want %d", len(results), len(msgs))
	}

	// Verify the Query message (at index 50) is correct
	query := results[50]
	assertMessageType(t, query, MsgClientQuery)
}

// ============================================================================
// Space Management Tests
// ============================================================================

func TestRingBuffer_ReleaseThrough(t *testing.T) {
	forEachConfig(t, func(t *testing.T, cfg RingTestConfig) {
		ring := cfg.NewRing()

		// Send multiple messages
		msgs := []pgproto3.FrontendMessage{
			&pgproto3.Query{String: "SELECT 1"},
			&pgproto3.Query{String: "SELECT 2"},
			&pgproto3.Query{String: "SELECT 3"},
		}

		conn := NewTestConnFromClient(msgs...)
		feedRingBuffer(t, ring, conn)

		cursor := NewClientCursor(ring)

		// Check that messages are published
		published := ring.PublishedMsgCount()
		if published != 3 {
			t.Fatalf("expected 3 published messages, got %d", published)
		}

		// Set up cursor range
		cursor.endIdx = published
		cursor.msgIdx = cursor.startIdx - 1

		// Consume messages
		count := 0
		for cursor.NextMsg() {
			count++
		}

		if count != 3 {
			t.Fatalf("got %d messages, want 3", count)
		}

		// Release the consumed messages
		ring.ReleaseThrough(cursor.endIdx)

		// Verify no more messages (we're at EOF)
		if ring.PublishedMsgCount() != cursor.endIdx {
			t.Error("expected no more messages after release")
		}
	})
}

// ============================================================================
// Error and Done Handling Tests
// ============================================================================

func TestRingBuffer_EOF(t *testing.T) {
	forEachConfig(t, func(t *testing.T, cfg RingTestConfig) {
		ring := cfg.NewRing()

		// Empty reader - immediate EOF
		conn := NewTestConnFromBytes(nil)
		feedRingBuffer(t, ring, conn)

		// Done channel should be closed
		select {
		case <-ring.Done():
			// Good
		case <-time.After(time.Second):
			t.Fatal("Done channel not closed after EOF")
		}

		// Error should be EOF
		if ring.Error() != io.EOF {
			t.Errorf("Error = %v, want io.EOF", ring.Error())
		}
	})
}

func TestRingBuffer_Close(t *testing.T) {
	forEachConfig(t, func(t *testing.T, cfg RingTestConfig) {
		ring := cfg.NewRing()

		// Close immediately
		ring.Close()

		// Done channel should be closed
		select {
		case <-ring.Done():
			// Good
		case <-time.After(time.Second):
			t.Fatal("Done channel not closed after Close")
		}

		// Error should be EOF
		if ring.Error() != io.EOF {
			t.Errorf("Error = %v, want io.EOF", ring.Error())
		}
	})
}

// ============================================================================
// Streaming (Oversized Message) Tests
// ============================================================================

func TestRingBuffer_StreamingMessage(t *testing.T) {
	// Use 16KB-minhead config where streaming threshold is 16KB - 5 = ~16KB
	cfg := RingTestConfig{
		Name:          "16KB-minhead",
		MessageBytes:  16 * 1024,
		MessageCount:  256,
		HeadroomBytes: 5,
	}

	ring := cfg.NewRing()

	// Create a message larger than the streaming threshold
	streamingThreshold := cfg.StreamingThreshold()
	dataSize := streamingThreshold + 1024 // Just over threshold
	largeData := make([]byte, dataSize)
	for i := range largeData {
		largeData[i] = byte(i % 256)
	}

	msg := &pgproto3.CopyData{Data: largeData}
	conn := NewTestConnFromClient(msg)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Use StartNetConnReader so streaming can access the connection
	ring.StartNetConnReader(ctx, conn)

	cursor := NewClientCursor(ring)
	timeout := time.After(10 * time.Second)

	// Wait for streaming message to be detected
	var gotBatch bool
	for !gotBatch {
		select {
		case <-timeout:
			t.Fatal("timeout waiting for streaming message")
		case <-cursor.Done():
			t.Fatalf("cursor done unexpectedly: %v", cursor.Err())
		case <-cursor.Ready():
		}

		// Check for new messages
		published := ring.PublishedMsgCount()
		if published > cursor.End() {
			gotBatch = true
			cursor.endIdx = published
			cursor.msgIdx = cursor.startIdx - 1
		}
	}

	if !cursor.NextMsg() {
		t.Fatal("expected message")
	}

	// Read the streaming message through RingRange.NewReader()
	rng := cursor.Slice(cursor.MsgIdx(), cursor.MsgIdx()+1)
	reader := rng.NewReader()

	var buf bytes.Buffer
	_, err := io.Copy(&buf, reader)
	if err != nil {
		t.Fatalf("error reading streaming message: %v", err)
	}

	// Verify the full wire message
	encoded, _ := msg.Encode(nil)
	if !bytes.Equal(buf.Bytes(), encoded) {
		t.Errorf("streaming message mismatch: got %d bytes, want %d bytes",
			buf.Len(), len(encoded))
	}
}

func TestRingBuffer_StreamingMessage_AfterBuffered(t *testing.T) {
	cfg := RingTestConfig{
		Name:          "16KB-minhead",
		MessageBytes:  16 * 1024,
		MessageCount:  256,
		HeadroomBytes: 5,
	}

	ring := cfg.NewRing()

	// First some small buffered messages, then a large streaming one
	streamingThreshold := cfg.StreamingThreshold()
	largeData := make([]byte, streamingThreshold+100)
	for i := range largeData {
		largeData[i] = byte(i % 256)
	}

	msgs := []pgproto3.FrontendMessage{
		&pgproto3.Query{String: "SELECT 1"},
		&pgproto3.Sync{},
		&pgproto3.CopyData{Data: largeData}, // This will stream
	}

	conn := NewTestConnFromClient(msgs...)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Use StartNetConnReader so streaming can access the connection
	ring.StartNetConnReader(ctx, conn)

	cursor := NewClientCursor(ring)
	var results [][]byte
	timeout := time.After(10 * time.Second)

readLoop:
	for len(results) < len(msgs) {
		select {
		case <-timeout:
			t.Fatalf("timeout: got %d messages, want %d", len(results), len(msgs))
		case <-cursor.Done():
			// Check for remaining messages before breaking
			published := ring.PublishedMsgCount()
			if published > cursor.End() {
				cursor.endIdx = published
				cursor.msgIdx = cursor.startIdx - 1
				for cursor.NextMsg() {
					rng := cursor.Slice(cursor.MsgIdx(), cursor.MsgIdx()+1)
					data, err := io.ReadAll(rng.NewReader())
					if err != nil {
						t.Fatalf("error reading message: %v", err)
					}
					results = append(results, data)
				}
			}
			if cursor.Err() != nil && cursor.Err() != io.EOF {
				t.Fatalf("cursor error: %v", cursor.Err())
			}
			break readLoop
		case <-cursor.Ready():
		}

		// Check for new messages
		published := ring.PublishedMsgCount()
		if published > cursor.End() {
			cursor.endIdx = published
			cursor.msgIdx = cursor.startIdx - 1

			for cursor.NextMsg() {
				// Read through RingRange.NewReader() to handle streaming
				rng := cursor.Slice(cursor.MsgIdx(), cursor.MsgIdx()+1)
				reader := rng.NewReader()
				data, err := io.ReadAll(reader)
				if err != nil {
					t.Fatalf("error reading message: %v", err)
				}
				results = append(results, data)
			}
			// Prepare for next batch
			cursor.startIdx = cursor.endIdx
			cursor.msgIdx = cursor.startIdx - 1
		}
	}

	if len(results) != len(msgs) {
		t.Fatalf("got %d messages, want %d", len(results), len(msgs))
	}

	// Verify each message
	for i, result := range results {
		encoded, _ := msgs[i].Encode(nil)
		if !bytes.Equal(result, encoded) {
			t.Errorf("message %d mismatch: got %d bytes, want %d bytes",
				i, len(result), len(encoded))
		}
	}
}

// ============================================================================
// COPY Message Flow Tests
// ============================================================================

func TestRingBuffer_CopyIn_SmallData(t *testing.T) {
	forEachConfig(t, func(t *testing.T, cfg RingTestConfig) {
		ring := cfg.NewRing()

		// Simulate client sending COPY data
		msgs := []pgproto3.FrontendMessage{
			&pgproto3.CopyData{Data: []byte("row1,data1\n")},
			&pgproto3.CopyData{Data: []byte("row2,data2\n")},
			&pgproto3.CopyData{Data: []byte("row3,data3\n")},
			&pgproto3.CopyDone{},
		}

		conn := NewTestConnFromClient(msgs...)
		feedRingBuffer(t, ring, conn)

		results := readAllMessages(t, ring, true)
		if len(results) != len(msgs) {
			t.Fatalf("got %d messages, want %d", len(results), len(msgs))
		}

		// Verify types
		for i := 0; i < 3; i++ {
			assertMessageType(t, results[i], MsgClientCopyData)
		}
		assertMessageType(t, results[3], MsgClientCopyDone)
	})
}

func TestRingBuffer_CopyOut_SmallData(t *testing.T) {
	forEachConfig(t, func(t *testing.T, cfg RingTestConfig) {
		ring := cfg.NewRing()

		// Simulate server sending COPY data
		msgs := []pgproto3.BackendMessage{
			&pgproto3.CopyOutResponse{
				OverallFormat:     0,
				ColumnFormatCodes: []uint16{0, 0, 0},
			},
			&pgproto3.CopyData{Data: []byte("row1\tdata1\n")},
			&pgproto3.CopyData{Data: []byte("row2\tdata2\n")},
			&pgproto3.CopyDone{},
			&pgproto3.CommandComplete{CommandTag: []byte("COPY 2")},
			&pgproto3.ReadyForQuery{TxStatus: 'I'},
		}

		conn := NewTestConnFromServer(msgs...)
		feedRingBuffer(t, ring, conn)

		results := readAllMessages(t, ring, false)
		if len(results) != len(msgs) {
			t.Fatalf("got %d messages, want %d", len(results), len(msgs))
		}

		// Verify types
		assertMessageType(t, results[0], MsgServerCopyOutResponse)
		assertMessageType(t, results[1], MsgServerCopyData)
		assertMessageType(t, results[2], MsgServerCopyData)
		assertMessageType(t, results[3], MsgServerCopyDone)
		assertMessageType(t, results[4], MsgServerCommandComplete)
		assertMessageType(t, results[5], MsgServerReadyForQuery)
	})
}

// ============================================================================
// Exhaustive Message Type Tests
// ============================================================================

func TestRingBuffer_AllClientMessageTypes(t *testing.T) {
	forEachConfig(t, func(t *testing.T, cfg RingTestConfig) {
		ring := cfg.NewRing()

		msgs := []pgproto3.FrontendMessage{
			&pgproto3.Bind{
				DestinationPortal:    "portal1",
				PreparedStatement:    "stmt1",
				ParameterFormatCodes: []int16{0},
				Parameters:           [][]byte{[]byte("param1")},
				ResultFormatCodes:    []int16{0},
			},
			&pgproto3.Close{ObjectType: 'S', Name: "stmt1"},
			&pgproto3.CopyData{Data: []byte("test data")},
			&pgproto3.CopyDone{},
			&pgproto3.CopyFail{Message: "copy failed"},
			&pgproto3.Describe{ObjectType: 'S', Name: "stmt1"},
			&pgproto3.Execute{Portal: "portal1", MaxRows: 100},
			&pgproto3.Flush{},
			&pgproto3.FunctionCall{
				Function:         12345,
				ArgFormatCodes:   []uint16{0},
				Arguments:        [][]byte{[]byte("arg1")},
				ResultFormatCode: 0,
			},
			&pgproto3.Parse{Name: "stmt1", Query: "SELECT $1", ParameterOIDs: []uint32{25}},
			&pgproto3.PasswordMessage{Password: "secret"},
			&pgproto3.Query{String: "SELECT 1"},
			&pgproto3.Sync{},
			&pgproto3.Terminate{},
		}

		conn := NewTestConnFromClient(msgs...)
		feedRingBuffer(t, ring, conn)

		results := readAllMessages(t, ring, true)
		if len(results) != len(msgs) {
			t.Fatalf("got %d messages, want %d", len(results), len(msgs))
		}

		expectedTypes := []MsgType{
			MsgClientBind, MsgClientClose, MsgClientCopyData, MsgClientCopyDone,
			MsgClientCopyFail, MsgClientDescribe, MsgClientExecute, MsgClientFlush,
			MsgClientFunc, MsgClientParse, MsgClientPassword, MsgClientQuery,
			MsgClientSync, MsgClientTerminate,
		}

		for i, result := range results {
			t.Run(MsgName.Get(expectedTypes[i]), func(t *testing.T) {
				assertMessageType(t, result, expectedTypes[i])

				// Verify body matches
				encoded, _ := msgs[i].Encode(nil)
				wantBody := encoded[5:]
				assertBytesEqual(t, result.Body, wantBody)
			})
		}
	})
}

func TestRingBuffer_AllServerMessageTypes(t *testing.T) {
	forEachConfig(t, func(t *testing.T, cfg RingTestConfig) {
		ring := cfg.NewRing()

		msgs := []pgproto3.BackendMessage{
			&pgproto3.AuthenticationOk{},
			&pgproto3.AuthenticationCleartextPassword{},
			&pgproto3.AuthenticationMD5Password{Salt: [4]byte{1, 2, 3, 4}},
			&pgproto3.BackendKeyData{ProcessID: 12345, SecretKey: 67890},
			&pgproto3.BindComplete{},
			&pgproto3.CloseComplete{},
			&pgproto3.CommandComplete{CommandTag: []byte("SELECT 1")},
			&pgproto3.CopyBothResponse{OverallFormat: 0, ColumnFormatCodes: []uint16{0}},
			&pgproto3.CopyData{Data: []byte("copy data")},
			&pgproto3.CopyDone{},
			&pgproto3.CopyInResponse{OverallFormat: 0, ColumnFormatCodes: []uint16{0}},
			&pgproto3.CopyOutResponse{OverallFormat: 0, ColumnFormatCodes: []uint16{0}},
			&pgproto3.DataRow{Values: [][]byte{[]byte("col1"), []byte("col2")}},
			&pgproto3.EmptyQueryResponse{},
			&pgproto3.ErrorResponse{
				Severity: "ERROR",
				Code:     "42000",
				Message:  "test error",
			},
			&pgproto3.FunctionCallResponse{Result: []byte("result")},
			&pgproto3.NoData{},
			&pgproto3.NoticeResponse{
				Severity: "NOTICE",
				Message:  "test notice",
			},
			&pgproto3.NotificationResponse{
				PID:     12345,
				Channel: "test_channel",
				Payload: "test payload",
			},
			&pgproto3.ParameterDescription{ParameterOIDs: []uint32{25, 23}},
			&pgproto3.ParameterStatus{Name: "server_version", Value: "15.0"},
			&pgproto3.ParseComplete{},
			&pgproto3.PortalSuspended{},
			&pgproto3.ReadyForQuery{TxStatus: 'I'},
			&pgproto3.RowDescription{Fields: []pgproto3.FieldDescription{
				{Name: []byte("col1"), DataTypeOID: 25, DataTypeSize: -1},
			}},
		}

		conn := NewTestConnFromServer(msgs...)
		feedRingBuffer(t, ring, conn)

		results := readAllMessages(t, ring, false)
		if len(results) != len(msgs) {
			t.Fatalf("got %d messages, want %d", len(results), len(msgs))
		}

		// Just verify we got the right number and can parse them all
		for i, result := range results {
			t.Run(MsgName.Get(result.Type), func(t *testing.T) {
				// Verify body matches
				encoded, _ := msgs[i].Encode(nil)
				wantBody := encoded[5:]
				assertBytesEqual(t, result.Body, wantBody)
			})
		}
	})
}

// ============================================================================
// Cursor Tests
// ============================================================================

func TestCursor_NewClient(t *testing.T) {
	forEachConfig(t, func(t *testing.T, cfg RingTestConfig) {
		ring := cfg.NewRing()
		cursor := NewClientCursor(ring)

		if cursor.clientFlyweights == nil {
			t.Error("client cursor should have clientFlyweights")
		}
		if cursor.serverFlyweights != nil {
			t.Error("client cursor should not have serverFlyweights")
		}
	})
}

func TestCursor_NewServer(t *testing.T) {
	forEachConfig(t, func(t *testing.T, cfg RingTestConfig) {
		ring := cfg.NewRing()
		cursor := NewServerCursor(ring)

		if cursor.serverFlyweights == nil {
			t.Error("server cursor should have serverFlyweights")
		}
		if cursor.clientFlyweights != nil {
			t.Error("server cursor should not have clientFlyweights")
		}
	})
}

func TestCursor_TryNextBatch_Available(t *testing.T) {
	// NOTE: This test uses StartNetConnReader so the ring buffer is NOT at EOF
	// when we call TryNextBatch. If we used feedRingBuffer, the ring would be
	// at EOF and TryNextBatch would return EOF error.
	forEachConfig(t, func(t *testing.T, cfg RingTestConfig) {
		ring := cfg.NewRing()

		msgs := []pgproto3.FrontendMessage{
			&pgproto3.Query{String: "SELECT 1"},
			&pgproto3.Sync{},
		}

		conn := NewTestConnFromClient(msgs...)

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		ring.StartNetConnReader(ctx, conn)

		cursor := NewClientCursor(ring)

		// Wait for messages to be available
		timeout := time.After(5 * time.Second)
		for ring.PublishedMsgCount() == 0 {
			select {
			case <-timeout:
				t.Fatal("timeout waiting for messages")
			case <-cursor.Ready():
			case <-cursor.Done():
				t.Fatalf("cursor done: %v", cursor.Err())
			}
		}

		// Now TryNextBatch should work (ring may or may not be at EOF yet)
		got, err := cursor.TryNextBatch()
		if err != nil && err != io.EOF {
			t.Fatalf("TryNextBatch error: %v", err)
		}
		// If not at EOF, got should be true
		if err == nil && !got {
			t.Error("expected batch to be available")
		}
		// If at EOF, we can still check published count
		if err == io.EOF && ring.PublishedMsgCount() == 0 {
			t.Error("expected messages to be published")
		}
	})
}

func TestCursor_TryNextBatch_Empty(t *testing.T) {
	forEachConfig(t, func(t *testing.T, cfg RingTestConfig) {
		ring := cfg.NewRing()
		cursor := NewClientCursor(ring)

		// No messages yet - should return false immediately
		got, err := cursor.TryNextBatch()
		if err != nil {
			t.Fatalf("TryNextBatch error: %v", err)
		}
		if got {
			t.Error("expected no batch on empty ring")
		}
	})
}

func TestCursor_TryNextBatch_EOF(t *testing.T) {
	forEachConfig(t, func(t *testing.T, cfg RingTestConfig) {
		ring := cfg.NewRing()

		// Feed empty reader to trigger EOF
		conn := NewTestConnFromBytes(nil)
		feedRingBuffer(t, ring, conn)

		cursor := NewClientCursor(ring)

		_, err := cursor.TryNextBatch()
		if err != io.EOF {
			t.Errorf("expected io.EOF, got %v", err)
		}
	})
}

func TestCursor_NextMsg(t *testing.T) {
	forEachConfig(t, func(t *testing.T, cfg RingTestConfig) {
		ring := cfg.NewRing()

		msgs := []pgproto3.FrontendMessage{
			&pgproto3.Query{String: "Q1"},
			&pgproto3.Query{String: "Q2"},
			&pgproto3.Query{String: "Q3"},
		}

		conn := NewTestConnFromClient(msgs...)
		feedRingBuffer(t, ring, conn)

		cursor := NewClientCursor(ring)
		setupCursorAfterFeed(t, cursor)

		// Should iterate through all messages
		count := 0
		for cursor.NextMsg() {
			count++
			assertMessageType(t, cursor, MsgClientQuery)
		}

		if count != 3 {
			t.Errorf("got %d messages, want 3", count)
		}

		// NextMsg should return false at end
		if cursor.NextMsg() {
			t.Error("NextMsg should return false at end of batch")
		}
	})
}

func TestCursor_PrevMsg(t *testing.T) {
	forEachConfig(t, func(t *testing.T, cfg RingTestConfig) {
		ring := cfg.NewRing()

		msgs := []pgproto3.FrontendMessage{
			&pgproto3.Query{String: "Q1"},
			&pgproto3.Query{String: "Q2"},
			&pgproto3.Query{String: "Q3"},
		}

		conn := NewTestConnFromClient(msgs...)
		feedRingBuffer(t, ring, conn)

		cursor := NewClientCursor(ring)
		setupCursorAfterFeed(t, cursor)

		// Move to end
		for cursor.NextMsg() {
		}

		// Now go backwards
		backCount := 0
		for cursor.PrevMsg() {
			backCount++
		}

		if backCount != 3 {
			t.Errorf("got %d messages going backwards, want 3", backCount)
		}
	})
}

func TestCursor_AsClient_OnServerCursor_Panics(t *testing.T) {
	forEachConfig(t, func(t *testing.T, cfg RingTestConfig) {
		ring := cfg.NewRing()
		cursor := NewServerCursor(ring)

		defer func() {
			if r := recover(); r == nil {
				t.Error("expected panic calling AsClient on server cursor")
			}
		}()

		cursor.AsClient()
	})
}

func TestCursor_AsServer_OnClientCursor_Panics(t *testing.T) {
	forEachConfig(t, func(t *testing.T, cfg RingTestConfig) {
		ring := cfg.NewRing()
		cursor := NewClientCursor(ring)

		defer func() {
			if r := recover(); r == nil {
				t.Error("expected panic calling AsServer on client cursor")
			}
		}()

		cursor.AsServer()
	})
}

func TestCursor_AsClient(t *testing.T) {
	forEachConfig(t, func(t *testing.T, cfg RingTestConfig) {
		ring := cfg.NewRing()

		msg := &pgproto3.Query{String: "SELECT 42"}
		conn := NewTestConnFromClient(msg)
		feedRingBuffer(t, ring, conn)

		cursor := NewClientCursor(ring)
		setupCursorAfterFeed(t, cursor)

		if !cursor.NextMsg() {
			t.Fatal("expected message")
		}

		parsed, err := cursor.AsClient()
		if err != nil {
			t.Fatalf("AsClient error: %v", err)
		}

		// The parsed message should be a ClientSimpleQueryQuery
		query, ok := parsed.(*ClientSimpleQueryQuery)
		if !ok {
			t.Fatalf("expected *ClientSimpleQueryQuery, got %T", parsed)
		}

		// Access the parsed Query message
		q := query.Parse()
		if q.String != "SELECT 42" {
			t.Errorf("Query.String = %q, want %q", q.String, "SELECT 42")
		}
	})
}

func TestCursor_AsServer(t *testing.T) {
	forEachConfig(t, func(t *testing.T, cfg RingTestConfig) {
		ring := cfg.NewRing()

		msg := &pgproto3.ReadyForQuery{TxStatus: 'T'}
		conn := NewTestConnFromServer(msg)
		feedRingBuffer(t, ring, conn)

		cursor := NewServerCursor(ring)
		setupCursorAfterFeed(t, cursor)

		if !cursor.NextMsg() {
			t.Fatal("expected message")
		}

		parsed, err := cursor.AsServer()
		if err != nil {
			t.Fatalf("AsServer error: %v", err)
		}

		// The parsed message should be a ServerResponseReadyForQuery
		rfq, ok := parsed.(*ServerResponseReadyForQuery)
		if !ok {
			t.Fatalf("expected *ServerResponseReadyForQuery, got %T", parsed)
		}

		// Access the parsed ReadyForQuery message
		r := rfq.Parse()
		if r.TxStatus != 'T' {
			t.Errorf("TxStatus = %c, want %c", r.TxStatus, 'T')
		}
	})
}

func TestCursor_Retain(t *testing.T) {
	forEachConfig(t, func(t *testing.T, cfg RingTestConfig) {
		ring := cfg.NewRing()

		msg := &pgproto3.Query{String: "RETAIN ME"}
		conn := NewTestConnFromClient(msg)
		feedRingBuffer(t, ring, conn)

		cursor := NewClientCursor(ring)
		setupCursorAfterFeed(t, cursor)

		if !cursor.NextMsg() {
			t.Fatal("expected message")
		}

		retained := cursor.Retain()

		// Should be a RawBody
		rawBody, ok := retained.(RawBody)
		if !ok {
			t.Fatalf("expected RawBody, got %T", retained)
		}

		// Verify content
		assertMessageType(t, rawBody, MsgClientQuery)
		encoded, _ := msg.Encode(nil)
		wantBody := encoded[5:]
		assertBytesEqual(t, rawBody.Body, wantBody)
	})
}

// ============================================================================
// RingRange Tests
// ============================================================================

func TestRingRange_Properties(t *testing.T) {
	forEachConfig(t, func(t *testing.T, cfg RingTestConfig) {
		ring := cfg.NewRing()

		msgs := []pgproto3.FrontendMessage{
			&pgproto3.Query{String: "Q1"},
			&pgproto3.Query{String: "Q2"},
			&pgproto3.Query{String: "Q3"},
		}

		conn := NewTestConnFromClient(msgs...)
		feedRingBuffer(t, ring, conn)

		cursor := NewClientCursor(ring)
		setupCursorAfterFeed(t, cursor)

		rng := &cursor.RingRange

		if rng.Start() != 0 {
			t.Errorf("Start = %d, want 0", rng.Start())
		}
		if rng.End() != 3 {
			t.Errorf("End = %d, want 3", rng.End())
		}
		if rng.Len() != 3 {
			t.Errorf("Len = %d, want 3", rng.Len())
		}
		if rng.Empty() {
			t.Error("range should not be empty")
		}

		// Bytes should be total wire bytes
		var totalBytes int64
		for _, msg := range msgs {
			encoded, _ := msg.Encode(nil)
			totalBytes += int64(len(encoded))
		}
		if rng.Bytes() != totalBytes {
			t.Errorf("Bytes = %d, want %d", rng.Bytes(), totalBytes)
		}
	})
}

func TestRingRange_Empty(t *testing.T) {
	forEachConfig(t, func(t *testing.T, cfg RingTestConfig) {
		ring := cfg.NewRing()

		// Feed and consume a message to get a valid range
		msg := &pgproto3.Sync{}
		conn := NewTestConnFromClient(msg)
		feedRingBuffer(t, ring, conn)

		cursor := NewClientCursor(ring)
		setupCursorAfterFeed(t, cursor)

		// Slice to create empty range
		rng := &cursor.RingRange
		emptyRng := rng.Slice(0, 0)

		if !emptyRng.Empty() {
			t.Error("range should be empty")
		}
		if emptyRng.Len() != 0 {
			t.Errorf("Len = %d, want 0", emptyRng.Len())
		}
	})
}

func TestRingRange_Slice(t *testing.T) {
	forEachConfig(t, func(t *testing.T, cfg RingTestConfig) {
		ring := cfg.NewRing()

		msgs := []pgproto3.FrontendMessage{
			&pgproto3.Query{String: "Q0"},
			&pgproto3.Query{String: "Q1"},
			&pgproto3.Query{String: "Q2"},
			&pgproto3.Query{String: "Q3"},
		}

		conn := NewTestConnFromClient(msgs...)
		feedRingBuffer(t, ring, conn)

		cursor := NewClientCursor(ring)
		setupCursorAfterFeed(t, cursor)

		rng := &cursor.RingRange

		// Slice middle two messages
		sliced := rng.Slice(1, 3)

		if sliced.Start() != 1 {
			t.Errorf("sliced Start = %d, want 1", sliced.Start())
		}
		if sliced.End() != 3 {
			t.Errorf("sliced End = %d, want 3", sliced.End())
		}
		if sliced.Len() != 2 {
			t.Errorf("sliced Len = %d, want 2", sliced.Len())
		}
	})
}

func TestRingRange_Slice_OutOfBounds_Panics(t *testing.T) {
	forEachConfig(t, func(t *testing.T, cfg RingTestConfig) {
		ring := cfg.NewRing()

		msgs := []pgproto3.FrontendMessage{
			&pgproto3.Sync{},
			&pgproto3.Sync{},
		}

		conn := NewTestConnFromClient(msgs...)
		feedRingBuffer(t, ring, conn)

		cursor := NewClientCursor(ring)
		setupCursorAfterFeed(t, cursor)

		rng := &cursor.RingRange

		defer func() {
			if r := recover(); r == nil {
				t.Error("expected panic for out of bounds slice")
			}
		}()

		rng.Slice(0, 10) // End is out of bounds
	})
}

func TestRingRange_Contains(t *testing.T) {
	forEachConfig(t, func(t *testing.T, cfg RingTestConfig) {
		ring := cfg.NewRing()

		msgs := []pgproto3.FrontendMessage{
			&pgproto3.Sync{},
			&pgproto3.Sync{},
			&pgproto3.Sync{},
		}

		conn := NewTestConnFromClient(msgs...)
		feedRingBuffer(t, ring, conn)

		cursor := NewClientCursor(ring)
		setupCursorAfterFeed(t, cursor)

		rng := &cursor.RingRange // [0, 3)

		tests := []struct {
			start, end int64
			want       bool
		}{
			{0, 3, true},   // Exact match
			{0, 2, true},   // Subset
			{1, 3, true},   // Subset
			{1, 2, true},   // Middle
			{0, 4, false},  // End out of bounds
			{-1, 2, false}, // Start out of bounds
		}

		for _, tt := range tests {
			got := rng.Contains(tt.start, tt.end)
			if got != tt.want {
				t.Errorf("Contains(%d, %d) = %v, want %v", tt.start, tt.end, got, tt.want)
			}
		}
	})
}

func TestRingRange_NewReader(t *testing.T) {
	forEachConfig(t, func(t *testing.T, cfg RingTestConfig) {
		ring := cfg.NewRing()

		msgs := []pgproto3.FrontendMessage{
			&pgproto3.Query{String: "SELECT 1"},
			&pgproto3.Sync{},
		}

		conn := NewTestConnFromClient(msgs...)
		feedRingBuffer(t, ring, conn)

		cursor := NewClientCursor(ring)
		setupCursorAfterFeed(t, cursor)

		rng := &cursor.RingRange
		reader := rng.NewReader()

		// Read all bytes
		data, err := io.ReadAll(reader)
		if err != nil {
			t.Fatalf("ReadAll error: %v", err)
		}

		// Verify it matches encoded messages
		expected := encodeClientMessages(msgs...)
		assertBytesEqual(t, data, expected)
	})
}

func TestRingRange_NewReader_Empty(t *testing.T) {
	forEachConfig(t, func(t *testing.T, cfg RingTestConfig) {
		ring := cfg.NewRing()

		msg := &pgproto3.Sync{}
		conn := NewTestConnFromClient(msg)
		feedRingBuffer(t, ring, conn)

		cursor := NewClientCursor(ring)
		setupCursorAfterFeed(t, cursor)

		// Create empty range
		emptyRng := cursor.Slice(0, 0)
		reader := emptyRng.NewReader()

		data, err := io.ReadAll(reader)
		if err != nil {
			t.Fatalf("ReadAll error: %v", err)
		}
		if len(data) != 0 {
			t.Errorf("expected empty reader, got %d bytes", len(data))
		}
	})
}

// ============================================================================
// RingMsg Tests
// ============================================================================

func TestRingMsg_Navigation(t *testing.T) {
	forEachConfig(t, func(t *testing.T, cfg RingTestConfig) {
		ring := cfg.NewRing()

		msgs := []pgproto3.FrontendMessage{
			&pgproto3.Query{String: "Q0"},
			&pgproto3.Query{String: "Q1"},
			&pgproto3.Query{String: "Q2"},
		}

		conn := NewTestConnFromClient(msgs...)
		feedRingBuffer(t, ring, conn)

		cursor := NewClientCursor(ring)
		setupCursorAfterFeed(t, cursor)

		// Check MsgIdx as we navigate
		if cursor.NextMsg() {
			if cursor.MsgIdx() != 0 {
				t.Errorf("MsgIdx = %d, want 0", cursor.MsgIdx())
			}
		}

		if cursor.NextMsg() {
			if cursor.MsgIdx() != 1 {
				t.Errorf("MsgIdx = %d, want 1", cursor.MsgIdx())
			}
		}

		if cursor.NextMsg() {
			if cursor.MsgIdx() != 2 {
				t.Errorf("MsgIdx = %d, want 2", cursor.MsgIdx())
			}
		}

		// At end
		if cursor.NextMsg() {
			t.Error("NextMsg should return false at end")
		}

		// Go back
		if !cursor.PrevMsg() {
			t.Error("PrevMsg should return true")
		}
		if cursor.MsgIdx() != 2 {
			t.Errorf("MsgIdx = %d, want 2", cursor.MsgIdx())
		}
	})
}

func TestRingMsg_MessageType_OutOfBounds_Panics(t *testing.T) {
	forEachConfig(t, func(t *testing.T, cfg RingTestConfig) {
		ring := cfg.NewRing()

		msg := &pgproto3.Sync{}
		conn := NewTestConnFromClient(msg)
		feedRingBuffer(t, ring, conn)

		cursor := NewClientCursor(ring)
		setupCursorAfterFeed(t, cursor)

		// Move past all messages
		for cursor.NextMsg() {
		}

		defer func() {
			if r := recover(); r == nil {
				t.Error("expected panic for out of bounds MessageType")
			}
		}()

		// This should panic - cursor is at end
		cursor.MessageType()
	})
}

func TestRingMsg_Retain_Independent(t *testing.T) {
	forEachConfig(t, func(t *testing.T, cfg RingTestConfig) {
		ring := cfg.NewRing()

		msg := &pgproto3.Query{String: "ORIGINAL"}
		conn := NewTestConnFromClient(msg)
		feedRingBuffer(t, ring, conn)

		cursor := NewClientCursor(ring)
		setupCursorAfterFeed(t, cursor)
		cursor.NextMsg()

		// Retain the message
		retained := cursor.Retain().(RawBody)
		originalBody := slices.Clone(retained.Body)

		// Verify retained is a separate copy - not sharing underlying array
		if &retained.Body[0] == &cursor.MessageBody()[0] {
			t.Error("retained body shares memory with cursor - should be independent")
		}

		// Retained should match the original
		if !bytes.Equal(retained.Body, originalBody) {
			t.Error("retained message body doesn't match original")
		}

		// Verify message type is correct
		if retained.Type != MsgClientQuery {
			t.Errorf("retained type = %c, want %c", retained.Type, MsgClientQuery)
		}
	})
}

// ============================================================================
// NewWithSameBuffers Tests
// ============================================================================

func TestRingBuffer_NewWithSameBuffers(t *testing.T) {
	forEachConfig(t, func(t *testing.T, cfg RingTestConfig) {
		ring1 := cfg.NewRing()

		// Use the ring
		msg := &pgproto3.Query{String: "TEST"}
		conn := NewTestConnFromClient(msg)
		feedRingBuffer(t, ring1, conn)

		// Create new ring with same buffers
		ring2 := ring1.NewWithSameBuffers()

		// Verify config is preserved
		if ring2.MessageBytes != ring1.MessageBytes {
			t.Errorf("MessageBytes = %d, want %d", ring2.MessageBytes, ring1.MessageBytes)
		}
		if ring2.MessageCount != ring1.MessageCount {
			t.Errorf("MessageCount = %d, want %d", ring2.MessageCount, ring1.MessageCount)
		}
		if ring2.HeadroomBytes != ring1.HeadroomBytes {
			t.Errorf("HeadroomBytes = %d, want %d", ring2.HeadroomBytes, ring1.HeadroomBytes)
		}

		// New ring should be usable
		msg2 := &pgproto3.Sync{}
		conn2 := NewTestConnFromClient(msg2)
		feedRingBuffer(t, ring2, conn2)

		results := readAllMessages(t, ring2, true)
		if len(results) != 1 {
			t.Fatalf("got %d messages, want 1", len(results))
		}
		assertMessageType(t, results[0], MsgClientSync)
	})
}
