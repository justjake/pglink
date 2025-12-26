package pgwire

import (
	"bytes"
	"fmt"
	"io"
	"slices"
)

// Cursor provides zero-allocation iteration over messages in a RingBuffer.
// It implements RawMessageSource for the current message, enabling lazy parsing.
//
// Single-cursor usage:
//
//	cursor := NewClientCursor(ring)
//	for {
//	    if err := cursor.NextBatch(); err != nil {
//	        return err
//	    }
//	    for cursor.NextMsg() {
//	        msg, _ := cursor.AsClient()
//	        // Process msg...
//	    }
//	    cursor.WriteAll(dst)
//	}
//
// Multi-cursor usage (e.g., proxying between frontend and backend):
//
//	frontend := NewClientCursor(frontendRing)
//	backend := NewServerCursor(backendRing)
//	for {
//	    // Check both sides without blocking
//	    gotFrontend, errF := frontend.TryNextBatch()
//	    gotBackend, errB := backend.TryNextBatch()
//	    if errF != nil { return errF }
//	    if errB != nil { return errB }
//
//	    // Process server responses first, then client requests
//	    if gotBackend {
//	        for backend.NextMsg() { /* forward to client */ }
//	    }
//	    if gotFrontend {
//	        for frontend.NextMsg() { /* forward to server */ }
//	    }
//
//	    // If nothing available, wait for either side
//	    if !gotFrontend && !gotBackend {
//	        select {
//	        case <-backend.Ready():
//	        case <-frontend.Ready():
//	        case <-backend.Done():
//	            return backend.Err()
//	        case <-frontend.Done():
//	            return frontend.Err()
//	        }
//	    }
//	}
type Cursor struct {
	ring *RingBuffer

	// Batch state
	RingRange
	RingMsg

	// Flyweights - only one set allocated based on direction
	clientFlyweights *ClientFlyweights // nil for server cursor
	serverFlyweights *ServerFlyweights // nil for client cursor
}

// NewClientCursor creates a cursor for iterating client messages.
func NewClientCursor(ring *RingBuffer) *Cursor {
	c := &Cursor{
		ring:             ring,
		clientFlyweights: &ClientFlyweights{},
	}
	c.RingRange.ring = ring
	c.capacity = &c.RingRange
	c.in = &c.RingRange
	return c
}

// NewServerCursor creates a cursor for iterating server messages.
func NewServerCursor(ring *RingBuffer) *Cursor {
	c := &Cursor{
		ring:             ring,
		serverFlyweights: &ServerFlyweights{},
	}
	c.RingRange.ring = ring
	c.capacity = &c.RingRange
	c.in = &c.RingRange
	return c
}

// TryNextBatch checks for new messages without blocking.
// Returns (true, nil) if messages are available for processing.
// Returns (false, nil) if no messages yet - use Ready() channel to wait.
// Returns (false, err) on EOF or error.
func (c *Cursor) TryNextBatch() (bool, error) {
	// Release previous batch (if any)
	if c.endIdx > 0 {
		c.ring.ReleaseThrough(c.endIdx)
	}

	// Check for error first
	if err := c.ring.Error(); err != nil {
		return false, err
	}

	// Non-blocking check for new messages
	newEnd := c.ring.PublishedMsgCount()
	if newEnd <= c.endIdx {
		return false, nil
	}

	c.startIdx = c.endIdx
	c.endIdx = newEnd
	c.msgIdx = c.startIdx - 1 // NextMsg will increment
	return true, nil
}

// Ready returns a channel that signals when new messages may be available.
// Use in select with other cursors for multiplexing.
func (c *Cursor) Ready() <-chan struct{} {
	return c.ring.ReaderWake()
}

// Done returns a channel that's closed when the cursor is done (EOF or error).
func (c *Cursor) Done() <-chan struct{} {
	return c.ring.Done()
}

// Err returns the error that caused the cursor to close, or nil.
func (c *Cursor) Err() error {
	return c.ring.Error()
}

// AsClient returns the current message as a ClientMessage using flyweights.
// The returned message is only valid until the next call to AsClient or NextMsg.
// Returns a pointer to a flyweight slot - zero allocation.
func (c *Cursor) AsClient() (ClientMessage, error) {
	if c.clientFlyweights == nil {
		panic("AsClient called on server cursor")
	}
	return c.clientFlyweights.Parse(c)
}

// AsServer returns the current message as a ServerMessage using flyweights.
// The returned message is only valid until the next call to AsServer or NextMsg.
// Returns a pointer to a flyweight slot - zero allocation.
func (c *Cursor) AsServer() (ServerMessage, error) {
	if c.serverFlyweights == nil {
		panic("AsServer called on client cursor")
	}
	return c.serverFlyweights.Parse(c)
}

func (c *Cursor) String() string {
	return fmt.Sprintf("Cursor{%s %s}", &c.RingRange, &c.RingMsg)
}

// Compile-time check that Cursor implements RawMessageSource
var _ RawMessageSource = (*Cursor)(nil)

// RingRange represents a range of messages in a RingBuffer.
// Range contains [start, end) (end is exclusive, and not in the range).
type RingRange struct {
	startIdx int64
	endIdx   int64
	capacity *RingRange
	ring     *RingBuffer
}

func (r *RingRange) Start() int64 {
	return r.startIdx
}

func (r *RingRange) SetStart(start int64) {
	if r.capacity.startIdx > start {
		panic(fmt.Sprintf("start out of bounds %s: %d", r.capacity, start))
	}
	if start > r.endIdx {
		panic(fmt.Sprintf("start after end %s: %d", r, start))
	}
	r.startIdx = start
}

func (r *RingRange) End() int64 {
	return r.endIdx
}

func (r *RingRange) SetEnd(end int64) {
	if r.capacity.endIdx < end {
		panic(fmt.Sprintf("end out of bounds %s: %d", r.capacity, end))
	}
	if end < r.startIdx {
		panic(fmt.Sprintf("end before start %s: %d", r, end))
	}
	r.endIdx = end
}

func (r *RingRange) SetEndInclusive(end int64) {
	r.SetEnd(end + 1)
}

// Range returns the start and end indices of the range.
// The range is [start, end) (end is exclusive, and not in the range).
func (r *RingRange) Range() (int64, int64) {
	return r.startIdx, r.endIdx
}

func (r *RingRange) Slice(start, end int64) *RingRange {
	if start < r.startIdx || end > r.endIdx {
		panic(fmt.Sprintf("slice out of bounds %s: [%d-%d)", r, start, end))
	}
	return &RingRange{
		startIdx: start,
		endIdx:   end,
		capacity: r.capacity,
		ring:     r.ring,
	}
}

func (r *RingRange) Ring() *RingBuffer {
	return r.ring
}

func (r *RingRange) Contains(start, end int64) bool {
	return start >= r.startIdx && end <= r.endIdx && start <= end
}

func (r *RingRange) Empty() bool {
	return r.startIdx >= r.endIdx
}

func (r *RingRange) Len() int64 {
	return r.endIdx - r.startIdx
}

func (r *RingRange) Bytes() int64 {
	return r.ring.MessageEnd(r.endIdx-1) - r.ring.MessageOffset(r.startIdx)
}

func (r *RingRange) String() string {
	return fmt.Sprintf("RingRange{[%d-%d) %d bytes ring=%p}", r.startIdx, r.endIdx, r.Bytes(), r.ring)
}

// NewReader returns an io.Reader that reads the raw wire bytes of all messages
// in this range.
//
// For streaming (oversized) messages, Read will block while reading remaining
// bytes directly from the network source.
//
// The reader is only valid while the underlying RingRange is valid. Do not call
// ReleaseThrough on messages that are still being read.
func (r *RingRange) NewReader() io.Reader {
	if r.Empty() {
		return bytes.NewReader(nil)
	}

	lastIdx := r.endIdx - 1

	// Check if last message is streaming (only last can be - reader pauses after detecting it)
	if r.ring.isStreaming(lastIdx) {
		streamingPart := &streamingCompleter{
			ring:   r.ring,
			reader: r.ring.streamingMessageReader(lastIdx),
		}

		if r.startIdx == lastIdx {
			// Only one message and it's streaming
			return streamingPart
		}

		// Multiple messages: buffered prefix + streaming last
		bufferedStart := r.ring.MessageOffset(r.startIdx)
		bufferedEnd := r.ring.MessageOffset(lastIdx)
		return io.MultiReader(
			r.ring.rangeReader(bufferedStart, bufferedEnd),
			streamingPart,
		)
	}

	// All buffered - single contiguous range
	start := r.ring.MessageOffset(r.startIdx)
	end := r.ring.MessageEnd(lastIdx)
	return r.ring.rangeReader(start, end)
}

// streamingCompleter wraps a reader and calls completeStreaming on EOF/error.
type streamingCompleter struct {
	ring   *RingBuffer
	reader io.Reader
	done   bool
}

func (s *streamingCompleter) Read(p []byte) (int, error) {
	n, err := s.reader.Read(p)
	if err != nil && !s.done {
		s.done = true
		if err == io.EOF {
			s.ring.completeStreaming(nil)
		} else {
			s.ring.completeStreaming(err)
		}
	}
	return n, err
}

// RingMsg represents a single message in a RingBuffer.
type RingMsg struct {
	msgIdx int64
	in     *RingRange
}

func (r *RingMsg) MsgIdx() int64 {
	return r.msgIdx
}

func (r *RingMsg) Range() *RingRange {
	return r.in
}

func (r *RingMsg) NextMsg() bool {
	r.msgIdx++
	return r.msgIdx < r.in.endIdx
}

func (r *RingMsg) PrevMsg() bool {
	r.msgIdx = max(r.msgIdx-1, r.in.startIdx-1)
	return r.msgIdx >= r.in.startIdx
}

func (r *RingMsg) MessageType() MsgType {
	r.panicUnlessValid()
	return r.in.ring.MessageType(r.msgIdx)
}

func (r *RingMsg) MessageBody() []byte {
	r.panicUnlessValid()
	return r.in.ring.MessageBody(r.msgIdx)
}

func (r *RingMsg) MessageLen() int64 {
	r.panicUnlessValid()
	return r.in.ring.MessageLen(r.msgIdx)
}

func (r *RingMsg) BodyLen() int {
	r.panicUnlessValid()
	return int(r.in.ring.MessageLen(r.msgIdx) - 5)
}

func (r *RingMsg) Retain() RawMessageSource {
	return RawBody{
		Type: r.MessageType(),
		Body: slices.Clone(r.MessageBody()),
	}
}

func (r *RingMsg) String() string {
	return fmt.Sprintf("RingMsg{idx=%d type=%c bytes=%d ring=%p}", r.msgIdx, r.MessageType(), r.MessageLen(), r.in.ring)
}

func (r *RingMsg) panicUnlessValid() {
	start, end := r.in.capacity.Range()
	if r.msgIdx < start || r.msgIdx >= end {
		panic(fmt.Sprintf("out of bounds %s: %s", r.in.capacity, r))
	}
}

// Compile-time check that RingMsgSource implements RawMessageSource
var _ RawMessageSource = (*RingMsg)(nil)
