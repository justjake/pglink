package pgwire

import (
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
	batchStart int64
	batchEnd   int64

	// Iteration state
	msgIdx   int64 // Current message index
	writePos int64 // Messages written so far in this batch

	// Flyweights - only one set allocated based on direction
	clientFlyweights *ClientFlyweights // nil for server cursor
	serverFlyweights *ServerFlyweights // nil for client cursor
}

// NewClientCursor creates a cursor for iterating client messages.
func NewClientCursor(ring *RingBuffer) *Cursor {
	return &Cursor{
		ring:             ring,
		clientFlyweights: &ClientFlyweights{},
	}
}

// NewServerCursor creates a cursor for iterating server messages.
func NewServerCursor(ring *RingBuffer) *Cursor {
	return &Cursor{
		ring:             ring,
		serverFlyweights: &ServerFlyweights{},
	}
}

// === RawMessageSource implementation ===

// MessageType returns the message type of the current message.
func (c *Cursor) MessageType() MsgType {
	return c.ring.MessageType(c.msgIdx)
}

// MessageBody returns the body bytes of the current message.
// This may allocate if the message wraps around the ring buffer.
func (c *Cursor) MessageBody() []byte {
	return c.ring.MessageBody(c.msgIdx)
}

// BodyLen returns the body length without copying.
func (c *Cursor) BodyLen() int {
	return int(c.ring.MessageLen(c.msgIdx) - 5)
}

// WriteTo writes the current message to dst.
// The ring buffer stores full wire bytes, so this is a direct write.
func (c *Cursor) WriteTo(w io.Writer) (int64, error) {
	return c.ring.WriteMessage(c.msgIdx, w)
}

// Retain returns an owned RawBody copy of the current message.
func (c *Cursor) Retain() RawMessageSource {
	return RawBody{
		Type: c.MessageType(),
		Body: slices.Clone(c.MessageBody()),
	}
}

// === Batch iteration ===

// TryNextBatch checks for new messages without blocking.
// Returns (true, nil) if messages are available for processing.
// Returns (false, nil) if no messages yet - use Ready() channel to wait.
// Returns (false, err) on EOF or error.
func (c *Cursor) TryNextBatch() (bool, error) {
	// Release previous batch (if any)
	if c.batchEnd > 0 {
		c.ring.ReleaseThrough(c.batchEnd)
	}

	// Check for error first
	if err := c.ring.Error(); err != nil {
		return false, err
	}

	// Non-blocking check for new messages
	newEnd := c.ring.PublishedMsgCount()
	if newEnd <= c.batchEnd {
		return false, nil
	}

	c.batchStart = c.batchEnd
	c.batchEnd = newEnd
	c.msgIdx = c.batchStart - 1 // NextMsg will increment
	c.writePos = c.batchStart
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

// NextMsg advances to the next message in the batch.
// Returns false when the batch is exhausted.
func (c *Cursor) NextMsg() bool {
	c.msgIdx++
	return c.msgIdx < c.batchEnd
}

// MsgIdx returns the current message index.
func (c *Cursor) MsgIdx() int64 {
	return c.msgIdx
}

// FirstMsgIdx returns the index of the first unwritten message in this batch.
func (c *Cursor) FirstMsgIdx() int64 {
	return c.writePos
}

// === Writing ===

// Write writes messages [fromMsg, toMsg) to dst.
// The ring buffer stores full wire bytes, so this writes directly without
// reconstructing headers.
func (c *Cursor) Write(fromMsg, toMsg int64, dst io.Writer) error {
	if fromMsg >= toMsg {
		return nil
	}

	_, err := c.ring.WriteBatch(fromMsg, toMsg, dst)
	if err != nil {
		return err
	}

	c.writePos = toMsg
	return nil
}

// WriteThrough writes all messages up to and including the current message.
func (c *Cursor) WriteThrough(dst io.Writer) error {
	return c.Write(c.writePos, c.msgIdx+1, dst)
}

// WriteAll writes all remaining messages in the batch.
func (c *Cursor) WriteAll(dst io.Writer) error {
	return c.Write(c.writePos, c.batchEnd, dst)
}

// SkipThrough marks messages up to and including current as written (without writing).
func (c *Cursor) SkipThrough() {
	c.writePos = c.msgIdx + 1
}

// === Typed message access (flyweight pattern) ===

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

// Compile-time check that Cursor implements RawMessageSource
var _ RawMessageSource = (*Cursor)(nil)
