package pgwire

import (
	"context"
	"encoding/binary"
	"errors"
	"io"
	"sync/atomic"
)

// Ring buffer constants
const (
	// minDataHeadroom ensures we can always fit a message that has already started
	// being parsed. Without this, we could deadlock: parser waits for complete message,
	// writer can't read more because buffer is full.
	minDataHeadroom = 8192

	// defaultDataSize is the default ring buffer data capacity
	defaultDataSize = 256 * 1024 // 256KB

	// defaultMetaSize is the default number of message metadata slots
	defaultMetaSize = 4096
)

// ErrMessageTooLarge is returned when a message exceeds the buffer capacity
type ErrMessageTooLarge struct {
	MessageLen int64
	BufferCap  int64
}

func (e ErrMessageTooLarge) Error() string {
	return "message too large for ring buffer"
}

// RingBuffer is a single-producer single-consumer ring buffer for PostgreSQL
// wire protocol messages. The producer reads from a network connection and
// parses message boundaries. The consumer iterates over complete messages.
//
// Memory layout:
// - data[]byte: circular buffer for raw message bytes
// - types[]byte: message type byte for each message
// - offsets[]int64: byte offset in data where each message starts
//
// Cache-line padding is used to prevent false sharing between producer and
// consumer positions.
type RingBuffer struct {
	// Message data buffer (circular)
	data     []byte
	dataMask int64

	// Message metadata (circular)
	types    []byte
	offsets  []int64
	metaMask int64

	// === Cache-line padded atomic positions ===
	// Writer updates these, reader reads them
	_            [64]byte
	dataWritePos int64 // Next byte to write in data buffer
	_            [64 - 8]byte
	msgWritePos int64 // Next message slot to write
	_           [64 - 8]byte

	// Reader updates these, writer reads them
	dataReadPos int64 // Bytes consumed by reader (can be reclaimed)
	_           [64 - 8]byte
	msgReadPos int64 // Messages consumed by reader
	_          [64 - 8]byte

	// Signaling channels (buffered(1) for coalescing)
	spaceReady chan struct{} // Reader signals writer when space freed
	dataReady  chan struct{} // Writer signals reader when new messages available
	done       chan struct{} // Closed when buffer is done (error or EOF)

	// Error state (written once by writer, read by reader)
	err atomic.Pointer[error]
}

// NewRingBuffer creates a new ring buffer with default sizes.
func NewRingBuffer() *RingBuffer {
	return NewRingBufferWithSize(defaultDataSize, defaultMetaSize)
}

// NewRingBufferWithSize creates a new ring buffer with custom sizes.
// dataSize must be a power of 2. metaSize must be a power of 2.
func NewRingBufferWithSize(dataSize, metaSize int) *RingBuffer {
	// Round up to power of 2
	dataSize = nextPowerOf2(dataSize)
	metaSize = nextPowerOf2(metaSize)

	return &RingBuffer{
		data:       make([]byte, dataSize),
		dataMask:   int64(dataSize - 1),
		types:      make([]byte, metaSize),
		offsets:    make([]int64, metaSize),
		metaMask:   int64(metaSize - 1),
		spaceReady: make(chan struct{}, 1),
		dataReady:  make(chan struct{}, 1),
		done:       make(chan struct{}),
	}
}

func nextPowerOf2(n int) int {
	n--
	n |= n >> 1
	n |= n >> 2
	n |= n >> 4
	n |= n >> 8
	n |= n >> 16
	n |= n >> 32
	n++
	return n
}

// signal sends a non-blocking signal on the channel.
// Multiple signals coalesce since channel is buffered(1).
func (r *RingBuffer) signal(ch chan struct{}) {
	select {
	case ch <- struct{}{}:
	default:
	}
}

// === Writer methods (called from reader goroutine) ===

// ReadFrom reads PostgreSQL wire protocol messages from src until EOF or error.
// Messages are parsed and stored in the ring buffer for consumption.
// This method blocks until the source is exhausted or context is cancelled.
func (r *RingBuffer) ReadFrom(ctx context.Context, src io.Reader) error {
	var header [5]byte

	for {
		// Check context
		select {
		case <-ctx.Done():
			r.setError(ctx.Err())
			return ctx.Err()
		default:
		}

		// Read message header (type + length)
		_, err := io.ReadFull(src, header[:])
		if err != nil {
			if errors.Is(err, io.EOF) {
				r.setError(io.EOF)
				return nil
			}
			r.setError(err)
			return err
		}

		msgType := header[0]
		msgLen := int64(binary.BigEndian.Uint32(header[1:5])) - 4 // Length includes itself

		if msgLen < 0 {
			err := errors.New("invalid message length")
			r.setError(err)
			return err
		}

		// Check if message fits in buffer (with headroom)
		if msgLen > int64(len(r.data))-minDataHeadroom {
			err := ErrMessageTooLarge{MessageLen: msgLen + 5, BufferCap: int64(len(r.data))}
			r.setError(err)
			return err
		}

		// Wait for space in data buffer
		if err := r.waitForDataSpace(ctx, msgLen); err != nil {
			return err
		}

		// Wait for space in metadata buffer
		if err := r.waitForMetaSpace(ctx); err != nil {
			return err
		}

		// Record message start position and type
		dataPos := atomic.LoadInt64(&r.dataWritePos)
		msgPos := atomic.LoadInt64(&r.msgWritePos)

		r.types[msgPos&r.metaMask] = msgType
		r.offsets[msgPos&r.metaMask] = dataPos

		// Read message body into ring buffer
		if err := r.readIntoRing(src, msgLen); err != nil {
			r.setError(err)
			return err
		}

		// Update write positions (atomic, visible to reader)
		atomic.AddInt64(&r.dataWritePos, msgLen)
		atomic.AddInt64(&r.msgWritePos, 1)

		// Signal reader that new data is available
		r.signal(r.dataReady)
	}
}

// waitForDataSpace waits until there's enough space for msgLen bytes.
func (r *RingBuffer) waitForDataSpace(ctx context.Context, msgLen int64) error {
	for {
		dataWrite := atomic.LoadInt64(&r.dataWritePos)
		dataRead := atomic.LoadInt64(&r.dataReadPos)
		available := int64(len(r.data)) - (dataWrite - dataRead) - minDataHeadroom

		if available >= msgLen {
			return nil
		}

		// Wait for reader to free space
		select {
		case <-ctx.Done():
			r.setError(ctx.Err())
			return ctx.Err()
		case <-r.spaceReady:
			// Retry
		}
	}
}

// waitForMetaSpace waits until there's a metadata slot available.
func (r *RingBuffer) waitForMetaSpace(ctx context.Context) error {
	for {
		msgWrite := atomic.LoadInt64(&r.msgWritePos)
		msgRead := atomic.LoadInt64(&r.msgReadPos)
		available := int64(len(r.types)) - (msgWrite - msgRead)

		if available > 0 {
			return nil
		}

		// Wait for reader to free space
		select {
		case <-ctx.Done():
			r.setError(ctx.Err())
			return ctx.Err()
		case <-r.spaceReady:
			// Retry
		}
	}
}

// readIntoRing reads exactly n bytes from src into the ring buffer at current write position.
// Handles wraparound.
func (r *RingBuffer) readIntoRing(src io.Reader, n int64) error {
	dataPos := atomic.LoadInt64(&r.dataWritePos)
	bufLen := int64(len(r.data))

	remaining := n
	for remaining > 0 {
		offset := dataPos & r.dataMask
		// How much can we write before wraparound?
		chunk := bufLen - offset
		if chunk > remaining {
			chunk = remaining
		}

		_, err := io.ReadFull(src, r.data[offset:offset+chunk])
		if err != nil {
			return err
		}

		dataPos += chunk
		remaining -= chunk
	}

	return nil
}

// setError sets the error and closes the done channel.
func (r *RingBuffer) setError(err error) {
	if r.err.CompareAndSwap(nil, &err) {
		close(r.done)
	}
}

// === Reader methods (called from cursor) ===

// DataReady returns a channel that signals when new messages are available.
func (r *RingBuffer) DataReady() <-chan struct{} {
	return r.dataReady
}

// Done returns a channel that's closed when the buffer is done (EOF or error).
func (r *RingBuffer) Done() <-chan struct{} {
	return r.done
}

// Error returns the error that caused the buffer to close, or nil.
func (r *RingBuffer) Error() error {
	errPtr := r.err.Load()
	if errPtr == nil {
		return nil
	}
	return *errPtr
}

// MsgWriteCount returns the total number of messages written.
func (r *RingBuffer) MsgWriteCount() int64 {
	return atomic.LoadInt64(&r.msgWritePos)
}

// MsgReadCount returns the total number of messages consumed.
func (r *RingBuffer) MsgReadCount() int64 {
	return atomic.LoadInt64(&r.msgReadPos)
}

// MessageType returns the type byte for message at index msgIdx.
func (r *RingBuffer) MessageType(msgIdx int64) byte {
	return r.types[msgIdx&r.metaMask]
}

// MessageOffset returns the byte offset where message msgIdx starts.
func (r *RingBuffer) MessageOffset(msgIdx int64) int64 {
	return r.offsets[msgIdx&r.metaMask]
}

// MessageEnd returns the byte offset where message msgIdx ends (exclusive).
func (r *RingBuffer) MessageEnd(msgIdx int64) int64 {
	// End of this message is start of next message, or current write position
	nextMsgIdx := msgIdx + 1
	msgWrite := atomic.LoadInt64(&r.msgWritePos)
	if nextMsgIdx < msgWrite {
		return r.offsets[nextMsgIdx&r.metaMask]
	}
	return atomic.LoadInt64(&r.dataWritePos)
}

// MessageBody returns the body bytes for message at msgIdx.
// This may allocate if the message wraps around the ring buffer.
func (r *RingBuffer) MessageBody(msgIdx int64) []byte {
	start := r.MessageOffset(msgIdx)
	end := r.MessageEnd(msgIdx)
	return r.readRange(start, end)
}

// readRange reads bytes from start to end (exclusive), handling wraparound.
func (r *RingBuffer) readRange(start, end int64) []byte {
	n := end - start
	if n <= 0 {
		return nil
	}

	startOff := start & r.dataMask
	endOff := end & r.dataMask

	// Fast path: no wraparound
	if startOff < endOff || endOff == 0 {
		if endOff == 0 {
			return r.data[startOff:]
		}
		return r.data[startOff:endOff]
	}

	// Slow path: message wraps around, need to copy
	result := make([]byte, n)
	firstChunk := int64(len(r.data)) - startOff
	copy(result[:firstChunk], r.data[startOff:])
	copy(result[firstChunk:], r.data[:endOff])
	return result
}

// WriteRange writes bytes from start to end (exclusive) to dst, handling wraparound.
func (r *RingBuffer) WriteRange(start, end int64, dst io.Writer) error {
	n := end - start
	if n <= 0 {
		return nil
	}

	startOff := start & r.dataMask
	endOff := end & r.dataMask

	// Fast path: no wraparound
	if startOff < endOff || endOff == 0 {
		var data []byte
		if endOff == 0 {
			data = r.data[startOff:]
		} else {
			data = r.data[startOff:endOff]
		}
		_, err := dst.Write(data)
		return err
	}

	// Slow path: wraparound
	if _, err := dst.Write(r.data[startOff:]); err != nil {
		return err
	}
	if _, err := dst.Write(r.data[:endOff]); err != nil {
		return err
	}
	return nil
}

// AdvanceReader updates the read position, freeing space for the writer.
// Called when the reader has finished processing messages up to (but not including) msgIdx.
func (r *RingBuffer) AdvanceReader(msgIdx int64) {
	// Update message read position
	atomic.StoreInt64(&r.msgReadPos, msgIdx)

	// Update data read position to free space
	if msgIdx > 0 {
		// Data up to the start of msgIdx is now free
		dataPos := r.MessageOffset(msgIdx)
		atomic.StoreInt64(&r.dataReadPos, dataPos)
	}

	// Signal writer that space is available
	r.signal(r.spaceReady)
}

// Close closes the ring buffer and wakes up any waiting goroutines.
func (r *RingBuffer) Close() {
	r.setError(io.EOF)
}
