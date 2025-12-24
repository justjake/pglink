package pgwire

import (
	"context"
	"encoding/binary"
	"errors"
	"io"
	"runtime"
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

	// contextCheckBytes is how often to check context cancellation (amortized)
	contextCheckBytes = 64 * 1024

	// spaceCheckMsgs is how often to refresh cached consumer positions
	spaceCheckMsgs = 32

	// readerSpinCount is how many times reader spins before blocking on channel
	readerSpinCount = 64
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
// wire protocol messages. The producer reads from a network connection directly
// into the ring buffer and parses message boundaries. The consumer iterates
// over complete messages and can write them directly to an output connection.
//
// Memory layout:
// - data[]byte: circular buffer for raw wire bytes (including headers)
// - offsets[]int64: byte offset in data where each message starts
//
// The ring stores exact wire bytes, enabling zero-copy proxying: messages that
// don't need modification can be written directly from the ring to the output.
//
// Cache-line padding separates variables with different access patterns to
// prevent false sharing. Variables with the same writer can share a cache line.
type RingBuffer struct {
	// === Immutable after construction ===
	data     []byte
	dataMask int64
	offsets  []int64
	metaMask int64

	// === Writer-local state (only writer touches, no synchronization) ===
	rawEnd      int64 // Where raw bytes from network end
	parsePos    int64 // Where we've parsed up to (looking for message boundaries)
	localMsgCnt int64 // Complete messages found (unpublished)

	// Cached reader positions (refreshed periodically to reduce atomic loads)
	cachedConsumedBytes int64
	cachedConsumedMsgs  int64

	// === Cache line boundary ===
	_ [16]byte // Pad writer-local (48 bytes above) to 64

	// === Published positions (writer stores, reader loads) ===
	// These share a cache line since they have the same access pattern
	publishedBytes int64    // Bytes available to reader (end of last complete message)
	publishedMsgs  int64    // Messages available to reader
	_              [48]byte // Pad to 64 bytes

	// === Consumed positions (reader stores, writer loads) ===
	// These share a cache line since they have the same access pattern
	consumedBytes int64    // Bytes consumed by reader (can be reclaimed)
	consumedMsgs  int64    // Messages consumed by reader
	_             [48]byte // Pad to 64 bytes

	// === Signaling (cold path) ===
	writerWake chan struct{} // Reader signals writer: space freed
	readerWake chan struct{} // Writer signals reader: data ready
	done       chan struct{} // Closed when buffer is done (error or EOF)
	err        atomic.Pointer[error]
}

// NewRingBuffer creates a new ring buffer with default sizes.
func NewRingBuffer() *RingBuffer {
	return NewRingBufferWithSize(defaultDataSize, defaultMetaSize)
}

// NewRingBufferWithSize creates a new ring buffer with custom sizes.
// dataSize must be a power of 2. metaSize must be a power of 2.
func NewRingBufferWithSize(dataSize, metaSize int) *RingBuffer {
	dataSize = nextPowerOf2(dataSize)
	metaSize = nextPowerOf2(metaSize)

	return &RingBuffer{
		data:       make([]byte, dataSize),
		dataMask:   int64(dataSize - 1),
		offsets:    make([]int64, metaSize),
		metaMask:   int64(metaSize - 1),
		writerWake: make(chan struct{}, 1),
		readerWake: make(chan struct{}, 1),
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

// === Writer methods ===

// ReadFrom reads PostgreSQL wire protocol messages from src until EOF or error.
// Bytes are read directly into the ring buffer, then parsed to find message
// boundaries. Complete messages are published in batches for the consumer.
func (r *RingBuffer) ReadFrom(ctx context.Context, src io.Reader) error {
	bytesUntilContextCheck := int64(contextCheckBytes)
	msgsUntilSpaceRefresh := spaceCheckMsgs

	for {
		// Calculate available contiguous space
		used := r.rawEnd - r.cachedConsumedBytes
		available := int64(len(r.data)) - used - minDataHeadroom

		if available <= 0 {
			// Refresh cached position and retry
			r.refreshCachedPositions()
			used = r.rawEnd - r.cachedConsumedBytes
			available = int64(len(r.data)) - used - minDataHeadroom

			if available <= 0 {
				// Actually need to wait for space
				if err := r.waitForSpace(ctx); err != nil {
					return err
				}
				r.refreshCachedPositions()
				continue
			}
		}

		// Get contiguous region (handle wraparound)
		writeOff := r.rawEnd & r.dataMask
		contiguous := int64(len(r.data)) - writeOff
		if contiguous > available {
			contiguous = available
		}

		// Read directly from network into ring buffer
		n, err := src.Read(r.data[writeOff : writeOff+contiguous])
		if n > 0 {
			r.rawEnd += int64(n)

			// Parse all complete messages from the new data
			r.parseCompleteMessages(&msgsUntilSpaceRefresh)

			// Publish if we have new complete messages
			if r.localMsgCnt > atomic.LoadInt64(&r.publishedMsgs) {
				r.publish()
			}

			// Amortized context check
			bytesUntilContextCheck -= int64(n)
			if bytesUntilContextCheck <= 0 {
				select {
				case <-ctx.Done():
					r.setError(ctx.Err())
					return ctx.Err()
				default:
				}
				bytesUntilContextCheck = contextCheckBytes
			}
		}

		if err != nil {
			if errors.Is(err, io.EOF) {
				r.setError(io.EOF)
				return nil
			}
			r.setError(err)
			return err
		}
	}
}

// parseCompleteMessages scans from parsePos to rawEnd finding complete messages.
func (r *RingBuffer) parseCompleteMessages(msgsUntilSpaceRefresh *int) {
	bufLen := int64(len(r.data))
	metaLen := int64(len(r.offsets))

	for {
		available := r.rawEnd - r.parsePos

		// Need at least 5 bytes for header
		if available < 5 {
			return
		}

		// Check metadata space
		usedMeta := r.localMsgCnt - r.cachedConsumedMsgs
		if usedMeta >= metaLen {
			// Refresh and check again
			r.refreshCachedPositions()
			usedMeta = r.localMsgCnt - r.cachedConsumedMsgs
			if usedMeta >= metaLen {
				return // No metadata space, stop parsing
			}
		}

		// Read header (handle wraparound)
		hdrOff := r.parsePos & r.dataMask
		var msgLen int64

		if hdrOff+5 <= bufLen {
			// Fast path: header doesn't wrap
			msgLen = int64(binary.BigEndian.Uint32(r.data[hdrOff+1:hdrOff+5])) + 1
		} else {
			// Slow path: header wraps around
			var hdr [5]byte
			for i := 0; i < 5; i++ {
				hdr[i] = r.data[(hdrOff+int64(i))&r.dataMask]
			}
			msgLen = int64(binary.BigEndian.Uint32(hdr[1:5])) + 1
		}

		// Validate message length
		if msgLen < 5 {
			r.setError(errors.New("invalid message length"))
			return
		}

		// Check if complete message is available
		if available < msgLen {
			return
		}

		// Check if message fits in buffer
		if msgLen > bufLen-minDataHeadroom {
			r.setError(ErrMessageTooLarge{MessageLen: msgLen, BufferCap: bufLen})
			return
		}

		// Record message metadata
		r.offsets[r.localMsgCnt&r.metaMask] = r.parsePos
		r.localMsgCnt++
		r.parsePos += msgLen

		// Periodic refresh of cached positions
		*msgsUntilSpaceRefresh--
		if *msgsUntilSpaceRefresh <= 0 {
			r.refreshCachedPositions()
			*msgsUntilSpaceRefresh = spaceCheckMsgs
		}
	}
}

func (r *RingBuffer) refreshCachedPositions() {
	r.cachedConsumedBytes = atomic.LoadInt64(&r.consumedBytes)
	r.cachedConsumedMsgs = atomic.LoadInt64(&r.consumedMsgs)
}

func (r *RingBuffer) publish() {
	// Order matters: bytes first, then message count (commit point)
	atomic.StoreInt64(&r.publishedBytes, r.parsePos)
	atomic.StoreInt64(&r.publishedMsgs, r.localMsgCnt)

	// Coalesced signal
	select {
	case r.readerWake <- struct{}{}:
	default:
	}
}

func (r *RingBuffer) waitForSpace(ctx context.Context) error {
	select {
	case <-ctx.Done():
		r.setError(ctx.Err())
		return ctx.Err()
	case <-r.writerWake:
		return nil
	case <-r.done:
		return r.Error()
	}
}

func (r *RingBuffer) setError(err error) {
	if r.err.CompareAndSwap(nil, &err) {
		close(r.done)
	}
}

// === Reader methods ===

// AvailableMessages waits for messages and returns the count of available messages.
// Spins briefly before falling back to channel wait for low latency.
func (r *RingBuffer) AvailableMessages(afterMsg int64) (toMsg int64, err error) {
	// Hot path: spin check
	for i := 0; i < readerSpinCount; i++ {
		toMsg = atomic.LoadInt64(&r.publishedMsgs)
		if toMsg > afterMsg {
			return toMsg, nil
		}
		select {
		case <-r.done:
			// Check for remaining messages before returning error
			toMsg = atomic.LoadInt64(&r.publishedMsgs)
			if toMsg > afterMsg {
				return toMsg, nil
			}
			return afterMsg, r.Error()
		default:
		}
		runtime.Gosched()
	}

	// Cold path: wait on channel
	for {
		select {
		case <-r.readerWake:
			toMsg = atomic.LoadInt64(&r.publishedMsgs)
			if toMsg > afterMsg {
				return toMsg, nil
			}
		case <-r.done:
			toMsg = atomic.LoadInt64(&r.publishedMsgs)
			if toMsg > afterMsg {
				return toMsg, nil
			}
			return afterMsg, r.Error()
		}
	}
}

// ReleaseThrough marks messages [0, throughMsg) as consumed, freeing space for writer.
func (r *RingBuffer) ReleaseThrough(throughMsg int64) {
	if throughMsg <= 0 {
		return
	}

	// Get the byte position of the next unconsumed message
	dataPos := r.offsets[throughMsg&r.metaMask]

	atomic.StoreInt64(&r.consumedMsgs, throughMsg)
	atomic.StoreInt64(&r.consumedBytes, dataPos)

	// Wake writer if waiting
	select {
	case r.writerWake <- struct{}{}:
	default:
	}
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

// PublishedMsgCount returns the total number of messages published.
func (r *RingBuffer) PublishedMsgCount() int64 {
	return atomic.LoadInt64(&r.publishedMsgs)
}

// === Message access methods ===

// MessageType returns the type byte for message at index msgIdx.
// Reads directly from the ring buffer data.
func (r *RingBuffer) MessageType(msgIdx int64) byte {
	off := r.offsets[msgIdx&r.metaMask]
	return r.data[off&r.dataMask]
}

// MessageOffset returns the byte offset where message msgIdx starts (including header).
func (r *RingBuffer) MessageOffset(msgIdx int64) int64 {
	return r.offsets[msgIdx&r.metaMask]
}

// MessageEnd returns the byte offset where message msgIdx ends (exclusive).
func (r *RingBuffer) MessageEnd(msgIdx int64) int64 {
	nextMsgIdx := msgIdx + 1
	published := atomic.LoadInt64(&r.publishedMsgs)
	if nextMsgIdx < published {
		return r.offsets[nextMsgIdx&r.metaMask]
	}
	return atomic.LoadInt64(&r.publishedBytes)
}

// MessageLen returns the total wire length of message msgIdx (type + length + body).
func (r *RingBuffer) MessageLen(msgIdx int64) int64 {
	return r.MessageEnd(msgIdx) - r.MessageOffset(msgIdx)
}

// MessageBody returns the body bytes for message at msgIdx (excluding 5-byte header).
// This may allocate if the message wraps around the ring buffer.
func (r *RingBuffer) MessageBody(msgIdx int64) []byte {
	start := r.MessageOffset(msgIdx) + 5 // Skip header
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

// === Batch write methods (for zero-copy proxying) ===

// WriteBatch writes messages [fromIdx, toIdx) directly to dst.
// This is the hot path for proxying unmodified messages.
func (r *RingBuffer) WriteBatch(fromIdx, toIdx int64, dst io.Writer) (int64, error) {
	if fromIdx >= toIdx {
		return 0, nil
	}

	start := r.MessageOffset(fromIdx)
	end := r.MessageEnd(toIdx - 1)

	return r.writeRange(start, end, dst)
}

// WriteMessage writes a single message to dst.
func (r *RingBuffer) WriteMessage(msgIdx int64, dst io.Writer) (int64, error) {
	start := r.MessageOffset(msgIdx)
	end := r.MessageEnd(msgIdx)
	return r.writeRange(start, end, dst)
}

func (r *RingBuffer) writeRange(start, end int64, dst io.Writer) (int64, error) {
	n := end - start
	if n <= 0 {
		return 0, nil
	}

	startOff := start & r.dataMask
	endOff := end & r.dataMask

	// Fast path: no wraparound - single write syscall
	if startOff < endOff || endOff == 0 {
		var data []byte
		if endOff == 0 {
			data = r.data[startOff:]
		} else {
			data = r.data[startOff:endOff]
		}
		written, err := dst.Write(data)
		return int64(written), err
	}

	// Slow path: wraparound - two writes
	n1, err := dst.Write(r.data[startOff:])
	if err != nil {
		return int64(n1), err
	}
	n2, err := dst.Write(r.data[:endOff])
	return int64(n1 + n2), err
}

// Close closes the ring buffer and wakes up any waiting goroutines.
func (r *RingBuffer) Close() {
	r.setError(io.EOF)
}
