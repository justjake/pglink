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

	// spaceCheckMsgs is how often to refresh cached consumer positions
	spaceCheckMsgs = 32
)

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
// For messages too large to buffer, the ring buffer supports streaming: the
// header and any buffered portion are written from the ring, then the remaining
// bytes are streamed directly from the source to the destination. This is
// transparent to the consumer - WriteBatch handles both cases internally.
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

	// === Streaming state (for oversized messages) ===
	// When a message is too large to buffer, we record it as "streaming".
	// The reader goroutine blocks until the consumer streams the message through.
	streaming struct {
		active     bool  // True if current message is streaming
		msgIdx     int64 // Which message is streaming
		totalLen   int64 // Total wire length of the message
		bodyInRing int64 // How many body bytes are in the ring buffer
	}
	streamReq  chan streamRequest // Consumer sends request to stream remaining bytes
	streamDone chan error         // Reader goroutine signals completion

	// === Signaling (cold path) ===
	writerWake chan struct{} // Reader signals writer: space freed
	readerWake chan struct{} // Writer signals reader: data ready
	done       chan struct{} // Closed when buffer is done (error or EOF)
	err        atomic.Pointer[error]
}

// streamRequest is sent from consumer to reader goroutine to stream remaining bytes.
type streamRequest struct {
	dst       io.Writer
	remaining int64
}

// NewRingBuffer creates a new ring buffer with default sizes.
func NewRingBuffer() *RingBuffer {
	return NewRingBufferWithSize(defaultDataSize, defaultMetaSize)
}

// NewRingBufferWithSize creates a new ring buffer with custom sizes.
// dataSize and metaSize must be powers of 2; panics otherwise.
func NewRingBufferWithSize(dataSize, metaSize int) *RingBuffer {
	if dataSize <= 0 || dataSize&(dataSize-1) != 0 {
		panic("dataSize must be a power of 2")
	}
	if metaSize <= 0 || metaSize&(metaSize-1) != 0 {
		panic("metaSize must be a power of 2")
	}

	return &RingBuffer{
		data:       make([]byte, dataSize),
		dataMask:   int64(dataSize - 1),
		offsets:    make([]int64, metaSize),
		metaMask:   int64(metaSize - 1),
		streamReq:  make(chan streamRequest),
		streamDone: make(chan error),
		writerWake: make(chan struct{}, 1),
		readerWake: make(chan struct{}, 1),
		done:       make(chan struct{}),
	}
}

// maxBufferableSize returns the maximum message size that can be fully buffered.
func (r *RingBuffer) maxBufferableSize() int64 {
	return int64(len(r.data)) - minDataHeadroom
}

// === Writer methods ===

// ReadFrom reads PostgreSQL wire protocol messages from src until EOF or error.
// Bytes are read directly into the ring buffer, then parsed to find message
// boundaries. Complete messages are published in batches for the consumer.
// For oversized messages, coordinates with the consumer to stream them through.
func (r *RingBuffer) ReadFrom(ctx context.Context, src io.Reader) error {
	msgsUntilSpaceRefresh := spaceCheckMsgs

	for {
		// Check for cancellation
		select {
		case <-ctx.Done():
			r.setError(ctx.Err())
			return ctx.Err()
		default:
		}

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
			streamingDetected := r.parseCompleteMessages(&msgsUntilSpaceRefresh)

			// Publish if we have new complete messages
			if r.localMsgCnt > atomic.LoadInt64(&r.publishedMsgs) {
				r.publish()
			}

			// If we detected a streaming message, wait for consumer to handle it
			if streamingDetected {
				if err := r.handleStreamingMessage(ctx, src); err != nil {
					return err
				}
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
// Returns true if a streaming (oversized) message was detected.
func (r *RingBuffer) parseCompleteMessages(msgsUntilSpaceRefresh *int) bool {
	bufLen := int64(len(r.data))
	metaLen := int64(len(r.offsets))
	maxSize := r.maxBufferableSize()

	for {
		available := r.rawEnd - r.parsePos

		// Need at least 5 bytes for header
		if available < 5 {
			return false
		}

		// Check metadata space
		usedMeta := r.localMsgCnt - r.cachedConsumedMsgs
		if usedMeta >= metaLen {
			// Refresh and check again
			r.refreshCachedPositions()
			usedMeta = r.localMsgCnt - r.cachedConsumedMsgs
			if usedMeta >= metaLen {
				return false // No metadata space, stop parsing
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
			return false
		}

		// Check if message is too large to buffer
		if msgLen > maxSize {
			// Set up streaming for this message
			bodyInRing := available - 5 // How much body we already have
			if bodyInRing < 0 {
				bodyInRing = 0
			}

			r.streaming.active = true
			r.streaming.msgIdx = r.localMsgCnt
			r.streaming.totalLen = msgLen
			r.streaming.bodyInRing = bodyInRing

			// Record message metadata (offset points to header start)
			r.offsets[r.localMsgCnt&r.metaMask] = r.parsePos
			r.localMsgCnt++

			// Don't advance parsePos - streaming will handle the rest
			return true
		}

		// Check if complete message is available
		if available < msgLen {
			return false
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

// handleStreamingMessage waits for the consumer to request streaming,
// then streams the remaining bytes directly from src to the consumer's dst.
func (r *RingBuffer) handleStreamingMessage(ctx context.Context, src io.Reader) error {
	select {
	case <-ctx.Done():
		r.setError(ctx.Err())
		return ctx.Err()
	case req := <-r.streamReq:
		// Stream remaining bytes directly from src to dst
		_, err := io.CopyN(req.dst, src, req.remaining)
		r.streamDone <- err

		if err != nil {
			r.setError(err)
			return err
		}

		// Reset streaming state and advance parsePos
		r.parsePos += r.streaming.totalLen
		r.rawEnd = r.parsePos // Discard any buffered data
		r.streaming.active = false

		return nil
	case <-r.done:
		return r.Error()
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
func (r *RingBuffer) AvailableMessages(afterMsg int64) (toMsg int64, err error) {
	// Fast path: check if messages already available
	toMsg = atomic.LoadInt64(&r.publishedMsgs)
	if toMsg > afterMsg {
		return toMsg, nil
	}

	// Slow path: wait on channel
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

// ReaderWake returns a channel that signals when new messages may be available.
// Use with PublishedMsgCount() for non-blocking multi-buffer select loops.
func (r *RingBuffer) ReaderWake() <-chan struct{} {
	return r.readerWake
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

// isStreaming returns true if msgIdx is a streaming (oversized) message.
func (r *RingBuffer) isStreaming(msgIdx int64) bool {
	return r.streaming.active && r.streaming.msgIdx == msgIdx
}

// MessageType returns the message type for message at index msgIdx.
// Reads directly from the ring buffer data.
func (r *RingBuffer) MessageType(msgIdx int64) MsgType {
	off := r.offsets[msgIdx&r.metaMask]
	return MsgType(r.data[off&r.dataMask])
}

// MessageOffset returns the byte offset where message msgIdx starts (including header).
func (r *RingBuffer) MessageOffset(msgIdx int64) int64 {
	return r.offsets[msgIdx&r.metaMask]
}

// MessageEnd returns the byte offset where message msgIdx ends (exclusive).
// For streaming messages, returns the end based on the total message length.
func (r *RingBuffer) MessageEnd(msgIdx int64) int64 {
	if r.isStreaming(msgIdx) {
		return r.MessageOffset(msgIdx) + r.streaming.totalLen
	}
	nextMsgIdx := msgIdx + 1
	published := atomic.LoadInt64(&r.publishedMsgs)
	if nextMsgIdx < published {
		return r.offsets[nextMsgIdx&r.metaMask]
	}
	return atomic.LoadInt64(&r.publishedBytes)
}

// MessageLen returns the total wire length of message msgIdx (type + length + body).
func (r *RingBuffer) MessageLen(msgIdx int64) int64 {
	if r.isStreaming(msgIdx) {
		return r.streaming.totalLen
	}
	return r.MessageEnd(msgIdx) - r.MessageOffset(msgIdx)
}

// MessageBody returns the body bytes for message at msgIdx (excluding 5-byte header).
// Panics if the message is streaming (too large to buffer).
// This may allocate if the message wraps around the ring buffer.
func (r *RingBuffer) MessageBody(msgIdx int64) []byte {
	if r.isStreaming(msgIdx) {
		panic("MessageBody called on streaming message - message too large to buffer")
	}
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
// Handles streaming messages transparently - if the last message in the batch
// is streaming, coordinates with the reader goroutine to stream it through.
func (r *RingBuffer) WriteBatch(fromIdx, toIdx int64, dst io.Writer) (int64, error) {
	if fromIdx >= toIdx {
		return 0, nil
	}

	lastIdx := toIdx - 1

	// Check if last message is streaming
	if r.isStreaming(lastIdx) {
		var written int64

		// Write all fully-buffered messages first [fromIdx, lastIdx)
		if lastIdx > fromIdx {
			start := r.MessageOffset(fromIdx)
			end := r.MessageOffset(lastIdx)
			n, err := r.writeRange(start, end, dst)
			written += n
			if err != nil {
				return written, err
			}
		}

		// Stream the last message
		n, err := r.writeStreamingMessage(lastIdx, dst)
		return written + n, err
	}

	// All buffered - simple batch write
	start := r.MessageOffset(fromIdx)
	end := r.MessageEnd(lastIdx)
	return r.writeRange(start, end, dst)
}

// writeStreamingMessage writes a streaming message to dst.
// Writes header + buffered body portion, then coordinates with reader goroutine
// to stream the remaining bytes directly from the source.
func (r *RingBuffer) writeStreamingMessage(msgIdx int64, dst io.Writer) (int64, error) {
	if !r.isStreaming(msgIdx) {
		// Not streaming, use normal write
		return r.WriteMessage(msgIdx, dst)
	}

	var written int64
	offset := r.MessageOffset(msgIdx)

	// Write header (5 bytes)
	hdrOff := offset & r.dataMask
	if hdrOff+5 <= int64(len(r.data)) {
		n, err := dst.Write(r.data[hdrOff : hdrOff+5])
		written += int64(n)
		if err != nil {
			return written, err
		}
	} else {
		// Header wraps around
		var hdr [5]byte
		for i := 0; i < 5; i++ {
			hdr[i] = r.data[(hdrOff+int64(i))&r.dataMask]
		}
		n, err := dst.Write(hdr[:])
		written += int64(n)
		if err != nil {
			return written, err
		}
	}

	// Write buffered body portion
	bodyStart := offset + 5
	bodyBuffered := r.streaming.bodyInRing
	if bodyBuffered > 0 {
		n, err := r.writeRange(bodyStart, bodyStart+bodyBuffered, dst)
		written += n
		if err != nil {
			return written, err
		}
	}

	// Calculate remaining bytes to stream
	totalBodyLen := r.streaming.totalLen - 5
	remaining := totalBodyLen - bodyBuffered

	if remaining > 0 {
		// Send request to reader goroutine to stream remaining bytes
		r.streamReq <- streamRequest{dst: dst, remaining: remaining}

		// Wait for completion
		if err := <-r.streamDone; err != nil {
			return written, err
		}
		written += remaining
	}

	return written, nil
}

// WriteMessage writes a single message to dst.
func (r *RingBuffer) WriteMessage(msgIdx int64, dst io.Writer) (int64, error) {
	if r.isStreaming(msgIdx) {
		return r.writeStreamingMessage(msgIdx, dst)
	}
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
