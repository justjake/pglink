package pgwire

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"slices"
	"sync/atomic"
	"time"
)

// Ring buffer constants
const (
	// DefaultHeadroomBytes ensures we can always fit a message that has already started
	// being parsed. Without this, we could deadlock: parser waits for complete message,
	// writer can't read more because buffer is full.
	DefaultHeadroomBytes = 8192

	// DefaultMessageBytes is the default ring buffer data capacity
	DefaultMessageBytes = 16 * 1024 // 16KiB

	// DefaultMessageCount is the default number of message metadata slots
	DefaultMessageCount = 4096

	// spaceCheckMsgs is how often to refresh cached consumer positions
	spaceCheckMsgs = 32
)

// RingBufferConfig configures a ring buffer's capacity.
type RingBufferConfig struct {
	// MessageBytes is the total byte capacity for message data.
	// Must be a power of 2. Defaults to DefaultMessageBytes (16KiB).
	MessageBytes int64

	// MessageCount is the maximum number of messages that can be tracked.
	// Must be a power of 2. Defaults to DefaultMessageCount (4096).
	MessageCount int64

	// HeadroomBytes is reserved space to prevent deadlock when a message
	// header has been read but the body hasn't arrived yet.
	// Defaults to DefaultHeadroomBytes (8KB).
	HeadroomBytes int64
}

// DefaultRingBufferConfig returns a config with default buffer sizes.
func DefaultRingBufferConfig() RingBufferConfig {
	return RingBufferConfig{
		MessageBytes:  DefaultMessageBytes,
		MessageCount:  DefaultMessageCount,
		HeadroomBytes: 0,
	}
}

// RingBufferConfigForSize returns a config for the given buffer size in bytes.
// Uses the default message count. If messageBytes is 0, returns the default config.
func RingBufferConfigForSize(messageBytes int64) RingBufferConfig {
	if messageBytes <= 0 {
		return DefaultRingBufferConfig()
	}
	return RingBufferConfig{
		MessageBytes:  messageBytes,
		MessageCount:  DefaultMessageCount,
		HeadroomBytes: 0,
	}
}

// ringBuffers holds the allocated buffer memory that can be reused.
type ringBuffers struct {
	data     []byte
	dataMask int64
	offsets  []int64
	metaMask int64
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
// For messages too large to buffer, the ring buffer supports streaming: the
// header and any buffered portion are written from the ring, then the remaining
// bytes are streamed directly from the source to the destination.
//
// Cache-line padding separates variables with different access patterns to
// prevent false sharing. Variables with the same writer can share a cache line.
type RingBuffer struct {
	// === Immutable after construction ===
	RingBufferConfig
	ringBuffers

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
	// The reader goroutine pauses until the consumer finishes reading the
	// streaming message directly from the network connection.
	streaming struct {
		active     bool  // True if current message is streaming
		msgIdx     int64 // Which message is streaming
		totalLen   int64 // Total wire length of the message
		bodyInRing int64 // How many body bytes are in the ring buffer
	}
	streamDone chan error // Consumer signals when streaming is complete

	// === Signaling (cold path) ===
	writerWake chan struct{} // Reader signals writer: space freed
	readerWake chan struct{} // Writer signals reader: data ready
	done       chan struct{} // Closed when buffer is done (error or EOF)
	err        atomic.Pointer[error]
	// Set by StartNetConnReader
	conn       net.Conn
	readerDone chan struct{}
}

func (r *RingBuffer) initChannels() {
	r.streamDone = make(chan error)
	r.writerWake = make(chan struct{}, 1)
	r.readerWake = make(chan struct{}, 1)
	r.done = make(chan struct{})
}

// NewRingBuffer creates a new ring buffer with the given configuration.
// MessageBytes and MessageCount must be powers of 2; panics otherwise.
// Zero values are replaced with defaults.
func NewRingBuffer(cfg RingBufferConfig) *RingBuffer {
	// Apply defaults
	if cfg.MessageBytes == 0 {
		cfg.MessageBytes = DefaultMessageBytes
	}
	if cfg.MessageCount == 0 {
		cfg.MessageCount = DefaultMessageCount
	}
	if cfg.HeadroomBytes == 0 {
		cfg.HeadroomBytes = DefaultHeadroomBytes
	}

	// Validate
	if cfg.MessageBytes <= 0 || cfg.MessageBytes&(cfg.MessageBytes-1) != 0 {
		panic("MessageBytes must be a power of 2")
	}
	if cfg.MessageCount <= 0 || cfg.MessageCount&(cfg.MessageCount-1) != 0 {
		panic("MessageCount must be a power of 2")
	}

	r := &RingBuffer{
		RingBufferConfig: cfg,
		ringBuffers: ringBuffers{
			data:     make([]byte, cfg.MessageBytes),
			dataMask: cfg.MessageBytes - 1,
			offsets:  make([]int64, cfg.MessageCount),
			metaMask: cfg.MessageCount - 1,
		},
	}
	r.initChannels()
	return r
}

// maxBufferableSize returns the maximum message size that can be fully buffered.
func (r *RingBuffer) maxBufferableSize() int64 {
	return int64(len(r.data)) - r.HeadroomBytes
}

// === Writer methods ===

func (r *RingBuffer) StartNetConnReader(ctx context.Context, src net.Conn) {
	if r.conn != nil {
		panic("RingBuffer.StartNetConnReader called more than once")
	}
	r.conn = src
	r.readerDone = make(chan struct{})
	go func() {
		defer close(r.readerDone)
		// ReadFrom stores any error via setError(), making it available via Error() and Done().
		// The return value is the same error, so we don't need to handle it here.
		r.ReadFrom(ctx, src) //nolint:errcheck
	}()
}

func (r *RingBuffer) StopNetConnReader() error {
	if r.conn == nil {
		return nil
	}
	// Set deadline in the past to interrupt any blocking Read()
	if err := r.conn.SetDeadline(time.Now().Add(-time.Second)); err != nil {
		// Connection might already be closed; wait briefly then give up
		select {
		case <-r.readerDone:
		case <-time.After(100 * time.Millisecond):
			return fmt.Errorf("SetDeadline failed and reader didn't stop: %w", err)
		}
	} else {
		<-r.readerDone
	}
	// Clear deadline so connection can be reused
	if err := r.conn.SetDeadline(time.Time{}); err != nil {
		// Connection is likely unusable, but we still cleaned up the reader
		r.conn = nil
		r.readerDone = nil
		return fmt.Errorf("failed to clear deadline: %w", err)
	}
	r.conn = nil
	r.readerDone = nil
	return nil
}

func (r *RingBuffer) Running() bool {
	return r.conn != nil
}

func (r *RingBuffer) String() string {
	published := atomic.LoadInt64(&r.publishedMsgs)
	return fmt.Sprintf("RingBuffer{%dKB/%d msgs=%d %p}",
		r.MessageBytes/1024, r.MessageCount, published, r)
}

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
		available := int64(len(r.data)) - used - r.HeadroomBytes

		if available <= 0 {
			// Refresh cached position and retry
			r.refreshCachedPositions()
			used = r.rawEnd - r.cachedConsumedBytes
			available = int64(len(r.data)) - used - r.HeadroomBytes

			if available <= 0 {
				// Actually need to wait for space
				if err := r.waitForSpace(ctx); err != nil {
					return err
				}
				r.refreshCachedPositions()
				continue
			}
		}

		// Before reading more data, check if we have unparsed data in the buffer
		// that we couldn't parse before due to metadata slots being full.
		// This can happen when the reader releases messages faster than we parse.
		unparsedData := r.rawEnd - r.parsePos
		if unparsedData >= 5 { // At least a header worth of unparsed data
			// Refresh metadata space info and try to parse
			r.refreshCachedPositions()
			usedMeta := r.localMsgCnt - r.cachedConsumedMsgs
			if usedMeta >= int64(len(r.offsets)) {
				// We have unparsed data but no metadata space.
				// Wait for metadata space instead of blocking on Read().
				if err := r.waitForSpace(ctx); err != nil {
					return err
				}
				r.refreshCachedPositions()
				continue
			}
			// We have metadata space, try to parse
			prevMsgCnt := r.localMsgCnt
			streamingDetected := r.parseCompleteMessages(&msgsUntilSpaceRefresh)
			if r.localMsgCnt > prevMsgCnt {
				// Made progress - parsed at least one message
				if r.localMsgCnt > r.publishedMsgs {
					r.publish()
				}
				if streamingDetected {
					if err := r.handleStreamingMessage(ctx, src); err != nil {
						return err
					}
				}
				continue // Loop back to check for more
			}
			// No progress - we have a partial message (header but not body).
			// Fall through to read more data from the network.
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
			if r.localMsgCnt > r.publishedMsgs {
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
			for i := range 5 {
				hdr[i] = r.data[(hdrOff+int64(i))&r.dataMask]
			}
			msgLen = int64(binary.BigEndian.Uint32(hdr[1:5])) + 1
		}

		// Validate message length
		if msgLen < 5 {
			r.setError(fmt.Errorf("invalid message length %d < 5", msgLen))
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

// handleStreamingMessage pauses the reader goroutine until the consumer
// finishes reading the streaming message directly from the network connection.
func (r *RingBuffer) handleStreamingMessage(ctx context.Context, src io.Reader) error {
	// Wait for consumer to signal completion
	select {
	case <-ctx.Done():
		r.setError(ctx.Err())
		return ctx.Err()
	case err := <-r.streamDone:
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

// streamingMessageReader returns an io.Reader for reading the entire streaming
// message: header + buffered body from ring + remaining from network.
// The caller MUST call completeStreaming when done (even on error).
func (r *RingBuffer) streamingMessageReader(msgIdx int64) io.Reader {
	offset := r.MessageOffset(msgIdx)
	bodyInRing := r.streaming.bodyInRing
	remaining := r.streaming.totalLen - 5 - bodyInRing

	return io.MultiReader(
		r.rangeReader(offset, offset+5),              // header
		r.rangeReader(offset+5, offset+5+bodyInRing), // buffered body
		io.LimitReader(r.conn, remaining),            // network remainder
	)
}

// completeStreaming signals that the consumer has finished reading the
// streaming message. This resumes the paused reader goroutine.
func (r *RingBuffer) completeStreaming(err error) {
	r.streamDone <- err
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

// ReleaseThrough marks messages [0, throughMsg) as consumed, freeing space for writer.
func (r *RingBuffer) ReleaseThrough(throughMsg int64) {
	if throughMsg <= 0 {
		return
	}

	// Get the byte position after the last consumed message.
	// We use MessageEnd(throughMsg-1) instead of offsets[throughMsg&metaMask] because
	// the offset slots wrap around - when throughMsg >= metaSize and slots have been
	// reused, offsets[throughMsg&metaMask] would give the wrong position.
	dataPos := r.MessageEnd(throughMsg - 1)

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
	first, second := r.rangeSlices(start, end)

	if second == nil {
		return first
	}

	// Slow path: message wraps around, need to copy
	return slices.Concat(first, second)
}

// === Batch write methods (for zero-copy proxying) ===

// writeStreamingMessage writes a streaming message to dst.
// Composes readers for: header + buffered body (from ring) + remaining (from network).
func (r *RingBuffer) writeStreamingMessage(msgIdx int64, dst io.Writer) (int64, error) {
	if !r.isStreaming(msgIdx) {
		return r.WriteMessage(msgIdx, dst)
	}

	n, err := io.Copy(dst, r.streamingMessageReader(msgIdx))
	r.completeStreaming(err)
	return n, err
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

func (r *RingBuffer) rangeSlices(start, end int64) ([]byte, []byte) {
	if end <= start {
		return nil, nil
	}

	startOff := start & r.dataMask
	endOff := end & r.dataMask

	// Fast path: no wraparound
	if startOff < endOff || endOff == 0 {
		if endOff == 0 {
			return r.data[startOff:], nil
		}
		return r.data[startOff:endOff], nil
	}

	// Slow path: wraparound - combine two segments
	return r.data[startOff:], r.data[:endOff]
}

// rangeReader returns an io.Reader for bytes [start, end) in the ring buffer.
func (r *RingBuffer) rangeReader(start, end int64) io.Reader {
	first, second := r.rangeSlices(start, end)

	if second == nil {
		// Fast path - single contiguous
		return bytes.NewReader(first)
	}

	// Slow path - wraparound into two segments
	return io.MultiReader(
		bytes.NewReader(first),
		bytes.NewReader(second),
	)
}

func (r *RingBuffer) writeRange(start, end int64, dst io.Writer) (int64, error) {
	return io.Copy(dst, r.rangeReader(start, end))
}

// Close closes the ring buffer and wakes up any waiting goroutines.
func (r *RingBuffer) Close() {
	r.setError(io.EOF)
}

// NewWithSameBuffers creates a fresh RingBuffer reusing the underlying data and
// offset slices. All position counters start at 0, and channels are fresh.
// This avoids reallocating the ~260KB data buffer on each acquire.
func (r *RingBuffer) NewWithSameBuffers() *RingBuffer {
	r2 := &RingBuffer{
		RingBufferConfig: r.RingBufferConfig,
		ringBuffers:      r.ringBuffers,
	}
	r2.initChannels()
	return r2
}
