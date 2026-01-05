package pgproxy

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"iter"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/gammazero/deque"
	"github.com/justjake/pglink/pkg/pgwire"
	"github.com/panjf2000/gnet/v2"
)

const gnetTickDuration = 1 * time.Second

var errNotResolved = errors.New("promise not resolved")
var errWriteTooShort = errors.New("write too short")

// The goal of gnet is to avoid goroutine schedule overhead. this promise construct will probably not help
// we are using it while standing up gnet, can optimize later.
type promise[T any] struct {
	r      atomic.Bool
	done   chan struct{}
	err    error
	result T
}

func newPromise[T any]() *promise[T] {
	return &promise[T]{
		done: make(chan struct{}),
	}
}

func (p *promise[t]) Resolved() bool {
	return p.r.Load()
}

func (p *promise[T]) Result() (result T, err error) {
	if !p.Resolved() {
		err = errNotResolved
		return
	}
	return p.result, p.err
}

func (p *promise[T]) Wait(ctx context.Context) (result T, err error) {
	select {
	case <-p.done:
		return p.Result()
	case <-ctx.Done():
		err = ctx.Err()
		return
	}
}

func (p *promise[T]) Resolve(result T, err error) *promise[T] {
	if p.r.CompareAndSwap(false, true) {
		if err != nil {
			p.err = err
			close(p.done)
		} else {
			p.result = result
			close(p.done)
		}
	}
	return p
}

// EventHandler represents the engine events' callbacks for the Run call.
// Each event has an Action return value that is used manage the state
// of the connection and engine.
type gnetEventHandler interface {
	// OnBoot fires when the engine is ready for accepting connections.
	// The parameter engine has information and various utilities.
	OnBoot(eng gnet.Engine) (action gnet.Action)

	// OnShutdown fires when the engine is being shut down, it is called right after
	// all event-loops and connections are closed.
	OnShutdown(eng gnet.Engine)

	// OnOpen fires when a new connection has been opened.
	//
	// The Conn c has information about the connection such as its local and remote addresses.
	// The parameter out is the return value which is going to be sent back to the remote.
	// Sending large amounts of data back to the remote in OnOpen is usually not recommended.
	OnOpen(c gnet.Conn) (out []byte, action gnet.Action)

	// OnClose fires when a connection has been closed.
	// The parameter err is the last known connection error.
	OnClose(c gnet.Conn, err error) (action gnet.Action)

	// OnTraffic fires when a socket receives data from the remote.
	//
	// Also check out the comments on Reader and Writer interfaces.
	OnTraffic(c gnet.Conn) (action gnet.Action)

	// OnTick fires immediately after the engine starts and will fire again
	// following the duration specified by the delay return value.
	OnTick() (delay time.Duration, action gnet.Action)
}

type gnetProxyEngine struct {
	// gnet.BuiltinEventEngine
	eng    gnet.Engine
	client gnet.Client
	logger *slog.Logger
	ticks  int
}

// OnBoot implements [gnet.EventHandler].
func (g *gnetProxyEngine) OnBoot(eng gnet.Engine) (action gnet.Action) {
	g.logger.Info("gnet.OnBoot", "eng", eng)
	g.eng = eng
	return gnet.None
}

// OnClose implements [gnet.EventHandler].
func (g *gnetProxyEngine) OnClose(c gnet.Conn, err error) (action gnet.Action) {
	g.logger.Info("gnet.OnClose", "c", c, "err", err)
	return gnet.None
}

// OnOpen implements [gnet.EventHandler].
func (g *gnetProxyEngine) OnOpen(c gnet.Conn) (out []byte, action gnet.Action) {
	g.logger.Info("gnet.OnOpen", "c", c)
	return nil, gnet.None
}

// OnShutdown implements [gnet.EventHandler].
func (g *gnetProxyEngine) OnShutdown(eng gnet.Engine) {
	g.logger.Info("gnet.OnShutdown", "eng", eng)
}

// OnTick fires immediately after the engine starts and will fire again
// following the duration specified by the delay return value.
//
// OnTick implements [gnet.EventHandler].
func (g *gnetProxyEngine) OnTick() (delay time.Duration, action gnet.Action) {
	g.ticks++
	g.logger.Info("gnet.OnTick", "ticks", g.ticks)
	return gnetTickDuration, gnet.None
}

// OnTraffic implements [gnet.EventHandler].
func (g *gnetProxyEngine) OnTraffic(c gnet.Conn) (action gnet.Action) {
	handler, ok := c.Context().(*gnetProxyConn)
	if !ok {
		g.logger.Error("gnet.OnTraffic: invalid context: closing conn", "gconn", c, "context", c.Context())
		return gnet.Close
	}
	return handler.OnTraffic(c)
}

var _ gnet.EventHandler = (*gnetProxyEngine)(nil)

type gnetProxyConn struct {
	gconn  gnet.Conn
	ring   *pgwire.RingBuffer
	logger *slog.Logger
}

func (p *gnetProxyConn) OnTraffic(gconn gnet.Conn) (action gnet.Action) {
	if p.gconn != gconn {
		p.logger.Error("gnetProxyConn.OnTraffic: invalid gconn", "gconn", gconn, "p.gconn", p.gconn)
		return gnet.Close
	}

	return gnet.None
}

// ParseState is the internal state machine state for message parsing.
type ParseState int

const (
	ParseIdle ParseState = iota
	ParseReadingSize
	ParseReadingBody
)

func (s ParseState) String() string {
	switch s {
	case ParseIdle:
		return "idle"
	case ParseReadingSize:
		return "reading_size"
	case ParseReadingBody:
		return "reading_body"
	default:
		return fmt.Sprintf("unknown(%d)", s)
	}
}

type GnetStream struct {
	bufmsgs StreamMessages
	buf     OffsetSlice[byte]
	parser  StreamParser
	onBatch func(StreamSlice)

	streamDestination io.Writer
}

const MaxMessageSize = 1024 * 1024 // 1MB

func (s *GnetStream) OnTraffic(c gnet.Conn) error {
	if s.bufmsgs.Len() > 0 {
		return fmt.Errorf("gnetStream.OnTraffic: buffer messages not empty: %v", s.bufmsgs)
	}

	if !s.parser.Idle() {
		return fmt.Errorf("gnetStream.OnTraffic: parser not idle: %v", s.parser)
	}

	max := c.InboundBuffered()
	buf, err := c.Next(max)
	if err != nil {
		fmt.Errorf("gnetStream.OnTraffic: failed to read next %v bytes: %w", max, err)
	}

	// Copy the parser to accept partial writes without updating state.
	parser := s.parser
	parser.OnMsg = s.bufmsgs.Push
	written, err := parser.Write(buf)
	defer func() {
		_, discardErr := c.Discard(written)
		if discardErr != nil {
			panic(fmt.Errorf("discard error: %w", discardErr))
		}
	}()

	if err != nil {
		return fmt.Errorf("parse error: %w", err)
	}

	if parser.Idle() {
		s.parser = parser
	} else if s.bufmsgs.Len() > 0 {
		// accept whole message prefix
		written = int(s.bufmsgs.EndOffset() - s.bufmsgs.StartOffset())
		s.parser.ResetToIdleAt(s.bufmsgs.EndOffset())
	} else if parser.BytesNeeded() > MaxMessageSize {
		// too big to buffer, switch to streaming.
		panic("streaming not implemented")
	} else {
		written = 0
		return nil
	}

	streamSlice := StreamSlice{
		StreamMessages: &s.bufmsgs,
		Slice: OffsetSlice[byte]{
			Offset: s.bufmsgs.StartOffset(),
			B:      buf,
		},
	}

	var lastHandledIdx int64 = -1
	defer func() {
		if lastHandledIdx != -1 {
			s.bufmsgs.Truncate(lastHandledIdx + 1)
		}
	}()

	for msg := range streamSlice.All() {
		s.onMsgComplete(msg)
		lastHandledIdx = msg.Idx
	}

	return nil
}

type OffsetSlice[T any] struct {
	Offset int64
	B      []T
}

func NewStreamSlice(offset int64, bytes []T) OffsetSlice[T] {
	return OffsetSlice[byte]{
		Offset: offset,
		B:      bytes,
	}
}

func (s OffsetSlice[T]) Len() int {
	return len(s.B)
}

func (s OffsetSlice[T]) StartOffset() int64 {
	return s.Offset
}

func (s OffsetSlice[T]) EndOffset() int64 {
	return s.Offset + int64(len(s.B))
}

func (s OffsetSlice[T]) String() string {
	return fmt.Sprintf("StreamSlice{[%d,%d) %d bytes}", s.Offset, s.EndOffset(), len(s.B))
}

func (s OffsetSlice[T]) Slice(start, end int64) OffsetSlice[T] {
	return OffsetSlice[T]{
		Offset: start,
		B:      s.B[start-s.Offset : end-s.Offset],
	}
}

type StreamSliceMsg struct {
	Idx    int64
	Offset int64
	pgwire.SliceMsg
}

func NewStreamSliceMessage(idx int64, msg OffsetSlice[byte]) StreamSliceMsg {
	return StreamSliceMsg{
		Idx:    idx,
		Offset: msg.Offset,
		SliceMsg: pgwire.SliceMsg{
			Slice: msg.B,
		},
	}
}

type StreamSlice struct {
	*StreamMessages
	Slice OffsetSlice[byte]
}

func (s *StreamSlice) Whole() iter.Seq[StreamSliceMsg] {
	return func(yield func(StreamSliceMsg) bool) {
		for idx := s.StartMsgIdx(); idx < s.EndMsgIdx(); idx++ {
			if !yield(s.At(idx)) {
				return
			}
		}
	}
}

func (s *StreamSlice) At(idx int64) StreamSliceMsg {
	startOffset, endOffset := s.MsgRange(idx)
	dataEndOffset := s.Slice.EndOffset()
	if endOffset > dataEndOffset {
		slice := s.Slice.Slice(startOffset, dataEndOffset)
		msg := NewStreamSliceMessage(idx, slice)
		msg.Remaining = int(endOffset - dataEndOffset)
		return msg
	} else {
		return NewStreamSliceMessage(idx, s.Slice.Slice(startOffset, endOffset))
	}
}

// StreamMessages tracks message metadata in an abstract byte stream.
// Messages are indexed by a logical message index starting at msgStartIdx.
// Stores the byte offset where each message starts; the end of message N
// is the start of message N+1 (or endOffset for the last message).
type StreamMessages struct {
	msgStartIdx int64 // logical index of first message in deque
	endOffset   int64 // byte offset of end of last message (== stream position)

	// offsets[i] is the byte offset where message (msgStartIdx + i) starts.
	// len(offsets) == number of messages tracked.
	offsets deque.Deque[int64]
}

// Push adds a new message. The type is currently unused but available for future use.
func (p *StreamMessages) Push(_ pgwire.MsgType, startOffset, endOffset int64) {
	p.offsets.PushBack(startOffset)
	p.endOffset = endOffset
}

// Shift removes and returns the first message's byte range.
// Returns ok=false if no messages are available.
func (p *StreamMessages) Shift() (startOffset, endOffset int64, ok bool) {
	if p.offsets.Len() == 0 {
		return 0, 0, false
	}
	startOffset = p.offsets.PopFront()
	p.msgStartIdx++
	if p.offsets.Len() > 0 {
		endOffset = p.offsets.Front()
	} else {
		endOffset = p.endOffset
	}
	return startOffset, endOffset, true
}

// ShiftN removes the first n messages.
func (p *StreamMessages) ShiftN(n int) {
	if n <= 0 {
		return
	}
	if n >= p.offsets.Len() {
		p.msgStartIdx += int64(p.offsets.Len())
		p.offsets.Clear()
		return
	}
	for i := 0; i < n; i++ {
		p.offsets.PopFront()
	}
	p.msgStartIdx += int64(n)
}

// Truncate removes all messages before newStartMsgIdx.
func (p *StreamMessages) Truncate(newStartMsgIdx int64) {
	toRemove := int(newStartMsgIdx - p.msgStartIdx)
	p.ShiftN(toRemove)
}

// Len returns the number of messages currently tracked.
func (p *StreamMessages) Len() int {
	return p.offsets.Len()
}

// StartMsgIdx returns the logical index of the first message.
func (p *StreamMessages) StartMsgIdx() int64 {
	return p.msgStartIdx
}

// EndMsgIdx returns the logical index one past the last message.
func (p *StreamMessages) EndMsgIdx() int64 {
	return p.msgStartIdx + int64(p.offsets.Len())
}

func (p *StreamMessages) StartOffset() int64 {
	return p.offsets.Front()
}

func (p *StreamMessages) EndOffset() int64 {
	return p.endOffset
}

// Offset returns the start byte offset of the message at msgIdx.
// Panics if msgIdx is out of range.
func (p *StreamMessages) Offset(msgIdx int64) int64 {
	idx := int(msgIdx - p.msgStartIdx)
	return p.offsets.At(idx)
}

// Size returns the byte size of the message at msgIdx.
// Panics if msgIdx is out of range.
func (p *StreamMessages) Size(msgIdx int64) int64 {
	start, end := p.MsgRange(msgIdx)
	return end - start
}

// MsgRange returns the byte range [start, end) of the message at msgIdx.
// Panics if msgIdx is out of range.
func (p *StreamMessages) MsgRange(msgIdx int64) (startOffset, endOffset int64) {
	idx := int(msgIdx - p.msgStartIdx)
	startOffset = p.offsets.At(idx)
	if idx+1 < p.offsets.Len() {
		endOffset = p.offsets.At(idx + 1)
	} else {
		endOffset = p.endOffset
	}
	return
}

// Range returns the byte range [start, end) spanning messages [startMsg, endMsg).
// Panics if indices are out of range.
func (p *StreamMessages) Range(startMsg, endMsg int64) (startOffset, endOffset int64) {
	startOffset = p.Offset(startMsg)
	if endMsg >= p.EndMsgIdx() {
		endOffset = p.endOffset
	} else {
		endOffset = p.Offset(endMsg)
	}
	return
}

type IncompleteStreamSliceMsg struct {
	Remaining int64
	StreamSliceMsg
}

type OnBatchCallback func(complete StreamSlice, IncompleteStreamSliceMsg, remaining []byte)

type StreamBatchParser struct {
	OnBatch  OnBatchCallback
	Parser   StreamParser
	complete StreamSlice
}

func NewStreamBatchParser(onBatch OnBatchCallback) *StreamBatchParser {
	parser := &StreamBatchParser{OnBatch: onBatch}
	parser.Parser.OnMsg = parser.complete.Push
	return parser
}

func (p *StreamBatchParser) Write(b []byte) (int, error) {
	// preconditions
	if p.complete.Slice.Len() > 0 {
		return 0, fmt.Errorf("stream batch parser: complete slice not empty")
	}
	if !p.Parser.Idle() || p.Parser.BytesNeeded() > 0 {
		return 0, fmt.Errorf("stream batch parser: parser not idle: need %d bytes", p.Parser.BytesNeeded())
	}
	if p.Parser.curIdx != p.complete.EndOffset() {
		return 0, fmt.Errorf("stream batch parser: parser curIdx != complete last message endOffset")
	}

	// p.complete aliases data in b until the end of the Write call.
	p.complete.Slice = OffsetSlice[byte]{Offset: p.Parser.curIdx, B: b}
	defer func() {
		p.complete.Slice = OffsetSlice[byte]{}
	}()

	written, err := p.Parser.WriteComplete(b)
	if written == 0 || (err != nil && !errors.Is(err, ErrIncompleteMessage)) {
		return written, err
	}

	remaining := b[written:]

}

func (p *StreamBatchParser) onMsg(msgType pgwire.MsgType, startIdx, endIdx int64) {
	p.complete.Push(msgType, startIdx, endIdx)
}

// StreamParser is a PostgreSQL wire protocol message boundary parser for normal mode.
// It parses messages with format: type (1 byte) + length (4 bytes) + body.
// Implements io.Writer and calls onMsgComplete for each complete message.
//
// The zero value is an idle parser with no OnMsg callback.
type StreamParser struct {
	// Called when a complete message is parsed.
	// msgType is the message type byte.
	// startIdx and endIdx are byte offsets in the stream.
	OnMsg func(msgType pgwire.MsgType, startIdx, endIdx int64)

	state ParseState

	curIdx   int64   // total bytes processed
	header   [5]byte // type (1) + length (4)
	headerN  int     // bytes accumulated in header (0-5)
	bodyLen  int64   // body length (length field value - 4)
	bodyRead int64   // body bytes consumed so far
}

func NewStreamParser(onComplete func(pgwire.MsgType, int64, int64)) *StreamParser {
	return &StreamParser{OnMsg: onComplete}
}

func (p *StreamParser) ResetToIdleAt(idx int64) {
	p.state = ParseIdle
	p.curIdx = idx
	p.headerN = 0
	p.bodyLen = 0
	p.bodyRead = 0
}

func (p *StreamParser) Idle() bool {
	return p.state == ParseIdle
}

func (p *StreamParser) State() ParseState {
	return p.state
}

// BytesNeeded returns the number of bytes needed to complete the current message.
// Returns 0 if idle (no message in progress).
func (p *StreamParser) BytesNeeded() int {
	switch p.state {
	case ParseIdle:
		return 0
	case ParseReadingSize:
		return 5 - p.headerN // need 5 bytes total: type (1) + length (4)
	case ParseReadingBody:
		return int(p.bodyLen - p.bodyRead)
	default:
		return 0
	}
}

var ErrIncompleteMessage = errors.New("incomplete message")

// WriteComplete writes up to the last complete message in b to the parser.
// Writes of partial messages are rejected by returning [ErrIncompleteMessage].
func (p *StreamParser) WriteComplete(b []byte, largeMessageSize int64) (int, error) {
	startOffset := p.curIdx
	completeBytes := func() int {
		return int(p.curIdx - startOffset)
	}

	provisional := *p
	provisional.OnMsg = func(msgType pgwire.MsgType, startIdx, endIdx int64) {
		// Move the parser forward to the start of the next message.
		p.ResetToIdleAt(endIdx)
		p.OnMsg(msgType, startIdx, endIdx)
	}
	provisionallyWritten, err := provisional.Write(b)

	if err != nil {
		return completeBytes(), err
	} else if provisionallyWritten != completeBytes() {
		return completeBytes(), ErrIncompleteMessage
	} else {
		return provisionallyWritten, nil
	}
}

// Write implements io.Writer. Parses PostgreSQL messages (type + length + body)
// and calls onMsgComplete for each complete message found.
func (p *StreamParser) Write(b []byte) (int, error) {
	if len(b) == 0 {
		return 0, nil
	}

	written := 0

	// Fast path: process complete messages without state transitions
	for p.state == ParseIdle && len(b) >= 5 {
		length := int64(binary.BigEndian.Uint32(b[1:5]))
		if length < 4 {
			return written, fmt.Errorf("invalid message length: %d", length)
		}
		msgSize := 1 + length
		if int64(len(b)) < msgSize {
			break // incomplete message, use slow path
		}
		// Complete message available
		msgType := pgwire.MsgType(b[0])
		startIdx := p.curIdx
		p.curIdx += msgSize
		if p.OnMsg != nil {
			p.OnMsg(msgType, startIdx, p.curIdx)
		}
		b = b[msgSize:]
		written += int(msgSize)
	}

	// Slow path: state machine for partial messages
	for len(b) > 0 {
		switch p.state {
		case ParseIdle:
			// Start reading header (type + 4-byte length)
			p.state = ParseReadingSize
			fallthrough

		case ParseReadingSize:
			var complete bool
			b, written, complete = p.accumulateHeader(b, written, 5)
			if !complete {
				continue
			}

			bodyLen, err := p.parseLength(1) // length is at offset 1 (after type byte)
			if err != nil {
				return written, err
			}

			p.bodyLen = bodyLen
			p.bodyRead = 0
			if p.bodyLen == 0 {
				p.finishMessage()
				continue
			}
			p.state = ParseReadingBody

		case ParseReadingBody:
			var err error
			b, written, err = p.consumeBody(b, written)
			if err != nil {
				return written, err
			}

		default:
			return written, fmt.Errorf("invalid state: %v", p.state)
		}
	}
	return written, nil
}

// consumeBody consumes body bytes and finishes the message when complete.
func (p *StreamParser) consumeBody(b []byte, written int) ([]byte, int, error) {
	need := p.bodyLen - p.bodyRead
	n := min(need, int64(len(b)))

	p.bodyRead += n
	p.curIdx += n
	written += int(n)
	b = b[n:]

	if p.bodyRead >= p.bodyLen {
		p.finishMessage()
	}
	return b, written, nil
}

// finishMessage completes a message and calls the callback.
func (p *StreamParser) finishMessage() {
	msgType := pgwire.MsgType(p.header[0])
	endIdx := p.curIdx
	startIdx := endIdx - p.bodyLen - 5 // 5 = type + length header

	if p.OnMsg != nil {
		p.OnMsg(msgType, startIdx, endIdx)
	}

	p.resetForNextMessage()
}

// accumulateHeader copies bytes from b into the header buffer.
// Returns remaining bytes, updated written count, and whether header is complete.
func (p *StreamParser) accumulateHeader(b []byte, written, need int) ([]byte, int, bool) {
	have := len(b)
	n := min(need-p.headerN, have)
	copy(p.header[p.headerN:], b[:n])
	p.headerN += n
	p.curIdx += int64(n)
	return b[n:], written + n, p.headerN >= need
}

// parseLength parses a big-endian int32 length from header at the given offset.
// Returns the body length (length field - 4) and any validation error.
func (p *StreamParser) parseLength(offset int) (int64, error) {
	length := int64(binary.BigEndian.Uint32(p.header[offset:]))
	if length < 4 {
		return 0, fmt.Errorf("invalid message length: %d", length)
	}
	return length - 4, nil
}

// resetForNextMessage resets state for parsing the next message.
func (p *StreamParser) resetForNextMessage() {
	p.state = ParseIdle
	p.headerN = 0
}

// FrontendStreamParser parses client->server messages.
// Handles startup messages (length + body, no type byte) then normal messages.
type FrontendStreamParser struct {
	StreamParser
	startup bool // true while in startup phase
}

// NewFrontendStream creates a parser for client->server messages.
func NewFrontendStream(onComplete func(pgwire.MsgType, int64, int64)) *FrontendStreamParser {
	return &FrontendStreamParser{
		StreamParser: StreamParser{OnMsg: onComplete},
		startup:      true,
	}
}

// SetNormalPhase transitions to normal message parsing.
func (p *FrontendStreamParser) SetNormalPhase() {
	p.startup = false
}

// Write implements io.Writer.
func (p *FrontendStreamParser) Write(b []byte) (int, error) {
	if len(b) == 0 {
		return 0, nil
	}
	if p.startup {
		return p.writeStartup(b)
	}
	return p.StreamParser.Write(b)
}

// writeStartup handles startup messages: length (4 bytes) + body (no type byte).
func (p *FrontendStreamParser) writeStartup(b []byte) (int, error) {
	written := 0

	for len(b) > 0 {
		switch p.state {
		case ParseIdle:
			p.state = ParseReadingSize
			fallthrough

		case ParseReadingSize:
			var complete bool
			b, written, complete = p.accumulateHeader(b, written, 4)
			if !complete {
				continue
			}

			bodyLen, err := p.parseLength(0)
			if err != nil {
				return written, err
			}

			p.bodyLen = bodyLen
			p.bodyRead = 0
			if p.bodyLen == 0 {
				p.finishStartupMessage()
				continue
			}
			p.state = ParseReadingBody

		case ParseReadingBody:
			var err error
			b, written, err = p.consumeStartupBody(b, written)
			if err != nil {
				return written, err
			}

		default:
			return written, fmt.Errorf("invalid state: %v", p.state)
		}
	}
	return written, nil
}

// consumeStartupBody consumes body bytes for startup messages.
func (p *FrontendStreamParser) consumeStartupBody(b []byte, written int) ([]byte, int, error) {
	need := p.bodyLen - p.bodyRead
	n := min(need, int64(len(b)))

	p.bodyRead += n
	p.curIdx += n
	written += int(n)
	b = b[n:]

	if p.bodyRead >= p.bodyLen {
		p.finishStartupMessage()
	}
	return b, written, nil
}

// finishStartupMessage completes a startup message.
func (p *FrontendStreamParser) finishStartupMessage() {
	endIdx := p.curIdx
	startIdx := endIdx - p.bodyLen - 4 // 4 = length header only

	if p.OnMsg != nil {
		p.OnMsg(pgwire.MsgStartup, startIdx, endIdx)
	}

	p.resetForNextMessage()
}

// BackendStreamParser parses server->client messages.
// Handles SSL response ('S'/'N' single byte) then normal messages.
type BackendStreamParser struct {
	StreamParser
	startup bool // true while in startup phase
}

// NewBackendStream creates a parser for server->client messages.
func NewBackendStream(onComplete func(pgwire.MsgType, int64, int64)) *BackendStreamParser {
	return &BackendStreamParser{
		StreamParser: StreamParser{OnMsg: onComplete},
		startup:      true,
	}
}

// SetNormalPhase transitions to normal message parsing.
func (p *BackendStreamParser) SetNormalPhase() {
	p.startup = false
}

// Write implements io.Writer.
func (p *BackendStreamParser) Write(b []byte) (int, error) {
	if len(b) == 0 {
		return 0, nil
	}
	if p.startup {
		return p.writeStartup(b)
	}
	return p.StreamParser.Write(b)
}

// writeStartup handles SSL response detection.
func (p *BackendStreamParser) writeStartup(b []byte) (int, error) {
	written := 0

	// Check for SSL response (single byte 'S' or 'N')
	if p.state == ParseIdle {
		ch := b[0]
		if ch == 'S' || ch == 'N' {
			startIdx := p.curIdx
			p.curIdx++
			if p.OnMsg != nil {
				p.OnMsg(pgwire.MsgType(ch), startIdx, p.curIdx)
			}
			b = b[1:]
			written++
			if len(b) == 0 {
				return written, nil
			}
		}
		// Not an SSL response - switch to normal phase
		p.startup = false
	}

	// Continue with normal message parsing
	n, err := p.StreamParser.Write(b)
	return written + n, err
}
