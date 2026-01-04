package pgproxy

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/gammazero/deque"
	"github.com/justjake/pglink/pkg/pgwire"
	"github.com/panjf2000/gnet/v2"
)

const gnetTickDuration = 1 * time.Second

var errNotResolved = errors.New("promise not resolved")

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

// pgbufstate is the internal state machine state for message parsing.
type pgbufstate int

const (
	pgbufstateIdle pgbufstate = iota
	pgbufstateReadingSize
	pgbufstateReadingBody
)

func (s pgbufstate) String() string {
	switch s {
	case pgbufstateIdle:
		return "idle"
	case pgbufstateReadingSize:
		return "reading_size"
	case pgbufstateReadingBody:
		return "reading_body"
	default:
		return fmt.Sprintf("unknown(%d)", s)
	}
}

// tracks message metadata in an abstract stream.
type pgstreambuf struct {
	msgStartIdx int64
	msgEndIdx   int64
	// contains the offsets of messages [msgStartIdx, msgEndIdx]).
	// len(offsets) == msgEndIdx - msgStartIdx
	//
	// the offset is the byte offset where the message starts in the stream.
	offsets deque.Deque[int64]
}

func (p *pgstreambuf) AddMessage(t pgwire.MsgType, startOffset, endOffset int64) {
	// todo
}

func (p *pgstreambuf) Size(msg int64) int64 {
	// todo
}

func (p *pgstreambuf) MsgRange(msg int64) (startOffset, endOffset int64) {
	// todo
}

func (p *pgstreambuf) Range(startMsg, endMsg int64) (startOffset, endOffset int64) {
	// todo
}

// pgstream is a PostgreSQL wire protocol message boundary parser for normal mode.
// It parses messages with format: type (1 byte) + length (4 bytes) + body.
// Implements io.Writer and calls onMsgComplete for each complete message.
type pgstream struct {
	state pgbufstate

	curIdx   int64   // total bytes processed
	header   [5]byte // type (1) + length (4)
	headerN  int     // bytes accumulated in header (0-5)
	bodyLen  int64   // body length (length field value - 4)
	bodyRead int64   // body bytes consumed so far

	// Called when a complete message is parsed.
	// msgType is the message type byte.
	// startIdx and endIdx are byte offsets in the stream.
	onMsgComplete func(msgType pgwire.MsgType, startIdx, endIdx int64)
}

// accumulateHeader copies bytes from b into the header buffer.
// Returns remaining bytes, updated written count, and whether header is complete.
func (p *pgstream) accumulateHeader(b []byte, written, need int) ([]byte, int, bool) {
	have := len(b)
	n := min(need-p.headerN, have)
	copy(p.header[p.headerN:], b[:n])
	p.headerN += n
	p.curIdx += int64(n)
	return b[n:], written + n, p.headerN >= need
}

// parseLength parses a big-endian int32 length from header at the given offset.
// Returns the body length (length field - 4) and any validation error.
func (p *pgstream) parseLength(offset int) (int64, error) {
	length := int64(binary.BigEndian.Uint32(p.header[offset:]))
	if length < 4 {
		return 0, fmt.Errorf("invalid message length: %d", length)
	}
	return length - 4, nil
}

// resetForNextMessage resets state for parsing the next message.
func (p *pgstream) resetForNextMessage() {
	p.state = pgbufstateIdle
	p.headerN = 0
}

// Write implements io.Writer. Parses PostgreSQL messages (type + length + body)
// and calls onMsgComplete for each complete message found.
func (p *pgstream) Write(b []byte) (int, error) {
	if len(b) == 0 {
		return 0, nil
	}

	written := 0

	// Fast path: process complete messages without state transitions
	for p.state == pgbufstateIdle && len(b) >= 5 {
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
		if p.onMsgComplete != nil {
			p.onMsgComplete(msgType, startIdx, p.curIdx)
		}
		b = b[msgSize:]
		written += int(msgSize)
	}

	// Slow path: state machine for partial messages
	for len(b) > 0 {
		switch p.state {
		case pgbufstateIdle:
			// Start reading header (type + 4-byte length)
			p.state = pgbufstateReadingSize
			fallthrough

		case pgbufstateReadingSize:
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
			p.state = pgbufstateReadingBody

		case pgbufstateReadingBody:
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
func (p *pgstream) consumeBody(b []byte, written int) ([]byte, int, error) {
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
func (p *pgstream) finishMessage() {
	msgType := pgwire.MsgType(p.header[0])
	endIdx := p.curIdx
	startIdx := endIdx - p.bodyLen - 5 // 5 = type + length header

	if p.onMsgComplete != nil {
		p.onMsgComplete(msgType, startIdx, endIdx)
	}

	p.resetForNextMessage()
}

// pgfrontendstream parses client->server messages.
// Handles startup messages (length + body, no type byte) then normal messages.
type pgfrontendstream struct {
	pgstream
	startup bool // true while in startup phase
}

// NewFrontendStream creates a parser for client->server messages.
func NewFrontendStream(onComplete func(pgwire.MsgType, int64, int64)) *pgfrontendstream {
	return &pgfrontendstream{
		pgstream: pgstream{onMsgComplete: onComplete},
		startup:  true,
	}
}

// SetNormalPhase transitions to normal message parsing.
func (p *pgfrontendstream) SetNormalPhase() {
	p.startup = false
}

// Write implements io.Writer.
func (p *pgfrontendstream) Write(b []byte) (int, error) {
	if len(b) == 0 {
		return 0, nil
	}
	if p.startup {
		return p.writeStartup(b)
	}
	return p.pgstream.Write(b)
}

// writeStartup handles startup messages: length (4 bytes) + body (no type byte).
func (p *pgfrontendstream) writeStartup(b []byte) (int, error) {
	written := 0

	for len(b) > 0 {
		switch p.state {
		case pgbufstateIdle:
			p.state = pgbufstateReadingSize
			fallthrough

		case pgbufstateReadingSize:
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
			p.state = pgbufstateReadingBody

		case pgbufstateReadingBody:
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
func (p *pgfrontendstream) consumeStartupBody(b []byte, written int) ([]byte, int, error) {
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
func (p *pgfrontendstream) finishStartupMessage() {
	endIdx := p.curIdx
	startIdx := endIdx - p.bodyLen - 4 // 4 = length header only

	if p.onMsgComplete != nil {
		p.onMsgComplete(pgwire.MsgStartup, startIdx, endIdx)
	}

	p.resetForNextMessage()
}

// pgbackendstream parses server->client messages.
// Handles SSL response ('S'/'N' single byte) then normal messages.
type pgbackendstream struct {
	pgstream
	startup bool // true while in startup phase
}

// NewBackendStream creates a parser for server->client messages.
func NewBackendStream(onComplete func(pgwire.MsgType, int64, int64)) *pgbackendstream {
	return &pgbackendstream{
		pgstream: pgstream{onMsgComplete: onComplete},
		startup:  true,
	}
}

// SetNormalPhase transitions to normal message parsing.
func (p *pgbackendstream) SetNormalPhase() {
	p.startup = false
}

// Write implements io.Writer.
func (p *pgbackendstream) Write(b []byte) (int, error) {
	if len(b) == 0 {
		return 0, nil
	}
	if p.startup {
		return p.writeStartup(b)
	}
	return p.pgstream.Write(b)
}

// writeStartup handles SSL response detection.
func (p *pgbackendstream) writeStartup(b []byte) (int, error) {
	written := 0

	// Check for SSL response (single byte 'S' or 'N')
	if p.state == pgbufstateIdle {
		ch := b[0]
		if ch == 'S' || ch == 'N' {
			startIdx := p.curIdx
			p.curIdx++
			if p.onMsgComplete != nil {
				p.onMsgComplete(pgwire.MsgType(ch), startIdx, p.curIdx)
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
	n, err := p.pgstream.Write(b)
	return written + n, err
}
