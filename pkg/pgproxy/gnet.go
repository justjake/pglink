package pgproxy

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync/atomic"
	"time"

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

type pgstream struct {
	state pgbufstate

	// todo: add remaining necessary state modeling.
	curIdx         int64
	header         [5]byte
	headerStartIdx int64

	// called from Write as soon as a complete message is parsed and pgstream's
	// internal state is in idle. at that point curIdx will be == endIdx.
	onMsgComplete func(t pgwire.MsgType, startIdx, endIdx int64)
}

type pgstreamStateFn func([]byte, int) ([]byte, int, error)

func (p *pgstream) Write(b []byte) (written int, err error) {
	if len(b) == 0 {
		return 0, nil
	}

	for len(b) > 0 {
		switch p.state {
		// todo: handling for exotic like SSL yes/no.
		case pgbufstateIdle:
			b, written, err = p.writeIdle(b, written)
			if err != nil {
				return written, err
			}
		case pgbufstateReadingSize:
			// TODO
		case pgbufstateReadingBody:
			// TODO
		default:
			panic(fmt.Sprintf("invalid state: %v", p.state))
		}
	}

	return
}

func (p *pgstream) writeIdle(b []byte, written int) (remaining []byte, written int, err error) {
	// todo
}
