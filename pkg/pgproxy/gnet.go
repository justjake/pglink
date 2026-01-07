package pgproxy

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/justjake/pglink/pkg/pgwire"
	"github.com/panjf2000/gnet/v2"
)

const gnetTickDuration = 10 * time.Second
const gnetBufferMaxSize = 64 * 1024              // 64KiB
const gnetMaxMessageSize = gnetBufferMaxSize / 2 // TODO

type gnetTrafficHandler interface {
	// OnTraffic fires when a socket receives data from the remote.
	//
	// Also check out the comments on Reader and Writer interfaces.
	OnTraffic(c gnet.Conn) (action gnet.Action)
	OnClose(c gnet.Conn) (action gnet.Action)
	OnOpen(c gnet.Conn) (action gnet.Action)
}

type gnetProxyEngine struct {
	logger    *slog.Logger
	eng       gnet.Engine
	client    *gnet.Client
	tickCount int

	startOnce sync.Once
	startErr  error

	mu sync.Mutex
}

func (g *gnetProxyEngine) Start() error {
	g.startOnce.Do(func() {
		logger := slog.Default().WithGroup("gnet")
		// The fastest we've tested is 2 loops.
		// This gives 4 loops on my 16 core MacBook Pro.
		// We continue to test w/ >2 to improve scalability.
		numLoops := runtime.NumCPU() / 4
		numLoops = min(numLoops, 1)
		logger.Info("starting gnet engine", "numLoops", numLoops)
		client, err := gnet.NewClient(g,
			gnet.WithReadBufferCap(gnetBufferMaxSize),
			gnet.WithTicker(true),
			gnet.WithNumEventLoop(numLoops),
			gnet.WithLockOSThread(true),
			gnet.WithLogger(&gnetLogger{logger}),
			gnet.WithReadBufferCap(gnetBufferMaxSize),
		)
		if err != nil {
			g.startErr = err
			return
		}
		g.client = client
		g.logger = logger
		client.Start()
	})
	return g.startErr
}

// OnBoot implements [gnet.EventHandler].
func (g *gnetProxyEngine) OnBoot(eng gnet.Engine) (action gnet.Action) {
	g.logger.Info("gnet.OnBoot", "eng", eng)
	g.eng = eng
	return gnet.None
}

// OnClose implements [gnet.EventHandler].
func (g *gnetProxyEngine) OnClose(c gnet.Conn, err error) (action gnet.Action) {
	g.logger.Info("OnClose", "fd", c.Fd(), "addr", c.RemoteAddr(), "err", err, "errT", fmt.Sprintf("%#T", err))
	return gnet.None
}

// OnOpen implements [gnet.EventHandler].
func (g *gnetProxyEngine) OnOpen(c gnet.Conn) (out []byte, action gnet.Action) {
	g.logger.Info("OnOpen", "fd", c.Fd(), "addr", c.RemoteAddr())
	handler, ok := c.Context().(gnetTrafficHandler)
	if !ok {
		g.logger.Error("OnOpen: context not a gnetTrafficHandler: closing conn", "type", fmt.Sprintf("%T", c.Context()), "context", c.Context())
		return nil, gnet.Close
	}
	handler.OnOpen(c)
	return nil, gnet.None
}

// OnShutdown implements [gnet.EventHandler].
func (g *gnetProxyEngine) OnShutdown(eng gnet.Engine) {
	g.logger.Info("OnShutdown", "eng", eng)

	// TODO: cancel all sessions we own.
}

// OnTick fires immediately after the engine starts and will fire again
// following the duration specified by the delay return value.
//
// OnTick implements [gnet.EventHandler].
func (g *gnetProxyEngine) OnTick() (delay time.Duration, action gnet.Action) {
	g.tickCount++
	if g.tickCount%100 == 0 {
		g.logger.Debug("OnTick", "ticks", g.tickCount)
	}
	return gnetTickDuration, gnet.None
}

// OnTraffic implements [gnet.EventHandler].
func (g *gnetProxyEngine) OnTraffic(c gnet.Conn) (action gnet.Action) {
	debugEnabled := g.logger.Enabled(context.Background(), slog.LevelDebug)
	if debugEnabled {
		g.logger.Debug("OnTraffic", "fd", c.Fd(), "addr", c.RemoteAddr())
	}
	handler, ok := c.Context().(gnetTrafficHandler)
	if !ok {
		g.logger.Error("OnTraffic: context not a gnetTrafficHandler: closing conn", "type", fmt.Sprintf("%T", c.Context()), "context", c.Context())
		return gnet.Close
	}
	if debugEnabled {
		g.logger.Debug("OnTraffic: calling handler", "handler", fmt.Sprintf("%T", handler))
	}
	res := handler.OnTraffic(c)
	if debugEnabled {
		g.logger.Debug("OnTraffic: action", "action", res)
	}
	return res
}

var _ gnet.EventHandler = (*gnetProxyEngine)(nil)

var defaultGnetEngine = &gnetProxyEngine{}

type gnetProxyRuntime struct {
	// static
	session *Session
	logger  *slog.Logger
	engine  *gnetProxyEngine
	runCtx  context.Context

	// state
	mu      sync.Mutex
	wg      sync.WaitGroup
	client  *gnetProxyConn
	backend *gnetProxyConn
	loop    gnet.EventLoop
	running atomic.Bool
	runErr  error
	pos     Pos2
}

func NewGnetProxyRuntime(ctx context.Context, session *Session) (Runtime, error) {
	if err := defaultGnetEngine.Start(); err != nil {
		return nil, fmt.Errorf("failed to start gnet engine: %w", err)
	}

	runtime := &gnetProxyRuntime{
		session: session,
		logger:  session.Logger().WithGroup("runtime").With("type", "gnet"),
		engine:  defaultGnetEngine,
	}

	return runtime, nil
}

// Run implements [Runtime].
// Called on main thread.
func (g *gnetProxyRuntime) Run(ctx context.Context) error {
	if g.running.Load() {
		return fmt.Errorf("already running")
	}

	synced := func() error {
		g.mu.Lock()
		defer g.mu.Unlock()

		if g.running.Load() {
			return fmt.Errorf("already running")
		}

		if g.client == nil {
			return fmt.Errorf("client not started")
		}

		g.runCtx = ctx
		g.running.Store(true)

		if err := g.client.gconn.Wake(nil); err != nil {
			return fmt.Errorf("failed to wake client: %w", err)
		}
		if g.backend != nil {
			if err := g.backend.gconn.Wake(nil); err != nil {
				return fmt.Errorf("failed to wake backend: %w", err)
			}
		}

		return nil
	}

	if err := synced(); err != nil {
		return err
	}
	defer func() {
		g.running.Store(false)
		g.logger.Debug("Run: done", "err", g.runErr)
	}()

	g.wg.Wait()
	return g.runErr
}

// StartConn implements [Runtime].
// Called on main thread.
func (g *gnetProxyRuntime) StartConn(ctx context.Context, role ProxyRole, conn Conn) (err error) {
	g.mu.Lock()
	defer g.mu.Unlock()

	connPtr := g.conn(role)
	if *connPtr != nil {
		return fmt.Errorf("connection already exists")
	}

	netConn, err := conn.AcquireNetConn(ctx)
	if err != nil {
		return err
	}

	dupableConn, ok := netConn.(filedupConn)
	if !ok {
		return fmt.Errorf("Conn.AcquireNetConn returned a net.Conn without a File() method: %T", netConn)
	}

	file, err := dupableConn.File()
	if err != nil {
		return fmt.Errorf("failed to get file from net.Conn: %w", err)
	}
	defer func() {
		err = errors.Join(err, file.Close())
	}()

	fileConn, err := net.FileConn(file)
	if err != nil {
		return fmt.Errorf("failed to convert file to net.Conn: %w", err)
	}
	defer func() {
		if !ok {
			err = errors.Join(err, fileConn.Close())
		}
	}()

	proxyConn := &gnetProxyConn{
		role:    role,
		runtime: g,
		logger:  g.logger.With("role", role),
	}

	onSuccess := func(gconn gnet.Conn) {
		if g.logger.Enabled(context.Background(), slog.LevelDebug) {
			g.logger.Debug("enrolled conn",
				"role", role,
				"conn", fmt.Sprintf("%T", conn),
				"netConn", fmt.Sprintf("%T", netConn),
				"gconn", fmt.Sprintf("%T", gconn),
			)
		}

		g.wg.Add(1)
		proxyConn.onClose = g.wg.Done
		proxyConn.gconn = gconn
		*connPtr = proxyConn
	}

	if loop := g.eventLoop(); loop != nil {
		// Must register on the same event loop as other connections
		// so we run in the same goroutine.
		ch, err := loop.Enroll(gnet.NewContext(ctx, proxyConn), fileConn)
		if err != nil {
			return fmt.Errorf("gnet enroll: event loop: %w", err)
		}

		// If we are calling from inside the event loop, waiting on the result will deadlock.
		// Asynchronously wait on a background goroutine, then complete on the event loop thread.
		ok = true // so far as we know anyways
		*connPtr = proxyConn
		await(ctx, loop, ch, func(result gnet.RegisteredResult, err error) error {
			if err != nil {
				g.runErr = errors.Join(
					fmt.Errorf("gnet enroll: event loop: awaited: %w", err),
					fileConn.Close(),
					file.Close(),
				)
				g.logger.Error("gnet enroll: event loop: awaited: failed", "error", g.runErr)
				_ = g.Stop(ctx)
				return g.runErr
			}
			onSuccess(result.Conn)
			return nil
		})
		return nil
	}

	// First connection, no event loop assigned.
	gconn, err := g.engine.client.EnrollContext(fileConn, proxyConn)
	if err != nil {
		return fmt.Errorf("gnet enroll: %w", err)
	}

	ok = true
	onSuccess(gconn)
	return nil
}

// Stop implements [Runtime].
func (g *gnetProxyRuntime) Stop(ctx context.Context) error {
	g.mu.Lock()
	defer g.mu.Unlock()

	if g.client != nil {
		if err := g.client.Close(); err != nil {
			return fmt.Errorf("failed to close client: %w", err)
		}
		g.client = nil
	}
	if g.backend != nil {
		if err := g.backend.Close(); err != nil {
			return fmt.Errorf("failed to close backend: %w", err)
		}
		g.backend = nil
	}
	return nil
}

// StopConn implements [Runtime].
func (g *gnetProxyRuntime) StopConn(ctx context.Context, role ProxyRole) error {
	g.mu.Lock()
	defer g.mu.Unlock()

	connPtr := g.conn(role)
	if *connPtr == nil {
		return fmt.Errorf("connection not started")
	}
	err := (*connPtr).Close()
	if err != nil {
		return fmt.Errorf("failed to close connection: %w", err)
	}
	*connPtr = nil
	return nil
}

// WriteConn implements [Runtime].
// Assumed to be only called from within the loop...?
func (g *gnetProxyRuntime) WriteConn(ctx context.Context, role ProxyRole, queued *pgwire.WriteQueue) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}

	p := *g.conn(role)
	if p == nil {
		return fmt.Errorf("connection not started")
	}

	var written int64
	if g.logger.Enabled(ctx, slog.LevelDebug) {
		g.logger.Debug("gnetProxyRuntime.WriteConn", "role", role, "queued", queued)
		defer func() {
			g.logger.Debug("gnetProxyRuntime.WriteConn: done", "written", written)
		}()
	}

	// TODO: use writev
	var err error
	if p.gconn == nil {
		// Still starting up
		g.logger.Debug("gnetProxyRuntime.WriteConn: connection not started yet, promise to write later")
		written, err = queued.WriteTo(&p.promisedWrites)
		if err != nil {
			return fmt.Errorf("gconn WriteTo promisedWrites buffer: %w", err)
		}
	} else if buffers := queued.Buffers(); buffers != nil {
		writtenInt, err := p.gconn.Writev(buffers)
		written = int64(writtenInt)
		if err != nil {
			return fmt.Errorf("gconn Writev: %w", err)
		}
	} else {
		written, err = queued.WriteTo(p.gconn)
		if err != nil {
			return fmt.Errorf("gconn WriteTo: %w", err)
		}
	}

	return nil
}

func (g *gnetProxyRuntime) handleBatchEnd(p *gnetProxyConn, processed int) error {
	if g.logger.Enabled(g.runCtx, slog.LevelDebug) {
		g.logger.Debug("gnetProxyRuntime.handleBatch", "p", p, "processed", processed)
	}

	flushErr := g.session.Flush(g.runCtx)
	if flushErr != nil {
		if err := g.handleConnErr(p, flushErr); err != nil {
			p.logger.Error("exit: handler returned error (handling flush error)", "error", err, "flushErr", flushErr)
			g.runErr = err
			return err
		} else {
			p.logger.Warn("failed to flush after handling batch", "error", flushErr)
		}
	}

	return nil
}

func (g *gnetProxyRuntime) handleConnErr(p *gnetProxyConn, err error) error {
	outErr := g.session.HandlePos(g.runCtx, nil, err)
	if outErr != nil {
		g.logger.Error("gnetProxyRuntime.handleConnErr: failed to handle connection error, shutting down", "error", outErr)
		stopErr := g.Stop(g.runCtx)
		if stopErr != nil {
			g.logger.Error("gnetProxyRuntime.handleConnErr: failed to stop runtime!", "error", stopErr)
		}
		return errors.Join(outErr, stopErr)
	}
	return nil
}

func (g *gnetProxyRuntime) handleMessage(p *gnetProxyConn, streamPos pgwire.StreamPos, msg pgwire.Msg) error {
	pos := &g.pos
	pos.StreamPos = streamPos
	pos.Msg = msg
	pos.Session = g.session
	pos.Handled = false

	err := g.session.HandlePos(g.runCtx, pos, nil)
	if err != nil {
		if !IsCleanTermination(err) {
			g.logger.Error("gnetProxyRuntime.handleMessage: failed to handle message, shutting down", "error", err)
		} else {
			g.logger.Info("stopping runtime: clean termination", "error", err)
		}
		stopErr := g.Stop(g.runCtx)
		if stopErr != nil {
			g.logger.Error("gnetProxyRuntime.handleMessage: failed to stop runtime!", "error", stopErr)
		}
		return errors.Join(err, stopErr)
	}

	return nil
}

func (g *gnetProxyRuntime) conn(role ProxyRole) **gnetProxyConn {
	if role == RoleClient {
		return &g.client
	} else {
		return &g.backend
	}
}

func (g *gnetProxyRuntime) eventLoop() gnet.EventLoop {
	if g.client != nil {
		return g.client.gconn.EventLoop()
	}
	return nil
}

// Receive from ch on loop.
func await[T any](ctx context.Context, loop gnet.EventLoop, ch <-chan T, then func(T, error) error) {
	go func() {
		var err error
		var result T
		var ok bool

		select {
		case <-ctx.Done():
			err = ctx.Err()
			// ok
		case result, ok = <-ch:
			if !ok {
				err = fmt.Errorf("channel closed")
			}
			err = ctx.Err()
		}

		var runnable gnet.RunnableFunc = func(ctx context.Context) error {
			err := then(result, err)
			if err != nil {
				slog.Default().Error("await: then() returned error", "error", err)
			}
			return err
		}

		scheduleErr := loop.Execute(ctx, runnable)
		if scheduleErr != nil {
			slog.Default().Error("await: couldn't schedule on event loop, handling on waiter goroutine", "error", scheduleErr)
			_ = runnable.Run(ctx)
		}
	}()
}

type LimitedWriter struct {
	Written   int
	Remaining int
	Writer    io.Writer
}

var ErrLimitedWriterExhausted = errors.New("limited writer exhausted")

func (w *LimitedWriter) Write(b []byte) (int, error) {
	if len(b) == 0 {
		return 0, nil
	}

	if w.Remaining == 0 {
		return 0, ErrLimitedWriterExhausted
	}

	slice := b[:min(len(b), w.Remaining)]
	n, err := w.Writer.Write(slice)
	w.Remaining -= n
	w.Written += n
	if err != nil {
		return n, err
	}

	if len(slice) < len(b) {
		return n, ErrLimitedWriterExhausted
	}

	return n, nil
}

var _ Runtime = (*gnetProxyRuntime)(nil)

type gnetProxyConn struct {
	// static
	role    ProxyRole
	runtime *gnetProxyRuntime
	logger  *slog.Logger
	onClose func()

	// state
	started        atomic.Bool
	gconn          gnet.Conn
	promisedWrites bytes.Buffer
	closed         atomic.Bool

	streamingLargeMessageWriter *LimitedWriter
	offset                      int64
	seq                         int64
}

type filedupConn interface {
	File() (f *os.File, err error)
}

var _ filedupConn = (*net.TCPConn)(nil)
var _ gnetTrafficHandler = (*gnetProxyConn)(nil)

func (p *gnetProxyConn) OnOpen(c gnet.Conn) (action gnet.Action) {
	if p.closed.Load() {
		p.logger.Debug("gnetProxyConn.OnOpen: already closed, ignoring")
		return gnet.Close
	}

	p.gconn = c
	p.logger.Debug("gconn opened", "promisedWrites", p.promisedWrites.Len())
	if p.promisedWrites.Len() > 0 {
		_, err := p.promisedWrites.WriteTo(c)
		if err != nil {
			p.logger.Error("gnetProxyConn.OnOpen: exit: failed to write promised writes", "error", err)
			return gnet.Close
		}
	}

	return gnet.None
}

func (p *gnetProxyConn) OnClose(c gnet.Conn) (action gnet.Action) {
	p.logger.Info("gnetProxyConn.OnClose", "c", c)
	if err := p.Close(); err != nil {
		p.logger.Error("gnetProxyConn.OnClose: failed to close", "error", err)
		return gnet.Close
	}
	return gnet.None
}

func (p *gnetProxyConn) OnTraffic(gconn gnet.Conn) (action gnet.Action) {
	// TODO: panic/recover?

	if p.closed.Load() {
		p.logger.Debug("gnetProxyConn.OnOpen: already closed, ignoring")
		return gnet.Close
	}

	if p.gconn != nil && p.gconn != gconn {
		p.logger.Error("gnetProxyConn.OnTraffic: invalid gconn", "gconn", gconn, "p.gconn", p.gconn)
		return gnet.Close
	}

	if err := p.handleTraffic(gconn); err != nil {
		if !IsCleanTermination(err) {
			p.logger.Error("gnetProxyConn.OnTraffic: failed to handle traffic", "error", err)
		}
		return gnet.Close
	}

	if p.runtime.runErr != nil {
		p.logger.Error("gnetProxyConn.OnTraffic: detected runtime error, closing connection", "error", p.runtime.runErr)
		return gnet.Close
	}

	return gnet.None
}

func (p *gnetProxyConn) handleTraffic(c gnet.Conn) (err error) {
	if !p.runtime.running.Load() {
		p.logger.Debug("not running, ignoring traffic")
		return nil
	}

	if p.runtime.runCtx.Err() != nil {
		return p.runtime.runCtx.Err()
	}

	max := c.InboundBuffered()
	if max == 0 {
		p.logger.Debug("no inbound buffered, ignoring traffic")
		return nil
	}

	// Buf either begins with a message header, or we are streaming a large message.
	buf, err := c.Next(max)
	if err != nil {
		return fmt.Errorf("failed to read next %v bytes: %w", max, err)
	}

	var processed int
	startSeq := p.seq
	DEBUG := p.debugEnabled()
	defer func() {
		discarded, discardErr := c.Discard(processed)
		err = errors.Join(err, discardErr)
		if discarded != processed {
			p.logger.Error("Boundary issue: discarded %v bytes, but wrote %v bytes", discarded, processed)
		}
		startOffset := p.offset
		p.offset += int64(discarded)
		if DEBUG {
			p.logger.Debug("progress", "processed", processed, "startSeq", startSeq, "endSeq", p.seq, "startOffset", startOffset, "endOffset", p.offset)
		}
	}()

	if p.streamingLargeMessageWriter != nil {
		processed, err = p.streamingLargeMessageWriter.Write(buf)
		if err == ErrLimitedWriterExhausted || p.streamingLargeMessageWriter.Remaining == 0 {
			p.logger.Debug("finished streaming large message", "seq", p.seq, "endOffset", p.offset)
			p.streamingLargeMessageWriter = nil
			p.seq++
			buf = buf[processed:]
		} else if err != nil {
			return fmt.Errorf("large message writer: %w", err)
		} else {
			if DEBUG {
				p.logger.Debug("streaming large message", "seq", p.seq, "currentOffset", p.offset)
			}
			return nil
		}
	}

	for len(buf) >= 5 {
		msgLen, ok := pgwire.MsgRequiredLen(buf)
		if !ok {
			// Should never happen (>= 5 bytes).
			return fmt.Errorf("MsgRequiredLen failed to determine msg len, even though we have at least 5 bytes")
		}

		// Buffer ends with incomplete message.
		if len(buf) < msgLen {
			if DEBUG {
				p.logger.Debug("buffer ends mid-message", "msgLen", msgLen, "bufLen", len(buf))
			}

			if msgLen > gnetMaxMessageSize {
				// Start streaming large message.
				// We initially set destination to nil, so Write will panic rather than
				// silently discarding bytes.
				//
				// If the session informes us it wishes to stream to a different
				// destination, we will update the writer as long as nothing was written.
				//
				// Once `handleTraffic` returns, the session must have already chosen a destination,
				// otherwise we'll end up with part of the message discarded.
				p.streamingLargeMessageWriter = &LimitedWriter{
					Remaining: msgLen,
					Writer:    nil, // Write will panic unless destination is set.
				}
				// Note: below, we send the incomplete message to the application so it
				// can decide how to handle it.
			} else {
				// Small message: wait for message to buffer.
				break
			}
		}

		// We impliclty handle large messages by streaming them; they are detected [Msg.IsIncomplete].
		offset := p.offset + int64(processed)
		seq := p.seq
		data := buf[:min(len(buf), msgLen)]
		err = p.runtime.handleMessage(p, pgwire.StreamPos{Seq: seq, Offset: offset}, pgwire.Msg{Sender: p.role, Data: data})
		// Didn't panic? Update state.
		if msgLen > gnetMaxMessageSize {
			// Decrement remaining bytes by the length we revealed to the application.
			p.streamingLargeMessageWriter.Remaining -= len(data)
		}
		processed += len(data)
		p.seq++
		buf = buf[len(data):]
		if err != nil {
			break
		}
	}

	// TODO: somehow, arrange to handle streaming requests
	// We may need to store the original `buf` ?

	if processed > 0 {
		// Do we need to put this in defer?
		return errors.Join(err, p.runtime.handleBatchEnd(p, processed))
	}

	return nil
}

func (p *gnetProxyConn) Close() error {
	if p.gconn != nil {
		err := p.gconn.Close()
		if err != nil {
			return fmt.Errorf("gconn Close: %w", err)
		}
	}
	if p.onClose != nil {
		p.onClose()
	}
	p.closed.Store(true)
	return nil
}

func (p *gnetProxyConn) debugEnabled() bool {
	if p.runtime != nil && p.runtime.session != nil {
		return p.runtime.session.DebugEnabled(p.runtime.runCtx)
	}
	return p.logger.Enabled(context.Background(), slog.LevelDebug)
}
