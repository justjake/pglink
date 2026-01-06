package pgproxy

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"os"
	"sync"
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
		logger := slog.Default().WithGroup("gnet").With("e", fmt.Sprintf("%p", g))
		client, err := gnet.NewClient(g, gnet.WithReadBufferCap(gnetBufferMaxSize), gnet.WithTicker(true), gnet.WithMulticore(true), gnet.WithLogger(&gnetLogger{logger}))
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
	g.logger.Info("gnet.OnClose", "c.fd", c.Fd(), "c.remoteAddr", c.RemoteAddr(), "err", err)
	return gnet.None
}

// OnOpen implements [gnet.EventHandler].
func (g *gnetProxyEngine) OnOpen(c gnet.Conn) (out []byte, action gnet.Action) {
	g.logger.Info("gnet.OnOpen", "c.fd", c.Fd(), "c.remoteAddr", c.RemoteAddr())
	return nil, gnet.None
}

// OnShutdown implements [gnet.EventHandler].
func (g *gnetProxyEngine) OnShutdown(eng gnet.Engine) {
	g.logger.Info("gnet.OnShutdown", "eng", eng)

	// TODO: cancel all sessions we own.
}

// OnTick fires immediately after the engine starts and will fire again
// following the duration specified by the delay return value.
//
// OnTick implements [gnet.EventHandler].
func (g *gnetProxyEngine) OnTick() (delay time.Duration, action gnet.Action) {
	g.tickCount++
	g.logger.Info("gnet.OnTick", "ticks", g.tickCount)
	return gnetTickDuration, gnet.None
}

// OnTraffic implements [gnet.EventHandler].
func (g *gnetProxyEngine) OnTraffic(c gnet.Conn) (action gnet.Action) {
	g.logger.Info("gnet.OnTraffic", "c.fd", c.Fd(), "c.remoteAddr", c.RemoteAddr())
	handler, ok := c.Context().(gnetTrafficHandler)
	if !ok {
		g.logger.Error("gnet.OnTraffic: invalid context: closing conn", "context", c.Context())
		return gnet.Close
	}
	res := handler.OnTraffic(c)
	g.logger.Info("gnet.OnTraffic: action", "action", res)
	return res
}

var _ gnet.EventHandler = (*gnetProxyEngine)(nil)

var defaultGnetEngine = &gnetProxyEngine{}

type gnetProxyRuntime struct {
	// static
	session   *Session
	logger    *slog.Logger
	engine    *gnetProxyEngine
	runCtx    context.Context
	msgParser flyweightParser

	// state
	mu      sync.Mutex
	wg      sync.WaitGroup
	client  *gnetProxyConn
	backend *gnetProxyConn
	loop    gnet.EventLoop
	running bool
	runErr  error
	pos     pos
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
	if g.running {
		return fmt.Errorf("already running")
	}

	synced := func() error {
		g.mu.Lock()
		defer g.mu.Unlock()

		if g.running {
			return fmt.Errorf("already running")
		}

		if g.client == nil {
			return fmt.Errorf("client not started")
		}

		g.running = true
		g.runCtx = ctx

		return nil
	}

	if err := synced(); err != nil {
		return err
	}
	defer func() {
		g.mu.Lock()
		g.running = false
		g.mu.Unlock()
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
	proxyConn.parser = pgwire.NewStreamBatchParser(proxyConn.handleBatch)

	gconn, err := g.engine.client.EnrollContext(fileConn, proxyConn)
	if err != nil {
		return fmt.Errorf("gnet enroll: %w", err)
	}
	ok = true

	g.wg.Add(1)
	proxyConn.onClose = g.wg.Done
	proxyConn.gconn = gconn
	*connPtr = proxyConn
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
	connPtr := g.conn(role)
	if *connPtr == nil {
		return fmt.Errorf("connection not started")
	}

	// TODO: use writev
	proxyConn := *connPtr
	_, err := queued.WriteTo(proxyConn.gconn)
	if err != nil {
		return fmt.Errorf("gconn WriteTo: %w", err)
	}
	return nil
}

func (g *gnetProxyRuntime) handleBatch(p *gnetProxyConn, batch pgwire.StreamBatch) {
	g.logger.Debug("gnetProxyRuntime.handleBatch", "p", p, "batch", batch)

	for msg := range batch.Complete.All() {
		if err := g.handleMessage(p, msg); err != nil {
			return
		}
	}

	if batch.Partial != nil {
		if err := g.handleMessage(p, batch.Partial); err != nil {
			return
		}
	}
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

func (g *gnetProxyRuntime) handleMessage(p *gnetProxyConn, msg ProxyMessage) error {
	parser := &g.msgParser
	parser.Prepare(p.role)
	defer parser.Release()

	pos := &g.pos
	pos.reset(p.role, msg, g.session, g.runCtx, parser)
	defer func() {
		pos.reset(RoleProxy, nil, nil, nil, nil)
	}()

	err := g.session.HandlePos(g.runCtx, pos, nil)
	if err != nil {
		g.logger.Error("gnetProxyRuntime.handleMessage: failed to handle message, shutting down", "error", err)
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

var _ Runtime = (*gnetProxyRuntime)(nil)

type gnetProxyConn struct {
	// static
	role    ProxyRole
	runtime *gnetProxyRuntime
	logger  *slog.Logger
	onClose func()

	// state
	gconn  gnet.Conn
	parser *pgwire.StreamBatchParser
}

type filedupConn interface {
	File() (f *os.File, err error)
}

var _ filedupConn = (*net.TCPConn)(nil)
var _ gnetTrafficHandler = (*gnetProxyConn)(nil)

func (p *gnetProxyConn) OnTraffic(gconn gnet.Conn) (action gnet.Action) {
	// TODO: panic/recover?

	if p.gconn != gconn {
		p.logger.Error("gnetProxyConn.OnTraffic: invalid gconn", "gconn", fmt.Sprintf("%p", gconn), "p.gconn", fmt.Sprintf("%p", p.gconn))
		return gnet.Close
	}

	if err := p.handleTraffic(gconn); err != nil {
		p.logger.Error("gnetProxyConn.OnTraffic: failed to handle traffic", "error", err)
		return gnet.Close
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

func (p *gnetProxyConn) Close() error {
	err := p.gconn.Close()
	if err != nil {
		return fmt.Errorf("gconn Close: %w", err)
	}
	p.onClose()
	return nil
}

func (p *gnetProxyConn) handleTraffic(c gnet.Conn) error {
	max := c.InboundBuffered()
	buf, err := c.Next(max)
	if err != nil {
		return fmt.Errorf("gnetStream.OnTraffic: failed to read next %v bytes: %w", max, err)
	}

	// Parser will call p.handleBatch implicitly if necessary.
	written, err := p.parser.Write(buf)
	_, discardErr := c.Discard(written)

	if discardErr != nil {
		return errors.Join(err, fmt.Errorf("gnetProxyConn.handleTraffic: failed to discard: %w", discardErr))
	}
	if errors.Is(err, pgwire.ErrIncompleteMessage) {
		p.logger.Debug("gnetProxyConn.handleTraffic: incomplete message (waiting for more data)", "error", err)
		return nil
	} else if err != nil {
		return fmt.Errorf("gnetProxyConn.handleTraffic: failed to write to parser: %w", err)
	}

	return nil
}

func (p *gnetProxyConn) handleBatch(batch pgwire.StreamBatch) {
	// delegate
	p.runtime.handleBatch(p, batch)
}
