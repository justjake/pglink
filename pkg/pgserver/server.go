package pgserver

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/justjake/pglink/pkg/pgwire"
)

var _ = http.ServerContextKey

var (
	ErrTLSFailed      = errors.New("TLS failed")
	ErrAuthFailed     = errors.New("authentication failed")
	ErrStartupFailed  = errors.New("startup failed")
	ErrCancelFailed   = errors.New("cancel failed")
	ErrAcceptFailed   = errors.New("accept failed")
	ErrValidateFailed = errors.New("validate failed")
	ErrCloseFailed    = errors.New("close failed")
	ErrServerClosed   = errors.New("pgserver closed")
)

type UnauthorizedConn struct {
	// Conn is the underlying connection.
	// It's often a [*tls.Conn] wrapping a [net.TCPConn].
	Conn           net.Conn
	Frontend       *pgproto3.Frontend
	StartupMessage *pgproto3.StartupMessage
}

type AuthorizedConn struct {
	Conn           net.Conn
	Frontend       *pgproto3.Frontend
	User           string
	Database       string
	StartupMessage *pgproto3.StartupMessage
}

type CancelConn struct {
	Conn          net.Conn
	Frontend      *pgproto3.Frontend
	CancelMessage *pgproto3.CancelRequest
}

// This is the type used by [pgproto3.BackendKeyData].
type ProcessID uint32

// This is the type used by [pgproto3.BackendKeyData].
// However, the PostgreSQL wire protocol allows for up to 256 bytes.
// TODO: support longer secret keys.
type SecretKey uint32

type Conn struct {
	Conn              net.Conn
	Frontend          *pgproto3.Frontend
	User              string
	Database          string
	ProcessID         ProcessID
	SecretKey         SecretKey
	StartupParameters pgwire.ParameterStatuses
}

type ConnValidator func(ctx context.Context, conn net.Conn) (net.Conn, error)
type AuthHandler func(ctx context.Context, conn UnauthorizedConn) (AuthorizedConn, error)
type CancelHandler func(ctx context.Context, conn CancelConn) error
type StartupHandler func(ctx context.Context, conn AuthorizedConn) (*Conn, error)
type ConnHandler func(ctx context.Context, conn *Conn) error

type contextKey struct{ name string }

func (k *contextKey) String() string { return "pgserver context value " + k.name }

var ServerContextKey = &contextKey{"server"}

type ServerConfig struct {
	// How the listener accepts underlying connections.
	Addr string
	// If provided, clients may upgrade to TLS.
	TLSConfig *tls.Config
	// If true, clients may choose to communicate over plaintext when TLSConfig is
	// non-nil.
	//
	// By default, clients are rejected if they do not upgrade to TLS before
	// starting authentication.
	TLSOptional bool
	// If true, disables fast-startup of TLS.
	//
	// By default, when TLSConfig is non-nil, the the listener will recognize
	// connections which immediately begin SSL negotiation without any previous
	// SSLRequest packets. Once the SSL connection is established the server will
	// expect a normal startup-request packet and continue negotiation over the
	// encrypted channel
	TLSFastStartDisabled bool

	// BaseContext optionally specifies a function that returns
	// the base context for incoming requests on this server.
	// The provided Listener is the specific Listener that's
	// about to start accepting requests.
	// If BaseContext is nil, the default is context.Background().
	// If non-nil, it must return a non-nil context.
	BaseContext func(net.Listener) context.Context

	//	ConnContext optionally specifies a function that modifies
	// the context used for a new connection c. The provided ctx
	// is derived from the base context and has a ServerContextKey
	// value.
	//
	// ConnContext may return nil, error to reject the connection.
	ConnContext func(ctx context.Context, conn net.Conn) (context.Context, error)

	// Required. Authorizes the connection by communicating with the client.
	// On error, the connection is closed.
	// On success, passes the authorized connection to the [StartupHandler].
	AuthHandler AuthHandler

	// Required. Handles cancellation request connections.
	// On error, the connection is closed.
	CancelHandler CancelHandler

	// Required. Handles the startup of the connection after authorization.
	// On error, the connection is closed.
	// On success, emits startup messages to the client before calling [Handler].
	StartupHandler StartupHandler

	// Required. Handles the connection after startup.
	// Connections run in their own service goroutine.
	Handler ConnHandler

	// Log errors
	Logger *slog.Logger
}

type Server struct {
	ServerConfig
	ConnMap *ConnMap
	serverTrackers
}

func (s *Server) Serve(l net.Listener) error {
	origListener := l
	l = &onceCloseListener{Listener: l}
	defer l.Close()

	if !s.trackListener(&l, true) {
		return ErrServerClosed
	}
	defer s.trackListener(&l, false)

	baseCtx := context.Background()
	if s.BaseContext != nil {
		baseCtx = s.BaseContext(origListener)
		if baseCtx == nil {
			panic("BaseContext returned a nil context")
		}
	}
	ctx := context.WithValue(baseCtx, ServerContextKey, s)
	logger := s.Logger.WithGroup("pgserver").With("addr", l.Addr())

	var tempDelay time.Duration // how long to sleep on accept failure
	updateTempDelay := func() time.Duration {
		if tempDelay == 0 {
			tempDelay = 5 * time.Millisecond
		} else {
			tempDelay *= 2
		}
		if max := 1 * time.Second; tempDelay > max {
			tempDelay = max
		}
		return tempDelay
	}

	for {
		rawConn, err := l.Accept()
		if err != nil {
			if s.shuttingDown() {
				return ErrServerClosed
			}
			if ne, ok := err.(net.Error); ok && ne.Temporary() {
				updateTempDelay()
				logger.Error("accept error", "error", err, "retryDelay", tempDelay)
				time.Sleep(tempDelay)
				continue
			}
			return err
		}

		connLogger := logger.With("client", rawConn.RemoteAddr().String())
		connCtx := ctx
		if cc := s.ConnContext; cc != nil {
			newCtx, err := cc(ctx, rawConn)
			if err != nil {
				err = fmt.Errorf("%w: %w", ErrValidateFailed, err)
				updateTempDelay()
				connLogger.Error("rejected conn", "error", err, "retryDelay", tempDelay)
				time.Sleep(tempDelay)
				continue
			}
			if newCtx == nil {
				panic("ConnContext returned a nil context")
			}
			connCtx = newCtx
		}

		tempDelay = 0
		c := s.newConn(rawConn, connLogger)
		c.setState(connStateNew)
		go c.serve(connCtx)
	}
}

func (s *Server) handle(ctx context.Context, rawConn net.Conn) (err error) {
	defer func() {
		closeErr := rawConn.Close()
		if closeErr != nil {
			err = errors.Join(err, fmt.Errorf("%w: %w", ErrCloseFailed, closeErr))
		}
	}()

	var unauthedConn *UnauthorizedConn
	var cancelConn *CancelConn

	unauthedConn, cancelConn, err = s.encryptConn(ctx, rawConn)
	if err != nil {
		return err
	}

	if cancelConn != nil {
		if cancelErr := s.CancelHandler(ctx, *cancelConn); cancelErr != nil {
			err = errors.Join(err, fmt.Errorf("%w: %w", ErrCancelFailed, cancelErr))
		}
		return
	}

	var authConn AuthorizedConn
	authConn, err = s.AuthHandler(ctx, *unauthedConn)
	if err != nil {
		err = errors.Join(err, fmt.Errorf("%w: %w", ErrAuthFailed, err))
		return
	}

	var startedConn *Conn
	startedConn, err = s.StartupHandler(ctx, authConn)
	if err != nil {
		err = errors.Join(err, fmt.Errorf("%w: %w", ErrStartupFailed, err))
		return
	}

	var key ConnKey
	key, err = s.ConnMap.Add(startedConn)
	if err != nil {
		err = errors.Join(err, fmt.Errorf("%w: %w", ErrStartupFailed, err))
		return
	}
	defer s.ConnMap.Remove(key)

	err = s.Handler(ctx, startedConn)
	return
}

func (s *Server) newConn(rawConn net.Conn, logger *slog.Logger) *conn {
	return &conn{
		raw:    rawConn,
		server: s,
		logger: logger,
	}
}

func CtxServer(ctx context.Context) *Server {
	return ctx.Value(ServerContextKey).(*Server)
}

// onceCloseListener wraps a net.Listener, protecting it from
// multiple Close calls.
type onceCloseListener struct {
	net.Listener
	once     sync.Once
	closeErr error
}

func (oc *onceCloseListener) Close() error {
	oc.once.Do(oc.close)
	return oc.closeErr
}

func (oc *onceCloseListener) close() { oc.closeErr = oc.Listener.Close() }

type serverTrackers struct {
	mu            sync.Mutex
	activeConn    map[*conn]struct{}
	listeners     map[*net.Listener]struct{}
	listenerGroup sync.WaitGroup
	inShutdown    atomic.Bool
}

func (s *serverTrackers) shuttingDown() bool {
	return s.inShutdown.Load()
}

// trackListener adds or removes a net.Listener to the set of tracked
// listeners.
//
// We store a pointer to interface in the map set, in case the
// net.Listener is not comparable. This is safe because we only call
// trackListener via Serve and can track+defer untrack the same
// pointer to local variable there. We never need to compare a
// Listener from another caller.
//
// It reports whether the server is still up (not Shutdown or Closed).
func (s *serverTrackers) trackListener(ln *net.Listener, add bool) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.listeners == nil {
		s.listeners = make(map[*net.Listener]struct{})
	}
	if add {
		if s.shuttingDown() {
			return false
		}
		s.listeners[ln] = struct{}{}
		s.listenerGroup.Add(1)
	} else {
		delete(s.listeners, ln)
		s.listenerGroup.Done()
	}
	return true
}

func (s *serverTrackers) trackConn(c *conn, add bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.activeConn == nil {
		s.activeConn = make(map[*conn]struct{})
	}
	if add {
		s.activeConn[c] = struct{}{}
	} else {
		delete(s.activeConn, c)
	}
}

type conn struct {
	raw    net.Conn
	state  atomic.Uint32
	server *Server
	ready  *Conn
	logger *slog.Logger
}

func (c *conn) setState(state connState) {
	srv := c.server
	switch state {
	case connStateNew:
		srv.trackConn(c, true)
	case connStateClosed:
		srv.trackConn(c, false)
	}
	c.state.Store(uint32(state))
}

func (c *conn) serve(ctx context.Context) {
	s := c.server

	defer func() {
		if err := recover(); err != nil {
			const size = 64 << 10
			buf := make([]byte, size)
			buf = buf[:runtime.Stack(buf, false)]
			c.logger.Error("panic serving connection", "error", err, "stack", string(buf))
		}
		closeErr := c.raw.Close()
		if closeErr != nil {
			c.logger.Error("error closing connection", "error", closeErr)
		}
		c.setState(connStateClosed)
	}()

	ctx, cancelCtx := context.WithCancel(ctx)
	defer cancelCtx()

	// TODO: configure all sorts of timeouts.

	unauthedConn, cancelConn, err := c.connect(ctx)
	if err != nil {
		c.logger.Error("connect error", "error", err)
		return
	}

	if cancelConn != nil {
		if cancelErr := s.CancelHandler(ctx, *cancelConn); cancelErr != nil {
			c.logger.Error("cancel error", "error", cancelErr)
		}
		return
	}
	if unauthedConn == nil {
		panic("encryptConn returned no connection types")
	}

	authConn, err := s.AuthHandler(ctx, *unauthedConn)
	if err != nil {
		c.logger.Error("auth error", "error", err)
		return
	}

	startedConn, err := s.StartupHandler(ctx, authConn)
	if err != nil {
		c.logger.Error("startup error", "error", err)
		return
	}

	key, err := s.ConnMap.Add(startedConn)
	if err != nil {
		c.logger.Error("startup error", "error", err)
		return
	}
	defer s.ConnMap.Remove(key)

	err = s.Handler(ctx, startedConn)
	if err != nil {
		c.logger.Error("handler error", "error", err)
	}
}

func (c *conn) encryptConn(ctx context.Context) (unauthedConn *UnauthorizedConn, cancelConn *CancelConn, err error) {

}

type connState int

const (
	connStateNew connState = iota
	connStateConnected
	connStateClosed
)

var connStateName = map[connState]string{
	connStateNew:       "new",
	connStateConnected: "connected",
	connStateClosed:    "closed",
}

func (s connState) String() string {
	return connStateName[s]
}
