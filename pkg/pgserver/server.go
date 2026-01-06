package pgserver

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgerrcode"
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

// isAlreadyClosedError returns true if the error indicates the connection
// was already closed. This is expected when the handler closes the connection
// before the server's cleanup code runs.
func isAlreadyClosedError(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, net.ErrClosed) {
		return true
	}
	// Check for the specific error message from net package
	if strings.Contains(err.Error(), "use of closed network connection") {
		return true
	}
	return false
}

type UnauthorizedConn struct {
	// Conn is the underlying connection.
	// It's often a [*tls.Conn] wrapping a [net.TCPConn].
	FrontendConn
	StartupMessage *pgproto3.StartupMessage
	// TLSState is the TLS connection state, or nil if the connection is not TLS.
	TLSState *tls.ConnectionState
	// ServerTLSCertificate is the server's TLS certificate used for this connection.
	// This is needed for tls-server-end-point channel binding.
	// It will be nil if:
	// - The connection is not TLS
	// - Multiple certificates are configured without GetCertificate
	ServerTLSCertificate *x509.Certificate
	// Can be used to store arbitrary data about the connection for the caller's use.
	ExtraData any
}

type AuthorizedConn struct {
	FrontendConn
	User           string
	Database       string
	StartupMessage *pgproto3.StartupMessage
	// Can be used to store arbitrary data about the connection for the caller's use.
	ExtraData any
}

type CancelConn struct {
	FrontendConn
	CancelMessage *pgproto3.CancelRequest
}

// ClientConn represents a successfully established client connection.
type ClientConn struct {
	FrontendConn
	// Assigned by the [AuthHandler] or [StartupHandler].
	User string
	// Assigned by the [AuthHandler] or [StartupHandler].
	Database string
	// Assigned by the [StartupHandler].
	ProcessID pgwire.ProcessID
	// Assigned by the [StartupHandler].
	SecretKey pgwire.SecretKey
	// Assigned by the [StartupHandler].
	StartupParameters pgwire.ParameterStatuses
	// Can be used to store arbitrary data about the connection for the caller's use.
	ExtraData any
	// Called by [DefaultCancelHandler] if set.
	CancelHandler func(ctx context.Context, conn *ClientConn, cancel *CancelConn) error
}

// AuthHandler authenticates and authorizes the connection by communicating with the client.
//
// It should return an error if the client's indicated user is not authorized to connect to the database,
// or if the client sends invalid credentials.
//
// AuthHandlers should send the client the [pgproto3.AuthenticationOk] message to signal authentication success.
type AuthHandler func(ctx context.Context, conn *UnauthorizedConn) (*AuthorizedConn, error)

// CancelHandler handles cancellation request connections.
// In PostgreSQL, cancellations are sent on new TCP connections rather than inline as a message on an existing connection.
type CancelHandler func(ctx context.Context, conn *CancelConn) error

// StartupHandler handles remaining setup for the connection after authentication.
// The primary task is to decide a [pgwire.ProcessID] and [pgwire.SecretKey] for the connection,
// as well as the [pgwire.ParameterStatuses] startup parameters.
//
// From the PostgreSQL documentation (https://www.postgresql.org/docs/current/protocol-flow.html#PROTOCOL-FLOW-START-UP)
//
// > In this phase a backend process is being started, and the frontend is just an interested bystander. It is still possible for the startup attempt to fail (ErrorResponse) or the server to decline support for the requested minor protocol version (NegotiateProtocolVersion), but in the normal case the backend will send some ParameterStatus messages, BackendKeyData, and finally ReadyForQuery.
// >
// > During this phase the backend will attempt to apply any additional run-time parameter settings that were given in the startup message. If successful, these values become session defaults. An error causes ErrorResponse and exit.
//
// StartupHandlers do not need to explicitly send any messages to the client.
// The server will send the appropriate messages to the client based on the StartupHandler's return value.
type StartupHandler func(ctx context.Context, conn *AuthorizedConn) (*ClientConn, error)

// ConnHandler handles successfully started client connections.
// The client connection is closed once the handler returns.
type ConnHandler func(ctx context.Context, conn *ClientConn) error

type contextKey struct{ name string }

func (k *contextKey) String() string { return "pgserver context value " + k.name }

var ServerContextKey = &contextKey{"server"}

// ServerConfig is the configuration for a [Server].
type ServerConfig struct {
	// How the listener accepts underlying connections.
	// If blank, [Server.ListenAndServe] will panic.
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

	// ConnContext optionally specifies a function that modifies
	// the context used for a new connection c. The provided ctx
	// is derived from the base context and has a ServerContextKey
	// value.
	//
	// ConnContext may return nil, error to reject the connection.
	ConnContext func(ctx context.Context, conn net.Conn) (context.Context, error)

	// Required. Authenticates and authorizes the connection by communicating with
	// the client.
	//
	// On error, the connection is closed.
	// On success, passes the authorized connection to the [StartupHandler].
	//
	// [PasswordAuthenticator] provides [PasswordAuthenticator.CleartextPassword], [PasswordAuthenticator.MD5Password], and [PasswordAuthenticator.SASL] handlers.
	AuthHandler AuthHandler

	// Handles cancellation request connections.
	// If nil, [DefaultCancelHandler] is used, which calls the corresponding client connection's [ClientConn.CancelHandler] if set.
	//
	// On error, the connection is closed.
	CancelHandler CancelHandler

	// Optional. Handles the startup of the connection after authorization.
	// If nil, [DefaultStartupHandler] is used.
	//
	// On error, the connection is closed.
	// On success,  emits startup messages to the client before calling [Handler].
	StartupHandler StartupHandler

	// Required. Handles the connection after startup.
	// Connections run in their own service goroutine.
	Handler ConnHandler

	// StartupTimeout is the maximum time allowed for the entire startup
	// handshake (TLS negotiation, authentication, and startup).
	// Default: 60 seconds. Set to negative value for no timeout.
	StartupTimeout time.Duration

	// Log errors
	Logger *slog.Logger
}

// Server implements a PostgreSQL wire protocol server.
// Connections run in their own service goroutine.
// New connections can be denied before a goroutine is started by implementng [ServerConfig.ConnContext].
type Server struct {
	// It is unsafe to modify the configuration after the server is started.
	ServerConfig
	// ConnMap tracks client connections by their [pgwire.ProcessID] and [pgwire.SecretKey].
	// It contains successfully established [ClientConn]s.
	ConnMap *ConnMap
	serverTrackers
}

func NewServer(config ServerConfig) (*Server, error) {
	if config.AuthHandler == nil {
		return nil, errors.New("AuthHandler is required")
	}
	if config.CancelHandler == nil {
		config.CancelHandler = DefaultCancelHandler
	}
	if config.StartupHandler == nil {
		config.StartupHandler = DefaultStartupHandler
	}
	if config.Handler == nil {
		return nil, errors.New("handler is required")
	}
	server := &Server{ServerConfig: config, ConnMap: &ConnMap{}}

	if server.Logger == nil {
		server.Logger = slog.Default()
	}

	return server, nil
}

// ListenAndServe listens on the TCP network address s.Addr and then
// calls [Serve] to handle requests on incoming connections.
// Accepted connections are configured to enable TCP keep-alives.
//
// If s.Addr is blank, ListenAndServe will panic.
//
// ListenAndServe always returns a non-nil error. After [Server.Shutdown] or [Server.Close],
// the returned error is [ErrServerClosed].
func (s *Server) ListenAndServe() error {
	if s.shuttingDown() {
		return ErrServerClosed
	}
	addr := s.Addr
	if addr == "" {
		panic("Addr is blank")
	}
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	return s.Serve(ln)
}

func (s *Server) Serve(l net.Listener) (err error) {
	origListener := l
	l = &onceCloseListener{Listener: l}
	defer func() {
		if closeErr := l.Close(); closeErr != nil && err == nil {
			err = closeErr
		}
	}()

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
			// Retry on timeout errors with exponential backoff
			if ne, ok := err.(net.Error); ok && ne.Timeout() {
				updateTempDelay()
				logger.Error("accept error", "error", err, "retryDelay", tempDelay)
				time.Sleep(tempDelay)
				continue
			}
			return err
		}

		// Configure TCP socket options
		if tcpConn, ok := rawConn.(*net.TCPConn); ok {
			if err := tcpConn.SetNoDelay(true); err != nil {
				logger.Warn("failed to set TCP_NODELAY", "error", err)
			}
			if err := tcpConn.SetKeepAlive(true); err != nil {
				logger.Warn("failed to enable TCP keep-alive", "error", err)
			}
		}

		connLogger := logger.With("client", rawConn.RemoteAddr().String())
		connCtx := ctx
		if cc := s.ConnContext; cc != nil {
			newCtx, err := cc(ctx, rawConn)
			if err != nil {
				if closeErr := rawConn.Close(); closeErr != nil {
					connLogger.Warn("failed to close rejected connection", "error", closeErr)
				}
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

func (s *Server) newConn(rawConn net.Conn, logger *slog.Logger) *conn {
	return &conn{
		raw:    rawConn,
		server: s,
		logger: logger,
	}
}

// CtxServer returns the [Server] from the context, or nil if not found.
func CtxServer(ctx context.Context) *Server {
	if server, ok := ctx.Value(ServerContextKey).(*Server); ok {
		return server
	}
	return nil
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

// closeListeners closes all tracked listeners.
func (s *serverTrackers) closeListeners() error {
	s.mu.Lock()
	listeners := make([]net.Listener, 0, len(s.listeners))
	for ln := range s.listeners {
		listeners = append(listeners, *ln)
	}
	s.mu.Unlock()

	var lastErr error
	for _, ln := range listeners {
		if err := ln.Close(); err != nil {
			lastErr = err
		}
	}
	return lastErr
}

// Close immediately closes all active listeners.
// Active connections will finish handling their current request before being closed.
func (s *Server) Close() error {
	s.inShutdown.Store(true)
	return s.closeListeners()
}

type conn struct {
	raw      net.Conn
	conn     net.Conn
	tls      *tls.Conn
	tlsCert  *x509.Certificate // Server cert used for this connection (captured from GetCertificate)
	state    atomic.Uint32
	server   *Server
	logger   *slog.Logger
	frontend *pgproto3.Backend
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
			fmt.Println(err)
			fmt.Println(string(buf))
		}
		closeErr := c.raw.Close()
		if closeErr != nil && !isAlreadyClosedError(closeErr) {
			c.logger.Error("error closing connection", "error", closeErr)
		}
		c.setState(connStateClosed)
	}()

	ctx, cancelCtx := context.WithCancel(ctx)
	defer cancelCtx()

	// Apply startup timeout if configured
	startupCtx := ctx
	startupTimeout := c.server.StartupTimeout
	if startupTimeout == 0 {
		startupTimeout = 60 * time.Second // default
	}
	if startupTimeout > 0 {
		var cancelStartup context.CancelFunc
		startupCtx, cancelStartup = context.WithTimeout(ctx, startupTimeout)
		defer cancelStartup()
	}
	// startupTimeout < 0 means no timeout

	unauthedConn, cancelConn, err := c.connect(startupCtx)
	if err != nil {
		c.logger.Error("connect error", "error", err)
		c.sendErr(err)
		return
	}

	if cancelConn != nil {
		if cancelErr := s.CancelHandler(ctx, cancelConn); cancelErr != nil {
			c.logger.Error("cancel error", "error", cancelErr)
			c.sendErr(cancelErr)
		}
		return
	}
	if unauthedConn == nil {
		panic("encryptConn returned no connection types")
	}

	authConn, err := s.AuthHandler(startupCtx, unauthedConn)
	if err != nil {
		c.logger.Error("auth error", "error", err)
		c.sendErr(err)
		return
	}

	startedConn, err := s.StartupHandler(startupCtx, authConn)
	if err != nil {
		c.logger.Error("startup error", "error", err)
		c.sendErr(err)
		return
	}

	// Send startup completion messages to client
	if err := c.sendStartupMessages(startupCtx, startedConn); err != nil {
		c.logger.Error("startup error", "error", err)
		c.sendErr(err)
		return
	}

	key, err := s.ConnMap.Add(startedConn)
	if err != nil {
		c.logger.Error("startup error", "error", err)
		c.sendErr(err)
		return
	}
	defer s.ConnMap.Remove(key)

	err = s.Handler(ctx, startedConn)
	if err != nil {
		c.logger.Error("handler error", "error", err)
		c.sendErr(err)
	}
}

func (c *conn) connect(ctx context.Context) (unauthedConn *UnauthorizedConn, cancelConn *CancelConn, err error) {
	s := c.server
	conn := c.raw

	// Check for TLS fast-start (client begins with TLS handshake directly)
	if s.TLSConfig != nil && !s.TLSFastStartDisabled {
		var isTLS bool
		conn, isTLS, err = isTLSHandshake(conn)
		if err != nil {
			return nil, nil, fmt.Errorf("%w: peek failed: %w", ErrTLSFailed, err)
		}
		if isTLS {
			tlsConn, err := c.updateToTLS(ctx, conn)
			if err != nil {
				return nil, nil, err
			}
			conn = tlsConn
			c.logger.Debug("TLS fast-start completed")
		}
	}

	frontend := pgproto3.NewBackend(conn, conn)

loop:
	for {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return nil, nil, ctxErr
		}

		var msg pgproto3.FrontendMessage
		msg, err = frontend.ReceiveStartupMessage()
		if err != nil {
			return nil, nil, pgwire.NewProtocolViolation(fmt.Errorf("%w: reading first message: %w", ErrStartupFailed, err), nil)
		}

		if ctxErr := ctx.Err(); ctxErr != nil {
			return nil, nil, ctxErr
		}

		switch msg := msg.(type) {
		case *pgproto3.SSLRequest:
			if s.TLSConfig == nil {
				if _, err := conn.Write([]byte{'N'}); err != nil {
					return nil, nil, fmt.Errorf("writing TLS decline `N`: %w", err)
				}
			} else {
				if _, err := conn.Write([]byte{'S'}); err != nil {
					return nil, nil, fmt.Errorf("%w: wirting TLS approve `S`: %w", ErrTLSFailed, err)
				}
				tlsConn, err := c.updateToTLS(ctx, conn)
				if err != nil {
					return nil, nil, err
				}
				conn = tlsConn
				frontend = pgproto3.NewBackend(conn, conn)
			}
		case *pgproto3.CancelRequest:
			cancelConn = &CancelConn{
				FrontendConn: FrontendConn{
					Conn:     conn,
					Frontend: frontend,
					Logger:   c.logger,
				},
				CancelMessage: msg,
			}
			break loop
		case *pgproto3.StartupMessage:
			c.frontend = frontend
			// Reject plaintext connections when TLS is required.
			// c.tls is set by updateToTLS() when TLS handshake completes (via SSLRequest or fast-start).
			if s.TLSConfig != nil && !s.TLSOptional && c.tls == nil {
				err = pgwire.NewProtocolViolation(fmt.Errorf("%w: TLS required", ErrTLSFailed), pgwire.Client(msg))
				break loop
			}

			// Build TLS info for the connection
			var tlsState *tls.ConnectionState
			var serverCert *x509.Certificate
			if c.tls != nil {
				state := c.tls.ConnectionState()
				tlsState = &state

				// Use captured cert if available (from wrapped GetCertificate)
				if c.tlsCert != nil {
					serverCert = c.tlsCert
				} else if s.TLSConfig != nil && len(s.TLSConfig.Certificates) == 1 {
					// Single cert configured, we know which one was used
					cert := &s.TLSConfig.Certificates[0]
					if cert.Leaf != nil {
						serverCert = cert.Leaf
					} else if len(cert.Certificate) > 0 {
						serverCert, _ = x509.ParseCertificate(cert.Certificate[0])
					}
				}
				// If multiple certs and no GetCertificate, serverCert stays nil.
				// tls-server-end-point channel binding will fail with a clear error.
			}

			unauthedConn = &UnauthorizedConn{
				FrontendConn: FrontendConn{
					Conn:     conn,
					Frontend: frontend,
					Logger:   c.logger,
				},
				StartupMessage:       msg,
				TLSState:             tlsState,
				ServerTLSCertificate: serverCert,
			}
			break loop
		default:
			c.frontend = frontend
			err = pgwire.NewProtocolViolation(fmt.Errorf("unsupported startup message"), pgwire.Client(msg))
			break loop
		}
	}

	if err != nil {
		return nil, nil, err
	}

	c.conn = conn
	return
}

func (c *conn) updateToTLS(ctx context.Context, conn net.Conn) (*tls.Conn, error) {
	if c.tls != nil {
		return nil, fmt.Errorf("%w: TLS already established", ErrTLSFailed)
	}

	tlsConfig := c.server.TLSConfig

	// Wrap GetCertificate if user provided one, so we can capture which cert was selected.
	// This is needed for tls-server-end-point channel binding.
	if tlsConfig.GetCertificate != nil {
		tlsConfig = tlsConfig.Clone()
		originalGetCert := tlsConfig.GetCertificate
		tlsConfig.GetCertificate = func(hello *tls.ClientHelloInfo) (*tls.Certificate, error) {
			cert, err := originalGetCert(hello)
			if cert != nil && err == nil {
				if cert.Leaf != nil {
					c.tlsCert = cert.Leaf
				} else if len(cert.Certificate) > 0 {
					c.tlsCert, _ = x509.ParseCertificate(cert.Certificate[0])
				}
			}
			return cert, err
		}
	}

	tlsConn := tls.Server(conn, tlsConfig)
	if err := tlsConn.HandshakeContext(ctx); err != nil {
		return nil, fmt.Errorf("%w: handshake failed: %w", ErrTLSFailed, err)
	}
	c.logger.Debug("TLS handshake completed")
	c.tls = tlsConn
	return tlsConn, nil
}

// sendStartupMessages sends the ParameterStatus, BackendKeyData, and ReadyForQuery messages
// that complete the PostgreSQL startup sequence.
func (c *conn) sendStartupMessages(ctx context.Context, conn *ClientConn) error {
	// Send ParameterStatus messages
	for key, value := range conn.StartupParameters {
		c.frontend.Send(&pgproto3.ParameterStatus{Name: key, Value: value})
	}

	// Send BackendKeyData
	c.frontend.Send(&pgproto3.BackendKeyData{
		ProcessID: uint32(conn.ProcessID),
		SecretKey: uint32(conn.SecretKey),
	})

	// Send ReadyForQuery
	c.frontend.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})

	// Flush all messages
	if err := c.frontend.Flush(); err != nil {
		return fmt.Errorf("%w: sending startup messages: %w", ErrStartupFailed, err)
	}

	return nil
}

func (c *conn) sendErr(err error) {
	if c.frontend == nil {
		c.logger.Debug("cannot send error: no frontend")
		return
	}

	var pgErr *pgwire.Err
	if !errors.As(err, &pgErr) {
		pgErr = pgwire.NewErr(pgwire.ErrorFatal, pgerrcode.InternalError, "unexpected error", err)
	}
	c.frontend.Send(pgErr)
	if err := c.frontend.Flush(); err != nil {
		c.logger.Error("error flushing error to client", "error", err)
	}
}

// tlsRecordTypeHandshake is the TLS record type for handshake messages.
// A TLS ClientHello starts with this byte.
const tlsRecordTypeHandshake = 0x16

// peekedConn wraps a net.Conn with a peeked byte that's returned first on Read.
type peekedConn struct {
	net.Conn
	peeked byte
	used   bool
}

func (c *peekedConn) Read(b []byte) (int, error) {
	if !c.used && len(b) > 0 {
		b[0] = c.peeked
		c.used = true
		if len(b) == 1 {
			return 1, nil
		}
		n, err := c.Conn.Read(b[1:])
		return n + 1, err
	}
	return c.Conn.Read(b)
}

// isTLSHandshake peeks the first byte to check if it's a TLS ClientHello.
// Returns the (possibly wrapped) connection and whether TLS was detected.
func isTLSHandshake(conn net.Conn) (net.Conn, bool, error) {
	var buf [1]byte
	n, err := conn.Read(buf[:])
	if err != nil {
		return conn, false, err
	}
	if n == 0 {
		return conn, false, nil
	}

	// Wrap conn to "unread" the peeked byte
	wrapped := &peekedConn{Conn: conn, peeked: buf[0], used: false}

	return wrapped, buf[0] == tlsRecordTypeHandshake, nil
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
