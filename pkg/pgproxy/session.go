package pgproxy

import (
	"context"
	"errors"
	"fmt"
	"io"
	"iter"
	"log/slog"
	"net"
	"sync"
	"time"

	"github.com/justjake/pglink/pkg/pgwire"
)

var ErrSessionClosed = errors.New("session closed")
var ErrBackendNotAcquired = errors.New("backend not acquired")

// ErrTornClientConnection indicates the client connection was closed with
// incomplete data in the ring buffer - a partial message was being received.
// This typically means the client crashed or the network connection was severed.
var ErrTornClientConnection = errors.New("torn client connection: incomplete message in buffer")

// ErrTornBackendConnection indicates the backend connection was closed with
// incomplete data in the ring buffer.
var ErrTornBackendConnection = errors.New("torn backend connection: incomplete message in buffer")

// IsCleanTermination returns true if err indicates a clean session termination.
// The Session iterator now handles EOF classification internally, so this mainly
// checks for torn connection errors.
func IsCleanTermination(err error) bool {
	if err == nil {
		return true
	}
	if errors.Is(err, ErrTornClientConnection) || errors.Is(err, ErrTornBackendConnection) {
		return false
	}
	if errors.Is(err, io.EOF) {
		return true // Legacy compatibility
	}
	return false
}

// classifyIOError classifies an IO error from the given side (RoleClient or RoleServer).
// For EOF errors, it checks ring buffer state to determine if termination was clean or torn.
// Returns nil for clean terminations, or ErrTornClientConnection/ErrTornBackendConnection.
// Non-EOF errors are returned unchanged.
func (s *Session) classifyIOError(from ProxyRole, err error) error {
	if !errors.Is(err, io.EOF) {
		return err // Not EOF - return as-is
	}

	// Get state for this side
	var ring *pgwire.RingBuffer
	var terminatedByProxy bool
	var terminateReceived bool
	var tornErr error
	var sideName string

	if from == RoleClient {
		ring = s.clientAdapter.ringBuffer
		terminatedByProxy = s.clientTerminatedByProxy
		terminateReceived = s.clientTerminateReceived
		tornErr = ErrTornClientConnection
		sideName = "client"
	} else {
		ring = s.backendAdapter.ringBuffer
		terminatedByProxy = s.backendTerminatedByProxy
		terminateReceived = false // Backend doesn't send Terminate
		tornErr = ErrTornBackendConnection
		sideName = "backend"
	}

	if ring == nil {
		return nil
	}

	stats := ring.Stats()
	isTorn := stats.TotalBytes > stats.PublishedBytes
	unparsedBytes := stats.TotalBytes - stats.PublishedBytes
	logger := s.Logger().With("side", sideName, "unparsed_bytes", unparsedBytes)

	if isTorn {
		if terminatedByProxy {
			if from == RoleClient {
				// Client torn + proxy closed → WARN
				logger.Warn("torn connection (proxy-initiated close)")
			} else {
				// Backend torn + proxy closed → ERROR (possible proxy bug)
				logger.Error("torn connection (proxy-initiated close, possible bug)")
			}
		} else {
			// Unknown why torn → ERROR
			logger.Error("torn connection")
		}
		return tornErr
	}

	// Clean message boundary
	if terminatedByProxy {
		return nil // Proxy initiated, clean - no log
	}
	if terminateReceived {
		return nil // Client sent Terminate - cleanest case
	}

	// Closed without Terminate at message boundary - unusual
	if from == RoleClient {
		logger.Warn("connection closed without Terminate message")
	} else {
		logger.Warn("connection closed unexpectedly")
	}
	return nil
}

var noHealthCheckChan = make(chan time.Time)

// IOMode determines how the session handles I/O between client and backend.
type IOMode int

const (
	// IOModeDefault uses a single orchestrating goroutine with background readers.
	// This is the original 3-goroutine model: 1 main + 2 background readers.
	IOModeDefault IOMode = iota

	// IOModeSplit uses 2 goroutines that each do blocking reads from their side.
	// Client goroutine: blocking read → lock → process → flush → unlock → repeat
	// Server goroutine: blocking read → lock → process → flush → unlock → repeat
	// This reduces channel overhead and goroutine switching.
	IOModeSplit
)

// MessageTracker is a pluggable mechanism for tracking state as messages are processed.
type MessageTracker interface {
	// TrackMessage tracks the message.
	// The tracker may return a modified context for tracing.
	TrackMessage(ctx context.Context, msg pgwire.Message) (context.Context, error)
}

// Conn represents a connection to a frontend (client) or backend (server).
type Conn interface {
	// AcquireNetConn takes exclusive ownership of the Conn's underlying net.Conn.
	// While acquired, Conn should not attempt to use the net.Conn.
	// It should return an error if the net.Conn is already acquired.
	AcquireNetConn(ctx context.Context) (net.Conn, error)
	// ReleaseNetConn releases the net.Conn back to the Conn.
	// It should return an error if the net.Conn is not acquired.
	ReleaseNetConn() error
	// Terminate terminates the connection.
	// The implementation may handle `err` as it sees fit, although typically the proxy already sends a termination message.
	Terminate(ctx context.Context, err error) error
	// MessageTrackers returns the trackers for messages read from or written to the connection,
	// that must be updated for the Conn's internal state to stay valid.
	MessageTrackers() []MessageTracker
	fmt.Stringer
}

// Frontend represents a connection to a client.
type Frontend interface {
	Conn
}

// Backend represents a connection to a server.
type Backend interface {
	Conn
	// Release releases the backend connection back to some underlying pool.
	// It is expected that calling backend methods after Release may panic.
	Release()
	// OutstandingRequests returns the queue of outstanding requests sent to the backend.
	// This is used to attach response handlers to requests.
	OutstandingRequests() *OutstandingRequestQueue
}

// SessionConfig configures a [Session].
type SessionConfig struct {
	// The client. Required.
	Frontend Frontend

	// FrontendTrackers are called when messages are read from or written to the frontend,
	// in addition to trackers in [Conn.Trackers].
	FrontendTrackers []MessageTracker
	// MakeBackendTrackers are called when messages are read from or written to the backend,
	// in addition to trackers in [Conn.Trackers].
	// Called when a new backend is acquired.
	MakeBackendTrackers func(ctx context.Context, backend Backend) ([]MessageTracker, error)

	// Function to acquire a [Backend] connection.
	// Should perform whatever setup is needed on a backend before it can be used for this session.
	AcquireBackend func(ctx context.Context) (Backend, error)

	// HealthCheck is called periodically while idle. Use to implement idle timeouts.
	// If not set, no health check is performed, and HealthCheckPeriod is ignored.
	HealthCheck func(ctx context.Context) error
	// HealthCheckPeriod is the period between calls to HealthCheck.
	// If not set, defaults to 1 second.
	HealthCheckPeriod time.Duration

	// If not set, defaults to slog.Default().
	Logger *slog.Logger

	// Optional: sets ring buffer size.
	pgwire.RingBufferConfig
}

// Session wraps a connected [Frontend] to proxy its messages to a [Backend] and vis versa.
//
// While the session is open, it takes ownership of the ClientConn and Backend connections.
// You must call [Session.Close] before calling send or receive methods on them directly.
//
// Not safe for use from multiple goroutines.
//
// Use [Session.Next] to iterate messages.
// See [Pos] and [Action] for how to handle messages.
type Session struct {
	*SessionState[*ringBufferAdapter]

	// TODO: otel

	healthCheckTicker *time.Ticker
	nextMsgWaitCtx    context.Context
	cancelWaitCtx     func()

	isStreaming bool
	pullNext    func() (Pos, error, bool)
	pullStop    func()

	ringCtx       context.Context
	cancelRingCtx func()

	closeOnce sync.Once
}

// NewSession creates a new session.
// The provided `ctx` is expected to be valid for the lifetime of the session.
// However, you must always call [Session.Close] if NewSession returns successfully, do not rely on context cancellation.
func NewSession(ctx context.Context, cfg SessionConfig) (*Session, error) {
	session := &Session{
		SessionState: &SessionState[*ringBufferAdapter]{
			cfg:        cfg,
			clientConn: cfg.Frontend,
		},
	}
	session.ringCtx, session.cancelRingCtx = context.WithCancel(ctx)
	state, err := NewSessionState[*ringBufferAdapter](ctx, cfg, func(ctx context.Context, role ProxyRole, conn Conn) (*ringBufferAdapter, error) {
		return newRingBufferAdapter(ctx, session, role, conn, session.Logger(), &cfg.RingBufferConfig)
	})
	if err != nil {
		return nil, err
	}
	session.SessionState = state

	return session, nil
}

// Close flushes pending writes then closes the session. It releases the
// acquired backend, and stops all concurrent reads so the client and backend
// can be re-used.
func (s *Session) Close(ctx context.Context) error {
	var res error

	// TODO: is it a good idea to only error once?
	s.closeOnce.Do(func() {
		if s.cancelWaitCtx != nil {
			s.cancelWaitCtx()
			s.cancelWaitCtx = nil
		} else {
			s.SetWaitCtx(ctx)
			s.cancelWaitCtx()
			s.cancelWaitCtx = nil
		}

		s.cancelRingCtx()

		res = s.SessionState.Close(ctx)
	})

	return res
}

func (s *Session) getCursor(from ProxyRole) *pgwire.Cursor {
	if from == RoleServer {
		return s.backendAdapter.cursor
	} else {
		return s.clientAdapter.cursor
	}
}

func (s *Session) resetClientPos() *pos {
	s.clientAdapter.pos.reset(s, s.clientAdapter.cursor, RoleClient)
	return &s.clientAdapter.pos
}

func (s *Session) resetBackendPos() *pos {
	s.backendAdapter.pos.reset(s, s.backendAdapter.cursor, RoleServer)
	return &s.backendAdapter.pos
}

func (s *Session) healthCheckChan() <-chan time.Time {
	if s.cfg.HealthCheck == nil {
		return noHealthCheckChan
	}
	if s.healthCheckTicker == nil {
		s.healthCheckTicker = time.NewTicker(s.HealthCheckPeriod())
	}
	return s.healthCheckTicker.C
}

// SetWaitCtx sets the context used for the next stream iteration.
// This method is called with the context provided to [Session.Next] or [Session.Stream].
func (s *Session) SetWaitCtx(ctx context.Context) {
	ctx, cancel := context.WithCancel(ctx)
	if s.cancelWaitCtx != nil {
		s.cancelWaitCtx()
	}
	s.cancelWaitCtx = cancel
	s.nextMsgWaitCtx = ctx
}

func (s *Session) waitCtx() context.Context {
	if s.nextMsgWaitCtx == nil {
		return context.Background()
	}
	return s.nextMsgWaitCtx
}

// Stream returns [iter.Seq2] of [Pos] and [error] for reading messages from the session.
// When `ctx` is cancelled, the iterator will exit return (nil, ctx.Err()) then end.
// See [Session.Next] for the "pull" form, which allows per-iteration cancellation.
//
// Stream will panic if re-used or called while already streaming.
func (s *Session) Stream(ctx context.Context) iter.Seq2[Pos, error] {
	used := false
	return func(yield func(Pos, error) bool) {
		if s.isStreaming {
			panic("already streaming")
		}

		if used {
			panic("proxy stream iterator cannot be re-used")
		} else {
			used = true
		}

		s.SetWaitCtx(ctx)

		s.isStreaming = true
		defer func() {
			s.isStreaming = false
		}()

		for pos, err := range s.yieldMsgs {
			if !yield(pos, err) {
				return
			}
		}
	}
}

// Next pulls the next message from either the client or backend.
// The returned [Pos] is only valid until the next call to [Session.Next].
//
// Next will panic if an iterator returned from [Session.Stream] is still running.
// After calling Next, calling [Session.Stream] will panic.
func (s *Session) Next(ctx context.Context) (Pos, error) {
	if s.pullStop == nil {
		if s.isStreaming {
			panic("already streaming")
		}
		s.pullNext, s.pullStop = iter.Pull2(s.Stream(ctx))
	}

	s.SetWaitCtx(ctx)
	pos, err, ok := s.pullNext()
	if !ok {
		s.pullStop()
		s.pullStop = nil
		s.pullNext = nil
		return nil, io.EOF
	}

	return pos, err
}

// Run executes the session.
// The handler is called for each message position and any errors.
// Run returns when the session ends (EOF, error, or handler returns error).
//
// Run will panic if called while already streaming.
func (s *Session) Run(ctx context.Context, handler func(Pos, error) error) error {
	if s.isStreaming {
		panic("already streaming")
	}

	for pos, err := range s.Stream(ctx) {
		if err := handler(pos, err); err != nil {
			return err
		}
	}
	return nil
}

func (s *Session) yieldMsgs(yield func(*pos, error) bool) {
	var flushErr error

	defer func() {
		if flushErr != nil {
			s.logger.Error("unhandled flush error", "err", flushErr)
		}
	}()

	for batchFrom, batchErr := range s.yieldBatches {
		if batchErr != nil {
			yield(nil, batchErr)
			return
		}

		cursorIter := s.iterCursor(batchFrom, &flushErr)

		for pos, err := range cursorIter {
			if !yield(pos, err) {
				return
			}
		}

		if flushErr != nil {
			yield(nil, flushErr)
			flushErr = nil
			return
		}
	}
}

func (s *Session) yieldBatches(yield func(ProxyRole, error) bool) {
	lastHealthCheckTime := time.Now()

	for {
		if s.waitCtx().Err() != nil {
			yield(RoleProxy, s.waitCtx().Err())
			return
		}

		if s.cfg.HealthCheck != nil {
			if time.Since(lastHealthCheckTime) >= s.HealthCheckPeriod() {
				lastHealthCheckTime = time.Now()
				if err := s.cfg.HealthCheck(s.waitCtx()); err != nil {
					yield(RoleProxy, err)
					return
				}
			}
		}

		gotFrontend := false
		gotBackend := false
		gotFrontend, errF := s.clientAdapter.cursor.TryNextBatch()
		if errF != nil {
			yield(RoleClient, s.classifyIOError(RoleClient, errF))
			return
		}

		if s.backendAdapter != nil {
			var err error
			gotBackend, err = s.backendAdapter.cursor.TryNextBatch()
			if err != nil {
				yield(RoleServer, s.classifyIOError(RoleServer, err))
				return
			}
		}

		// Always handle backend first: backend responds to frontend.
		// Handle responses before sending new requests from client.
		if gotBackend {
			if !yield(RoleServer, nil) {
				return
			}
		}

		if gotFrontend {
			if !yield(RoleClient, nil) {
				return
			}
		}

		// If no messages were available synchronously, wait for either side to be ready.
		if !gotFrontend && !gotBackend {
			select {
			case <-s.waitCtx().Done():
			case <-s.clientAdapter.cursor.Ready():
			case <-s.backendReadyChan():
			case <-s.clientAdapter.cursor.Done():
				yield(RoleClient, s.classifyIOError(RoleClient, s.clientAdapter.cursor.Err()))
				return
			case <-s.backendDoneChan():
				yield(RoleServer, s.classifyIOError(RoleServer, s.backendAdapter.cursor.Err()))
				return
			case <-s.healthCheckChan():
				// Continue to run health check.
			}
		}
	}
}

func (s *Session) iterCursor(from ProxyRole, flushErr *error) iter.Seq2[*pos, error] {
	return func(yield func(*pos, error) bool) {
		cursor := s.getCursor(from)
		if cursor == nil {
			return
		}

		var err error
		var pos *pos

		defer func() {
			*flushErr = s.Flush(s.waitCtx())
		}()

		// if cursor changes (like backend release/re-acquire), drop the loop (and messages)
		for s.getCursor(from) == cursor && cursor.NextMsg() {
			if from == RoleServer {
				_, err = cursor.AsServer()
				pos = s.resetBackendPos()
			} else {
				_, err = cursor.AsClient()
				pos = s.resetClientPos()
			}

			if !s.yieldSinglePos(yield, pos, err) {
				return
			}
		}

		if s.getCursor(from) != cursor {
			s.logger.Debug("cursor changed, dropping loop", "from", from, "old", cursor, "new", s.getCursor(from))
		}
	}
}

func (s *Session) yieldSinglePos(yield func(*pos, error) bool, pos *pos, err error) (loop bool) {
	if err != nil {
		return yield(pos, err)
	}

	if err := s.beforeReadPos(pos); err != nil {
		return yield(pos, err)
	}
	loop = yield(pos, nil)
	err = s.afterReadPos(pos, loop)
	if err != nil {
		if loop {
			loop = yield(pos, err)
		} else {
			pos.logger.Error("unhandled action error", "err", err)
		}
	}

	return loop
}

func (s *Session) conn(dest ProxyRole) Conn {
	if dest == RoleServer {
		return s.backendConn
	} else {
		return s.clientConn
	}
}

func (s *Session) backendReadyChan() <-chan struct{} {
	if s.backendAdapter == nil {
		return nil
	}
	return s.backendAdapter.cursor.Ready()
}

func (s *Session) backendDoneChan() <-chan struct{} {
	if s.backendAdapter == nil {
		return nil
	}
	return s.backendAdapter.cursor.Done()
}

func dest(msg pgwire.Message) ProxyRole {
	if _, ok := msg.(pgwire.ClientMessage); ok {
		return RoleServer
	} else {
		return RoleClient
	}
}

type ringBufferAdapter struct {
	conn       Conn
	netConn    net.Conn
	ringBuffer *pgwire.RingBuffer
	cursor     *pgwire.Cursor
	pos        pos
}

func newRingBufferAdapter(
	ctx context.Context,
	session *Session,
	role ProxyRole,
	conn Conn,
	logger *slog.Logger,
	ringBufferConfig *pgwire.RingBufferConfig,
) (*ringBufferAdapter, error) {
	netConn, err := conn.AcquireNetConn(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to acquire net conn: %w", err)
	}

	ring := pgwire.NewRingBuffer(*ringBufferConfig)
	if logger.Enabled(context.Background(), slog.LevelDebug) {
		ring.SetDebugLog(func(msg string, args ...any) {
			logger.Debug(msg, args...)
		})
	}
	// Set connection but don't start reader - done lazily by Run() or Stream()
	ring.SetConn(netConn)
	ring.StartNetConnReader(ctx, netConn)

	var cursor *pgwire.Cursor
	if role == RoleServer {
		cursor = pgwire.NewServerCursor(ring)
	} else {
		cursor = pgwire.NewClientCursor(ring)
	}

	adapter := &ringBufferAdapter{conn: conn, netConn: netConn, ringBuffer: ring, cursor: cursor}
	adapter.pos.reset(session, cursor, role)
	return adapter, nil
}

// WriteDeadlineSetter implements [connAdapter].
func (r *ringBufferAdapter) WriteDeadlineSetter() WriteDeadlineSetter {
	return r.netConn
}

// WriteFlusher implements [connAdapter].
func (r *ringBufferAdapter) WriteFlusher() WriteFlusher {
	return nil
}

// Close implements [connAdapter].
func (r *ringBufferAdapter) Close(ctx context.Context) error {
	if err := r.ringBuffer.StopNetConnReader(); err != nil {
		return fmt.Errorf("failed to stop ring buffer reader: %w", err)
	}

	r.ringBuffer.Close()

	if err := r.conn.ReleaseNetConn(); err != nil {
		return fmt.Errorf("failed to release net conn: %w", err)
	}

	return nil
}

var _ ConnAdapter = (*ringBufferAdapter)(nil)
