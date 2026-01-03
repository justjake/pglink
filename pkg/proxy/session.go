package proxy

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

var ErrBackendNotAcquired = errors.New("backend not acquired")
var noHealthCheckChan = make(chan time.Time)

// SessionConfig configures a [Session].
type SessionConfig struct {
	// The client. Required.
	Frontend Frontend

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
	clientConn       Frontend
	clientNetConn    net.Conn
	clientRingBuffer *pgwire.RingBuffer
	clientCursor     *pgwire.Cursor
	clientPos        pos
	crq              pgwire.WriteQueue

	backendConn       Backend
	backendNetConn    net.Conn
	backendRingBuffer *pgwire.RingBuffer
	backendCursor     *pgwire.Cursor
	backendPos        pos
	bwq               pgwire.WriteQueue

	// TODO: write queues
	// TODO: flush
	// TODO: trackers
	// TODO: otel

	healthCheckTicker *time.Ticker
	nextMsgWaitCtx    context.Context
	cancelWaitCtx     func()

	isStreaming bool
	pullNext    func() (Pos, error, bool)
	pullStop    func()

	ringCtx       context.Context
	cancelRingCtx func()

	cfg       SessionConfig
	closed    bool
	closeOnce sync.Once
	logger    *slog.Logger
}

func NewSession(ctx context.Context, cfg SessionConfig) (*Session, error) {
	session := &Session{cfg: cfg}

	clientNetConn, err := cfg.Frontend.AcquireNetConn(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to acquire client net conn: %w", err)
	}

	session.ringCtx, session.cancelRingCtx = context.WithCancel(ctx)
	session.clientConn = cfg.Frontend
	session.clientNetConn = clientNetConn
	session.clientRingBuffer = session.newRingBuffer(session.ringCtx, clientNetConn)
	session.clientCursor = pgwire.NewClientCursor(session.clientRingBuffer)

	return session, nil
}

// Close flushes pending writes then closes the session. It releases the
// acquired backend, and stops all concurrent reads so the client and backend
// can be re-used.
func (s *Session) Close(ctx context.Context) error {
	var res error

	// TODO: is it a good idea to only error once?
	s.closeOnce.Do(func() {
		defer func() {
			s.closed = true
		}()

		if s.cancelWaitCtx != nil {
			s.cancelWaitCtx()
			s.cancelWaitCtx = nil
		} else {
			s.SetWaitCtx(ctx)
			s.cancelWaitCtx()
			s.cancelWaitCtx = nil
		}

		s.cancelRingCtx()

		if err := s.ReleaseBackend(ctx); err != nil {
			res = fmt.Errorf("failed to release backend: %w", err)
			return
		}

		if err := s.releaseClient(ctx); err != nil {
			res = fmt.Errorf("failed to release client: %w", err)
			return
		}
	})

	return res
}

// Flush flushes pending writes to the client and backend.
func (s *Session) Flush(ctx context.Context) error {
	s.assertNotClosed()

	if err := s.flush(ctx, RoleServer); err != nil {
		return err
	}

	if err := s.flush(ctx, RoleClient); err != nil {
		return err
	}

	return nil
}

// FlushBackend flushes pending writes to the backend.
func (s *Session) FlushBackend(ctx context.Context) error {
	s.assertNotClosed()
	return s.flush(ctx, RoleServer)
}

// FlushClient flushes pending writes to the client.
func (s *Session) FlushClient(ctx context.Context) error {
	s.assertNotClosed()
	return s.flush(ctx, RoleClient)
}

func (s *Session) flush(ctx context.Context, dest ProxyRole) error {
	writeQueue := s.writeQueue(dest)
	if writeQueue.IsEmpty() {
		return nil
	}

	if dest == RoleServer {
		if _, err := s.AcquireBackend(ctx); err != nil {
			return fmt.Errorf("flush %s: %w", dest, err)
		}
	}

	netConn := s.netConn(dest)
	if netConn == nil {
		return fmt.Errorf("flush %s: %w", dest, ErrBackendNotAcquired)
	}

	if deadline, ok := ctx.Deadline(); ok {
		if err := netConn.SetWriteDeadline(deadline); err != nil {
			return fmt.Errorf("flush %s: failed to set write deadline: %w", dest, err)
		}
		defer netConn.SetWriteDeadline(time.Time{})
	}

	_, err := writeQueue.WriteTo(netConn)
	if err != nil {
		return fmt.Errorf("flush %s: %w", dest, err)
	}
	writeQueue.Clear()
	return nil
}

// Backend returns the currently acquired backend, or nil if no backend is acquired.
func (s *Session) Backend() Backend {
	s.assertNotClosed()
	return s.backendConn
}

// AcquireBackend acquires a backend from the configured [SessionConfig.AcquireBackend] function.
// The provided `ctx` is passed to [SessionConfig.AcquireBackend] and is
// expected to specify the deadline for acquiring the backend.
//
// If a backend is already acquired, it is returned immediately.
func (s *Session) AcquireBackend(ctx context.Context) (Backend, error) {
	s.assertNotClosed()
	if s.backendConn != nil {
		return s.backendConn, nil
	}

	backend, err := s.cfg.AcquireBackend(ctx)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrBackendNotAcquired, err)
	}

	netConn, err := backend.AcquireNetConn(ctx)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrBackendNotAcquired, err)
	}

	s.backendConn = backend
	s.backendNetConn = netConn
	s.backendRingBuffer = s.newRingBuffer(ctx, netConn)
	s.backendCursor = pgwire.NewServerCursor(s.backendRingBuffer)
	s.logger = nil // refresh

	return backend, nil
}

func (s *Session) ReleaseBackend(ctx context.Context) error {
	if s.backendConn == nil {
		return nil
	}

	if err := s.Flush(ctx); err != nil {
		return fmt.Errorf("failed to release backend: flush: %w", err)
	}

	if err := releaseRingBuffer(&s.backendRingBuffer); err != nil {
		return fmt.Errorf("failed to release backend: %w", err)
	}

	if err := releaseNetConn(&s.backendNetConn, s.backendConn); err != nil {
		return fmt.Errorf("failed to release backend: %w", err)
	}

	s.backendConn.Release()
	s.backendConn = nil
	s.logger = nil // refresh

	return nil
}

// QueueSendBytes queues writing bytes to dest.
func (s *Session) QueueSendBytes(dest ProxyRole, bytes []byte) error {
	// TODO: track write
	s.assertNotClosed()
	_, err := s.writeQueue(dest).Write(bytes)
	return err
}

// QueueSend queues a message to be sent to its destination.
// Messages from the backend are queued to the client.
// Messages from the client are queued to the backend. During flush, backend will be acquired if needed.
func (s *Session) QueueSend(msg pgwire.Message) error {
	// TODO: track write
	s.assertNotClosed()
	writeQueue := s.writeQueue(dest(msg))
	return writeQueue.WriteMsg(msg)
}

func (s *Session) QueueSendPos(pos Pos) error {
	// TODO: track write
	s.assertNotClosed()
	writeQueue := s.writeQueue(pos.From().Flipped())
	return writeQueue.WriteRingMsg(pos.unwrap().RingMsg)
}

// ClearQueue clears the write queue for the given destination.
func (s *Session) ClearQueue(dest ProxyRole) {
	s.writeQueue(dest).Clear()
}

func (s *Session) releaseClient(ctx context.Context) error {
	if err := s.Flush(ctx); err != nil {
		return fmt.Errorf("failed to release client: flush: %w", err)
	}

	if err := releaseRingBuffer(&s.clientRingBuffer); err != nil {
		return fmt.Errorf("failed to release client: %w", err)
	}

	if err := releaseNetConn(&s.clientNetConn, s.clientConn); err != nil {
		return fmt.Errorf("failed to release client: %w", err)
	}

	return nil
}

func (s *Session) getPos(from ProxyRole) *pos {
	if from == RoleServer {
		return s.getBackendPos()
	} else {
		return s.getClientPos()
	}
}

func (s *Session) getCursor(from ProxyRole) *pgwire.Cursor {
	if from == RoleServer {
		return s.backendCursor
	} else {
		return s.clientCursor
	}
}

func (s *Session) getClientPos() *pos {
	s.clientPos.reset(s.clientCursor, RoleClient)
	return &s.clientPos
}

func (s *Session) getBackendPos() *pos {
	s.backendPos.reset(s.backendCursor, RoleServer)
	return &s.backendPos
}

func (s *Session) assertNotClosed() {
	if s.closed {
		panic("session already closed")
	}
}

func (s *Session) Logger() *slog.Logger {
	if s.logger != nil {
		return s.logger
	}

	var logger *slog.Logger
	if s.cfg.Logger != nil {
		logger = s.cfg.Logger
	} else {
		logger = slog.Default()
	}

	logger = logger.With("client", s.clientConn.String())
	if s.backendConn != nil {
		logger = logger.With("backend", s.backendConn.String())
	}
	s.logger = logger

	return logger
}

// TODO: ensure ctx passed here has the expected lifetime: that of the Session as a whole,
// not the deadline for AcquireBackend() or ReleaseBackend().
func (s *Session) newRingBuffer(ctx context.Context, netConn net.Conn) *pgwire.RingBuffer {
	ring := pgwire.NewRingBuffer(s.cfg.RingBufferConfig)
	logger := s.Logger()
	if logger.Enabled(ctx, slog.LevelDebug) {
		ring.SetDebugLog(func(msg string, args ...any) {
			logger.Debug(msg, args...)
		})
	}
	ring.StartNetConnReader(s.ringCtx, netConn)
	return ring
}

func (s *Session) healthCheckPeriod() time.Duration {
	if s.cfg.HealthCheckPeriod == 0 {
		return time.Second
	}
	return s.cfg.HealthCheckPeriod
}

func (s *Session) healthCheckChan() <-chan time.Time {
	if s.cfg.HealthCheck == nil {
		return noHealthCheckChan
	}
	if s.healthCheckTicker == nil {
		s.healthCheckTicker = time.NewTicker(s.healthCheckPeriod())
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
			if time.Since(lastHealthCheckTime) >= s.healthCheckPeriod() {
				lastHealthCheckTime = time.Now()
				if err := s.cfg.HealthCheck(s.waitCtx()); err != nil {
					yield(RoleProxy, err)
					return
				}
			}
		}

		gotFrontend := false
		gotBackend := false
		gotFrontend, errF := s.clientCursor.TryNextBatch()
		if errF != nil {
			yield(RoleClient, errF)
			return
		}

		if s.backendCursor != nil {
			var err error
			gotBackend, err = s.backendCursor.TryNextBatch()
			if err != nil {
				yield(RoleServer, err)
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
			case <-s.clientCursor.Ready():
			case <-s.backendCursor.Ready():
			case <-s.clientCursor.Done():
				yield(RoleClient, s.clientCursor.Err())
				return
			case <-s.backendCursor.Done():
				yield(RoleServer, s.backendCursor.Err())
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
		var err error
		var pos *pos

		defer func() {
			*flushErr = s.Flush(s.waitCtx())
		}()

		for cursor.NextMsg() {
			if from == RoleServer {
				_, err = cursor.AsServer()
				pos = s.getBackendPos()
			} else {
				_, err = cursor.AsClient()
				pos = s.getClientPos()
			}

			if !s.yieldSinglePos(yield, pos, err) {
				return
			}

			if s.waitCtx().Err() != nil {
				yield(nil, s.waitCtx().Err())
				return
			}
		}
	}
}

// This kind of shit tells me we should make actions imperative too...
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

func (s *Session) beforeReadPos(pos *pos) error {
	// TODO: track read
	return nil
}

func (s *Session) afterReadPos(pos *pos, loopContinues bool) error {
	if loopContinues {
		return pos.notHandledError()
	}
	return nil
}

func (s *Session) writeQueue(dest ProxyRole) *pgwire.WriteQueue {
	if dest == RoleServer {
		return &s.bwq
	} else {
		return &s.crq
	}
}

func (s *Session) netConn(dest ProxyRole) net.Conn {
	if dest == RoleServer {
		return s.backendNetConn
	} else {
		return s.clientNetConn
	}
}
func releaseRingBuffer(ringBuffer **pgwire.RingBuffer) error {
	if ringBuffer == nil {
		return nil
	}

	if err := (*ringBuffer).StopNetConnReader(); err != nil {
		return fmt.Errorf("failed to stop ring buffer reader: %w", err)
	}
	(*ringBuffer).Close()
	*ringBuffer = nil

	return nil
}

func releaseNetConn(netConn *net.Conn, releaser interface{ ReleaseNetConn() error }) error {
	if netConn == nil {
		return nil
	}

	if err := releaser.ReleaseNetConn(); err != nil {
		return fmt.Errorf("failed to release net conn: %w", err)
	}
	*netConn = nil

	return nil
}

func dest(msg pgwire.Message) ProxyRole {
	if _, ok := msg.(pgwire.ClientMessage); ok {
		return RoleServer
	} else {
		return RoleClient
	}
}
