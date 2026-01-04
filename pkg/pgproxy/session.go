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

	"github.com/jackc/pgerrcode"
	"github.com/justjake/pglink/pkg/pgwire"
)

var ErrSessionClosed = errors.New("session closed")
var ErrBackendNotAcquired = errors.New("backend not acquired")
var noHealthCheckChan = make(chan time.Time)

type MessageTracker interface {
	TrackMessage(ctx context.Context, msg pgwire.Message) (context.Context, error)
}

type Conn interface {
	AcquireNetConn(ctx context.Context) (net.Conn, error)
	ReleaseNetConn() error
	Terminate(ctx context.Context, err error) error
	MessageTrackers() []MessageTracker
	fmt.Stringer
}

type Frontend interface {
	Conn
}

type Backend interface {
	Conn
	Release()
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
	backendTrackers   []MessageTracker
	bwq               pgwire.WriteQueue

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

		var errs []error
		if err := s.ReleaseBackend(ctx); err != nil {
			errs = append(errs, err)
		}

		if err := s.releaseClient(ctx); err != nil {
			errs = append(errs, err)
			return
		}

		res = errors.Join(errs...)
	})

	return res
}

// FlushDest flushes pending writes to the given destination.
func (s *Session) FlushDest(ctx context.Context, dest ProxyRole) error {
	if closedErr := s.alreadyClosedError("flush"); closedErr != nil {
		return closedErr
	}
	return s.flush(ctx, dest)
}

// Flush flushes pending writes to the client and backend.
func (s *Session) Flush(ctx context.Context) error {
	if closedErr := s.alreadyClosedError("flush"); closedErr != nil {
		return closedErr
	}

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
	return s.FlushDest(ctx, RoleServer)
}

// FlushClient flushes pending writes to the client.
func (s *Session) FlushClient(ctx context.Context) error {
	return s.FlushDest(ctx, RoleClient)
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
	return nil
}

// Backend returns the currently acquired backend, or nil if no backend is acquired.
func (s *Session) Backend() Backend {
	if closedErr := s.alreadyClosedError("use backend"); closedErr != nil {
		panic(closedErr)
	}
	return s.backendConn
}

// AcquireBackend acquires a backend from the configured [SessionConfig.AcquireBackend] function.
// The provided `ctx` is passed to [SessionConfig.AcquireBackend] and is
// expected to specify the deadline for acquiring the backend.
//
// If a backend is already acquired, it is returned immediately.
func (s *Session) AcquireBackend(ctx context.Context) (Backend, error) {
	if closedErr := s.alreadyClosedError("acquire backend"); closedErr != nil {
		return nil, fmt.Errorf("%w: %w", ErrBackendNotAcquired, closedErr)
	}
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
	var trackers []MessageTracker
	if s.cfg.MakeBackendTrackers != nil {
		trackers, err = s.cfg.MakeBackendTrackers(ctx, backend)
		if err != nil {
			return nil, fmt.Errorf("%w: MakeBackendTrackers: %w", ErrBackendNotAcquired, err)
		}
	}

	s.backendConn = backend
	s.backendNetConn = netConn
	s.backendRingBuffer = s.newRingBuffer(ctx, netConn)
	s.backendCursor = pgwire.NewServerCursor(s.backendRingBuffer)
	s.backendTrackers = trackers
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
	s.backendTrackers = nil
	s.logger = nil // refresh

	return nil
}

// QueueSendBytes queues writing bytes to dest.
func (s *Session) QueueSendBytes(dest ProxyRole, bytes []byte) error {
	// TODO: track write
	if closedErr := s.alreadyClosedError("send"); closedErr != nil {
		return closedErr
	}
	_, err := s.writeQueue(dest).Write(bytes)
	return err
}

// QueueSend queues a message to be sent to its destination.
// Messages from the backend are queued to the client.
// Messages from the client are queued to the backend. During flush, backend will be acquired if needed.
func (s *Session) QueueSend(ctx context.Context, msg pgwire.Message) error {
	// TODO: track write
	if closedErr := s.alreadyClosedError("send"); closedErr != nil {
		return closedErr
	}
	if dest(msg) == RoleServer {
		if _, err := s.AcquireBackend(ctx); err != nil {
			return fmt.Errorf("queue send: %w", err)
		}
	}
	if _, err := s.trackMessage(ctx, dest(msg), msg); err != nil {
		return err
	}
	return s.writeQueue(dest(msg)).WriteMsg(msg)
}

func (s *Session) QueueSendPos(ctx context.Context, pos Pos) error {
	// TODO: track write
	if closedErr := s.alreadyClosedError("send"); closedErr != nil {
		return closedErr
	}
	unwrapped := pos.unwrap()
	to := unwrapped.From().To()
	if to == RoleServer {
		if _, err := s.AcquireBackend(ctx); err != nil {
			return fmt.Errorf("queue pos: %w", err)
		}
	}
	if _, err := s.trackPos(ctx, to, unwrapped); err != nil {
		return err
	}
	return s.writeQueue(to).WriteRingMsg(unwrapped.RingMsg)
}

// TerminateClient sends terminationMessage to the client, flushes pending writes, and terminates the client connection.
// We may still have pending client messages in the ring buffer.
func (s *Session) TerminateClient(ctx context.Context, terminationMessage *pgwire.Err) error {
	logger := s.Logger().WithGroup("terminate").With("endpoint", RoleClient, "message", terminationMessage)
	if closedErr := s.alreadyClosedError("terminate client"); closedErr != nil {
		logger.Warn("cannot terminate client: session already closed", "err", closedErr)
		return closedErr
	}

	if err := s.QueueSend(ctx, terminationMessage.ToMessage()); err != nil {
		logger.Warn("ignored error sending termination message", "err", err)
	}

	if flushErr := s.flush(ctx, RoleClient); flushErr != nil {
		logger.Warn("ignored flush error, may not have received all messages", "err", flushErr)
	}

	if termErr := s.clientConn.Terminate(ctx, terminationMessage); termErr != nil {
		logger.Error("failed to terminate", "err", termErr)
		return fmt.Errorf("failed to terminate client: %w: %w", termErr, terminationMessage)
	}
	logger.Info("terminated")
	return nil
}

// TerminateBackend flushes pending writes to the backend and terminates the backend connection.
// The backend is released after termination.
func (s *Session) TerminateBackend(ctx context.Context, cause error) error {
	logger := s.Logger().WithGroup("terminate").With("endpoint", RoleServer, "cause", cause)
	if closedErr := s.alreadyClosedError("terminate backend"); closedErr != nil {
		logger.Warn("cannot terminate backend: session already closed", "err", closedErr)
		return closedErr
	}
	if s.backendConn == nil {
		return fmt.Errorf("failed to terminate backend: %w: %w", ErrBackendNotAcquired, cause)
	}

	if flushErr := s.flush(ctx, RoleServer); flushErr != nil {
		s.bwq.Clear() // Do not retry during ReleaseBackend.
		logger.Warn("ignored flush error, may not have received all messages", "err", flushErr)
	}

	if termErr := s.backendConn.Terminate(ctx, cause); termErr != nil {
		logger.Error("failed to terminate", "err", termErr)
		return fmt.Errorf("failed to terminate backend: %w: %w", termErr, cause)
	}

	if releaseErr := s.ReleaseBackend(ctx); releaseErr != nil {
		logger.Error("failed to release backend", "err", releaseErr)
		return fmt.Errorf("failed to release backend: %w: %w", releaseErr, cause)
	}

	logger.Info("terminated")
	return nil
}

func (s *Session) TerminateBoth(ctx context.Context, terminationMessage *pgwire.Err) error {
	clientErr := s.TerminateClient(ctx, terminationMessage)
	backendErr := s.TerminateBackend(ctx, terminationMessage)
	return errors.Join(clientErr, backendErr)
}

func (s *Session) TerminateBothUnexpectedError(ctx context.Context, cause error) error {
	var pgErr *pgwire.Err
	if !errors.As(cause, &pgErr) {
		pgErr = pgwire.NewErr(pgwire.ErrorPanic, pgerrcode.InternalError, "unexpected error", cause)
	}
	return s.TerminateBoth(ctx, pgErr)
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

func (s *Session) getCursor(from ProxyRole) *pgwire.Cursor {
	if from == RoleServer {
		return s.backendCursor
	} else {
		return s.clientCursor
	}
}

func (s *Session) resetClientPos() *pos {
	s.clientPos.reset(s.clientCursor, RoleClient)
	return &s.clientPos
}

func (s *Session) resetBackendPos() *pos {
	s.backendPos.reset(s.backendCursor, RoleServer)
	return &s.backendPos
}

func (s *Session) alreadyClosedError(operation string) error {
	if s.closed {
		return fmt.Errorf("cannot %s: %w", operation, ErrSessionClosed)
	}
	return nil
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
			case <-s.backendReadyChan():
			case <-s.clientCursor.Done():
				yield(RoleClient, s.clientCursor.Err())
				return
			case <-s.backendDoneChan():
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

func (s *Session) Trackers(role ProxyRole) iter.Seq[MessageTracker] {
	return func(yield func(MessageTracker) bool) {
		var configTrackers []MessageTracker
		if role == RoleClient {
			configTrackers = s.cfg.FrontendTrackers
		} else {
			configTrackers = s.backendTrackers
		}
		for _, tracker := range configTrackers {
			if !yield(tracker) {
				return
			}
		}

		if conn := s.conn(role); conn != nil {
			for _, tracker := range conn.MessageTrackers() {
				if !yield(tracker) {
					return
				}
			}
		}
	}
}

func (s *Session) trackPos(rootCtx context.Context, role ProxyRole, pos *pos) (context.Context, error) {
	msg, err := pos.AsMessage()
	if err != nil {
		return rootCtx, fmt.Errorf("track %s: %v: %w", role, pos, err)
	}

	return s.trackMessage(rootCtx, role, msg)
}

func (s *Session) trackMessage(rootCtx context.Context, role ProxyRole, msg pgwire.Message) (context.Context, error) {
	var errs []error
	ctx := rootCtx
	for tracker := range s.Trackers(role) {
		nextCtx, err := tracker.TrackMessage(ctx, msg)
		if err != nil {
			errs = append(errs, err)
		} else if nextCtx != nil {
			ctx = nextCtx
		}
	}
	if len(errs) > 0 {
		return rootCtx, fmt.Errorf("track %v: %T: %w", role, msg, errors.Join(errs...))
	}
	return ctx, nil
}

func (s *Session) beforeReadPos(pos *pos) error {
	ctx, err := s.trackPos(pos.Ctx(), pos.From(), pos)
	if err != nil {
		return err
	}
	pos.ctx = ctx
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

func (s *Session) conn(dest ProxyRole) Conn {
	if dest == RoleServer {
		return s.backendConn
	} else {
		return s.clientConn
	}
}

func (s *Session) backendReadyChan() <-chan struct{} {
	if s.backendCursor == nil {
		return nil
	}
	return s.backendCursor.Ready()
}

func (s *Session) backendDoneChan() <-chan struct{} {
	if s.backendCursor == nil {
		return nil
	}
	return s.backendCursor.Done()
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
