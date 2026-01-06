package pgproxy

import (
	"context"
	"errors"
	"fmt"
	"iter"
	"log/slog"
	"slices"
	"sync"
	"time"

	"github.com/jackc/pgerrcode"
	"github.com/justjake/pglink/pkg/pgwire"
)

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
	clientConn     Frontend
	crq            pgwire.WriteQueue
	clientTrackers []MessageTracker

	backendConn     Backend
	backendTrackers []MessageTracker
	bwq             pgwire.WriteQueue

	runtime   Runtime
	cancelRun func()

	cfg       SessionConfig
	closed    bool
	closeOnce sync.Once
	logger    *slog.Logger

	// Termination tracking state
	clientTerminateTracker   FlowTracker[TerminateFlow]
	clientTerminatedByProxy  bool // Proxy called TerminateClient()
	backendTerminatedByProxy bool // Proxy called TerminateBackend()
}

func NewDefaultRuntime(ctx context.Context, session *Session) (Runtime, error) {
	return newRingBufferRuntime(ctx, session, pgwire.DefaultRingBufferConfig())
}

// NewSession creates a new session.
// The provided `ctx` is expected to be valid for the lifetime of the session.
// However, you must always call [Session.Close] if NewSession returns successfully, do not rely on context cancellation.
func NewSession(ctx context.Context, cfg SessionConfig) (*Session, error) {
	if cfg.Frontend == nil {
		return nil, fmt.Errorf("Frontend is required")
	}
	if cfg.AcquireBackend == nil {
		return nil, fmt.Errorf("AcquireBackend is required")
	}
	if cfg.NewRuntime == nil {
		cfg.NewRuntime = NewDefaultRuntime
	}

	var err error

	session := &Session{
		cfg:                    cfg,
		clientConn:             cfg.Frontend,
		clientTerminateTracker: NewTerminateFlowTracker(),
	}

	var clientTrackers []MessageTracker
	if cfg.FrontendTrackers != nil {
		clientTrackers = slices.Clone(cfg.FrontendTrackers)
	}
	clientTrackers = append(clientTrackers, session.clientTerminateTracker)
	session.clientTrackers = clientTrackers

	session.runtime, err = cfg.NewRuntime(ctx, session)
	if err != nil {
		return nil, fmt.Errorf("failed to create runtime: %w", err)
	}

	return session, nil
}

// Run executes the session.
// The handler is called for each message position and any errors.
// Run returns when the session ends (EOF, error, or handler returns error).
//
// Run will panic if called while already streaming.
func (s *Session) Run(ctx context.Context) error {
	if s.cancelRun != nil {
		panic("already running")
	}
	runCtx, runCancel := context.WithCancel(ctx)
	s.cancelRun = runCancel
	defer runCancel()

	if err := s.runtime.StartConn(runCtx, RoleClient, s.clientConn); err != nil {
		return fmt.Errorf("failed to start client connection: %w", err)
	}
	if err := s.runtime.Run(runCtx); err != nil {
		return fmt.Errorf("failed to start runtime: %w", err)
	}

	return nil
}

// HandlePos handles a message position and any errors.
// This method is called by the session's [Runtime].
func (s *Session) HandlePos(ctx context.Context, pos Pos, posErr error) error {
	trackCtx, trackErr := s.trackPos(ctx, pos.From(), pos.unwrap())
	if trackErr != nil {
		pos.Logger().Error("failed to track message before handler", "err", trackErr)
		if handlerErr := s.HandlePos(ctx, pos, trackErr); handlerErr != nil {
			return handlerErr
		}
	}
	if trackCtx != nil {
		ctx = trackCtx
	}

	pos.unwrap().ctx = ctx
	defer func() {
		pos.unwrap().ctx = nil
	}()

	var handleErr error
	logger := pos.Logger()
	if logger.Enabled(ctx, slog.LevelDebug) {
		logger.Debug("calling handler")
	}

	if handleErr = s.cfg.Handler(ctx, s, pos, posErr); handleErr != nil {
		pos.Logger().Error("handler returned error", "err", handleErr)
		return handleErr
	}

	if notHandledErr := pos.unwrap().notHandledError(); notHandledErr != nil {
		pos.Logger().Error("poxy message not handled, exiting")
		return notHandledErr
	}

	return nil
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

		if s.cancelRun != nil {
			s.cancelRun()
			s.cancelRun = nil
		}

		if err := s.runtime.Stop(ctx); err != nil {
			res = fmt.Errorf("%w: stop: %w", ErrRuntime, err)
		}

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

	if err := s.runtime.WriteConn(ctx, dest, writeQueue); err != nil {
		return fmt.Errorf("flush %s: %w: %w", dest, ErrRuntime, err)
	}
	writeQueue.Clear()
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

	var success bool
	defer func() {
		if !success {
			backend.Release()
		}
	}()

	var trackers []MessageTracker
	if s.cfg.NewBackendTrackers != nil {
		trackers, err = s.cfg.NewBackendTrackers(ctx, backend)
		if err != nil {
			return nil, fmt.Errorf("%w: MakeBackendTrackers: %w", ErrBackendNotAcquired, err)
		}
	}

	if err := s.runtime.StartConn(ctx, RoleServer, backend); err != nil {
		return nil, fmt.Errorf("%w: %w: %w", ErrBackendNotAcquired, ErrRuntime, err)
	}

	success = true
	s.backendTrackers = trackers
	s.backendConn = backend
	s.logger = nil // refresh

	return backend, nil
}

func (s *Session) ReleaseBackend(ctx context.Context) error {
	if s.backendConn == nil {
		return nil
	}

	if err := s.Flush(ctx); err != nil {
		return fmt.Errorf("failed to release backend: %w", err)
	}

	if err := s.runtime.StopConn(ctx, RoleServer); err != nil {
		return fmt.Errorf("failed to release backend: %w: %w", ErrRuntime, err)
	}

	s.backendConn.Release()
	s.backendConn = nil
	s.backendTrackers = nil
	s.logger = nil // refresh

	return nil
}

// QueueSend queues a message to be sent to its destination.
// Messages from the backend are queued to the client.
// Messages from the client are queued to the backend. During flush, backend will be acquired if needed.
func (s *Session) QueueSend(ctx context.Context, msg pgwire.Message) error {
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
	// Mark as proxy-initiated before any termination logic
	s.clientTerminatedByProxy = true

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
	// Mark as proxy-initiated before any termination logic
	s.backendTerminatedByProxy = true

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
		return fmt.Errorf("failed to release client: %w", err)
	}

	if err := s.runtime.StopConn(ctx, RoleClient); err != nil {
		return fmt.Errorf("failed to release client: %w: %w", ErrRuntime, err)
	}

	return nil
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

func (s *Session) HealthCheckPeriod() time.Duration {
	if s.cfg.HealthCheckPeriod == 0 {
		return time.Second
	}
	return s.cfg.HealthCheckPeriod
}

func (s *Session) Trackers(role ProxyRole) iter.Seq[MessageTracker] {
	return func(yield func(MessageTracker) bool) {
		var configTrackers []MessageTracker
		if role == RoleClient {
			configTrackers = s.clientTrackers
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

func (s *Session) ClientTerminateTracker() FlowTracker[TerminateFlow] {
	return s.clientTerminateTracker
}

func (s *Session) beforeReadPos(ctx context.Context, unwrapped *pos) (context.Context, error) {
	posCtx, err := s.trackPos(ctx, unwrapped.From(), unwrapped)
	if err != nil {
		return ctx, err
	}
	if posCtx != nil {
		ctx = posCtx
	}
	unwrapped.ctx = ctx

	return ctx, nil
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

func (s *Session) conn(dest ProxyRole) Conn {
	if dest == RoleServer {
		return s.backendConn
	} else {
		return s.clientConn
	}
}
