package pgproxy

import (
	"context"
	"errors"
	"fmt"
	"io"
	"iter"
	"log/slog"
	"sync"
	"time"

	"github.com/jackc/pgerrcode"
	"github.com/justjake/pglink/pkg/pgwire"
)

type WriteFlusher interface {
	io.Writer
	Writev(ctx context.Context, bufs [][]byte) error
	Flush(ctx context.Context) error
}

type WriteDeadlineSetter interface {
	io.Writer
	SetWriteDeadline(deadline time.Time) error
}

type ConnAdapter interface {
	WriteDeadlineSetter() WriteDeadlineSetter
	WriteFlusher() WriteFlusher
	Close(ctx context.Context) error
}

type ConnAdapterFactory[C ConnAdapter] func(ctx context.Context, role ProxyRole, conn Conn) (C, error)

type SessionState[C ConnAdapter] struct {
	clientConn    Frontend
	clientAdapter C
	crq           pgwire.WriteQueue

	backendConn     Backend
	backendAdapter  C
	backendTrackers []MessageTracker
	bwq             pgwire.WriteQueue

	acquireConnAdapter ConnAdapterFactory[C]

	cfg       SessionConfig
	closed    bool
	closeOnce sync.Once
	logger    *slog.Logger

	// Termination tracking state
	clientTerminateReceived  bool // Client sent MsgClientTerminate
	clientTerminatedByProxy  bool // Proxy called TerminateClient()
	backendTerminatedByProxy bool // Proxy called TerminateBackend()
}

// NewSession creates a new session.
// The provided `ctx` is expected to be valid for the lifetime of the session.
// However, you must always call [Session.Close] if NewSession returns successfully, do not rely on context cancellation.
func NewSessionState[C ConnAdapter](ctx context.Context, cfg SessionConfig, acquireConnAdapter ConnAdapterFactory[C]) (*SessionState[C], error) {
	session := &SessionState[C]{cfg: cfg, acquireConnAdapter: acquireConnAdapter}
	if cfg.Frontend == nil {
		return nil, fmt.Errorf("Frontend is required")
	}
	if cfg.AcquireBackend == nil {
		return nil, fmt.Errorf("AcquireBackend is required")
	}

	session.clientConn = cfg.Frontend
	var err error
	session.clientAdapter, err = session.acquireConnAdapter(ctx, RoleClient, session.clientConn)
	if err != nil {
		return nil, fmt.Errorf("failed to make client adapter: %w", err)
	}

	return session, nil
}

func (s *SessionState[C]) HandlePos(ctx context.Context, pos Pos, callback func(ctx context.Context, pos Pos) error) error {
	if err := s.beforeReadPos(pos.unwrap()); err != nil {
		return err
	}
	err := callback(ctx, pos)
	afterReadErr := s.afterReadPos(pos.unwrap(), err == nil)
	return errors.Join(err, afterReadErr)
}

// Close flushes pending writes then closes the session. It releases the
// acquired backend, and stops all concurrent reads so the client and backend
// can be re-used.
func (s *SessionState[C]) Close(ctx context.Context) error {
	var res error

	// TODO: is it a good idea to only error once?
	s.closeOnce.Do(func() {
		defer func() {
			s.closed = true
		}()

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
func (s *SessionState[C]) FlushDest(ctx context.Context, dest ProxyRole) error {
	if closedErr := s.alreadyClosedError("flush"); closedErr != nil {
		return closedErr
	}
	return s.flush(ctx, dest)
}

// Flush flushes pending writes to the client and backend.
func (s *SessionState[C]) Flush(ctx context.Context) error {
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
func (s *SessionState[C]) FlushBackend(ctx context.Context) error {
	return s.FlushDest(ctx, RoleServer)
}

// FlushClient flushes pending writes to the client.
func (s *SessionState[C]) FlushClient(ctx context.Context) error {
	return s.FlushDest(ctx, RoleClient)
}

func (s *SessionState[C]) flush(ctx context.Context, dest ProxyRole) error {
	writeQueue := s.writeQueue(dest)
	if writeQueue.IsEmpty() {
		return nil
	}

	if dest == RoleServer {
		if _, err := s.AcquireBackend(ctx); err != nil {
			return fmt.Errorf("flush %s: %w", dest, err)
		}
	}

	adapter := s.connAdapter(dest)
	if adapter == nil {
		return fmt.Errorf("flush %s: %w", dest, ErrBackendNotAcquired)
	}

	if netConn := adapter.WriteDeadlineSetter(); netConn != nil {
		if deadline, ok := ctx.Deadline(); ok {
			if err := netConn.SetWriteDeadline(deadline); err != nil {
				return fmt.Errorf("flush %s: failed to set write deadline: %w", dest, err)
			}
			defer netConn.SetWriteDeadline(time.Time{})
		}

		_, err := writeQueue.WriteTo(netConn)
		if err != nil {
			return fmt.Errorf("flush %s: WriteTo: %w", dest, err)
		}
		writeQueue.Clear()
	} else if writeFlusher := adapter.WriteFlusher(); writeFlusher != nil {
		_, err := writeQueue.WriteTo(writeFlusher)
		if err != nil {
			return fmt.Errorf("flush %s: WriteTo: %w", dest, err)
		}
		writeQueue.Clear()

		if err := writeFlusher.Flush(ctx); err != nil {
			return fmt.Errorf("flush %s: Flush: %w", dest, err)
		}
	} else {
		fmt.Errorf("flush %s: no net conn or write flusher", dest)
	}

	return nil
}

// Backend returns the currently acquired backend, or nil if no backend is acquired.
func (s *SessionState[C]) Backend() Backend {
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
func (s *SessionState[C]) AcquireBackend(ctx context.Context) (Backend, error) {
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

	connAdapter, err := s.acquireConnAdapter(ctx, RoleServer, backend)
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
	s.backendAdapter = connAdapter
	s.backendTrackers = trackers
	s.logger = nil // refresh

	return backend, nil
}

func (s *SessionState[C]) ReleaseBackend(ctx context.Context) error {
	if s.backendConn == nil {
		return nil
	}

	if err := s.Flush(ctx); err != nil {
		return fmt.Errorf("failed to release backend: flush: %w", err)
	}

	if err := releaseConnAdapter(ctx, &s.backendAdapter); err != nil {
		return fmt.Errorf("failed to release backend: %w", err)
	}

	s.backendConn.Release()
	s.backendConn = nil
	var zero C
	s.backendAdapter = zero
	s.backendTrackers = nil
	s.logger = nil // refresh

	return nil
}

// QueueSendBytes queues writing bytes to dest.
func (s *SessionState[C]) QueueSendBytes(dest ProxyRole, bytes []byte) error {
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
func (s *SessionState[C]) QueueSend(ctx context.Context, msg pgwire.Message) error {
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

func (s *SessionState[C]) QueueSendPos(ctx context.Context, pos Pos) error {
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
func (s *SessionState[C]) TerminateClient(ctx context.Context, terminationMessage *pgwire.Err) error {
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
func (s *SessionState[C]) TerminateBackend(ctx context.Context, cause error) error {
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

func (s *SessionState[C]) TerminateBoth(ctx context.Context, terminationMessage *pgwire.Err) error {
	clientErr := s.TerminateClient(ctx, terminationMessage)
	backendErr := s.TerminateBackend(ctx, terminationMessage)
	return errors.Join(clientErr, backendErr)
}

func (s *SessionState[C]) TerminateBothUnexpectedError(ctx context.Context, cause error) error {
	var pgErr *pgwire.Err
	if !errors.As(cause, &pgErr) {
		pgErr = pgwire.NewErr(pgwire.ErrorPanic, pgerrcode.InternalError, "unexpected error", cause)
	}
	return s.TerminateBoth(ctx, pgErr)
}

func (s *SessionState[C]) releaseClient(ctx context.Context) error {
	if err := s.Flush(ctx); err != nil {
		return fmt.Errorf("failed to release client: flush: %w", err)
	}

	if err := releaseConnAdapter(ctx, &s.clientAdapter); err != nil {
		return fmt.Errorf("failed to release client: %w", err)
	}

	return nil
}

func (s *SessionState[C]) alreadyClosedError(operation string) error {
	if s.closed {
		return fmt.Errorf("cannot %s: %w", operation, ErrSessionClosed)
	}
	return nil
}

func (s *SessionState[C]) Logger() *slog.Logger {
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

func (s *SessionState[C]) HealthCheckPeriod() time.Duration {
	if s.cfg.HealthCheckPeriod == 0 {
		return time.Second
	}
	return s.cfg.HealthCheckPeriod
}

func (s *SessionState[C]) Trackers(role ProxyRole) iter.Seq[MessageTracker] {
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

func (s *SessionState[C]) trackPos(rootCtx context.Context, role ProxyRole, pos *pos) (context.Context, error) {
	msg, err := pos.AsMessage()
	if err != nil {
		return rootCtx, fmt.Errorf("track %s: %v: %w", role, pos, err)
	}

	return s.trackMessage(rootCtx, role, msg)
}

func (s *SessionState[C]) trackMessage(rootCtx context.Context, role ProxyRole, msg pgwire.Message) (context.Context, error) {
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

func (s *SessionState[C]) beforeReadPos(pos *pos) error {
	ctx, err := s.trackPos(pos.Ctx(), pos.From(), pos)
	if err != nil {
		return err
	}
	pos.ctx = ctx

	// Track client Terminate message for clean termination detection
	if pos.FromClient() && pos.MessageType() == pgwire.MsgClientTerminate {
		s.clientTerminateReceived = true
	}

	return nil
}

func (s *SessionState[C]) afterReadPos(pos *pos, loopContinues bool) error {
	if loopContinues {
		return pos.notHandledError()
	}
	return nil
}

func (s *SessionState[C]) writeQueue(dest ProxyRole) *pgwire.WriteQueue {
	if dest == RoleServer {
		return &s.bwq
	} else {
		return &s.crq
	}
}

func (s *SessionState[C]) conn(dest ProxyRole) Conn {
	if dest == RoleServer {
		return s.backendConn
	} else {
		return s.clientConn
	}
}

func (s *SessionState[C]) connAdapter(dest ProxyRole) ConnAdapter {
	if dest == RoleServer {
		return s.backendAdapter
	} else {
		return s.clientAdapter
	}
}

func releaseConnAdapter[C ConnAdapter](ctx context.Context, adapter *C) error {
	if adapter == nil {
		return nil
	}
	if err := (*adapter).Close(ctx); err != nil {
		return fmt.Errorf("failed to close conn adapter: %w", err)
	}
	var zero C
	*adapter = zero
	return nil
}
