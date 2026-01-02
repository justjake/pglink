package proxy

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/justjake/pglink/pkg/pgwire"
	"github.com/justjake/pglink/pkg/pure"
)

type Tracker interface {
	TrackEffect(msg pgwire.Message) pure.Effect
}

type Releaser interface {
	Release()
}

type Terminater interface {
	Terminate(ctx context.Context, err error) error
}

type Conn[Rx pgwire.Message, Tx pgwire.Message] interface {
	WriteBuffered(ctx context.Context, msg Tx) error
	HasBufferedWrite() bool
	Flush(ctx context.Context) error
	Cursor(ctx context.Context) (*pgwire.Cursor, error)
	fmt.Stringer
	Terminater
	Releaser
}

type Frontend interface {
	Conn[pgwire.ClientMessage, pgwire.ServerMessage]
	StartupParameters() pgwire.ParameterStatuses
	ParameterStatuses() pgwire.ParameterStatuses
}

type Backend interface {
	Conn[pgwire.ServerMessage, pgwire.ClientMessage]
	ParameterStatuses() pgwire.ParameterStatuses
}

var ErrLoopTrackingFailed = errors.New("proxy loop: tracking failed")
var ErrLoopActionFailed = errors.New("proxy loop: action failed")
var ErrLoopActionEffectsFailed = errors.New("proxy loop: action effects failed")
var ErrLoopNoBackend = errors.New("proxy loop: backend not acquired")
var ErrLoopFrontendNotConnected = errors.New("proxy loop: frontend not connected")
var ErrLoopBackendAcquireFailed = errors.New("proxy loop: backend acquire failed")

type LoopHandler func(ctx context.Context, loop Loop, msg pgwire.Message) Action

type Loop interface {
	Loop() Loop

	SetStateEffect(nextState LoopHandler)

	RequestQueue() OutstandingRequestQueue
	Frontend(ctx context.Context) (Frontend, error)
	Backend(ctx context.Context, acquire bool) (Backend, error)
	ReleaseBackendEffect()
}

var _ ActionHandler = (*LoopState)(nil)

type LoopState struct {
	Logger          *slog.Logger
	BackendTrackers []Tracker
	MessageHandler  LoopHandler
	ActionHandler   ActionHandler
	// Should return
	BackendProvider       func(ctx context.Context) (Backend, error)
	HealthCheckHandler    func(ctx context.Context) error
	HealthCheckTicker     *time.Ticker
	EffectHandler         pure.EffectHandler
	AcquiredBackend       Backend
	ConnectedFrontend     Frontend
	BackendRequestTracker OutstandingRequestQueue
}

type cursorCtx struct {
	logger *slog.Logger
	from   ProxyRole
	cursor *pgwire.Cursor
}

func (l *LoopState) sendToClient(ctx context.Context, msg pgwire.ServerMessage) error {
	logger := getCursorCtx(ctx).logger
	if logger.Enabled(ctx, slog.LevelDebug) {
		logger.Debug("sending to client", "msg", msg)
	}

	frontend, err := l.Frontend(ctx)
	if err != nil {
		return err
	}

	if err := frontend.WriteBuffered(ctx, msg); err != nil {
		return err
	}

	return nil
}

func (l *LoopState) sendToServer(ctx context.Context, msg pgwire.ClientMessage) error {
	logger := getCursorCtx(ctx).logger
	if logger.Enabled(ctx, slog.LevelDebug) {
		logger.Debug("sending to server", "msg", msg)
	}

	backend, err := l.Backend(ctx, true)
	if err != nil {
		return err
	}

	if err := backend.WriteBuffered(ctx, msg); err != nil {
		return err
	}

	if err := l.trackNow(ctx, msg); err != nil {
		return err
	}

	return nil
}

// ForwardClientMessage implements [ActionHandler].
func (l *LoopState) ForwardClientMessage(ctx context.Context, msg pgwire.ClientMessage, responseHandler ResponseHandler) error {
	return l.sendToServer(ctx, msg)
}

// ForwardServerMessage implements [ActionHandler].
func (l *LoopState) ForwardServerMessage(ctx context.Context, msg pgwire.ServerMessage) error {
	return l.sendToClient(ctx, msg)
}

// RespondToClient implements [ActionHandler].
func (l *LoopState) RespondToClient(ctx context.Context, req pgwire.ClientMessage, res pgwire.ServerMessage) error {
	return l.sendToClient(ctx, res)
}

// RewriteClientMessage implements [ActionHandler].
func (l *LoopState) RewriteClientMessage(ctx context.Context, original pgwire.ClientMessage, rewritten pgwire.ClientMessage, responseHandler ResponseHandler) error {
	return l.sendToServer(ctx, rewritten)
}

// RewriteServerMessage implements [ActionHandler].
func (l *LoopState) RewriteServerMessage(ctx context.Context, original pgwire.ServerMessage, rewritten pgwire.ServerMessage) error {
	return l.sendToClient(ctx, rewritten)
}

// SendToClient implements [ActionHandler].
func (l *LoopState) SendToClient(ctx context.Context, msg pgwire.ServerMessage) error {
	return l.sendToClient(ctx, msg)
}

// SendToServer implements [ActionHandler].
func (l *LoopState) SendToServer(ctx context.Context, msg pgwire.ClientMessage, responseHandler ResponseHandler) error {
	return l.sendToServer(ctx, msg)
}

// SkipClientMessage implements [ActionHandler].
func (l *LoopState) SkipClientMessage(ctx context.Context, skipped pgwire.ClientMessage) error {
	return nil
}

// SkipServerMessage implements [ActionHandler].
func (l *LoopState) SkipServerMessage(ctx context.Context, skipped pgwire.ServerMessage) error {
	return nil
}

// TerminateBoth implements [ActionHandler].
func (l *LoopState) TerminateBoth(ctx context.Context, err error) error {
	clientErr := l.TerminateClient(ctx, err)
	serverErr := l.TerminateServer(ctx, err)
	return errors.Join(err, clientErr, serverErr)
}

// TerminateClient implements [ActionHandler].
func (l *LoopState) TerminateClient(ctx context.Context, err error) error {
	if client := l.ConnectedFrontend; client != nil {
		if err := client.Terminate(ctx, err); err != nil {
			return err
		}
		l.ConnectedFrontend = nil
	}

	return nil
}

// TerminateServer implements [ActionHandler].
func (l *LoopState) TerminateServer(ctx context.Context, err error) error {
	if server := l.AcquiredBackend; server != nil {
		if err := server.Terminate(ctx, err); err != nil {
			return err
		}
		l.AcquiredBackend = nil
	}

	return nil
}

// UnexpectedError implements [ActionHandler].
func (l *LoopState) UnexpectedError(ctx context.Context, err error) error {
	return l.TerminateBoth(ctx, err)
}

func (l *LoopState) RunConn(ctx context.Context) (err error) {
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		if err := l.HealthCheckHandler(ctx); err != nil {
			return err
		}

		frontend, err := l.Frontend(ctx)
		if err != nil {
			return err
		}
		logger := l.Logger.With("client", frontend)

		backend := l.AcquiredBackend

		var frontendCursor *pgwire.Cursor
		frontendCursor, err = frontend.Cursor(ctx)
		if err != nil {
			return err
		}

		var backendCursor *pgwire.Cursor
		if backend != nil {
			logger = logger.With("backend", backend)
			backendCursor, err = backend.Cursor(ctx)
			if err != nil {
				return err
			}
		}

		gotFrontend, errF := frontendCursor.TryNextBatch()
		gotBackend := false
		if backendCursor != nil {
			gotBackend, err = backendCursor.TryNextBatch()
			if err != nil {
				return errors.Join(errF, err)
			}
		}
		if errF != nil {
			return errF
		}

		if gotBackend {
			if err := l.RunCursor(ctx, logger, RoleServer, backendCursor); err != nil {
				return err
			}
		}

		if gotFrontend {
			if err := l.RunCursor(ctx, logger, RoleClient, frontendCursor); err != nil {
				return err
			}
		}

		if !gotFrontend && !gotBackend {
			select {
			case <-ctx.Done():
			case <-frontendCursor.Ready():
			case <-backendCursor.Ready():
			case <-frontendCursor.Done():
				return frontendCursor.Err()
			case <-backendCursor.Done():
				return backendCursor.Err()
			case <-l.HealthCheckTicker.C:
			}
		}
	}
}

func (l *LoopState) RunCursor(ctx context.Context, logger *slog.Logger, role ProxyRole, cursor *pgwire.Cursor) (err error) {
	cursorLogger := logger.With("handling", role, "cursor", cursor.String())
	defer func() {
		if frontend := l.ConnectedFrontend; frontend != nil {
			if flushErr := frontend.Flush(ctx); flushErr != nil {
				cursorLogger.Warn("flush: failed to flush frontend", "err", flushErr)
				err = errors.Join(err, flushErr)
			}
		}
		if backend := l.AcquiredBackend; backend != nil {
			if flushErr := backend.Flush(ctx); flushErr != nil {
				cursorLogger.Warn("flush: failed to flush backend", "err", flushErr)
				err = errors.Join(err, flushErr)
			}
		}
	}()

	for cursor.NextMsg() {
		msgLogger := cursorLogger.With("seq", cursor.MsgIdx())
		var msg pgwire.Message
		if role == RoleServer {
			msg, err = cursor.AsServer()
			if err != nil {
				return err
			}
		} else {
			msg, err = cursor.AsClient()
			if err != nil {
				return err
			}
		}
		msgLogger = msgLogger.With("msg", fmt.Sprintf("%T", msg))
		wrapper := &cursorCtx{
			logger: msgLogger,
			from:   role,
			cursor: cursor,
		}

		msgCtx := context.WithValue(ctx, cursorCtx{}, wrapper)
		if err = l.HandleMessage(msgCtx, msg); err != nil {
			msgLogger.Error("handling message failed", "err", err)
			return err
		}
	}

	return nil
}

func (l *LoopState) HandleMessage(ctx context.Context, msg pgwire.Message) error {
	if msg, ok := msg.(pgwire.ServerMessage); ok {
		if err := l.trackNow(ctx, msg); err != nil {
			return fmt.Errorf("%w: %w", ErrLoopTrackingFailed, err)
		}
	}

	action := l.MessageHandler(ctx, l.Loop(), msg).unwrap()
	cursorCtx := getCursorCtx(ctx)
	cursorCtx.logger = cursorCtx.logger.With("action", action.String())
	ctx = withCursorCtx(ctx, cursorCtx)

	if err := ApplyAction(ctx, l.ActionHandler, action); err != nil {
		return fmt.Errorf("%w: %w", ErrLoopActionFailed, err)
	}

	if err := ApplyActionEffects(ctx, l.EffectHandler, action); err != nil {
		return fmt.Errorf("%w: %w", ErrLoopActionEffectsFailed, err)
	}

	return nil
}

func (l *LoopState) Frontend(ctx context.Context) (Frontend, error) {
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}

	if l.ConnectedFrontend != nil {
		return l.ConnectedFrontend, nil
	}

	return nil, ErrLoopFrontendNotConnected
}

func (l *LoopState) Backend(ctx context.Context, acquire bool) (Backend, error) {
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}

	if l.AcquiredBackend != nil {
		return l.AcquiredBackend, nil
	}

	if !acquire {
		return nil, ErrLoopNoBackend
	}

	backend, err := l.BackendProvider(ctx)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrLoopBackendAcquireFailed, err)
	}
	l.AcquiredBackend = backend
	return backend, nil
}

func (l *LoopState) RequestQueue() OutstandingRequestQueue {
	return l.BackendRequestTracker
}

func (l *LoopState) SetStateEffect(nextState LoopHandler) pure.Effect {
	return pure.DoNamed(fmt.Sprintf("Loop.SetState(%s)", pure.DescribeFunction(nextState)), func() {
		l.MessageHandler = nextState
	})
}

func (l *LoopState) trackNow(ctx context.Context, msg pgwire.Message) error {
	var effects pure.Effects
	effects.Add(l.BackendRequestTracker.TrackEffect(msg))
	for _, tracker := range l.BackendTrackers {
		effects.Add(tracker.TrackEffect(msg))
	}
	return l.EffectHandler(ctx, effects)
}

var cursorCtxKey = cursorCtx{}

func getCursorCtx(ctx context.Context) cursorCtx {
	return ctx.Value(cursorCtxKey).(cursorCtx)
}

func withCursorCtx(ctx context.Context, cursorCtx cursorCtx) context.Context {
	return context.WithValue(ctx, cursorCtxKey, cursorCtx)
}
