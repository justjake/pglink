package pgproxy

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/justjake/pglink/pkg/pgwire"
)

var ErrPosAlreadyHandled = errors.New("pos already handled")
var ErrPosNotHandled = errors.New("pos not handled")
var ErrWrongDestination = errors.New("wrong destination")
var ErrPosActionMismatch = errors.New("pos action mismatch")

// Pos represents a position in a message stream of a proxy [Session].
// Pos is only during the lifetime of a [Session.HandlePos] call.
// Each Pos must be handled with an action, or skipped.
type Pos2 struct {
	pgwire.StreamPos
	pgwire.Msg
	// Session is the proxy session that owns this position.
	Session *Session
	// Handled is true if the proxy session handled the message.
	Handled bool
}

// MsgSeq identifies the logical index of the message in the [Pos2.From] message stream.
func (p *Pos2) MsgSeq() int64 {
	return p.StreamPos.Seq
}

// MsgOffset identifies the byte offset of the message in the [Pos2.From] message stream.
func (p *Pos2) MsgOffset() int64 {
	return p.StreamPos.Offset
}

// Skip marks the message as handled without taking any action.
// Skip returns an error if the message has already been handled.
func (p *Pos2) Skip() error {
	return p.tryMarkHandled("skip")
}

// Forward queues sending the message to the destination.
// Forward returns an error if the message has already been handled.
func (p *Pos2) Forward(ctx context.Context) error {
	if err := p.tryMarkHandled("forward"); err != nil {
		return err
	}
	return p.Session.QueueSendMsg(ctx, p.Msg)
}

// Respond queues sending a response message to the client.
// The incoming message is skipped.
//
// Respond returns an error if response is for the wrong destination,
// or the incoming message has already been handled.
func (p *Pos2) Respond(ctx context.Context, response pgwire.Msg) error {
	if response.Destination() != p.From() {
		return fmt.Errorf("cannot respond: %w: %v -> %v != %v", ErrWrongDestination, response, response.Destination(), p.From())
	}
	if err := p.tryMarkHandled("respond"); err != nil {
		return err
	}
	// TODO: order tracking.
	return p.Session.QueueSendMsg(ctx, response)
}

// Rewrite queues sending a rewritten message to the destination.
//
// Rewrite returns an error if rewritten is for the wrong destination,
// or the incoming message has already been handled.
func (p *Pos2) Rewrite(ctx context.Context, rewritten pgwire.Msg) error {
	if rewritten.Destination() != p.Destination() {
		return fmt.Errorf("cannot rewrite: %w: %v -> %v != %v", ErrWrongDestination, rewritten, rewritten.Destination(), p.From())
	}
	if err := p.tryMarkHandled("rewrite"); err != nil {
		return err
	}
	return p.Session.QueueSendMsg(ctx, rewritten)
}

// Dispatch dispatches an [Action], as an alternative to calling methods directly.
// Dispatch returns an error if the incoming message has already been handled,
// or if the action fails validation.
func (p *Pos2) Dispatch(ctx context.Context, action Action) (err error) {
	panic("not implemented")

	// if unwrapped.incoming != nil {
	// 	if unwrapped.incoming.Source() != p.ProxyMessage {
	// 		return fmt.Errorf("%w: %v: action source %v != pos %v", ErrPosActionMismatch, action, unwrapped.incoming.Source(), p.ProxyMessage)
	// 	}
	// }

	// if p.debugEnabled() {
	// 	p.Logger().Debug("dispatch", "action", action)
	// }

	// defer func() {
	// 	if err != nil && !IsCleanTermination(err) {
	// 		p.Logger().Error("dispatch", "action", action, "error", err)
	// 	} else if err == nil {
	// 		// Attach response handler to the last outstanding request if present.
	// 		// This runs after the message has been queued and tracked by the OutstandingRequestQueue.
	// 		if unwrapped.responseHandler != nil {
	// 			if action.To() != RoleServer {
	// 				err = fmt.Errorf("dispatch %v: response handler only supported for messages to server, got %v", action, action.To())
	// 				return
	// 			}
	// 			backend := p.session.Backend()
	// 			if backend == nil {
	// 				err = fmt.Errorf("dispatch %v: response handler requires backend: %w", action, ErrBackendNotAcquired)
	// 				return
	// 			}
	// 			lastReq := backend.OutstandingRequests().LastOutstanding()
	// 			if lastReq == nil {
	// 				err = fmt.Errorf("dispatch %v: response handler requires outstanding request, but queue is empty", action)
	// 				return
	// 			}
	// 			lastReq.SetResponseHandler(unwrapped.responseHandler)
	// 			p.Logger().Debug("attached response handler", "action", action, "request", lastReq)
	// 		}

	// 		// TODO: possibly remove effect concept entirely.
	// 		// New code should not use effects.
	// 		var errs []error
	// 		var cleanupEffects []pure.Effect
	// 		for _, effect := range unwrapped.effects {
	// 			cleanup, effectErr := effect.Apply(ctx)
	// 			if effectErr != nil {
	// 				errs = append(errs, effectErr)
	// 			}
	// 			if cleanup != nil {
	// 				cleanupEffects = append(cleanupEffects, cleanup)
	// 			}
	// 		}

	// 		if len(errs) > 0 {
	// 			err = fmt.Errorf("dispatch %v: effect error: %w", action, errors.Join(errs...))
	// 			for _, cleanup := range cleanupEffects {
	// 				_, cleanupErr := cleanup.Apply(ctx)
	// 				if cleanupErr != nil {
	// 					p.Logger().Error("ignored effect rollback error", "rollback", cleanup, "error", cleanupErr, "cause", err)
	// 				}
	// 			}
	// 		}
	// 	}
	// }()

	// switch unwrapped.t {
	// case ProxyForward:
	// 	return p.Forward(ctx)
	// case ProxyRespond:
	// 	return p.Respond(ctx, unwrapped.outgoing)
	// case ProxyRewrite:
	// 	return p.Rewrite(ctx, unwrapped.outgoing)
	// case ProxySend:
	// 	if err := p.Skip(); err != nil {
	// 		return fmt.Errorf("%w: %v", err, action)
	// 	}
	// 	return p.session.QueueSend(ctx, unwrapped.outgoing)
	// case ProxySkip:
	// 	return p.Skip()
	// case ProxyTerminateBoth:
	// 	if !p.Handled() {
	// 		_ = p.Skip()
	// 	}
	// 	var pgErr *pgwire.Err
	// 	if !errors.As(unwrapped.err, &pgErr) {
	// 		pgErr = pgwire.NewErr(pgwire.ErrorFatal, pgerrcode.InternalError, "unexpected error", unwrapped.err)
	// 	}
	// 	return p.session.TerminateBoth(ctx, pgErr)
	// case ProxyTerminateClient:
	// 	if !p.Handled() {
	// 		_ = p.Skip()
	// 	}
	// 	var pgErr *pgwire.Err
	// 	if !errors.As(unwrapped.err, &pgErr) {
	// 		pgErr = pgwire.NewErr(pgwire.ErrorFatal, pgerrcode.InternalError, "unexpected error", unwrapped.err)
	// 	}
	// 	return p.session.TerminateClient(ctx, pgErr)
	// case ProxyTerminateServer:
	// 	if !p.Handled() {
	// 		_ = p.Skip()
	// 	}
	// 	return p.session.TerminateBackend(ctx, unwrapped.err)
	// case ProxyUnexpectedError:
	// 	if !p.Handled() {
	// 		_ = p.Skip()
	// 	}
	// 	return p.session.TerminateBothUnexpectedError(ctx, unwrapped.err)
	// default:
	// 	panic(fmt.Sprintf("unexpected proxy.ActionType: %#v", unwrapped.t))
	// }
}

func (p *Pos2) String() string {
	return fmt.Sprintf("Pos{%v %v}", p.Msg, p.StreamPos)
}

func (p *Pos2) Logger() *slog.Logger {
	if p.debugEnabled() {
		return p.baseLogger().With("seq", p.MsgSeq(), "offset", p.MsgOffset(), "msg", p.Msg)
	}
	return p.baseLogger()
}

func (p *Pos2) baseLogger() *slog.Logger {
	if p.Session == nil {
		return slog.Default()
	}
	return p.Session.Logger()
}

func (p *Pos2) debugEnabled() bool {
	return p.baseLogger().Enabled(context.Background(), slog.LevelDebug)
}

func (p *Pos2) tryMarkHandled(action string) error {
	if p.Handled {
		return fmt.Errorf("cannot %s: %w: %v", action, ErrPosAlreadyHandled, p)
	}
	if p.debugEnabled() {
		p.Logger().Debug("handled", "action", action)
	}
	p.Handled = true
	return nil
}
