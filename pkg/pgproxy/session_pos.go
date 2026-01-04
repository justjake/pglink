package pgproxy

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/jackc/pgerrcode"
	"github.com/justjake/pglink/pkg/pgwire"
	"github.com/justjake/pglink/pkg/pure"
)

var ErrPosAlreadyHandled = errors.New("pos already handled")
var ErrPosNotHandled = errors.New("pos not handled")
var ErrWrongDestination = errors.New("wrong destination")
var ErrPosActionMismatch = errors.New("pos action mismatch")

// Pos represents a position in a message stream of a proxy [Session].
// Pos is only valid until the next call to [Session.Next].
// Each pos must be handled with an action, or skipped.
type Pos interface {
	// Identifies if the message is from the client, or the backend.
	From() ProxyRole
	FromClient() bool
	FromServer() bool

	// Index of the message in the `From()` message stream.
	FromMsgIdx() int64

	// Returns the message at this position.
	// Panics if the message is not a valid message.
	Msg() pgwire.Message
	// Returns the message at this position.
	// Returns an error if the message is not a valid message.
	AsMessage() (pgwire.Message, error)

	// If From() is RoleClient, returns the client message.
	// Panics if the message is not a valid client message.
	ClientMsg() pgwire.ClientMessage
	AsClient() (pgwire.ClientMessage, error)

	// If From() is RoleServer, returns the server message.
	// Panics if the message is not a valid server message.
	ServerMsg() pgwire.ServerMessage
	AsServer() (pgwire.ServerMessage, error)

	// Mark this message as handled without taking any action.
	// Returns an error if the message has already been handled.
	Skip() error
	// Forward the message to the destination.
	// If the destination is the backend, an optional response handler can be provided.
	// Returns an error if the message has already been handled.
	Forward(ctx context.Context) error
	// Replace the message with a new message for the destination.
	// Returns an error if the rewritten message is for the source.
	// Returns an error if the message has already been handled.
	Rewrite(ctx context.Context, rewritten pgwire.Message) error
	// Respond to the message with a message for the source.
	// Returns an error if the response is for the destination.
	// Returns an error if the message has already been handled.
	Respond(ctx context.Context, response pgwire.Message) error
	// Dispatch an Action object as an alternative to calling a method directly.
	Dispatch(ctx context.Context, action Action) error
	// Returns true if the message has been handled by any action above.
	Handled() bool

	// Logger annotated with position information.
	Logger() *slog.Logger

	String() string

	// Ctx returns a context derived from the one passed to [Session.Next] or [Session.Stream],
	// modified by [MessageTracker]s.
	Ctx() context.Context

	// Readers for message data.
	pgwire.RawMessageSource

	// Interface cannot be implemented outside this package due to private method.
	unwrap() *pos
}

type pos struct {
	session *Session
	*pgwire.RingMsg
	Cursor     *pgwire.Cursor
	from       ProxyRole
	action     Action
	baseLogger *slog.Logger
	logger     *slog.Logger
	ctx        context.Context
	handled    bool
}

// var _ Pos = (*pos)(nil)

func (p *pos) reset(session *Session, cursor *pgwire.Cursor, from ProxyRole) {
	p.session = session
	p.Cursor = cursor
	p.RingMsg = &cursor.RingMsg
	p.from = from
	p.baseLogger = session.Logger()
	p.logger = nil
	p.handled = false
}

// FromClient implements [Pos].
func (p *pos) FromClient() bool {
	return p.from == RoleClient
}

// FromServer implements [Pos].
func (p *pos) FromServer() bool {
	return p.from == RoleServer
}

// Msg implements [Pos].
func (p *pos) Msg() pgwire.Message {
	msg, err := p.AsMessage()
	if err != nil {
		panic(err)
	}
	return msg
}

func (p *pos) AsMessage() (pgwire.Message, error) {
	if p.FromClient() {
		return p.AsClient()
	} else {
		return p.AsServer()
	}
}

func (p *pos) AsClient() (pgwire.ClientMessage, error) {
	return p.Cursor.AsClient()
}

// ClientMsg implements [Pos].
func (p *pos) ClientMsg() pgwire.ClientMessage {
	msg, err := p.AsClient()
	if err != nil {
		panic(err)
	}
	return msg
}

// From implements [Pos].
func (p *pos) From() ProxyRole {
	return p.from
}

// FromMsgIdx implements [Pos].
func (p *pos) FromMsgIdx() int64 {
	return p.Cursor.MsgIdx()
}

// Logger implements [Pos].
func (p *pos) Logger() *slog.Logger {
	if p.logger != nil {
		return p.logger
	}
	p.logger = p.baseLogger.With(
		"from", p.from,
		"cursor", p.Cursor.String(),
		"idx", p.Cursor.MsgIdx(),
		"type", p.Cursor.MessageType(),
	)
	return p.logger
}

func (p *pos) AsServer() (pgwire.ServerMessage, error) {
	return p.Cursor.AsServer()
}

// ServerMsg implements [Pos].
func (p *pos) ServerMsg() pgwire.ServerMessage {
	msg, err := p.AsServer()
	if err != nil {
		panic(err)
	}
	return msg
}

func (p *pos) Skip() error {
	return p.tryMarkHandled("skip")
}

func (p *pos) Forward(ctx context.Context) error {
	if err := p.tryMarkHandled("forward"); err != nil {
		return err
	}
	return p.session.QueueSendPos(ctx, p)
}

func (p *pos) Respond(ctx context.Context, response pgwire.Message) error {
	if dest(response) != p.from {
		return fmt.Errorf("cannot respond: %w: %T -> %v != %v", ErrWrongDestination, response, dest(response), p.from)
	}
	if err := p.tryMarkHandled("respond"); err != nil {
		return err
	}
	return p.session.QueueSend(ctx, response)
}

func (p *pos) Rewrite(ctx context.Context, rewritten pgwire.Message) error {
	if dest(rewritten) != p.from.To() {
		return fmt.Errorf("cannot rewrite: %w: %T -> %v != %v", ErrWrongDestination, rewritten, dest(rewritten), p.from.To())
	}
	if err := p.tryMarkHandled("rewrite"); err != nil {
		return err
	}
	return p.session.QueueSend(ctx, rewritten)
}

func (p *pos) Dispatch(ctx context.Context, action Action) (err error) {
	unwrapped := action.unwrap()

	if unwrapped.incoming != nil {
		if unwrapped.incoming.Source() != p.RingMsg {
			return fmt.Errorf("%w: %v: action source %v != pos %v", ErrPosActionMismatch, action, unwrapped.incoming.Source(), p.RingMsg)
		}
	}

	p.Logger().Debug("dispatch", "action", action)
	if unwrapped.responseHandler != nil {
		panic(fmt.Errorf("dispatch %v: response handler not implemented yet", action))
	}

	defer func() {
		if err != nil {
			p.Logger().Error("dispatch", "action", action, "error", err)
		} else {
			// TODO: possibly remove effect concept entirely.
			// New code should not use effects.
			var errs []error
			var cleanupEffects []pure.Effect
			for _, effect := range unwrapped.effects {
				cleanup, effectErr := effect.Apply(ctx)
				if effectErr != nil {
					errs = append(errs, effectErr)
				}
				if cleanup != nil {
					cleanupEffects = append(cleanupEffects, cleanup)
				}
			}

			if len(errs) > 0 {
				err = fmt.Errorf("dispatch %v: effect error: %w", action, errors.Join(errs...))
				for _, cleanup := range cleanupEffects {
					_, cleanupErr := cleanup.Apply(ctx)
					if cleanupErr != nil {
						p.Logger().Error("ignored effect rollback error", "rollback", cleanup, "error", cleanupErr, "cause", err)
					}
				}
			}
		}
	}()

	switch unwrapped.t {
	case ProxyForward:
		return p.Forward(ctx)
	case ProxyRespond:
		return p.Respond(ctx, unwrapped.outgoing)
	case ProxyRewrite:
		return p.Rewrite(ctx, unwrapped.outgoing)
	case ProxySend:
		if err := p.Skip(); err != nil {
			return fmt.Errorf("%w: %v", err, action)
		}
		return p.session.QueueSend(ctx, unwrapped.outgoing)
	case ProxySkip:
		return p.Skip()
	case ProxyTerminateBoth:
		if !p.Handled() {
			_ = p.Skip()
		}
		var pgErr *pgwire.Err
		if !errors.As(unwrapped.err, &pgErr) {
			pgErr = pgwire.NewErr(pgwire.ErrorFatal, pgerrcode.InternalError, "unexpected error", unwrapped.err)
		}
		return p.session.TerminateBoth(ctx, pgErr)
	case ProxyTerminateClient:
		if !p.Handled() {
			_ = p.Skip()
		}
		var pgErr *pgwire.Err
		if !errors.As(unwrapped.err, &pgErr) {
			pgErr = pgwire.NewErr(pgwire.ErrorFatal, pgerrcode.InternalError, "unexpected error", unwrapped.err)
		}
		return p.session.TerminateClient(ctx, pgErr)
	case ProxyTerminateServer:
		if !p.Handled() {
			_ = p.Skip()
		}
		return p.session.TerminateBackend(ctx, unwrapped.err)
	case ProxyUnexpectedError:
		if !p.Handled() {
			_ = p.Skip()
		}
		return p.session.TerminateBothUnexpectedError(ctx, unwrapped.err)
	default:
		panic(fmt.Sprintf("unexpected proxy.ActionType: %#v", unwrapped.t))
	}
}

func (p *pos) Handled() bool {
	return p.handled
}

func (p *pos) Ctx() context.Context {
	return p.ctx
}

// String implements [Pos].
// Subtle: this method shadows the method (RingMsg).String of pos.RingMsg.
func (p *pos) String() string {
	return fmt.Sprintf("Pos{%v %v}", p.From(), &p.Cursor.RingMsg)
}

// unwrap implements [Pos].
func (p *pos) unwrap() *pos {
	return p
}

func (p *pos) tryMarkHandled(action string) error {
	if p.handled {
		return fmt.Errorf("cannot %s: %w: %v", action, ErrPosAlreadyHandled, p)
	}
	p.Logger().Debug("handled", "action", action)
	p.handled = true
	return nil
}

func (p *pos) notHandledError() error {
	if !p.handled {
		return fmt.Errorf("%w: %v", ErrPosNotHandled, p)
	}
	return nil
}
