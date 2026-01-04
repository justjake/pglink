package proxy

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

func (p *pos) reset(cursor *pgwire.Cursor, from ProxyRole) {
	p.Cursor = cursor
	p.RingMsg = &cursor.RingMsg
	p.from = from
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
