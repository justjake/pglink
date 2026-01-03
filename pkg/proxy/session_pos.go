package proxy

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/justjake/pglink/pkg/pgwire"
)

// Pos represents a position in a message stream of a proxy [Session].
// Pos is only valid until the next call to [Session.Next].
type Pos interface {
	// Identifies if the message is from the client, or the backend.
	From() ProxyRole
	FromClient() bool
	FromServer() bool

	// Index of the message in the `From()` message stream.
	FromMsgIdx() int64

	// If From() is RoleClient, returns the client message.
	// Panics if the message is not a valid client message.
	ClientMsg() pgwire.ClientMessage
	AsClient() (pgwire.ClientMessage, error)

	// If From() is RoleServer, returns the server message.
	// Panics if the message is not a valid server message.
	ServerMsg() pgwire.ServerMessage
	AsServer() (pgwire.ServerMessage, error)

	// Set the action to handle this message.
	// TODO: should we be imperative instead?
	// TODO: just have action methods like .Forward(ctx) instead?
	SetAction(action Action)
	// Get the action to handle this message.
	// The default action is to forward the message.
	Action() Action

	// Logger annotated with position information.
	Logger() *slog.Logger

	String() string

	// Ctx returns a context derived from the one passed to [Session.Next] or [Session.Stream].
	// The context is cancelled when [Session.Close] is called.
	//
	// TODO: possibly pass OTEL context from client message here.
	Ctx() context.Context

	// Readers for message data.
	pgwire.RawMessageSource

	// Interface cannot be implemented outside this package due to private method.
	unwrap() *pos
}

type pos struct {
	*pgwire.Cursor
	from   ProxyRole
	action Action
	logger *slog.Logger
	ctx    context.Context
}

var _ Pos = (*pos)(nil)

// FromClient implements [Pos].
func (p *pos) FromClient() bool {
	return p.from == RoleClient
}

// FromServer implements [Pos].
func (p *pos) FromServer() bool {
	return p.from == RoleServer
}

// Action implements [Pos].
func (p *pos) Action() Action {
	return p.action
}

// ClientMsg implements [Pos].
func (p *pos) ClientMsg() pgwire.ClientMessage {
	msg, err := p.Cursor.AsClient()
	if err != nil {
		panic(err)
	}
	return msg
}

// From implements [Pos].
func (p *pos) From() ProxyRole {
	panic("unimplemented")
}

// FromMsgIdx implements [Pos].
func (p *pos) FromMsgIdx() int64 {
	return p.RingMsg.MsgIdx()
}

// Logger implements [Pos].
func (p *pos) Logger() *slog.Logger {
	return p.logger
}

// ServerMsg implements [Pos].
func (p *pos) ServerMsg() pgwire.ServerMessage {
	msg, err := p.Cursor.AsServer()
	if err != nil {
		panic(err)
	}
	return msg
}

// SetAction implements [Pos].
func (p *pos) SetAction(action Action) {
	p.action = action
}

func (p *pos) Ctx() context.Context {
	return p.ctx
}

// String implements [Pos].
// Subtle: this method shadows the method (RingMsg).String of pos.RingMsg.
func (p *pos) String() string {
	return fmt.Sprintf("Pos{%v %v}", p.From(), p.RingMsg)
}

// unwrap implements [Pos].
func (p *pos) unwrap() *pos {
	return p
}
