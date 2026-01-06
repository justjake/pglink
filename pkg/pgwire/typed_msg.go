package pgwire

import "errors"

// ErrMsgGoTypeMismatch is returned when a typed message wrapper's actual MsgType
// doesn't match its expected type.
var ErrMsgGoTypeMismatch = errors.New("msg Go type does not match wire type")

// ErrMsgSenderMismatch is returned when a typed message wrapper's Sender
// doesn't match its expected sender.
var ErrMsgSenderMismatch = errors.New("msg sender does not match expected sender")

// TypedMsg is a Msg with a specific expected wire protocol type.
// Implementations are generated wrapper types like Bind, Query, DataRow, etc.
type TypedMsg interface {
	RawMessageSource
	// ExpectedType returns the MsgType constant this wrapper expects.
	ExpectedType() MsgType
	// From returns the sender of the underlying Msg.
	From() Sender
	// Validate checks that the message is well-formed and matches ExpectedType.
	Validate() error
	// Msg returns the underlying Msg.
	Msg() Msg
	// Copy returns a copy of this message as TypedMsg with its own data slice.
	Copy() TypedMsg
}

// ClientMsg is a TypedMsg that can only be sent by clients (frontend).
type ClientMsg interface {
	TypedMsg
	ExpectedClientType() MsgType
	// ExpectedFrom returns SenderClient.
	ExpectedFrom() Sender
	// CopyClient returns a copy as a ClientMsg.
	CopyClient() ClientMsg
}

// ServerMsg is a TypedMsg that can only be sent by servers (backend).
type ServerMsg interface {
	TypedMsg
	ExpectedServerType() MsgType
	// ExpectedFrom returns SenderServer.
	ExpectedFrom() Sender
	// CopyServer returns a copy as a ServerMsg.
	CopyServer() ServerMsg
}
