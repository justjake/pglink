package pgwire

import (
	"errors"

	"github.com/jackc/pgx/v5/pgproto3"
)

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
	// ParseAny decodes the message body into its corresponding pgproto3 type.
	// Returns pgproto3.Message interface to allow uniform handling.
	ParseAny() (pgproto3.Message, error)
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

func DecodeTypedMsg[
	T any,
	R interface {
		Parse() (T, error)
	},
	PT interface {
		*T
		pgproto3.Message
	},
](msg R) (out pgproto3.Message, err error) {
	t, err := msg.Parse()
	if err != nil {
		return nil, err
	}
	return PT(&t), nil
}
