package pgwire

// The PostreSQL wire protocol identifies the "backend process" a client is connected to by its ProcessID.
// Cancellation requests are sent to a specific ProcessID authenticated by a [SecretKey].
// Docs: https://www.postgresql.org/docs/current/protocol-flow.html#PROTOCOL-FLOW-CANCELING-REQUESTS
//
// This is the type used by [pgproto3.BackendKeyData].
type ProcessID uint32

// Cancellation requests are sent to a specific ProcessID authenticated by a [SecretKey].
// https://www.postgresql.org/docs/current/protocol-flow.html#PROTOCOL-FLOW-CANCELING-REQUESTS
//
// This is the type used by [pgproto3.BackendKeyData].
// However, the PostgreSQL wire protocol allows for up to 256 bytes.
// TODO: support longer secret keys.
type SecretKey uint32

// ResponseAction determines how to handle a server response
type ResponseAction int

const (
	// ActionForward forwards the response to the client
	ActionForward ResponseAction = iota
	// ActionSkip consumes the response silently without forwarding
	ActionSkip
	// ActionFake generates a synthetic response without sending to server
	ActionFake
)

func (a ResponseAction) String() string {
	switch a {
	case ActionForward:
		return "forward"
	case ActionSkip:
		return "skip"
	case ActionFake:
		return "fake"
	default:
		return "unknown"
	}
}
