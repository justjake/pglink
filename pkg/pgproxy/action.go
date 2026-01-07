package pgproxy

import (
	"fmt"

	"github.com/justjake/pglink/pkg/pgwire"
	"github.com/justjake/pglink/pkg/pure"
)

type ActionType string

const (
	ProxyForward         ActionType = "forward"
	ProxySkip            ActionType = "skip"
	ProxyRespond         ActionType = "respond"
	ProxyRewrite         ActionType = "rewrite"
	ProxySend            ActionType = "send"
	ProxyTerminateClient ActionType = "terminate client"
	ProxyTerminateServer ActionType = "terminate server"
	ProxyTerminateBoth   ActionType = "terminate both"
	ProxyUnexpectedError ActionType = "unexpected error"
)

func (t ActionType) String() string {
	return string(t)
}

type ProxyRole = pgwire.Sender

const (
	RoleProxy  ProxyRole = pgwire.SenderProxy
	RoleClient ProxyRole = pgwire.SenderClient
	RoleServer ProxyRole = pgwire.SenderServer
)

type Action interface {
	Type() ActionType
	From() ProxyRole
	To() ProxyRole
	String() string
	Err() error
	WithEffects(effects ...pure.Effect) Action

	// Action cannot be implemented outside this package due to private method.
	unwrap() *action
}

type action struct {
	t               ActionType
	incoming        pgwire.Message
	outgoing        pgwire.Message
	effects         pure.Effects
	responseHandler ResponseHandler

	// if t = ProxyTerminateClient/ProxyTerminateServer/ProxyTerminateBoth
	err error
}

func (a *action) Type() ActionType {
	return a.t
}

func (a *action) From() ProxyRole {
	if a.incoming == nil {
		return a.To().To()
	}
	switch a.incoming.(type) {
	case pgwire.ClientMessage:
		return RoleClient
	case pgwire.ServerMessage:
		return RoleServer
	default:
		panic(fmt.Sprintf("unexpected incoming message type: %T", a.incoming))
	}
}

func (a *action) To() ProxyRole {
	if a.outgoing == nil {
		return a.From().To()
	}
	switch a.outgoing.(type) {
	case pgwire.ClientMessage:
		return RoleClient
	case pgwire.ServerMessage:
		return RoleServer
	default:
		panic(fmt.Sprintf("unexpected outgoing message type: %T", a.outgoing))
	}
}

func (a *action) Err() error {
	return a.err
}

// WithEffects is deprecated. We are considering removing the effects concept entirely.
func (a *action) WithEffects(effects ...pure.Effect) Action {
	if len(effects) == 0 {
		return a
	}
	b := *a
	b.effects = make(pure.Effects, len(a.effects)+len(effects))
	copy(b.effects, a.effects)
	copy(b.effects[len(a.effects):], effects)
	return &b
}

func (a *action) unwrap() *action {
	return a
}

func (a *action) String() string {
	switch a.t {
	case ProxyForward:
		return fmt.Sprintf("Forward(%T -> %s %v)", a.incoming, a.To(), a.effects)
	case ProxyRespond:
		return fmt.Sprintf("Respond(req %T -> %s, res %T -> %s %v)", a.incoming, a.From().To(), a.outgoing, a.To(), a.effects)
	case ProxyRewrite:
		return fmt.Sprintf("Rewrite(%T to %T -> %s %v)", a.incoming, a.outgoing, a.To(), a.effects)
	case ProxySend:
		return fmt.Sprintf("Send(%T -> %s %v)", a.outgoing, a.To(), a.effects)
	case ProxySkip:
		return fmt.Sprintf("Skip(%T -!-> %s %v)", a.incoming, a.To(), a.effects)
	case ProxyTerminateClient:
		return fmt.Sprintf("TerminateClient(%T -> %s: %v %s)", a.incoming, a.To(), a.err, a.effects)
	case ProxyTerminateServer:
		return fmt.Sprintf("TerminateServer(%T -> %s: %v %s)", a.incoming, a.To(), a.err, a.effects)
	case ProxyTerminateBoth:
		return fmt.Sprintf("TerminateBoth(%T -> %s: %v %s)", a.incoming, a.To(), a.err, a.effects.String())
	case ProxyUnexpectedError:
		return fmt.Sprintf("UnexpectedError(%T -> %s: %v %s)", a.incoming, a.To(), a.err, a.effects)
	default:
		panic(fmt.Sprintf("unexpected proxy action type: %#v", string(a.t)))
	}
}

func Forward(msg pgwire.Message) Action {
	return &action{
		t:        ProxyForward,
		incoming: msg,
		outgoing: msg,
	}
}

func ForwardAndHandleResponse(msg pgwire.ClientMessage, responseHandler ResponseHandler) Action {
	return &action{
		t:               ProxyForward,
		incoming:        msg,
		outgoing:        msg,
		responseHandler: responseHandler,
	}
}

func Rewrite(msg pgwire.Message, rewritten pgwire.Message) Action {
	return &action{
		t:        ProxyRewrite,
		incoming: msg,
		outgoing: rewritten,
	}
}

func RewriteAndHandleResponse(msg pgwire.ClientMessage, rewritten pgwire.ClientMessage, responseHandler ResponseHandler) Action {
	return &action{
		t:               ProxyRewrite,
		incoming:        msg,
		outgoing:        rewritten,
		responseHandler: responseHandler,
	}
}

func SendToServerAndHandleResponse(msg pgwire.ClientMessage, responseHandler ResponseHandler) Action {
	return &action{
		t:               ProxySend,
		outgoing:        msg,
		responseHandler: responseHandler,
	}
}

func SendToClient(msg pgwire.ServerMessage) Action {
	return &action{
		t:        ProxySend,
		outgoing: msg,
	}
}

func Skip(msg pgwire.Message) Action {
	return &action{
		t:        ProxySkip,
		incoming: msg,
	}
}

func TerminateClient(msg pgwire.ClientMessage, err error) Action {
	return &action{
		t:        ProxyTerminateClient,
		incoming: msg,
		err:      err,
	}
}

func TerminateServer(msg pgwire.ServerMessage, err error) Action {
	return &action{
		t:        ProxyTerminateServer,
		incoming: msg,
		err:      err,
	}
}

func TerminateBoth(msg pgwire.Message, err error) Action {
	return &action{
		t:        ProxyTerminateBoth,
		incoming: msg,
		err:      err,
	}
}

func UnexpectedError(msg pgwire.Message, err error) Action {
	return &action{
		t:        ProxyUnexpectedError,
		incoming: msg,
		err:      err,
	}
}
