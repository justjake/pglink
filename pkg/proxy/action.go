package proxy

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

type ProxyDestination string

const (
	ProxyDestinationClient ProxyDestination = "client"
	ProxyDestinationServer ProxyDestination = "server"
)

func (d ProxyDestination) String() string {
	s := string(d)
	if s == "" {
		return "?"
	}
	return s
}

func (d ProxyDestination) Flipped() ProxyDestination {
	if d == ProxyDestinationClient {
		return ProxyDestinationServer
	} else {
		return ProxyDestinationClient
	}
}

type Action interface {
	Type() ActionType
	String() string
	Err() error
	WithEffects(effects ...pure.Effect) Action

	// Action cannot be implemented outside this package due to private method.
	unwrap() *action
}

type action struct {
	t        ActionType
	to       ProxyDestination
	original pgwire.Message
	effects  pure.Effects

	// if t = ProxyRewrite or ProxySend
	toDestination pgwire.Message
	// if t = ProxyRespond
	toSource pgwire.Message
	// if t = ProxyForward/Proxy

	responseHandler ResponseHandler

	// if t = ProxyTerminateClient/ProxyTerminateServer/ProxyTerminateBoth
	err error
}

func (a *action) Type() ActionType {
	return a.t
}

func (a *action) Err() error {
	return a.err
}

func (a *action) WithEffects(effects ...pure.Effect) Action {
	var b action
	b = *a
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
		return fmt.Sprintf("Forward(%T -> %s %s)", a.original, a.to, a.effects.String())
	case ProxyRespond:
		return fmt.Sprintf("Respond(req %T -> %s, res %T -> %s %s)", a.original, a.to, a.toSource, a.to.Flipped(), a.effects.String())
	case ProxyRewrite:
		return fmt.Sprintf("Rewrite(%T to %T -> %s %s)", a.original, a.toDestination, a.to, a.effects.String())
	case ProxySend:
		return fmt.Sprintf("Send(%T -> %s %s)", a.toDestination, a.to, a.effects.String())
	case ProxySkip:
		return fmt.Sprintf("Skip(%T -!-> %s %s)", a.original, a.to, a.effects.String())
	case ProxyTerminateClient:
		return fmt.Sprintf("TerminateClient(%T -> %s: %v %s)", a.original, a.to, a.err, a.effects.String())
	case ProxyTerminateServer:
		return fmt.Sprintf("TerminateServer(%T -> %s: %v %s)", a.original, a.to, a.err, a.effects.String())
	case ProxyTerminateBoth:
		return fmt.Sprintf("TerminateBoth(%T -> %s: %v %s)", a.original, a.to, a.err, a.effects.String())
	case ProxyUnexpectedError:
		return fmt.Sprintf("UnexpectedError(%T -> %s: %v %s)", a.original, a.to, a.err, a.effects.String())
	default:
		panic(fmt.Sprintf("unexpected proxy action type: %#v", string(a.t)))
	}
}

func Forward(msg pgwire.Message, effects ...pure.Effect) Action {
	return &action{
		t:        ProxyForward,
		original: msg,
		effects:  effects,
	}
}

func UnexpectedError(msg pgwire.Message, err error, effects ...pure.Effect) Action {
	return &action{
		t:        ProxyUnexpectedError,
		original: msg,
		err:      err,
		effects:  effects,
	}
}
