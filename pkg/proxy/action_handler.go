package proxy

import (
	"context"
	"fmt"

	"github.com/justjake/pglink/pkg/pgwire"
	"github.com/justjake/pglink/pkg/pure"
)

type ActionHandler interface {
	ForwardClientMessage(ctx context.Context, msg pgwire.ClientMessage, responseHandler ResponseHandler) error
	ForwardServerMessage(ctx context.Context, msg pgwire.ServerMessage) error

	RewriteClientMessage(ctx context.Context, original pgwire.ClientMessage, rewritten pgwire.ClientMessage, responseHandler ResponseHandler) error
	RewriteServerMessage(ctx context.Context, original pgwire.ServerMessage, rewritten pgwire.ServerMessage) error

	// Respond
	RespondToClient(ctx context.Context, req pgwire.ClientMessage, res pgwire.ServerMessage) error
	// RespondToServer: not possible

	// Send
	SendToServer(ctx context.Context, msg pgwire.ClientMessage, responseHandler ResponseHandler) error
	SendToClient(ctx context.Context, msg pgwire.ServerMessage) error

	// Skip
	SkipClientMessage(ctx context.Context, skipped pgwire.ClientMessage) error
	SkipServerMessage(ctx context.Context, skipped pgwire.ServerMessage) error

	// Errors
	TerminateClient(ctx context.Context, err error) error
	TerminateServer(ctx context.Context, err error) error
	TerminateBoth(ctx context.Context, err error) error
	UnexpectedError(ctx context.Context, err error) error
}

func ApplyAction(ctx context.Context, handler ActionHandler, action Action) error {
	a := action.unwrap()

	switch a.t {
	case ProxyForward:
		switch a.From() {
		case RoleClient:
			return handler.ForwardClientMessage(ctx, a.incoming.(pgwire.ClientMessage), a.responseHandler)
		case RoleServer:
			return handler.ForwardServerMessage(ctx, a.incoming.(pgwire.ServerMessage))
		default:
			panic(fmt.Sprintf("unexpected proxy.ProxyRole: %#v", a.incoming))
		}

	case ProxyRespond:
		switch a.From() {
		case RoleClient:
			return handler.RespondToClient(ctx, a.incoming.(pgwire.ClientMessage), a.outgoing.(pgwire.ServerMessage))
		case RoleServer:
			panic("respond to server is not possible")
		default:
			panic(fmt.Sprintf("unexpected proxy.ProxyRole: %#v", a.incoming))
		}

	case ProxyRewrite:
		switch a.From() {
		case RoleClient:
			return handler.RewriteClientMessage(ctx, a.incoming.(pgwire.ClientMessage), a.outgoing.(pgwire.ClientMessage), a.responseHandler)
		case RoleServer:
			return handler.RewriteServerMessage(ctx, a.incoming.(pgwire.ServerMessage), a.outgoing.(pgwire.ServerMessage))
		default:
			panic(fmt.Sprintf("unexpected proxy.ProxyRole: %#v", a.incoming))
		}

	case ProxySend:
		switch a.To() {
		case RoleClient:
			return handler.SendToClient(ctx, a.outgoing.(pgwire.ServerMessage))
		case RoleServer:
			return handler.SendToServer(ctx, a.outgoing.(pgwire.ClientMessage), a.responseHandler)
		default:
			panic("unexpected proxy.ProxyRole")
		}

	case ProxySkip:
		switch a.From() {
		case RoleClient:
			return handler.SkipClientMessage(ctx, a.incoming.(pgwire.ClientMessage))
		case RoleServer:
			return handler.SkipServerMessage(ctx, a.incoming.(pgwire.ServerMessage))
		default:
			panic(fmt.Sprintf("unexpected proxy.ProxyRole: %#v", a.incoming))
		}

	case ProxyTerminateBoth:
		return handler.TerminateBoth(ctx, a.err)
	case ProxyTerminateClient:
		return handler.TerminateClient(ctx, a.err)
	case ProxyTerminateServer:
		return handler.TerminateServer(ctx, a.err)
	case ProxyUnexpectedError:
		return handler.UnexpectedError(ctx, a.err)

	default:
		panic(fmt.Sprintf("unexpected proxy.ActionType: %#v", a.t))
	}
}

func ApplyActionEffects(ctx context.Context, effectHandler pure.EffectHandler, action Action) error {
	return effectHandler(ctx, action.unwrap().effects)
}
