package proxy

import (
	"context"

	"github.com/justjake/pglink/pkg/pgwire"
	"github.com/justjake/pglink/pkg/pure"
)

// ResponseHandlerArgs is the arguments for a response handler state.
type ResponseHandlerArgs struct {
	// If nil, signals the response handler did not handle the message.
	// Otherwise, the action is performed.
	Action Action

	// Input. Not required to be returned by the response handler.
	Res pgwire.ServerMessage
}

// ResponseHandler is a state machine that handles a response message.
// It must return the Action to perform to handle the response message.
type ResponseHandler = pure.State[ResponseHandlerArgs]

type MessageHandlerResult struct {
	Action Action
	State  ResponseHandler
}

// TODO: lots of hacking around pgwire.ServerResponseHandlers to inject context.
type MessageHandlerState = func(ctx context.Context) (handlers pgwire.ServerMessageHandlers[MessageHandlerResult], defaultHandler func(res pgwire.ServerMessage) (MessageHandlerResult, error))

// TODO: lots of hacking around pgwire.ServerResponseHandlers to inject context.
func ResponseHandlers(bind MessageHandlerState) ResponseHandler {
	var state ResponseHandler
	state = func(ctx context.Context, args ResponseHandlerArgs) (ResponseHandlerArgs, ResponseHandler, error) {
		handlers, defaultHandler := bind(ctx)
		if defaultHandler == nil {
			defaultHandler = func(res pgwire.ServerMessage) (MessageHandlerResult, error) {
				// Continue with same state.
				return MessageHandlerResult{State: state}, nil
			}
		}
		handlerResult, err := handlers.HandleDefault(args.Res, defaultHandler)
		if err != nil {
			return args, nil, err
		}
		return ResponseHandlerArgs{handlerResult.Action, nil}, handlerResult.State, nil
	}
	return state
}
