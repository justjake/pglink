package proxy

import (
	"context"

	"github.com/justjake/pglink/pkg/pgwire"
	"github.com/justjake/pglink/pkg/pure"
)

// ResponseHandlerResult is the arguments for a response handler state.
type ResponseHandlerResult struct {
	// If nil, signals the response handler did not handle the message.
	// Otherwise, the action is performed.
	Action Action
}

type ResponseEvent struct {
	// Input. Not required to be returned by the response handler.
	Res pgwire.ServerMessage
	// Original request message. Parsed data may not be available.
	ReqType pgwire.MsgType
}

// ResponseHandler is a state machine that handles a response message.
// It must return the Action to perform to handle the response message.
type ResponseHandler = pure.Reducer[Action, ResponseEvent]

type MessageHandlerResult struct {
	Action  Action
	State   ResponseHandler
	Changed bool // this is getting annoying, TODO remove
}

// TODO: lots of hacking around pgwire.ServerResponseHandlers to inject context.
type MessageResponseHandlers = pgwire.ServerHandlers[pgwire.MsgType, MessageHandlerResult]

// TODO: lots of hacking around pgwire.ServerResponseHandlers to inject context.
func ResponseHandlers(handlers MessageResponseHandlers) ResponseHandler {
	return func(ctx context.Context, _ Action, event ResponseEvent) (bool, Action, ResponseHandler, error) {
		messageResult, err := handlers.Handle(ctx, event.Res, event.ReqType)
		return messageResult.Changed, messageResult.Action, messageResult.State, err
	}
}
