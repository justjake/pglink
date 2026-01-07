package pgproxy

import (
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
	Res FlowMsg
	// Original request message. Parsed data may not be available.
	Req FlowState[RequestFlow]
}

// ResponseHandler is a state machine that handles a response message.
// It must return the Action to perform to handle the response message.
type ResponseHandler = pure.Reducer[Action, ResponseEvent]
