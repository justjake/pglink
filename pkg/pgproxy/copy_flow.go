package pgproxy

import (
	"context"

	"github.com/justjake/pglink/pkg/pgwire"
)

type CopyFlow struct {
	Mode        pgwire.CopyMode
	ServerBytes int64
	ClientBytes int64
}

func NewCopyFlowTracker(onComplete FlowCompleteHandler[CopyFlow]) FlowTracker[CopyFlow] {
	return NewFlowTracker(onComplete, waitingForCopyResponse)
}

func waitingForCopyResponse(ctx context.Context, state FlowState[CopyFlow], msg pgwire.Message) (bool, FlowState[CopyFlow], FlowReducer[CopyFlow], error) {
	switch msg.(type) {
	case *pgwire.ServerCopyBothResponse:
		return true, StartedFlowState(state, CopyFlow{Mode: pgwire.CopyBoth}), copyActive, nil
	case *pgwire.ServerCopyInResponse:
		return true, StartedFlowState(state, CopyFlow{Mode: pgwire.CopyIn}), copyActive, nil
	case *pgwire.ServerCopyOutResponse:
		return true, StartedFlowState(state, CopyFlow{Mode: pgwire.CopyOut}), copyActive, nil
	default:
		return false, state, waitingForCopyResponse, nil
	}
}

func copyActive(ctx context.Context, state FlowState[CopyFlow], msg pgwire.Message) (bool, FlowState[CopyFlow], FlowReducer[CopyFlow], error) {
	switch msg := msg.(type) {
	case *pgwire.ServerCopyData:
		state.Flow.ServerBytes += int64(msg.DataSize())
		return true, state, copyActive, nil
	case *pgwire.ClientCopyData:
		state.Flow.ClientBytes += int64(msg.DataSize())
		return true, state, copyActive, nil

	case *pgwire.ServerCommandComplete:
		return true, EndedFlowState(state), nil, nil
	case *pgwire.ServerErrorResponse:
		return true, EndedFlowState(state), nil, nil

	default:
		// Continue
		return false, state, copyActive, nil
	}
}

var _ FlowReducer[CopyFlow] = waitingForCopyResponse
var _ FlowReducer[CopyFlow] = copyActive
