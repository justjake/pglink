package proxy

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
	case *pgwire.ServerCopyCopyBothResponse:
		return true, StartedFlowState(CopyFlow{Mode: pgwire.CopyBoth}), copyActive, nil
	case *pgwire.ServerCopyCopyInResponse:
		return true, StartedFlowState(CopyFlow{Mode: pgwire.CopyIn}), copyActive, nil
	case *pgwire.ServerCopyCopyOutResponse:
		return true, StartedFlowState(CopyFlow{Mode: pgwire.CopyOut}), copyActive, nil
	default:
		return false, state, waitingForCopyResponse, nil
	}
}

func copyActive(ctx context.Context, state FlowState[CopyFlow], msg pgwire.Message) (bool, FlowState[CopyFlow], FlowReducer[CopyFlow], error) {
	switch msg := msg.(type) {
	case *pgwire.ServerCopyCopyData:
		state.Flow.ServerBytes += int64(msg.DataSize())
		return true, state, copyActive, nil
	case *pgwire.ClientCopyCopyData:
		state.Flow.ClientBytes += int64(msg.DataSize())
		return true, state, copyActive, nil

	case *pgwire.ServerResponseCommandComplete:
		return true, EndedFlowState(state), nil, nil
	case *pgwire.ServerResponseErrorResponse:
		return true, EndedFlowState(state), nil, nil

	default:
		// Continue
		return false, state, copyActive, nil
	}
}

var _ FlowReducer[CopyFlow] = waitingForCopyResponse
var _ FlowReducer[CopyFlow] = copyActive
