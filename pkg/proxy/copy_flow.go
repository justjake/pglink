package proxy

import (
	"context"
	"time"

	"github.com/justjake/pglink/pkg/pgwire"
)

type CopyFlow struct {
	StartTime   time.Time
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
		state.Flow = CopyFlow{Mode: pgwire.CopyBoth, StartTime: time.Now()}
		state.Active = true
		return true, state, copyActive, nil
	case *pgwire.ServerCopyCopyInResponse:
		state.Flow = CopyFlow{Mode: pgwire.CopyIn, StartTime: time.Now()}
		state.Active = true
		return true, state, copyActive, nil
	case *pgwire.ServerCopyCopyOutResponse:
		state.Flow = CopyFlow{Mode: pgwire.CopyOut, StartTime: time.Now()}
		state.Active = true
		return true, state, copyActive, nil
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
		state.Active = false
		return true, state, nil, nil
	case *pgwire.ServerResponseErrorResponse:
		state.Active = false
		return true, state, nil, nil

	default:
		// Continue
		return false, state, copyActive, nil
	}
}

var _ FlowReducer[CopyFlow] = waitingForCopyResponse
var _ FlowReducer[CopyFlow] = copyActive
