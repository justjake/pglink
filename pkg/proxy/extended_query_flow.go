package proxy

import (
	"context"

	"github.com/justjake/pglink/pkg/pgwire"
)

type ExtendedQueryFlow struct {
	Err *pgwire.ServerErrorResponse
}

func NewExtendedQueryFlowTracker(onComplete FlowCompleteHandler[ExtendedQueryFlow]) FlowTracker[ExtendedQueryFlow] {
	return NewFlowTracker(onComplete, waitingForExtendedQueryRequest)
}

func waitingForExtendedQueryRequest(ctx context.Context, state FlowState[ExtendedQueryFlow], msg pgwire.Message) (bool, FlowState[ExtendedQueryFlow], FlowReducer[ExtendedQueryFlow], error) {
	switch msg := msg.(type) {
	case pgwire.ClientExtendedQuery:
		return extendedQueryActive(ctx, StartedFlowState(state, ExtendedQueryFlow{}), msg)
	default:
		return false, state, waitingForExtendedQueryRequest, nil
	}
}

func extendedQueryActive(ctx context.Context, state FlowState[ExtendedQueryFlow], msg pgwire.Message) (bool, FlowState[ExtendedQueryFlow], FlowReducer[ExtendedQueryFlow], error) {
	switch msg := msg.(type) {
	case *pgwire.ServerErrorResponse:
		errorResponse := msg.Retain()
		state.Flow.Err = &errorResponse
		return true, state, extendedQueryActive, nil

	case *pgwire.ServerReadyForQuery:
		return true, EndedFlowState(state), waitingForExtendedQueryRequest, nil
	}

	return false, state, extendedQueryActive, nil
}
