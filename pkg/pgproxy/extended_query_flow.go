package pgproxy

import (
	"context"

	"github.com/justjake/pglink/pkg/pgwire"
)

type ExtendedQueryFlow struct {
	Err pgwire.ErrorResponse
}

func NewExtendedQueryFlowTracker(onComplete FlowCompleteHandler[ExtendedQueryFlow]) FlowTracker[ExtendedQueryFlow] {
	return NewFlowTracker(onComplete, waitingForExtendedQueryRequest)
}

func waitingForExtendedQueryRequest(ctx context.Context, state FlowState[ExtendedQueryFlow], msg FlowMsg) (bool, FlowState[ExtendedQueryFlow], FlowReducer[ExtendedQueryFlow], error) {
	switch msg.Typed().(type) {
	case pgwire.Parse:
		return extendedQueryActive(ctx, StartedFlowState(state, ExtendedQueryFlow{}), msg)
	case pgwire.Bind:
		return extendedQueryActive(ctx, StartedFlowState(state, ExtendedQueryFlow{}), msg)
	case pgwire.Execute:
		return extendedQueryActive(ctx, StartedFlowState(state, ExtendedQueryFlow{}), msg)
	case pgwire.Describe:
		return extendedQueryActive(ctx, StartedFlowState(state, ExtendedQueryFlow{}), msg)
	case pgwire.Close:
		return extendedQueryActive(ctx, StartedFlowState(state, ExtendedQueryFlow{}), msg)
	case pgwire.Sync:
		return extendedQueryActive(ctx, StartedFlowState(state, ExtendedQueryFlow{}), msg)
	default:
		return false, state, waitingForExtendedQueryRequest, nil
	}
}

func extendedQueryActive(ctx context.Context, state FlowState[ExtendedQueryFlow], msg FlowMsg) (bool, FlowState[ExtendedQueryFlow], FlowReducer[ExtendedQueryFlow], error) {
	switch msg := msg.Typed().(type) {
	case pgwire.ErrorResponse:
		state.Flow.Err = msg.CopyTyped()
		return true, state, extendedQueryActive, nil

	case pgwire.ReadyForQuery:
		return true, EndedFlowState(state), waitingForExtendedQueryRequest, nil
	}

	return false, state, extendedQueryActive, nil
}
