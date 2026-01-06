package pgproxy

import (
	"context"

	"github.com/justjake/pglink/pkg/pgwire"
)

// TerminateFlow tracks the flow of a terminate message.
// No data is captured besides the flow start time.
type TerminateFlow struct{}

func NewTerminateFlowTracker() FlowTracker[TerminateFlow] {
	return NewFlowTracker(nil, waitingForTerminate)
}

func waitingForTerminate(ctx context.Context, state FlowState[TerminateFlow], msg pgwire.Message) (bool, FlowState[TerminateFlow], FlowReducer[TerminateFlow], error) {
	switch msg.(type) {
	case *pgwire.ClientTerminate:
		return true, StartedFlowState(state, TerminateFlow{}), waitingForTerminate, nil
	}
	return false, state, waitingForTerminate, nil
}
