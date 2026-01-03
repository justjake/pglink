package proxy

import (
	"context"
	"time"

	"github.com/justjake/pglink/pkg/pgwire"
)

type TransactionFlow struct {
	// idle transaction timemout start time
	LastServerMessageTime time.Time
	TxStatus              pgwire.TxStatus
}

func NewTransactionFlowTracker(onComplete FlowCompleteHandler[TransactionFlow]) FlowTracker[TransactionFlow] {
	return NewFlowTracker(onComplete, waitingForTransactionStart)
}

func waitingForTransactionStart(ctx context.Context, state FlowState[TransactionFlow], msg pgwire.Message) (bool, FlowState[TransactionFlow], FlowReducer[TransactionFlow], error) {
	transactionStarted := func() (bool, FlowState[TransactionFlow], FlowReducer[TransactionFlow], error) {
		return true, StartedFlowState(state, TransactionFlow{}), inTransaction, nil
	}
	switch msg.(type) {
	case *pgwire.ClientQuery:
		return transactionStarted()
	case *pgwire.ClientParse:
		return transactionStarted()
	case *pgwire.ClientBind:
		return transactionStarted()
	case *pgwire.ClientExecute:
		return transactionStarted()
	case *pgwire.ClientFunctionCall:
		return transactionStarted()
	default:
		return false, state, waitingForTransactionStart, nil
	}
}

func inTransaction(ctx context.Context, state FlowState[TransactionFlow], msg pgwire.Message) (bool, FlowState[TransactionFlow], FlowReducer[TransactionFlow], error) {
	if msg, ok := msg.(pgwire.ServerMessage); ok {
		state.Flow.LastServerMessageTime = time.Now()

		if msg, ok := msg.(*pgwire.ServerReadyForQuery); ok {
			state.Flow.TxStatus = msg.TxStatus()
			if state.Flow.TxStatus == pgwire.TxIdle {
				return true, EndedFlowState(state), waitingForTransactionStart, nil
			}
		}

		return true, state, inTransaction, nil
	}

	return false, state, inTransaction, nil
}
