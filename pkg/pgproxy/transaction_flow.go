package pgproxy

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

func waitingForTransactionStart(ctx context.Context, state FlowState[TransactionFlow], msg FlowMsg) (bool, FlowState[TransactionFlow], FlowReducer[TransactionFlow], error) {
	transactionStarted := func() (bool, FlowState[TransactionFlow], FlowReducer[TransactionFlow], error) {
		return true, StartedFlowState(state, TransactionFlow{}), inTransaction, nil
	}
	switch msg.Typed().(type) {
	case pgwire.Query:
		return transactionStarted()
	case pgwire.Parse:
		return transactionStarted()
	case pgwire.Bind:
		return transactionStarted()
	case pgwire.Execute:
		return transactionStarted()
	case pgwire.FunctionCall:
		return transactionStarted()
	default:
		return false, state, waitingForTransactionStart, nil
	}
}

func inTransaction(ctx context.Context, state FlowState[TransactionFlow], msg FlowMsg) (bool, FlowState[TransactionFlow], FlowReducer[TransactionFlow], error) {
	if msg, ok := msg.Typed().(pgwire.ServerMsg); ok {
		state.Flow.LastServerMessageTime = time.Now()

		if msg, ok := msg.(pgwire.ReadyForQuery); ok {
			tx, err := msg.TxStatus()
			if err != nil {
				return false, state, inTransaction, err
			}
			state.Flow.TxStatus = tx
			if state.Flow.TxStatus == pgwire.TxIdle {
				return true, EndedFlowState(state), waitingForTransactionStart, nil
			}
		}

		return true, state, inTransaction, nil
	}

	return false, state, inTransaction, nil
}
