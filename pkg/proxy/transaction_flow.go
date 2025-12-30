package proxy

import (
	"context"
	"time"

	"github.com/justjake/pglink/pkg/pgwire"
)

type TransactionFlow struct {
	// transaction timeout start time
	StartTime time.Time
	// idle transaction timemout start time
	LastServerMessageTime time.Time
	EndTime               time.Time
	TxStatus              pgwire.TxStatus
}

func NewTransactionFlowTracker(onComplete FlowCompleteHandler[TransactionFlow]) FlowTracker[TransactionFlow] {
	return NewFlowTracker(onComplete, waitingForQuery)
}

func waitingForQuery(ctx context.Context, state FlowState[TransactionFlow], msg pgwire.Message) (bool, FlowState[TransactionFlow], FlowReducer[TransactionFlow], error) {
	transactionStarted := func() (bool, FlowState[TransactionFlow], FlowReducer[TransactionFlow], error) {
		state.Active = true
		state.Flow = TransactionFlow{
			StartTime: time.Now(),
			TxStatus:  pgwire.TxInTransaction,
		}
		return true, state, inTransaction, nil
	}
	switch msg.(type) {
	case *pgwire.ClientSimpleQueryQuery:
		return transactionStarted()
	case *pgwire.ClientExtendedQueryParse:
		return transactionStarted()
	case *pgwire.ClientExtendedQueryBind:
		return transactionStarted()
	case *pgwire.ClientExtendedQueryExecute:
		return transactionStarted()
	case *pgwire.ClientSimpleQueryFunctionCall:
		return transactionStarted()
	default:
		return false, state, waitingForQuery, nil
	}
}

var inTransactionHandlers = ServerFlowReducers[TransactionFlow]{
	Default: func(ctx context.Context, msg pgwire.ServerMessage, state MessageFlowState[TransactionFlow]) (MessageFlowState[TransactionFlow], error) {
		state.State.Flow.LastServerMessageTime = time.Now()
		return state, nil
	},
	ReadyForQuery: func(ctx context.Context, msg *pgwire.ServerResponseReadyForQuery, state MessageFlowState[TransactionFlow]) (MessageFlowState[TransactionFlow], error) {
		status := pgwire.TxStatus(msg.TxStatusByte())
		state.State.Flow.TxStatus = status
		if status == pgwire.TxIdle {
			state.State.Flow.EndTime = time.Now()
			state.State.Active = false
			state.Reducer = nil // will get reset. using waitingForQuery causes dependency cycle.
		}
		return state, nil
	},
}

func inTransaction(ctx context.Context, state FlowState[TransactionFlow], msg pgwire.Message) (bool, FlowState[TransactionFlow], FlowReducer[TransactionFlow], error) {
	if msg, ok := msg.(pgwire.ServerMessage); ok {
		reducerState := MessageFlowState[TransactionFlow]{
			State:   state,
			Reducer: inTransaction,
		}
		var err error
		reducerState, err = inTransactionHandlers.Handle(ctx, msg, reducerState)
		if err != nil {
			return false, state, inTransaction, err
		}
		return true, reducerState.State, reducerState.Reducer, nil
	}

	return false, state, inTransaction, nil
}
