package proxy

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/justjake/pglink/pkg/pgwire"
)

type ExtendedQueryFlow struct {
	StartTime time.Time
	//// We could have more than one of these...
	// SQL           string
	// StatementName string
	// PortalName    string
	// CommandTag    pgconn.CommandTag
	Err      *pgproto3.ErrorResponse
	RowCount int64
}

func NewExtendedQueryFlowTracker(onComplete FlowCompleteHandler[ExtendedQueryFlow]) FlowTracker[ExtendedQueryFlow] {
	return NewFlowTracker(onComplete, waitingForExtendedQueryRequest)
}

func waitingForExtendedQueryRequest(ctx context.Context, state FlowState[ExtendedQueryFlow], msg pgwire.Message) (bool, FlowState[ExtendedQueryFlow], FlowReducer[ExtendedQueryFlow], error) {
	switch msg := msg.(type) {
	case pgwire.ClientExtendedQuery:
		state.Active = true
		state.Flow = ExtendedQueryFlow{
			StartTime: time.Now(),
		}
		return extendedQueryActive(ctx, state, msg)
	default:
		return false, state, waitingForExtendedQueryRequest, nil
	}
}

func extendedQueryActive(ctx context.Context, state FlowState[ExtendedQueryFlow], msg pgwire.Message) (bool, FlowState[ExtendedQueryFlow], FlowReducer[ExtendedQueryFlow], error) {
	switch msg := msg.(type) {
	case pgwire.ClientExtendedQuery:
		// TODO: various accounting...
		switch msg := msg.(type) {
		case *pgwire.ClientExtendedQueryBind:
		case *pgwire.ClientExtendedQueryClose:
		case *pgwire.ClientExtendedQueryDescribe:
		case *pgwire.ClientExtendedQueryExecute:
		case *pgwire.ClientExtendedQueryFlush:
		case *pgwire.ClientExtendedQueryParse:
		case *pgwire.ClientExtendedQuerySync:
		default:
			panic(fmt.Sprintf("unexpected pgwire.ClientExtendedQuery: %T", msg))
		}

	case *pgwire.ServerResponseDataRow:
		state.Flow.RowCount++
		return true, state, extendedQueryActive, nil
	case *pgwire.ServerResponseErrorResponse:
		state.Flow.Err = msg.Parse()
		return true, state, waitingForExtendedQueryRequest, nil

	case *pgwire.ServerResponseReadyForQuery:
		// End.
		state.Active = false
		return true, state, waitingForExtendedQueryRequest, nil
	}

	return false, state, extendedQueryActive, nil
}

var _ FlowReducer[ExtendedQueryFlow] = waitingForExtendedQueryRequest
var _ FlowReducer[ExtendedQueryFlow] = extendedQueryActive
