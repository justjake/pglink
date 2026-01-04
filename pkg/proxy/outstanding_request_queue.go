package proxy

import (
	"context"
	"fmt"

	"github.com/justjake/pglink/pkg/pgwire"
	"github.com/justjake/pglink/pkg/pure"
)

type OutstandingRequestQueue struct {
	seq           int
	outstanding   []*OutstandingRequest
	lastCompleted *OutstandingRequest
}

var _ MessageTracker = (*OutstandingRequestQueue)(nil)

func (q *OutstandingRequestQueue) Len() int {
	return len(q.outstanding)
}

func (q *OutstandingRequestQueue) LastCompleted() *OutstandingRequest {
	return q.lastCompleted
}

func (q *OutstandingRequestQueue) FirstOutstanding() *OutstandingRequest {
	if len(q.outstanding) != 0 {
		return q.outstanding[0]
	}
	return nil
}

func (q *OutstandingRequestQueue) LastOutstanding() *OutstandingRequest {
	if len(q.outstanding) != 0 {
		return q.outstanding[len(q.outstanding)-1]
	}
	return nil
}

func (q *OutstandingRequestQueue) GetResponseHandler(res pgwire.ServerMessage) *OutstandingRequest {
	if req := q.LastCompleted(); req != nil {
		if pgwire.MsgTerminalResponse.Get(req.flowState.Flow.ReqType).Contains(res.MsgType()) {
			return req
		}
	}

	if req := q.FirstOutstanding(); req != nil {
		if pgwire.MsgResponse.Get(req.flowState.Flow.ReqType).Contains(res.MsgType()) {
			return req
		}
	}

	return nil
}

func (q *OutstandingRequestQueue) TrackEffect(msg pgwire.Message) pure.Effect {
	return pure.DoNamedCleanup(fmt.Sprintf("OutstandingRequestQueue.Track(%T)", msg), func(ctx context.Context) (cleanup pure.Effect, err error) {
		_, err = q.trackNow(ctx, msg)
		return
	})
}

func (q *OutstandingRequestQueue) TrackMessage(ctx context.Context, msg pgwire.Message) (context.Context, error) {
	_, err := q.trackNow(ctx, msg)
	return ctx, err
}

func (q *OutstandingRequestQueue) trackNow(ctx context.Context, msg pgwire.Message) (bool, error) {
	// The message may be a response to the earliest outstanding request.
	if req := q.FirstOutstanding(); req != nil {
		changed, flowState, _, err := inOutstandingRequest(ctx, req.flowState, msg)
		if err != nil {
			return false, err
		}

		if changed {
			req.flowState = flowState
			// Once the request is complete, remove it from the queue.
			if !flowState.Active {
				q.lastCompleted = req
				q.outstanding = q.outstanding[1:]
			}
			return true, nil
		}
	}

	// If the msg wasn't handled by the earliest outstanding request, it may need
	// to start a new request.
	start, newFlowState, _, err := waitingForRequestStart(ctx, FlowState[RequestFlow]{Seq: q.seq}, msg)
	if err != nil {
		return false, err
	}
	if start {
		q.seq = newFlowState.Seq
		req := &OutstandingRequest{flowState: newFlowState}
		q.outstanding = append(q.outstanding, req)
		return true, nil
	}

	return false, nil
}

type OutstandingRequest struct {
	flowState FlowState[RequestFlow]
	handler   ResponseHandler
}

func (r *OutstandingRequest) Seq() int {
	return r.flowState.Seq
}

func (r *OutstandingRequest) ReqType() pgwire.MsgType {
	return r.flowState.Flow.ReqType
}

func (r *OutstandingRequest) FlowState() FlowState[RequestFlow] {
	return r.flowState
}

func (r *OutstandingRequest) String() string {
	return fmt.Sprintf("OutstandingRequest(%d %v)", r.Seq(), r.ReqType())
}

func (r *OutstandingRequest) Handle(ctx context.Context, msg pgwire.ServerMessage) (Action, bool) {
	if r.handler == nil {
		return nil, false
	}

	changed, action, state, err := r.handler(ctx, nil, ResponseEvent{Res: msg, Req: r.flowState})
	if err != nil {
		return UnexpectedError(msg, err), true
	}

	effect := pure.NoOp()
	if changed {
		effect = r.SetResponseHandlerEffect(state)
	}
	if action == nil {
		action = Forward(msg)
		if effect != nil {
			action = action.WithEffects(effect)
		}
		return action, true
	} else {
		return action.WithEffects(effect), true
	}
}

func (r *OutstandingRequest) SetResponseHandlerEffect(state ResponseHandler) pure.Effect {
	return pure.DoNamed(fmt.Sprintf("%v.SetState(%s)", r, pure.DescribeFunction(state)), func() {
		r.handler = state
	})
}

type RequestFlow struct {
	ReqType pgwire.MsgType
}

func waitingForRequestStart(ctx context.Context, state FlowState[RequestFlow], msg pgwire.Message) (bool, FlowState[RequestFlow], FlowReducer[RequestFlow], error) {
	msgType := msg.MsgType()
	responseTypes := pgwire.MsgResponse.Get(msgType)
	if len(responseTypes) != 0 {
		return true, StartedFlowState(state, RequestFlow{ReqType: msgType}), inOutstandingRequest, nil
	}

	return false, state, waitingForRequestStart, nil
}

func inOutstandingRequest(ctx context.Context, state FlowState[RequestFlow], msg pgwire.Message) (bool, FlowState[RequestFlow], FlowReducer[RequestFlow], error) {
	responseTypes := pgwire.MsgResponse.Get(state.Flow.ReqType)
	if pgwire.MsgTypeIndex(responseTypes, msg.MsgType()) != -1 {
		return true, EndedFlowState(state), nil, nil
	}

	return false, state, inOutstandingRequest, nil
}
