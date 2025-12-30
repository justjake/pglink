package proxy

import (
	"bytes"
	"context"
	"fmt"
	"time"
	"unsafe"

	"github.com/justjake/pglink/pkg/pgwire"
	"github.com/justjake/pglink/pkg/pure"
)

type OutstandingRequestQueue struct {
	seq      int
	requests []*OutstandingRequest
}

// Mutation must be private (return effects)
func (q *OutstandingRequestQueue) push(req *OutstandingRequest) {
	q.seq++
	req.seq = q.seq
	q.requests = append(q.requests, req)
}

func (q *OutstandingRequestQueue) drop(req *OutstandingRequest) {
	if q.requests[0] == req {
		q.requests = q.requests[1:]
	} else {
		for i, r := range q.requests {
			if r == req {
				q.requests = append(q.requests[:i], q.requests[i+1:]...)
				break
			}
		}
	}
}

func (q *OutstandingRequestQueue) WaitingFor(res pgwire.ServerMessage) *OutstandingRequest {
	if len(q.requests) == 0 {
		return nil
	}

	first := q.requests[0]
	responseTypes := pgwire.MsgResponse.Get(first.RequestType)
	if len(responseTypes) == 0 {
		return nil
	}

	// TODO: need to add .MsgType() to ServerMessage interface
	responseTypesBytes := unsafe.Slice((*byte)(unsafe.SliceData(responseTypes)), len(responseTypes))
	if bytes.IndexByte(responseTypesBytes, byte(res.MsgType())) != -1 {
		return first
	}

	return nil
}

func (q *OutstandingRequestQueue) EnqueueRequestEffect(msg pgwire.ClientMessage, state ResponseHandler) pure.Effect {
	req := &OutstandingRequest{
		RequestType: msg.MsgType(),
		StartTime:   time.Now(),
		handler:     state,
		q:           q,
		seq:         q.seq + 1, // prediction
	}
	return req.enqueueEffect()
}

type OutstandingRequest struct {
	RequestType pgwire.MsgType // should be a client message
	StartTime   time.Time
	handler     ResponseHandler
	// TODO: remove this pgbouncer representation, perhaps..?
	// waitingFor []pgwire.MsgType
	q   *OutstandingRequestQueue
	seq int
}

func (r *OutstandingRequest) String() string {
	return fmt.Sprintf("OutstandingRequest(%d %s)", r.seq, r.RequestType)
}

func (r *OutstandingRequest) Handle(ctx context.Context, msg pgwire.ServerMessage) Action {
	out, state, err := r.handler(ctx, ResponseHandlerArgs{Res: msg})
	if err != nil {
		return UnexpectedError(msg, err)
	}

	action := out.Action
	if action == nil {
		return Forward(msg, r.setStateEffect(state))
	} else {
		return action.WithEffects(r.setStateEffect(state))
	}
}

func (r *OutstandingRequest) setStateEffect(state ResponseHandler) pure.Effect {
	if state == nil {
		return r.dequeueEffect()
	}
	return pure.DoNamed(fmt.Sprintf("SetState(%s = %s)", r.String(), pure.DescribeFunction(state)), func() {
		r.handler = state
	})
}

func (r *OutstandingRequest) dequeue() {
	r.q.drop(r)
}

func (r *OutstandingRequest) dequeueEffect() pure.Effect {
	return pure.DoNamed(fmt.Sprintf("Drop(%s)", r.String()), r.dequeue)
}

func (r *OutstandingRequest) enqueue() {
	r.q.push(r)
}

func (r *OutstandingRequest) enqueueEffect() pure.Effect {
	return pure.DoNamed(fmt.Sprintf("Enqueue(%s)", r.String()), r.enqueue)
}
