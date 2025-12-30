package proxy

import (
	"context"
	"fmt"

	"github.com/justjake/pglink/pkg/pgwire"
	"github.com/justjake/pglink/pkg/pure"
)

type FlowReducer[T any] = pure.Reducer[FlowState[T], pgwire.Message]
type FlowCompleteHandler[T any] = func(ctx context.Context, flow T) error

type MessageFlowState[T any] = pure.ReducerState[FlowState[T], pgwire.Message]
type ServerFlowReducers[T any] = pgwire.ServerHandlers[MessageFlowState[T], MessageFlowState[T]]
type ClientFlowReducers[T any] = pgwire.ClientHandlers[MessageFlowState[T], MessageFlowState[T]]

type FlowTracker[T any] interface {
	Flow() (T, bool)
	Active() bool
	TrackEffect(msg pgwire.Message) pure.Effect
	ResetEffect() pure.Effect
}

type FlowState[T any] struct {
	Active bool
	Flow   T
}

type flowTracker[T any] struct {
	onComplete    FlowCompleteHandler[T]
	inactiveState FlowReducer[T]
	state         pure.ReducerState[FlowState[T], pgwire.Message]
}

func NewFlowTracker[T any](onComplete FlowCompleteHandler[T], inactiveState FlowReducer[T]) FlowTracker[T] {
	return &flowTracker[T]{
		onComplete:    onComplete,
		inactiveState: inactiveState,
		state: pure.ReducerState[FlowState[T], pgwire.Message]{
			Reducer: inactiveState,
			State: FlowState[T]{
				Active: false,
			},
		},
	}
}

func (t *flowTracker[T]) Flow() (T, bool) {
	return t.state.State.Flow, t.state.State.Active
}

func (t *flowTracker[T]) Active() bool {
	return t.state.State.Active
}

func (t *flowTracker[T]) TrackEffect(msg pgwire.Message) pure.Effect {
	return pure.DoNamedCleanup(fmt.Sprintf("Track(%T)", msg), func(ctx context.Context) (cleanup pure.Effect, err error) {
		cleanup = t.state.RevertEffect()
		changed, err := t.updateNow(ctx, msg)
		if changed {
			return cleanup, err
		} else {
			return nil, err
		}
	})
}

func (t *flowTracker[T]) ResetEffect() pure.Effect {
	return pure.DoNamedCleanup(fmt.Sprintf("Reset(%T)", t.state.State.Flow), func(ctx context.Context) (cleanup pure.Effect, err error) {
		cleanup = t.state.RevertEffect()
		t.reset()
		return cleanup, nil
	})
}

func (t *flowTracker[T]) updateNow(ctx context.Context, msg pgwire.Message) (bool, error) {
	wasActive := t.state.State.Active
	changed, state, _, err := t.state.UpdateNow(ctx, msg)
	if err != nil || !changed {
		return changed, err
	}

	if wasActive && !state.Active {
		err = t.onComplete(ctx, state.Flow)
	}

	if t.state.Reducer == nil {
		t.reset()
	}

	return changed, nil
}

func (t *flowTracker[T]) reset() {
	t.state.Reducer = t.inactiveState
	t.state.State = FlowState[T]{
		Active: false,
	}
}
