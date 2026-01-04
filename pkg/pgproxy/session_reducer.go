package pgproxy

import (
	"context"

	"github.com/justjake/pglink/pkg/pure"
)

type WithAction[S any] struct {
	State  S
	Action Action
	Ctx    context.Context
}

type SessionReducer[S any] = pure.Reducer[WithAction[S], Pos]

// RunSessionReducer provides a functional approach to writing proxy logic.
// It runs the reducer for each message in the session, and dispatches the returned action.
func RunSessionReducer[S any](ctx context.Context, session *Session, reducer SessionReducer[S], state S) (S, error) {
	for {
		pos, err := session.Next(ctx)
		if err != nil {
			return state, err
		}

		changed, newState, newReducer, err := reducer(ctx, WithAction[S]{State: state}, pos)
		if err != nil {
			return state, err
		}

		if changed {
			if newState.Ctx != nil {
				ctx = newState.Ctx
			}
			reducer = newReducer
			state = newState.State
		}

		actionErr := pos.Dispatch(pos.Ctx(), newState.Action)
		if actionErr != nil {
			return state, actionErr
		}
	}
}
