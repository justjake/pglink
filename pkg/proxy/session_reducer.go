package proxy

import (
	"context"

	"github.com/justjake/pglink/pkg/pure"
)

type WithAction[S any] struct {
	State  S
	Action Action
}

type SessionReducer[S any] = pure.Reducer[WithAction[S], Pos]

func ReduceSessionMessages[S any](ctx context.Context, session *Session, reducer SessionReducer[S], state S) (S, error) {
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
			reducer = newReducer
			state = newState.State
		}

		// pos.SetAction(newState.Action)
	}
}
