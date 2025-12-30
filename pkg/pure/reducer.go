package pure

import (
	"context"
	"fmt"
)

// Hmm, a pure function shouldn't take ctx...
type Reducer[S any, A any] func(ctx context.Context, state S, action A) (bool, S, Reducer[S, A], error)

type ReducerStore[S any, A any] interface {
	Value() S
	DispatchEffect(action A) Effect
	RevertEffect() Effect
}

type ReducerState[S any, A any] struct {
	Reducer Reducer[S, A]
	State   S
}

func NewReducerStore[S any, A any](reducer Reducer[S, A], initialValue S) ReducerStore[S, A] {
	return &ReducerState[S, A]{reducer, initialValue}
}

func (s *ReducerState[S, A]) Value() S {
	return s.State
}

func (s *ReducerState[S, A]) RevertEffect() Effect {
	prev := s.State
	prevReducer := s.Reducer
	return DoNamed(fmt.Sprintf("Revert(%T)", s.State), func() {
		s.State = prev
		s.Reducer = prevReducer
	})
}

func (s *ReducerState[S, A]) DispatchEffect(action A) Effect {
	return DoNamedCleanup(fmt.Sprintf("Dispatch(%T -> %T)", action, s.State), func(ctx context.Context) (cleanup Effect, err error) {
		cleanup = s.RevertEffect()
		changed, _, _, err := s.UpdateNow(ctx, action)
		if err != nil || !changed {
			return nil, err
		}
		return cleanup, nil
	})
}

func (s *ReducerState[S, A]) UpdateNow(ctx context.Context, action A) (bool, S, Reducer[S, A], error) {
	changed, state, reducer, err := s.Reducer(ctx, s.State, action)
	if err != nil || !changed {
		return changed, state, reducer, err
	}
	s.State = state
	s.Reducer = reducer
	return changed, state, reducer, nil
}

func ReducerWithEffectHandler[S any, A any](reducer Reducer[WithEffects[S], A], handler EffectHandler) Reducer[S, A] {
	var self Reducer[S, A]
	self = func(ctx context.Context, state S, action A) (bool, S, Reducer[S, A], error) {
		changed, stateWithEffects, reducerWithEffects, err := reducer(ctx, WithEffects[S]{T: state}, action)
		if err != nil {
			return changed, stateWithEffects.T, self, err
		}

		// Handle effects.
		err = handler(ctx, stateWithEffects.Effects)
		if err != nil {
			return changed, stateWithEffects.T, self, err
		}

		return changed, stateWithEffects.T, ReducerWithEffectHandler(reducerWithEffects, handler), nil
	}
	return self
}
