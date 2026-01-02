package pure

import (
	"context"
)

// State is a function/method that receives a set of arguments as any type T and returns the arguments for the next state and the next State to run or an error.
// To end: return (T, nil, nil)
// To error: return (?, nil, error)
// https://medium.com/@johnsiilver/go-state-machine-patterns-3b667f345b5e
type State[T any] func(ctx context.Context, args T) (T, State[T], error)

// RunState runs a state machine with the given initial state and arguments until completion.
func RunState[T any](ctx context.Context, args T, initialState State[T]) (T, error) {
	var err error
	current := initialState
	for {
		if ctx.Err() != nil {
			return args, ctx.Err()
		}
		args, current, err = current(ctx, args)
		if err != nil {
			return args, err
		}
		if current == nil {
			return args, nil
		}
	}
}

// WithEffects is a wrapper type that adds output effects to a state machine's return value.
type WithEffects[T any] struct {
	T       T
	Effects Effects
}

// EffectHandler is a function that handles effects. It should schedule cleanup internally.
type EffectHandler func(ctx context.Context, effects Effects) error

func (e EffectHandler) HandleOne(ctx context.Context, effect Effect) error {
	return e(ctx, Effects{effect})
}

// RunWithEffects runs a state machine handling effects as they are produced.
func RunWithEffects[T any](ctx context.Context, handler EffectHandler, args T, initialState State[WithEffects[T]]) (WithEffects[T], error) {
	return RunState(ctx, WithEffects[T]{T: args}, WithEffectHandler(handler, initialState))
}

// WithEffectHandler binds an effect handler to a state machine.
func WithEffectHandler[T any](handler EffectHandler, state State[WithEffects[T]]) State[WithEffects[T]] {
	return func(ctx context.Context, args WithEffects[T]) (WithEffects[T], State[WithEffects[T]], error) {
		args, unboundNext, err := state(ctx, args)
		if err != nil {
			return args, nil, err
		}
		if effectErr := handler(ctx, args.Effects); effectErr != nil {
			return args, nil, effectErr
		}
		return args, WithEffectHandler(handler, unboundNext), nil
	}
}
