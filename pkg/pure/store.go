package pure

import "context"

type Store[T any] interface {
	Value() T
	SetEffect(next T) Effect
	UpdateEffect(update func(T) T) Effect
	RevertEffect() Effect
}

type StoreState[T any] struct {
	State T
}

func NewStore[T any](value T) Store[T] {
	return &StoreState[T]{value}
}

func (s *StoreState[T]) Value() T {
	return s.State
}

func (s *StoreState[T]) RevertEffect() Effect {
	prev := s.State
	return DoNamed("Store.Revert", func() {
		s.State = prev
	})
}

func (s *StoreState[T]) SetEffect(next T) Effect {
	return DoNamedCleanup("Store.Set", func(ctx context.Context) (cleanup Effect, err error) {
		cleanup = s.RevertEffect()
		s.State = next
		return
	})
}

func (s *StoreState[T]) UpdateEffect(update func(T) T) Effect {
	return DoNamedCleanup("Store.Update", func(ctx context.Context) (cleanup Effect, err error) {
		cleanup = s.RevertEffect()
		s.UpdateNow(update)
		return
	})
}

func (s *StoreState[T]) UpdateNow(update func(T) T) T {
	state := update(s.State)
	s.State = state
	return state
}
