package pure

import "context"

type Store[T any] interface {
	Value() T
	SetEffect(next T) Effect
	UpdateEffect(update func(T) T) Effect
	RevertEffect() Effect
}

type store[T any] struct {
	value T
}

func NewStore[T any](value T) Store[T] {
	return &store[T]{value}
}

func (s *store[T]) Value() T {
	return s.value
}

func (s *store[T]) RevertEffect() Effect {
	prev := s.value
	return DoNamed("Store.Revert", func() {
		s.value = prev
	})
}

func (s *store[T]) SetEffect(next T) Effect {
	return DoNamedCleanup("Store.Set", func(ctx context.Context) (cleanup Effect, err error) {
		cleanup = s.RevertEffect()
		s.value = next
		return
	})
}

func (s *store[T]) UpdateEffect(update func(T) T) Effect {
	return DoNamedCleanup("Store.Update", func(ctx context.Context) (cleanup Effect, err error) {
		cleanup = s.RevertEffect()
		s.value = update(s.value)
		return
	})
}
