package backend

import (
	"context"
	"sync/atomic"
)

type ReadResult[T any] struct {
	Value T
	Error error
}

const (
	ChanReaderNotReading uint32 = iota
	ChanReaderStarted
	ChanReaderReading
	ChanReaderSendingResult
	ChanReaderWaitingForContinue
)

type ChanReader[T any] struct {
	read       func() (*T, error)
	outCh      chan ReadResult[T]
	continueCh chan bool
	state      atomic.Uint32
}

func NewChanReader[T any](read func() (*T, error)) *ChanReader[T] {
	return &ChanReader[T]{
		read:       read,
		outCh:      make(chan ReadResult[T]),
		continueCh: make(chan bool),
	}
}

func (r *ChanReader[T]) ReadingChan() <-chan ReadResult[T] {
	if r.state.CompareAndSwap(ChanReaderNotReading, ChanReaderStarted) {
		go r.readerLoop()
	}
	return r.outCh
}

func (r *ChanReader[T]) State() uint32 {
	return r.state.Load()
}

func (r *ChanReader[T]) Continue() {
	if r.State() == ChanReaderNotReading {
		// TODO: confusing semantics
		// TODO: rethink this whole idea
		panic("reader not reading")
	}
	r.continueCh <- true
}

func (r *ChanReader[T]) Cancel() {
	if r.State() == ChanReaderNotReading {
		return
	}
	r.continueCh <- false
}

func (r *ChanReader[T]) Read(ctx context.Context) (out T, err error) {
	select {
	case <-ctx.Done():
		err = ctx.Err()
		return
	case async := <-r.ReadingChan():
		out = async.Value
		err = async.Error
		return
	}
}

func (r *ChanReader[T]) readerLoop() {
	defer r.state.Store(ChanReaderNotReading)
	for {
		r.state.Store(ChanReaderReading)
		value, err := r.read()
		if value == nil && err == nil {
			// EOF
			return
		}

		r.state.Store(ChanReaderSendingResult)
		var result ReadResult[T]
		result.Error = err
		if value != nil {
			result.Value = *value
		}
		r.outCh <- result

		if err != nil {
			// Other error
			return
		}

		r.state.Store(ChanReaderWaitingForContinue)
		shouldReadAgain := <-r.continueCh
		if !shouldReadAgain {
			return
		}
	}
}
