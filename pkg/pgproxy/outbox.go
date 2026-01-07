package pgproxy

import (
	"fmt"

	"github.com/gammazero/deque"
	"github.com/justjake/pglink/pkg/pgwire"
)

// Outbox is a queue of messages to be written to a proxy connection.
// It is not concurrency safe besides [Outbox.NotifyStreamingComplete].
//
// Buffers in the outbox are assumed to be borrowed from a sender buffer.
// If messages need to outlive the lifetime of the borrow, call [Outbox.Retain].
type Outbox struct {
	queue deque.Deque[pgwire.Msg]
	// Tracks how many messages at the front of the queue are retained.
	retainedLen int
}

func (o *Outbox) PushMsg(msg pgwire.Msg) {
	o.queue.Grow(1)
	o.queue.PushBack(msg)
}

func (o *Outbox) String() string {
	return fmt.Sprintf("Outbox{queueLen: %v, retainedLen: %d}", o.queue.Len(), o.retainedLen)
}

func (o *Outbox) Len() int {
	return o.queue.Len()
}

// Peek returns the first message in the queue, and if it needs streaming, the sender name to stream from.
// If the queue is empty, ok is false.
func (o *Outbox) Peek(i int) (msg pgwire.Msg, streamFrom pgwire.Sender, ok bool) {
	if i >= o.queue.Len() {
		return pgwire.Msg{}, pgwire.SenderNone, false
	}

	msg = o.queue.At(i)
	if msg.IsIncomplete() {
		return msg, msg.Sender, true
	}

	return msg, pgwire.SenderNone, true
}

// Discard the first n messages in the queue.
// Panics if n > len(o.queue).
func (o *Outbox) Discard(n int) {
	for range n {
		o.queue.PopFront()
	}
}

// Next removes and returns the first message in the queue, and if it needs streaming, the sender name to stream from.
func (o *Outbox) Next() (msg pgwire.Msg, streamFrom pgwire.Sender, ok bool) {
	msg, streamFrom, ok = o.Peek(0)
	if ok {
		o.Discard(1)
	}
	return
}

// Retain deep-copies any messages left in the queue to prevent aliasing stale buffers.
func (o *Outbox) Retain() {
	startIdx := o.retainedLen
	endIdx := o.queue.Len()
	for i := startIdx; i < endIdx; i++ {
		msg := o.queue.At(i)
		o.queue.Set(i, msg.Copy())
	}
	o.retainedLen = o.queue.Len()
}

func (o *Outbox) Clear() {
	o.queue.Clear()
	o.retainedLen = 0
}
