package pgwire

import (
	"bytes"
	"fmt"
	"io"

	"github.com/gammazero/deque"
	"github.com/jackc/pgx/v5/pgproto3"
)

type queueItem struct {
	prefix bytes.Buffer
	suffix RingRange
}

type WriteQueue struct {
	deque deque.Deque[*queueItem]
}

func (q *WriteQueue) IsEmpty() bool {
	if q.deque.Len() == 0 {
		return true
	}
	return q.deque.Back().prefix.Len() == 0 && q.deque.Back().suffix.Len() == 0
}

func (q *WriteQueue) AvailableBuffer() []byte {
	return q.getByteSlot().AvailableBuffer()
}

func (q *WriteQueue) Write(bytes []byte) (int, error) {
	return q.getByteSlot().Write(bytes)
}

func (q *WriteQueue) WriteMsg(msg Message) error {
	if msg.Source() != nil {
		return q.WriteRawMsg(msg.Source())
	}
	if msg.IsParsed() {
		return q.WriteParsedMsg(msg.ParseAny())
	}
	return fmt.Errorf("message appears blank: %T", msg)
}

func (q *WriteQueue) WriteParsedMsg(msg pgproto3.Message) error {
	item := q.getByteSlot()
	buf := item.AvailableBuffer()
	buf, err := msg.Encode(buf)
	if err != nil {
		return err
	}
	_, err = item.Write(buf)
	if err != nil {
		return err
	}
	return nil
}

func (q *WriteQueue) WriteRawMsg(msg RawMessageSource) error {
	if ringMsg, ok := msg.(*RingMsg); ok {
		return q.WriteRingMsg(ringMsg)
	}
	_, err := msg.WriteTo(q)
	return err
}

func (q *WriteQueue) WriteRingMsg(r *RingMsg) error {
	return q.WriteRingRange(r.ToRange())
}

func (q *WriteQueue) WriteRingRange(r *RingRange) error {
	item := q.getRingSlot()
	if item.Empty() {
		*item = *r
	} else if item.End() == r.Start() && item.ring == r.ring {
		item.Extend(r)
	} else {
		q.pushItem().suffix = *r
	}
	// TODO: error unless item is valid
	return nil
}

func (q *WriteQueue) WriteTo(w io.Writer) (int64, error) {
	total := int64(0)
	for item := range q.deque.Iter() {
		n, err := w.Write(item.prefix.Bytes())
		total += int64(n)
		if err != nil {
			return total, err
		}

		r := item.suffix.NewReader()
		// In many cases, r will implement WriterTo, avoinding a buffer allocation entirely.
		// It remains to be seen if we're better off w/ a buffer allocation here.
		n64, err := io.Copy(w, r)
		total += n64
		if err != nil {
			return total, err
		}
	}
	return total, nil
}

func (q *WriteQueue) Clear() {
	if q.deque.Len() == 0 {
		return
	}

	if q.deque.Len() > 1 {
		retain := q.deque.Front()
		q.deque.Clear()
		q.deque.PushBack(retain)
	}

	item := q.deque.Front()
	item.prefix.Reset()
	item.suffix = RingRange{}
}

func (q *WriteQueue) pushItem() *queueItem {
	item := &queueItem{}
	q.deque.PushBack(item)
	return item
}

func (q *WriteQueue) getByteSlot() *bytes.Buffer {
	if q.deque.Len() == 0 || q.deque.Back().suffix.Len() != 0 {
		return &q.pushItem().prefix
	}

	return &q.deque.Back().prefix
}

func (q *WriteQueue) getRingSlot() *RingRange {
	if q.deque.Len() == 0 {
		return &q.pushItem().suffix
	}

	return &q.deque.Back().suffix
}
