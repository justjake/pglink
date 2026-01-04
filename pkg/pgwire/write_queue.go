package pgwire

import (
	"bytes"
	"fmt"
	"io"
	"net"

	"github.com/jackc/pgx/v5/pgproto3"
)

type queueItem struct {
	prefix bytes.Buffer
	suffix RingRange
}

// WriteQueue collects messages to be written in a single writev syscall.
// Uses inline structs to avoid allocation.
type WriteQueue struct {
	items    [1]queueItem // Inline storage for common case (1 item)
	overflow []queueItem  // Overflow for rare cases with multiple items
	len      int          // Number of items in use
	// Inline storage for writev buffers - avoids allocation in common case.
	// Typical case: 1 item with prefix + suffix (2 slices), or just prefix (1 slice).
	// We need up to 3 slices per item (prefix + 2 for wrap-around ring buffer).
	writeBufsBacking [6][]byte   // Backing array for common case (2 items * 3 slices)
	writeBufs        net.Buffers // Stored inline to avoid escape when calling WriteTo
}

func (q *WriteQueue) Len() int {
	return q.len
}

func (q *WriteQueue) back() *queueItem {
	if q.len == 0 {
		return nil
	}
	if q.len == 1 {
		return &q.items[0]
	}
	return &q.overflow[q.len-2] // overflow[0] is items[1]
}

func (q *WriteQueue) front() *queueItem {
	if q.len == 0 {
		return nil
	}
	return &q.items[0]
}

func (q *WriteQueue) at(i int) *queueItem {
	if i == 0 {
		return &q.items[0]
	}
	return &q.overflow[i-1]
}

func (q *WriteQueue) pushItem() *queueItem {
	q.len++
	if q.len == 1 {
		return &q.items[0]
	}
	// Need overflow
	idx := q.len - 2 // overflow index
	if idx >= len(q.overflow) {
		q.overflow = append(q.overflow, queueItem{})
	}
	return &q.overflow[idx]
}

func (q *WriteQueue) IsEmpty() bool {
	if q.len == 0 {
		return true
	}
	back := q.back()
	return back.prefix.Len() == 0 && back.suffix.Len() == 0
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
	item := q.getRingSlot()
	if item.Empty() {
		// Fill directly into the slot, avoiding allocation
		r.ToRangeInto(item)
		return nil
	}
	// Check if we can extend the existing range
	if item.End() == r.MsgIdx() && item.ring == r.Ring() {
		item.endIdx = r.MsgIdx() + 1
		return nil
	}
	// Need a new slot
	newItem := q.pushItem()
	r.ToRangeInto(&newItem.suffix)
	return nil
}

func (q *WriteQueue) WriteRingRange(r *RingRange) error {
	item := q.getRingSlot()
	if item.Empty() {
		*item = *r
		// Ensure self-referential capacity so the stored range doesn't
		// hold a pointer to a mutable cursor RingRange.
		item.capacity = item
	} else if item.End() == r.Start() && item.ring == r.ring {
		// Extend uses safe direct assignment for self-referential capacity.
		item.Extend(r)
	} else {
		newItem := q.pushItem()
		newItem.suffix = *r
		newItem.suffix.capacity = &newItem.suffix // Self-referential
	}
	// TODO: error unless item is valid
	return nil
}

func (q *WriteQueue) WriteTo(w io.Writer) (int64, error) {
	// Try to use net.Buffers for writev optimization if w is a net.Conn
	if conn, ok := w.(net.Conn); ok {
		return q.writeToConn(conn)
	}

	// Fallback to individual writes for non-net.Conn writers
	return q.writeToWriter(w)
}

// writeToConn uses net.Buffers (writev syscall) to write all data in a single syscall.
func (q *WriteQueue) writeToConn(conn net.Conn) (int64, error) {
	// Use inline backing array if it fits, otherwise allocate.
	// This avoids allocation in the common case (1-2 items).
	// We store writeBufs inline in WriteQueue to avoid escape when calling WriteTo.
	needCap := q.len * 3
	if needCap <= len(q.writeBufsBacking) {
		q.writeBufs = q.writeBufsBacking[:0]
	} else {
		q.writeBufs = make(net.Buffers, 0, needCap)
	}

	// Track stats
	var prefixBytes, ringBytes int64
	var ringBufs int
	var wrapArounds int

	// Collect all slices
	hasStreaming := false
	for i := range q.len {
		item := q.at(i)
		if item.prefix.Len() > 0 {
			prefixBytes += int64(item.prefix.Len())
			q.writeBufs = append(q.writeBufs, item.prefix.Bytes())
		}

		if !item.suffix.Empty() {
			beforeLen := len(q.writeBufs)
			var ok bool
			q.writeBufs, ok = item.suffix.AppendSlices(q.writeBufs)
			if !ok {
				// Has streaming message - fall back to slow path
				hasStreaming = true
				break
			}
			// Track ring buffer contribution
			addedBufs := len(q.writeBufs) - beforeLen
			ringBufs += addedBufs
			if addedBufs == 2 {
				wrapArounds++ // 2 slices means wrap-around
			}
			for j := beforeLen; j < len(q.writeBufs); j++ {
				ringBytes += int64(len(q.writeBufs[j]))
			}
		}
	}

	// If we have streaming messages, fall back to slow path
	if hasStreaming {
		RecordStreamingWrite()
		return q.writeToWriter(conn)
	}

	// Record statistics
	RecordWritev(len(q.writeBufs), prefixBytes, ringBytes, ringBufs, wrapArounds > 0)

	// Write all buffers in one syscall using writev
	return q.writeBufs.WriteTo(conn)
}

// writeToWriter is the fallback that writes each item individually.
func (q *WriteQueue) writeToWriter(w io.Writer) (int64, error) {
	total := int64(0)
	for i := range q.len {
		item := q.at(i)
		n, err := w.Write(item.prefix.Bytes())
		total += int64(n)
		if err != nil {
			return total, err
		}

		r := item.suffix.NewReader()
		// In many cases, r will implement WriterTo, avoiding a buffer allocation entirely.
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
	if q.len == 0 {
		return
	}

	// Reset first item for reuse
	q.items[0].prefix.Reset()
	q.items[0].suffix = RingRange{}

	// Reset overflow items (keep slice capacity)
	for i := range q.overflow[:q.len-1] {
		q.overflow[i].prefix.Reset()
		q.overflow[i].suffix = RingRange{}
	}

	q.len = 0
}

func (q *WriteQueue) getByteSlot() *bytes.Buffer {
	if q.len == 0 || q.back().suffix.Len() != 0 {
		return &q.pushItem().prefix
	}

	return &q.back().prefix
}

func (q *WriteQueue) getRingSlot() *RingRange {
	if q.len == 0 {
		return &q.pushItem().suffix
	}

	return &q.back().suffix
}
