package pgwire

import "iter"

type WriteBatch[T Message] struct {
	WriteBatchLink
	tail *WriteBatchLink
}

type WriteBatchLink struct {
	// Ordered:
	Batch   *Cursor         // 1
	Message Message         // 2
	next    *WriteBatchLink // ...
}

func NewWriteBatch[T Message]() *WriteBatch[T] {
	return &WriteBatch[T]{}
}

func (wb *WriteBatch[T]) WriteBatch(cursor *Cursor) *WriteBatch[T] {
	wb.writeBatch(cursor)
	return wb
}

func (wb *WriteBatch[T]) WriteBatchSuffix(cursor *Cursor, suffix T) *WriteBatch[T] {
	link := wb.writeBatch(cursor)
	link.Message = suffix
	return wb
}

func (wb *WriteBatch[T]) WriteMsg(msg T) *WriteBatch[T] {
	if wb.next == nil && wb.Message == nil {
		wb.Message = msg
		return wb
	}

	if wb.tail != nil && wb.tail.Message == nil {
		wb.tail.Message = msg
		return wb
	}

	link := wb.writeBatch(nil)
	link.Message = msg
	return wb
}

func (wb *WriteBatch[T]) writeBatch(cursor *Cursor) *WriteBatchLink {
	if wb.Batch == nil && wb.Message == nil {
		wb.Batch = cursor
		return &wb.WriteBatchLink
	}

	link := &wb.WriteBatchLink
	if wb.next == nil {
		wb.next = &WriteBatchLink{Batch: cursor}
	} else {
		wb.tail.next = link
	}
	wb.tail = link
	return link
}

func (wb *WriteBatch[T]) IterWriteBatch() iter.Seq2[*Cursor, T] {
	return func(yield func(*Cursor, T) bool) {
		for link := &wb.WriteBatchLink; link != nil; link = link.next {
			var msg T
			if link.Message != nil {
				msg = link.Message.(T)
			}
			if !yield(link.Batch, msg) {
				break
			}
		}
	}
}
