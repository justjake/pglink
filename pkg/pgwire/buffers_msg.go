package pgwire

import (
	"bytes"
	"io"
	"net"
	"slices"
)

type Buffers[T any] [][]T

func (b Buffers[T]) Len() int {
	r := 0
	for _, child := range b {
		r += len(child)
	}
	return r
}

func (b Buffers[T]) At(idx int) T {
	return b.AtAddress(b.Address(idx))
}

func (b Buffers[T]) AtAddress(parentIdx int, childIdx int) T {
	return b[parentIdx][childIdx]
}

func (b Buffers[T]) Empty() bool {
	return b.Len() == 0
}

func (b Buffers[T]) Address(idx int) (parentIdx int, childIdx int) {
	offset := 0
	for parentIdx, child := range b {
		if idx <= offset+len(child) {
			childIdx = idx - offset
			return parentIdx, childIdx
		}
		offset += len(child)
	}
	return -1, -1
}

func (b Buffers[T]) Slice(start, end int) Buffers[T] {
	startParent, startChild := b.Address(start)
	endParent, endChild := b.Address(end)
	if startParent == -1 || endParent == -1 {
		return nil
	}

	if startParent == endParent {
		return Buffers[T]{b[startParent][startChild:endChild]}
	}

	result := slices.Clone(b[startParent:endParent])

	result[0] = result[0][startChild:]

	lastIdx := len(result) - 1
	result[lastIdx] = result[lastIdx][:endChild]

	return result
}

func (b Buffers[T]) AppendTo(buf []T) []T {
	for _, child := range b {
		buf = append(buf, child...)
	}
	return buf
}

func (b Buffers[T]) IsFlat() bool {
	return len(b) <= 1
}

func (b Buffers[T]) FlatChild() ([]T, bool) {
	if len(b) == 0 {
		return nil, true
	}
	if len(b) == 1 {
		return b[0], true
	}
	return nil, false
}

func (b Buffers[T]) FlatBytes() []T {
	if flat, ok := b.FlatChild(); ok {
		return flat
	}
	return b.FlatCopy()
}

func (b Buffers[T]) FlatCopy() []T {
	r := make([]T, 0, b.Len())
	return b.AppendTo(r)
}

type ToByteBuffers interface {
	ToByteBuffers() Buffers[byte]
}

var _ ToByteBuffers = (*BuffersMessage)(nil)

type BuffersMessage struct {
	B Buffers[byte]
}

func (b BuffersMessage) ToByteBuffers() Buffers[byte] {
	return b.B
}

// AppendTo implements [RawMessageSource].
func (b BuffersMessage) AppendTo(buf []byte) ([]byte, error) {
	return b.B.AppendTo(buf), nil
}

// Body implements [RawMessageSource].
func (b BuffersMessage) Body() []byte {
	return b.Bytes()[5:]
}

// Bytes implements [RawMessageSource].
func (b BuffersMessage) Bytes() []byte {
	return b.B.FlatBytes()
}

// Len implements [RawMessageSource].
func (b BuffersMessage) Len() int {
	return b.B.Len()
}

// MessageType implements [RawMessageSource].
func (b BuffersMessage) MessageType() MsgType {
	return MsgType(b.B[0][0])
}

// NewReader implements [RawMessageSource].
func (b BuffersMessage) NewReader() io.Reader {
	if flat, ok := b.B.FlatChild(); ok {
		return bytes.NewReader(flat)
	}
	readers := make([]io.Reader, len(b.B))
	for i, child := range b.B {
		readers[i] = bytes.NewReader(child)
	}
	return io.MultiReader(readers...)
}

// Retain implements [RawMessageSource].
func (b BuffersMessage) Retain() RawMessageSource {
	return SliceMsg{b.B.FlatCopy(), true}
}

// WriteTo implements [RawMessageSource].
func (b BuffersMessage) WriteTo(w io.Writer) (int64, error) {
	netBuffers := net.Buffers(b.B)
	return netBuffers.WriteTo(w)
}

var _ RawMessageSource = BuffersMessage{}
