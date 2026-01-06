package pgwire

import (
	"fmt"
	"io"
	"iter"
	"strings"

	"github.com/gammazero/deque"
)

// StreamPos describes a position in a stream of messages.
type StreamPos struct {
	// Seq is the logical index of the message in the stream.
	Seq int64
	// Offset is the byte offset of the message in the stream.
	Offset int64
}

func (s StreamPos) String() string {
	return fmt.Sprintf("#%d@%d", s.Seq, s.Offset)
}

type OffsetSlice[T any] struct {
	Offset int64
	B      []T
}

func NewOffsetSlice[T any](offset int64, bytes []T) OffsetSlice[T] {
	return OffsetSlice[T]{
		Offset: offset,
		B:      bytes,
	}
}

func (s OffsetSlice[T]) Len() int {
	return len(s.B)
}

func (s OffsetSlice[T]) StartOffset() int64 {
	return s.Offset
}

func (s OffsetSlice[T]) EndOffset() int64 {
	return s.Offset + int64(len(s.B))
}

func (s OffsetSlice[T]) String() string {
	return fmt.Sprintf("StreamSlice{[%d,%d) %d bytes}", s.Offset, s.EndOffset(), len(s.B))
}

func (s OffsetSlice[T]) Slice(start, end int64) OffsetSlice[T] {
	return OffsetSlice[T]{
		Offset: start,
		B:      s.B[start-s.Offset : end-s.Offset],
	}
}

type MsgSeq interface {
	MsgSeq() int64
}

type MsgOffset interface {
	MsgOffset() int64
}

// TODO: replace with {StreamPos, Msg}
type StreamMsg[T RawMessageSource] struct {
	Idx    int64
	Offset int64
	T      T
}

var _ MsgSeq = (*StreamMsg[RawMessageSource])(nil)
var _ MsgOffset = (*StreamMsg[RawMessageSource])(nil)

// MsgOffset implements [MsgOffset].
func (s *StreamMsg[T]) MsgOffset() int64 {
	return s.Offset
}

// MsgIdx implements [MsgSeq].
func (s *StreamMsg[T]) MsgSeq() int64 {
	return s.Idx
}

// AppendTo implements [RawMessageSource].
func (s *StreamMsg[T]) AppendTo(buf []byte) ([]byte, error) {
	return s.T.AppendTo(buf)
}

// Body implements [RawMessageSource].
func (s *StreamMsg[T]) Body() []byte {
	return s.T.Body()
}

// Bytes implements [RawMessageSource].
func (s *StreamMsg[T]) Bytes() []byte {
	return s.T.Bytes()
}

// Len implements [RawMessageSource].
func (s *StreamMsg[T]) Len() int {
	return s.T.Len()
}

// MessageType implements [RawMessageSource].
func (s *StreamMsg[T]) MessageType() MsgType {
	return s.T.MessageType()
}

// NewReader implements [RawMessageSource].
func (s *StreamMsg[T]) NewReader() io.Reader {
	return s.T.NewReader()
}

// Retain implements [RawMessageSource].
func (s *StreamMsg[T]) Retain() RawMessageSource {
	source := s.T.Retain()
	if t, ok := source.(T); ok {
		return &StreamMsg[T]{
			Idx:    s.Idx,
			Offset: s.Offset,
			T:      t,
		}
	}
	return &StreamMsg[RawMessageSource]{
		Idx:    s.Idx,
		Offset: s.Offset,
		T:      source,
	}
}

// WriteTo implements [RawMessageSource].
func (s StreamMsg[T]) WriteTo(w io.Writer) (int64, error) {
	return s.T.WriteTo(w)
}

var _ RawMessageSource = (*StreamMsg[RawMessageSource])(nil)

type StreamSliceMsg = StreamMsg[SliceMsg]
type StreamBuffersMsg = StreamMsg[BuffersMessage]

func NewStreamSliceMessage(idx int64, msg OffsetSlice[byte]) StreamSliceMsg {
	return StreamSliceMsg{
		Idx:    idx,
		Offset: msg.Offset,
		T:      SliceMsg{Slice: msg.B},
	}
}

// StreamSlice is a slice out of a stream of messages.
type StreamSlice struct {
	*MessageOffsets
	Slice OffsetSlice[byte]
}

func (s *StreamSlice) String() string {
	var builder strings.Builder
	builder.WriteString("StreamSlice{")
	first := true
	for msg := range s.All() {
		if !first {
			builder.WriteString(" ")
		}
		first = false
		fmt.Fprintf(&builder, "%v", msg)
	}
	builder.WriteString("}")
	return builder.String()
}

func (s *StreamSlice) All() iter.Seq[StreamSliceMsg] {
	return func(yield func(StreamSliceMsg) bool) {
		for idx := s.StartMsgSeq(); idx < s.EndMsgSeq(); idx++ {
			if !yield(s.At(idx)) {
				return
			}
		}
	}
}

func (s *StreamSlice) At(idx int64) StreamSliceMsg {
	startOffset, endOffset := s.MsgRange(idx)
	return NewStreamSliceMessage(idx, s.Slice.Slice(startOffset, endOffset))
}

// MessageOffsets tracks message metadata in an abstract byte stream.
// Messages are indexed by a logical message index starting at msgStartIdx.
// Stores the byte offset where each message starts; the end of message N
// is the start of message N+1 (or endOffset for the last message).
type MessageOffsets struct {
	msgStartIdx int64 // logical index of first message in deque
	endOffset   int64 // byte offset of end of last message (== stream position)

	// offsets[i] is the byte offset where message (msgStartIdx + i) starts.
	// len(offsets) == number of messages tracked.
	offsets deque.Deque[int64]
}

func (p *MessageOffsets) String() string {
	var builder strings.Builder
	fmt.Fprintf(&builder, "StreamMessages[%d]{", p.Len())
	first := true
	for idx := p.StartMsgSeq(); idx < p.EndMsgSeq(); idx++ {
		if !first {
			builder.WriteString(" ")
		}
		first = false
		fmt.Fprintf(&builder, "idx=%v@offset=%v", idx, p.Offset(idx))
	}
	builder.WriteString("}")
	return builder.String()
}

func (p *MessageOffsets) Copy() *MessageOffsets {
	result := *p
	result.offsets = deque.Deque[int64]{}
	result.offsets.Copy(p.offsets)
	return &result
}

// Push adds a new message. The type is currently unused but available for future use.
func (p *MessageOffsets) Push(_ MsgType, startOffset, endOffset int64) {
	p.offsets.PushBack(startOffset)
	p.endOffset = endOffset
}

// Shift removes and returns the first message's byte range.
// Returns ok=false if no messages are available.
func (p *MessageOffsets) Shift() (startOffset, endOffset int64, ok bool) {
	if p.offsets.Len() == 0 {
		return 0, 0, false
	}
	startOffset = p.offsets.PopFront()
	p.msgStartIdx++
	if p.offsets.Len() > 0 {
		endOffset = p.offsets.Front()
	} else {
		endOffset = p.endOffset
	}
	return startOffset, endOffset, true
}

// ShiftN removes the first n messages.
func (p *MessageOffsets) ShiftN(n int) {
	if n <= 0 {
		return
	}
	if n >= p.offsets.Len() {
		p.msgStartIdx += int64(p.offsets.Len())
		p.offsets.Clear()
		return
	}
	for i := 0; i < n; i++ {
		p.offsets.PopFront()
	}
	p.msgStartIdx += int64(n)
}

// Truncate removes all messages before newStartMsgIdx.
func (p *MessageOffsets) Truncate(newStartMsgIdx int64) {
	toRemove := int(newStartMsgIdx - p.msgStartIdx)
	p.ShiftN(toRemove)
}

// Len returns the number of messages currently tracked.
func (p *MessageOffsets) Len() int {
	return p.offsets.Len()
}

// StartMsgIdx returns the logical index of the first message.
func (p *MessageOffsets) StartMsgSeq() int64 {
	return p.msgStartIdx
}

// EndMsgIdx returns the logical index one past the last message.
func (p *MessageOffsets) EndMsgSeq() int64 {
	return p.msgStartIdx + int64(p.offsets.Len())
}

func (p *MessageOffsets) StartOffset() int64 {
	return p.offsets.Front()
}

func (p *MessageOffsets) EndOffset() int64 {
	return p.endOffset
}

// Offset returns the start byte offset of the message at msgIdx.
// Panics if msgIdx is out of range.
func (p *MessageOffsets) Offset(msgIdx int64) int64 {
	idx := int(msgIdx - p.msgStartIdx)
	return p.offsets.At(idx)
}

// Size returns the byte size of the message at msgIdx.
// Panics if msgIdx is out of range.
func (p *MessageOffsets) Size(msgIdx int64) int64 {
	start, end := p.MsgRange(msgIdx)
	return end - start
}

// MsgRange returns the byte range [start, end) of the message at msgIdx.
// Panics if msgIdx is out of range.
func (p *MessageOffsets) MsgRange(msgIdx int64) (startOffset, endOffset int64) {
	idx := int(msgIdx - p.msgStartIdx)
	startOffset = p.offsets.At(idx)
	if idx+1 < p.offsets.Len() {
		endOffset = p.offsets.At(idx + 1)
	} else {
		endOffset = p.endOffset
	}
	return
}

// Range returns the byte range [start, end) spanning messages [startMsg, endMsg).
// Panics if indices are out of range.
func (p *MessageOffsets) Range(startMsg, endMsg int64) (startOffset, endOffset int64) {
	startOffset = p.Offset(startMsg)
	if endMsg >= p.EndMsgSeq() {
		endOffset = p.endOffset
	} else {
		endOffset = p.Offset(endMsg)
	}
	return
}
