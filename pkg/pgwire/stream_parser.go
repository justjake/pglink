package pgwire

import (
	"encoding/binary"
	"errors"
	"fmt"
)

// ParseState is the internal state machine state for message parsing.
type ParseState int

const (
	ParseIdle ParseState = iota
	ParseReadingSize
	ParseReadingBody
)

func (s ParseState) String() string {
	switch s {
	case ParseIdle:
		return "idle"
	case ParseReadingSize:
		return "reading_size"
	case ParseReadingBody:
		return "reading_body"
	default:
		return fmt.Sprintf("unknown(%d)", s)
	}
}

type StreamBatch struct {
	Complete StreamSlice
	Partial  *IncompleteStreamMsg[SliceMsg]
}

type OnBatchCallback func(batch StreamBatch)

// StreamBatchParser parses a stream of PostgreSQL wire protocol messages written to it.
// It only accepts writes that produce a message batch (i.e. complete messages).
//
// See the [Write] method for details.
type StreamBatchParser struct {
	// Size of messages that must be streamed because they are too large to keep
	// in memory.
	MaxParseMessageSize int
	OnBatch             OnBatchCallback
	Parser              StreamParser
	// Must not be aliased after the Write method returns.
	complete StreamSlice
}

func NewStreamBatchParser(onBatch OnBatchCallback) *StreamBatchParser {
	parser := &StreamBatchParser{
		OnBatch:  onBatch,
		complete: StreamSlice{StreamMessages: &StreamMessages{}},
	}
	parser.Parser.OnMsg = parser.complete.Push
	return parser
}

// Write segments up to len(b) bytes into a message batch, which is passed to the [OnBatch] callback.
// A batch is >0 complete messages, plus 0 or 1 incomplete message with size > MaxParseMessageSize.
// The returned written bytes are equal to the number of bytes in the batch.
//
// if len(b) !== written:
// - an underlying parse error occured
// - b[written:] contains an incomplete message header, so no pending message size can be computed.
// - b[written:] contains a partial message with known size, but the size < MaxParseMessageSize.
func (p *StreamBatchParser) Write(b []byte) (int, error) {
	if len(b) == 0 {
		return 0, nil
	}

	// preconditions
	if p.complete.Slice.Len() > 0 {
		return 0, fmt.Errorf("stream batch parser: complete slice not empty")
	}
	if p.complete.Len() > 0 {
		return 0, fmt.Errorf("stream batch parser: complete batch metadata not empty")
	}

	skipFirstMessage := false
	switch p.Parser.State() {
	case ParseReadingBody:
		// We are part-way through reading a message.
		// We only enter this state when the message is >MaxParseMessageSize.
		// This read must advance:
		// - If it completes >0 messages, then the first completed message was the streaming message. It must be ignored and not double-reported.
		// - If it completes 0 messages, then we accept the write to make progress, and emit no batch.
		skipFirstMessage = true
	case ParseIdle:
		// We are idle.
		// Produce a batch if either:
		// - b contains at least one complete message
		// - b contains a partial message that is >MaxParseMessageSize
		skipFirstMessage = false
	case ParseReadingSize:
		return 0, fmt.Errorf("stream batch parser: parser in reading size state (must be idle or reading body)")
	default:
		panic("unexpected pgproxy.ParseState")
	}

	// For the duraction of Write, p.Slice aliases the data in b.
	// This is only necessary if we emit a batch.
	p.complete.Slice = OffsetSlice[byte]{Offset: p.Parser.curIdx, B: b}
	defer func() {
		p.complete.Slice = OffsetSlice[byte]{}
	}()

	// At the end of Write, we must have handled any messages.
	defer func() {
		if p.complete.Len() > 0 {
			p.complete.Truncate(p.complete.EndMsgIdx() + 1)
		}
	}()

	written, err := p.Parser.WriteComplete(b)
	remaining := b[written:]

	// if the suffix indicatges a message of size >MaxParseMessageSize, then we must stream it.
	var incompleteMsg IncompleteStreamMsg[SliceMsg]
	if _, startOffset, endOffset, ok := p.Parser.PeekPendingMessage(remaining); ok {
		len := int(endOffset - startOffset)
		// It's big.
		if len > p.MaxParseMessageSize {
			// Write is guaranteed to succeed by PeekPendingMessage ok.
			extraWritten, extraErr := p.Parser.Write(remaining)
			written += extraWritten
			if extraErr != nil {
				panic(fmt.Errorf("stream batch parser: large message: unexpected error writing partial message: %w", extraErr))
			}
			if p.Parser.State() != ParseReadingBody {
				panic(fmt.Errorf("stream batch parser: large message: parser did not enter reading body state)"))
			}

			// We must emit a batch containing any complete messages within the written bytes.
			incompleteMsg = IncompleteStreamMsg[SliceMsg]{
				Remaining: int(endOffset - p.Parser.curIdx),
				StreamMsg: StreamMsg[SliceMsg]{
					Idx:    p.complete.EndMsgIdx(),
					Offset: startOffset,
					T:      SliceMsg{Slice: remaining},
				},
			}
		}
	}

	if skipFirstMessage {
		if p.complete.Len() > 0 {
			// Ignore first message.
			p.complete.Shift()
		} else if incompleteMsg.Remaining > 0 {
			// the partial message is the "first" message
			// it's currently streaming
			incompleteMsg = IncompleteStreamMsg[SliceMsg]{}
		}
	}

	// Emit batch, if any.
	if (p.complete.Len() > 0 || incompleteMsg.Remaining > 0) && p.OnBatch != nil {
		res := StreamBatch{p.complete, nil}
		if incompleteMsg.Remaining > 0 {
			res.Partial = &incompleteMsg
		}
		p.OnBatch(res)
	}

	return written, err
}

func (p *StreamBatchParser) Copy(onBatch OnBatchCallback) *StreamBatchParser {
	result := *p
	result.OnBatch = onBatch
	result.complete.Slice = OffsetSlice[byte]{}
	result.complete.StreamMessages = result.complete.StreamMessages.Copy()
	result.Parser.OnMsg = result.complete.Push
	return &result
}

// StreamParser is a PostgreSQL wire protocol message boundary parser for normal mode.
// It parses messages with format: type (1 byte) + length (4 bytes) + body.
// Implements io.Writer and calls OnMsg for each complete message.
//
// The struct is kept compact by computing derived values from curIdx and msgStart:
//   - headerN = min(5, curIdx - msgStart)
//   - bodyLen = lengthField - 4 (when header is complete)
//   - bodyRead = curIdx - msgStart - 5 (when in body)
//   - state = derived from (curIdx - msgStart)
//
// The zero value is an idle parser with no OnMsg callback.
type StreamParser struct {
	// Called when a complete message is parsed.
	// msgType is the message type byte.
	// startIdx and endIdx are byte offsets in the stream.
	OnMsg func(msgType MsgType, startIdx, endIdx int64)

	curIdx      int64   // current position in stream (total bytes processed)
	msgStart    int64   // start of current message (== curIdx when idle)
	header      [5]byte // type (1) + length (4)
	lengthField int64   // parsed value from header[1:5] (includes itself, set when header complete)
}

func NewStreamParser(onComplete func(MsgType, int64, int64)) *StreamParser {
	return &StreamParser{OnMsg: onComplete}
}

func (p *StreamParser) ResetToIdleAt(idx int64) {
	p.curIdx = idx
	p.msgStart = idx
}

// Idle returns true if no message is currently being parsed.
func (p *StreamParser) Idle() bool {
	return p.curIdx == p.msgStart
}

// offset returns the number of bytes into the current message.
func (p *StreamParser) offset() int64 {
	return p.curIdx - p.msgStart
}

// headerN returns how many header bytes have been accumulated (0-5).
func (p *StreamParser) headerN() int {
	return int(min(5, p.offset()))
}

// bodyLen returns the body length (lengthField - 4).
// Only valid when headerN() == 5 and lengthField has been set.
func (p *StreamParser) bodyLen() int64 {
	return p.lengthField - 4
}

// bodyRead returns how many body bytes have been consumed.
// Only valid when offset() >= 5.
func (p *StreamParser) bodyRead() int64 {
	return p.offset() - 5
}

// State returns the current parse state.
func (p *StreamParser) State() ParseState {
	offset := p.offset()
	if offset == 0 {
		return ParseIdle
	}
	if offset < 5 {
		return ParseReadingSize
	}
	return ParseReadingBody
}

// PendingMessage returns the message currently being written
// or 0 if idle or indeterminate.
func (p *StreamParser) PendingMessage() (msgType MsgType, startOffset, endOffset int64, ok bool) {
	if p.State() != ParseReadingBody {
		return 0, 0, 0, false
	}
	msgType = MsgType(p.header[0])
	startOffset = p.msgStart
	endOffset = startOffset + 1 + p.lengthField
	ok = true
	return
}

// Peeker returns a clone of this parser with no OnMsg callback.
func (p *StreamParser) Peeker() StreamParser {
	result := *p
	result.OnMsg = nil
	return result
}

func (p *StreamParser) PeekPendingMessage(b []byte) (msgType MsgType, startOffset, endOffset int64, ok bool) {
	if len(b) == 0 {
		return 0, 0, 0, false
	}

	peeker := p.Peeker()
	_, err := peeker.Write(b)
	if err != nil {
		return 0, 0, 0, false
	}

	return peeker.PendingMessage()
}

// BytesNeeded returns the number of bytes needed to complete the current message.
// Returns 0 if idle (no message in progress).
func (p *StreamParser) BytesNeeded() int {
	offset := p.offset()
	if offset == 0 {
		return 0 // idle
	}
	if offset < 5 {
		return 5 - int(offset) // need rest of header
	}
	// In body: need bodyLen - bodyRead
	return int(p.bodyLen() - p.bodyRead())
}

var ErrIncompleteMessage = errors.New("incomplete message")

// WriteComplete writes up to the last complete message in b to the parser.
// Writes of partial messages are rejected by returning [ErrIncompleteMessage].
func (p *StreamParser) WriteComplete(b []byte) (int, error) {
	provisional := *p
	provisionallyWritten, err := provisional.Write(b)
	written := int(provisional.msgStart - p.curIdx)
	if written <= 0 {
		return 0, errors.Join(err, ErrIncompleteMessage)
	}

	p.ResetToIdleAt(provisional.msgStart)
	if err != nil {
		return written, err
	} else if provisionallyWritten != written {
		return written, ErrIncompleteMessage
	} else {
		return written, nil
	}
}

// Write implements io.Writer. Parses PostgreSQL messages (type + length + body)
// and calls OnMsg for each complete message found.
func (p *StreamParser) Write(b []byte) (int, error) {
	if len(b) == 0 {
		return 0, nil
	}

	written := 0

	// Fast path: process complete messages without state transitions
	for p.Idle() && len(b) >= 5 {
		length := int64(binary.BigEndian.Uint32(b[1:5]))
		if length < 4 {
			return written, fmt.Errorf("invalid message length: %d", length)
		}
		msgSize := 1 + length
		if int64(len(b)) < msgSize {
			break // incomplete message, use slow path
		}
		// Complete message available
		msgType := MsgType(b[0])
		startIdx := p.curIdx
		p.curIdx += msgSize
		p.msgStart = p.curIdx // stay idle
		if p.OnMsg != nil {
			p.OnMsg(msgType, startIdx, p.curIdx)
		}
		b = b[msgSize:]
		written += int(msgSize)
	}

	// Slow path: state machine for partial messages
	for len(b) > 0 {
		switch p.State() {
		case ParseIdle, ParseReadingSize:
			// Accumulate header bytes
			headerN := p.headerN()
			need := 5 - headerN
			have := len(b)
			n := min(need, have)
			copy(p.header[headerN:], b[:n])
			p.curIdx += int64(n)
			written += n
			b = b[n:]

			// Check if header is complete
			if p.headerN() < 5 {
				continue
			}

			// Parse and validate length field
			p.lengthField = int64(binary.BigEndian.Uint32(p.header[1:5]))
			if p.lengthField < 4 {
				return written, fmt.Errorf("invalid message length: %d", p.lengthField)
			}

			// Check for zero-length body
			if p.bodyLen() == 0 {
				p.finishMessage()
				continue
			}
			// Fall through to body reading on next iteration

		case ParseReadingBody:
			need := p.bodyLen() - p.bodyRead()
			n := min(need, int64(len(b)))
			p.curIdx += n
			written += int(n)
			b = b[n:]

			if p.bodyRead() >= p.bodyLen() {
				p.finishMessage()
			}

		default:
			return written, fmt.Errorf("invalid state: %v", p.State())
		}
	}
	return written, nil
}

// finishMessage completes a message and calls the callback.
func (p *StreamParser) finishMessage() {
	msgType := MsgType(p.header[0])
	if p.OnMsg != nil {
		p.OnMsg(msgType, p.msgStart, p.curIdx)
	}
	p.msgStart = p.curIdx // reset to idle
}

// FrontendStreamParser parses client->server messages.
// Handles startup messages (length + body, no type byte) then normal messages.
type FrontendStreamParser struct {
	StreamParser
	startup bool // true while in startup phase
}

// NewFrontendStream creates a parser for client->server messages.
func NewFrontendStream(onComplete func(MsgType, int64, int64)) *FrontendStreamParser {
	return &FrontendStreamParser{
		StreamParser: StreamParser{OnMsg: onComplete},
		startup:      true,
	}
}

// SetNormalPhase transitions to normal message parsing.
func (p *FrontendStreamParser) SetNormalPhase() {
	p.startup = false
}

// Write implements io.Writer.
func (p *FrontendStreamParser) Write(b []byte) (int, error) {
	if len(b) == 0 {
		return 0, nil
	}
	if p.startup {
		return p.writeStartup(b)
	}
	return p.StreamParser.Write(b)
}

// startupHeaderN returns how many startup header bytes have been accumulated (0-4).
// Startup messages have only a 4-byte length header (no type byte).
func (p *FrontendStreamParser) startupHeaderN() int {
	return int(min(4, p.offset()))
}

// startupBodyLen returns the body length for a startup message (lengthField - 4).
// Only valid when startupHeaderN() == 4 and lengthField has been set.
func (p *FrontendStreamParser) startupBodyLen() int64 {
	return p.lengthField - 4
}

// startupBodyRead returns how many body bytes have been consumed for a startup message.
// Only valid when offset() >= 4.
func (p *FrontendStreamParser) startupBodyRead() int64 {
	return p.offset() - 4
}

// writeStartup handles startup messages: length (4 bytes) + body (no type byte).
func (p *FrontendStreamParser) writeStartup(b []byte) (int, error) {
	written := 0

	for len(b) > 0 {
		offset := p.offset()

		if offset < 4 {
			// Accumulate header bytes (4-byte length only)
			headerN := int(offset)
			need := 4 - headerN
			n := min(need, len(b))
			copy(p.header[headerN:], b[:n])
			p.curIdx += int64(n)
			written += n
			b = b[n:]

			// Check if header is complete
			if p.startupHeaderN() < 4 {
				continue
			}

			// Parse and validate length field
			p.lengthField = int64(binary.BigEndian.Uint32(p.header[0:4]))
			if p.lengthField < 4 {
				return written, fmt.Errorf("invalid startup message length: %d", p.lengthField)
			}

			// Check for zero-length body
			if p.startupBodyLen() == 0 {
				p.finishStartupMessage()
				continue
			}
			// Fall through to body reading on next iteration
		} else {
			// Reading body
			need := p.startupBodyLen() - p.startupBodyRead()
			n := min(need, int64(len(b)))
			p.curIdx += n
			written += int(n)
			b = b[n:]

			if p.startupBodyRead() >= p.startupBodyLen() {
				p.finishStartupMessage()
			}
		}
	}
	return written, nil
}

// finishStartupMessage completes a startup message.
func (p *FrontendStreamParser) finishStartupMessage() {
	if p.OnMsg != nil {
		p.OnMsg(MsgStartup, p.msgStart, p.curIdx)
	}
	p.msgStart = p.curIdx // reset to idle
}

// BackendStreamParser parses server->client messages.
// Handles SSL response ('S'/'N' single byte) then normal messages.
type BackendStreamParser struct {
	StreamParser
	startup bool // true while in startup phase
}

// NewBackendStream creates a parser for server->client messages.
func NewBackendStream(onComplete func(MsgType, int64, int64)) *BackendStreamParser {
	return &BackendStreamParser{
		StreamParser: StreamParser{OnMsg: onComplete},
		startup:      true,
	}
}

// SetNormalPhase transitions to normal message parsing.
func (p *BackendStreamParser) SetNormalPhase() {
	p.startup = false
}

// Write implements io.Writer.
func (p *BackendStreamParser) Write(b []byte) (int, error) {
	if len(b) == 0 {
		return 0, nil
	}
	if p.startup {
		return p.writeStartup(b)
	}
	return p.StreamParser.Write(b)
}

// writeStartup handles SSL response detection.
func (p *BackendStreamParser) writeStartup(b []byte) (int, error) {
	written := 0

	// Check for SSL response (single byte 'S' or 'N')
	if p.Idle() {
		ch := b[0]
		if ch == 'S' || ch == 'N' {
			startIdx := p.curIdx
			p.curIdx++
			p.msgStart = p.curIdx // stay idle
			if p.OnMsg != nil {
				p.OnMsg(MsgType(ch), startIdx, p.curIdx)
			}
			b = b[1:]
			written++
			if len(b) == 0 {
				return written, nil
			}
		}
		// Not an SSL response - switch to normal phase
		p.startup = false
	}

	// Continue with normal message parsing
	n, err := p.StreamParser.Write(b)
	return written + n, err
}

type IncompleteStreamMsg[T RawMessageSource] struct {
	Remaining int
	StreamMsg[T]
}

func (s IncompleteStreamMsg[T]) Len() int {
	return s.Remaining + s.T.Len()
}
