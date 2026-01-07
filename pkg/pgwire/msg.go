package pgwire

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"

	"github.com/jackc/pgx/v5/pgproto3"
)

type Msg struct {
	Sender Sender
	Data   []byte
}

var ErrMsgNoData = errors.New("msg has no data")
var ErrMsgTooShort = errors.New("msg too short")
var ErrMsgTooLong = errors.New("msg too long")
var ErrMsgUnknownSender = errors.New("msg sender unknown")
var ErrMsgTypeUnknown = errors.New("msg has unknown type byte for sender")

func (m Msg) StringName() string {
	if len(m.Data) == 0 {
		return "Zero"
	}
	t := m.MessageType()
	switch m.Sender {
	case SenderClient:
		if name := ClientMsgName.Get(t); name != "" {
			return name
		}
	case SenderServer:
		if name := ServerMsgName.Get(t); name != "" {
			return name
		}
	}
	return t.String()
}

func (m Msg) String() string {
	return fmt.Sprintf("Msg{%v %s %s}", m.Sender, m.StringName(), describeMsgLen(m.Data))
}

func (m Msg) IsZero() bool {
	return m.Sender == SenderNone && len(m.Data) == 0
}

func (m Msg) RequiredLen() (int, bool) {
	return MsgRequiredLen(m.Data)
}

func (m Msg) From() Sender {
	return m.Sender
}

func (m Msg) Destination() Sender {
	return m.Sender.Destination()
}

func (m Msg) Typed() TypedMsg {
	return Typed(m)
}

func (m Msg) Validate() error {
	if len(m.Data) == 0 {
		return ErrMsgNoData
	}

	switch m.Sender {
	case SenderServer:
		if !MsgIsServer.Get(m.MessageType()) {
			return ErrMsgTypeUnknown
		}
	case SenderClient:
		if !MsgIsClient.Get(m.MessageType()) {
			return ErrMsgTypeUnknown
		}
	default:
		return ErrMsgUnknownSender
	}

	required, ok := m.RequiredLen()
	if !ok || required > len(m.Data) {
		return ErrMsgTooShort
	}

	if len(m.Data) > required {
		return ErrMsgTooLong
	}

	return nil
}

func (m Msg) IsIncomplete() bool {
	required, ok := m.RequiredLen()
	if !ok {
		return true
	}
	return len(m.Data) < required
}

// AppendTo implements [RawMessageSource].
func (m Msg) AppendTo(buf []byte) ([]byte, error) {
	return append(buf, m.Data...), nil
}

// Body implements [RawMessageSource].
func (m Msg) Body() []byte {
	body, err := m.BodyErr()
	if err != nil {
		panic(err)
	}
	return body
}

func (m Msg) BodyErr() ([]byte, error) {
	if len(m.Data) == 0 {
		return nil, ErrMsgNoData
	}
	if len(m.Data) < 5 {
		return nil, ErrMsgTooShort
	}
	return m.Data[5:], nil
}

// Bytes implements [RawMessageSource].
func (m Msg) Bytes() []byte {
	return m.Data
}

// Len implements [RawMessageSource].
func (m Msg) Len() int {
	return len(m.Data)
}

// MessageType implements [RawMessageSource].
func (m Msg) MessageType() MsgType {
	return MsgType(m.Data[0])
}

// NewReader implements [RawMessageSource].
func (m Msg) NewReader() io.Reader {
	return bytes.NewReader(m.Data)
}

// Retain implements [RawMessageSource].
func (m Msg) Retain() RawMessageSource {
	return &Msg{Sender: m.Sender, Data: m.Data}
}

// WriteTo implements [RawMessageSource].
func (m Msg) WriteTo(w io.Writer) (int64, error) {
	written, err := w.Write(m.Data)
	return int64(written), err
}

// Copy returns a copy of this message with its own data slice.
func (m Msg) Copy() Msg {
	return Msg{Sender: m.Sender, Data: bytes.Clone(m.Data)}
}

// Parser wraps m's data as a MessageParser.
func (m Msg) Parser() MessageParser {
	return MessageParser{m.Data}
}

func (m Msg) BodyParser() (MessageParser, error) {
	p := m.Parser()
	if err := p.SkipMessageHeader(); err != nil {
		return MessageParser{}, err
	}
	return p, nil
}

var _ RawMessageSource = Msg{}

// MsgLen assumes `data` starts at a message boundary.
// MsgLen returns the total wire length of the message if it can be determined.
func MsgRequiredLen(data []byte) (int, bool) {
	if len(data) < 5 {
		return 0, false
	}
	return int(binary.BigEndian.Uint32(data[1:5])) + 1, true
}

func describeMsgLen(data []byte) string {
	bytes := len(data)
	if bytes == 0 {
		return "no data"
	}
	if bytes < 5 {
		missing := 5 - bytes
		return fmt.Sprintf("%3d bytes %d header missing", bytes, missing)
	}
	required, _ := MsgRequiredLen(data)
	if required > bytes {
		return fmt.Sprintf("%3d bytes %d required", bytes, required)
	}
	return fmt.Sprintf("%3d bytes", bytes)
}

func DecodeMsg[T any, PT interface {
	*T
	pgproto3.Message
}](msg Msg) (out T, err error) {
	data := msg.Data
	// We don't use BodyErr because pgproto3 typically has more descriptive errors
	// when body is too short.
	if len(data) < 5 {
		return out, ErrMsgTooShort
	}
	err = PT(&out).Decode(data[5:])
	return
}
