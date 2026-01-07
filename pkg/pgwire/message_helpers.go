package pgwire

import (
	"bytes"
	"errors"
	"io"

	"github.com/jackc/pgx/v5/pgproto3"
)

var ErrNoMessageSource = errors.New("pgwire: no message source")

// This file contains manually-added helper methods for message types.

func (m *ClientStartupMessage) StartupParameters() ParameterStatuses {
	return ParameterStatuses(m.Parse().Parameters)
}

func (m *ClientStartupMessage) User() string {
	return m.StartupParameters().User()
}

func (m *ClientStartupMessage) Database() string {
	return m.StartupParameters().Database()
}

// DataSize returns the size of the copy data without parsing the full message.
// This allows counting bytes transferred during COPY without allocation.
func (m *OldClientCopyData) DataSize() int {
	if m.isParsed {
		return len(m.parsed.Data)
	}
	if m.source != nil {
		// Body IS the data for CopyData messages
		return m.source.Len() - 5
	}
	return 0
}

// DataSize returns the size of the copy data without parsing the full message.
// This allows counting bytes transferred during COPY without allocation.
func (m ClientCopyData) DataSize() int {
	if len, ok := m.Msg().RequiredLen(); ok {
		return len - 5
	}
	return m.Msg().Len()
}

func (m ServerCopyData) DataSize() int {
	if len, ok := m.Msg().RequiredLen(); ok {
		return len - 5
	}
	return m.Msg().Len()
}

// DataSize returns the size of the copy data without parsing the full message.
// This allows counting bytes transferred during COPY without allocation.
func (m *OldServerCopyData) DataSize() int {
	if m.isParsed {
		return len(m.parsed.Data)
	}
	if m.source != nil {
		// Body IS the data for CopyData messages
		return m.source.Len() - 5
	}
	return 0
}

func (m *ServerReadyForQuery) TxStatus() TxStatus {
	if m.isParsed {
		return TxStatus(m.parsed.TxStatus)
	}
	if m.source != nil {
		body := m.source.Body()
		if len(body) >= 1 {
			return TxStatus(body[0])
		}
	}
	panic(ErrNoMessageSource)
}

func (m ReadyForQuery) TxStatus() (TxStatus, error) {
	data := m.Msg().Data
	if len(data) > 5 {
		return TxStatus(data[5]), nil
	}
	return 0, ErrMsgTooShort
}

func (m *ClientBind) BindData() (BindData, error) {
	if m.source != nil {
		parser := MessageParser{m.source.Body()}
		statement, err := parser.ReadString()
		if err != nil {
			return BindData{}, err
		}
		portal, err := parser.ReadString()
		if err != nil {
			return BindData{}, err
		}
		return BindData{
			PreparedStatement: statement,
			DestinationPortal: portal,
			Rest:              parser.Rest,
		}, nil
	}
	return BindData{}, ErrNoMessageSource
}

type BindData struct {
	PreparedStatement string
	DestinationPortal string
	Rest              []byte // TODO: make io.Reader ?
	RetainedBytes     []byte
}

func (d *BindData) SetPreparedStatement(statement string) {
	if d.PreparedStatement == statement {
		return
	}
	d.PreparedStatement = statement
	d.RetainedBytes = nil
}

func (d *BindData) SetDestinationPortal(portal string) {
	if d.DestinationPortal == portal {
		return
	}
	d.DestinationPortal = portal
	d.RetainedBytes = nil
}

func (d *BindData) SetRest(rest []byte) {
	d.Rest = rest
	d.RetainedBytes = nil
}

// BodyLen implements [RawMessageSource].
func (d *BindData) Len() int {
	return 5 + len(d.PreparedStatement) + 1 + len(d.DestinationPortal) + 1 + len(d.Rest)
}

func (d *BindData) appendHeader(buf []byte) []byte {
	encoder := MessageEncoder{buf}
	encoder.AppendByte(byte(MsgClientBind))
	encoder.WriteInt32(int32(d.Len() - 1))
	encoder.WriteString(d.PreparedStatement)
	encoder.WriteString(d.DestinationPortal)
	return encoder.Buffer
}

func (d *BindData) appendToRest(buf []byte) ([]byte, []byte) {
	encoder := MessageEncoder{d.appendHeader(buf)}
	restStart := len(encoder.Buffer)
	encoder.WriteBytes(d.Rest)
	return encoder.Buffer, encoder.Buffer[restStart:]
}

func (d *BindData) AppendTo(buf []byte) ([]byte, error) {
	if d.RetainedBytes != nil {
		return append(buf, d.RetainedBytes...), nil
	}
	buf, _ = d.appendToRest(buf)
	return buf, nil
}

func (d *BindData) Bytes() []byte {
	if d.RetainedBytes != nil {
		return d.RetainedBytes
	}
	buf, rest := d.appendToRest(make([]byte, 0, d.Len()))
	d.Rest = rest
	d.RetainedBytes = buf
	return buf
}

func (d *BindData) NewReader() io.Reader {
	if d.RetainedBytes != nil {
		return bytes.NewReader(d.RetainedBytes)
	}
	header := d.appendHeader(nil)
	return io.MultiReader(bytes.NewReader(header), bytes.NewReader(d.Rest))
}

func (d *BindData) Body() []byte {
	return d.Bytes()[5:]
}

func (d *BindData) WriteTo(w io.Writer) (int64, error) {
	header := d.appendHeader(nil)
	n, err := w.Write(header)
	if err != nil {
		return int64(n), err
	}
	n2, err := w.Write(d.Rest)
	return int64(n + n2), err
}

// MessageType implements [RawMessageSource].
func (d *BindData) MessageType() MsgType {
	return MsgClientBind
}

// Retain implements [RawMessageSource].
func (d *BindData) Retain() RawMessageSource {
	_ = d.Bytes()
	return d
}

var _ RawMessageSource = (*BindData)(nil)

func WriteMsg(dest io.Writer, msg Message) (int, error) {
	if source := msg.Source(); source != nil {
		n, err := io.Copy(dest, source.NewReader())
		return int(n), err
	}
	return 0, ErrNoMessageSource
}

func (m ParameterStatus) NameValue() (string, string, error) {
	body, err := m.Msg().BodyErr()
	if err != nil {
		return "", "", err
	}

	var pgmsg pgproto3.ParameterStatus
	if err := pgmsg.Decode(body); err != nil {
		return "", "", err
	}

	return pgmsg.Name, pgmsg.Value, nil
}

func (m Parse) Name() (string, error) {
	parser := MessageParser{m.Msg().Data}
	if err := parser.SkipMessageHeader(); err != nil {
		return "", err
	}
	return parser.ReadString()
}

func (m Parse) Query() (string, error) {
	body, err := m.Msg().BodyErr()
	if err != nil {
		return "", err
	}

	parser := MessageParser{body}
	if err := parser.SkipMessageHeader(); err != nil {
		return "", err
	}
	if _, err := parser.ReadString(); err != nil {
		return "", err
	}
	return parser.ReadString()
}

func (m Parse) ParameterOIDs() ([]uint32, error) {
	body, err := m.Msg().BodyErr()
	if err != nil {
		return nil, err
	}

	var parser pgproto3.Parse
	if err := parser.Decode(body); err != nil {
		return nil, err
	}

	return parser.ParameterOIDs, nil
}
