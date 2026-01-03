package pgwire

import "errors"

var ErrNoMessageSource = errors.New("pgwire: no message source")

// This file contains manually-added helper methods for message types.

func (m *ClientStartupStartupMessage) StartupParameters() ParameterStatuses {
	return ParameterStatuses(m.Parse().Parameters)
}

func (m *ClientStartupStartupMessage) User() string {
	return m.StartupParameters().User()
}

func (m *ClientStartupStartupMessage) Database() string {
	return m.StartupParameters().Database()
}

// DataSize returns the size of the copy data without parsing the full message.
// This allows counting bytes transferred during COPY without allocation.
func (m *ClientCopyCopyData) DataSize() int {
	if m.isParsed {
		return len(m.parsed.Data)
	}
	if m.source != nil {
		// Body IS the data for CopyData messages
		return len(m.source.MessageBody())
	}
	return 0
}

// DataSize returns the size of the copy data without parsing the full message.
// This allows counting bytes transferred during COPY without allocation.
func (m *ServerCopyCopyData) DataSize() int {
	if m.isParsed {
		return len(m.parsed.Data)
	}
	if m.source != nil {
		// Body IS the data for CopyData messages
		return len(m.source.MessageBody())
	}
	return 0
}

func (m *ServerResponseReadyForQuery) TxStatus() TxStatus {
	if m.isParsed {
		return TxStatus(m.parsed.TxStatus)
	}
	if m.source != nil {
		body := m.source.MessageBody()
		if len(body) >= 1 {
			return TxStatus(body[0])
		}
	}
	panic(ErrNoMessageSource)
}

func (m *ClientExtendedQueryBind) BindData() (BindData, error) {
	if m.source != nil {
		parser := MessageParser{m.source.MessageBody()}
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
	Rest              []byte
	RetainedBody      []byte
}

func (d *BindData) SetPreparedStatement(statement string) {
	if d.PreparedStatement == statement {
		return
	}
	d.PreparedStatement = statement
	d.RetainedBody = nil
}

func (d *BindData) SetDestinationPortal(portal string) {
	if d.DestinationPortal == portal {
		return
	}
	d.DestinationPortal = portal
	d.RetainedBody = nil
}

func (d *BindData) SetRest(rest []byte) {
	d.Rest = rest
	d.RetainedBody = nil
}

// BodyLen implements [RawMessageSource].
func (d *BindData) BodyLen() int {
	return len(d.PreparedStatement) + 1 + len(d.DestinationPortal) + 1 + len(d.Rest)
}

// MessageBody implements [RawMessageSource].
func (d *BindData) MessageBody() []byte {
	if d.RetainedBody != nil {
		return d.RetainedBody
	}
	encoder := MessageEncoder{make([]byte, 0, d.BodyLen())}
	encoder.WriteString(d.PreparedStatement)
	encoder.WriteString(d.DestinationPortal)
	restStart := len(encoder.Buffer)
	encoder.WriteBytes(d.Rest)
	d.RetainedBody = encoder.Buffer
	d.Rest = d.RetainedBody[restStart:]
	return d.RetainedBody
}

// MessageType implements [RawMessageSource].
func (d *BindData) MessageType() MsgType {
	return MsgClientBind
}

// Retain implements [RawMessageSource].
func (d *BindData) Retain() RawMessageSource {
	_ = d.MessageBody()
	return d
}

var _ RawMessageSource = (*BindData)(nil)
