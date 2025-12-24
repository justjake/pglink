package pgwire

import (
	"encoding/binary"
	"fmt"

	"github.com/jackc/pgx/v5/pgproto3"
)

// RawMessageSource provides lazy access to wire protocol message bytes.
// Implementations include RawBody (owned bytes) and Cursor (ring buffer reference).
// This interface enables zero-allocation message handling by deferring byte extraction
// until Parse() is called.
type RawMessageSource interface {
	// MessageType returns the message type identifier.
	MessageType() MsgType

	// MessageBody returns the message body bytes (after 5-byte header).
	// This may allocate if the bytes need to be copied (e.g., ring buffer wraparound).
	// Only call this when you need to parse the message.
	//
	// May panic if the message is enormous.
	// TODO: return []byte, error
	MessageBody() []byte

	// Retain returns an owned copy of this message source that is safe to keep
	// beyond the current iteration. For already-owned sources like RawBody,
	// this returns the receiver. For borrowed sources like Cursor, this copies the bytes.
	Retain() RawMessageSource
}

// RawBody holds unparsed PostgreSQL wire protocol message bytes.
// It can be forwarded directly without parsing.
// RawBody implements RawMessageSource.
type RawBody struct {
	Type MsgType // Message type identifier
	Body []byte  // Message body (after 5-byte header: type + length)
}

// IsZero returns true if this RawBody has no data.
func (r RawBody) IsZero() bool {
	return r.Body == nil
}

// MessageType implements RawMessageSource.
func (r RawBody) MessageType() MsgType {
	return r.Type
}

// MessageBody implements RawMessageSource.
func (r RawBody) MessageBody() []byte {
	return r.Body
}

// Retain implements RawMessageSource. Since RawBody already owns its bytes,
// it returns itself.
func (r RawBody) Retain() RawMessageSource {
	return r
}

// Len returns the total wire length of the message (header + body).
func (r RawBody) Len() int {
	return 5 + len(r.Body)
}

// WriteTo writes the complete wire protocol message to w.
// This is the fast path for forwarding messages without parsing.
// decodeBackendMessage decodes raw bytes into a pgproto3.BackendMessage.
// The returned message is newly allocated.
func decodeBackendMessage(raw RawBody) (pgproto3.BackendMessage, error) {
	// Create the appropriate message type based on message type
	var msg pgproto3.BackendMessage
	switch raw.Type {
	case MsgServerAuth:
		// Authentication messages - need to peek at first 4 bytes
		if len(raw.Body) < 4 {
			return nil, fmt.Errorf("authentication message too short")
		}
		authType := binary.BigEndian.Uint32(raw.Body[0:4])
		switch authType {
		case 0:
			msg = &pgproto3.AuthenticationOk{}
		case 3:
			msg = &pgproto3.AuthenticationCleartextPassword{}
		case 5:
			msg = &pgproto3.AuthenticationMD5Password{}
		case 7:
			msg = &pgproto3.AuthenticationGSS{}
		case 8:
			msg = &pgproto3.AuthenticationGSSContinue{}
		case 10:
			msg = &pgproto3.AuthenticationSASL{}
		case 11:
			msg = &pgproto3.AuthenticationSASLContinue{}
		case 12:
			msg = &pgproto3.AuthenticationSASLFinal{}
		default:
			return nil, fmt.Errorf("unknown authentication type: %d", authType)
		}
	case MsgServerParseComplete:
		msg = &pgproto3.ParseComplete{}
	case MsgServerBindComplete:
		msg = &pgproto3.BindComplete{}
	case MsgServerCloseComplete:
		msg = &pgproto3.CloseComplete{}
	case MsgServerNotificationResponse:
		msg = &pgproto3.NotificationResponse{}
	case MsgServerCopyDone:
		msg = &pgproto3.CopyDone{}
	case MsgServerCommandComplete:
		msg = &pgproto3.CommandComplete{}
	case MsgServerCopyData:
		msg = &pgproto3.CopyData{}
	case MsgServerDataRow:
		msg = &pgproto3.DataRow{}
	case MsgServerErrorResponse:
		msg = &pgproto3.ErrorResponse{}
	case MsgServerCopyInResponse:
		msg = &pgproto3.CopyInResponse{}
	case MsgServerCopyOutResponse:
		msg = &pgproto3.CopyOutResponse{}
	case MsgServerEmptyQueryResponse:
		msg = &pgproto3.EmptyQueryResponse{}
	case MsgServerBackendKeyData:
		msg = &pgproto3.BackendKeyData{}
	case MsgServerNoData:
		msg = &pgproto3.NoData{}
	case MsgServerNoticeResponse:
		msg = &pgproto3.NoticeResponse{}
	case MsgServerParameterStatus:
		msg = &pgproto3.ParameterStatus{}
	case MsgServerPortalSuspended:
		msg = &pgproto3.PortalSuspended{}
	case MsgServerParameterDescription:
		msg = &pgproto3.ParameterDescription{}
	case MsgServerRowDescription:
		msg = &pgproto3.RowDescription{}
	case MsgServerFuncCallResponse:
		msg = &pgproto3.FunctionCallResponse{}
	case MsgServerCopyBothResponse:
		msg = &pgproto3.CopyBothResponse{}
	case MsgServerReadyForQuery:
		msg = &pgproto3.ReadyForQuery{}
	default:
		return nil, fmt.Errorf("unknown backend message type: %c", raw.Type)
	}

	// Decode the body
	if err := msg.Decode(raw.Body); err != nil {
		return nil, fmt.Errorf("decode %T: %w", msg, err)
	}

	return msg, nil
}

// FromServer holds a lazily-parsed backend message.
// It stores a RawMessageSource and only parses when Parse() is called.
// Once parsed, the result is cached for subsequent calls.
type FromServer[T pgproto3.BackendMessage] struct {
	source   RawMessageSource
	parsed   T
	isParsed bool
}

// Parse decodes the message body if not already parsed and returns the result.
// The parsed result is cached for subsequent calls.
// This is when MessageBody() is called on the source - lazy extraction.
func (m *FromServer[T]) Parse() T {
	if m.isParsed {
		return m.parsed
	}
	// Lazily extract body bytes from source
	raw := RawBody{
		Type: m.source.MessageType(),
		Body: m.source.MessageBody(),
	}
	msg, err := decodeBackendMessage(raw)
	if err != nil {
		panic(fmt.Sprintf("FromServer.Parse: %v", err))
	}
	m.parsed = msg.(T)
	m.isParsed = true
	return m.parsed
}

// Source returns the underlying RawMessageSource.
func (m FromServer[T]) Source() RawMessageSource {
	return m.source
}

// retainFields returns the retained source and parsed state for use by generated Retain() methods.
func (m *FromServer[T]) retainFields() (RawMessageSource, T, bool) {
	if m.source != nil {
		return m.source.Retain(), m.parsed, m.isParsed
	}
	// No source - encode from parsed if available
	if m.isParsed {
		return EncodeBackendMessage(m.parsed), m.parsed, m.isParsed
	}
	return nil, m.parsed, m.isParsed
}

// FromClient holds a lazily-parsed frontend message.
// It stores a RawMessageSource and only parses when Parse() is called.
// Once parsed, the result is cached for subsequent calls.
type FromClient[T pgproto3.FrontendMessage] struct {
	source   RawMessageSource
	parsed   T
	isParsed bool
}

// Parse decodes the message body if not already parsed and returns the result.
// The parsed result is cached for subsequent calls.
// This is when MessageBody() is called on the source - lazy extraction.
func (m *FromClient[T]) Parse() T {
	if m.isParsed {
		return m.parsed
	}
	// Lazily extract body bytes from source
	raw := RawBody{
		Type: m.source.MessageType(),
		Body: m.source.MessageBody(),
	}
	msg, err := decodeFrontendMessage(raw)
	if err != nil {
		panic(fmt.Sprintf("FromClient.Parse: %v", err))
	}
	m.parsed = msg.(T)
	m.isParsed = true
	return m.parsed
}

// Source returns the underlying RawMessageSource.
func (m FromClient[T]) Source() RawMessageSource {
	return m.source
}

// retainFields returns the retained source and parsed state for use by generated Retain() methods.
func (m *FromClient[T]) retainFields() (RawMessageSource, T, bool) {
	if m.source != nil {
		return m.source.Retain(), m.parsed, m.isParsed
	}
	// No source - encode from parsed if available
	if m.isParsed {
		return EncodeFrontendMessage(m.parsed), m.parsed, m.isParsed
	}
	return nil, m.parsed, m.isParsed
}

// ServerParsed creates a FromServer from an already-parsed message.
// This is used for backward compatibility with code that already has parsed messages.
func ServerParsed[T pgproto3.BackendMessage](msg T) FromServer[T] {
	return FromServer[T]{parsed: msg, isParsed: true}
}

// ClientParsed creates a FromClient from an already-parsed message.
// This is used for backward compatibility with code that already has parsed messages.
func ClientParsed[T pgproto3.FrontendMessage](msg T) FromClient[T] {
	return FromClient[T]{parsed: msg, isParsed: true}
}

// EncodeBackendMessage encodes a pgproto3.BackendMessage to RawBody.
func EncodeBackendMessage(msg pgproto3.BackendMessage) RawBody {
	encoded, err := msg.Encode(nil)
	if err != nil || len(encoded) < 5 {
		return RawBody{}
	}
	return RawBody{
		Type: MsgType(encoded[0]),
		Body: encoded[5:], // Skip the 5-byte header (type + length)
	}
}

// EncodeFrontendMessage encodes a pgproto3.FrontendMessage to RawBody.
func EncodeFrontendMessage(msg pgproto3.FrontendMessage) RawBody {
	encoded, err := msg.Encode(nil)
	if err != nil || len(encoded) < 5 {
		return RawBody{}
	}
	return RawBody{
		Type: MsgType(encoded[0]),
		Body: encoded[5:], // Skip the 5-byte header (type + length)
	}
}

// decodeFrontendMessage decodes raw bytes into a pgproto3.FrontendMessage.
// The returned message is newly allocated.
func decodeFrontendMessage(raw RawBody) (pgproto3.FrontendMessage, error) {
	var msg pgproto3.FrontendMessage
	switch raw.Type {
	case MsgClientBind:
		msg = &pgproto3.Bind{}
	case MsgClientClose:
		msg = &pgproto3.Close{}
	case MsgClientCopyDone:
		msg = &pgproto3.CopyDone{}
	case MsgClientCopyData:
		msg = &pgproto3.CopyData{}
	case MsgClientDescribe:
		msg = &pgproto3.Describe{}
	case MsgClientExecute:
		msg = &pgproto3.Execute{}
	case MsgClientCopyFail:
		msg = &pgproto3.CopyFail{}
	case MsgClientFunc:
		msg = &pgproto3.FunctionCall{}
	case MsgClientFlush:
		msg = &pgproto3.Flush{}
	case MsgClientParse:
		msg = &pgproto3.Parse{}
	case MsgClientPassword:
		// Password or SASL response - context-dependent
		// Default to PasswordMessage for now
		msg = &pgproto3.PasswordMessage{}
	case MsgClientQuery:
		msg = &pgproto3.Query{}
	case MsgClientSync:
		msg = &pgproto3.Sync{}
	case MsgClientTerminate:
		msg = &pgproto3.Terminate{}
	default:
		return nil, fmt.Errorf("unknown frontend message type: %c", raw.Type)
	}

	if err := msg.Decode(raw.Body); err != nil {
		return nil, fmt.Errorf("decode %T: %w", msg, err)
	}

	return msg, nil
}
