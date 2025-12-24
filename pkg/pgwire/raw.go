package pgwire

import (
	"encoding/binary"
	"fmt"
	"io"

	"github.com/jackc/pgx/v5/pgproto3"
)

// RawMessageSource provides lazy access to wire protocol message bytes.
// Implementations include RawBody (owned bytes) and Cursor (ring buffer reference).
// This interface enables zero-allocation message handling by deferring byte extraction
// until Parse() is called.
type RawMessageSource interface {
	// MessageType returns the single-byte message type identifier.
	MessageType() byte

	// MessageBody returns the message body bytes (after 5-byte header).
	// This may allocate if the bytes need to be copied (e.g., ring buffer wraparound).
	// Only call this when you need to parse the message.
	MessageBody() []byte

	// WriteTo writes the complete wire protocol message (header + body) to w.
	WriteTo(w io.Writer) (int64, error)

	// Retain returns an owned copy of this message source that is safe to keep
	// beyond the current iteration. For already-owned sources like RawBody,
	// this returns the receiver. For borrowed sources like Cursor, this copies the bytes.
	Retain() RawMessageSource
}

// RawBody holds unparsed PostgreSQL wire protocol message bytes.
// It can be forwarded directly without parsing.
// RawBody implements RawMessageSource.
type RawBody struct {
	Type byte   // Message type identifier (single byte)
	Body []byte // Message body (after 5-byte header: type + length)
}

// IsZero returns true if this RawBody has no data.
func (r RawBody) IsZero() bool {
	return r.Body == nil
}

// MessageType implements RawMessageSource.
func (r RawBody) MessageType() byte {
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

// BodyLen returns just the body length.
func (r RawBody) BodyLen() int {
	return len(r.Body)
}

// WriteTo writes the complete wire protocol message to w.
// This is the fast path for forwarding messages without parsing.
func (r RawBody) WriteTo(w io.Writer) (int64, error) {
	var header [5]byte
	header[0] = r.Type
	binary.BigEndian.PutUint32(header[1:5], uint32(len(r.Body)+4))

	n1, err := w.Write(header[:])
	if err != nil {
		return int64(n1), err
	}

	n2, err := w.Write(r.Body)
	return int64(n1 + n2), err
}

// AppendTo appends the complete wire protocol message to buf.
func (r RawBody) AppendTo(buf []byte) []byte {
	buf = append(buf, r.Type)
	buf = binary.BigEndian.AppendUint32(buf, uint32(len(r.Body)+4))
	buf = append(buf, r.Body...)
	return buf
}

// IsFastForwardable returns true if this message type can be forwarded
// without any inspection or modification. These messages don't affect
// protocol state and are never rewritten by the proxy.
func (r RawBody) IsFastForwardable() bool {
	return IsFastForwardableType(r.Type)
}

// IsFastForwardableType returns true if the given message type byte
// represents a message that can be forwarded without inspection.
func IsFastForwardableType(t byte) bool {
	switch t {
	case
		'D', // DataRow - bulk result data
		'd', // CopyData - bulk COPY data
		'T', // RowDescription - schema info, forwarded as-is
		'C', // CommandComplete - command finished
		'1', // ParseComplete
		'2', // BindComplete
		'3', // CloseComplete
		'N', // NoticeResponse
		'n', // NoData
		't', // ParameterDescription
		's': // PortalSuspended
		return true
	}
	return false
}

// decodeBackendMessage decodes raw bytes into a pgproto3.BackendMessage.
// The returned message is newly allocated.
func decodeBackendMessage(raw RawBody) (pgproto3.BackendMessage, error) {
	// Create the appropriate message type based on type byte
	var msg pgproto3.BackendMessage
	switch raw.Type {
	case 'R':
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
	case '1':
		msg = &pgproto3.ParseComplete{}
	case '2':
		msg = &pgproto3.BindComplete{}
	case '3':
		msg = &pgproto3.CloseComplete{}
	case 'A':
		msg = &pgproto3.NotificationResponse{}
	case 'c':
		msg = &pgproto3.CopyDone{}
	case 'C':
		msg = &pgproto3.CommandComplete{}
	case 'd':
		msg = &pgproto3.CopyData{}
	case 'D':
		msg = &pgproto3.DataRow{}
	case 'E':
		msg = &pgproto3.ErrorResponse{}
	case 'G':
		msg = &pgproto3.CopyInResponse{}
	case 'H':
		msg = &pgproto3.CopyOutResponse{}
	case 'I':
		msg = &pgproto3.EmptyQueryResponse{}
	case 'K':
		msg = &pgproto3.BackendKeyData{}
	case 'n':
		msg = &pgproto3.NoData{}
	case 'N':
		msg = &pgproto3.NoticeResponse{}
	case 'S':
		msg = &pgproto3.ParameterStatus{}
	case 's':
		msg = &pgproto3.PortalSuspended{}
	case 't':
		msg = &pgproto3.ParameterDescription{}
	case 'T':
		msg = &pgproto3.RowDescription{}
	case 'V':
		msg = &pgproto3.FunctionCallResponse{}
	case 'W':
		msg = &pgproto3.CopyBothResponse{}
	case 'Z':
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

// LazyServer holds a lazily-parsed backend message.
// It stores a RawMessageSource and only parses when Parse() is called.
// Once parsed, the result is cached for subsequent calls.
type LazyServer[T pgproto3.BackendMessage] struct {
	source   RawMessageSource
	parsed   T
	isParsed bool
}

// Parse decodes the message body if not already parsed and returns the result.
// The parsed result is cached for subsequent calls.
// This is when MessageBody() is called on the source - lazy extraction.
func (m *LazyServer[T]) Parse() T {
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
		panic(fmt.Sprintf("LazyServer.Parse: %v", err))
	}
	m.parsed = msg.(T)
	m.isParsed = true
	return m.parsed
}

// IsParsed returns true if the message has already been parsed.
func (m LazyServer[T]) IsParsed() bool {
	return m.isParsed
}

// Source returns the underlying RawMessageSource.
func (m LazyServer[T]) Source() RawMessageSource {
	return m.source
}

// Body returns the raw message body bytes for direct access.
// This is useful for fast accessors that read specific fields without full parsing.
func (m LazyServer[T]) Body() []byte {
	if m.source != nil {
		return m.source.MessageBody()
	}
	return nil
}

// Raw returns a RawBody for compatibility with existing interfaces.
// If the source is already a RawBody, returns it directly.
// Otherwise, extracts the bytes from the source.
func (m LazyServer[T]) Raw() RawBody {
	if m.source == nil {
		if m.isParsed {
			return EncodeBackendMessage(m.parsed)
		}
		return RawBody{}
	}
	if raw, ok := m.source.(RawBody); ok {
		return raw
	}
	return RawBody{
		Type: m.source.MessageType(),
		Body: m.source.MessageBody(),
	}
}

// retainFields returns the retained source and parsed state for use by generated Retain() methods.
func (m *LazyServer[T]) retainFields() (RawMessageSource, T, bool) {
	if m.source != nil {
		return m.source.Retain(), m.parsed, m.isParsed
	}
	// No source - encode from parsed if available
	if m.isParsed {
		return EncodeBackendMessage(m.parsed), m.parsed, m.isParsed
	}
	return nil, m.parsed, m.isParsed
}

// WriteTo writes the message to w. If the message was created from a source,
// delegates to source.WriteTo(). If created from parsed, encodes and writes.
func (m *LazyServer[T]) WriteTo(w io.Writer) (int64, error) {
	if m.source != nil {
		return m.source.WriteTo(w)
	}
	if m.isParsed {
		raw := EncodeBackendMessage(m.parsed)
		return raw.WriteTo(w)
	}
	return 0, nil
}

// LazyClient holds a lazily-parsed frontend message.
// It stores a RawMessageSource and only parses when Parse() is called.
// Once parsed, the result is cached for subsequent calls.
type LazyClient[T pgproto3.FrontendMessage] struct {
	source   RawMessageSource
	parsed   T
	isParsed bool
}

// Parse decodes the message body if not already parsed and returns the result.
// The parsed result is cached for subsequent calls.
// This is when MessageBody() is called on the source - lazy extraction.
func (m *LazyClient[T]) Parse() T {
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
		panic(fmt.Sprintf("LazyClient.Parse: %v", err))
	}
	m.parsed = msg.(T)
	m.isParsed = true
	return m.parsed
}

// IsParsed returns true if the message has already been parsed.
func (m LazyClient[T]) IsParsed() bool {
	return m.isParsed
}

// Source returns the underlying RawMessageSource.
func (m LazyClient[T]) Source() RawMessageSource {
	return m.source
}

// Body returns the raw message body bytes for direct access.
// This is useful for fast accessors that read specific fields without full parsing.
func (m LazyClient[T]) Body() []byte {
	if m.source != nil {
		return m.source.MessageBody()
	}
	return nil
}

// Raw returns a RawBody for compatibility with existing interfaces.
// If the source is already a RawBody, returns it directly.
// Otherwise, extracts the bytes from the source.
func (m LazyClient[T]) Raw() RawBody {
	if m.source == nil {
		if m.isParsed {
			return EncodeFrontendMessage(m.parsed)
		}
		return RawBody{}
	}
	if raw, ok := m.source.(RawBody); ok {
		return raw
	}
	return RawBody{
		Type: m.source.MessageType(),
		Body: m.source.MessageBody(),
	}
}

// retainFields returns the retained source and parsed state for use by generated Retain() methods.
func (m *LazyClient[T]) retainFields() (RawMessageSource, T, bool) {
	if m.source != nil {
		return m.source.Retain(), m.parsed, m.isParsed
	}
	// No source - encode from parsed if available
	if m.isParsed {
		return EncodeFrontendMessage(m.parsed), m.parsed, m.isParsed
	}
	return nil, m.parsed, m.isParsed
}

// WriteTo writes the message to w. If the message was created from a source,
// delegates to source.WriteTo(). If created from parsed, encodes and writes.
func (m *LazyClient[T]) WriteTo(w io.Writer) (int64, error) {
	if m.source != nil {
		return m.source.WriteTo(w)
	}
	if m.isParsed {
		raw := EncodeFrontendMessage(m.parsed)
		return raw.WriteTo(w)
	}
	return 0, nil
}

// NewLazyServerFromParsed creates a LazyServer from an already-parsed message.
// This is used for backward compatibility with code that already has parsed messages.
func NewLazyServerFromParsed[T pgproto3.BackendMessage](msg T) LazyServer[T] {
	return LazyServer[T]{parsed: msg, isParsed: true}
}

// NewLazyClientFromParsed creates a LazyClient from an already-parsed message.
// This is used for backward compatibility with code that already has parsed messages.
func NewLazyClientFromParsed[T pgproto3.FrontendMessage](msg T) LazyClient[T] {
	return LazyClient[T]{parsed: msg, isParsed: true}
}

// EnsureRaw returns a RawBody for forwarding.
// This is equivalent to Raw() - the "ensure" semantics are handled by Raw() which
// extracts from source or encodes from parsed as needed.
func (m *LazyServer[T]) EnsureRaw() RawBody {
	return m.Raw()
}

// EnsureRaw returns a RawBody for forwarding.
// This is equivalent to Raw() - the "ensure" semantics are handled by Raw() which
// extracts from source or encodes from parsed as needed.
func (m *LazyClient[T]) EnsureRaw() RawBody {
	return m.Raw()
}

// EncodeBackendMessage encodes a pgproto3.BackendMessage to RawBody.
func EncodeBackendMessage(msg pgproto3.BackendMessage) RawBody {
	encoded, err := msg.Encode(nil)
	if err != nil || len(encoded) < 5 {
		return RawBody{}
	}
	return RawBody{
		Type: encoded[0],
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
		Type: encoded[0],
		Body: encoded[5:], // Skip the 5-byte header (type + length)
	}
}

// decodeFrontendMessage decodes raw bytes into a pgproto3.FrontendMessage.
// The returned message is newly allocated.
func decodeFrontendMessage(raw RawBody) (pgproto3.FrontendMessage, error) {
	var msg pgproto3.FrontendMessage
	switch raw.Type {
	case 'B':
		msg = &pgproto3.Bind{}
	case 'C':
		msg = &pgproto3.Close{}
	case 'c':
		msg = &pgproto3.CopyDone{}
	case 'd':
		msg = &pgproto3.CopyData{}
	case 'D':
		msg = &pgproto3.Describe{}
	case 'E':
		msg = &pgproto3.Execute{}
	case 'f':
		msg = &pgproto3.CopyFail{}
	case 'F':
		msg = &pgproto3.FunctionCall{}
	case 'H':
		msg = &pgproto3.Flush{}
	case 'P':
		msg = &pgproto3.Parse{}
	case 'p':
		// Password or SASL response - context-dependent
		// Default to PasswordMessage for now
		msg = &pgproto3.PasswordMessage{}
	case 'Q':
		msg = &pgproto3.Query{}
	case 'S':
		msg = &pgproto3.Sync{}
	case 'X':
		msg = &pgproto3.Terminate{}
	default:
		return nil, fmt.Errorf("unknown frontend message type: %c", raw.Type)
	}

	if err := msg.Decode(raw.Body); err != nil {
		return nil, fmt.Errorf("decode %T: %w", msg, err)
	}

	return msg, nil
}
