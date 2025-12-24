package pgwire

import (
	"encoding/binary"
	"fmt"
	"io"

	"github.com/jackc/pgx/v5/pgproto3"
)

// RawBody holds unparsed PostgreSQL wire protocol message bytes.
// It can be forwarded directly without parsing.
type RawBody struct {
	Type byte   // Message type identifier (single byte)
	Body []byte // Message body (after 5-byte header: type + length)
}

// IsZero returns true if this RawBody has no data.
func (r RawBody) IsZero() bool {
	return r.Body == nil
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
// It stores raw wire bytes and only parses when Parse() is called.
// Once parsed, the result is cached for subsequent calls.
type LazyServer[T pgproto3.BackendMessage] struct {
	RawBody
	parsed   T
	isParsed bool
}

// Parse decodes the message body if not already parsed and returns the result.
// The parsed result is cached for subsequent calls.
func (m *LazyServer[T]) Parse() T {
	if m.isParsed {
		return m.parsed
	}
	// Decode from RawBody
	msg, err := decodeBackendMessage(m.RawBody)
	if err != nil {
		// This shouldn't happen if RawBody was properly constructed
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

// Raw returns the underlying RawBody for fast forwarding.
func (m LazyServer[T]) Raw() RawBody {
	return m.RawBody
}

// LazyClient holds a lazily-parsed frontend message.
// It stores raw wire bytes and only parses when Parse() is called.
// Once parsed, the result is cached for subsequent calls.
type LazyClient[T pgproto3.FrontendMessage] struct {
	RawBody
	parsed   T
	isParsed bool
}

// Parse decodes the message body if not already parsed and returns the result.
// The parsed result is cached for subsequent calls.
func (m *LazyClient[T]) Parse() T {
	if m.isParsed {
		return m.parsed
	}
	// Decode from RawBody
	msg, err := decodeFrontendMessage(m.RawBody)
	if err != nil {
		// This shouldn't happen if RawBody was properly constructed
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

// Raw returns the underlying RawBody for fast forwarding.
func (m LazyClient[T]) Raw() RawBody {
	return m.RawBody
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

// EnsureRaw ensures the RawBody is populated, encoding from parsed if necessary.
// Returns the RawBody for forwarding. This is useful when you have a parsed
// message and want to forward it without re-encoding on every call.
func (m *LazyServer[T]) EnsureRaw() RawBody {
	if !m.RawBody.IsZero() {
		return m.RawBody
	}
	if !m.isParsed {
		return m.RawBody // Empty, nothing to encode
	}
	// Encode the parsed message to get raw bytes
	m.RawBody = EncodeBackendMessage(m.parsed)
	return m.RawBody
}

// EnsureRaw ensures the RawBody is populated, encoding from parsed if necessary.
func (m *LazyClient[T]) EnsureRaw() RawBody {
	if !m.RawBody.IsZero() {
		return m.RawBody
	}
	if !m.isParsed {
		return m.RawBody // Empty, nothing to encode
	}
	// Encode the parsed message to get raw bytes
	m.RawBody = EncodeFrontendMessage(m.parsed)
	return m.RawBody
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
