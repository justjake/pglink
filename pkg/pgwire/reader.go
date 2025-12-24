package pgwire

import (
	"encoding/binary"
	"fmt"
	"io"

)

// RawReader reads PostgreSQL wire protocol messages from an io.Reader
// and returns lazy-parsed type-safe message wrappers.
type RawReader struct {
	r         io.Reader
	headerBuf [5]byte
}

// NewRawReader creates a new RawReader that reads from r.
func NewRawReader(r io.Reader) *RawReader {
	return &RawReader{r: r}
}

// ReadRaw reads a single message's raw bytes from the wire.
// It reads the 5-byte header (type + length) then the body.
// The returned RawBody can be forwarded directly or parsed lazily.
func (r *RawReader) ReadRaw() (RawBody, error) {
	// Read 5-byte header: type (1) + length (4)
	if _, err := io.ReadFull(r.r, r.headerBuf[:]); err != nil {
		return RawBody{}, err
	}

	msgType := r.headerBuf[0]
	// Length includes the 4-byte length field itself
	length := binary.BigEndian.Uint32(r.headerBuf[1:5])
	if length < 4 {
		return RawBody{}, fmt.Errorf("invalid message length: %d", length)
	}

	bodyLen := length - 4
	body := make([]byte, bodyLen)
	if bodyLen > 0 {
		if _, err := io.ReadFull(r.r, body); err != nil {
			return RawBody{}, err
		}
	}

	return RawBody{Type: msgType, Body: body}, nil
}

// ReadServerMessage reads a backend message and wraps it in a type-safe ServerMessage.
// The message body is not parsed until Parse() is called on the wrapper.
func (r *RawReader) ReadServerMessage() (ServerMessage, error) {
	raw, err := r.ReadRaw()
	if err != nil {
		return nil, err
	}
	msg, err := WrapServerMessage(raw)
	if err != nil {
		return nil, err
	}
	return msg, nil
}

// ReadClientMessage reads a frontend message and wraps it in a type-safe ClientMessage.
// The message body is not parsed until Parse() is called on the wrapper.
// Note: This does not handle startup messages (StartupMessage, SSLRequest, etc.)
// which have a different wire format without the type byte.
func (r *RawReader) ReadClientMessage() (ClientMessage, error) {
	raw, err := r.ReadRaw()
	if err != nil {
		return nil, err
	}
	msg, err := WrapClientMessage(raw)
	if err != nil {
		return nil, err
	}
	return msg, nil
}

// WrapServerMessage wraps raw bytes in the appropriate type-safe ServerMessage wrapper.
// This dispatches based on the message type byte and returns a lazy-parsed message.
func WrapServerMessage(raw RawBody) (ServerMessage, error) {
	switch raw.Type {
	// Response messages
	case 'Z':
		return ServerResponseReadyForQuery{source: raw}, nil
	case 'C':
		return ServerResponseCommandComplete{source: raw}, nil
	case 'D':
		return ServerResponseDataRow{source: raw}, nil
	case 'I':
		return ServerResponseEmptyQueryResponse{source: raw}, nil
	case 'E':
		return ServerResponseErrorResponse{source: raw}, nil
	case 'V':
		return ServerResponseFunctionCallResponse{source: raw}, nil

	// Extended query messages
	case '1':
		return ServerExtendedQueryParseComplete{source: raw}, nil
	case '2':
		return ServerExtendedQueryBindComplete{source: raw}, nil
	case 't':
		return ServerExtendedQueryParameterDescription{source: raw}, nil
	case 'T':
		return ServerExtendedQueryRowDescription{source: raw}, nil
	case 'n':
		return ServerExtendedQueryNoData{source: raw}, nil
	case 's':
		return ServerExtendedQueryPortalSuspended{source: raw}, nil
	case '3':
		return ServerExtendedQueryCloseComplete{source: raw}, nil

	// Copy messages
	case 'G':
		return ServerCopyCopyInResponse{source: raw}, nil
	case 'H':
		return ServerCopyCopyOutResponse{source: raw}, nil
	case 'W':
		return ServerCopyCopyBothResponse{source: raw}, nil
	case 'd':
		return ServerCopyCopyData{source: raw}, nil
	case 'c':
		return ServerCopyCopyDone{source: raw}, nil

	// Async messages
	case 'N':
		return ServerAsyncNoticeResponse{source: raw}, nil
	case 'A':
		return ServerAsyncNotificationResponse{source: raw}, nil
	case 'S':
		return ServerAsyncParameterStatus{source: raw}, nil

	// Startup/Authentication messages
	case 'R':
		return wrapAuthenticationMessage(raw)
	case 'K':
		return ServerStartupBackendKeyData{source: raw}, nil

	default:
		return nil, fmt.Errorf("unknown server message type: %c (0x%02x)", raw.Type, raw.Type)
	}
}

// wrapAuthenticationMessage handles the 'R' message type which has subtypes.
func wrapAuthenticationMessage(raw RawBody) (ServerMessage, error) {
	if len(raw.Body) < 4 {
		return nil, fmt.Errorf("authentication message too short: %d bytes", len(raw.Body))
	}
	authType := binary.BigEndian.Uint32(raw.Body[0:4])
	switch authType {
	case 0:
		return ServerStartupAuthenticationOk{source: raw}, nil
	case 3:
		return ServerStartupAuthenticationCleartextPassword{source: raw}, nil
	case 5:
		return ServerStartupAuthenticationMD5Password{source: raw}, nil
	case 7:
		return ServerStartupAuthenticationGSS{source: raw}, nil
	case 8:
		return ServerStartupAuthenticationGSSContinue{source: raw}, nil
	case 10:
		return ServerStartupAuthenticationSASL{source: raw}, nil
	case 11:
		return ServerStartupAuthenticationSASLContinue{source: raw}, nil
	case 12:
		return ServerStartupAuthenticationSASLFinal{source: raw}, nil
	default:
		return nil, fmt.Errorf("unknown authentication type: %d", authType)
	}
}

// WrapClientMessage wraps raw bytes in the appropriate type-safe ClientMessage wrapper.
// This dispatches based on the message type byte and returns a lazy-parsed message.
// Note: This does not handle startup messages (StartupMessage, SSLRequest, etc.)
// which have a different wire format without the type byte.
func WrapClientMessage(raw RawBody) (ClientMessage, error) {
	switch raw.Type {
	// Simple query
	case 'Q':
		return ClientSimpleQueryQuery{source: raw}, nil
	case 'F':
		return ClientSimpleQueryFunctionCall{source: raw}, nil

	// Extended query
	case 'P':
		return ClientExtendedQueryParse{source: raw}, nil
	case 'B':
		return ClientExtendedQueryBind{source: raw}, nil
	case 'E':
		return ClientExtendedQueryExecute{source: raw}, nil
	case 'S':
		return ClientExtendedQuerySync{source: raw}, nil
	case 'D':
		return ClientExtendedQueryDescribe{source: raw}, nil
	case 'C':
		return ClientExtendedQueryClose{source: raw}, nil
	case 'H':
		return ClientExtendedQueryFlush{source: raw}, nil

	// Copy
	case 'd':
		return ClientCopyCopyData{source: raw}, nil
	case 'c':
		return ClientCopyCopyDone{source: raw}, nil
	case 'f':
		return ClientCopyCopyFail{source: raw}, nil

	// Terminate
	case 'X':
		return ClientTerminateConnTerminate{source: raw}, nil

	// Authentication responses (during startup)
	case 'p':
		// 'p' can be PasswordMessage, SASLInitialResponse, SASLResponse, or GSSResponse
		// We default to PasswordMessage; the caller can use context to determine the correct type
		return ClientStartupPasswordMessage{source: raw}, nil

	default:
		return nil, fmt.Errorf("unknown client message type: %c (0x%02x)", raw.Type, raw.Type)
	}
}

// ServerRawReader is an interface for reading server messages that can provide raw bytes.
type ServerRawReader interface {
	ReadServerMessage() (ServerMessage, error)
}

// ClientRawReader is an interface for reading client messages that can provide raw bytes.
type ClientRawReader interface {
	ReadClientMessage() (ClientMessage, error)
}

// Ensure RawReader implements both interfaces
var (
	_ ServerRawReader = (*RawReader)(nil)
	_ ClientRawReader = (*RawReader)(nil)
)
