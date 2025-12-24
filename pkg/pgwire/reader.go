package pgwire

import (
	"encoding/binary"
	"fmt"
	"io"

	"github.com/jackc/pgx/v5/pgproto3"
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
		return ServerResponseReadyForQuery{LazyServer[*pgproto3.ReadyForQuery]{RawBody: raw}}, nil
	case 'C':
		return ServerResponseCommandComplete{LazyServer[*pgproto3.CommandComplete]{RawBody: raw}}, nil
	case 'D':
		return ServerResponseDataRow{LazyServer[*pgproto3.DataRow]{RawBody: raw}}, nil
	case 'I':
		return ServerResponseEmptyQueryResponse{LazyServer[*pgproto3.EmptyQueryResponse]{RawBody: raw}}, nil
	case 'E':
		return ServerResponseErrorResponse{LazyServer[*pgproto3.ErrorResponse]{RawBody: raw}}, nil
	case 'V':
		return ServerResponseFunctionCallResponse{LazyServer[*pgproto3.FunctionCallResponse]{RawBody: raw}}, nil

	// Extended query messages
	case '1':
		return ServerExtendedQueryParseComplete{LazyServer[*pgproto3.ParseComplete]{RawBody: raw}}, nil
	case '2':
		return ServerExtendedQueryBindComplete{LazyServer[*pgproto3.BindComplete]{RawBody: raw}}, nil
	case 't':
		return ServerExtendedQueryParameterDescription{LazyServer[*pgproto3.ParameterDescription]{RawBody: raw}}, nil
	case 'T':
		return ServerExtendedQueryRowDescription{LazyServer[*pgproto3.RowDescription]{RawBody: raw}}, nil
	case 'n':
		return ServerExtendedQueryNoData{LazyServer[*pgproto3.NoData]{RawBody: raw}}, nil
	case 's':
		return ServerExtendedQueryPortalSuspended{LazyServer[*pgproto3.PortalSuspended]{RawBody: raw}}, nil
	case '3':
		return ServerExtendedQueryCloseComplete{LazyServer[*pgproto3.CloseComplete]{RawBody: raw}}, nil

	// Copy messages
	case 'G':
		return ServerCopyCopyInResponse{LazyServer[*pgproto3.CopyInResponse]{RawBody: raw}}, nil
	case 'H':
		return ServerCopyCopyOutResponse{LazyServer[*pgproto3.CopyOutResponse]{RawBody: raw}}, nil
	case 'W':
		return ServerCopyCopyBothResponse{LazyServer[*pgproto3.CopyBothResponse]{RawBody: raw}}, nil
	case 'd':
		return ServerCopyCopyData{LazyServer[*pgproto3.CopyData]{RawBody: raw}}, nil
	case 'c':
		return ServerCopyCopyDone{LazyServer[*pgproto3.CopyDone]{RawBody: raw}}, nil

	// Async messages
	case 'N':
		return ServerAsyncNoticeResponse{LazyServer[*pgproto3.NoticeResponse]{RawBody: raw}}, nil
	case 'A':
		return ServerAsyncNotificationResponse{LazyServer[*pgproto3.NotificationResponse]{RawBody: raw}}, nil
	case 'S':
		return ServerAsyncParameterStatus{LazyServer[*pgproto3.ParameterStatus]{RawBody: raw}}, nil

	// Startup/Authentication messages
	case 'R':
		return wrapAuthenticationMessage(raw)
	case 'K':
		return ServerStartupBackendKeyData{LazyServer[*pgproto3.BackendKeyData]{RawBody: raw}}, nil

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
		return ServerStartupAuthenticationOk{LazyServer[*pgproto3.AuthenticationOk]{RawBody: raw}}, nil
	case 3:
		return ServerStartupAuthenticationCleartextPassword{LazyServer[*pgproto3.AuthenticationCleartextPassword]{RawBody: raw}}, nil
	case 5:
		return ServerStartupAuthenticationMD5Password{LazyServer[*pgproto3.AuthenticationMD5Password]{RawBody: raw}}, nil
	case 7:
		return ServerStartupAuthenticationGSS{LazyServer[*pgproto3.AuthenticationGSS]{RawBody: raw}}, nil
	case 8:
		return ServerStartupAuthenticationGSSContinue{LazyServer[*pgproto3.AuthenticationGSSContinue]{RawBody: raw}}, nil
	case 10:
		return ServerStartupAuthenticationSASL{LazyServer[*pgproto3.AuthenticationSASL]{RawBody: raw}}, nil
	case 11:
		return ServerStartupAuthenticationSASLContinue{LazyServer[*pgproto3.AuthenticationSASLContinue]{RawBody: raw}}, nil
	case 12:
		return ServerStartupAuthenticationSASLFinal{LazyServer[*pgproto3.AuthenticationSASLFinal]{RawBody: raw}}, nil
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
		return ClientSimpleQueryQuery{LazyClient[*pgproto3.Query]{RawBody: raw}}, nil
	case 'F':
		return ClientSimpleQueryFunctionCall{LazyClient[*pgproto3.FunctionCall]{RawBody: raw}}, nil

	// Extended query
	case 'P':
		return ClientExtendedQueryParse{LazyClient[*pgproto3.Parse]{RawBody: raw}}, nil
	case 'B':
		return ClientExtendedQueryBind{LazyClient[*pgproto3.Bind]{RawBody: raw}}, nil
	case 'E':
		return ClientExtendedQueryExecute{LazyClient[*pgproto3.Execute]{RawBody: raw}}, nil
	case 'S':
		return ClientExtendedQuerySync{LazyClient[*pgproto3.Sync]{RawBody: raw}}, nil
	case 'D':
		return ClientExtendedQueryDescribe{LazyClient[*pgproto3.Describe]{RawBody: raw}}, nil
	case 'C':
		return ClientExtendedQueryClose{LazyClient[*pgproto3.Close]{RawBody: raw}}, nil
	case 'H':
		return ClientExtendedQueryFlush{LazyClient[*pgproto3.Flush]{RawBody: raw}}, nil

	// Copy
	case 'd':
		return ClientCopyCopyData{LazyClient[*pgproto3.CopyData]{RawBody: raw}}, nil
	case 'c':
		return ClientCopyCopyDone{LazyClient[*pgproto3.CopyDone]{RawBody: raw}}, nil
	case 'f':
		return ClientCopyCopyFail{LazyClient[*pgproto3.CopyFail]{RawBody: raw}}, nil

	// Terminate
	case 'X':
		return ClientTerminateConnTerminate{LazyClient[*pgproto3.Terminate]{RawBody: raw}}, nil

	// Authentication responses (during startup)
	case 'p':
		// 'p' can be PasswordMessage, SASLInitialResponse, SASLResponse, or GSSResponse
		// We default to PasswordMessage; the caller can use context to determine the correct type
		return ClientStartupPasswordMessage{LazyClient[*pgproto3.PasswordMessage]{RawBody: raw}}, nil

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
