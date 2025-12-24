package pgwire

import (
	"bytes"
	"encoding/binary"
	"testing"

	"github.com/jackc/pgx/v5/pgproto3"
)

// encodeMessage encodes a message type and body into wire format.
func encodeMessage(t byte, body []byte) []byte {
	buf := make([]byte, 5+len(body))
	buf[0] = t
	binary.BigEndian.PutUint32(buf[1:5], uint32(len(body)+4))
	copy(buf[5:], body)
	return buf
}

func TestRawReader_ReadRaw(t *testing.T) {
	// Create a ReadyForQuery message
	body := []byte{'I'}
	wire := encodeMessage('Z', body)

	r := NewRawReader(bytes.NewReader(wire))
	raw, err := r.ReadRaw()
	if err != nil {
		t.Fatalf("ReadRaw() error = %v", err)
	}
	if raw.Type != 'Z' {
		t.Errorf("ReadRaw() Type = %c, want Z", raw.Type)
	}
	if !bytes.Equal(raw.Body, body) {
		t.Errorf("ReadRaw() Body = %v, want %v", raw.Body, body)
	}
}

func TestRawReader_ReadServerMessage(t *testing.T) {
	tests := []struct {
		name     string
		msgType  byte
		body     []byte
		wantType string
	}{
		{
			name:     "ReadyForQuery",
			msgType:  'Z',
			body:     []byte{'I'},
			wantType: "ServerResponseReadyForQuery",
		},
		{
			name:     "DataRow",
			msgType:  'D',
			body:     []byte{0, 2, 0, 0, 0, 3, 'f', 'o', 'o', 0, 0, 0, 3, 'b', 'a', 'r'},
			wantType: "ServerResponseDataRow",
		},
		{
			name:     "CommandComplete",
			msgType:  'C',
			body:     []byte("SELECT 1\x00"),
			wantType: "ServerResponseCommandComplete",
		},
		{
			name:     "ParseComplete",
			msgType:  '1',
			body:     []byte{},
			wantType: "ServerExtendedQueryParseComplete",
		},
		{
			name:     "BindComplete",
			msgType:  '2',
			body:     []byte{},
			wantType: "ServerExtendedQueryBindComplete",
		},
		{
			name:     "CloseComplete",
			msgType:  '3',
			body:     []byte{},
			wantType: "ServerExtendedQueryCloseComplete",
		},
		{
			name:     "RowDescription",
			msgType:  'T',
			body:     []byte{0, 0}, // 0 columns
			wantType: "ServerExtendedQueryRowDescription",
		},
		{
			name:     "NoData",
			msgType:  'n',
			body:     []byte{},
			wantType: "ServerExtendedQueryNoData",
		},
		{
			name:     "CopyInResponse",
			msgType:  'G',
			body:     []byte{0, 0, 0}, // text format, 0 columns
			wantType: "ServerCopyCopyInResponse",
		},
		{
			name:     "CopyOutResponse",
			msgType:  'H',
			body:     []byte{0, 0, 0}, // text format, 0 columns
			wantType: "ServerCopyCopyOutResponse",
		},
		{
			name:     "CopyData",
			msgType:  'd',
			body:     []byte("test data"),
			wantType: "ServerCopyCopyData",
		},
		{
			name:     "CopyDone",
			msgType:  'c',
			body:     []byte{},
			wantType: "ServerCopyCopyDone",
		},
		{
			name:     "NoticeResponse",
			msgType:  'N',
			body:     []byte{0}, // empty notice
			wantType: "ServerAsyncNoticeResponse",
		},
		{
			name:     "ParameterStatus",
			msgType:  'S',
			body:     []byte("server_version\x0015.0\x00"),
			wantType: "ServerAsyncParameterStatus",
		},
		{
			name:     "ErrorResponse",
			msgType:  'E',
			body:     []byte{0}, // empty error
			wantType: "ServerResponseErrorResponse",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			wire := encodeMessage(tt.msgType, tt.body)
			r := NewRawReader(bytes.NewReader(wire))
			msg, err := r.ReadServerMessage()
			if err != nil {
				t.Fatalf("ReadServerMessage() error = %v", err)
			}

			gotType := readerTypeName(msg)
			if gotType != tt.wantType {
				t.Errorf("ReadServerMessage() type = %s, want %s", gotType, tt.wantType)
			}

			// Verify raw body is accessible
			raw := msg.(interface{ Raw() RawBody }).Raw()
			if raw.Type != tt.msgType {
				t.Errorf("Raw().Type = %c, want %c", raw.Type, tt.msgType)
			}
		})
	}
}

func TestRawReader_ReadServerMessage_Authentication(t *testing.T) {
	tests := []struct {
		name     string
		authType uint32
		extra    []byte
		wantType string
	}{
		{
			name:     "AuthenticationOk",
			authType: 0,
			wantType: "ServerStartupAuthenticationOk",
		},
		{
			name:     "AuthenticationCleartextPassword",
			authType: 3,
			wantType: "ServerStartupAuthenticationCleartextPassword",
		},
		{
			name:     "AuthenticationMD5Password",
			authType: 5,
			extra:    []byte{0, 0, 0, 0}, // salt
			wantType: "ServerStartupAuthenticationMD5Password",
		},
		{
			name:     "AuthenticationSASL",
			authType: 10,
			extra:    []byte("SCRAM-SHA-256\x00\x00"),
			wantType: "ServerStartupAuthenticationSASL",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			body := make([]byte, 4+len(tt.extra))
			binary.BigEndian.PutUint32(body[0:4], tt.authType)
			copy(body[4:], tt.extra)

			wire := encodeMessage('R', body)
			r := NewRawReader(bytes.NewReader(wire))
			msg, err := r.ReadServerMessage()
			if err != nil {
				t.Fatalf("ReadServerMessage() error = %v", err)
			}

			gotType := readerTypeName(msg)
			if gotType != tt.wantType {
				t.Errorf("ReadServerMessage() type = %s, want %s", gotType, tt.wantType)
			}
		})
	}
}

func TestRawReader_ReadClientMessage(t *testing.T) {
	tests := []struct {
		name     string
		msgType  byte
		body     []byte
		wantType string
	}{
		{
			name:     "Query",
			msgType:  'Q',
			body:     []byte("SELECT 1\x00"),
			wantType: "ClientSimpleQueryQuery",
		},
		{
			name:     "Parse",
			msgType:  'P',
			body:     []byte("\x00SELECT 1\x00\x00\x00"), // unnamed, query, 0 params
			wantType: "ClientExtendedQueryParse",
		},
		{
			name:     "Bind",
			msgType:  'B',
			body:     []byte("\x00\x00\x00\x00\x00\x00\x00\x00"), // minimal bind
			wantType: "ClientExtendedQueryBind",
		},
		{
			name:     "Execute",
			msgType:  'E',
			body:     []byte("\x00\x00\x00\x00\x00"), // unnamed portal, 0 rows
			wantType: "ClientExtendedQueryExecute",
		},
		{
			name:     "Sync",
			msgType:  'S',
			body:     []byte{},
			wantType: "ClientExtendedQuerySync",
		},
		{
			name:     "Describe",
			msgType:  'D',
			body:     []byte{'S', 0}, // describe unnamed statement
			wantType: "ClientExtendedQueryDescribe",
		},
		{
			name:     "Close",
			msgType:  'C',
			body:     []byte{'S', 0}, // close unnamed statement
			wantType: "ClientExtendedQueryClose",
		},
		{
			name:     "Flush",
			msgType:  'H',
			body:     []byte{},
			wantType: "ClientExtendedQueryFlush",
		},
		{
			name:     "CopyData",
			msgType:  'd',
			body:     []byte("test data"),
			wantType: "ClientCopyCopyData",
		},
		{
			name:     "CopyDone",
			msgType:  'c',
			body:     []byte{},
			wantType: "ClientCopyCopyDone",
		},
		{
			name:     "CopyFail",
			msgType:  'f',
			body:     []byte("error message\x00"),
			wantType: "ClientCopyCopyFail",
		},
		{
			name:     "Terminate",
			msgType:  'X',
			body:     []byte{},
			wantType: "ClientTerminateConnTerminate",
		},
		{
			name:     "PasswordMessage",
			msgType:  'p',
			body:     []byte("password\x00"),
			wantType: "ClientStartupPasswordMessage",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			wire := encodeMessage(tt.msgType, tt.body)
			r := NewRawReader(bytes.NewReader(wire))
			msg, err := r.ReadClientMessage()
			if err != nil {
				t.Fatalf("ReadClientMessage() error = %v", err)
			}

			gotType := readerTypeName(msg)
			if gotType != tt.wantType {
				t.Errorf("ReadClientMessage() type = %s, want %s", gotType, tt.wantType)
			}

			// Verify raw body is accessible
			raw := msg.(interface{ Raw() RawBody }).Raw()
			if raw.Type != tt.msgType {
				t.Errorf("Raw().Type = %c, want %c", raw.Type, tt.msgType)
			}
		})
	}
}

func TestRawReader_MultipleMessages(t *testing.T) {
	// Create multiple messages
	var buf bytes.Buffer
	buf.Write(encodeMessage('Z', []byte{'I'}))
	buf.Write(encodeMessage('D', []byte{0, 1, 0, 0, 0, 3, 'f', 'o', 'o'})) // 1 column, "foo"
	buf.Write(encodeMessage('C', []byte("SELECT 1\x00")))
	buf.Write(encodeMessage('Z', []byte{'I'}))

	r := NewRawReader(&buf)

	// Read first message
	msg1, err := r.ReadServerMessage()
	if err != nil {
		t.Fatalf("ReadServerMessage() 1 error = %v", err)
	}
	if _, ok := msg1.(ServerResponseReadyForQuery); !ok {
		t.Errorf("msg1 type = %T, want ServerResponseReadyForQuery", msg1)
	}

	// Read second message
	msg2, err := r.ReadServerMessage()
	if err != nil {
		t.Fatalf("ReadServerMessage() 2 error = %v", err)
	}
	if dr, ok := msg2.(ServerResponseDataRow); !ok {
		t.Errorf("msg2 type = %T, want ServerResponseDataRow", msg2)
	} else {
		// Test fast accessor
		if dr.ColumnCount() != 1 {
			t.Errorf("ColumnCount() = %d, want 1", dr.ColumnCount())
		}
	}

	// Read third message
	msg3, err := r.ReadServerMessage()
	if err != nil {
		t.Fatalf("ReadServerMessage() 3 error = %v", err)
	}
	if _, ok := msg3.(ServerResponseCommandComplete); !ok {
		t.Errorf("msg3 type = %T, want ServerResponseCommandComplete", msg3)
	}

	// Read fourth message
	msg4, err := r.ReadServerMessage()
	if err != nil {
		t.Fatalf("ReadServerMessage() 4 error = %v", err)
	}
	if rfq, ok := msg4.(ServerResponseReadyForQuery); !ok {
		t.Errorf("msg4 type = %T, want ServerResponseReadyForQuery", msg4)
	} else {
		// Test fast accessor
		if rfq.TxStatusByte() != 'I' {
			t.Errorf("TxStatusByte() = %c, want I", rfq.TxStatusByte())
		}
	}
}

func TestRawReader_LazyParsing(t *testing.T) {
	// Create a DataRow message
	body := []byte{0, 2, 0, 0, 0, 3, 'f', 'o', 'o', 0, 0, 0, 3, 'b', 'a', 'r'}
	wire := encodeMessage('D', body)

	r := NewRawReader(bytes.NewReader(wire))
	msg, err := r.ReadServerMessage()
	if err != nil {
		t.Fatalf("ReadServerMessage() error = %v", err)
	}

	dr := msg.(ServerResponseDataRow)

	// Fast accessor should work without parsing
	if dr.ColumnCount() != 2 {
		t.Errorf("ColumnCount() = %d, want 2", dr.ColumnCount())
	}

	// Should not be parsed yet
	if dr.IsParsed() {
		t.Error("IsParsed() = true before Parse(), want false")
	}

	// Now parse
	parsed := dr.Parse()
	if len(parsed.Values) != 2 {
		t.Errorf("Parse().Values len = %d, want 2", len(parsed.Values))
	}
	if string(parsed.Values[0]) != "foo" {
		t.Errorf("Parse().Values[0] = %s, want foo", parsed.Values[0])
	}
	if string(parsed.Values[1]) != "bar" {
		t.Errorf("Parse().Values[1] = %s, want bar", parsed.Values[1])
	}

	// Should be parsed now
	if !dr.IsParsed() {
		t.Error("IsParsed() = false after Parse(), want true")
	}
}

func TestRawReader_RoundTrip(t *testing.T) {
	// Create a message, read it, and verify WriteTo produces the same bytes
	body := []byte{'I'}
	wire := encodeMessage('Z', body)

	r := NewRawReader(bytes.NewReader(wire))
	raw, err := r.ReadRaw()
	if err != nil {
		t.Fatalf("ReadRaw() error = %v", err)
	}

	var buf bytes.Buffer
	n, err := raw.WriteTo(&buf)
	if err != nil {
		t.Fatalf("WriteTo() error = %v", err)
	}
	if n != int64(len(wire)) {
		t.Errorf("WriteTo() n = %d, want %d", n, len(wire))
	}
	if !bytes.Equal(buf.Bytes(), wire) {
		t.Errorf("WriteTo() bytes = %v, want %v", buf.Bytes(), wire)
	}
}

func TestWrapServerMessage_UnknownType(t *testing.T) {
	raw := RawBody{Type: 0xFF, Body: []byte{}}
	_, err := WrapServerMessage(raw)
	if err == nil {
		t.Error("WrapServerMessage() error = nil, want error for unknown type")
	}
}

func TestWrapClientMessage_UnknownType(t *testing.T) {
	raw := RawBody{Type: 0xFF, Body: []byte{}}
	_, err := WrapClientMessage(raw)
	if err == nil {
		t.Error("WrapClientMessage() error = nil, want error for unknown type")
	}
}

// readerTypeName returns the type name without package prefix for reader tests
func readerTypeName(v interface{}) string {
	switch v.(type) {
	case ServerResponseReadyForQuery:
		return "ServerResponseReadyForQuery"
	case ServerResponseCommandComplete:
		return "ServerResponseCommandComplete"
	case ServerResponseDataRow:
		return "ServerResponseDataRow"
	case ServerResponseEmptyQueryResponse:
		return "ServerResponseEmptyQueryResponse"
	case ServerResponseErrorResponse:
		return "ServerResponseErrorResponse"
	case ServerResponseFunctionCallResponse:
		return "ServerResponseFunctionCallResponse"
	case ServerExtendedQueryParseComplete:
		return "ServerExtendedQueryParseComplete"
	case ServerExtendedQueryBindComplete:
		return "ServerExtendedQueryBindComplete"
	case ServerExtendedQueryParameterDescription:
		return "ServerExtendedQueryParameterDescription"
	case ServerExtendedQueryRowDescription:
		return "ServerExtendedQueryRowDescription"
	case ServerExtendedQueryNoData:
		return "ServerExtendedQueryNoData"
	case ServerExtendedQueryPortalSuspended:
		return "ServerExtendedQueryPortalSuspended"
	case ServerExtendedQueryCloseComplete:
		return "ServerExtendedQueryCloseComplete"
	case ServerCopyCopyInResponse:
		return "ServerCopyCopyInResponse"
	case ServerCopyCopyOutResponse:
		return "ServerCopyCopyOutResponse"
	case ServerCopyCopyBothResponse:
		return "ServerCopyCopyBothResponse"
	case ServerCopyCopyData:
		return "ServerCopyCopyData"
	case ServerCopyCopyDone:
		return "ServerCopyCopyDone"
	case ServerAsyncNoticeResponse:
		return "ServerAsyncNoticeResponse"
	case ServerAsyncNotificationResponse:
		return "ServerAsyncNotificationResponse"
	case ServerAsyncParameterStatus:
		return "ServerAsyncParameterStatus"
	case ServerStartupAuthenticationOk:
		return "ServerStartupAuthenticationOk"
	case ServerStartupAuthenticationCleartextPassword:
		return "ServerStartupAuthenticationCleartextPassword"
	case ServerStartupAuthenticationMD5Password:
		return "ServerStartupAuthenticationMD5Password"
	case ServerStartupAuthenticationGSS:
		return "ServerStartupAuthenticationGSS"
	case ServerStartupAuthenticationGSSContinue:
		return "ServerStartupAuthenticationGSSContinue"
	case ServerStartupAuthenticationSASL:
		return "ServerStartupAuthenticationSASL"
	case ServerStartupAuthenticationSASLContinue:
		return "ServerStartupAuthenticationSASLContinue"
	case ServerStartupAuthenticationSASLFinal:
		return "ServerStartupAuthenticationSASLFinal"
	case ServerStartupBackendKeyData:
		return "ServerStartupBackendKeyData"
	case ClientSimpleQueryQuery:
		return "ClientSimpleQueryQuery"
	case ClientSimpleQueryFunctionCall:
		return "ClientSimpleQueryFunctionCall"
	case ClientExtendedQueryParse:
		return "ClientExtendedQueryParse"
	case ClientExtendedQueryBind:
		return "ClientExtendedQueryBind"
	case ClientExtendedQueryExecute:
		return "ClientExtendedQueryExecute"
	case ClientExtendedQuerySync:
		return "ClientExtendedQuerySync"
	case ClientExtendedQueryDescribe:
		return "ClientExtendedQueryDescribe"
	case ClientExtendedQueryClose:
		return "ClientExtendedQueryClose"
	case ClientExtendedQueryFlush:
		return "ClientExtendedQueryFlush"
	case ClientCopyCopyData:
		return "ClientCopyCopyData"
	case ClientCopyCopyDone:
		return "ClientCopyCopyDone"
	case ClientCopyCopyFail:
		return "ClientCopyCopyFail"
	case ClientTerminateConnTerminate:
		return "ClientTerminateConnTerminate"
	case ClientStartupPasswordMessage:
		return "ClientStartupPasswordMessage"
	default:
		return "unknown"
	}
}

// Benchmark comparing lazy vs eager parsing
func BenchmarkDataRow_LazyVsEager(b *testing.B) {
	// Create a DataRow with 10 columns
	var body []byte
	body = binary.BigEndian.AppendUint16(body, 10) // 10 columns
	for i := 0; i < 10; i++ {
		data := []byte("test data column")
		body = binary.BigEndian.AppendUint32(body, uint32(len(data)))
		body = append(body, data...)
	}
	wire := encodeMessage('D', body)

	b.Run("Lazy_ColumnCountOnly", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			r := NewRawReader(bytes.NewReader(wire))
			msg, _ := r.ReadServerMessage()
			dr := msg.(ServerResponseDataRow)
			_ = dr.ColumnCount() // Fast path - no parsing
		}
	})

	b.Run("Lazy_FullParse", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			r := NewRawReader(bytes.NewReader(wire))
			msg, _ := r.ReadServerMessage()
			dr := msg.(ServerResponseDataRow)
			_ = dr.Parse() // Full parse
		}
	})

	b.Run("Eager_pgproto3", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			msg := &pgproto3.DataRow{}
			_ = msg.Decode(body)
		}
	})
}

func BenchmarkReadyForQuery_LazyVsEager(b *testing.B) {
	body := []byte{'I'}
	wire := encodeMessage('Z', body)

	b.Run("Lazy_TxStatusOnly", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			r := NewRawReader(bytes.NewReader(wire))
			msg, _ := r.ReadServerMessage()
			rfq := msg.(ServerResponseReadyForQuery)
			_ = rfq.TxStatusByte() // Fast path
		}
	})

	b.Run("Lazy_FullParse", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			r := NewRawReader(bytes.NewReader(wire))
			msg, _ := r.ReadServerMessage()
			rfq := msg.(ServerResponseReadyForQuery)
			_ = rfq.Parse()
		}
	})

	b.Run("Eager_pgproto3", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			msg := &pgproto3.ReadyForQuery{}
			_ = msg.Decode(body)
		}
	})
}
