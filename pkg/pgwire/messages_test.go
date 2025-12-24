package pgwire

import (
	"fmt"
	"testing"

	"github.com/jackc/pgx/v5/pgproto3"
)

// TestToClientMessage tests conversion of pgproto3 frontend messages to pgwire types.
func TestToClientMessage(t *testing.T) {
	tests := []struct {
		name     string
		input    pgproto3.FrontendMessage
		wantType string
		wantOk   bool
	}{
		// Startup messages
		{
			name:     "StartupMessage",
			input:    &pgproto3.StartupMessage{ProtocolVersion: 196608, Parameters: map[string]string{"user": "test"}},
			wantType: "ClientStartupStartupMessage",
			wantOk:   true,
		},
		{
			name:     "SSLRequest",
			input:    &pgproto3.SSLRequest{},
			wantType: "ClientStartupSSLRequest",
			wantOk:   true,
		},
		{
			name:     "PasswordMessage",
			input:    &pgproto3.PasswordMessage{Password: "secret"},
			wantType: "ClientStartupPasswordMessage",
			wantOk:   true,
		},
		{
			name:     "SASLInitialResponse",
			input:    &pgproto3.SASLInitialResponse{AuthMechanism: "SCRAM-SHA-256"},
			wantType: "ClientStartupSASLInitialResponse",
			wantOk:   true,
		},
		{
			name:     "SASLResponse",
			input:    &pgproto3.SASLResponse{Data: []byte("response")},
			wantType: "ClientStartupSASLResponse",
			wantOk:   true,
		},

		// Simple query messages
		{
			name:     "Query",
			input:    &pgproto3.Query{String: "SELECT 1"},
			wantType: "ClientSimpleQueryQuery",
			wantOk:   true,
		},
		{
			name:     "FunctionCall",
			input:    &pgproto3.FunctionCall{Function: 123},
			wantType: "ClientSimpleQueryFunctionCall",
			wantOk:   true,
		},

		// Extended query messages
		{
			name:     "Parse",
			input:    &pgproto3.Parse{Name: "stmt1", Query: "SELECT $1"},
			wantType: "ClientExtendedQueryParse",
			wantOk:   true,
		},
		{
			name:     "Bind",
			input:    &pgproto3.Bind{DestinationPortal: "p1", PreparedStatement: "stmt1"},
			wantType: "ClientExtendedQueryBind",
			wantOk:   true,
		},
		{
			name:     "Execute",
			input:    &pgproto3.Execute{Portal: "p1", MaxRows: 0},
			wantType: "ClientExtendedQueryExecute",
			wantOk:   true,
		},
		{
			name:     "Describe",
			input:    &pgproto3.Describe{ObjectType: 'S', Name: "stmt1"},
			wantType: "ClientExtendedQueryDescribe",
			wantOk:   true,
		},
		{
			name:     "Close",
			input:    &pgproto3.Close{ObjectType: 'S', Name: "stmt1"},
			wantType: "ClientExtendedQueryClose",
			wantOk:   true,
		},
		{
			name:     "Sync",
			input:    &pgproto3.Sync{},
			wantType: "ClientExtendedQuerySync",
			wantOk:   true,
		},
		{
			name:     "Flush",
			input:    &pgproto3.Flush{},
			wantType: "ClientExtendedQueryFlush",
			wantOk:   true,
		},

		// Copy messages
		{
			name:     "CopyData",
			input:    &pgproto3.CopyData{Data: []byte("data")},
			wantType: "ClientCopyCopyData",
			wantOk:   true,
		},
		{
			name:     "CopyDone",
			input:    &pgproto3.CopyDone{},
			wantType: "ClientCopyCopyDone",
			wantOk:   true,
		},
		{
			name:     "CopyFail",
			input:    &pgproto3.CopyFail{Message: "error"},
			wantType: "ClientCopyCopyFail",
			wantOk:   true,
		},

		// Cancel
		{
			name:     "CancelRequest",
			input:    &pgproto3.CancelRequest{ProcessID: 123, SecretKey: 456},
			wantType: "ClientCancelCancelRequest",
			wantOk:   true,
		},

		// Terminate
		{
			name:     "Terminate",
			input:    &pgproto3.Terminate{},
			wantType: "ClientTerminateConnTerminate",
			wantOk:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, ok := ToClientMessage(tt.input)
			if ok != tt.wantOk {
				t.Errorf("ToClientMessage() ok = %v, want %v", ok, tt.wantOk)
				return
			}
			if !ok {
				return
			}
			gotType := typeName(got)
			if gotType != tt.wantType {
				t.Errorf("ToClientMessage() type = %v, want %v", gotType, tt.wantType)
			}
			// Verify round-trip
			if got.Client() != tt.input {
				t.Errorf("ToClientMessage().Client() did not return original message")
			}
			if got.PgwireMessage() != tt.input {
				t.Errorf("ToClientMessage().PgwireMessage() did not return original message")
			}
		})
	}
}

// TestToServerMessage tests conversion of pgproto3 backend messages to pgwire types.
func TestToServerMessage(t *testing.T) {
	tests := []struct {
		name     string
		input    pgproto3.BackendMessage
		wantType string
		wantOk   bool
	}{
		// Startup messages
		{
			name:     "AuthenticationOk",
			input:    &pgproto3.AuthenticationOk{},
			wantType: "ServerStartupAuthenticationOk",
			wantOk:   true,
		},
		{
			name:     "AuthenticationCleartextPassword",
			input:    &pgproto3.AuthenticationCleartextPassword{},
			wantType: "ServerStartupAuthenticationCleartextPassword",
			wantOk:   true,
		},
		{
			name:     "AuthenticationMD5Password",
			input:    &pgproto3.AuthenticationMD5Password{Salt: [4]byte{1, 2, 3, 4}},
			wantType: "ServerStartupAuthenticationMD5Password",
			wantOk:   true,
		},
		{
			name:     "AuthenticationSASL",
			input:    &pgproto3.AuthenticationSASL{AuthMechanisms: []string{"SCRAM-SHA-256"}},
			wantType: "ServerStartupAuthenticationSASL",
			wantOk:   true,
		},
		{
			name:     "AuthenticationSASLContinue",
			input:    &pgproto3.AuthenticationSASLContinue{Data: []byte("data")},
			wantType: "ServerStartupAuthenticationSASLContinue",
			wantOk:   true,
		},
		{
			name:     "AuthenticationSASLFinal",
			input:    &pgproto3.AuthenticationSASLFinal{Data: []byte("data")},
			wantType: "ServerStartupAuthenticationSASLFinal",
			wantOk:   true,
		},
		{
			name:     "BackendKeyData",
			input:    &pgproto3.BackendKeyData{ProcessID: 123, SecretKey: 456},
			wantType: "ServerStartupBackendKeyData",
			wantOk:   true,
		},

		// Response messages
		{
			name:     "ReadyForQuery",
			input:    &pgproto3.ReadyForQuery{TxStatus: 'I'},
			wantType: "ServerResponseReadyForQuery",
			wantOk:   true,
		},
		{
			name:     "CommandComplete",
			input:    &pgproto3.CommandComplete{CommandTag: []byte("SELECT 1")},
			wantType: "ServerResponseCommandComplete",
			wantOk:   true,
		},
		{
			name:     "DataRow",
			input:    &pgproto3.DataRow{Values: [][]byte{[]byte("1")}},
			wantType: "ServerResponseDataRow",
			wantOk:   true,
		},
		{
			name:     "EmptyQueryResponse",
			input:    &pgproto3.EmptyQueryResponse{},
			wantType: "ServerResponseEmptyQueryResponse",
			wantOk:   true,
		},
		{
			name:     "ErrorResponse",
			input:    &pgproto3.ErrorResponse{Severity: "ERROR", Code: "42000", Message: "test error"},
			wantType: "ServerResponseErrorResponse",
			wantOk:   true,
		},

		// Extended query messages
		{
			name:     "ParseComplete",
			input:    &pgproto3.ParseComplete{},
			wantType: "ServerExtendedQueryParseComplete",
			wantOk:   true,
		},
		{
			name:     "BindComplete",
			input:    &pgproto3.BindComplete{},
			wantType: "ServerExtendedQueryBindComplete",
			wantOk:   true,
		},
		{
			name:     "CloseComplete",
			input:    &pgproto3.CloseComplete{},
			wantType: "ServerExtendedQueryCloseComplete",
			wantOk:   true,
		},
		{
			name:     "ParameterDescription",
			input:    &pgproto3.ParameterDescription{ParameterOIDs: []uint32{23}},
			wantType: "ServerExtendedQueryParameterDescription",
			wantOk:   true,
		},
		{
			name:     "RowDescription",
			input:    &pgproto3.RowDescription{Fields: []pgproto3.FieldDescription{{Name: []byte("col1")}}},
			wantType: "ServerExtendedQueryRowDescription",
			wantOk:   true,
		},
		{
			name:     "NoData",
			input:    &pgproto3.NoData{},
			wantType: "ServerExtendedQueryNoData",
			wantOk:   true,
		},
		{
			name:     "PortalSuspended",
			input:    &pgproto3.PortalSuspended{},
			wantType: "ServerExtendedQueryPortalSuspended",
			wantOk:   true,
		},

		// Copy messages
		{
			name:     "CopyInResponse",
			input:    &pgproto3.CopyInResponse{OverallFormat: 0},
			wantType: "ServerCopyCopyInResponse",
			wantOk:   true,
		},
		{
			name:     "CopyOutResponse",
			input:    &pgproto3.CopyOutResponse{OverallFormat: 0},
			wantType: "ServerCopyCopyOutResponse",
			wantOk:   true,
		},
		{
			name:     "CopyData",
			input:    &pgproto3.CopyData{Data: []byte("data")},
			wantType: "ServerCopyCopyData",
			wantOk:   true,
		},
		{
			name:     "CopyDone",
			input:    &pgproto3.CopyDone{},
			wantType: "ServerCopyCopyDone",
			wantOk:   true,
		},

		// Async messages
		{
			name:     "NoticeResponse",
			input:    &pgproto3.NoticeResponse{Severity: "WARNING", Message: "test"},
			wantType: "ServerAsyncNoticeResponse",
			wantOk:   true,
		},
		{
			name:     "NotificationResponse",
			input:    &pgproto3.NotificationResponse{PID: 123, Channel: "test", Payload: "data"},
			wantType: "ServerAsyncNotificationResponse",
			wantOk:   true,
		},
		{
			name:     "ParameterStatus",
			input:    &pgproto3.ParameterStatus{Name: "server_version", Value: "15.0"},
			wantType: "ServerAsyncParameterStatus",
			wantOk:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, ok := ToServerMessage(tt.input)
			if ok != tt.wantOk {
				t.Errorf("ToServerMessage() ok = %v, want %v", ok, tt.wantOk)
				return
			}
			if !ok {
				return
			}
			gotType := typeName(got)
			if gotType != tt.wantType {
				t.Errorf("ToServerMessage() type = %v, want %v", gotType, tt.wantType)
			}
			// Verify round-trip
			if got.Server() != tt.input {
				t.Errorf("ToServerMessage().Server() did not return original message")
			}
			if got.PgwireMessage() != tt.input {
				t.Errorf("ToServerMessage().PgwireMessage() did not return original message")
			}
		})
	}
}

// TestToMessage tests the unified message conversion function.
func TestToMessage(t *testing.T) {
	// Test frontend message
	query := &pgproto3.Query{String: "SELECT 1"}
	msg, ok := ToMessage(query)
	if !ok {
		t.Fatal("ToMessage failed for Query")
	}
	if _, isClient := msg.(ClientMessage); !isClient {
		t.Error("ToMessage(Query) should return ClientMessage")
	}

	// Test backend message
	rfq := &pgproto3.ReadyForQuery{TxStatus: 'I'}
	msg, ok = ToMessage(rfq)
	if !ok {
		t.Fatal("ToMessage failed for ReadyForQuery")
	}
	if _, isServer := msg.(ServerMessage); !isServer {
		t.Error("ToMessage(ReadyForQuery) should return ServerMessage")
	}
}

// TestIsSimpleQueryModeMessage tests simple query message detection.
func TestIsSimpleQueryModeMessage(t *testing.T) {
	tests := []struct {
		name  string
		input pgproto3.FrontendMessage
		want  bool
	}{
		{"Query", &pgproto3.Query{String: "SELECT 1"}, true},
		{"FunctionCall", &pgproto3.FunctionCall{Function: 123}, true},
		{"Parse", &pgproto3.Parse{Query: "SELECT $1"}, false},
		{"Bind", &pgproto3.Bind{}, false},
		{"Execute", &pgproto3.Execute{}, false},
		{"Sync", &pgproto3.Sync{}, false},
		{"Terminate", &pgproto3.Terminate{}, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := IsSimpleQueryModeMessage(tt.input); got != tt.want {
				t.Errorf("IsSimpleQueryModeMessage() = %v, want %v", got, tt.want)
			}
		})
	}
}

// TestIsExtendedQueryModeMessage tests extended query message detection.
func TestIsExtendedQueryModeMessage(t *testing.T) {
	tests := []struct {
		name  string
		input pgproto3.FrontendMessage
		want  bool
	}{
		{"Parse", &pgproto3.Parse{Query: "SELECT $1"}, true},
		{"Bind", &pgproto3.Bind{}, true},
		{"Execute", &pgproto3.Execute{}, true},
		{"Describe", &pgproto3.Describe{ObjectType: 'S'}, true},
		{"Close", &pgproto3.Close{ObjectType: 'S'}, true},
		{"Sync", &pgproto3.Sync{}, true},
		{"Flush", &pgproto3.Flush{}, true},
		{"Query", &pgproto3.Query{String: "SELECT 1"}, false},
		{"Terminate", &pgproto3.Terminate{}, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := IsExtendedQueryModeMessage(tt.input); got != tt.want {
				t.Errorf("IsExtendedQueryModeMessage() = %v, want %v", got, tt.want)
			}
		})
	}
}

// TestIsCopyModeMessage tests copy mode message detection.
func TestIsCopyModeMessage(t *testing.T) {
	tests := []struct {
		name  string
		input pgproto3.FrontendMessage
		want  bool
	}{
		{"CopyData", &pgproto3.CopyData{Data: []byte("data")}, true},
		{"CopyDone", &pgproto3.CopyDone{}, true},
		{"CopyFail", &pgproto3.CopyFail{Message: "error"}, true},
		{"Query", &pgproto3.Query{String: "COPY"}, false},
		{"Parse", &pgproto3.Parse{}, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := IsCopyModeMessage(tt.input); got != tt.want {
				t.Errorf("IsCopyModeMessage() = %v, want %v", got, tt.want)
			}
		})
	}
}

// TestIsCancelMessage tests cancel message detection.
func TestIsCancelMessage(t *testing.T) {
	tests := []struct {
		name  string
		input pgproto3.FrontendMessage
		want  bool
	}{
		{"CancelRequest", &pgproto3.CancelRequest{ProcessID: 123, SecretKey: 456}, true},
		{"Query", &pgproto3.Query{String: "SELECT 1"}, false},
		{"Terminate", &pgproto3.Terminate{}, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := IsCancelMessage(tt.input); got != tt.want {
				t.Errorf("IsCancelMessage() = %v, want %v", got, tt.want)
			}
		})
	}
}

// TestIsTerminateConnMessage tests terminate message detection.
func TestIsTerminateConnMessage(t *testing.T) {
	tests := []struct {
		name  string
		input pgproto3.FrontendMessage
		want  bool
	}{
		{"Terminate", &pgproto3.Terminate{}, true},
		{"Query", &pgproto3.Query{String: "SELECT 1"}, false},
		{"CancelRequest", &pgproto3.CancelRequest{}, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := IsTerminateConnMessage(tt.input); got != tt.want {
				t.Errorf("IsTerminateConnMessage() = %v, want %v", got, tt.want)
			}
		})
	}
}

// TestIsBackendStartupModeMessage tests backend startup message detection.
func TestIsBackendStartupModeMessage(t *testing.T) {
	tests := []struct {
		name  string
		input pgproto3.BackendMessage
		want  bool
	}{
		{"AuthenticationOk", &pgproto3.AuthenticationOk{}, true},
		{"AuthenticationCleartextPassword", &pgproto3.AuthenticationCleartextPassword{}, true},
		{"AuthenticationMD5Password", &pgproto3.AuthenticationMD5Password{}, true},
		{"AuthenticationSASL", &pgproto3.AuthenticationSASL{}, true},
		{"AuthenticationSASLContinue", &pgproto3.AuthenticationSASLContinue{}, true},
		{"AuthenticationSASLFinal", &pgproto3.AuthenticationSASLFinal{}, true},
		{"BackendKeyData", &pgproto3.BackendKeyData{}, true},
		{"ReadyForQuery", &pgproto3.ReadyForQuery{}, false},
		{"ErrorResponse", &pgproto3.ErrorResponse{}, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := IsBackendStartupModeMessage(tt.input); got != tt.want {
				t.Errorf("IsBackendStartupModeMessage() = %v, want %v", got, tt.want)
			}
		})
	}
}

// TestIsBackendExtendedQueryModeMessage tests backend extended query message detection.
func TestIsBackendExtendedQueryModeMessage(t *testing.T) {
	tests := []struct {
		name  string
		input pgproto3.BackendMessage
		want  bool
	}{
		{"ParseComplete", &pgproto3.ParseComplete{}, true},
		{"BindComplete", &pgproto3.BindComplete{}, true},
		{"CloseComplete", &pgproto3.CloseComplete{}, true},
		{"ParameterDescription", &pgproto3.ParameterDescription{}, true},
		{"RowDescription", &pgproto3.RowDescription{}, true},
		{"NoData", &pgproto3.NoData{}, true},
		{"PortalSuspended", &pgproto3.PortalSuspended{}, true},
		{"ReadyForQuery", &pgproto3.ReadyForQuery{}, false},
		{"DataRow", &pgproto3.DataRow{}, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := IsBackendExtendedQueryModeMessage(tt.input); got != tt.want {
				t.Errorf("IsBackendExtendedQueryModeMessage() = %v, want %v", got, tt.want)
			}
		})
	}
}

// TestIsBackendCopyModeMessage tests backend copy mode message detection.
func TestIsBackendCopyModeMessage(t *testing.T) {
	tests := []struct {
		name  string
		input pgproto3.BackendMessage
		want  bool
	}{
		{"CopyInResponse", &pgproto3.CopyInResponse{}, true},
		{"CopyOutResponse", &pgproto3.CopyOutResponse{}, true},
		{"CopyBothResponse", &pgproto3.CopyBothResponse{}, true},
		{"CopyData", &pgproto3.CopyData{}, true},
		{"CopyDone", &pgproto3.CopyDone{}, true},
		{"ReadyForQuery", &pgproto3.ReadyForQuery{}, false},
		{"ErrorResponse", &pgproto3.ErrorResponse{}, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := IsBackendCopyModeMessage(tt.input); got != tt.want {
				t.Errorf("IsBackendCopyModeMessage() = %v, want %v", got, tt.want)
			}
		})
	}
}

// TestIsBackendResponseMessage tests backend response message detection.
func TestIsBackendResponseMessage(t *testing.T) {
	tests := []struct {
		name  string
		input pgproto3.BackendMessage
		want  bool
	}{
		{"ReadyForQuery", &pgproto3.ReadyForQuery{}, true},
		{"CommandComplete", &pgproto3.CommandComplete{}, true},
		{"DataRow", &pgproto3.DataRow{}, true},
		{"EmptyQueryResponse", &pgproto3.EmptyQueryResponse{}, true},
		{"ErrorResponse", &pgproto3.ErrorResponse{}, true},
		{"FunctionCallResponse", &pgproto3.FunctionCallResponse{}, true},
		{"ParseComplete", &pgproto3.ParseComplete{}, false},
		{"CopyInResponse", &pgproto3.CopyInResponse{}, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := IsBackendResponseMessage(tt.input); got != tt.want {
				t.Errorf("IsBackendResponseMessage() = %v, want %v", got, tt.want)
			}
		})
	}
}

// TestIsBackendAsyncMessage tests backend async message detection.
func TestIsBackendAsyncMessage(t *testing.T) {
	tests := []struct {
		name  string
		input pgproto3.BackendMessage
		want  bool
	}{
		{"NoticeResponse", &pgproto3.NoticeResponse{}, true},
		{"NotificationResponse", &pgproto3.NotificationResponse{}, true},
		{"ParameterStatus", &pgproto3.ParameterStatus{}, true},
		{"ReadyForQuery", &pgproto3.ReadyForQuery{}, false},
		{"ErrorResponse", &pgproto3.ErrorResponse{}, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := IsBackendAsyncMessage(tt.input); got != tt.want {
				t.Errorf("IsBackendAsyncMessage() = %v, want %v", got, tt.want)
			}
		})
	}
}

func typeName(v any) string {
	return fmt.Sprintf("%T", v)[7:] // Strip "pgwire." prefix (7 chars)
}
