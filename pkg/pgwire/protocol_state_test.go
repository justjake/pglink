package pgwire

import (
	"testing"

	"github.com/jackc/pgx/v5/pgproto3"
)

// TestNewProtocolState tests that NewProtocolState creates a properly initialized state.
func TestNewProtocolState(t *testing.T) {
	state := NewProtocolState()

	if state.TxStatus != TxIdle {
		t.Errorf("TxStatus = %v, want %v", state.TxStatus, TxIdle)
	}
	if state.ExtendedQueryMode {
		t.Error("ExtendedQueryMode should be false")
	}
	if state.SyncsInFlight != 0 {
		t.Errorf("SyncsInFlight = %v, want 0", state.SyncsInFlight)
	}
	if state.CopyMode != CopyNone {
		t.Errorf("CopyMode = %v, want %v", state.CopyMode, CopyNone)
	}
	if state.Statements.Alive == nil {
		t.Error("Statements.Alive should not be nil")
	}
	if state.Portals.Alive == nil {
		t.Error("Portals.Alive should not be nil")
	}
}

// TestProtocolState_SimpleQuery tests state updates for simple query flow.
func TestProtocolState_SimpleQuery(t *testing.T) {
	state := NewProtocolState()

	// Send a Query message
	state.UpdateForFrontentMessage(&pgproto3.Query{String: "SELECT 1"})

	if state.ExtendedQueryMode {
		t.Error("ExtendedQueryMode should be false after Query")
	}
	if state.Statements.Executing == nil {
		t.Error("Statements.Executing should be set after Query")
	}

	// Server sends DataRow
	state.UpdateForServerMessage((*ServerResponseDataRow)(ServerParsed(&pgproto3.DataRow{Values: [][]byte{[]byte("1")}})))
	// No state change expected

	// Server sends CommandComplete
	state.UpdateForServerMessage((*ServerResponseCommandComplete)(ServerParsed(&pgproto3.CommandComplete{CommandTag: []byte("SELECT 1")})))

	// Server sends ReadyForQuery
	state.UpdateForServerMessage((*ServerResponseReadyForQuery)(ServerParsed(&pgproto3.ReadyForQuery{TxStatus: 'I'})))

	if state.TxStatus != TxIdle {
		t.Errorf("TxStatus = %v, want %v", state.TxStatus, TxIdle)
	}
	if state.Statements.Executing != nil {
		t.Error("Statements.Executing should be nil after ReadyForQuery")
	}
}

// TestProtocolState_ExtendedQuery tests state updates for extended query flow.
func TestProtocolState_ExtendedQuery(t *testing.T) {
	state := NewProtocolState()

	// Parse
	state.UpdateForFrontentMessage(&pgproto3.Parse{Name: "stmt1", Query: "SELECT $1"})

	if !state.ExtendedQueryMode {
		t.Error("ExtendedQueryMode should be true after Parse")
	}
	if _, ok := state.Statements.PendingCreate["stmt1"]; !ok {
		t.Error("stmt1 should be in PendingCreate after Parse")
	}

	// Describe statement
	state.UpdateForFrontentMessage(&pgproto3.Describe{ObjectType: 'S', Name: "stmt1"})

	// Sync
	state.UpdateForFrontentMessage(&pgproto3.Sync{})

	if state.SyncsInFlight != 1 {
		t.Errorf("SyncsInFlight = %v, want 1", state.SyncsInFlight)
	}

	// Server: ParseComplete
	state.UpdateForServerMessage((*ServerExtendedQueryParseComplete)(ServerParsed(&pgproto3.ParseComplete{})))

	if _, ok := state.Statements.Alive["stmt1"]; !ok {
		t.Error("stmt1 should be Alive after ParseComplete")
	}
	if len(state.Statements.PendingCreate) != 0 {
		t.Error("PendingCreate should be empty after ParseComplete")
	}

	// Server: ParameterDescription
	state.UpdateForServerMessage((*ServerExtendedQueryParameterDescription)(ServerParsed(&pgproto3.ParameterDescription{})))

	// Server: RowDescription
	state.UpdateForServerMessage((*ServerExtendedQueryRowDescription)(ServerParsed(&pgproto3.RowDescription{})))

	// Server: ReadyForQuery
	state.UpdateForServerMessage((*ServerResponseReadyForQuery)(ServerParsed(&pgproto3.ReadyForQuery{TxStatus: 'I'})))

	if state.SyncsInFlight != 0 {
		t.Errorf("SyncsInFlight = %v, want 0 after ReadyForQuery", state.SyncsInFlight)
	}
}

// TestProtocolState_BindExecute tests Bind and Execute flow.
func TestProtocolState_BindExecute(t *testing.T) {
	state := NewProtocolState()

	// First prepare a statement
	state.UpdateForFrontentMessage(&pgproto3.Parse{Name: "stmt1", Query: "SELECT $1"})
	state.UpdateForFrontentMessage(&pgproto3.Sync{})
	state.UpdateForServerMessage((*ServerExtendedQueryParseComplete)(ServerParsed(&pgproto3.ParseComplete{})))
	state.UpdateForServerMessage((*ServerResponseReadyForQuery)(ServerParsed(&pgproto3.ReadyForQuery{TxStatus: 'I'})))

	// Now bind and execute
	state.UpdateForFrontentMessage(&pgproto3.Bind{DestinationPortal: "p1", PreparedStatement: "stmt1"})

	if _, ok := state.Portals.PendingCreate["p1"]; !ok {
		t.Error("p1 should be in Portals.PendingCreate after Bind")
	}

	state.UpdateForFrontentMessage(&pgproto3.Execute{Portal: "p1"})

	if state.Portals.Executing == nil || *state.Portals.Executing != "p1" {
		t.Error("Portals.Executing should be 'p1' after Execute")
	}

	state.UpdateForFrontentMessage(&pgproto3.Sync{})

	// Server responses
	state.UpdateForServerMessage((*ServerExtendedQueryBindComplete)(ServerParsed(&pgproto3.BindComplete{})))

	if _, ok := state.Portals.Alive["p1"]; !ok {
		t.Error("p1 should be Alive after BindComplete")
	}

	state.UpdateForServerMessage((*ServerResponseDataRow)(ServerParsed(&pgproto3.DataRow{})))
	state.UpdateForServerMessage((*ServerResponseCommandComplete)(ServerParsed(&pgproto3.CommandComplete{CommandTag: []byte("SELECT 1")})))
	state.UpdateForServerMessage((*ServerResponseReadyForQuery)(ServerParsed(&pgproto3.ReadyForQuery{TxStatus: 'I'})))

	if state.Portals.Executing != nil {
		t.Error("Portals.Executing should be nil after ReadyForQuery")
	}
}

// TestProtocolState_CloseStatement tests closing a prepared statement.
func TestProtocolState_CloseStatement(t *testing.T) {
	state := NewProtocolState()

	// Prepare a statement first
	state.Statements.Alive["stmt1"] = true

	// Close the statement
	state.UpdateForFrontentMessage(&pgproto3.Close{ObjectType: 'S', Name: "stmt1"})

	if _, ok := state.Statements.PendingClose["stmt1"]; !ok {
		t.Error("stmt1 should be in PendingClose after Close")
	}

	state.UpdateForFrontentMessage(&pgproto3.Sync{})

	// Server: CloseComplete
	state.UpdateForServerMessage((*ServerExtendedQueryCloseComplete)(ServerParsed(&pgproto3.CloseComplete{})))

	if state.Statements.Alive["stmt1"] {
		t.Error("stmt1 should not be Alive after CloseComplete")
	}
}

// TestProtocolState_CopyIn tests COPY FROM (client to server) flow.
func TestProtocolState_CopyIn(t *testing.T) {
	state := NewProtocolState()

	// Server sends CopyInResponse
	state.UpdateForServerMessage((*ServerCopyCopyInResponse)(ServerParsed(&pgproto3.CopyInResponse{})))

	if state.CopyMode != CopyIn {
		t.Errorf("CopyMode = %v, want CopyIn", state.CopyMode)
	}

	// Server sends CopyDone
	state.UpdateForServerMessage((*ServerCopyCopyDone)(ServerParsed(&pgproto3.CopyDone{})))

	if state.CopyMode != CopyNone {
		t.Errorf("CopyMode = %v, want CopyNone after CopyDone", state.CopyMode)
	}
}

// TestProtocolState_CopyOut tests COPY TO (server to client) flow.
func TestProtocolState_CopyOut(t *testing.T) {
	state := NewProtocolState()

	// Server sends CopyOutResponse
	state.UpdateForServerMessage((*ServerCopyCopyOutResponse)(ServerParsed(&pgproto3.CopyOutResponse{})))

	if state.CopyMode != CopyOut {
		t.Errorf("CopyMode = %v, want CopyOut", state.CopyMode)
	}

	// Server sends CopyData
	state.UpdateForServerMessage((*ServerCopyCopyData)(ServerParsed(&pgproto3.CopyData{Data: []byte("data")})))

	// CopyMode should still be CopyOut
	if state.CopyMode != CopyOut {
		t.Errorf("CopyMode = %v, want CopyOut after CopyData", state.CopyMode)
	}

	// Server sends CopyDone
	state.UpdateForServerMessage((*ServerCopyCopyDone)(ServerParsed(&pgproto3.CopyDone{})))

	if state.CopyMode != CopyNone {
		t.Errorf("CopyMode = %v, want CopyNone after CopyDone", state.CopyMode)
	}
}

// TestProtocolState_ErrorInExtendedQuery tests error handling in extended query mode.
func TestProtocolState_ErrorInExtendedQuery(t *testing.T) {
	state := NewProtocolState()

	// Start extended query
	state.UpdateForFrontentMessage(&pgproto3.Parse{Name: "stmt1", Query: "INVALID SQL"})
	state.UpdateForFrontentMessage(&pgproto3.Sync{})

	// Server sends ErrorResponse
	state.UpdateForServerMessage((*ServerResponseErrorResponse)(ServerParsed(&pgproto3.ErrorResponse{
		Severity: "ERROR",
		Code:     "42601",
		Message:  "syntax error",
	})))

	if !state.ServerIgnoringMessagesUntilSync {
		t.Error("ServerIgnoringMessagesUntilSync should be true after ErrorResponse in extended query mode")
	}

	// Server sends ReadyForQuery
	state.UpdateForServerMessage((*ServerResponseReadyForQuery)(ServerParsed(&pgproto3.ReadyForQuery{TxStatus: 'I'})))

	if state.ServerIgnoringMessagesUntilSync {
		t.Error("ServerIgnoringMessagesUntilSync should be false after ReadyForQuery")
	}
}

// TestProtocolState_TransactionStatus tests transaction status tracking.
func TestProtocolState_TransactionStatus(t *testing.T) {
	tests := []struct {
		name     string
		txStatus byte
		want     TxStatus
	}{
		{"Idle", 'I', TxIdle},
		{"InTransaction", 'T', TxInTransaction},
		{"Failed", 'E', TxFailed},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			state := NewProtocolState()
			state.UpdateForServerMessage((*ServerResponseReadyForQuery)(ServerParsed(&pgproto3.ReadyForQuery{TxStatus: tt.txStatus})))

			if state.TxStatus != tt.want {
				t.Errorf("TxStatus = %v, want %v", state.TxStatus, tt.want)
			}
		})
	}
}

// TestProtocolState_ParameterStatus tests parameter status tracking.
func TestProtocolState_ParameterStatus(t *testing.T) {
	state := NewProtocolState()

	// Set a parameter
	state.UpdateForServerMessage((*ServerAsyncParameterStatus)(ServerParsed(&pgproto3.ParameterStatus{
		Name:  "client_encoding",
		Value: "UTF8",
	})))

	if v := state.ParameterStatuses["client_encoding"]; v != "UTF8" {
		t.Errorf("client_encoding = %q, want %q", v, "UTF8")
	}

	// Update the parameter
	state.UpdateForServerMessage((*ServerAsyncParameterStatus)(ServerParsed(&pgproto3.ParameterStatus{
		Name:  "client_encoding",
		Value: "LATIN1",
	})))

	if v := state.ParameterStatuses["client_encoding"]; v != "LATIN1" {
		t.Errorf("client_encoding = %q, want %q", v, "LATIN1")
	}

	// Delete the parameter (empty value)
	state.UpdateForServerMessage((*ServerAsyncParameterStatus)(ServerParsed(&pgproto3.ParameterStatus{
		Name:  "client_encoding",
		Value: "",
	})))

	if _, ok := state.ParameterStatuses["client_encoding"]; ok {
		t.Error("client_encoding should be deleted after empty value")
	}
}

// TestProtocolState_InTxOrQuery tests the InTxOrQuery method.
func TestProtocolState_InTxOrQuery(t *testing.T) {
	tests := []struct {
		name  string
		setup func(*ProtocolState)
		want  bool
	}{
		{
			name:  "IdleState",
			setup: func(s *ProtocolState) {},
			want:  false,
		},
		{
			name: "InTransaction",
			setup: func(s *ProtocolState) {
				s.TxStatus = TxInTransaction
			},
			want: true,
		},
		{
			name: "FailedTransaction",
			setup: func(s *ProtocolState) {
				s.TxStatus = TxFailed
			},
			want: true,
		},
		{
			name: "ExecutingStatement",
			setup: func(s *ProtocolState) {
				name := ""
				s.Statements.Executing = &name
			},
			want: true,
		},
		{
			name: "PendingExecuteStatement",
			setup: func(s *ProtocolState) {
				name := "stmt1"
				s.Statements.PendingExecute = &name
			},
			want: true,
		},
		{
			name: "ExecutingPortal",
			setup: func(s *ProtocolState) {
				name := ""
				s.Portals.Executing = &name
			},
			want: true,
		},
		{
			name: "PendingExecutePortal",
			setup: func(s *ProtocolState) {
				name := "portal1"
				s.Portals.PendingExecute = &name
			},
			want: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			state := NewProtocolState()
			tt.setup(&state)
			if got := state.InTxOrQuery(); got != tt.want {
				t.Errorf("InTxOrQuery() = %v, want %v", got, tt.want)
			}
		})
	}
}

// TestProtocolState_MultipleSyncsInFlight tests tracking of multiple syncs.
func TestProtocolState_MultipleSyncsInFlight(t *testing.T) {
	state := NewProtocolState()

	// Send multiple Parse+Sync sequences (pipelining)
	state.UpdateForFrontentMessage(&pgproto3.Parse{Name: "stmt1", Query: "SELECT 1"})
	state.UpdateForFrontentMessage(&pgproto3.Sync{})
	state.UpdateForFrontentMessage(&pgproto3.Parse{Name: "stmt2", Query: "SELECT 2"})
	state.UpdateForFrontentMessage(&pgproto3.Sync{})

	if state.SyncsInFlight != 2 {
		t.Errorf("SyncsInFlight = %v, want 2", state.SyncsInFlight)
	}

	// Server responds to first
	state.UpdateForServerMessage((*ServerExtendedQueryParseComplete)(ServerParsed(&pgproto3.ParseComplete{})))
	state.UpdateForServerMessage((*ServerResponseReadyForQuery)(ServerParsed(&pgproto3.ReadyForQuery{TxStatus: 'I'})))

	if state.SyncsInFlight != 1 {
		t.Errorf("SyncsInFlight = %v, want 1 after first ReadyForQuery", state.SyncsInFlight)
	}

	// Server responds to second
	state.UpdateForServerMessage((*ServerExtendedQueryParseComplete)(ServerParsed(&pgproto3.ParseComplete{})))
	state.UpdateForServerMessage((*ServerResponseReadyForQuery)(ServerParsed(&pgproto3.ReadyForQuery{TxStatus: 'I'})))

	if state.SyncsInFlight != 0 {
		t.Errorf("SyncsInFlight = %v, want 0 after second ReadyForQuery", state.SyncsInFlight)
	}
}

// TestProtocolState_UnnamedStatementOverwrite tests unnamed statement behavior.
func TestProtocolState_UnnamedStatementOverwrite(t *testing.T) {
	state := NewProtocolState()

	// Parse unnamed statement
	state.UpdateForFrontentMessage(&pgproto3.Parse{Name: "", Query: "SELECT 1"})
	state.UpdateForFrontentMessage(&pgproto3.Sync{})
	state.UpdateForServerMessage((*ServerExtendedQueryParseComplete)(ServerParsed(&pgproto3.ParseComplete{})))
	state.UpdateForServerMessage((*ServerResponseReadyForQuery)(ServerParsed(&pgproto3.ReadyForQuery{TxStatus: 'I'})))

	if _, ok := state.Statements.Alive[""]; !ok {
		t.Error("Unnamed statement should be Alive")
	}

	// Send Query which destroys unnamed statement
	state.UpdateForFrontentMessage(&pgproto3.Query{String: "SELECT 2"})

	if _, ok := state.Statements.Alive[""]; ok {
		t.Error("Unnamed statement should be destroyed after Query")
	}
}
