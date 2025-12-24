package pgwire

import (
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgproto3"
)

// TestSimpleQueryFlow_BasicFlow tests a basic simple query flow.
func TestSimpleQueryFlow_BasicFlow(t *testing.T) {
	var closeCalled bool
	var closedFlow *SimpleQueryFlow

	flow := NewSimpleQueryFlow(
		ClientSimpleQueryQuery{NewLazyClientFromParsed(&pgproto3.Query{String: "SELECT 1"})},
		func(f *SimpleQueryFlow) {
			closeCalled = true
			closedFlow = f
		},
	)

	if flow.SQL != "SELECT 1" {
		t.Errorf("SQL = %q, want %q", flow.SQL, "SELECT 1")
	}
	if flow.StartTime.IsZero() {
		t.Error("StartTime should be set")
	}

	state := NewProtocolState()

	// Simulate server responses
	handlers := flow.UpdateHandlers(&state)

	// DataRow - should continue
	if !handlers.Update(ServerResponseDataRow{NewLazyServerFromParsed(&pgproto3.DataRow{Values: [][]byte{[]byte("1")}})}) {
		t.Error("DataRow should continue flow")
	}
	if flow.RowCount != 1 {
		t.Errorf("RowCount = %d, want 1", flow.RowCount)
	}

	// More data rows
	handlers.Update(ServerResponseDataRow{NewLazyServerFromParsed(&pgproto3.DataRow{Values: [][]byte{[]byte("2")}})})
	if flow.RowCount != 2 {
		t.Errorf("RowCount = %d, want 2", flow.RowCount)
	}

	// CommandComplete
	if !handlers.Update(ServerResponseCommandComplete{NewLazyServerFromParsed(&pgproto3.CommandComplete{CommandTag: []byte("SELECT 2")})}) {
		t.Error("CommandComplete should continue flow")
	}
	if flow.CommandTag.String() != "SELECT 2" {
		t.Errorf("CommandTag = %q, want %q", flow.CommandTag.String(), "SELECT 2")
	}

	// ReadyForQuery - should complete flow
	if handlers.Update(ServerResponseReadyForQuery{NewLazyServerFromParsed(&pgproto3.ReadyForQuery{TxStatus: 'I'})}) {
		t.Error("ReadyForQuery should complete flow (return false)")
	}
	if flow.EndTime.IsZero() {
		t.Error("EndTime should be set after ReadyForQuery")
	}

	// Close the flow
	flow.Close()

	if !closeCalled {
		t.Error("OnClose callback should have been called")
	}
	if closedFlow != flow {
		t.Error("OnClose callback should receive the flow")
	}
}

// TestSimpleQueryFlow_WithError tests a simple query that returns an error.
func TestSimpleQueryFlow_WithError(t *testing.T) {
	flow := NewSimpleQueryFlow(
		ClientSimpleQueryQuery{NewLazyClientFromParsed(&pgproto3.Query{String: "INVALID SQL"})},
		nil,
	)

	state := NewProtocolState()
	handlers := flow.UpdateHandlers(&state)

	// ErrorResponse
	if !handlers.Update(ServerResponseErrorResponse{NewLazyServerFromParsed(&pgproto3.ErrorResponse{
		Severity: "ERROR",
		Code:     "42601",
		Message:  "syntax error",
	})}) {
		t.Error("ErrorResponse should continue flow (waiting for ReadyForQuery)")
	}

	if flow.Err == nil {
		t.Error("Err should be set after ErrorResponse")
	}
	if flow.Err.Code != "42601" {
		t.Errorf("Err.Code = %q, want %q", flow.Err.Code, "42601")
	}

	// ReadyForQuery completes flow
	if handlers.Update(ServerResponseReadyForQuery{NewLazyServerFromParsed(&pgproto3.ReadyForQuery{TxStatus: 'I'})}) {
		t.Error("ReadyForQuery should complete flow")
	}
}

// TestSimpleQueryFlow_NoCallback tests that flow works without a callback.
func TestSimpleQueryFlow_NoCallback(t *testing.T) {
	flow := NewSimpleQueryFlow(
		ClientSimpleQueryQuery{NewLazyClientFromParsed(&pgproto3.Query{String: "SELECT 1"})},
		nil, // No callback
	)

	// Should not panic when closing without callback
	flow.Close()
}

// TestExtendedQueryFlow_ParseDescribeSync tests a prepare flow.
func TestExtendedQueryFlow_ParseDescribeSync(t *testing.T) {
	var closedFlow *ExtendedQueryFlow

	flow := NewExtendedQueryFlow(
		ClientExtendedQueryParse{NewLazyClientFromParsed(&pgproto3.Parse{Name: "stmt1", Query: "SELECT $1"})},
		func(f *ExtendedQueryFlow) {
			closedFlow = f
		},
	)

	if flow.SQL != "SELECT $1" {
		t.Errorf("SQL = %q, want %q", flow.SQL, "SELECT $1")
	}
	if flow.StatementName != "stmt1" {
		t.Errorf("StatementName = %q, want %q", flow.StatementName, "stmt1")
	}
	if flow.Type != FlowTypePrepare {
		t.Errorf("Type = %v, want %v", flow.Type, FlowTypePrepare)
	}

	state := NewProtocolState()
	handlers := flow.UpdateHandlers(&state)

	// ParseComplete
	handlers.Update(ServerExtendedQueryParseComplete{NewLazyServerFromParsed(&pgproto3.ParseComplete{})})

	// ParameterDescription
	handlers.Update(ServerExtendedQueryParameterDescription{NewLazyServerFromParsed(&pgproto3.ParameterDescription{})})

	// RowDescription
	handlers.Update(ServerExtendedQueryRowDescription{NewLazyServerFromParsed(&pgproto3.RowDescription{})})

	// ReadyForQuery completes flow
	if handlers.Update(ServerResponseReadyForQuery{NewLazyServerFromParsed(&pgproto3.ReadyForQuery{TxStatus: 'I'})}) {
		t.Error("ReadyForQuery should complete flow")
	}

	flow.Close()

	if closedFlow == nil {
		t.Error("OnClose callback should have been called")
	}
	if closedFlow.Type != FlowTypePrepare {
		t.Errorf("Type = %v, want %v", closedFlow.Type, FlowTypePrepare)
	}
}

// TestExtendedQueryFlow_ParseBindExecuteSync tests a full execute flow.
func TestExtendedQueryFlow_ParseBindExecuteSync(t *testing.T) {
	flow := NewExtendedQueryFlow(
		ClientExtendedQueryParse{NewLazyClientFromParsed(&pgproto3.Parse{Name: "", Query: "SELECT $1"})},
		nil,
	)

	state := NewProtocolState()
	handlers := flow.UpdateHandlers(&state)

	// Client: Bind
	handlers.Update(ClientExtendedQueryBind{NewLazyClientFromParsed(&pgproto3.Bind{
		DestinationPortal:    "p1",
		PreparedStatement:    "",
		ParameterFormatCodes: nil,
		Parameters:           [][]byte{[]byte("42")},
	})})

	if !flow.seenBind {
		t.Error("seenBind should be true after Bind")
	}
	if flow.PortalName != "p1" {
		t.Errorf("PortalName = %q, want %q", flow.PortalName, "p1")
	}

	// Client: Execute
	handlers.Update(ClientExtendedQueryExecute{NewLazyClientFromParsed(&pgproto3.Execute{Portal: "p1"})})

	if !flow.seenExecute {
		t.Error("seenExecute should be true after Execute")
	}
	if flow.Type != FlowTypeExecute {
		t.Errorf("Type = %v, want %v", flow.Type, FlowTypeExecute)
	}

	// Server: ParseComplete, BindComplete, DataRow, CommandComplete, ReadyForQuery
	handlers.Update(ServerExtendedQueryParseComplete{NewLazyServerFromParsed(&pgproto3.ParseComplete{})})
	handlers.Update(ServerExtendedQueryBindComplete{NewLazyServerFromParsed(&pgproto3.BindComplete{})})
	handlers.Update(ServerResponseDataRow{NewLazyServerFromParsed(&pgproto3.DataRow{Values: [][]byte{[]byte("42")}})})
	if flow.RowCount != 1 {
		t.Errorf("RowCount = %d, want 1", flow.RowCount)
	}
	handlers.Update(ServerResponseCommandComplete{NewLazyServerFromParsed(&pgproto3.CommandComplete{CommandTag: []byte("SELECT 1")})})

	// ReadyForQuery
	if handlers.Update(ServerResponseReadyForQuery{NewLazyServerFromParsed(&pgproto3.ReadyForQuery{TxStatus: 'I'})}) {
		t.Error("ReadyForQuery should complete flow")
	}

	if flow.CommandTag.String() != "SELECT 1" {
		t.Errorf("CommandTag = %q, want %q", flow.CommandTag.String(), "SELECT 1")
	}
}

// TestExtendedQueryFlow_WithError tests an extended query that returns an error.
func TestExtendedQueryFlow_WithError(t *testing.T) {
	flow := NewExtendedQueryFlow(
		ClientExtendedQueryParse{NewLazyClientFromParsed(&pgproto3.Parse{Name: "", Query: "INVALID"})},
		nil,
	)

	state := NewProtocolState()
	handlers := flow.UpdateHandlers(&state)

	// Server: ErrorResponse
	handlers.Update(ServerResponseErrorResponse{NewLazyServerFromParsed(&pgproto3.ErrorResponse{
		Severity: "ERROR",
		Code:     "42601",
		Message:  "syntax error",
	})})

	if flow.Err == nil {
		t.Error("Err should be set after ErrorResponse")
	}

	// ReadyForQuery
	if handlers.Update(ServerResponseReadyForQuery{NewLazyServerFromParsed(&pgproto3.ReadyForQuery{TxStatus: 'I'})}) {
		t.Error("ReadyForQuery should complete flow")
	}
}

// TestCopyFlow_CopyIn tests a COPY FROM flow (client to server).
func TestCopyFlow_CopyIn(t *testing.T) {
	var closedFlow *CopyFlow

	flow := NewCopyInFlow("COPY test FROM STDIN", func(f *CopyFlow) {
		closedFlow = f
	})

	if flow.Type != FlowTypeCopyIn {
		t.Errorf("Type = %v, want %v", flow.Type, FlowTypeCopyIn)
	}
	if flow.SQL != "COPY test FROM STDIN" {
		t.Errorf("SQL = %q, want %q", flow.SQL, "COPY test FROM STDIN")
	}

	state := NewProtocolState()
	handlers := flow.UpdateHandlers(&state)

	// Client sends CopyData
	handlers.Update(ClientCopyCopyData{NewLazyClientFromParsed(&pgproto3.CopyData{Data: []byte("line1\n")})})
	if flow.ByteCount != 6 {
		t.Errorf("ByteCount = %d, want 6", flow.ByteCount)
	}

	handlers.Update(ClientCopyCopyData{NewLazyClientFromParsed(&pgproto3.CopyData{Data: []byte("line2\n")})})
	if flow.ByteCount != 12 {
		t.Errorf("ByteCount = %d, want 12", flow.ByteCount)
	}

	// Server: CommandComplete
	handlers.Update(ServerResponseCommandComplete{NewLazyServerFromParsed(&pgproto3.CommandComplete{CommandTag: []byte("COPY 2")})})
	if flow.CommandTag.String() != "COPY 2" {
		t.Errorf("CommandTag = %q, want %q", flow.CommandTag.String(), "COPY 2")
	}

	// Server: ReadyForQuery
	if handlers.Update(ServerResponseReadyForQuery{NewLazyServerFromParsed(&pgproto3.ReadyForQuery{TxStatus: 'I'})}) {
		t.Error("ReadyForQuery should complete flow")
	}

	flow.Close()

	if closedFlow == nil {
		t.Error("OnClose callback should have been called")
	}
}

// TestCopyFlow_CopyOut tests a COPY TO flow (server to client).
func TestCopyFlow_CopyOut(t *testing.T) {
	flow := NewCopyOutFlow("COPY test TO STDOUT", nil)

	if flow.Type != FlowTypeCopyOut {
		t.Errorf("Type = %v, want %v", flow.Type, FlowTypeCopyOut)
	}

	state := NewProtocolState()
	handlers := flow.UpdateHandlers(&state)

	// Server sends CopyData
	handlers.Update(ServerCopyCopyData{NewLazyServerFromParsed(&pgproto3.CopyData{Data: []byte("row1\n")})})
	if flow.ByteCount != 5 {
		t.Errorf("ByteCount = %d, want 5", flow.ByteCount)
	}

	handlers.Update(ServerCopyCopyData{NewLazyServerFromParsed(&pgproto3.CopyData{Data: []byte("row2\n")})})
	if flow.ByteCount != 10 {
		t.Errorf("ByteCount = %d, want 10", flow.ByteCount)
	}

	// Server: CommandComplete
	handlers.Update(ServerResponseCommandComplete{NewLazyServerFromParsed(&pgproto3.CommandComplete{CommandTag: []byte("COPY 2")})})

	// Server: ReadyForQuery
	if handlers.Update(ServerResponseReadyForQuery{NewLazyServerFromParsed(&pgproto3.ReadyForQuery{TxStatus: 'I'})}) {
		t.Error("ReadyForQuery should complete flow")
	}
}

// TestCopyFlow_WithError tests a COPY that fails.
func TestCopyFlow_WithError(t *testing.T) {
	flow := NewCopyInFlow("COPY nonexistent FROM STDIN", nil)

	state := NewProtocolState()
	handlers := flow.UpdateHandlers(&state)

	// Server: ErrorResponse
	handlers.Update(ServerResponseErrorResponse{NewLazyServerFromParsed(&pgproto3.ErrorResponse{
		Severity: "ERROR",
		Code:     "42P01",
		Message:  "relation does not exist",
	})})

	if flow.Err == nil {
		t.Error("Err should be set after ErrorResponse")
	}

	// Server: ReadyForQuery
	if handlers.Update(ServerResponseReadyForQuery{NewLazyServerFromParsed(&pgproto3.ReadyForQuery{TxStatus: 'I'})}) {
		t.Error("ReadyForQuery should complete flow")
	}
}

// TestSimpleQueryRecognizer tests the simple query recognizer.
func TestSimpleQueryRecognizer(t *testing.T) {
	var startedWith string
	recognizer := &SimpleQueryRecognizer{
		OnStart: func(msg ClientSimpleQueryQuery) func(*SimpleQueryFlow) {
			startedWith = msg.Parse().String
			return nil
		},
	}

	state := NewProtocolState()
	handlers := recognizer.StartHandlers(&state)

	// Query message should start a flow
	flow := handlers.Start(ClientSimpleQueryQuery{NewLazyClientFromParsed(&pgproto3.Query{String: "SELECT 1"})})
	if flow == nil {
		t.Error("Query should start a flow")
	}
	if startedWith != "SELECT 1" {
		t.Errorf("startedWith = %q, want %q", startedWith, "SELECT 1")
	}

	// Non-Query messages should not start a flow
	flow = handlers.Start(ClientExtendedQueryParse{NewLazyClientFromParsed(&pgproto3.Parse{Query: "SELECT $1"})})
	if flow != nil {
		t.Error("Parse should not start a simple query flow")
	}
}

// TestExtendedQueryRecognizer tests the extended query recognizer.
func TestExtendedQueryRecognizer(t *testing.T) {
	var startedSQL string
	recognizer := &ExtendedQueryRecognizer{
		OnStart: func(msg ClientExtendedQueryParse) func(*ExtendedQueryFlow) {
			startedSQL = msg.Parse().Query
			return nil
		},
	}

	state := NewProtocolState()
	handlers := recognizer.StartHandlers(&state)

	// Parse message should start a flow
	flow := handlers.Start(ClientExtendedQueryParse{NewLazyClientFromParsed(&pgproto3.Parse{Name: "stmt1", Query: "SELECT $1"})})
	if flow == nil {
		t.Error("Parse should start a flow")
	}
	if startedSQL != "SELECT $1" {
		t.Errorf("startedSQL = %q, want %q", startedSQL, "SELECT $1")
	}

	// Other extended query messages should not start a new flow
	flow = handlers.Start(ClientExtendedQueryBind{NewLazyClientFromParsed(&pgproto3.Bind{})})
	if flow != nil {
		t.Error("Bind should not start a new extended query flow")
	}
}

// TestCopyRecognizer tests the copy recognizer.
func TestCopyRecognizer(t *testing.T) {
	lastSQL := "COPY test FROM STDIN"
	var startedType FlowType
	var startedSQL string

	recognizer := &CopyRecognizer{
		GetLastSQL: func() string {
			return lastSQL
		},
		OnStart: func(flowType FlowType, sql string) func(*CopyFlow) {
			startedType = flowType
			startedSQL = sql
			return nil
		},
	}

	state := NewProtocolState()
	handlers := recognizer.StartHandlers(&state)

	// CopyInResponse should start a CopyIn flow
	flow := handlers.Start(ServerCopyCopyInResponse{NewLazyServerFromParsed(&pgproto3.CopyInResponse{})})
	if flow == nil {
		t.Error("CopyInResponse should start a flow")
	}
	if startedType != FlowTypeCopyIn {
		t.Errorf("startedType = %v, want %v", startedType, FlowTypeCopyIn)
	}
	if startedSQL != lastSQL {
		t.Errorf("startedSQL = %q, want %q", startedSQL, lastSQL)
	}

	// CopyOutResponse should start a CopyOut flow
	lastSQL = "COPY test TO STDOUT"
	flow = handlers.Start(ServerCopyCopyOutResponse{NewLazyServerFromParsed(&pgproto3.CopyOutResponse{})})
	if flow == nil {
		t.Error("CopyOutResponse should start a flow")
	}
	if startedType != FlowTypeCopyOut {
		t.Errorf("startedType = %v, want %v", startedType, FlowTypeCopyOut)
	}

	// CopyData should not start a new flow
	flow = handlers.Start(ServerCopyCopyData{NewLazyServerFromParsed(&pgproto3.CopyData{})})
	if flow != nil {
		t.Error("CopyData should not start a new flow")
	}
}

// TestProtocolState_FlowTracking tests the integrated flow tracking.
func TestProtocolState_FlowTracking(t *testing.T) {
	state := NewProtocolState()

	var flowsClosed int
	state.AddRecognizer(&SimpleQueryRecognizer{
		OnStart: func(msg ClientSimpleQueryQuery) func(*SimpleQueryFlow) {
			return func(f *SimpleQueryFlow) {
				flowsClosed++
			}
		},
	})

	// No active flows initially
	if state.ActiveFlowCount() != 0 {
		t.Errorf("ActiveFlowCount = %d, want 0", state.ActiveFlowCount())
	}

	// Process a Query message
	queryMsg := ClientSimpleQueryQuery{NewLazyClientFromParsed(&pgproto3.Query{String: "SELECT 1"})}
	state.ProcessFlows(queryMsg)

	if state.ActiveFlowCount() != 1 {
		t.Errorf("ActiveFlowCount = %d, want 1", state.ActiveFlowCount())
	}

	// Process server responses
	state.ProcessFlows(ServerResponseDataRow{NewLazyServerFromParsed(&pgproto3.DataRow{})})
	state.ProcessFlows(ServerResponseCommandComplete{NewLazyServerFromParsed(&pgproto3.CommandComplete{CommandTag: []byte("SELECT 1")})})

	if state.ActiveFlowCount() != 1 {
		t.Error("Flow should still be active before ReadyForQuery")
	}

	// ReadyForQuery completes the flow
	state.ProcessFlows(ServerResponseReadyForQuery{NewLazyServerFromParsed(&pgproto3.ReadyForQuery{TxStatus: 'I'})})

	if state.ActiveFlowCount() != 0 {
		t.Errorf("ActiveFlowCount = %d, want 0 after ReadyForQuery", state.ActiveFlowCount())
	}
	if flowsClosed != 1 {
		t.Errorf("flowsClosed = %d, want 1", flowsClosed)
	}
}

// TestProtocolState_MultipleFlows tests tracking multiple concurrent flows.
func TestProtocolState_MultipleFlows(t *testing.T) {
	state := NewProtocolState()

	var simpleQueryClosed, extendedQueryClosed int

	state.AddRecognizer(&SimpleQueryRecognizer{
		OnStart: func(msg ClientSimpleQueryQuery) func(*SimpleQueryFlow) {
			return func(f *SimpleQueryFlow) {
				simpleQueryClosed++
			}
		},
	})

	state.AddRecognizer(&ExtendedQueryRecognizer{
		OnStart: func(msg ClientExtendedQueryParse) func(*ExtendedQueryFlow) {
			return func(f *ExtendedQueryFlow) {
				extendedQueryClosed++
			}
		},
	})

	// Start a simple query flow
	state.ProcessFlows(ClientSimpleQueryQuery{NewLazyClientFromParsed(&pgproto3.Query{String: "SELECT 1"})})

	if state.ActiveFlowCount() != 1 {
		t.Errorf("ActiveFlowCount = %d, want 1", state.ActiveFlowCount())
	}

	// Complete the simple query
	state.ProcessFlows(ServerResponseCommandComplete{NewLazyServerFromParsed(&pgproto3.CommandComplete{CommandTag: []byte("SELECT 1")})})
	state.ProcessFlows(ServerResponseReadyForQuery{NewLazyServerFromParsed(&pgproto3.ReadyForQuery{TxStatus: 'I'})})

	if simpleQueryClosed != 1 {
		t.Errorf("simpleQueryClosed = %d, want 1", simpleQueryClosed)
	}

	// Start an extended query flow
	state.ProcessFlows(ClientExtendedQueryParse{NewLazyClientFromParsed(&pgproto3.Parse{Name: "stmt1", Query: "SELECT $1"})})

	if state.ActiveFlowCount() != 1 {
		t.Errorf("ActiveFlowCount = %d, want 1", state.ActiveFlowCount())
	}

	// Complete the extended query
	state.ProcessFlows(ServerExtendedQueryParseComplete{NewLazyServerFromParsed(&pgproto3.ParseComplete{})})
	state.ProcessFlows(ServerResponseReadyForQuery{NewLazyServerFromParsed(&pgproto3.ReadyForQuery{TxStatus: 'I'})})

	if extendedQueryClosed != 1 {
		t.Errorf("extendedQueryClosed = %d, want 1", extendedQueryClosed)
	}

	if state.ActiveFlowCount() != 0 {
		t.Errorf("ActiveFlowCount = %d, want 0", state.ActiveFlowCount())
	}
}

// TestProtocolState_CloseAllFlows tests that CloseAllFlows closes all active flows.
func TestProtocolState_CloseAllFlows(t *testing.T) {
	state := NewProtocolState()

	var flowsClosed int
	state.AddRecognizer(&SimpleQueryRecognizer{
		OnStart: func(msg ClientSimpleQueryQuery) func(*SimpleQueryFlow) {
			return func(f *SimpleQueryFlow) {
				flowsClosed++
			}
		},
	})

	// Start a flow
	state.ProcessFlows(ClientSimpleQueryQuery{NewLazyClientFromParsed(&pgproto3.Query{String: "SELECT 1"})})

	if state.ActiveFlowCount() != 1 {
		t.Fatalf("ActiveFlowCount = %d, want 1", state.ActiveFlowCount())
	}

	// Close all flows (e.g., session ending)
	state.CloseAllFlows()

	if state.ActiveFlowCount() != 0 {
		t.Errorf("ActiveFlowCount = %d, want 0 after CloseAllFlows", state.ActiveFlowCount())
	}
	if flowsClosed != 1 {
		t.Errorf("flowsClosed = %d, want 1", flowsClosed)
	}
}

// TestFlowType_String tests FlowType string representation.
func TestFlowType_String(t *testing.T) {
	tests := []struct {
		flowType FlowType
		want     string
	}{
		{FlowTypeSimpleQuery, "simple_query"},
		{FlowTypePrepare, "prepare"},
		{FlowTypeExecute, "execute"},
		{FlowTypeCopyIn, "copy_in"},
		{FlowTypeCopyOut, "copy_out"},
		{FlowType(999), "unknown"},
	}

	for _, tt := range tests {
		t.Run(tt.want, func(t *testing.T) {
			if got := tt.flowType.String(); got != tt.want {
				t.Errorf("String() = %q, want %q", got, tt.want)
			}
		})
	}
}

// TestSimpleQueryFlow_Duration tests duration calculation.
func TestSimpleQueryFlow_Duration(t *testing.T) {
	flow := &SimpleQueryFlow{
		StartTime: time.Now(),
	}

	// Sleep briefly to ensure some time passes
	time.Sleep(10 * time.Millisecond)
	flow.EndTime = time.Now()

	duration := flow.EndTime.Sub(flow.StartTime)
	if duration < 10*time.Millisecond {
		t.Errorf("Duration = %v, want >= 10ms", duration)
	}
}
