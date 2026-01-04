package pgwire

import (
	"testing"

	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Helper to create server messages for testing
func parseComplete() ServerMessage {
	return (*ServerParseComplete)(ServerParsed(&pgproto3.ParseComplete{}))
}

func bindComplete() ServerMessage {
	return (*ServerBindComplete)(ServerParsed(&pgproto3.BindComplete{}))
}

func closeComplete() ServerMessage {
	return (*ServerCloseComplete)(ServerParsed(&pgproto3.CloseComplete{}))
}

func noData() ServerMessage {
	return (*ServerNoData)(ServerParsed(&pgproto3.NoData{}))
}

func parameterDescription() ServerMessage {
	return (*ServerParameterDescription)(ServerParsed(&pgproto3.ParameterDescription{}))
}

func rowDescription() ServerMessage {
	return (*ServerRowDescription)(ServerParsed(&pgproto3.RowDescription{}))
}

func readyForQuery() ServerMessage {
	return (*ServerReadyForQuery)(ServerParsed(&pgproto3.ReadyForQuery{TxStatus: 'I'}))
}

func commandComplete() ServerMessage {
	return (*ServerCommandComplete)(ServerParsed(&pgproto3.CommandComplete{CommandTag: []byte("SELECT 1")}))
}

func emptyQueryResponse() ServerMessage {
	return (*ServerEmptyQueryResponse)(ServerParsed(&pgproto3.EmptyQueryResponse{}))
}

func portalSuspended() ServerMessage {
	return (*ServerPortalSuspended)(ServerParsed(&pgproto3.PortalSuspended{}))
}

func errorResponse() ServerMessage {
	return (*ServerErrorResponse)(ServerParsed(&pgproto3.ErrorResponse{Severity: "ERROR", Code: "42601"}))
}

func dataRow() ServerMessage {
	return (*ServerDataRow)(ServerParsed(&pgproto3.DataRow{}))
}

// Test request flow through ProtocolState.Update - the public API
func TestProtocolState_PushAndUpdate(t *testing.T) {
	state := NewProtocolState()

	// Push a Parse request
	state.PushRequest(PendingRequest{RequestType: MsgClientParse, Action: ActionForward})
	assert.Equal(t, 1, state.OutstandingRequestCount())

	// Update with matching response type (ParseComplete)
	req := state.Update(parseComplete())
	require.NotNil(t, req)
	assert.Equal(t, MsgClientParse, req.RequestType)
	assert.Equal(t, ActionForward, req.Action)
	assert.Equal(t, 0, state.OutstandingRequestCount())
}

func TestProtocolState_UpdateMismatch(t *testing.T) {
	state := NewProtocolState()
	state.PushRequest(PendingRequest{RequestType: MsgClientParse, Action: ActionForward})

	// Update with wrong response type (BindComplete) - should not pop
	req := state.Update(bindComplete())
	assert.Nil(t, req)
	assert.Equal(t, 1, state.OutstandingRequestCount()) // Request still in queue
}

func TestProtocolState_UpdateEmpty(t *testing.T) {
	state := NewProtocolState()

	req := state.Update(parseComplete())
	assert.Nil(t, req)
}

func TestRequestFlow_Peek(t *testing.T) {
	flow := NewRequestFlow()

	// Peek on empty
	_, ok := flow.Peek()
	assert.False(t, ok)

	// Push and peek
	flow.Push(PendingRequest{RequestType: MsgClientParse, Action: ActionSkip})
	req, ok := flow.Peek()
	assert.True(t, ok)
	assert.Equal(t, MsgClientParse, req.RequestType)
	assert.Equal(t, ActionSkip, req.Action)
	assert.Equal(t, 1, flow.Len()) // Still in queue
}

func TestRequestFlow_ClearUntilSync(t *testing.T) {
	flow := NewRequestFlow()
	flow.Push(PendingRequest{RequestType: MsgClientParse})    // Parse
	flow.Push(PendingRequest{RequestType: MsgClientBind})     // Bind
	flow.Push(PendingRequest{RequestType: MsgClientExecute})  // Execute
	flow.Push(PendingRequest{RequestType: MsgClientSync})     // Sync
	flow.Push(PendingRequest{RequestType: MsgClientParse})    // Parse (next batch)
	flow.Push(PendingRequest{RequestType: MsgClientDescribe}) // Describe (next batch)

	assert.Equal(t, 6, flow.Len())

	flow.ClearUntilSync()

	// Should have 2 remaining (Parse and Describe after the Sync)
	assert.Equal(t, 2, flow.Len())

	// Verify remaining requests
	req, ok := flow.Peek()
	assert.True(t, ok)
	assert.Equal(t, MsgClientParse, req.RequestType)
}

func TestRequestFlow_ClearUntilSync_NoSync(t *testing.T) {
	flow := NewRequestFlow()
	flow.Push(PendingRequest{RequestType: MsgClientParse})
	flow.Push(PendingRequest{RequestType: MsgClientBind})

	flow.ClearUntilSync()

	// No Sync found - should clear everything
	assert.Equal(t, 0, flow.Len())
}

func TestRequestFlow_ClearUntilSync_MultipleSync(t *testing.T) {
	flow := NewRequestFlow()
	flow.Push(PendingRequest{RequestType: MsgClientParse}) // Parse
	flow.Push(PendingRequest{RequestType: MsgClientSync})  // Sync 1
	flow.Push(PendingRequest{RequestType: MsgClientParse}) // Parse
	flow.Push(PendingRequest{RequestType: MsgClientSync})  // Sync 2

	flow.ClearUntilSync()

	// Should clear up to first Sync, leaving 2 (Parse + Sync 2)
	assert.Equal(t, 2, flow.Len())
}

func TestResponseMatchesRequest(t *testing.T) {
	cases := []struct {
		name     string
		response ServerMessage
		request  MsgType
		matches  bool
	}{
		{"ParseComplete <- Parse", parseComplete(), MsgClientParse, true},
		{"BindComplete <- Bind", bindComplete(), MsgClientBind, true},
		{"CloseComplete <- Close", closeComplete(), MsgClientClose, true},
		{"NoData <- Describe", noData(), MsgClientDescribe, true},
		{"ParameterDescription <- Describe", parameterDescription(), MsgClientDescribe, true},
		{"RowDescription <- Describe", rowDescription(), MsgClientDescribe, true},
		{"ReadyForQuery <- Sync", readyForQuery(), MsgClientSync, true},
		{"ReadyForQuery <- Query", readyForQuery(), MsgClientQuery, true},
		{"ErrorResponse <- Parse", errorResponse(), MsgClientParse, true},
		{"ErrorResponse <- Bind", errorResponse(), MsgClientBind, true},
		{"ErrorResponse <- Execute", errorResponse(), MsgClientExecute, true},
		{"EmptyQueryResponse <- Query", emptyQueryResponse(), MsgClientQuery, true},
		{"EmptyQueryResponse <- Execute", emptyQueryResponse(), MsgClientExecute, true},
		{"CommandComplete <- Query", commandComplete(), MsgClientQuery, true},
		{"CommandComplete <- Execute", commandComplete(), MsgClientExecute, true},
		{"PortalSuspended <- Execute", portalSuspended(), MsgClientExecute, true},
		{"ParseComplete !<- Bind", parseComplete(), MsgClientBind, false},
		{"BindComplete !<- Parse", bindComplete(), MsgClientParse, false},
		{"ReadyForQuery !<- Parse", readyForQuery(), MsgClientParse, false},
		{"DataRow !<- any", dataRow(), MsgClientParse, false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := responseMatchesRequest(tc.response, tc.request)
			assert.Equal(t, tc.matches, got)
		})
	}
}

func TestProtocolState_ExtendedQuerySequence(t *testing.T) {
	state := NewProtocolState()

	// Simulate Parse+Bind+Execute+Sync sequence
	state.PushRequest(PendingRequest{RequestType: MsgClientParse, Action: ActionForward, StatementName: "stmt1"})
	state.PushRequest(PendingRequest{RequestType: MsgClientBind, Action: ActionForward})
	state.PushRequest(PendingRequest{RequestType: MsgClientExecute, Action: ActionForward})
	state.PushRequest(PendingRequest{RequestType: MsgClientSync, Action: ActionForward})

	assert.Equal(t, 4, state.OutstandingRequestCount())

	// Simulate server responses

	// ParseComplete
	req := state.Update(parseComplete())
	require.NotNil(t, req)
	assert.Equal(t, MsgClientParse, req.RequestType)
	assert.Equal(t, "stmt1", req.StatementName)

	// BindComplete
	req = state.Update(bindComplete())
	require.NotNil(t, req)
	assert.Equal(t, MsgClientBind, req.RequestType)

	// DataRow (doesn't consume a request)
	req = state.Update(dataRow())
	assert.Nil(t, req)

	// CommandComplete (consumes Execute)
	req = state.Update(commandComplete())
	require.NotNil(t, req)
	assert.Equal(t, MsgClientExecute, req.RequestType)

	// ReadyForQuery (consumes Sync and ends flow)
	req = state.Update(readyForQuery())
	require.NotNil(t, req)
	assert.Equal(t, MsgClientSync, req.RequestType)

	assert.Equal(t, 0, state.OutstandingRequestCount())
	// Flow should be ended automatically
	assert.Nil(t, state.ActiveRequestFlow)
}

func TestProtocolState_WithActions(t *testing.T) {
	state := NewProtocolState()

	// Mix of actions
	state.PushRequest(PendingRequest{RequestType: MsgClientParse, Action: ActionForward})
	state.PushRequest(PendingRequest{RequestType: MsgClientParse, Action: ActionSkip})
	state.PushRequest(PendingRequest{RequestType: MsgClientParse, Action: ActionFake})

	req := state.Update(parseComplete())
	require.NotNil(t, req)
	assert.Equal(t, ActionForward, req.Action)

	req = state.Update(parseComplete())
	require.NotNil(t, req)
	assert.Equal(t, ActionSkip, req.Action)

	req = state.Update(parseComplete())
	require.NotNil(t, req)
	assert.Equal(t, ActionFake, req.Action)
}

func TestProtocolState_RequestFlow(t *testing.T) {
	state := NewProtocolState()

	// No flow initially
	assert.Nil(t, state.ActiveRequestFlow)
	assert.Equal(t, 0, state.OutstandingRequestCount())

	// Push creates flow lazily
	state.PushRequest(PendingRequest{RequestType: MsgClientParse})
	assert.NotNil(t, state.ActiveRequestFlow)
	assert.Equal(t, 1, state.OutstandingRequestCount())

	// Push more
	state.PushRequest(PendingRequest{RequestType: MsgClientBind})
	assert.Equal(t, 2, state.OutstandingRequestCount())

	// Update with ParseComplete pops the Parse request
	poppedReq := state.Update(parseComplete())
	require.NotNil(t, poppedReq)
	assert.Equal(t, MsgClientParse, poppedReq.RequestType)
	assert.Equal(t, 1, state.OutstandingRequestCount())

	// End flow
	state.EndRequestFlow()
	assert.Nil(t, state.ActiveRequestFlow)
	assert.Equal(t, 0, state.OutstandingRequestCount())
}

func TestProtocolState_StartRequestFlow_Panic(t *testing.T) {
	state := NewProtocolState()
	state.StartRequestFlow()

	// Starting another flow should panic
	assert.Panics(t, func() {
		state.StartRequestFlow()
	})
}

func TestProtocolState_EndRequestFlow_NoOp(t *testing.T) {
	state := NewProtocolState()

	// Should not panic when no flow active
	assert.NotPanics(t, func() {
		state.EndRequestFlow()
	})
}

func TestProtocolState_Update_NoFlow(t *testing.T) {
	state := NewProtocolState()

	// Update with no flow should return nil
	poppedReq := state.Update(parseComplete())
	assert.Nil(t, poppedReq)
}

func TestResponseAction_String(t *testing.T) {
	assert.Equal(t, "forward", ActionForward.String())
	assert.Equal(t, "skip", ActionSkip.String())
	assert.Equal(t, "fake", ActionFake.String())
	assert.Equal(t, "unknown", ResponseAction(99).String())
}

func TestRequestFlow_OnComplete(t *testing.T) {
	called := false
	flow := NewRequestFlow()
	flow.OnComplete = func(f *RequestFlow) {
		called = true
	}

	flow.Close()
	assert.True(t, called)
}

func TestRequestFlow_OnComplete_Nil(t *testing.T) {
	flow := NewRequestFlow()
	// Should not panic
	assert.NotPanics(t, func() {
		flow.Close()
	})
}
