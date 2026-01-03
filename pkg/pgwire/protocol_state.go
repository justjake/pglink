package pgwire

import (
	"fmt"
	"time"
)

// The PostreSQL wire protocol identifies the "backend process" a client is connected to by its ProcessID.
// Cancellation requests are sent to a specific ProcessID authenticated by a [SecretKey].
// Docs: https://www.postgresql.org/docs/current/protocol-flow.html#PROTOCOL-FLOW-CANCELING-REQUESTS
//
// This is the type used by [pgproto3.BackendKeyData].
type ProcessID uint32

// Cancellation requests are sent to a specific ProcessID authenticated by a [SecretKey].
// https://www.postgresql.org/docs/current/protocol-flow.html#PROTOCOL-FLOW-CANCELING-REQUESTS
//
// This is the type used by [pgproto3.BackendKeyData].
// However, the PostgreSQL wire protocol allows for up to 256 bytes.
// TODO: support longer secret keys.
type SecretKey uint32

// NewProtocolState creates a new ProtocolState with all maps initialized.
func NewProtocolState() ProtocolState {
	return ProtocolState{
		TxStatus:          TxIdle,
		ParameterStatuses: ParameterStatuses{},
		Statements: NamedObjectState[bool]{
			Alive:         make(map[string]bool),
			PendingCreate: make(map[string]bool),
			PendingClose:  make(map[string]bool),
		},
		Portals: NamedObjectState[bool]{
			Alive:         make(map[string]bool),
			PendingCreate: make(map[string]bool),
			PendingClose:  make(map[string]bool),
		},
		recognizers: nil,
		activeFlows: nil,
	}
}

// State of a session in the PostgreSQL wire protocol.
type ProtocolState struct {
	// Immutable
	PID             uint32
	SecretCancelKey uint32

	// Dynamic
	TxStatus          TxStatus
	ParameterStatuses ParameterStatuses

	// Once the client sends an Extended Query message, the backend will enter
	// extended query mode.
	ExtendedQueryMode bool
	SyncsInFlight     int

	// When an error is detected while processing any extended-query message, the
	// backend issues ErrorResponse, then reads and discards messages until a Sync
	// is reached, then issues ReadyForQuery and returns to normal message
	// processing. (But note that no skipping occurs if an error is detected while
	// processing Sync — this ensures that there is one and only one ReadyForQuery
	// sent for each Sync.)
	//
	// In the event of a backend-detected error during copy-in mode (including
	// receipt of a CopyFail message), the backend will issue an ErrorResponse
	// message. If the COPY command was issued via an extended-query message, the
	// backend will now discard frontend messages until a Sync message is
	// received, then it will issue ReadyForQuery and return to normal processing.
	ServerIgnoringMessagesUntilSync bool
	// See CopyMode above.
	CopyMode CopyMode

	Statements NamedObjectState[bool]
	Portals    NamedObjectState[bool]

	// Flow tracking for observability
	recognizers []FlowRecognizer
	activeFlows []Flow

	// Request flow tracking for protocol correctness
	// ActiveRequestFlow tracks pending requests and their expected response actions.
	// It is nil when the session is idle (no outstanding requests).
	ActiveRequestFlow *RequestFlow

	// TxStartTime is when the current transaction started (or potentially started).
	// Set when a RequestFlow starts AND TxStartTime is zero. This captures "when did
	// the first query in this transaction start" - we set it before knowing if the
	// query will actually start a transaction.
	// Cleared when ReadyForQuery arrives with TxStatus 'I' (transaction ended or never started).
	// Kept through ReadyForQuery with 'T' or 'E' (we're now in a transaction).
	// Used for transaction_timeout calculation.
	TxStartTime time.Time

	// LastReadyForQueryTime is when the last ReadyForQuery was received.
	// Set on ALL ReadyForQuery messages (regardless of TxStatus).
	// Used for idle_transaction_timeout calculation. The timeout check condition
	// (TxStatus in {'T','E'} AND ActiveRequestFlow == nil) filters appropriately.
	LastReadyForQueryTime time.Time

	// TODO: do we have to track what portals are suspended?
}

type NamedObjectState[T any] struct {
	// Names created and not closed.
	Alive map[string]T
	// The client sent a Create, but the server hasn't responded yet.
	PendingCreate map[string]T
	// The client sent a Close, but the server hasn't responded yet.
	PendingClose map[string]T
	// True when the previous message was part of a strict Extended Query flow:
	// 1. Parse (set state.PreparedStatements.PendingExecute to the statement name)
	// 2. Bind (set state.Portals.PendingExecute to the portal name)
	// 3. Execute (set state.*.Executing to the corresponding name, clear PendingExecute)
	// (Calls to other Extended Query messages besides Close are permitted any time after Parse.)
	PendingExecute *string
	Executing      *string
}

func (s *ProtocolState) InTxOrQuery() bool {
	return s.TxStatus != TxIdle ||
		s.Statements.PendingExecute != nil ||
		s.Statements.Executing != nil ||
		s.Portals.PendingExecute != nil ||
		s.Portals.Executing != nil
}

// Update updates protocol state for the given message.
// For server messages, returns the popped request if one matched the response, nil otherwise.
// For client messages, always returns nil.
// Request flow ending happens automatically inside Update on ReadyForQuery.
func (s *ProtocolState) Update(msg Message) *PendingRequest {
	if m, ok := msg.(ClientMessage); ok {
		s.updateForFrontendMessage(m)
		return nil
	} else if m, ok := msg.(ServerMessage); ok {
		return s.updateForServerMessage(m)
	} else {
		panic(fmt.Sprintf("unexpected message: %T", msg))
	}
}

func (s *ProtocolState) updateForFrontendMessage(msg ClientMessage) {
	handlers := ClientMessageHandlers[struct{}]{
		SimpleQuery:   wrapVoid(s.updateForSimpleQueryMessage),
		ExtendedQuery: wrapVoid(s.updateForExtendedQueryMessage),
	}

	_, _ = handlers.HandleDefault(msg, func(msg ClientMessage) (struct{}, error) { return struct{}{}, nil })
}

func (s *ProtocolState) updateForServerMessage(msg ServerMessage) *PendingRequest {
	var poppedRequest *PendingRequest

	handlers := ServerMessageHandlers[struct{}]{
		Async: wrapVoid(s.updateForServerAsyncMessage),
		Copy:  wrapVoid(s.updateForServerCopyMessage),
		ExtendedQuery: func(msg ServerExtendedQuery) (struct{}, error) {
			poppedRequest = s.updateForServerExtendedQueryMessage(msg)
			return struct{}{}, nil
		},
		Response: func(msg ServerResponse) (struct{}, error) {
			poppedRequest = s.updateForServerResponseMessage(msg)
			return struct{}{}, nil
		},
	}
	_, _ = handlers.HandleDefault(msg, func(msg ServerMessage) (struct{}, error) { return struct{}{}, nil })

	return poppedRequest
}

func (s *ProtocolState) updateForSimpleQueryMessage(msg ClientSimpleQuery) {
	switch msg := msg.(type) {
	case *ClientQuery:
		s.clearPendingExecute()
		s.ExtendedQueryMode = false
		unnamed := ""
		delete(s.Statements.Alive, unnamed)
		delete(s.Portals.Alive, unnamed)
		s.Statements.Executing = &unnamed
		s.Portals.Executing = &unnamed
	case *ClientFunctionCall:
		// Nothing.
	default:
		panic(fmt.Sprintf("unexpected pgwire.ClientSimpleQuery: %#v", msg))
	}
}

func (s *ProtocolState) updateForExtendedQueryMessage(msg ClientExtendedQuery) {
	switch msg := msg.(type) {
	case *ClientParse:
		s.clearPendingExecute()
		s.ExtendedQueryMode = true
		parsed := msg.Parse()
		s.Statements.PendingCreate[parsed.Name] = true
		name := parsed.Name
		s.Statements.PendingExecute = &name
	case *ClientClose:
		s.clearPendingExecute()
		s.ExtendedQueryMode = true
		parsed := msg.Parse()
		if parsed.ObjectType == ObjectTypePreparedStatement {
			s.Statements.PendingClose[parsed.Name] = true
		} else {
			s.Portals.PendingClose[parsed.Name] = true
		}
	case *ClientBind:
		s.ExtendedQueryMode = true
		parsed := msg.Parse()
		s.Portals.PendingCreate[parsed.DestinationPortal] = true
		if s.Statements.PendingExecute != nil && *s.Statements.PendingExecute == parsed.PreparedStatement {
			dest := parsed.DestinationPortal
			s.Portals.PendingExecute = &dest
		} else {
			s.clearPendingExecute()
		}
	case *ClientDescribe:
		s.ExtendedQueryMode = true
	case *ClientExecute:
		s.ExtendedQueryMode = true
		parsed := msg.Parse()
		name := parsed.Portal
		s.Portals.Executing = &name
		if s.Portals.PendingExecute != nil && *s.Portals.PendingExecute == parsed.Portal {
			s.Statements.Executing = s.Statements.PendingExecute
		} else {
			stmtName := ""
			s.Statements.PendingExecute = nil
			s.Statements.Executing = &stmtName
		}
	case *ClientFlush:
	case *ClientSync:
		s.SyncsInFlight++
	default:
		panic(fmt.Sprintf("unexpected pgwire.ClientExtendedQuery: %#v", msg))
	}
}

// updateForServerExtendedQueryMessage updates state and pops the matching request.
// Returns the popped request if one matched, nil otherwise.
func (s *ProtocolState) updateForServerExtendedQueryMessage(msg ServerExtendedQuery) *PendingRequest {
	s.ExtendedQueryMode = true

	// Update object state
	switch msg.(type) {
	case *ServerParseComplete:
		for name := range s.Statements.PendingCreate {
			s.Statements.Alive[name] = true
		}
		clear(s.Statements.PendingCreate)
	case *ServerCloseComplete:
		for name := range s.Statements.PendingClose {
			s.Statements.Alive[name] = false
		}
		clear(s.Statements.PendingClose)
		for name := range s.Portals.PendingClose {
			s.Portals.Alive[name] = false
		}
		clear(s.Statements.PendingClose)
	case *ServerBindComplete:
		for name := range s.Portals.PendingCreate {
			s.Portals.Alive[name] = true
		}
		clear(s.Portals.PendingCreate)
	}

	// Pop matching request for ALL extended query response types.
	// This matches PgBouncer's behavior in server.c:pop_outstanding_request.
	if req, ok := s.popForResponse(msg); ok {
		return &req
	}
	return nil
}

func (s *ProtocolState) updateForServerCopyMessage(msg ServerCopy) {
	switch msg.(type) {
	case *ServerCopyInResponse:
		s.CopyMode = CopyIn
	case *ServerCopyOutResponse:
		s.CopyMode = CopyOut
	case *ServerCopyBothResponse:
		s.CopyMode = CopyBoth
	case *ServerCopyData:
		return
	case *ServerCopyDone:
		// TODO: should we actually only set this in ReadyForQuery?
		s.CopyMode = CopyNone
	default:
		panic(fmt.Sprintf("unexpected pgwire.ServerCopy: %T", msg))
	}
}

// updateForServerResponseMessage updates state and pops the matching request.
// Returns the popped request if one matched, nil otherwise.
// For ReadyForQuery, also ends the request flow if empty.
func (s *ProtocolState) updateForServerResponseMessage(msg ServerResponse) *PendingRequest {
	switch msg := msg.(type) {
	case *ServerReadyForQuery:
		s.CopyMode = CopyNone
		// Fast path: use TxStatusByte() to avoid full parsing
		s.TxStatus = msg.TxStatus()
		s.ServerIgnoringMessagesUntilSync = false
		if s.SyncsInFlight > 0 {
			s.SyncsInFlight--
		}
		// Always clear pending execute state when ReadyForQuery is received.
		// This handles flows like Parse+Describe+Sync where no Execute is sent.
		s.clearPendingExecute()

		// Update timeout tracking timestamps
		// Set LastReadyForQueryTime on ALL ReadyForQuery messages (for idle_transaction_timeout).
		s.LastReadyForQueryTime = time.Now()

		// Clear TxStartTime when transaction ends (TxStatus 'I').
		// Keep it when in transaction ('T' or 'E') for transaction_timeout tracking.
		// This matches pgbouncer's behavior in server.c:596-599.
		if s.TxStatus == TxIdle {
			s.TxStartTime = time.Time{}
		}

		// Pop the pending Sync/Query/FunctionCall request
		var poppedReq *PendingRequest
		if req, ok := s.popForResponse(msg); ok {
			poppedReq = &req
		}

		// End the flow only if there are no more pending requests.
		// This allows pipelining where multiple Sync messages are in flight.
		s.endRequestFlowIfEmpty()

		return poppedReq
	case *ServerCommandComplete, *ServerEmptyQueryResponse:
		// These complete Execute or Query requests
		if req, ok := s.popForResponse(msg); ok {
			return &req
		}
	case *ServerDataRow:
		// DataRow doesn't pop any request
	case *ServerFunctionCallResponse:
		s.clearPendingExecute()
		if req, ok := s.popForResponse(msg); ok {
			return &req
		}
	case *ServerErrorResponse:
		s.clearPendingExecute()
		if s.ExtendedQueryMode {
			s.ServerIgnoringMessagesUntilSync = true
		}
		// ErrorResponse can match any request
		if req, ok := s.popForResponse(msg); ok {
			return &req
		}
	default:
		panic(fmt.Sprintf("unexpected pgwire.ServerResponse: %T", msg))
	}
	return nil
}

func (s *ProtocolState) updateForServerAsyncMessage(msg ServerAsync) {
	switch msg := msg.(type) {
	case *ServerNoticeResponse:
	case *ServerNotificationResponse:
	case *ServerParameterStatus:
		parsed := msg.Parse()
		if parsed.Value == "" {
			delete(s.ParameterStatuses, parsed.Name)
		} else {
			s.ParameterStatuses[parsed.Name] = parsed.Value
		}
	default:
		panic(fmt.Sprintf("unexpected pgwire.ServerAsync: %T", msg))
	}
}

func (s *ProtocolState) clearPendingExecute() {
	s.Statements.PendingExecute = nil
	s.Statements.Executing = nil
	s.Portals.PendingExecute = nil
	s.Portals.Executing = nil
}

func wrapVoid[T any](fn func(T)) func(T) (struct{}, error) {
	return func(t T) (struct{}, error) {
		fn(t)
		return struct{}{}, nil
	}
}

// AddRecognizer adds a flow recognizer to track protocol flows.
// Recognizers are called in order when processing messages.
func (s *ProtocolState) AddRecognizer(r FlowRecognizer) {
	s.recognizers = append(s.recognizers, r)
}

// ProcessFlows updates active flows and detects new flows from the message.
// This should be called for each message after the protocol state is updated.
func (s *ProtocolState) ProcessFlows(msg Message) {
	// Check if any recognizer detects a new flow
	for _, r := range s.recognizers {
		handlers := r.StartHandlers(s)
		if flow := handlers.Start(msg); flow != nil {
			s.activeFlows = append(s.activeFlows, flow)
		}
	}

	// Update active flows, remove completed ones
	remaining := s.activeFlows[:0]
	for _, flow := range s.activeFlows {
		handlers := flow.UpdateHandlers(s)
		if handlers.Update(msg) { // returns true to continue
			remaining = append(remaining, flow)
		} else {
			flow.Close() // flow handles its own callbacks
		}
	}
	s.activeFlows = remaining
}

// ActiveFlowCount returns the number of currently active flows.
func (s *ProtocolState) ActiveFlowCount() int {
	return len(s.activeFlows)
}

// CloseAllFlows closes all active flows. Called when session ends.
func (s *ProtocolState) CloseAllFlows() {
	for _, flow := range s.activeFlows {
		flow.Close()
	}
	s.activeFlows = nil
}

// StartRequestFlow creates a new RequestFlow for tracking pending requests.
// Panics if a flow is already active - this indicates a bug.
func (s *ProtocolState) StartRequestFlow() *RequestFlow {
	if s.ActiveRequestFlow != nil {
		panic("starting request flow while one is already active")
	}
	s.ActiveRequestFlow = NewRequestFlow()
	return s.ActiveRequestFlow
}

// EndRequestFlow closes the active request flow and clears it.
// Safe to call when no flow is active (no-op).
func (s *ProtocolState) EndRequestFlow() {
	if s.ActiveRequestFlow != nil {
		s.ActiveRequestFlow.Close()
		s.ActiveRequestFlow = nil
	}
}

// PushRequest adds a pending request to the active flow.
// If no flow is active, starts a new one automatically.
// Also sets TxStartTime if it's zero (for transaction_timeout tracking).
func (s *ProtocolState) PushRequest(req PendingRequest) {
	if s.ActiveRequestFlow == nil {
		s.StartRequestFlow()
		// Set TxStartTime when starting a new request flow, if not already set.
		// This matches pgbouncer's behavior where xact_start = query_start when
		// query_start is set for the first time (client.c:1600-1602).
		if s.TxStartTime.IsZero() {
			s.TxStartTime = s.ActiveRequestFlow.StartTime
		}
	}
	s.ActiveRequestFlow.Push(req)
}

// popForResponse removes and returns the front request if it matches the response message.
// Returns (request, true) if matched, (zero, false) if no match or no active flow.
func (s *ProtocolState) popForResponse(msg ServerMessage) (PendingRequest, bool) {
	if s.ActiveRequestFlow == nil {
		return PendingRequest{}, false
	}
	return s.ActiveRequestFlow.popForResponse(msg)
}

// endRequestFlowIfEmpty ends the flow only if there are no pending requests.
// Returns true if the flow was ended.
func (s *ProtocolState) endRequestFlowIfEmpty() bool {
	if s.ActiveRequestFlow == nil {
		return false
	}
	if s.ActiveRequestFlow.Len() == 0 {
		s.ActiveRequestFlow.Close()
		s.ActiveRequestFlow = nil
		return true
	}
	return false
}

// OutstandingRequestCount returns the number of pending requests in the active flow.
// Returns 0 if no flow is active.
func (s *ProtocolState) OutstandingRequestCount() int {
	if s.ActiveRequestFlow == nil {
		return 0
	}
	return s.ActiveRequestFlow.Len()
}
