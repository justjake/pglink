package pgwire

import (
	"github.com/jackc/pgx/v5/pgproto3"
)

// ResponseAction determines how to handle a server response
type ResponseAction int

const (
	// ActionForward forwards the response to the client
	ActionForward ResponseAction = iota
	// ActionSkip consumes the response silently without forwarding
	ActionSkip
	// ActionFake generates a synthetic response without sending to server
	ActionFake
)

func (a ResponseAction) String() string {
	switch a {
	case ActionForward:
		return "forward"
	case ActionSkip:
		return "skip"
	case ActionFake:
		return "fake"
	default:
		return "unknown"
	}
}

// PendingRequest tracks a single request awaiting a server response
type PendingRequest struct {
	// RequestType is the PostgreSQL message type byte ('P' for Parse, 'B' for Bind, etc.)
	RequestType byte

	// Action determines how to handle the response
	Action ResponseAction

	// StatementName is the client's name for the prepared statement (for Parse/Bind/Execute)
	StatementName string

	// Query is the SQL query text (for Parse requests)
	Query string

	// QueryHash is a hash of the query for cache lookup
	QueryHash uint64

	// FakeResponse generates the wire-format response for ActionFake
	// Only used when Action == ActionFake
	FakeResponse func() []byte
}

// RequestFlow implements Flow and tracks outstanding requests in a protocol flow.
// A flow starts with a client message (Parse, Query, etc.) and ends with ReadyForQuery.
//
// RequestFlow composes the Flow abstraction with request/response tracking:
// - As a Flow, it tracks message sequences and determines when flows complete
// - As a request queue, it tracks pending requests and their expected response actions
type RequestFlow struct {
	requests   []PendingRequest
	isComplete bool

	// Callbacks (set by the creator/recognizer)
	OnResponse func(req PendingRequest, msg ServerMessage) ResponseAction
	OnComplete func(flow *RequestFlow)
	OnError    func(flow *RequestFlow, err *pgproto3.ErrorResponse)
}

// NewRequestFlow creates a new RequestFlow
func NewRequestFlow() *RequestFlow {
	return &RequestFlow{}
}

// UpdateHandlers implements the Flow interface.
// Returns handlers that process messages and determine when the flow is complete.
func (f *RequestFlow) UpdateHandlers(state *ProtocolState) FlowUpdateHandlers {
	return FlowUpdateHandlers{
		Client: FlowUpdateClientHandlers{
			ExtendedQuery: func(msg ClientExtendedQuery) bool {
				// Client messages are tracked via Push() - this is informational
				return true
			},
			SimpleQuery: func(msg ClientSimpleQuery) bool {
				return true
			},
		},
		Server: FlowUpdateServerHandlers{
			Response: func(msg ServerResponse) bool {
				if _, ok := msg.(*ServerResponseReadyForQuery); ok {
					f.isComplete = true
					return false // Flow complete
				}
				if errResp, ok := msg.(*ServerResponseErrorResponse); ok {
					if f.OnError != nil {
						f.OnError(f, errResp.Parse())
					}
					// Error recovery: clear pending requests until Sync
					if state.ExtendedQueryMode {
						f.ClearUntilSync()
					}
				}
				return true
			},
			ExtendedQuery: func(msg ServerExtendedQuery) bool {
				return true
			},
			Copy: func(msg ServerCopy) bool {
				return true
			},
			Async: func(msg ServerAsync) bool {
				// Async messages don't affect flow lifecycle
				return true
			},
		},
	}
}

// Close implements the Flow interface.
// Called when the flow ends (either completed or terminated).
func (f *RequestFlow) Close() {
	if f.OnComplete != nil {
		f.OnComplete(f)
	}
}

// Push adds a pending request to the queue
func (f *RequestFlow) Push(req PendingRequest) {
	f.requests = append(f.requests, req)
}

// PopForResponse removes and returns the front request if it matches the response type.
// Returns (request, true) if matched, (zero, false) if no match or queue is empty.
//
// This validates that responses arrive in the expected order - a ParseComplete
// should only be received after a Parse was sent.
func (f *RequestFlow) PopForResponse(responseType byte) (PendingRequest, bool) {
	if len(f.requests) == 0 {
		return PendingRequest{}, false
	}

	req := f.requests[0]
	if !responseMatchesRequest(responseType, req.RequestType) {
		return PendingRequest{}, false
	}

	f.requests = f.requests[1:]
	return req, true
}

// Peek returns the front request without removing it.
// Returns (request, true) if queue is non-empty, (zero, false) if empty.
func (f *RequestFlow) Peek() (PendingRequest, bool) {
	if len(f.requests) == 0 {
		return PendingRequest{}, false
	}
	return f.requests[0], true
}

// ClearUntilSync removes all pending requests up to and including the next Sync.
// This is called for error recovery - when an error occurs in extended query mode,
// the server ignores messages until Sync, so we clear our tracking to match.
func (f *RequestFlow) ClearUntilSync() {
	for i, req := range f.requests {
		if req.RequestType == 'S' { // Sync
			f.requests = f.requests[i+1:]
			return
		}
	}
	// No Sync found - clear everything
	f.requests = nil
}

// Len returns the number of pending requests
func (f *RequestFlow) Len() int {
	return len(f.requests)
}

// IsComplete returns true if the flow has completed (ReadyForQuery received)
func (f *RequestFlow) IsComplete() bool {
	return f.isComplete
}

// responseMatchesRequest checks if a server response type is valid for a given request type.
// This enforces the PostgreSQL protocol's request/response correspondence.
func responseMatchesRequest(responseType, requestType byte) bool {
	switch responseType {
	case '1': // ParseComplete
		return requestType == 'P' // Parse
	case '2': // BindComplete
		return requestType == 'B' // Bind
	case '3': // CloseComplete
		return requestType == 'C' // Close
	case 'n': // NoData (response to Describe for a statement with no result columns)
		return requestType == 'D' // Describe
	case 't': // ParameterDescription (response to Describe)
		return requestType == 'D' // Describe
	case 'T': // RowDescription (response to Describe or part of query result)
		return requestType == 'D' // Describe
	case 'Z': // ReadyForQuery
		return requestType == 'S' || requestType == 'Q' // Sync or Query
	case 'E': // ErrorResponse - can be generated by any request
		return true
	case 'I': // EmptyQueryResponse
		return requestType == 'Q' || requestType == 'E' // Query or Execute
	case 'C': // CommandComplete
		return requestType == 'Q' || requestType == 'E' // Query or Execute
	case 's': // PortalSuspended
		return requestType == 'E' // Execute
	default:
		// Unknown response type - don't match
		return false
	}
}

// PostgreSQL message type constants for clarity
const (
	MsgTypeParse    byte = 'P'
	MsgTypeBind     byte = 'B'
	MsgTypeDescribe byte = 'D'
	MsgTypeExecute  byte = 'E'
	MsgTypeClose    byte = 'C'
	MsgTypeSync     byte = 'S'
	MsgTypeQuery    byte = 'Q'
	MsgTypeFlush    byte = 'H'
)
