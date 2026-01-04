package pgwire

import (
	"time"
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
	// RequestType is the PostgreSQL message type (MsgClientParse, MsgClientBind, etc.)
	RequestType MsgType

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

// RequestFlow tracks outstanding requests in a protocol flow.
// A flow starts with a client message (Parse, Query, etc.) and ends with ReadyForQuery.
type RequestFlow struct {
	requests []PendingRequest

	// StartTime is when the flow was created. Used for query_timeout calculation.
	// For pipelined queries, this is when the first request was pushed to the flow.
	StartTime time.Time

	// OnComplete is called when the flow ends (Close is called).
	OnComplete func(flow *RequestFlow)
}

// NewRequestFlow creates a new RequestFlow with StartTime set to now.
func NewRequestFlow() *RequestFlow {
	return &RequestFlow{
		StartTime: time.Now(),
	}
}

// Close is called when the flow ends (either completed or terminated).
func (f *RequestFlow) Close() {
	if f.OnComplete != nil {
		f.OnComplete(f)
	}
}

// Push adds a pending request to the queue
func (f *RequestFlow) Push(req PendingRequest) {
	f.requests = append(f.requests, req)
}

// popForResponse removes and returns the front request if it matches the response message.
// Returns (request, true) if matched, (zero, false) if no match or queue is empty.
//
// This validates that responses arrive in the expected order - a ParseComplete
// should only be received after a Parse was sent.
// Uses type switch for type-safe matching instead of byte comparison.
func (f *RequestFlow) popForResponse(msg ServerMessage) (PendingRequest, bool) {
	if len(f.requests) == 0 {
		return PendingRequest{}, false
	}

	req := f.requests[0]
	if !responseMatchesRequest(msg, req.RequestType) {
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
		if req.RequestType == MsgClientSync {
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

// responseMatchesRequest checks if a server response message matches a pending request type.
// This enforces the PostgreSQL protocol's request/response correspondence.
// This exactly mirrors PgBouncer's pop_outstanding_request behavior.
func responseMatchesRequest(msg ServerMessage, requestType MsgType) bool {
	switch msg.(type) {
	case *ServerParseComplete:
		return requestType == MsgClientParse
	case *ServerBindComplete:
		return requestType == MsgClientBind
	case *ServerCloseComplete:
		return requestType == MsgClientClose
	case *ServerNoData, *ServerRowDescription, *ServerParameterDescription:
		return requestType == MsgClientDescribe
	case *ServerCommandComplete:
		return requestType == MsgClientQuery || requestType == MsgClientExecute
	case *ServerEmptyQueryResponse:
		return requestType == MsgClientQuery || requestType == MsgClientExecute
	case *ServerPortalSuspended:
		return requestType == MsgClientExecute
	case *ServerReadyForQuery:
		return requestType == MsgClientSync || requestType == MsgClientQuery || requestType == MsgClientFunc
	case *ServerFunctionCallResponse:
		return requestType == MsgClientFunc
	case *ServerErrorResponse:
		return true // ErrorResponse can match any request
	default:
		return false
	}
}
