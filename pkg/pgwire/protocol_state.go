package pgwire

import (
	"fmt"

	"github.com/jackc/pgx/v5/pgproto3"
)

// NewProtocolState creates a new ProtocolState with all maps initialized.
func NewProtocolState() ProtocolState {
	return ProtocolState{
		ParameterStatuses: ParameterStatuses{},
		PreparedStatements: NamedObjectState[bool]{
			Alive:         make(map[string]bool),
			PendingCreate: make(map[string]bool),
			PendingClose:  make(map[string]bool),
		},
		Portals: NamedObjectState[bool]{
			Alive:         make(map[string]bool),
			PendingCreate: make(map[string]bool),
			PendingClose:  make(map[string]bool),
		},
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

	PreparedStatements NamedObjectState[bool]
	Portals            NamedObjectState[bool]

	// TODO: do we have to track what portals are suspended? / What executions are underway?
}

type NamedObjectState[T any] struct {
	Alive         map[string]T
	PendingCreate map[string]T
	PendingClose  map[string]T
}

func (s *ProtocolState) UpdateForFrontentMessage(msg pgproto3.FrontendMessage) {
	if extendedQuery, ok := ToClientExtendedQuery(msg); ok {
		s.UpdateForExtendedQueryMessage(extendedQuery)
	}
}

func (s *ProtocolState) UpdateForServerMessage(msg ServerMessage) {
	handlers := ServerMessageHandlers[struct{}]{
		Async:         wrapVoid(s.UpdateForServerAsyncMessage),
		Copy:          wrapVoid(s.UpdateForServerCopyMessage),
		ExtendedQuery: wrapVoid(s.UpdateForServerExtendedQueryMessage),
		Response:      wrapVoid(s.UpdateForServerResponseMessage),
	}
	_, _ = handlers.HandleDefault(msg, func(msg ServerMessage) (struct{}, error) { return struct{}{}, nil })
}

func (s *ProtocolState) UpdateForExtendedQueryMessage(msg ClientExtendedQuery) {
	switch msg := msg.(type) {
	case ClientExtendedQueryParse:
		s.ExtendedQueryMode = true
		s.PreparedStatements.PendingCreate[msg.T.Name] = true
	case ClientExtendedQueryClose:
		s.ExtendedQueryMode = true
		if msg.T.ObjectType == ObjectTypePreparedStatement {
			s.PreparedStatements.PendingClose[msg.T.Name] = true
		} else {
			s.Portals.PendingClose[msg.T.Name] = true
		}
	case ClientExtendedQueryBind:
		s.ExtendedQueryMode = true
		s.PreparedStatements.PendingCreate[msg.T.DestinationPortal] = true
	case ClientExtendedQueryDescribe:
		s.ExtendedQueryMode = true
	case ClientExtendedQueryExecute:
		s.ExtendedQueryMode = true
	case ClientExtendedQueryFlush:
	case ClientExtendedQuerySync:
	default:
		panic(fmt.Sprintf("unexpected pgwire.ClientExtendedQuery: %#v", msg))
	}
}

func (s *ProtocolState) UpdateForServerExtendedQueryMessage(msg ServerExtendedQuery) {
	s.ExtendedQueryMode = true

	switch msg := msg.(type) {
	case ServerExtendedQueryParseComplete:
		for name := range s.PreparedStatements.PendingCreate {
			s.PreparedStatements.Alive[name] = true
		}
		clear(s.PreparedStatements.PendingCreate)
	case ServerExtendedQueryCloseComplete:
		for name := range s.PreparedStatements.PendingClose {
			s.PreparedStatements.Alive[name] = false
		}
		clear(s.PreparedStatements.PendingClose)
		for name := range s.Portals.PendingClose {
			s.Portals.Alive[name] = false
		}
		clear(s.PreparedStatements.PendingClose)
	case ServerExtendedQueryBindComplete:
		for name := range s.Portals.PendingCreate {
			s.Portals.Alive[name] = true
		}
		clear(s.Portals.PendingCreate)
	case ServerExtendedQueryNoData:
	case ServerExtendedQueryParameterDescription:
	case ServerExtendedQueryPortalSuspended:
	case ServerExtendedQueryRowDescription:
		return
	default:
		panic(fmt.Sprintf("unexpected pgwire.ServerExtendedQuery: %T", msg))
	}
}

func (s *ProtocolState) UpdateForServerCopyMessage(msg ServerCopy) {
	switch msg.(type) {
	case ServerCopyCopyInResponse:
		s.CopyMode = CopyIn
	case ServerCopyCopyOutResponse:
		s.CopyMode = CopyOut
	case ServerCopyCopyBothResponse:
		s.CopyMode = CopyBoth
	case ServerCopyCopyData:
		return
	case ServerCopyCopyDone:
		// TODO: should we actually only set this in ReadyForQuery?
		s.CopyMode = CopyNone
	default:
		panic(fmt.Sprintf("unexpected pgwire.ServerCopy: %T", msg))
	}
}

func (s *ProtocolState) UpdateForServerResponseMessage(msg ServerResponse) {
	switch msg := msg.(type) {
	case ServerResponseReadyForQuery:
		s.CopyMode = CopyNone
		s.TxStatus = TxStatus(msg.T.TxStatus)
		s.ServerIgnoringMessagesUntilSync = false
	case ServerResponseCommandComplete:
	case ServerResponseDataRow:
	case ServerResponseEmptyQueryResponse:
	case ServerResponseFunctionCallResponse:
	case ServerResponseErrorResponse:
		if s.ExtendedQueryMode {
			s.ServerIgnoringMessagesUntilSync = true
		}
	default:
		panic(fmt.Sprintf("unexpected pgwire.ServerResponse: %T", msg))
	}
}

func (s *ProtocolState) UpdateForServerAsyncMessage(msg ServerAsync) {
	switch msg := msg.(type) {
	case ServerAsyncNoticeResponse:
	case ServerAsyncNotificationResponse:
	case ServerAsyncParameterStatus:
		if msg.T.Value == "" {
			delete(s.ParameterStatuses, msg.T.Name)
		} else {
			s.ParameterStatuses[msg.T.Name] = msg.T.Value
		}
	default:
		panic(fmt.Sprintf("unexpected pgwire.ServerAsync: %T", msg))
	}
}

func wrapVoid[T any](fn func(T)) func(T) (struct{}, error) {
	return func(t T) (struct{}, error) {
		fn(t)
		return struct{}{}, nil
	}
}
