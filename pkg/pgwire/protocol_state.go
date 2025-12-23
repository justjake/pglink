package pgwire

import (
	"fmt"

	"github.com/jackc/pgx/v5/pgproto3"
)

// NewProtocolState creates a new ProtocolState with all maps initialized.
func NewProtocolState() ProtocolState {
	return ProtocolState{
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
	return s.TxStatus != TxIdle || s.Statements.PendingExecute != nil || s.Statements.Executing != nil || s.Portals.PendingExecute != nil && s.Portals.Executing != nil
}

func (s *ProtocolState) UpdateForFrontentMessage(msg pgproto3.FrontendMessage) {
	pgwireMsg, ok := ToClientMessage(msg)
	if !ok {
		panic(fmt.Sprintf("unexpected frontend message: %T", msg))
	}

	handlers := ClientMessageHandlers[struct{}]{
		SimpleQuery:   wrapVoid(s.UpdateForSimpleQueryMessage),
		ExtendedQuery: wrapVoid(s.UpdateForExtendedQueryMessage),
	}

	_, _ = handlers.HandleDefault(pgwireMsg, func(msg ClientMessage) (struct{}, error) { return struct{}{}, nil })
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

func (s *ProtocolState) UpdateForSimpleQueryMessage(msg ClientSimpleQuery) {
	switch msg := msg.(type) {
	case ClientSimpleQueryQuery:
		s.clearPendingExecute()
		s.ExtendedQueryMode = false
		unnamed := ""
		delete(s.Statements.Alive, unnamed)
		delete(s.Portals.Alive, unnamed)
		s.Statements.Executing = &unnamed
		s.Portals.Executing = &unnamed
	case ClientSimpleQueryFunctionCall:
		// Nothing.
	default:
		panic(fmt.Sprintf("unexpected pgwire.ClientSimpleQuery: %#v", msg))
	}
}

func (s *ProtocolState) UpdateForExtendedQueryMessage(msg ClientExtendedQuery) {
	switch msg := msg.(type) {
	case ClientExtendedQueryParse:
		s.clearPendingExecute()
		s.ExtendedQueryMode = true
		s.Statements.PendingCreate[msg.T.Name] = true
		name := msg.T.Name
		s.Statements.PendingExecute = &name
	case ClientExtendedQueryClose:
		s.clearPendingExecute()
		s.ExtendedQueryMode = true
		if msg.T.ObjectType == ObjectTypePreparedStatement {
			s.Statements.PendingClose[msg.T.Name] = true
		} else {
			s.Portals.PendingClose[msg.T.Name] = true
		}
	case ClientExtendedQueryBind:
		s.ExtendedQueryMode = true
		s.Statements.PendingCreate[msg.T.DestinationPortal] = true
		if s.Statements.PendingExecute != nil && *s.Statements.PendingExecute == msg.T.PreparedStatement {
			dest := msg.T.DestinationPortal
			s.Portals.PendingExecute = &dest
		} else {
			s.clearPendingExecute()
		}
	case ClientExtendedQueryDescribe:
		s.ExtendedQueryMode = true
	case ClientExtendedQueryExecute:
		s.ExtendedQueryMode = true
		name := msg.T.Portal
		s.Portals.Executing = &name
		if s.Portals.PendingExecute != nil && *s.Portals.PendingExecute == msg.T.Portal {
			s.Statements.Executing = s.Statements.PendingExecute
		} else {
			stmtName := ""
			s.Statements.PendingExecute = nil
			s.Statements.Executing = &stmtName
		}
	case ClientExtendedQueryFlush:
	case ClientExtendedQuerySync:
		s.SyncsInFlight++
	default:
		panic(fmt.Sprintf("unexpected pgwire.ClientExtendedQuery: %#v", msg))
	}
}

func (s *ProtocolState) UpdateForServerExtendedQueryMessage(msg ServerExtendedQuery) {
	s.ExtendedQueryMode = true

	switch msg := msg.(type) {
	case ServerExtendedQueryParseComplete:
		for name := range s.Statements.PendingCreate {
			s.Statements.Alive[name] = true
		}
		clear(s.Statements.PendingCreate)
	case ServerExtendedQueryCloseComplete:
		for name := range s.Statements.PendingClose {
			s.Statements.Alive[name] = false
		}
		clear(s.Statements.PendingClose)
		for name := range s.Portals.PendingClose {
			s.Portals.Alive[name] = false
		}
		clear(s.Statements.PendingClose)
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
		if s.SyncsInFlight > 0 {
			s.SyncsInFlight--
		}
		if s.Portals.Executing != nil {
			s.clearPendingExecute()
		}
	case ServerResponseCommandComplete:
	case ServerResponseDataRow:
	case ServerResponseEmptyQueryResponse:
		s.clearPendingExecute()
	case ServerResponseFunctionCallResponse:
		s.clearPendingExecute()
	case ServerResponseErrorResponse:
		s.clearPendingExecute()
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
