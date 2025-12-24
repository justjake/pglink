package pgwire

import (
	"fmt"

	"github.com/jackc/pgx/v5/pgproto3"
)

type Message interface {
	PgwireMessage() pgproto3.Message
}

// FromFrontend wraps a message that came from the frontend.
// Used if message is both Frontend and Backend, to distinguish its origin at runtime.
type FromClient[T pgproto3.FrontendMessage] struct {
	T T
}

type ClientMessage interface {
	Message
	Client() pgproto3.FrontendMessage
}

// FromBackend wraps a message that came from the backend.
// Used if message is both Frontend and Backend, to distinguish its origin at runtime.
type FromServer[T pgproto3.BackendMessage] struct {
	T T
}

type ServerMessage interface {
	Message
	Server() pgproto3.BackendMessage
}

func ToClientMessage(msg pgproto3.FrontendMessage) (ClientMessage, bool) {
	switch m := msg.(type) {
	// Cancel
	case *pgproto3.CancelRequest:
		return ClientCancelCancelRequest{m}, true
	// Copy
	case *pgproto3.CopyData:
		return ClientCopyCopyData{m}, true
	case *pgproto3.CopyDone:
		return ClientCopyCopyDone{m}, true
	case *pgproto3.CopyFail:
		return ClientCopyCopyFail{m}, true
	// SimpleQuery
	case *pgproto3.Query:
		return ClientSimpleQueryQuery{m}, true
	case *pgproto3.FunctionCall:
		return ClientSimpleQueryFunctionCall{m}, true
	// ExtendedQuery
	case *pgproto3.Parse:
		return ClientExtendedQueryParse{m}, true
	case *pgproto3.Bind:
		return ClientExtendedQueryBind{m}, true
	case *pgproto3.Execute:
		return ClientExtendedQueryExecute{m}, true
	case *pgproto3.Sync:
		return ClientExtendedQuerySync{m}, true
	case *pgproto3.Describe:
		return ClientExtendedQueryDescribe{m}, true
	case *pgproto3.Close:
		return ClientExtendedQueryClose{m}, true
	case *pgproto3.Flush:
		return ClientExtendedQueryFlush{m}, true
	// TerminateConn
	case *pgproto3.Terminate:
		return ClientTerminateConnTerminate{m}, true
	// Startup
	case *pgproto3.GSSEncRequest:
		return ClientStartupGSSEncRequest{m}, true
	case *pgproto3.GSSResponse:
		return ClientStartupGSSResponse{m}, true
	case *pgproto3.PasswordMessage:
		return ClientStartupPasswordMessage{m}, true
	case *pgproto3.SASLInitialResponse:
		return ClientStartupSASLInitialResponse{m}, true
	case *pgproto3.SASLResponse:
		return ClientStartupSASLResponse{m}, true
	case *pgproto3.SSLRequest:
		return ClientStartupSSLRequest{m}, true
	case *pgproto3.StartupMessage:
		return ClientStartupStartupMessage{m}, true
	}
	return nil, false
}

func ToServerMessage(msg pgproto3.BackendMessage) (ServerMessage, bool) {
	switch m := msg.(type) {
	// Async
	case *pgproto3.NoticeResponse:
		return ServerAsyncNoticeResponse{m}, true
	case *pgproto3.NotificationResponse:
		return ServerAsyncNotificationResponse{m}, true
	case *pgproto3.ParameterStatus:
		return ServerAsyncParameterStatus{m}, true
	// Copy
	case *pgproto3.CopyInResponse:
		return ServerCopyCopyInResponse{m}, true
	case *pgproto3.CopyOutResponse:
		return ServerCopyCopyOutResponse{m}, true
	case *pgproto3.CopyBothResponse:
		return ServerCopyCopyBothResponse{m}, true
	case *pgproto3.CopyData:
		return ServerCopyCopyData{m}, true
	case *pgproto3.CopyDone:
		return ServerCopyCopyDone{m}, true
	// ExtendedQuery
	case *pgproto3.ParseComplete:
		return ServerExtendedQueryParseComplete{m}, true
	case *pgproto3.BindComplete:
		return ServerExtendedQueryBindComplete{m}, true
	case *pgproto3.ParameterDescription:
		return ServerExtendedQueryParameterDescription{m}, true
	case *pgproto3.RowDescription:
		return ServerExtendedQueryRowDescription{m}, true
	case *pgproto3.NoData:
		return ServerExtendedQueryNoData{m}, true
	case *pgproto3.PortalSuspended:
		return ServerExtendedQueryPortalSuspended{m}, true
	case *pgproto3.CloseComplete:
		return ServerExtendedQueryCloseComplete{m}, true
	// Response
	case *pgproto3.ReadyForQuery:
		return ServerResponseReadyForQuery{m}, true
	case *pgproto3.CommandComplete:
		return ServerResponseCommandComplete{m}, true
	case *pgproto3.DataRow:
		return ServerResponseDataRow{m}, true
	case *pgproto3.EmptyQueryResponse:
		return ServerResponseEmptyQueryResponse{m}, true
	case *pgproto3.ErrorResponse:
		return ServerResponseErrorResponse{m}, true
	case *pgproto3.FunctionCallResponse:
		return ServerResponseFunctionCallResponse{m}, true
	// Startup
	case *pgproto3.AuthenticationCleartextPassword:
		return ServerStartupAuthenticationCleartextPassword{m}, true
	case *pgproto3.AuthenticationGSS:
		return ServerStartupAuthenticationGSS{m}, true
	case *pgproto3.AuthenticationGSSContinue:
		return ServerStartupAuthenticationGSSContinue{m}, true
	case *pgproto3.AuthenticationMD5Password:
		return ServerStartupAuthenticationMD5Password{m}, true
	case *pgproto3.AuthenticationOk:
		return ServerStartupAuthenticationOk{m}, true
	case *pgproto3.AuthenticationSASL:
		return ServerStartupAuthenticationSASL{m}, true
	case *pgproto3.AuthenticationSASLContinue:
		return ServerStartupAuthenticationSASLContinue{m}, true
	case *pgproto3.AuthenticationSASLFinal:
		return ServerStartupAuthenticationSASLFinal{m}, true
	case *pgproto3.BackendKeyData:
		return ServerStartupBackendKeyData{m}, true
	}
	return nil, false
}

func ToMessage(msg pgproto3.Message) (Message, bool) {
	if m, ok := msg.(pgproto3.FrontendMessage); ok {
		return ToClientMessage(m)
	} else if m, ok := msg.(pgproto3.BackendMessage); ok {
		return ToServerMessage(m)
	} else {
		return nil, false
	}
}

type ClientMessageHandlers[T any] struct {
	Cancel        func(msg ClientCancel) (T, error)
	Copy          func(msg ClientCopy) (T, error)
	ExtendedQuery func(msg ClientExtendedQuery) (T, error)
	SimpleQuery   func(msg ClientSimpleQuery) (T, error)
	Startup       func(msg ClientStartup) (T, error)
	TerminateConn func(msg ClientTerminateConn) (T, error)
}

func (h ClientMessageHandlers[T]) HandleDefault(msg ClientMessage, defaultHandler func(msg ClientMessage) (T, error)) (r T, err error) {
	switch msg := msg.(type) {
	case ClientCancel:
		if h.Cancel != nil {
			return h.Cancel(msg)
		} else {
			return defaultHandler(msg)
		}
	case ClientCopy:
		if h.Copy != nil {
			return h.Copy(msg)
		} else {
			return defaultHandler(msg)
		}
	case ClientExtendedQuery:
		if h.ExtendedQuery != nil {
			return h.ExtendedQuery(msg)
		} else {
			return defaultHandler(msg)
		}
	case ClientSimpleQuery:
		if h.SimpleQuery != nil {
			return h.SimpleQuery(msg)
		} else {
			return defaultHandler(msg)
		}
	case ClientStartup:
		if h.Startup != nil {
			return h.Startup(msg)
		} else {
			return defaultHandler(msg)
		}
	case ClientTerminateConn:
		if h.TerminateConn != nil {
			return h.TerminateConn(msg)
		} else {
			return defaultHandler(msg)
		}
	}
	err = fmt.Errorf("unknown client message: %T", msg)
	return
}

func (h ClientMessageHandlers[T]) Handle(msg ClientMessage) (r T, err error) {
	return h.HandleDefault(msg, func(msg ClientMessage) (T, error) {
		panic(fmt.Sprintf("no handler defined for client message: %T", msg))
	})
}

type ServerMessageHandlers[T any] struct {
	Async         func(msg ServerAsync) (T, error)
	Copy          func(msg ServerCopy) (T, error)
	ExtendedQuery func(msg ServerExtendedQuery) (T, error)
	Response      func(msg ServerResponse) (T, error)
	Startup       func(msg ServerStartup) (T, error)
}

func (h ServerMessageHandlers[T]) HandleDefault(msg ServerMessage, defaultHandler func(msg ServerMessage) (T, error)) (r T, err error) {
	switch msg := msg.(type) {
	case ServerAsync:
		if h.Async != nil {
			return h.Async(msg)
		} else {
			return defaultHandler(msg)
		}
	case ServerCopy:
		if h.Copy != nil {
			return h.Copy(msg)
		} else {
			return defaultHandler(msg)
		}
	case ServerExtendedQuery:
		if h.ExtendedQuery != nil {
			return h.ExtendedQuery(msg)
		} else {
			return defaultHandler(msg)
		}
	case ServerResponse:
		if h.Response != nil {
			return h.Response(msg)
		} else {
			return defaultHandler(msg)
		}
	case ServerStartup:
		if h.Startup != nil {
			return h.Startup(msg)
		} else {
			return defaultHandler(msg)
		}
	}
	err = fmt.Errorf("unknown server message: %T", msg)
	return
}

func (h ServerMessageHandlers[T]) Handle(msg ServerMessage) (r T, err error) {
	return h.HandleDefault(msg, func(msg ServerMessage) (T, error) {
		panic(fmt.Sprintf("no handler defined for server message: %T", msg))
	})
}

type MessageHandlers[T any] struct {
	Client ClientMessageHandlers[T]
	Server ServerMessageHandlers[T]
}

func (h MessageHandlers[T]) HandleDefault(msg Message, defaultHandler func(msg Message) (T, error)) (r T, err error) {
	if m, ok := msg.(ClientMessage); ok {
		return h.Client.HandleDefault(m, func(msg ClientMessage) (T, error) {
			return defaultHandler(msg)
		})
	} else if m, ok := msg.(ServerMessage); ok {
		return h.Server.HandleDefault(m, func(msg ServerMessage) (T, error) {
			return defaultHandler(msg)
		})
	}
	err = fmt.Errorf("unknown message (neither client nor server): %T", msg)
	return
}

func (h MessageHandlers[T]) Handle(msg Message) (r T, err error) {
	return h.HandleDefault(msg, func(msg Message) (T, error) {
		panic(fmt.Sprintf("no handler defined for message: %T", msg))
	})
}

const (
	ObjectTypePreparedStatement = 'S'
	ObjectTypePortal            = 'P'
)
