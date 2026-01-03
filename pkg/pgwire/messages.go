package pgwire

import (
	"fmt"

	"github.com/jackc/pgx/v5/pgproto3"
)

type Message interface {
	MsgType() MsgType
	Source() RawMessageSource
}

type ClientMessage interface {
	Message
	Client()
}

type ServerMessage interface {
	Message
	Server()
}

func ToClient(msg pgproto3.FrontendMessage) ClientMessage {
	if m, ok := ToClientMessage(msg); ok {
		return m
	} else {
		panic(fmt.Sprintf("unknown client message: %T", msg))
	}
}

// ToClientMessage converts a pgproto3.FrontendMessage to a ClientMessage.
// Note: This allocates. For zero-allocation iteration, use Cursor.AsClient().
func ToClientMessage(msg pgproto3.FrontendMessage) (ClientMessage, bool) {
	switch m := msg.(type) {
	// Cancel
	case *pgproto3.CancelRequest:
		return (*ClientCancelRequest)(ClientParsed(m)), true
	// Copy
	case *pgproto3.CopyData:
		return (*ClientCopyData)(ClientParsed(m)), true
	case *pgproto3.CopyDone:
		return (*ClientCopyDone)(ClientParsed(m)), true
	case *pgproto3.CopyFail:
		return (*ClientCopyFail)(ClientParsed(m)), true
	// SimpleQuery
	case *pgproto3.Query:
		return (*ClientQuery)(ClientParsed(m)), true
	case *pgproto3.FunctionCall:
		return (*ClientFunctionCall)(ClientParsed(m)), true
	// ExtendedQuery
	case *pgproto3.Parse:
		return (*ClientParse)(ClientParsed(m)), true
	case *pgproto3.Bind:
		return (*ClientBind)(ClientParsed(m)), true
	case *pgproto3.Execute:
		return (*ClientExecute)(ClientParsed(m)), true
	case *pgproto3.Sync:
		return (*ClientSync)(ClientParsed(m)), true
	case *pgproto3.Describe:
		return (*ClientDescribe)(ClientParsed(m)), true
	case *pgproto3.Close:
		return (*ClientClose)(ClientParsed(m)), true
	case *pgproto3.Flush:
		return (*ClientFlush)(ClientParsed(m)), true
	// TerminateConn
	case *pgproto3.Terminate:
		return (*ClientTerminate)(ClientParsed(m)), true
	// Startup
	case *pgproto3.GSSEncRequest:
		return (*ClientGSSEncRequest)(ClientParsed(m)), true
	case *pgproto3.GSSResponse:
		return (*ClientGSSResponse)(ClientParsed(m)), true
	case *pgproto3.PasswordMessage:
		return (*ClientPasswordMessage)(ClientParsed(m)), true
	case *pgproto3.SASLInitialResponse:
		return (*ClientSASLInitialResponse)(ClientParsed(m)), true
	case *pgproto3.SASLResponse:
		return (*ClientSASLResponse)(ClientParsed(m)), true
	case *pgproto3.SSLRequest:
		return (*ClientSSLRequest)(ClientParsed(m)), true
	case *pgproto3.StartupMessage:
		return (*ClientStartupMessage)(ClientParsed(m)), true
	}
	return nil, false
}

func ToServer(msg pgproto3.BackendMessage) ServerMessage {
	if m, ok := ToServerMessage(msg); ok {
		return m
	} else {
		panic(fmt.Sprintf("unknown server message: %T", msg))
	}
}

// ToServerMessage converts a pgproto3.BackendMessage to a ServerMessage.
// Note: This allocates. For zero-allocation iteration, use Cursor.AsServer().
func ToServerMessage(msg pgproto3.BackendMessage) (ServerMessage, bool) {
	switch m := msg.(type) {
	// Async
	case *pgproto3.NoticeResponse:
		return (*ServerNoticeResponse)(ServerParsed(m)), true
	case *pgproto3.NotificationResponse:
		return (*ServerNotificationResponse)(ServerParsed(m)), true
	case *pgproto3.ParameterStatus:
		return (*ServerParameterStatus)(ServerParsed(m)), true
	// Copy
	case *pgproto3.CopyInResponse:
		return (*ServerCopyInResponse)(ServerParsed(m)), true
	case *pgproto3.CopyOutResponse:
		return (*ServerCopyOutResponse)(ServerParsed(m)), true
	case *pgproto3.CopyBothResponse:
		return (*ServerCopyBothResponse)(ServerParsed(m)), true
	case *pgproto3.CopyData:
		return (*ServerCopyData)(ServerParsed(m)), true
	case *pgproto3.CopyDone:
		return (*ServerCopyDone)(ServerParsed(m)), true
	// ExtendedQuery
	case *pgproto3.ParseComplete:
		return (*ServerParseComplete)(ServerParsed(m)), true
	case *pgproto3.BindComplete:
		return (*ServerBindComplete)(ServerParsed(m)), true
	case *pgproto3.ParameterDescription:
		return (*ServerParameterDescription)(ServerParsed(m)), true
	case *pgproto3.RowDescription:
		return (*ServerRowDescription)(ServerParsed(m)), true
	case *pgproto3.NoData:
		return (*ServerNoData)(ServerParsed(m)), true
	case *pgproto3.PortalSuspended:
		return (*ServerPortalSuspended)(ServerParsed(m)), true
	case *pgproto3.CloseComplete:
		return (*ServerCloseComplete)(ServerParsed(m)), true
	// Response
	case *pgproto3.ReadyForQuery:
		return (*ServerReadyForQuery)(ServerParsed(m)), true
	case *pgproto3.CommandComplete:
		return (*ServerCommandComplete)(ServerParsed(m)), true
	case *pgproto3.DataRow:
		return (*ServerDataRow)(ServerParsed(m)), true
	case *pgproto3.EmptyQueryResponse:
		return (*ServerEmptyQueryResponse)(ServerParsed(m)), true
	case *pgproto3.ErrorResponse:
		return (*ServerErrorResponse)(ServerParsed(m)), true
	case *pgproto3.FunctionCallResponse:
		return (*ServerFunctionCallResponse)(ServerParsed(m)), true
	// Startup
	case *pgproto3.AuthenticationCleartextPassword:
		return (*ServerAuthenticationCleartextPassword)(ServerParsed(m)), true
	case *pgproto3.AuthenticationGSS:
		return (*ServerAuthenticationGSS)(ServerParsed(m)), true
	case *pgproto3.AuthenticationGSSContinue:
		return (*ServerAuthenticationGSSContinue)(ServerParsed(m)), true
	case *pgproto3.AuthenticationMD5Password:
		return (*ServerAuthenticationMD5Password)(ServerParsed(m)), true
	case *pgproto3.AuthenticationOk:
		return (*ServerAuthenticationOk)(ServerParsed(m)), true
	case *pgproto3.AuthenticationSASL:
		return (*ServerAuthenticationSASL)(ServerParsed(m)), true
	case *pgproto3.AuthenticationSASLContinue:
		return (*ServerAuthenticationSASLContinue)(ServerParsed(m)), true
	case *pgproto3.AuthenticationSASLFinal:
		return (*ServerAuthenticationSASLFinal)(ServerParsed(m)), true
	case *pgproto3.BackendKeyData:
		return (*ServerBackendKeyData)(ServerParsed(m)), true
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
	// TODO: default?
}

func (h ServerMessageHandlers[T]) HandleDefault(msg ServerMessage, defaultHandler func(msg ServerMessage) (T, error)) (r T, err error) {
	if defaultHandler == nil {
		return h.Handle(msg)
	}

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
