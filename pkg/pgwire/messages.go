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
	if m, ok := ToClientCancel(msg); ok {
		return m, true
	} else if m, ok := ToClientCopy(msg); ok {
		return m, true
	} else if m, ok := ToClientSimpleQuery(msg); ok {
		return m, true
	} else if m, ok := ToClientExtendedQuery(msg); ok {
		return m, true
	} else if m, ok := ToClientTerminateConn(msg); ok {
		return m, true
	} else if m, ok := ToClientStartup(msg); ok {
		return m, true
	}
	return nil, false
}

func ToServerMessage(msg pgproto3.BackendMessage) (ServerMessage, bool) {
	if m, ok := ToServerAsync(msg); ok {
		return m, true
	} else if m, ok := ToServerCopy(msg); ok {
		return m, true
	} else if m, ok := ToServerExtendedQuery(msg); ok {
		return m, true
	} else if m, ok := ToServerResponse(msg); ok {
		return m, true
	} else if m, ok := ToServerStartup(msg); ok {
		return m, true
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
