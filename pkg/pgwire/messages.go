package pgwire

import (
	"fmt"

	"github.com/jackc/pgx/v5/pgproto3"
)

type Message interface {
	PgwireMessage()
}

// FromFrontend wraps a message that came from the frontend.
// Used if message is both Frontend and Backend, to distinguish its origin at runtime.
type FromClient[T pgproto3.FrontendMessage] struct {
	T T
}

type ClientMessage interface {
	Message
	Client()
}

// FromBackend wraps a message that came from the backend.
// Used if message is both Frontend and Backend, to distinguish its origin at runtime.
type FromServer[T pgproto3.BackendMessage] struct {
	T T
}

type ServerMessage interface {
	Message
	Server()
}

//go:generate go run ./generate.go -methods=PgwireMessage -fn IsStartupModeMessage -from=Client -type=Startup
func IsStartupModeMessage(msg pgproto3.FrontendMessage) bool {
	switch msg.(type) {
	case *pgproto3.GSSEncRequest:
	case *pgproto3.GSSResponse:
	case *pgproto3.PasswordMessage:
	case *pgproto3.SASLInitialResponse:
	case *pgproto3.SASLResponse:
	case *pgproto3.SSLRequest:
	case *pgproto3.StartupMessage:
		return true
	}
	return false
}

//go:generate go run ./generate.go -methods=PgwireMessage -fn IsSimpleQueryModeMessage -from=Client -type=SimpleQuery
func IsSimpleQueryModeMessage(msg pgproto3.FrontendMessage) bool {
	switch msg.(type) {
	case *pgproto3.Query:
		// Simple query.
		// Destroys unnamed prepared statement & portal.
	case *pgproto3.FunctionCall:
		// Call a function; seems to work like a simple query? Or maybe it works with both modes?
		return true
	}
	return false
}

//go:generate go run ./generate.go -methods=PgwireMessage -fn IsExtendedQueryModeMessage -from=Client -type=ExtendedQuery
func IsExtendedQueryModeMessage(msg pgproto3.FrontendMessage) bool {
	switch msg.(type) {
	// Extended Query flow:
	case *pgproto3.Parse:
		// Extended Query 1: parse text into a prepared statement.
	case *pgproto3.Bind:
		// Extended Query 2: Bind parameters to a prepared statement.
	case *pgproto3.Execute:
		// Extended Query 3: Execute a prepared statement, requesting N or all rows.
		// May need to be called again if server replies PortalSuspended.
		// Execute phase is always terminated by the appearance of exactly one of
		// these messages:
		// - PortalSuspended: execute ended before completion, call Execute again.
		// - CommandComplete: success
		// - ErrorResponse: failure
		// - EmptyQueryResponse: the portal was created from an empty query string
	case *pgproto3.Sync:
		// Extended Query 4: Command pipeline complete.
		//
		// Causes the backend to close the current transaction if it's not inside a
		// BEGIN/COMMIT transaction block (“close” meaning to commit if no error, or
		// roll back if error).
		// then, a ReadyForQuery response is issued.
		//
		// The purpose of Sync is to provide a resynchronization point for error
		// recovery. When an error is detected while processing any extended-query
		// message, the backend issues ErrorResponse, then reads and discards
		// messages until a Sync is reached, then issues ReadyForQuery and returns
		// to normal message processing. (But note that no skipping occurs if an
		// error is detected while processing Sync — this ensures that there is
		// one and only one ReadyForQuery sent for each Sync.)

	// In addition to these fundamental, required operations, there are several
	// optional operations that can be used with extended-query protocol.
	case *pgproto3.Describe:
		// Extended Query tool: Describe prepared statement or portal.
		//
		// The Describe message (portal variant) specifies the name of an existing
		// portal (or an empty string for the unnamed portal). The response is a
		// RowDescription message describing the rows that will be returned by
		// executing the portal; or a NoData message if the portal does not
		// contain a query that will return rows; or ErrorResponse if there is no
		// such portal.
		//
		// The Describe message (statement variant) specifies the name of an
		// existing prepared statement (or an empty string for the unnamed
		// prepared statement). The response is a ParameterDescription message
		// describing the parameters needed by the statement, followed by a
		// RowDescription message describing the rows that will be returned when
		// the statement is eventually executed (or a NoData message if the
		// statement will not return rows). ErrorResponse is issued if there is no
		// such prepared statement. Note that since Bind has not yet been issued,
		// the formats to be used for returned columns are not yet known to the
		// backend; the format code fields in the RowDescription message will be
		// zeroes in this case.
	case *pgproto3.Close:
		// Close prepared statement/portal.
		// Note that closing a prepared statement implicitly closes any open
		// portals that were constructed from that statement.
	case *pgproto3.Flush:
		// The Flush message does not cause any specific output to be generated,
		// but forces the backend to deliver any data pending in its output
		// buffers. A Flush must be sent after any extended-query command except
		// Sync, if the frontend wishes to examine the results of that command
		// before issuing more commands. Without Flush, messages returned by the
		// backend will be combined into the minimum possible number of packets to
		// minimize network overhead.
		return true
	}
	return false
}

//go:generate go run ./generate.go -methods=PgwireMessage -fn IsCopyModeMessage -from=Client -type=Copy
func IsCopyModeMessage(msg pgproto3.FrontendMessage) bool {
	switch msg.(type) {
	case *pgproto3.CopyData:
	case *pgproto3.CopyDone:
	case *pgproto3.CopyFail:
		return true
	}
	return false
}

//go:generate go run ./generate.go -methods=PgwireMessage -fn IsCancelMessage -from=Client -type=Cancel
func IsCancelMessage(msg pgproto3.FrontendMessage) bool {
	switch msg.(type) {
	case *pgproto3.CancelRequest:
		return true
	}
	return false
}

//go:generate go run ./generate.go -methods=PgwireMessage -fn=IsTerminateConnMessage -from=Client -type=TerminateConn
func IsTerminateConnMessage(msg pgproto3.FrontendMessage) bool {
	switch msg.(type) {
	case *pgproto3.Terminate:
		return true
	}
	return false
}

// Backend Messages

//go:generate go run ./generate.go -methods=PgwireMessage -fn IsBackendStartupModeMessage -from=Server -type=Startup
func IsBackendStartupModeMessage(msg pgproto3.BackendMessage) bool {
	switch msg.(type) {
	case *pgproto3.AuthenticationCleartextPassword:
	case *pgproto3.AuthenticationGSS:
	case *pgproto3.AuthenticationGSSContinue:
	case *pgproto3.AuthenticationMD5Password:
	case *pgproto3.AuthenticationOk:
	case *pgproto3.AuthenticationSASL:
	case *pgproto3.AuthenticationSASLContinue:
	case *pgproto3.AuthenticationSASLFinal:
		return true
	case *pgproto3.BackendKeyData:
		// Secret key data for cancel requests.
		// This should be already captured when we establish the connection.
		return true
	}
	return false
}

//go:generate go run ./generate.go -methods=PgwireMessage -fn IsBackendExtendedQueryModeMessage -from=Server -type=ExtendedQuery
func IsBackendExtendedQueryModeMessage(msg pgproto3.BackendMessage) bool {
	switch msg.(type) {
	// Extended Query mode:
	case *pgproto3.ParseComplete:
		// Response to Parse.
	case *pgproto3.BindComplete:
		// Response to Bind.
	case *pgproto3.ParameterDescription:
		// Response to Describe of prepared statemnt
	case *pgproto3.RowDescription:
		// Response to Describe of portal or statement that returns data.
	case *pgproto3.NoData:
		// Response to Describe of portal or statement that doesn't return data.
	case *pgproto3.PortalSuspended:
		// Response to Execute if there are additional rows / the execute did not
		// complete during the Execute call, the client should call Execute again.
	case *pgproto3.CloseComplete:
		// Response to Close of prepared statement or portal.
		return true
	}
	return false
}

//go:generate go run ./generate.go -methods=PgwireMessage -fn IsBackendCopyModeMessage -from=Server -type=Copy
func IsBackendCopyModeMessage(msg pgproto3.BackendMessage) bool {
	switch msg.(type) {
	case *pgproto3.CopyInResponse:
		// Response to COPY FROM STDIN.
		// Backend ready to copy data from client to server.
		// Starts CopyIn mode.
	case *pgproto3.CopyOutResponse:
		// Response to COPY TO STDOUT.
		// Backend ready to copy data from server to client.
		// Starts CopyOut mode.
	case *pgproto3.CopyBothResponse:
		// Response to Replication.
	case *pgproto3.CopyData:
		// Copy Mode: data row.
	case *pgproto3.CopyDone:
		// Copy Mode: copy completed.
		return true
	}
	return false
}

// Applies to both query modes.
// Not really sure what to call this.
//
//go:generate go run ./generate.go -methods=PgwireMessage -fn IsBackendResponseMessage -from=Server -type=Response
func IsBackendResponseMessage(msg pgproto3.BackendMessage) bool {
	switch msg.(type) {
	case *pgproto3.ReadyForQuery:
		// Start-up completed.
		// Simple Query mode: ready for next query.
		// Extended Query mode: response to Sync; backend no longer ignoring messages, ready for next command.
	case *pgproto3.CommandComplete:
		// SQL command completed normally.
	case *pgproto3.DataRow:
		// Query results (both query modes)
	case *pgproto3.EmptyQueryResponse:
		// Response to empty query.
	case *pgproto3.ErrorResponse:
		// Error response.
	case *pgproto3.FunctionCallResponse:
		// Response to function call.
		return true
	}
	return false
}

//go:generate go run ./generate.go -methods=PgwireMessage -fn IsBackendAsyncMessage -from=Server -type=Async
func IsBackendAsyncMessage(msg pgproto3.BackendMessage) bool {
	switch msg.(type) {
	case *pgproto3.NoticeResponse:
		// Warning message.
	case *pgproto3.NotificationResponse:
		// LISTEN/NOTIFY notification.
	case *pgproto3.ParameterStatus:
		// Informs client that runtime parameter value changed.
		return true
	}
	return false
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

func (h ClientMessageHandlers[T]) Handle(msg ClientMessage) (r T, err error) {
	switch msg := msg.(type) {
	case ClientCancel:
		return h.Cancel(msg)
	case ClientCopy:
		return h.Copy(msg)
	case ClientExtendedQuery:
		return h.ExtendedQuery(msg)
	case ClientSimpleQuery:
		return h.SimpleQuery(msg)
	case ClientStartup:
		return h.Startup(msg)
	case ClientTerminateConn:
		return h.TerminateConn(msg)
	}
	err = fmt.Errorf("unknown client message: %T", msg)
	return
}

type ServerMessageHandlers[T any] struct {
	Async         func(msg ServerAsync) (T, error)
	Copy          func(msg ServerCopy) (T, error)
	ExtendedQuery func(msg ServerExtendedQuery) (T, error)
	Response      func(msg ServerResponse) (T, error)
	Startup       func(msg ServerStartup) (T, error)
}

func (h ServerMessageHandlers[T]) Handle(msg ServerMessage) (r T, err error) {
	switch msg := msg.(type) {
	case ServerAsync:
		return h.Async(msg)
	case ServerCopy:
		return h.Copy(msg)
	case ServerExtendedQuery:
		return h.ExtendedQuery(msg)
	case ServerResponse:
		return h.Response(msg)
	case ServerStartup:
		return h.Startup(msg)
	}
	err = fmt.Errorf("unknown server message: %T", msg)
	return
}

type MessageHandlers[T any] struct {
	Client ClientMessageHandlers[T]
	Server ServerMessageHandlers[T]
}

func (h MessageHandlers[T]) Handle(msg Message) (r T, err error) {
	if m, ok := msg.(ClientMessage); ok {
		return h.Client.Handle(m)
	} else if m, ok := msg.(ServerMessage); ok {
		return h.Server.Handle(m)
	}
	err = fmt.Errorf("unknown message (neither client nor server): %T", msg)
	return
}
