package pgwire

// This file contains template functions used by the code generator (generate.go).
// These functions define which pgproto3 message types belong to each category.
// The generator reads these switch statements to produce type-safe wrapper types
// and conversion functions (e.g., ToClientSimpleQuery, ClientSimpleQueryQuery).
//
// These functions are intentionally private (lowercase) because they are only
// used as templates for code generation. They are tested by unit tests to ensure
// the categorization is correct, but they are not called from production code.
//
// To regenerate the wrapper types after modifying these templates:
//   go generate ./pkg/pgwire/...

import (
	"github.com/jackc/pgx/v5/pgproto3"
)

// Client message templates

//go:generate go run ./generate.go -return=PgwireMessage=pgproto3.Message=t.T -fn isStartupModeMessage -from=Client -type=Startup -return=Client=pgproto3.FrontendMessage=t.T
func isStartupModeMessage(msg pgproto3.FrontendMessage) bool {
	switch msg.(type) {
	case *pgproto3.GSSEncRequest:
		return true
	case *pgproto3.GSSResponse:
		return true
	case *pgproto3.PasswordMessage:
		return true
	case *pgproto3.SASLInitialResponse:
		return true
	case *pgproto3.SASLResponse:
		return true
	case *pgproto3.SSLRequest:
		return true
	case *pgproto3.StartupMessage:
		return true
	}
	return false
}

//go:generate go run ./generate.go -return=PgwireMessage=pgproto3.Message=t.T -fn isSimpleQueryModeMessage -from=Client -type=SimpleQuery -return=Client=pgproto3.FrontendMessage=t.T
func isSimpleQueryModeMessage(msg pgproto3.FrontendMessage) bool {
	switch msg.(type) {
	case *pgproto3.Query:
		// Simple query.
		// Destroys unnamed prepared statement & portal.
		return true
	case *pgproto3.FunctionCall:
		// Call a function; seems to work like a simple query? Or maybe it works with both modes?
		return true
	}
	return false
}

//go:generate go run ./generate.go -return=PgwireMessage=pgproto3.Message=t.T -fn isExtendedQueryModeMessage -from=Client -type=ExtendedQuery -return=Client=pgproto3.FrontendMessage=t.T
func isExtendedQueryModeMessage(msg pgproto3.FrontendMessage) bool {
	switch msg.(type) {
	// Extended Query flow:
	case *pgproto3.Parse:
		// Extended Query 1: parse text into a prepared statement.
		return true
	case *pgproto3.Bind:
		// Extended Query 2: Bind parameters to a prepared statement.
		return true
	case *pgproto3.Execute:
		// Extended Query 3: Execute a prepared statement, requesting N or all rows.
		// May need to be called again if server replies PortalSuspended.
		// Execute phase is always terminated by the appearance of exactly one of
		// these messages:
		// - PortalSuspended: execute ended before completion, call Execute again.
		// - CommandComplete: success
		// - ErrorResponse: failure
		// - EmptyQueryResponse: the portal was created from an empty query string
		return true
	case *pgproto3.Sync:
		// Extended Query 4: Command pipeline complete.
		//
		// Causes the backend to close the current transaction if it's not inside a
		// BEGIN/COMMIT transaction block ("close" meaning to commit if no error, or
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
		return true

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
		return true
	case *pgproto3.Close:
		// Close prepared statement/portal.
		// Note that closing a prepared statement implicitly closes any open
		// portals that were constructed from that statement.
		return true
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

//go:generate go run ./generate.go -return=PgwireMessage=pgproto3.Message=t.T -fn isCopyModeMessage -from=Client -type=Copy -return=Client=pgproto3.FrontendMessage=t.T
func isCopyModeMessage(msg pgproto3.FrontendMessage) bool {
	switch msg.(type) {
	case *pgproto3.CopyData:
		return true
	case *pgproto3.CopyDone:
		return true
	case *pgproto3.CopyFail:
		return true
	}
	return false
}

//go:generate go run ./generate.go -return=PgwireMessage=pgproto3.Message=t.T -fn isCancelMessage -from=Client -type=Cancel -return=Client=pgproto3.FrontendMessage=t.T
func isCancelMessage(msg pgproto3.FrontendMessage) bool {
	switch msg.(type) {
	case *pgproto3.CancelRequest:
		return true
	}
	return false
}

//go:generate go run ./generate.go -return=PgwireMessage=pgproto3.Message=t.T -fn=isTerminateConnMessage -from=Client -type=TerminateConn -return=Client=pgproto3.FrontendMessage=t.T
func isTerminateConnMessage(msg pgproto3.FrontendMessage) bool {
	switch msg.(type) {
	case *pgproto3.Terminate:
		return true
	}
	return false
}

// Server message templates

//go:generate go run ./generate.go -return=PgwireMessage=pgproto3.Message=t.T -fn isBackendStartupModeMessage -from=Server -type=Startup -return=Server=pgproto3.BackendMessage=t.T
func isBackendStartupModeMessage(msg pgproto3.BackendMessage) bool {
	switch msg.(type) {
	case *pgproto3.AuthenticationCleartextPassword:
		return true
	case *pgproto3.AuthenticationGSS:
		return true
	case *pgproto3.AuthenticationGSSContinue:
		return true
	case *pgproto3.AuthenticationMD5Password:
		return true
	case *pgproto3.AuthenticationOk:
		return true
	case *pgproto3.AuthenticationSASL:
		return true
	case *pgproto3.AuthenticationSASLContinue:
		return true
	case *pgproto3.AuthenticationSASLFinal:
		return true
	case *pgproto3.BackendKeyData:
		// Secret key data for cancel requests.
		// This should be already captured when we establish the connection.
		return true
	}
	return false
}

//go:generate go run ./generate.go -return=PgwireMessage=pgproto3.Message=t.T -fn isBackendExtendedQueryModeMessage -from=Server -type=ExtendedQuery -return=Server=pgproto3.BackendMessage=t.T
func isBackendExtendedQueryModeMessage(msg pgproto3.BackendMessage) bool {
	switch msg.(type) {
	// Extended Query mode:
	case *pgproto3.ParseComplete:
		// Response to Parse.
		return true
	case *pgproto3.BindComplete:
		// Response to Bind.
		return true
	case *pgproto3.ParameterDescription:
		// Response to Describe of prepared statement.
		return true
	case *pgproto3.RowDescription:
		// Response to Describe of portal or statement that returns data.
		return true
	case *pgproto3.NoData:
		// Response to Describe of portal or statement that doesn't return data.
		return true
	case *pgproto3.PortalSuspended:
		// Response to Execute if there are additional rows / the execute did not
		// complete during the Execute call, the client should call Execute again.
		return true
	case *pgproto3.CloseComplete:
		// Response to Close of prepared statement or portal.
		return true
	}
	return false
}

//go:generate go run ./generate.go -return=PgwireMessage=pgproto3.Message=t.T -fn isBackendCopyModeMessage -from=Server -type=Copy -return=Server=pgproto3.BackendMessage=t.T
func isBackendCopyModeMessage(msg pgproto3.BackendMessage) bool {
	switch msg.(type) {
	case *pgproto3.CopyInResponse:
		// Response to COPY FROM STDIN.
		// Backend ready to copy data from client to server.
		// Starts CopyIn mode.
		return true
	case *pgproto3.CopyOutResponse:
		// Response to COPY TO STDOUT.
		// Backend ready to copy data from server to client.
		// Starts CopyOut mode.
		return true
	case *pgproto3.CopyBothResponse:
		// Response to Replication.
		return true
	case *pgproto3.CopyData:
		// Copy Mode: data row.
		return true
	case *pgproto3.CopyDone:
		// Copy Mode: copy completed.
		return true
	}
	return false
}

// isBackendResponseMessage matches messages that apply to both query modes.
//
//go:generate go run ./generate.go -return=PgwireMessage=pgproto3.Message=t.T -fn isBackendResponseMessage -from=Server -type=Response -return=Server=pgproto3.BackendMessage=t.T
func isBackendResponseMessage(msg pgproto3.BackendMessage) bool {
	switch msg.(type) {
	case *pgproto3.ReadyForQuery:
		// Start-up completed.
		// Simple Query mode: ready for next query.
		// Extended Query mode: response to Sync; backend no longer ignoring messages, ready for next command.
		return true
	case *pgproto3.CommandComplete:
		// SQL command completed normally.
		return true
	case *pgproto3.DataRow:
		// Query results (both query modes)
		return true
	case *pgproto3.EmptyQueryResponse:
		// Response to empty query.
		return true
	case *pgproto3.ErrorResponse:
		// Error response.
		return true
	case *pgproto3.FunctionCallResponse:
		// Response to function call.
		return true
	}
	return false
}

//go:generate go run ./generate.go -return=PgwireMessage=pgproto3.Message=t.T -fn isBackendAsyncMessage -from=Server -type=Async -return=Server=pgproto3.BackendMessage=t.T
func isBackendAsyncMessage(msg pgproto3.BackendMessage) bool {
	switch msg.(type) {
	case *pgproto3.NoticeResponse:
		// Warning message.
		return true
	case *pgproto3.NotificationResponse:
		// LISTEN/NOTIFY notification.
		return true
	case *pgproto3.ParameterStatus:
		// Informs client that runtime parameter value changed.
		return true
	}
	return false
}
