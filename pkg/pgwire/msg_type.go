package pgwire

import (
	"bytes"
	"fmt"
	"unsafe"
)

// MsgType represents a PostgreSQL wire protocol message type byte.
type MsgType byte

func (m MsgType) String() string {
	return fmt.Sprintf("%s (%s)", string(m), MsgName.Get(m))
}

// MsgLookup is a lookup table from MsgType to T.
// It uses [256]T so that indexing by a byte is always in-bounds, allowing
// the compiler to eliminate bounds checks entirely. The ~140 unused entries
// (max message type is 't'=116) cost only a few KB total across all tables.
// Use Get for bounds-safe access that will be inlined by the compiler.
type MsgLookup[T any] [256]T

// Get returns the value for the given message type.
// This method is inlined by the compiler to a single indexed load.
func (t *MsgLookup[T]) Get(m MsgType) T {
	return t[m]
}

// Protocol version and magic numbers for startup messages.
// These appear in the first 4 bytes of the message body (after the length field).
const (
	ProtocolVersion3_0 int32 = 196608   // 3 << 16 | 0
	SSLRequestMagic    int32 = 80877103 // 1234 << 16 | 5679
	CancelRequestMagic int32 = 80877102 // 1234 << 16 | 5678
	GSSENCRequestMagic int32 = 80877104 // 1234 << 16 | 5680
)

// Startup message types (synthetic - these are not actual wire bytes).
// Used by pgstream to identify startup-phase messages.
const (
	MsgStartup       MsgType = 0x00 // StartupMessage (version 3.0)
	MsgSSLRequest    MsgType = 0x01 // SSLRequest
	MsgCancelRequest MsgType = 0x02 // CancelRequest
	MsgGSSENCRequest MsgType = 0x03 // GSSENCRequest
)

// Client (frontend) message types
const (
	MsgClientBind      MsgType = 'B'
	MsgClientClose     MsgType = 'C'
	MsgClientCopyData  MsgType = 'd'
	MsgClientCopyDone  MsgType = 'c'
	MsgClientCopyFail  MsgType = 'f'
	MsgClientDescribe  MsgType = 'D'
	MsgClientExecute   MsgType = 'E'
	MsgClientFlush     MsgType = 'H'
	MsgClientFunc      MsgType = 'F'
	MsgClientParse     MsgType = 'P'
	MsgClientPassword  MsgType = 'p' // Also SASL responses
	MsgClientQuery     MsgType = 'Q'
	MsgClientSync      MsgType = 'S'
	MsgClientTerminate MsgType = 'X'
)

// Server (backend) message types
const (
	MsgServerAuth                 MsgType = 'R'
	MsgServerBackendKeyData       MsgType = 'K'
	MsgServerBindComplete         MsgType = '2'
	MsgServerCloseComplete        MsgType = '3'
	MsgServerCommandComplete      MsgType = 'C'
	MsgServerCopyBothResponse     MsgType = 'W'
	MsgServerCopyData             MsgType = 'd'
	MsgServerCopyDone             MsgType = 'c'
	MsgServerCopyInResponse       MsgType = 'G'
	MsgServerCopyOutResponse      MsgType = 'H'
	MsgServerDataRow              MsgType = 'D'
	MsgServerEmptyQueryResponse   MsgType = 'I'
	MsgServerErrorResponse        MsgType = 'E'
	MsgServerFuncCallResponse     MsgType = 'V'
	MsgServerNoData               MsgType = 'n'
	MsgServerNoticeResponse       MsgType = 'N'
	MsgServerNotificationResponse MsgType = 'A'
	MsgServerParameterDescription MsgType = 't'
	MsgServerParameterStatus      MsgType = 'S'
	MsgServerParseComplete        MsgType = '1'
	MsgServerPortalSuspended      MsgType = 's'
	MsgServerReadyForQuery        MsgType = 'Z'
	MsgServerRowDescription       MsgType = 'T'
)

// MsgIsClient indicates whether a message type can be sent by the client (frontend).
var MsgIsClient = MsgLookup[bool]{
	'B': true, // Bind
	'C': true, // Close
	'c': true, // CopyDone
	'd': true, // CopyData
	'D': true, // Describe
	'E': true, // Execute
	'f': true, // CopyFail
	'F': true, // FunctionCall
	'H': true, // Flush
	'P': true, // Parse
	'p': true, // PasswordMessage / SASL
	'Q': true, // Query
	'S': true, // Sync
	'X': true, // Terminate
}

// MsgIsServer indicates whether a message type can be sent by the server (backend).
var MsgIsServer = MsgLookup[bool]{
	'1': true, // ParseComplete
	'2': true, // BindComplete
	'3': true, // CloseComplete
	'A': true, // NotificationResponse
	'c': true, // CopyDone
	'C': true, // CommandComplete
	'd': true, // CopyData
	'D': true, // DataRow
	'E': true, // ErrorResponse
	'G': true, // CopyInResponse
	'H': true, // CopyOutResponse
	'I': true, // EmptyQueryResponse
	'K': true, // BackendKeyData
	'n': true, // NoData
	'N': true, // NoticeResponse
	'R': true, // Authentication
	'S': true, // ParameterStatus
	's': true, // PortalSuspended
	't': true, // ParameterDescription
	'T': true, // RowDescription
	'V': true, // FunctionCallResponse
	'W': true, // CopyBothResponse
	'Z': true, // ReadyForQuery
}

// MsgIsStartup indicates whether a message type is part of the startup/auth phase.
// These messages are only valid before the connection is fully established.
var MsgIsStartup = MsgLookup[bool]{
	// Server startup messages
	'R': true, // Authentication (all variants)
	'K': true, // BackendKeyData
	'S': true, // ParameterStatus (sent during startup)
	'Z': true, // ReadyForQuery (marks end of startup)
	'E': true, // ErrorResponse (can occur during startup)
	'N': true, // NoticeResponse (can occur during startup)

	// Client startup messages (after StartupMessage which has no type byte)
	'p': true, // PasswordMessage / SASLInitialResponse / SASLResponse
}

// MsgName returns a human-readable name for the message type.
var MsgName = MsgLookup[string]{
	// Startup messages (synthetic types)
	0x00: "StartupMessage",
	0x01: "SSLRequest",
	0x02: "CancelRequest",
	0x03: "GSSENCRequest",

	// Client messages
	'B': "Bind",
	'C': "Close/CommandComplete",
	'c': "CopyDone",
	'd': "CopyData",
	'D': "Describe/DataRow",
	'E': "Execute/ErrorResponse",
	'f': "CopyFail",
	'F': "FunctionCall",
	'H': "Flush/CopyOutResponse",
	'P': "Parse",
	'p': "PasswordMessage",
	'Q': "Query",
	'S': "Sync/ParameterStatus",
	'X': "Terminate",

	// Server-only messages
	'1': "ParseComplete",
	'2': "BindComplete",
	'3': "CloseComplete",
	'A': "NotificationResponse",
	'G': "CopyInResponse",
	'I': "EmptyQueryResponse",
	'K': "BackendKeyData",
	'n': "NoData",
	'N': "NoticeResponse",
	'R': "Authentication",
	's': "PortalSuspended",
	't': "ParameterDescription",
	'T': "RowDescription",
	'V': "FunctionCallResponse",
	'W': "CopyBothResponse",
	'Z': "ReadyForQuery",
}

type MsgTypeSet []MsgType

func (s MsgTypeSet) Contains(msg MsgType) bool {
	return MsgTypeIndex(s, msg) != -1
}

// MsgTerminalResponse returns the terminal response messages for the given request message type,
// which indicate the end of the request.
var MsgTerminalResponse = MsgLookup[MsgTypeSet]{
	MsgClientQuery:    {MsgServerReadyForQuery},
	MsgClientParse:    {MsgServerParseComplete, MsgServerErrorResponse},
	MsgClientBind:     {MsgServerBindComplete, MsgServerErrorResponse},
	MsgClientClose:    {MsgServerCloseComplete, MsgServerErrorResponse},
	MsgClientDescribe: {MsgServerRowDescription, MsgServerNoData, MsgServerErrorResponse},
	MsgClientExecute:  {MsgServerCommandComplete, MsgServerEmptyQueryResponse, MsgServerErrorResponse, MsgServerPortalSuspended},
	MsgClientFunc:     {MsgServerReadyForQuery},
	MsgClientSync:     {MsgServerReadyForQuery},
}

var MsgResponse = MsgLookup[MsgTypeSet]{
	// https://www.postgresql.org/docs/current/protocol-flow.html#PROTOCOL-FLOW-SIMPLE-QUERY
	// Should not complete until ReadyForQuery.
	MsgClientQuery: {
		// One (of possibly many) An SQL command completed normally.
		MsgServerCommandComplete,

		// Indicates copy mode began (sent before CommandComplete)
		MsgServerCopyInResponse,
		MsgServerCopyOutResponse,
		MsgServerCopyBothResponse,

		// Indicates that rows are about to be returned in response to a SELECT,
		// FETCH, etc. query. The contents of this message describe the column
		// layout of the rows. This will be followed by a DataRow message for each
		// row being returned to the frontend.
		MsgServerRowDescription,
		MsgServerDataRow,

		// Query SQL was empty string.
		MsgServerEmptyQueryResponse,
		// An error has occurred.
		MsgServerErrorResponse,
		// A warning message has been issued in relation to the query. Notices are
		// in addition to other responses, i.e., the backend will continue
		// processing the command.
		MsgServerNoticeResponse,

		// Processing of the query string is complete. A separate message is sent to
		// indicate this because the query string might contain multiple SQL
		// commands. (CommandComplete marks the end of processing one SQL command,
		// not the whole string.) ReadyForQuery will always be sent, whether
		// processing terminates successfully or with an error.
		MsgServerReadyForQuery, // terminal
	},
	MsgClientParse: MsgTerminalResponse[MsgClientParse],
	MsgClientBind:  MsgTerminalResponse[MsgClientBind],
	MsgClientClose: MsgTerminalResponse[MsgClientClose],
	// Describe statement or portal.
	MsgClientDescribe: {
		// Portal -> RowDescription message describing the rows that will be
		// returned by executing the portal; or a NoData message if the portal does
		// not contain a query that will return rows; or ErrorResponse if there is
		// no such portal
		MsgServerRowDescription, // terminal
		MsgServerNoData,         // terminal
		// ParameterDescription message describing the parameters needed by the
		// statement, followed by a RowDescription message describing the rows that
		// will be returned when the statement is eventually executed (or a NoData
		// message if the statement will not return rows)
		MsgServerParameterDescription,
		// No such statement or portal.
		MsgServerErrorResponse, // terminal
	},
	// https://www.postgresql.org/docs/current/protocol-flow.html#PROTOCOL-FLOW-EXT-QUERY
	// Specifically: https://arc.net/l/quote/xubsvspn
	MsgClientExecute: {
		// an Execute phase is always terminated by the appearance of exactly one of
		// these messages: CommandComplete, EmptyQueryResponse (if the portal was
		// created from an empty query string), ErrorResponse, or PortalSuspended.
		MsgServerCommandComplete, MsgServerEmptyQueryResponse, MsgServerErrorResponse, MsgServerPortalSuspended, // terminal
		// We also listen for Copy messages.
		// Execute handlers should not complete their flow on these messages.
		MsgServerCopyInResponse, MsgServerCopyOutResponse, MsgServerCopyBothResponse,
	},
	// https://www.postgresql.org/docs/current/protocol-flow.html#PROTOCOL-FLOW-FUNCTION-CALL
	MsgClientFunc: {
		MsgServerFuncCallResponse,
		MsgServerErrorResponse,
		MsgServerNoticeResponse,
		MsgServerReadyForQuery, // terminal

		// It's not well-documented, but we assume a function call could start a COPY query.
		MsgServerCopyInResponse, MsgServerCopyOutResponse, MsgServerCopyBothResponse,
	},
	// At completion of each series of extended-query messages, the frontend
	// should issue a Sync message. This parameterless message causes the backend
	// to close the current transaction if it's not inside a BEGIN/COMMIT
	// transaction block (“close” meaning to commit if no error, or roll back if
	// error). Then a ReadyForQuery response is issued. The purpose of Sync is to
	// provide a resynchronization point for error recovery. When an error is
	// detected while processing any extended-query message, the backend issues
	// ErrorResponse, then reads and discards messages until a Sync is reached,
	// then issues ReadyForQuery and returns to normal message processing. (But
	// note that no skipping occurs if an error is detected while processing Sync
	// — this ensures that there is one and only one ReadyForQuery sent for each
	// Sync.)
	MsgClientSync: MsgTerminalResponse[MsgClientSync],
}

func MsgTypeIndex(slice []MsgType, msg MsgType) int {
	if len(slice) == 0 {
		return -1
	}
	byteSlice := unsafe.Slice((*byte)(unsafe.SliceData(slice)), len(slice))
	return bytes.IndexByte(byteSlice, byte(msg))
}
