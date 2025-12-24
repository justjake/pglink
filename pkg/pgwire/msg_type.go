package pgwire

// MsgType represents a PostgreSQL wire protocol message type byte.
type MsgType byte

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
