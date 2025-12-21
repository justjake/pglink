package backend

import (
	"fmt"

	"github.com/justjake/pglink/pkg/params"
)

type TxStatus byte

const (
	TxIdle          TxStatus = 'I'
	TxActive        TxStatus = 'A'
	TxInTransaction TxStatus = 'T'
	TxFailed        TxStatus = 'E'
)

type CopyMode int8

const (
	CopyNone CopyMode = iota
	// COPY FROM STDIN (copy from client to server)
	//
	// Copy-in mode (data transfer to the server) is initiated when the backend
	// executes a COPY FROM STDIN SQL statement. The backend sends a
	// CopyInResponse message to the frontend. The frontend should then send zero
	// or more CopyData messages, forming a stream of input data. (The message
	// boundaries are not required to have anything to do with row boundaries,
	// although that is often a reasonable choice.) The frontend can terminate the
	// copy-in mode by sending either a CopyDone message (allowing successful
	// termination) or a CopyFail message (which will cause the COPY SQL statement
	// to fail with an error). The backend then reverts to the command-processing
	// mode it was in before the COPY started, which will be either simple or
	// extended query protocol. It will next send either CommandComplete (if
	// successful) or ErrorResponse (if not).
	//
	// The backend will ignore Flush and Sync messages received during copy-in
	// mode. Receipt of any other non-copy message type constitutes an error that
	// will abort the copy-in state as described above. (The exception for Flush
	// and Sync is for the convenience of client libraries that always send Flush
	// or Sync after an Execute message, without checking whether the command to
	// be executed is a COPY FROM STDIN.)
	CopyIn
	// COPY TO STDOUT (copy from server to client)
	// Copy-out mode (data transfer from the server) is initiated when the backend
	// executes a COPY TO STDOUT SQL statement. The backend sends a CopyOutResponse
	// message to the frontend, followed by zero or more CopyData messages (always
	// one per row), followed by CopyDone. The backend then reverts to the
	// command-processing mode it was in before the COPY started, and sends
	// CommandComplete. The frontend cannot abort the transfer (except by closing the
	// connection or issuing a Cancel request), but it can discard unwanted CopyData
	// and CopyDone messages.
	//
	// In the event of a backend-detected error during copy-out mode, the backend
	// will issue an ErrorResponse message and revert to normal processing. The
	// frontend should treat receipt of ErrorResponse as terminating the copy-out
	// mode.
	//
	// It is possible for NoticeResponse and ParameterStatus messages to be
	// interspersed between CopyData messages; frontends must handle these cases,
	// and should be prepared for other asynchronous message types as well (see
	// Section 54.2.7). Otherwise, any message type other than CopyData or
	// CopyDone may be treated as terminating copy-out mode.
	CopyOut
	// START_REPLICATION (copy both ways; walsender replication)
	// Probably not relevant to pglink.
	CopyBoth
)

func (m CopyMode) String() string {
	switch m {
	case CopyNone:
		return "none"
	case CopyIn:
		return "copy-in"
	case CopyOut:
		return "copy-out"
	case CopyBoth:
		return "copy-both"
	default:
		return fmt.Sprintf("unknown copy mode: %d", m)
	}
}

// State of a session in the PostgreSQL wire protocol.
type ServerSession struct {
	// Immutable
	PID             uint32
	SecretCancelKey uint32

	// Dynamic
	TxStatus          TxStatus
	ParameterStatuses params.ParameterStatuses

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
	IgnoringMessagesUntilSync bool
	// See CopyMode above.
	CopyMode CopyMode

	// TODO: track prepared statements
	// TODO: track portal names
}

type Severity string

const (
	// Used in ErrorResponse messages.
	Error      Severity = "ERROR"
	ErrorFatal Severity = "FATAL"
	ErrorPanic Severity = "PANIC"

	// Used in NoticeResponse messages.
	NoticeWarning Severity = "WARNING"
	Notice        Severity = "NOTICE"
	NoticeDebug   Severity = "DEBUG"
	NoticeInfo    Severity = "INFO"
	NoticeLog     Severity = "LOG"
)
