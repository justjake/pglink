package pgwire

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
	IgnoringMessagesUntilSync bool
	// See CopyMode above.
	CopyMode CopyMode

	// TODO: track prepared statements
	// TODO: track portal names
}
