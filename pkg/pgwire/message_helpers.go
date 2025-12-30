package pgwire

// This file contains manually-added helper methods for message types
// that provide fast-path access to specific fields without full parsing.

// DataSize returns the size of the copy data without parsing the full message.
// This allows counting bytes transferred during COPY without allocation.
func (m ClientCopyCopyData) DataSize() int {
	if m.isParsed {
		return len(m.parsed.Data)
	}
	if m.source != nil {
		// Body IS the data for CopyData messages
		return len(m.source.MessageBody())
	}
	return 0
}

// DataSize returns the size of the copy data without parsing the full message.
// This allows counting bytes transferred during COPY without allocation.
func (m ServerCopyCopyData) DataSize() int {
	if m.isParsed {
		return len(m.parsed.Data)
	}
	if m.source != nil {
		// Body IS the data for CopyData messages
		return len(m.source.MessageBody())
	}
	return 0
}

// TxStatusByte returns the transaction status byte without parsing the full message.
// This is a fast path for protocol state tracking.
// The byte values are: 'I' (idle), 'T' (in transaction), 'E' (failed transaction).
func (m ServerResponseReadyForQuery) TxStatus() TxStatus {
	if m.isParsed {
		return TxStatus(m.parsed.TxStatus)
	}
	if m.source != nil {
		body := m.source.MessageBody()
		if len(body) >= 1 {
			return TxStatus(body[0])
		}
	}
	panic("ServerResponseReadyForQuery.TxStatus: no source")
}
