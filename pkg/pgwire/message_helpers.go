package pgwire

// This file contains manually-added helper methods for message types
// that provide fast-path access to specific fields without full parsing.

// SourceProvider is an interface for message types that can provide their
// underlying RawMessageSource for zero-copy forwarding.
type SourceProvider interface {
	Source() RawMessageSource
}

// GetRawBody extracts a RawBody from a ServerMessage if the message has
// raw source bytes available. Returns zero RawBody if not available.
// This enables zero-copy forwarding of messages.
func GetRawBody(msg ServerMessage) RawBody {
	if sp, ok := msg.(SourceProvider); ok {
		if src := sp.Source(); src != nil {
			return RawBody{
				Type: src.MessageType(),
				Body: src.MessageBody(),
			}
		}
	}
	return RawBody{}
}

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
func (m ServerResponseReadyForQuery) TxStatusByte() byte {
	if m.isParsed {
		return m.parsed.TxStatus
	}
	if m.source != nil {
		body := m.source.MessageBody()
		if len(body) >= 1 {
			return body[0]
		}
	}
	return 'I' // Default to idle
}
