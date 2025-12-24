package pgwire

import (
	"encoding/binary"
)

// This file contains hand-written fast accessors for performance-critical fields.
// These methods read directly from raw wire bytes without full message parsing.
//
// Wire protocol reference: https://www.postgresql.org/docs/current/protocol-message-formats.html

// =============================================================================
// ReadyForQuery ('Z') - 1 byte body
// =============================================================================

// TxStatusByte returns the transaction status byte directly from raw bytes.
// Returns 'I' (idle), 'T' (in transaction), or 'E' (failed transaction).
// This is a fast path that avoids parsing the full message.
func (r ServerResponseReadyForQuery) TxStatusByte() byte {
	if r.IsParsed() {
		return r.Parse().TxStatus
	}
	if len(r.Body()) >= 1 {
		return r.Body()[0]
	}
	return 0
}

// =============================================================================
// DataRow ('D') - 2-byte column count + column data
// =============================================================================

// ColumnCount returns the number of columns in this row directly from raw bytes.
// Wire format: Int16 (column count) followed by column values.
// This is a fast path that avoids parsing the full message and allocating [][]byte.
func (d ServerResponseDataRow) ColumnCount() int16 {
	if d.IsParsed() {
		return int16(len(d.Parse().Values))
	}
	if len(d.Body()) >= 2 {
		return int16(binary.BigEndian.Uint16(d.Body()[0:2]))
	}
	return 0
}

// BodySize returns the size of the message body in bytes.
// Useful for metrics without parsing the actual column data.
func (d ServerResponseDataRow) BodySize() int {
	if d.IsParsed() {
		// If created from parsed, we need to estimate size
		// 2 bytes for column count + each column's data
		row := d.Parse()
		size := 2 // Int16 for column count
		for _, v := range row.Values {
			size += 4 // Int32 for length
			if v != nil {
				size += len(v)
			}
		}
		return size
	}
	return len(d.Body())
}

// =============================================================================
// CopyData ('d') - raw data bytes
// =============================================================================

// DataBytes returns the raw COPY data bytes directly.
// This is optimal for forwarding - no parsing needed.
func (c ServerCopyCopyData) DataBytes() []byte {
	if c.IsParsed() {
		return c.Parse().Data
	}
	return c.Body()
}

// DataSize returns the size of the COPY data in bytes.
func (c ServerCopyCopyData) DataSize() int {
	if c.IsParsed() {
		return len(c.Parse().Data)
	}
	return len(c.Body())
}

// ClientCopyCopyData accessors

// DataBytes returns the raw COPY data bytes directly.
func (c ClientCopyCopyData) DataBytes() []byte {
	if c.IsParsed() {
		return c.Parse().Data
	}
	return c.Body()
}

// DataSize returns the size of the COPY data in bytes.
func (c ClientCopyCopyData) DataSize() int {
	if c.IsParsed() {
		return len(c.Parse().Data)
	}
	return len(c.Body())
}

// =============================================================================
// CommandComplete ('C') - null-terminated command tag
// =============================================================================

// CommandTagBytes returns the command tag as raw bytes (including null terminator).
// For most use cases, Parse().CommandTag is fine, but this avoids the string conversion.
func (c ServerResponseCommandComplete) CommandTagBytes() []byte {
	if c.IsParsed() {
		return c.Parse().CommandTag
	}
	return c.Body()
}

// =============================================================================
// CopyInResponse ('G') / CopyOutResponse ('H') - format info
// =============================================================================

// Format returns the overall COPY format: 0 for text, 1 for binary.
// Wire format: Int8 (format) followed by column format codes.
func (c ServerCopyCopyInResponse) Format() byte {
	if c.IsParsed() {
		return byte(c.Parse().OverallFormat)
	}
	if len(c.Body()) >= 1 {
		return c.Body()[0]
	}
	return 0
}

// Format returns the overall COPY format: 0 for text, 1 for binary.
func (c ServerCopyCopyOutResponse) Format() byte {
	if c.IsParsed() {
		return byte(c.Parse().OverallFormat)
	}
	if len(c.Body()) >= 1 {
		return c.Body()[0]
	}
	return 0
}

// =============================================================================
// RowDescription ('T') - column metadata
// =============================================================================

// FieldCount returns the number of fields in the row description.
// Wire format: Int16 (field count) followed by field descriptors.
func (r ServerExtendedQueryRowDescription) FieldCount() int16 {
	if r.IsParsed() {
		return int16(len(r.Parse().Fields))
	}
	if len(r.Body()) >= 2 {
		return int16(binary.BigEndian.Uint16(r.Body()[0:2]))
	}
	return 0
}

// =============================================================================
// ParameterDescription ('t') - parameter type OIDs
// =============================================================================

// ParameterCount returns the number of parameters.
// Wire format: Int16 (parameter count) followed by OIDs.
func (p ServerExtendedQueryParameterDescription) ParameterCount() int16 {
	if p.IsParsed() {
		return int16(len(p.Parse().ParameterOIDs))
	}
	if len(p.Body()) >= 2 {
		return int16(binary.BigEndian.Uint16(p.Body()[0:2]))
	}
	return 0
}

// =============================================================================
// Query ('Q') - simple query string
// =============================================================================

// QueryBytes returns the query string as raw bytes (including null terminator).
// For forwarding, this avoids string allocation.
func (q ClientSimpleQueryQuery) QueryBytes() []byte {
	if q.IsParsed() {
		return []byte(q.Parse().String)
	}
	return q.Body()
}

// =============================================================================
// Parse ('P') - prepared statement
// =============================================================================

// StatementNameBytes returns the statement name directly from raw bytes.
// The name is null-terminated at the start of the body.
// Returns nil if the raw body is not available.
func (p ClientExtendedQueryParse) StatementNameBytes() []byte {
	if p.IsParsed() {
		return []byte(p.Parse().Name)
	}
	// Find the first null byte (end of name)
	body := p.Body()
	for i, b := range body {
		if b == 0 {
			return body[:i]
		}
	}
	return nil
}

// =============================================================================
// Bind ('B') - portal binding
// =============================================================================

// PortalNameBytes returns the destination portal name directly from raw bytes.
// The portal name is null-terminated at the start of the body.
func (b ClientExtendedQueryBind) PortalNameBytes() []byte {
	if b.IsParsed() {
		return []byte(b.Parse().DestinationPortal)
	}
	// Find the first null byte (end of portal name)
	body := b.Body()
	for i, c := range body {
		if c == 0 {
			return body[:i]
		}
	}
	return nil
}

// StatementNameBytes returns the prepared statement name directly from raw bytes.
// The statement name follows the portal name (after first null byte).
func (b ClientExtendedQueryBind) StatementNameBytes() []byte {
	if b.IsParsed() {
		return []byte(b.Parse().PreparedStatement)
	}
	// Find first null (end of portal name), then find second null (end of statement name)
	body := b.Body()
	start := -1
	for i, c := range body {
		if c == 0 {
			if start == -1 {
				start = i + 1
			} else {
				return body[start:i]
			}
		}
	}
	return nil
}

// =============================================================================
// Execute ('E') - portal execution
// =============================================================================

// PortalNameBytes returns the portal name directly from raw bytes.
func (e ClientExtendedQueryExecute) PortalNameBytes() []byte {
	if e.IsParsed() {
		return []byte(e.Parse().Portal)
	}
	// Find the first null byte (end of portal name)
	body := e.Body()
	for i, c := range body {
		if c == 0 {
			return body[:i]
		}
	}
	return nil
}

// MaxRows returns the maximum number of rows to return (0 = unlimited).
// Wire format: null-terminated portal name followed by Int32 max rows.
func (e ClientExtendedQueryExecute) MaxRows() int32 {
	if e.IsParsed() {
		return int32(e.Parse().MaxRows)
	}
	// Find null terminator, then read 4 bytes
	body := e.Body()
	for i, c := range body {
		if c == 0 && len(body) >= i+5 {
			return int32(binary.BigEndian.Uint32(body[i+1 : i+5]))
		}
	}
	return 0
}

// =============================================================================
// Describe ('D') - describe statement/portal
// =============================================================================

// ObjectType returns 'S' for statement or 'P' for portal.
func (d ClientExtendedQueryDescribe) ObjectType() byte {
	if d.IsParsed() {
		return d.Parse().ObjectType
	}
	body := d.Body()
	if len(body) >= 1 {
		return body[0]
	}
	return 0
}

// ObjectNameBytes returns the name of the object to describe.
func (d ClientExtendedQueryDescribe) ObjectNameBytes() []byte {
	if d.IsParsed() {
		return []byte(d.Parse().Name)
	}
	body := d.Body()
	if len(body) >= 2 {
		// Name starts after the object type byte, null-terminated
		for i := 1; i < len(body); i++ {
			if body[i] == 0 {
				return body[1:i]
			}
		}
	}
	return nil
}

// =============================================================================
// Close ('C') - close statement/portal
// =============================================================================

// ObjectType returns 'S' for statement or 'P' for portal.
func (c ClientExtendedQueryClose) ObjectType() byte {
	if c.IsParsed() {
		return c.Parse().ObjectType
	}
	body := c.Body()
	if len(body) >= 1 {
		return body[0]
	}
	return 0
}

// ObjectNameBytes returns the name of the object to close.
func (c ClientExtendedQueryClose) ObjectNameBytes() []byte {
	if c.IsParsed() {
		return []byte(c.Parse().Name)
	}
	body := c.Body()
	if len(body) >= 2 {
		// Name starts after the object type byte, null-terminated
		for i := 1; i < len(body); i++ {
			if body[i] == 0 {
				return body[1:i]
			}
		}
	}
	return nil
}
