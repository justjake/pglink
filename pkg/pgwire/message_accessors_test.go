package pgwire

import (
	"encoding/binary"
	"testing"

	"github.com/jackc/pgx/v5/pgproto3"
)

func TestReadyForQuery_TxStatusByte(t *testing.T) {
	tests := []struct {
		name     string
		txStatus byte
	}{
		{"idle", 'I'},
		{"in_transaction", 'T'},
		{"failed", 'E'},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Test with parsed message
			parsed := ServerResponseReadyForQuery(NewLazyServerFromParsed(&pgproto3.ReadyForQuery{TxStatus: tt.txStatus}))
			if got := parsed.TxStatusByte(); got != tt.txStatus {
				t.Errorf("TxStatusByte() from parsed = %c, want %c", got, tt.txStatus)
			}

			// Test with raw bytes
			raw := ServerResponseReadyForQuery{LazyServer[*pgproto3.ReadyForQuery]{
				source: RawBody{Type: 'Z', Body: []byte{tt.txStatus}},
			}}
			if got := raw.TxStatusByte(); got != tt.txStatus {
				t.Errorf("TxStatusByte() from raw = %c, want %c", got, tt.txStatus)
			}
		})
	}
}

func TestDataRow_ColumnCount(t *testing.T) {
	tests := []struct {
		name  string
		count int16
	}{
		{"zero_columns", 0},
		{"one_column", 1},
		{"many_columns", 100},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Test with parsed message
			values := make([][]byte, tt.count)
			for i := range values {
				values[i] = []byte("test")
			}
			parsed := ServerResponseDataRow(NewLazyServerFromParsed(&pgproto3.DataRow{Values: values}))
			if got := parsed.ColumnCount(); got != tt.count {
				t.Errorf("ColumnCount() from parsed = %d, want %d", got, tt.count)
			}

			// Test with raw bytes
			body := make([]byte, 2)
			binary.BigEndian.PutUint16(body, uint16(tt.count))
			raw := ServerResponseDataRow{LazyServer[*pgproto3.DataRow]{
				source: RawBody{Type: 'D', Body: body},
			}}
			if got := raw.ColumnCount(); got != tt.count {
				t.Errorf("ColumnCount() from raw = %d, want %d", got, tt.count)
			}
		})
	}
}

func TestDataRow_BodySize(t *testing.T) {
	body := []byte{0, 2, 0, 0, 0, 4, 't', 'e', 's', 't', 0, 0, 0, 3, 'f', 'o', 'o'}
	raw := ServerResponseDataRow{LazyServer[*pgproto3.DataRow]{
		source: RawBody{Type: 'D', Body: body},
	}}
	if got := raw.BodySize(); got != len(body) {
		t.Errorf("BodySize() = %d, want %d", got, len(body))
	}
}

func TestCopyData_DataBytes(t *testing.T) {
	data := []byte("line1\tvalue1\nline2\tvalue2\n")

	// Test server CopyData with parsed
	serverParsed := ServerCopyCopyData(NewLazyServerFromParsed(&pgproto3.CopyData{Data: data}))
	if got := serverParsed.DataBytes(); string(got) != string(data) {
		t.Errorf("ServerCopyCopyData.DataBytes() from parsed = %q, want %q", got, data)
	}

	// Test server CopyData with raw
	serverRaw := ServerCopyCopyData{LazyServer[*pgproto3.CopyData]{
		source: RawBody{Type: 'd', Body: data},
	}}
	if got := serverRaw.DataBytes(); string(got) != string(data) {
		t.Errorf("ServerCopyCopyData.DataBytes() from raw = %q, want %q", got, data)
	}

	// Test client CopyData with parsed
	clientParsed := ClientCopyCopyData(NewLazyClientFromParsed(&pgproto3.CopyData{Data: data}))
	if got := clientParsed.DataBytes(); string(got) != string(data) {
		t.Errorf("ClientCopyCopyData.DataBytes() from parsed = %q, want %q", got, data)
	}

	// Test client CopyData with raw
	clientRaw := ClientCopyCopyData{LazyClient[*pgproto3.CopyData]{
		source: RawBody{Type: 'd', Body: data},
	}}
	if got := clientRaw.DataBytes(); string(got) != string(data) {
		t.Errorf("ClientCopyCopyData.DataBytes() from raw = %q, want %q", got, data)
	}
}

func TestCopyData_DataSize(t *testing.T) {
	data := []byte("test data for copy")
	server := ServerCopyCopyData{LazyServer[*pgproto3.CopyData]{
		source: RawBody{Type: 'd', Body: data},
	}}
	if got := server.DataSize(); got != len(data) {
		t.Errorf("DataSize() = %d, want %d", got, len(data))
	}

	client := ClientCopyCopyData{LazyClient[*pgproto3.CopyData]{
		source: RawBody{Type: 'd', Body: data},
	}}
	if got := client.DataSize(); got != len(data) {
		t.Errorf("DataSize() = %d, want %d", got, len(data))
	}
}

func TestCommandComplete_CommandTagBytes(t *testing.T) {
	tag := []byte("SELECT 100")

	// Test with parsed
	parsed := ServerResponseCommandComplete(NewLazyServerFromParsed(&pgproto3.CommandComplete{CommandTag: tag}))
	if got := parsed.CommandTagBytes(); string(got) != string(tag) {
		t.Errorf("CommandTagBytes() from parsed = %q, want %q", got, tag)
	}

	// Test with raw (includes null terminator in wire format)
	rawBody := append(tag, 0)
	raw := ServerResponseCommandComplete{LazyServer[*pgproto3.CommandComplete]{
		source: RawBody{Type: 'C', Body: rawBody},
	}}
	if got := raw.CommandTagBytes(); string(got) != string(rawBody) {
		t.Errorf("CommandTagBytes() from raw = %q, want %q", got, rawBody)
	}
}

func TestCopyInResponse_Format(t *testing.T) {
	tests := []struct {
		name   string
		format byte
	}{
		{"text", 0},
		{"binary", 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Test with parsed
			parsed := ServerCopyCopyInResponse(NewLazyServerFromParsed(&pgproto3.CopyInResponse{OverallFormat: tt.format}))
			if got := parsed.Format(); got != tt.format {
				t.Errorf("Format() from parsed = %d, want %d", got, tt.format)
			}

			// Test with raw (format byte + column count + column formats)
			rawBody := []byte{tt.format, 0, 0} // format + 0 columns
			raw := ServerCopyCopyInResponse{LazyServer[*pgproto3.CopyInResponse]{
				source: RawBody{Type: 'G', Body: rawBody},
			}}
			if got := raw.Format(); got != tt.format {
				t.Errorf("Format() from raw = %d, want %d", got, tt.format)
			}
		})
	}
}

func TestCopyOutResponse_Format(t *testing.T) {
	// Test with parsed
	parsed := ServerCopyCopyOutResponse(NewLazyServerFromParsed(&pgproto3.CopyOutResponse{OverallFormat: 1}))
	if got := parsed.Format(); got != 1 {
		t.Errorf("Format() from parsed = %d, want 1", got)
	}

	// Test with raw
	raw := ServerCopyCopyOutResponse{LazyServer[*pgproto3.CopyOutResponse]{
		source: RawBody{Type: 'H', Body: []byte{1, 0, 0}},
	}}
	if got := raw.Format(); got != 1 {
		t.Errorf("Format() from raw = %d, want 1", got)
	}
}

func TestRowDescription_FieldCount(t *testing.T) {
	// Test with parsed
	parsed := ServerExtendedQueryRowDescription(NewLazyServerFromParsed(&pgproto3.RowDescription{
		Fields: []pgproto3.FieldDescription{{Name: []byte("col1")}, {Name: []byte("col2")}},
	}))
	if got := parsed.FieldCount(); got != 2 {
		t.Errorf("FieldCount() from parsed = %d, want 2", got)
	}

	// Test with raw
	body := make([]byte, 2)
	binary.BigEndian.PutUint16(body, 5)
	raw := ServerExtendedQueryRowDescription{LazyServer[*pgproto3.RowDescription]{
		source: RawBody{Type: 'T', Body: body},
	}}
	if got := raw.FieldCount(); got != 5 {
		t.Errorf("FieldCount() from raw = %d, want 5", got)
	}
}

func TestParameterDescription_ParameterCount(t *testing.T) {
	// Test with parsed
	parsed := ServerExtendedQueryParameterDescription(NewLazyServerFromParsed(&pgproto3.ParameterDescription{
		ParameterOIDs: []uint32{23, 25, 1043},
	}))
	if got := parsed.ParameterCount(); got != 3 {
		t.Errorf("ParameterCount() from parsed = %d, want 3", got)
	}

	// Test with raw
	body := make([]byte, 2)
	binary.BigEndian.PutUint16(body, 10)
	raw := ServerExtendedQueryParameterDescription{LazyServer[*pgproto3.ParameterDescription]{
		source: RawBody{Type: 't', Body: body},
	}}
	if got := raw.ParameterCount(); got != 10 {
		t.Errorf("ParameterCount() from raw = %d, want 10", got)
	}
}

func TestQuery_QueryBytes(t *testing.T) {
	query := "SELECT * FROM users WHERE id = $1"

	// Test with parsed
	parsed := ClientSimpleQueryQuery(NewLazyClientFromParsed(&pgproto3.Query{String: query}))
	if got := string(parsed.QueryBytes()); got != query {
		t.Errorf("QueryBytes() from parsed = %q, want %q", got, query)
	}

	// Test with raw (null-terminated)
	rawBody := append([]byte(query), 0)
	raw := ClientSimpleQueryQuery{LazyClient[*pgproto3.Query]{
		source: RawBody{Type: 'Q', Body: rawBody},
	}}
	if got := raw.QueryBytes(); string(got) != string(rawBody) {
		t.Errorf("QueryBytes() from raw = %q, want %q", got, rawBody)
	}
}

func TestParse_StatementNameBytes(t *testing.T) {
	// Test with parsed
	parsed := ClientExtendedQueryParse(NewLazyClientFromParsed(&pgproto3.Parse{Name: "stmt1", Query: "SELECT 1"}))
	if got := string(parsed.StatementNameBytes()); got != "stmt1" {
		t.Errorf("StatementNameBytes() from parsed = %q, want %q", got, "stmt1")
	}

	// Test with raw: name\0query\0paramcount(2 bytes)
	rawBody := append([]byte("my_stmt"), 0)
	rawBody = append(rawBody, []byte("SELECT $1")...)
	rawBody = append(rawBody, 0, 0, 0) // null + 0 params
	raw := ClientExtendedQueryParse{LazyClient[*pgproto3.Parse]{
		source: RawBody{Type: 'P', Body: rawBody},
	}}
	if got := string(raw.StatementNameBytes()); got != "my_stmt" {
		t.Errorf("StatementNameBytes() from raw = %q, want %q", got, "my_stmt")
	}
}

func TestBind_PortalNameBytes(t *testing.T) {
	// Test with parsed
	parsed := ClientExtendedQueryBind(NewLazyClientFromParsed(&pgproto3.Bind{
		DestinationPortal: "portal1",
		PreparedStatement: "stmt1",
	}))
	if got := string(parsed.PortalNameBytes()); got != "portal1" {
		t.Errorf("PortalNameBytes() from parsed = %q, want %q", got, "portal1")
	}

	// Test with raw: portal\0stmt\0...
	rawBody := append([]byte("p1"), 0)
	rawBody = append(rawBody, []byte("s1")...)
	rawBody = append(rawBody, 0)
	rawBody = append(rawBody, 0, 0, 0, 0, 0, 0) // format codes, params, result codes
	raw := ClientExtendedQueryBind{LazyClient[*pgproto3.Bind]{
		source: RawBody{Type: 'B', Body: rawBody},
	}}
	if got := string(raw.PortalNameBytes()); got != "p1" {
		t.Errorf("PortalNameBytes() from raw = %q, want %q", got, "p1")
	}
}

func TestBind_StatementNameBytes(t *testing.T) {
	// Test with parsed
	parsed := ClientExtendedQueryBind(NewLazyClientFromParsed(&pgproto3.Bind{
		DestinationPortal: "portal1",
		PreparedStatement: "stmt1",
	}))
	if got := string(parsed.StatementNameBytes()); got != "stmt1" {
		t.Errorf("StatementNameBytes() from parsed = %q, want %q", got, "stmt1")
	}

	// Test with raw: portal\0stmt\0...
	rawBody := append([]byte("p1"), 0)
	rawBody = append(rawBody, []byte("prepared_statement")...)
	rawBody = append(rawBody, 0)
	rawBody = append(rawBody, 0, 0, 0, 0, 0, 0) // rest of bind message
	raw := ClientExtendedQueryBind{LazyClient[*pgproto3.Bind]{
		source: RawBody{Type: 'B', Body: rawBody},
	}}
	if got := string(raw.StatementNameBytes()); got != "prepared_statement" {
		t.Errorf("StatementNameBytes() from raw = %q, want %q", got, "prepared_statement")
	}
}

func TestExecute_PortalNameBytes(t *testing.T) {
	// Test with parsed
	parsed := ClientExtendedQueryExecute(NewLazyClientFromParsed(&pgproto3.Execute{Portal: "my_portal", MaxRows: 100}))
	if got := string(parsed.PortalNameBytes()); got != "my_portal" {
		t.Errorf("PortalNameBytes() from parsed = %q, want %q", got, "my_portal")
	}

	// Test with raw: portal\0maxrows(4 bytes)
	rawBody := append([]byte("test_portal"), 0)
	maxRows := make([]byte, 4)
	binary.BigEndian.PutUint32(maxRows, 50)
	rawBody = append(rawBody, maxRows...)
	raw := ClientExtendedQueryExecute{LazyClient[*pgproto3.Execute]{
		source: RawBody{Type: 'E', Body: rawBody},
	}}
	if got := string(raw.PortalNameBytes()); got != "test_portal" {
		t.Errorf("PortalNameBytes() from raw = %q, want %q", got, "test_portal")
	}
}

func TestExecute_MaxRows(t *testing.T) {
	// Test with parsed
	parsed := ClientExtendedQueryExecute(NewLazyClientFromParsed(&pgproto3.Execute{Portal: "", MaxRows: 1000}))
	if got := parsed.MaxRows(); got != 1000 {
		t.Errorf("MaxRows() from parsed = %d, want 1000", got)
	}

	// Test with raw: portal\0maxrows(4 bytes)
	rawBody := []byte{0} // empty portal name + null
	maxRows := make([]byte, 4)
	binary.BigEndian.PutUint32(maxRows, 500)
	rawBody = append(rawBody, maxRows...)
	raw := ClientExtendedQueryExecute{LazyClient[*pgproto3.Execute]{
		source: RawBody{Type: 'E', Body: rawBody},
	}}
	if got := raw.MaxRows(); got != 500 {
		t.Errorf("MaxRows() from raw = %d, want 500", got)
	}
}

func TestDescribe_ObjectType(t *testing.T) {
	// Test statement describe with parsed
	parsedStmt := ClientExtendedQueryDescribe(NewLazyClientFromParsed(&pgproto3.Describe{ObjectType: 'S', Name: "stmt1"}))
	if got := parsedStmt.ObjectType(); got != 'S' {
		t.Errorf("ObjectType() from parsed statement = %c, want S", got)
	}

	// Test portal describe with parsed
	parsedPortal := ClientExtendedQueryDescribe(NewLazyClientFromParsed(&pgproto3.Describe{ObjectType: 'P', Name: "portal1"}))
	if got := parsedPortal.ObjectType(); got != 'P' {
		t.Errorf("ObjectType() from parsed portal = %c, want P", got)
	}

	// Test with raw
	rawBody := append([]byte{'S'}, []byte("my_stmt")...)
	rawBody = append(rawBody, 0)
	raw := ClientExtendedQueryDescribe{LazyClient[*pgproto3.Describe]{
		source: RawBody{Type: 'D', Body: rawBody},
	}}
	if got := raw.ObjectType(); got != 'S' {
		t.Errorf("ObjectType() from raw = %c, want S", got)
	}
}

func TestDescribe_ObjectNameBytes(t *testing.T) {
	// Test with parsed
	parsed := ClientExtendedQueryDescribe(NewLazyClientFromParsed(&pgproto3.Describe{ObjectType: 'S', Name: "test_stmt"}))
	if got := string(parsed.ObjectNameBytes()); got != "test_stmt" {
		t.Errorf("ObjectNameBytes() from parsed = %q, want %q", got, "test_stmt")
	}

	// Test with raw: type(1) + name + null
	rawBody := append([]byte{'P'}, []byte("my_portal")...)
	rawBody = append(rawBody, 0)
	raw := ClientExtendedQueryDescribe{LazyClient[*pgproto3.Describe]{
		source: RawBody{Type: 'D', Body: rawBody},
	}}
	if got := string(raw.ObjectNameBytes()); got != "my_portal" {
		t.Errorf("ObjectNameBytes() from raw = %q, want %q", got, "my_portal")
	}
}

func TestClose_ObjectType(t *testing.T) {
	// Test with parsed
	parsed := ClientExtendedQueryClose(NewLazyClientFromParsed(&pgproto3.Close{ObjectType: 'S', Name: "stmt1"}))
	if got := parsed.ObjectType(); got != 'S' {
		t.Errorf("ObjectType() from parsed = %c, want S", got)
	}

	// Test with raw
	rawBody := append([]byte{'P'}, []byte("portal")...)
	rawBody = append(rawBody, 0)
	raw := ClientExtendedQueryClose{LazyClient[*pgproto3.Close]{
		source: RawBody{Type: 'C', Body: rawBody},
	}}
	if got := raw.ObjectType(); got != 'P' {
		t.Errorf("ObjectType() from raw = %c, want P", got)
	}
}

func TestClose_ObjectNameBytes(t *testing.T) {
	// Test with parsed
	parsed := ClientExtendedQueryClose(NewLazyClientFromParsed(&pgproto3.Close{ObjectType: 'S', Name: "to_close"}))
	if got := string(parsed.ObjectNameBytes()); got != "to_close" {
		t.Errorf("ObjectNameBytes() from parsed = %q, want %q", got, "to_close")
	}

	// Test with raw
	rawBody := append([]byte{'S'}, []byte("stmt_to_close")...)
	rawBody = append(rawBody, 0)
	raw := ClientExtendedQueryClose{LazyClient[*pgproto3.Close]{
		source: RawBody{Type: 'C', Body: rawBody},
	}}
	if got := string(raw.ObjectNameBytes()); got != "stmt_to_close" {
		t.Errorf("ObjectNameBytes() from raw = %q, want %q", got, "stmt_to_close")
	}
}
