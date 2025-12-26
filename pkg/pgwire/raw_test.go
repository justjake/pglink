package pgwire

import (
	"bytes"
	"testing"

	"github.com/jackc/pgx/v5/pgproto3"
)

func TestRawBody_Len(t *testing.T) {
	tests := []struct {
		name string
		raw  RawBody
		want int
	}{
		{"empty body", RawBody{Type: 'Z', Body: nil}, 5},
		{"single byte", RawBody{Type: 'Z', Body: []byte{'I'}}, 6},
		{"multi byte", RawBody{Type: 'D', Body: make([]byte, 100)}, 105},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.raw.Len(); got != tt.want {
				t.Errorf("Len() = %d, want %d", got, tt.want)
			}
		})
	}
}

func TestDecodeBackendMessage(t *testing.T) {
	tests := []struct {
		name    string
		raw     RawBody
		check   func(t *testing.T, msg pgproto3.BackendMessage)
		wantErr bool
	}{
		{
			name: "ReadyForQuery idle",
			raw:  RawBody{Type: 'Z', Body: []byte{'I'}},
			check: func(t *testing.T, msg pgproto3.BackendMessage) {
				rfq, ok := msg.(*pgproto3.ReadyForQuery)
				if !ok {
					t.Fatalf("expected *ReadyForQuery, got %T", msg)
				}
				if rfq.TxStatus != 'I' {
					t.Errorf("TxStatus = %c, want I", rfq.TxStatus)
				}
			},
		},
		{
			name: "ReadyForQuery in transaction",
			raw:  RawBody{Type: 'Z', Body: []byte{'T'}},
			check: func(t *testing.T, msg pgproto3.BackendMessage) {
				rfq := msg.(*pgproto3.ReadyForQuery)
				if rfq.TxStatus != 'T' {
					t.Errorf("TxStatus = %c, want T", rfq.TxStatus)
				}
			},
		},
		{
			name: "CommandComplete",
			raw:  RawBody{Type: 'C', Body: append([]byte("SELECT 1"), 0)},
			check: func(t *testing.T, msg pgproto3.BackendMessage) {
				cc, ok := msg.(*pgproto3.CommandComplete)
				if !ok {
					t.Fatalf("expected *CommandComplete, got %T", msg)
				}
				if string(cc.CommandTag) != "SELECT 1" {
					t.Errorf("CommandTag = %q, want %q", cc.CommandTag, "SELECT 1")
				}
			},
		},
		{
			name: "AuthenticationOk",
			raw:  RawBody{Type: 'R', Body: []byte{0, 0, 0, 0}},
			check: func(t *testing.T, msg pgproto3.BackendMessage) {
				_, ok := msg.(*pgproto3.AuthenticationOk)
				if !ok {
					t.Fatalf("expected *AuthenticationOk, got %T", msg)
				}
			},
		},
		{
			name: "ParameterStatus",
			raw: RawBody{Type: 'S', Body: func() []byte {
				// name\0value\0
				return append(append([]byte("client_encoding"), 0), append([]byte("UTF8"), 0)...)
			}()},
			check: func(t *testing.T, msg pgproto3.BackendMessage) {
				ps, ok := msg.(*pgproto3.ParameterStatus)
				if !ok {
					t.Fatalf("expected *ParameterStatus, got %T", msg)
				}
				if ps.Name != "client_encoding" {
					t.Errorf("Name = %q, want %q", ps.Name, "client_encoding")
				}
				if ps.Value != "UTF8" {
					t.Errorf("Value = %q, want %q", ps.Value, "UTF8")
				}
			},
		},
		{
			name:    "unknown type",
			raw:     RawBody{Type: 0xFF, Body: nil},
			wantErr: true,
		},
		{
			name:    "auth too short",
			raw:     RawBody{Type: 'R', Body: []byte{0, 0}},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			msg, err := decodeBackendMessage(tt.raw)
			if tt.wantErr {
				if err == nil {
					t.Error("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("decodeBackendMessage error: %v", err)
			}
			tt.check(t, msg)
		})
	}
}

func TestDecodeFrontendMessage(t *testing.T) {
	tests := []struct {
		name    string
		raw     RawBody
		check   func(t *testing.T, msg pgproto3.FrontendMessage)
		wantErr bool
	}{
		{
			name: "Query",
			raw:  RawBody{Type: 'Q', Body: append([]byte("SELECT 1"), 0)},
			check: func(t *testing.T, msg pgproto3.FrontendMessage) {
				q, ok := msg.(*pgproto3.Query)
				if !ok {
					t.Fatalf("expected *Query, got %T", msg)
				}
				if q.String != "SELECT 1" {
					t.Errorf("String = %q, want %q", q.String, "SELECT 1")
				}
			},
		},
		{
			name: "Sync",
			raw:  RawBody{Type: 'S', Body: nil},
			check: func(t *testing.T, msg pgproto3.FrontendMessage) {
				_, ok := msg.(*pgproto3.Sync)
				if !ok {
					t.Fatalf("expected *Sync, got %T", msg)
				}
			},
		},
		{
			name: "Terminate",
			raw:  RawBody{Type: 'X', Body: nil},
			check: func(t *testing.T, msg pgproto3.FrontendMessage) {
				_, ok := msg.(*pgproto3.Terminate)
				if !ok {
					t.Fatalf("expected *Terminate, got %T", msg)
				}
			},
		},
		{
			name: "Parse",
			raw: RawBody{Type: 'P', Body: func() []byte {
				// name\0query\0numParams(2 bytes)
				b := append([]byte("stmt1"), 0)
				b = append(b, append([]byte("SELECT $1"), 0)...)
				b = append(b, 0, 0) // 0 parameter types
				return b
			}()},
			check: func(t *testing.T, msg pgproto3.FrontendMessage) {
				p, ok := msg.(*pgproto3.Parse)
				if !ok {
					t.Fatalf("expected *Parse, got %T", msg)
				}
				if p.Name != "stmt1" {
					t.Errorf("Name = %q, want %q", p.Name, "stmt1")
				}
				if p.Query != "SELECT $1" {
					t.Errorf("Query = %q, want %q", p.Query, "SELECT $1")
				}
			},
		},
		{
			name:    "unknown type",
			raw:     RawBody{Type: 0xFF, Body: nil},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			msg, err := decodeFrontendMessage(tt.raw)
			if tt.wantErr {
				if err == nil {
					t.Error("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("decodeFrontendMessage error: %v", err)
			}
			tt.check(t, msg)
		})
	}
}

func TestFromServer(t *testing.T) {
	t.Run("parse from raw", func(t *testing.T) {
		raw := RawBody{Type: 'Z', Body: []byte{'I'}}
		lazy := FromServer[*pgproto3.ReadyForQuery]{source: raw}

		msg := lazy.Parse()
		if msg.TxStatus != 'I' {
			t.Errorf("TxStatus = %c, want I", msg.TxStatus)
		}

		// Second parse should return cached value
		msg2 := lazy.Parse()
		if msg != msg2 {
			t.Error("Parse() should return cached value")
		}
	})

	t.Run("from parsed", func(t *testing.T) {
		parsed := &pgproto3.ReadyForQuery{TxStatus: 'T'}
		lazy := ServerParsed(parsed)

		msg := lazy.Parse()
		if msg != parsed {
			t.Error("Parse() should return original parsed message")
		}
	})

	t.Run("source accessor", func(t *testing.T) {
		raw := RawBody{Type: 'Z', Body: []byte{'E'}}
		lazy := FromServer[*pgproto3.ReadyForQuery]{source: raw}

		gotSource := lazy.Source()
		if gotSource.MessageType() != 'Z' || !bytes.Equal(gotSource.MessageBody(), []byte{'E'}) {
			t.Errorf("Source() = %v, want %v", gotSource, raw)
		}
	})
}

func TestFromClient(t *testing.T) {
	t.Run("parse from raw", func(t *testing.T) {
		raw := RawBody{Type: 'Q', Body: append([]byte("SELECT 1"), 0)}
		lazy := FromClient[*pgproto3.Query]{source: raw}

		msg := lazy.Parse()
		if msg.String != "SELECT 1" {
			t.Errorf("String = %q, want %q", msg.String, "SELECT 1")
		}

		// Second parse should return cached value
		msg2 := lazy.Parse()
		if msg != msg2 {
			t.Error("Parse() should return cached value")
		}
	})

	t.Run("from parsed", func(t *testing.T) {
		parsed := &pgproto3.Query{String: "SELECT 2"}
		lazy := ClientParsed(parsed)

		msg := lazy.Parse()
		if msg != parsed {
			t.Error("Parse() should return original parsed message")
		}
	})
}

func TestEncodeBackendMessage(t *testing.T) {
	tests := []struct {
		name string
		msg  pgproto3.BackendMessage
	}{
		{"ReadyForQuery", &pgproto3.ReadyForQuery{TxStatus: 'I'}},
		{"CommandComplete", &pgproto3.CommandComplete{CommandTag: []byte("SELECT 1")}},
		{"DataRow", &pgproto3.DataRow{Values: [][]byte{[]byte("foo"), []byte("bar")}}},
		{"ParseComplete", &pgproto3.ParseComplete{}},
		{"BindComplete", &pgproto3.BindComplete{}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			raw := EncodeBackendMessage(tt.msg)
			if raw.IsZero() {
				t.Fatal("EncodeBackendMessage returned zero RawBody")
			}

			// Decode and verify round-trip
			decoded, err := decodeBackendMessage(raw)
			if err != nil {
				t.Fatalf("decodeBackendMessage error: %v", err)
			}

			// Re-encode both and compare
			originalEncoded, _ := tt.msg.Encode(nil)
			decodedEncoded, _ := decoded.Encode(nil)
			if !bytes.Equal(originalEncoded, decodedEncoded) {
				t.Errorf("round-trip mismatch")
			}
		})
	}
}

func TestEncodeFrontendMessage(t *testing.T) {
	tests := []struct {
		name string
		msg  pgproto3.FrontendMessage
	}{
		{"Query", &pgproto3.Query{String: "SELECT 1"}},
		{"Sync", &pgproto3.Sync{}},
		{"Terminate", &pgproto3.Terminate{}},
		{"Parse", &pgproto3.Parse{Name: "stmt", Query: "SELECT $1"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			raw := EncodeFrontendMessage(tt.msg)
			if raw.IsZero() {
				t.Fatal("EncodeFrontendMessage returned zero RawBody")
			}

			// Decode and verify round-trip
			decoded, err := decodeFrontendMessage(raw)
			if err != nil {
				t.Fatalf("decodeFrontendMessage error: %v", err)
			}

			// Re-encode both and compare
			originalEncoded, _ := tt.msg.Encode(nil)
			decodedEncoded, _ := decoded.Encode(nil)
			if !bytes.Equal(originalEncoded, decodedEncoded) {
				t.Errorf("round-trip mismatch")
			}
		})
	}
}
