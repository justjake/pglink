// Package testing provides test utilities for pglink using pgmock.
// It enables testing of PostgreSQL proxy components by simulating
// both client and server sides of the PostgreSQL wire protocol.
package testing

import (
	"net"
	"testing"

	"github.com/jackc/pgmock"
	"github.com/jackc/pgproto3/v2"
)

// MockServer wraps pgmock.Script to provide a convenient test server.
type MockServer struct {
	Script   *pgmock.Script
	Listener net.Listener
	t        *testing.T
}

// NewMockServer creates a new mock PostgreSQL server for testing.
func NewMockServer(t *testing.T, steps ...pgmock.Step) *MockServer {
	t.Helper()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to create listener: %v", err)
	}

	return &MockServer{
		Script: &pgmock.Script{
			Steps: steps,
		},
		Listener: listener,
		t:        t,
	}
}

// Addr returns the address the mock server is listening on.
func (m *MockServer) Addr() string {
	return m.Listener.Addr().String()
}

// Serve accepts a single connection and runs the mock script.
// This should be called in a goroutine.
func (m *MockServer) Serve() error {
	conn, err := m.Listener.Accept()
	if err != nil {
		return err
	}
	defer conn.Close()

	backend := pgproto3.NewBackend(pgproto3.NewChunkReader(conn), conn)
	return m.Script.Run(backend)
}

// Close closes the listener.
func (m *MockServer) Close() error {
	return m.Listener.Close()
}

// AcceptConnSteps returns steps for accepting an unauthenticated connection.
// This handles the startup message exchange that occurs when a client connects.
func AcceptConnSteps() []pgmock.Step {
	return pgmock.AcceptUnauthenticatedConnRequestSteps()
}

// ExpectQuery returns a step that expects a simple query message.
func ExpectQuery(query string) pgmock.Step {
	return pgmock.ExpectMessage(&pgproto3.Query{String: query})
}

// SendRowDescription returns a step that sends column metadata.
func SendRowDescription(fields []pgproto3.FieldDescription) pgmock.Step {
	return pgmock.SendMessage(&pgproto3.RowDescription{Fields: fields})
}

// SendDataRow returns a step that sends a row of data.
func SendDataRow(values [][]byte) pgmock.Step {
	return pgmock.SendMessage(&pgproto3.DataRow{Values: values})
}

// SendCommandComplete returns a step that sends command completion.
func SendCommandComplete(tag string) pgmock.Step {
	return pgmock.SendMessage(&pgproto3.CommandComplete{CommandTag: []byte(tag)})
}

// SendReadyForQuery returns a step that sends ready for query status.
// status should be 'I' (idle), 'T' (in transaction), or 'E' (error).
func SendReadyForQuery(status byte) pgmock.Step {
	return pgmock.SendMessage(&pgproto3.ReadyForQuery{TxStatus: status})
}

// SendError returns a step that sends an error response.
func SendError(severity, code, message string) pgmock.Step {
	return pgmock.SendMessage(&pgproto3.ErrorResponse{
		Severity: severity,
		Code:     code,
		Message:  message,
	})
}

// WaitForClose returns a step that waits for connection close.
func WaitForClose() pgmock.Step {
	return pgmock.WaitForClose()
}

// SimpleQuerySteps returns a common pattern: expect query, return result, ready for query.
func SimpleQuerySteps(query string, tag string) []pgmock.Step {
	return []pgmock.Step{
		ExpectQuery(query),
		SendCommandComplete(tag),
		SendReadyForQuery('I'),
	}
}

// SimpleSelectSteps returns steps for a simple SELECT query with results.
func SimpleSelectSteps(query string, fields []pgproto3.FieldDescription, rows [][]byte, tag string) []pgmock.Step {
	steps := []pgmock.Step{
		ExpectQuery(query),
		SendRowDescription(fields),
	}
	if len(rows) > 0 {
		steps = append(steps, SendDataRow(rows))
	}
	steps = append(steps,
		SendCommandComplete(tag),
		SendReadyForQuery('I'),
	)
	return steps
}
