package testing

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5"
)

func TestMockServer_SimpleQuery(t *testing.T) {
	// Create a mock server that accepts a connection and handles a simple query
	steps := AcceptConnSteps()
	steps = append(steps, SimpleQuerySteps("SELECT 1", "SELECT 1")...)
	steps = append(steps, WaitForClose())

	server := NewMockServer(t, steps...)
	defer server.Close()

	// Run the mock server in a goroutine
	errCh := make(chan error, 1)
	go func() {
		errCh <- server.Serve()
	}()

	// Connect to the mock server
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, "postgres://postgres@"+server.Addr()+"/postgres?sslmode=disable")
	if err != nil {
		t.Fatalf("failed to connect: %v", err)
	}
	defer conn.Close(ctx)

	// Execute a query
	_, err = conn.Exec(ctx, "SELECT 1")
	if err != nil {
		t.Fatalf("failed to execute query: %v", err)
	}

	// Close the connection and wait for server to finish
	conn.Close(ctx)
	if err := <-errCh; err != nil {
		t.Fatalf("server error: %v", err)
	}
}

func TestMockServer_AcceptConnection(t *testing.T) {
	// Test that we can accept connections and handle basic handshake
	steps := AcceptConnSteps()
	steps = append(steps, WaitForClose())

	server := NewMockServer(t, steps...)
	defer server.Close()

	errCh := make(chan error, 1)
	go func() {
		errCh <- server.Serve()
	}()

	ctx := context.Background()
	conn, err := pgx.Connect(ctx, "postgres://postgres@"+server.Addr()+"/postgres?sslmode=disable")
	if err != nil {
		t.Fatalf("failed to connect: %v", err)
	}

	// Just close the connection - we verified we can connect
	conn.Close(ctx)

	if err := <-errCh; err != nil {
		t.Fatalf("server error: %v", err)
	}
}
