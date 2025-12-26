package frontend

import (
	"context"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// testTimeout is the maximum time for a single test case.
const shutdownTestTimeout = 5 * time.Second

// TestShutdownMode_String tests the String() method of ShutdownMode.
func TestShutdownMode_String(t *testing.T) {
	tests := []struct {
		mode     ShutdownMode
		expected string
	}{
		{ShutdownNone, "none"},
		{ShutdownWaitForClients, "wait_for_clients"},
		{ShutdownImmediate, "immediate"},
		{ShutdownMode(999), "unknown(999)"},
	}

	for _, tt := range tests {
		t.Run(tt.expected, func(t *testing.T) {
			assert.Equal(t, tt.expected, tt.mode.String())
		})
	}
}

// TestService_Shutdown_WaitForClients tests that ShutdownWaitForClients
// stops accepting new connections but allows existing sessions to continue.
func TestService_Shutdown_WaitForClients(t *testing.T) {
	// Create a mock service with minimal setup
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	svc := &Service{
		ctx:        ctx,
		cancel:     cancel,
		shutdownCh: make(chan struct{}),
	}

	// Initially not shutting down
	assert.Equal(t, ShutdownNone, svc.GetShutdownMode())
	assert.False(t, svc.IsShuttingDown())

	// Initiate graceful shutdown
	mode := svc.Shutdown(ShutdownWaitForClients)
	assert.Equal(t, ShutdownWaitForClients, mode)
	assert.Equal(t, ShutdownWaitForClients, svc.GetShutdownMode())
	assert.True(t, svc.IsShuttingDown())

	// Context should NOT be cancelled in wait-for-clients mode
	select {
	case <-ctx.Done():
		t.Fatal("context should not be cancelled in wait-for-clients mode")
	default:
		// Good, context is still active
	}
}

// TestService_Shutdown_Immediate tests that ShutdownImmediate
// cancels the context immediately.
func TestService_Shutdown_Immediate(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	svc := &Service{
		ctx:        ctx,
		cancel:     cancel,
		shutdownCh: make(chan struct{}),
		sessions:   make(map[*Session]struct{}),
	}

	// Initiate immediate shutdown
	mode := svc.Shutdown(ShutdownImmediate)
	assert.Equal(t, ShutdownImmediate, mode)
	assert.Equal(t, ShutdownImmediate, svc.GetShutdownMode())
	assert.True(t, svc.IsShuttingDown())

	// Context should be cancelled
	select {
	case <-ctx.Done():
		// Good, context is cancelled
	default:
		t.Fatal("context should be cancelled in immediate mode")
	}
}

// TestService_Shutdown_Escalation tests that a second shutdown call
// escalates from wait-for-clients to immediate.
func TestService_Shutdown_Escalation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	svc := &Service{
		ctx:        ctx,
		cancel:     cancel,
		shutdownCh: make(chan struct{}),
		sessions:   make(map[*Session]struct{}),
	}

	// First shutdown: wait for clients
	mode := svc.Shutdown(ShutdownWaitForClients)
	assert.Equal(t, ShutdownWaitForClients, mode)

	// Context should NOT be cancelled yet
	select {
	case <-ctx.Done():
		t.Fatal("context should not be cancelled after first signal")
	default:
		// Good
	}

	// Second shutdown: should escalate to immediate
	mode = svc.Shutdown(ShutdownWaitForClients)
	assert.Equal(t, ShutdownImmediate, mode)
	assert.Equal(t, ShutdownImmediate, svc.GetShutdownMode())

	// Context should now be cancelled
	select {
	case <-ctx.Done():
		// Good
	default:
		t.Fatal("context should be cancelled after escalation")
	}
}

// TestService_Shutdown_AlreadyImmediate tests that calling shutdown
// when already in immediate mode is a no-op.
func TestService_Shutdown_AlreadyImmediate(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	svc := &Service{
		ctx:        ctx,
		cancel:     cancel,
		shutdownCh: make(chan struct{}),
		sessions:   make(map[*Session]struct{}),
	}

	// First: immediate shutdown
	mode := svc.Shutdown(ShutdownImmediate)
	assert.Equal(t, ShutdownImmediate, mode)

	// Second: should return immediate (no change)
	mode = svc.Shutdown(ShutdownWaitForClients)
	assert.Equal(t, ShutdownImmediate, mode)

	// Third: should still return immediate
	mode = svc.Shutdown(ShutdownImmediate)
	assert.Equal(t, ShutdownImmediate, mode)
}

// TestService_Shutdown_ConcurrentCalls tests that concurrent shutdown calls
// are handled correctly.
func TestService_Shutdown_ConcurrentCalls(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	svc := &Service{
		ctx:        ctx,
		cancel:     cancel,
		shutdownCh: make(chan struct{}),
		sessions:   make(map[*Session]struct{}),
	}

	const numGoroutines = 10
	var wg sync.WaitGroup
	var immediateCount atomic.Int32

	wg.Add(numGoroutines)
	for i := 0; i < numGoroutines; i++ {
		go func() {
			defer wg.Done()
			mode := svc.Shutdown(ShutdownWaitForClients)
			if mode == ShutdownImmediate {
				immediateCount.Add(1)
			}
		}()
	}
	wg.Wait()

	// At least one call should have triggered wait-for-clients (the first one)
	// Subsequent calls should escalate to immediate
	// The final state should be immediate
	finalMode := svc.GetShutdownMode()
	assert.Equal(t, ShutdownImmediate, finalMode)

	// Multiple calls should have returned immediate (all but the first)
	assert.GreaterOrEqual(t, immediateCount.Load(), int32(numGoroutines-1))
}

// TestService_ShutdownHandler_ClosesListener tests that the shutdown handler
// closes the listener when shutdown is initiated.
func TestService_ShutdownHandler_ClosesListener(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), shutdownTestTimeout)
	defer cancel()

	// Create a real listener
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	svc := &Service{
		ctx:        ctx,
		cancel:     cancel,
		listener:   ln,
		shutdownCh: make(chan struct{}),
		sessions:   make(map[*Session]struct{}),
	}

	// Start the shutdown handler
	go svc.shutdownHandler()

	// Initiate shutdown
	svc.Shutdown(ShutdownWaitForClients)

	// Wait a bit for the handler to process
	time.Sleep(100 * time.Millisecond)

	// Try to accept - should fail because listener is closed
	_, err = ln.Accept()
	assert.Error(t, err)
}

// TestService_CancelAllSessions_ClosesConnections tests that cancelAllSessions
// closes all active session connections.
func TestService_CancelAllSessions_ClosesConnections(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Create a pair of connected sockets
	server, client := net.Pipe()
	defer client.Close()

	svc := &Service{
		ctx:      ctx,
		cancel:   cancel,
		sessions: make(map[*Session]struct{}),
	}

	// Create a mock session
	sessCtx, sessCancel := context.WithCancel(ctx)
	sess := &Session{
		ctx:    sessCtx,
		cancel: sessCancel,
		conn:   server,
	}
	svc.sessions[sess] = struct{}{}

	// Cancel all sessions
	svc.cancelAllSessions()

	// Try to read from client - should get EOF because server closed
	buf := make([]byte, 1)
	_, err := client.Read(buf)
	assert.Error(t, err)

	// Session context should be cancelled
	select {
	case <-sessCtx.Done():
		// Good
	default:
		t.Fatal("session context should be cancelled")
	}
}
