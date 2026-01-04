package pgserver

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	crand "crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"io"
	"math/big"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/justjake/pglink/pkg/pgwire"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// testTimeout is applied to all test contexts
const testTimeout = 10 * time.Second

func testContext(t *testing.T) (context.Context, context.CancelFunc) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	t.Cleanup(cancel)
	return ctx, cancel
}

// newTestServer creates a server for testing with sensible defaults.
func newTestServer(t *testing.T, cfg ServerConfig) *Server {
	t.Helper()

	if cfg.AuthHandler == nil {
		// Default: accept all connections with cleartext auth
		cfg.AuthHandler = func(ctx context.Context, conn *UnauthorizedConn) (*AuthorizedConn, error) {
			if err := conn.Send(ctx, &pgproto3.AuthenticationOk{}); err != nil {
				return nil, err
			}
			params := pgwire.ParameterStatuses(conn.StartupMessage.Parameters)
			return &AuthorizedConn{
				FrontendConn:   conn.FrontendConn,
				User:           params.User(),
				Database:       params.Database(),
				StartupMessage: conn.StartupMessage,
			}, nil
		}
	}

	if cfg.Handler == nil {
		// Default: immediately close
		cfg.Handler = func(ctx context.Context, conn *ClientConn) error {
			return nil
		}
	}

	if cfg.StartupTimeout == 0 {
		cfg.StartupTimeout = 5 * time.Second
	}

	server, err := NewServer(cfg)
	require.NoError(t, err)
	return server
}

// startTestServer starts a server on a random port and returns a function to get its address.
func startTestServer(t *testing.T, server *Server) string {
	t.Helper()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	errCh := make(chan error, 1)
	go func() {
		errCh <- server.Serve(ln)
	}()

	t.Cleanup(func() {
		ln.Close()
		// Wait briefly for shutdown
		select {
		case <-errCh:
		case <-time.After(time.Second):
		}
	})

	return ln.Addr().String()
}

func TestServer_BasicConnection(t *testing.T) {
	ctx, _ := testContext(t)

	var handlerCalled atomic.Bool
	server := newTestServer(t, ServerConfig{
		Handler: func(ctx context.Context, conn *ClientConn) error {
			handlerCalled.Store(true)
			assert.Equal(t, "testuser", conn.User)
			assert.Equal(t, "testdb", conn.Database)
			assert.NotZero(t, conn.ProcessID)
			assert.NotZero(t, conn.SecretKey)
			return nil
		},
	})

	addr := startTestServer(t, server)

	// Connect with pgx
	conn, err := pgx.Connect(ctx, "postgres://testuser@"+addr+"/testdb?sslmode=disable")
	require.NoError(t, err)
	defer conn.Close(ctx)

	// Connection should close gracefully after handler returns
	time.Sleep(100 * time.Millisecond)
	assert.True(t, handlerCalled.Load())
}

func TestServer_ConnMapTracking(t *testing.T) {
	ctx, _ := testContext(t)

	// Use a channel to coordinate between handler and test
	handlerStarted := make(chan *ClientConn, 1)
	handlerDone := make(chan struct{})

	server := newTestServer(t, ServerConfig{
		Handler: func(ctx context.Context, conn *ClientConn) error {
			handlerStarted <- conn
			<-handlerDone // Wait for test to signal
			return nil
		},
	})

	addr := startTestServer(t, server)

	// Connect
	conn, err := pgx.Connect(ctx, "postgres://testuser@"+addr+"/testdb?sslmode=disable")
	require.NoError(t, err)
	defer conn.Close(ctx)

	// Wait for handler to start
	clientConn := <-handlerStarted

	// Connection should be in ConnMap
	key := ConnKey{ProcessID: clientConn.ProcessID, SecretKey: clientConn.SecretKey}
	got, ok := server.ConnMap.Get(key)
	require.True(t, ok, "connection should be in ConnMap")
	assert.Same(t, clientConn, got)
	assert.Equal(t, 1, server.ConnMap.Len())

	// Signal handler to finish
	close(handlerDone)

	// Wait for cleanup
	time.Sleep(100 * time.Millisecond)

	// Connection should be removed from ConnMap
	_, ok = server.ConnMap.Get(key)
	assert.False(t, ok, "connection should be removed from ConnMap after handler returns")
	assert.Equal(t, 0, server.ConnMap.Len())
}

func TestServer_MultipleConnections(t *testing.T) {
	ctx, _ := testContext(t)

	var activeCount atomic.Int32
	var maxActive atomic.Int32

	server := newTestServer(t, ServerConfig{
		Handler: func(ctx context.Context, conn *ClientConn) error {
			current := activeCount.Add(1)
			// Track max concurrent connections
			for {
				oldMax := maxActive.Load()
				if current <= oldMax || maxActive.CompareAndSwap(oldMax, current) {
					break
				}
			}
			time.Sleep(50 * time.Millisecond)
			activeCount.Add(-1)
			return nil
		},
	})

	addr := startTestServer(t, server)

	// Connect multiple clients concurrently
	const numClients = 5
	var wg sync.WaitGroup
	wg.Add(numClients)

	for i := 0; i < numClients; i++ {
		go func() {
			defer wg.Done()
			conn, err := pgx.Connect(ctx, "postgres://user@"+addr+"/db?sslmode=disable")
			if err != nil {
				t.Logf("connect error: %v", err)
				return
			}
			defer conn.Close(ctx)
			time.Sleep(100 * time.Millisecond)
		}()
	}

	wg.Wait()

	// Should have seen multiple concurrent connections
	assert.GreaterOrEqual(t, maxActive.Load(), int32(2), "should handle concurrent connections")
}

func TestServer_AuthHandler(t *testing.T) {
	ctx, _ := testContext(t)

	// Auth handler that rejects certain users
	server := newTestServer(t, ServerConfig{
		AuthHandler: func(ctx context.Context, conn *UnauthorizedConn) (*AuthorizedConn, error) {
			params := pgwire.ParameterStatuses(conn.StartupMessage.Parameters)
			if params.User() == "baduser" {
				return nil, pgwire.NewErr(pgwire.ErrorFatal, "28000", "authentication failed", nil)
			}
			if err := conn.Send(ctx, &pgproto3.AuthenticationOk{}); err != nil {
				return nil, err
			}
			return &AuthorizedConn{
				FrontendConn:   conn.FrontendConn,
				User:           params.User(),
				Database:       params.Database(),
				StartupMessage: conn.StartupMessage,
			}, nil
		},
	})

	addr := startTestServer(t, server)

	// Good user should connect
	conn, err := pgx.Connect(ctx, "postgres://gooduser@"+addr+"/db?sslmode=disable")
	require.NoError(t, err)
	conn.Close(ctx)

	// Bad user should be rejected
	_, err = pgx.Connect(ctx, "postgres://baduser@"+addr+"/db?sslmode=disable")
	require.Error(t, err)
}

func TestServer_StartupTimeout(t *testing.T) {
	ctx, _ := testContext(t)

	server := newTestServer(t, ServerConfig{
		StartupTimeout: 100 * time.Millisecond,
		AuthHandler: func(ctx context.Context, conn *UnauthorizedConn) (*AuthorizedConn, error) {
			// Stall forever
			<-ctx.Done()
			return nil, ctx.Err()
		},
	})

	addr := startTestServer(t, server)

	// Should timeout during auth
	_, err := pgx.Connect(ctx, "postgres://user@"+addr+"/db?sslmode=disable")
	require.Error(t, err)
}

func TestServer_ConnContext(t *testing.T) {
	ctx, _ := testContext(t)

	type ctxKey struct{}
	var receivedValue atomic.Value

	server := newTestServer(t, ServerConfig{
		ConnContext: func(ctx context.Context, conn net.Conn) (context.Context, error) {
			return context.WithValue(ctx, ctxKey{}, "custom-value"), nil
		},
		Handler: func(ctx context.Context, conn *ClientConn) error {
			if v := ctx.Value(ctxKey{}); v != nil {
				receivedValue.Store(v)
			}
			return nil
		},
	})

	addr := startTestServer(t, server)

	conn, err := pgx.Connect(ctx, "postgres://user@"+addr+"/db?sslmode=disable")
	require.NoError(t, err)
	conn.Close(ctx)

	time.Sleep(100 * time.Millisecond)
	assert.Equal(t, "custom-value", receivedValue.Load())
}

func TestServer_ConnContextReject(t *testing.T) {
	ctx, _ := testContext(t)

	server := newTestServer(t, ServerConfig{
		ConnContext: func(ctx context.Context, conn net.Conn) (context.Context, error) {
			return nil, errors.New("connection rejected")
		},
	})

	addr := startTestServer(t, server)

	// Connection should be rejected
	_, err := pgx.Connect(ctx, "postgres://user@"+addr+"/db?sslmode=disable")
	require.Error(t, err)
}

func TestServer_CtxServer(t *testing.T) {
	ctx, _ := testContext(t)

	var receivedServer atomic.Pointer[Server]

	server := newTestServer(t, ServerConfig{
		Handler: func(ctx context.Context, conn *ClientConn) error {
			if s := CtxServer(ctx); s != nil {
				receivedServer.Store(s)
			}
			return nil
		},
	})

	addr := startTestServer(t, server)

	conn, err := pgx.Connect(ctx, "postgres://user@"+addr+"/db?sslmode=disable")
	require.NoError(t, err)
	conn.Close(ctx)

	time.Sleep(100 * time.Millisecond)
	assert.Same(t, server, receivedServer.Load())
}

func TestServer_HandlerError(t *testing.T) {
	ctx, _ := testContext(t)

	var errorCount atomic.Int32

	server := newTestServer(t, ServerConfig{
		Handler: func(ctx context.Context, conn *ClientConn) error {
			errorCount.Add(1)
			return errors.New("handler error")
		},
	})

	addr := startTestServer(t, server)

	// Connection succeeds (handler runs AFTER startup completes) but immediately closes
	conn, err := pgx.Connect(ctx, "postgres://user@"+addr+"/db?sslmode=disable")
	if err == nil {
		conn.Close(ctx)
	}

	// Server should still be operational - try another connection
	conn, err = pgx.Connect(ctx, "postgres://user@"+addr+"/db?sslmode=disable")
	if err == nil {
		conn.Close(ctx)
	}

	// Wait a bit for handlers to complete
	time.Sleep(100 * time.Millisecond)

	// Handler should have been called (at least once)
	assert.GreaterOrEqual(t, errorCount.Load(), int32(1))
}

func TestNewServer_Validation(t *testing.T) {
	// Missing AuthHandler
	_, err := NewServer(ServerConfig{
		Handler: func(ctx context.Context, conn *ClientConn) error { return nil },
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "AuthHandler")

	// Missing Handler
	_, err = NewServer(ServerConfig{
		AuthHandler: func(ctx context.Context, conn *UnauthorizedConn) (*AuthorizedConn, error) {
			return nil, nil
		},
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "Handler")

	// Valid config
	server, err := NewServer(ServerConfig{
		AuthHandler: func(ctx context.Context, conn *UnauthorizedConn) (*AuthorizedConn, error) {
			return nil, nil
		},
		Handler: func(ctx context.Context, conn *ClientConn) error { return nil },
	})
	require.NoError(t, err)
	require.NotNil(t, server)

	// Defaults should be set
	assert.NotNil(t, server.CancelHandler)
	assert.NotNil(t, server.StartupHandler)
}

func TestPeekedConn(t *testing.T) {
	// Test the peekedConn type used for TLS fast-start detection
	// When peekedConn is created, the first byte has already been read from underlying
	// The underlying conn is at offset 1, and peekedConn holds byte 0
	data := []byte("hello world")
	underlying := &mockConn{data: data, offset: 1} // Already read first byte

	peeked := &peekedConn{
		Conn:   underlying,
		peeked: data[0], // The byte we "peeked" (already read)
		used:   false,
	}

	// First read should return peeked byte + more from underlying
	buf := make([]byte, 5)
	n, err := peeked.Read(buf)
	require.NoError(t, err)
	assert.Equal(t, 5, n)
	assert.Equal(t, "hello", string(buf[:n]))

	// Subsequent reads should be normal from underlying
	buf = make([]byte, 6)
	n, err = peeked.Read(buf)
	require.NoError(t, err)
	assert.Equal(t, 6, n)
	assert.Equal(t, " world", string(buf[:n]))
}

func TestPeekedConn_SingleByteRead(t *testing.T) {
	data := []byte("hello")
	underlying := &mockConn{data: data, offset: 1} // Already read first byte

	peeked := &peekedConn{
		Conn:   underlying,
		peeked: data[0],
		used:   false,
	}

	// Single byte read returns just the peeked byte
	buf := make([]byte, 1)
	n, err := peeked.Read(buf)
	require.NoError(t, err)
	assert.Equal(t, 1, n)
	assert.Equal(t, byte('h'), buf[0])

	// Next read should continue from underlying (offset 1)
	buf = make([]byte, 4)
	n, err = peeked.Read(buf)
	require.NoError(t, err)
	assert.Equal(t, 4, n)
	assert.Equal(t, "ello", string(buf[:n]))
}

func TestIsTLSHandshake(t *testing.T) {
	tests := []struct {
		name      string
		firstByte byte
		wantTLS   bool
	}{
		{"TLS ClientHello", 0x16, true},
		{"PostgreSQL StartupMessage", 0x00, false},
		{"Random byte", 0x42, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			data := []byte{tt.firstByte, 0x01, 0x02, 0x03}
			conn := &mockConn{data: data}

			wrapped, isTLS, err := isTLSHandshake(conn)
			require.NoError(t, err)
			assert.Equal(t, tt.wantTLS, isTLS)

			// Should be able to read all data including peeked byte
			buf := make([]byte, 10)
			n, _ := wrapped.Read(buf)
			assert.Equal(t, 4, n)
			assert.Equal(t, data, buf[:n])
		})
	}
}

// mockConn is a minimal net.Conn implementation for testing
type mockConn struct {
	net.Conn
	data   []byte
	offset int
}

func (m *mockConn) Read(b []byte) (int, error) {
	if m.offset >= len(m.data) {
		return 0, io.EOF
	}
	n := copy(b, m.data[m.offset:])
	m.offset += n
	return n, nil
}

func (m *mockConn) Write(b []byte) (int, error) {
	return len(b), nil
}

func (m *mockConn) Close() error {
	return nil
}

func (m *mockConn) LocalAddr() net.Addr {
	return &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 5432}
}

func (m *mockConn) RemoteAddr() net.Addr {
	return &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 12345}
}

func (m *mockConn) SetDeadline(t time.Time) error      { return nil }
func (m *mockConn) SetReadDeadline(t time.Time) error  { return nil }
func (m *mockConn) SetWriteDeadline(t time.Time) error { return nil }

func TestServer_CancelHandler(t *testing.T) {
	ctx, _ := testContext(t)

	// Use channels for coordination
	handlerReady := make(chan ConnKey, 1)
	cancelReceived := make(chan pgwire.ProcessID, 1)
	handlerDone := make(chan struct{})

	server := newTestServer(t, ServerConfig{
		Handler: func(ctx context.Context, conn *ClientConn) error {
			// Set up cancel handler for this connection
			conn.CancelHandler = func(ctx context.Context, target *ClientConn, cancel *CancelConn) error {
				cancelReceived <- target.ProcessID
				return nil
			}
			// Signal that handler is ready
			handlerReady <- ConnKey{ProcessID: conn.ProcessID, SecretKey: conn.SecretKey}
			// Wait for test to finish
			<-handlerDone
			return nil
		},
	})

	addr := startTestServer(t, server)

	// Connect
	conn, err := pgx.Connect(ctx, "postgres://user@"+addr+"/db?sslmode=disable")
	require.NoError(t, err)
	defer func() {
		close(handlerDone)
		conn.Close(ctx)
	}()

	// Wait for handler to be ready and get the key
	var key ConnKey
	select {
	case key = <-handlerReady:
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for handler")
	}

	// Send cancel request on separate connection
	cancelConn, err := net.Dial("tcp", addr)
	require.NoError(t, err)
	defer cancelConn.Close()

	// Build cancel request message
	// Format: 16 bytes total
	// - 4 bytes: message length (16)
	// - 4 bytes: cancel request code (80877102)
	// - 4 bytes: process ID
	// - 4 bytes: secret key
	cancelMsg := []byte{
		0, 0, 0, 16, // length
		0x04, 0xd2, 0x16, 0x2e, // 80877102 in big endian
		byte(key.ProcessID >> 24), byte(key.ProcessID >> 16), byte(key.ProcessID >> 8), byte(key.ProcessID),
		byte(key.SecretKey >> 24), byte(key.SecretKey >> 16), byte(key.SecretKey >> 8), byte(key.SecretKey),
	}
	_, err = cancelConn.Write(cancelMsg)
	require.NoError(t, err)

	// Wait for cancel to be processed
	select {
	case receivedPID := <-cancelReceived:
		assert.Equal(t, key.ProcessID, receivedPID)
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for cancel handler")
	}
}

func TestDefaultStartupHandler(t *testing.T) {
	ctx := context.Background()

	authConn := &AuthorizedConn{
		User:     "testuser",
		Database: "testdb",
		StartupMessage: &pgproto3.StartupMessage{
			ProtocolVersion: pgproto3.ProtocolVersionNumber,
			Parameters: map[string]string{
				"user":            "testuser",
				"database":        "testdb",
				"client_encoding": "UTF8",
			},
		},
	}

	clientConn, err := DefaultStartupHandler(ctx, authConn)
	require.NoError(t, err)

	assert.Equal(t, "testuser", clientConn.User)
	assert.Equal(t, "testdb", clientConn.Database)
	assert.NotZero(t, clientConn.ProcessID)
	assert.NotZero(t, clientConn.SecretKey)
	assert.NotEmpty(t, clientConn.StartupParameters)
}

func TestDefaultStartupHandler_GeneratesUniqueIDs(t *testing.T) {
	ctx := context.Background()

	authConn := &AuthorizedConn{
		User:     "user",
		Database: "db",
		StartupMessage: &pgproto3.StartupMessage{
			ProtocolVersion: pgproto3.ProtocolVersionNumber,
			Parameters:      map[string]string{"user": "user", "database": "db"},
		},
	}

	// Generate multiple connections and check for uniqueness
	seen := make(map[ConnKey]bool)
	for i := 0; i < 100; i++ {
		clientConn, err := DefaultStartupHandler(ctx, authConn)
		require.NoError(t, err)

		key := ConnKey{ProcessID: clientConn.ProcessID, SecretKey: clientConn.SecretKey}
		assert.False(t, seen[key], "ProcessID/SecretKey should be unique")
		seen[key] = true
	}
}

// Ensure TLS config is optional
func TestServer_NoTLS(t *testing.T) {
	ctx, _ := testContext(t)

	server := newTestServer(t, ServerConfig{
		TLSConfig: nil, // No TLS
	})

	addr := startTestServer(t, server)

	// Should work without TLS
	conn, err := pgx.Connect(ctx, "postgres://user@"+addr+"/db?sslmode=disable")
	require.NoError(t, err)
	conn.Close(ctx)
}

func TestServer_TLSRequired(t *testing.T) {
	ctx, _ := testContext(t)

	cert := generateTestCert(t)

	t.Run("accepts TLS connections", func(t *testing.T) {
		server := newTestServer(t, ServerConfig{
			TLSConfig:   &tls.Config{Certificates: []tls.Certificate{cert}},
			TLSOptional: false,
		})

		addr := startTestServer(t, server)

		conn, err := pgx.Connect(ctx, "postgres://user@"+addr+"/db?sslmode=require")
		require.NoError(t, err)
		conn.Close(ctx)
	})

	t.Run("rejects plaintext connections", func(t *testing.T) {
		server := newTestServer(t, ServerConfig{
			TLSConfig:   &tls.Config{Certificates: []tls.Certificate{cert}},
			TLSOptional: false,
		})

		addr := startTestServer(t, server)

		// sslmode=disable should be rejected when TLS is required
		_, err := pgx.Connect(ctx, "postgres://user@"+addr+"/db?sslmode=disable")
		require.Error(t, err, "plaintext connection should be rejected when TLS is required")
	})
}

func TestServer_TLSOptional(t *testing.T) {
	ctx, _ := testContext(t)

	cert := generateTestCert(t)

	server := newTestServer(t, ServerConfig{
		TLSConfig:   &tls.Config{Certificates: []tls.Certificate{cert}},
		TLSOptional: true, // TLS optional
	})

	addr := startTestServer(t, server)

	// Both should work
	conn1, err := pgx.Connect(ctx, "postgres://user@"+addr+"/db?sslmode=disable")
	require.NoError(t, err)
	conn1.Close(ctx)

	conn2, err := pgx.Connect(ctx, "postgres://user@"+addr+"/db?sslmode=require")
	require.NoError(t, err)
	conn2.Close(ctx)
}

// generateTestCert generates a self-signed certificate for testing
func generateTestCert(t *testing.T) tls.Certificate {
	t.Helper()

	priv, err := ecdsa.GenerateKey(elliptic.P256(), crand.Reader)
	require.NoError(t, err)

	template := x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject: pkix.Name{
			Organization: []string{"pglink-test"},
		},
		NotBefore:             time.Now(),
		NotAfter:              time.Now().Add(24 * time.Hour),
		KeyUsage:              x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
		IPAddresses:           []net.IP{net.IPv4(127, 0, 0, 1)},
		DNSNames:              []string{"localhost"},
	}

	derBytes, err := x509.CreateCertificate(crand.Reader, &template, &template, &priv.PublicKey, priv)
	require.NoError(t, err)

	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: derBytes})

	privBytes, err := x509.MarshalPKCS8PrivateKey(priv)
	require.NoError(t, err)
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: privBytes})

	cert, err := tls.X509KeyPair(certPEM, keyPEM)
	require.NoError(t, err)

	return cert
}
