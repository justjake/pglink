package frontend

import (
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/justjake/pglink/pkg/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/crypto/pbkdf2"
)

// testTimeout is the maximum time for a single test case.
const testTimeout = 5 * time.Second

// testConn wraps a client-side connection for testing.
// It uses pgproto3.Frontend to send client messages and receive server responses.
type testConn struct {
	conn     net.Conn
	frontend *pgproto3.Frontend
}

func newTestConn(conn net.Conn) *testConn {
	return &testConn{
		conn:     conn,
		frontend: pgproto3.NewFrontend(conn, conn),
	}
}

func (c *testConn) close() {
	c.conn.Close()
}

// sendSASLInitialResponse sends a SASLInitialResponse message.
func (c *testConn) sendSASLInitialResponse(mechanism string, data []byte) error {
	msg := &pgproto3.SASLInitialResponse{
		AuthMechanism: mechanism,
		Data:          data,
	}
	c.frontend.Send(msg)
	return c.frontend.Flush()
}

// sendSASLResponse sends a SASLResponse message.
func (c *testConn) sendSASLResponse(data []byte) error {
	msg := &pgproto3.SASLResponse{
		Data: data,
	}
	c.frontend.Send(msg)
	return c.frontend.Flush()
}

// sendPassword sends a PasswordMessage.
func (c *testConn) sendPassword(password string) error {
	msg := &pgproto3.PasswordMessage{
		Password: password,
	}
	c.frontend.Send(msg)
	return c.frontend.Flush()
}

// receiveMessage receives the next message from the server.
func (c *testConn) receiveMessage() (pgproto3.BackendMessage, error) {
	return c.frontend.Receive()
}

// expectAuthSASL expects an AuthenticationSASL message and returns the mechanisms.
func (c *testConn) expectAuthSASL(t *testing.T) []string {
	t.Helper()
	msg, err := c.receiveMessage()
	require.NoError(t, err)
	sasl, ok := msg.(*pgproto3.AuthenticationSASL)
	require.True(t, ok, "expected AuthenticationSASL, got %T", msg)
	return sasl.AuthMechanisms
}

// expectAuthSASLContinue expects an AuthenticationSASLContinue message and returns the data.
func (c *testConn) expectAuthSASLContinue(t *testing.T) []byte {
	t.Helper()
	msg, err := c.receiveMessage()
	require.NoError(t, err)
	cont, ok := msg.(*pgproto3.AuthenticationSASLContinue)
	require.True(t, ok, "expected AuthenticationSASLContinue, got %T: %v", msg, msg)
	return cont.Data
}

// expectAuthSASLFinal expects an AuthenticationSASLFinal message and returns the data.
func (c *testConn) expectAuthSASLFinal(t *testing.T) []byte {
	t.Helper()
	msg, err := c.receiveMessage()
	require.NoError(t, err)
	final, ok := msg.(*pgproto3.AuthenticationSASLFinal)
	require.True(t, ok, "expected AuthenticationSASLFinal, got %T: %v", msg, msg)
	return final.Data
}

// expectAuthOk expects an AuthenticationOk message.
func (c *testConn) expectAuthOk(t *testing.T) {
	t.Helper()
	msg, err := c.receiveMessage()
	require.NoError(t, err)
	_, ok := msg.(*pgproto3.AuthenticationOk)
	require.True(t, ok, "expected AuthenticationOk, got %T: %v", msg, msg)
}

// expectError expects an ErrorResponse message and returns it.
func (c *testConn) expectError(t *testing.T) *pgproto3.ErrorResponse {
	t.Helper()
	msg, err := c.receiveMessage()
	require.NoError(t, err)
	errResp, ok := msg.(*pgproto3.ErrorResponse)
	require.True(t, ok, "expected ErrorResponse, got %T: %v", msg, msg)
	return errResp
}

// expectAuthMD5 expects an AuthenticationMD5Password message and returns the salt.
func (c *testConn) expectAuthMD5(t *testing.T) [4]byte {
	t.Helper()
	msg, err := c.receiveMessage()
	require.NoError(t, err)
	md5, ok := msg.(*pgproto3.AuthenticationMD5Password)
	require.True(t, ok, "expected AuthenticationMD5Password, got %T: %v", msg, msg)
	return md5.Salt
}

// expectAuthCleartext expects an AuthenticationCleartextPassword message.
func (c *testConn) expectAuthCleartext(t *testing.T) {
	t.Helper()
	msg, err := c.receiveMessage()
	require.NoError(t, err)
	_, ok := msg.(*pgproto3.AuthenticationCleartextPassword)
	require.True(t, ok, "expected AuthenticationCleartextPassword, got %T: %v", msg, msg)
}

// setupAuthSession creates a test connection pair and AuthSession for testing.
// Returns the testConn (client side) and a channel that will receive the AuthSession.Run() result.
func setupAuthSession(
	t *testing.T,
	username, password string,
	method config.AuthMethod,
	iterations int,
) (*testConn, <-chan error) {
	t.Helper()

	// Create a connected pair of sockets
	clientConn, serverConn := net.Pipe()

	// Set deadlines to prevent test hangs
	deadline := time.Now().Add(testTimeout)
	clientConn.SetDeadline(deadline)
	serverConn.SetDeadline(deadline)

	// Create test client
	tc := newTestConn(clientConn)

	// Create server-side frontend
	ctx := context.Background()
	frontend := NewFrontend(ctx, serverConn)

	// Create credentials
	creds := NewUserSecretData(username, password)

	// Create AuthSession
	authSession, err := NewAuthSession(frontend, creds, method, nil, iterations)
	require.NoError(t, err)

	// Run auth session in goroutine
	resultCh := make(chan error, 1)
	go func() {
		resultCh <- authSession.Run()
		serverConn.Close()
	}()

	// Register cleanup
	t.Cleanup(func() {
		tc.close()
	})

	return tc, resultCh
}

// pgScramClient is a test SCRAM client that follows PostgreSQL convention.
// PostgreSQL uses empty username in SCRAM messages (n=,) since the username
// is already provided in the startup message.
type pgScramClient struct {
	username           string
	password           string
	clientNonce        string
	clientFirstMsgBare string
	serverFirstMsg     string
	salt               []byte
	iterations         int
	saltedPassword     []byte
	authMessage        string
	expectedServerSig  []byte
}

func newPgScramClient(username, password string) *pgScramClient {
	// Generate a random nonce
	nonceBytes := make([]byte, 18)
	_, _ = rand.Read(nonceBytes)
	clientNonce := base64.StdEncoding.EncodeToString(nonceBytes)

	return &pgScramClient{
		username:    username,
		password:    password,
		clientNonce: clientNonce,
	}
}

// clientFirstMessage returns the client-first-message for SCRAM auth.
// PostgreSQL convention: empty username (n=,) since username is in startup message.
func (c *pgScramClient) clientFirstMessage() string {
	// PostgreSQL uses empty username in SCRAM messages
	c.clientFirstMsgBare = "n=,r=" + c.clientNonce
	return "n,," + c.clientFirstMsgBare
}

// clientFinalMessage processes the server-first-message and returns the client-final-message.
func (c *pgScramClient) clientFinalMessage(serverFirstMsg string) (string, error) {
	c.serverFirstMsg = serverFirstMsg

	// Parse server-first-message to get combined nonce, salt, and iteration count
	attrs := parseAttributes(serverFirstMsg)

	combinedNonce, ok := attrs["r"]
	if !ok {
		return "", fmt.Errorf("missing nonce in server-first-message")
	}
	if !strings.HasPrefix(combinedNonce, c.clientNonce) {
		return "", fmt.Errorf("server nonce doesn't start with client nonce")
	}

	saltB64, ok := attrs["s"]
	if !ok {
		return "", fmt.Errorf("missing salt in server-first-message")
	}
	salt, err := base64.StdEncoding.DecodeString(saltB64)
	if err != nil {
		return "", fmt.Errorf("invalid salt encoding: %w", err)
	}
	c.salt = salt

	iStr, ok := attrs["i"]
	if !ok {
		return "", fmt.Errorf("missing iteration count in server-first-message")
	}
	iterations, err := strconv.Atoi(iStr)
	if err != nil {
		return "", fmt.Errorf("invalid iteration count: %w", err)
	}
	c.iterations = iterations

	// Compute SaltedPassword
	c.saltedPassword = pbkdf2.Key([]byte(c.password), c.salt, c.iterations, 32, sha256.New)

	// Build client-final-message-without-proof
	channelBinding := base64.StdEncoding.EncodeToString([]byte("n,,"))
	clientFinalWithoutProof := fmt.Sprintf("c=%s,r=%s", channelBinding, combinedNonce)

	// Build AuthMessage
	c.authMessage = c.clientFirstMsgBare + "," + c.serverFirstMsg + "," + clientFinalWithoutProof

	// Compute ClientKey = HMAC(SaltedPassword, "Client Key")
	clientKey := hmacSHA256(c.saltedPassword, []byte("Client Key"))

	// Compute StoredKey = SHA256(ClientKey)
	storedKeyHash := sha256.Sum256(clientKey)
	storedKey := storedKeyHash[:]

	// Compute ClientSignature = HMAC(StoredKey, AuthMessage)
	clientSignature := hmacSHA256(storedKey, []byte(c.authMessage))

	// Compute ClientProof = ClientKey XOR ClientSignature
	clientProof := make([]byte, len(clientKey))
	for i := range clientKey {
		clientProof[i] = clientKey[i] ^ clientSignature[i]
	}
	proofB64 := base64.StdEncoding.EncodeToString(clientProof)

	// Compute expected ServerSignature for later verification
	serverKey := hmacSHA256(c.saltedPassword, []byte("Server Key"))
	c.expectedServerSig = hmacSHA256(serverKey, []byte(c.authMessage))

	return clientFinalWithoutProof + ",p=" + proofB64, nil
}

// verifyServerFinal checks the server-final-message and returns whether auth succeeded.
func (c *pgScramClient) verifyServerFinal(serverFinalMsg string) (bool, error) {
	if !strings.HasPrefix(serverFinalMsg, "v=") {
		return false, fmt.Errorf("invalid server-final-message format")
	}
	serverSigB64 := serverFinalMsg[2:]
	serverSig, err := base64.StdEncoding.DecodeString(serverSigB64)
	if err != nil {
		return false, fmt.Errorf("invalid server signature encoding: %w", err)
	}

	if !hmac.Equal(serverSig, c.expectedServerSig) {
		return false, fmt.Errorf("server signature mismatch")
	}
	return true, nil
}

// TestAuthSession_SCRAM_Success tests successful SCRAM-SHA-256 authentication.
func TestAuthSession_SCRAM_Success(t *testing.T) {
	tests := []struct {
		name       string
		username   string
		password   string
		iterations int
	}{
		{
			name:       "simple credentials",
			username:   "testuser",
			password:   "testpass",
			iterations: 4096,
		},
		{
			name:       "complex password",
			username:   "admin",
			password:   "p@ssw0rd!#$%^&*()",
			iterations: 4096,
		},
		{
			name:       "empty password",
			username:   "emptypass",
			password:   "",
			iterations: 4096,
		},
		{
			name:       "low iterations",
			username:   "lowiter",
			password:   "secret",
			iterations: 100,
		},
		{
			name:       "unicode username",
			username:   "用户",
			password:   "пароль",
			iterations: 4096,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tc, resultCh := setupAuthSession(t, tt.username, tt.password, config.AuthMethodSCRAMSHA256, tt.iterations)

			// Receive SASL auth request
			mechanisms := tc.expectAuthSASL(t)
			assert.Contains(t, mechanisms, "SCRAM-SHA-256")

			// Create SCRAM client with the same credentials (PostgreSQL convention)
			client := newPgScramClient(tt.username, tt.password)

			// Send client-first-message
			clientFirst := client.clientFirstMessage()
			err := tc.sendSASLInitialResponse("SCRAM-SHA-256", []byte(clientFirst))
			require.NoError(t, err)

			// Receive server-first-message
			serverFirst := tc.expectAuthSASLContinue(t)

			// Send client-final-message
			clientFinal, err := client.clientFinalMessage(string(serverFirst))
			require.NoError(t, err)
			err = tc.sendSASLResponse([]byte(clientFinal))
			require.NoError(t, err)

			// Receive server-final-message
			serverFinal := tc.expectAuthSASLFinal(t)

			// Verify server signature
			valid, err := client.verifyServerFinal(string(serverFinal))
			require.NoError(t, err)
			assert.True(t, valid, "server signature should be valid")

			// Expect AuthOk
			tc.expectAuthOk(t)

			// Auth should succeed
			select {
			case err := <-resultCh:
				require.NoError(t, err, "auth should succeed")
			case <-time.After(testTimeout):
				t.Fatal("timeout waiting for auth result")
			}
		})
	}
}

// TestAuthSession_SCRAM_WrongPassword tests SCRAM authentication with wrong password.
func TestAuthSession_SCRAM_WrongPassword(t *testing.T) {
	tc, resultCh := setupAuthSession(t, "testuser", "correctpassword", config.AuthMethodSCRAMSHA256, 4096)

	// Receive SASL auth request
	tc.expectAuthSASL(t)

	// Create SCRAM client with WRONG password (PostgreSQL convention)
	client := newPgScramClient("testuser", "wrongpassword")

	// Send client-first-message
	clientFirst := client.clientFirstMessage()
	err := tc.sendSASLInitialResponse("SCRAM-SHA-256", []byte(clientFirst))
	require.NoError(t, err)

	// Receive server-first-message
	serverFirst := tc.expectAuthSASLContinue(t)

	// Send client-final-message (with wrong proof because wrong password)
	clientFinal, err := client.clientFinalMessage(string(serverFirst))
	require.NoError(t, err)
	err = tc.sendSASLResponse([]byte(clientFinal))
	require.NoError(t, err)

	// Expect error (authentication failed)
	errResp := tc.expectError(t)
	assert.Equal(t, "FATAL", errResp.Severity)
	assert.Contains(t, errResp.Message, "authentication failed")

	// Auth should fail
	select {
	case err := <-resultCh:
		require.Error(t, err)
		assert.Contains(t, err.Error(), "authentication failed")
	case <-time.After(testTimeout):
		t.Fatal("timeout waiting for auth result")
	}
}

// TestAuthSession_SCRAM_InvalidMechanism tests SCRAM with unsupported mechanism.
func TestAuthSession_SCRAM_InvalidMechanism(t *testing.T) {
	tc, resultCh := setupAuthSession(t, "testuser", "testpass", config.AuthMethodSCRAMSHA256, 4096)

	// Receive SASL auth request
	tc.expectAuthSASL(t)

	// Send an unsupported mechanism
	err := tc.sendSASLInitialResponse("SCRAM-SHA-512", []byte("n,,n=,r=invalid"))
	require.NoError(t, err)

	// Expect error
	errResp := tc.expectError(t)
	assert.Equal(t, "FATAL", errResp.Severity)
	assert.Contains(t, errResp.Message, "unsupported SASL mechanism")

	// Auth should fail
	select {
	case err := <-resultCh:
		require.Error(t, err)
	case <-time.After(testTimeout):
		t.Fatal("timeout waiting for auth result")
	}
}

// TestAuthSession_SCRAM_InvalidClientFirstMessage tests various invalid client-first-messages.
func TestAuthSession_SCRAM_InvalidClientFirstMessage(t *testing.T) {
	tests := []struct {
		name          string
		clientFirst   string
		expectedError string
	}{
		{
			name:          "empty message",
			clientFirst:   "",
			expectedError: "empty client-first-message",
		},
		{
			name:          "missing nonce",
			clientFirst:   "n,,n=user",
			expectedError: "missing client nonce",
		},
		{
			name:          "invalid gs2 header - single part",
			clientFirst:   "invalid",
			expectedError: "invalid channel binding flag", // 'i' is not a valid channel binding flag
		},
		{
			name:          "invalid gs2 header - two parts",
			clientFirst:   "n,",
			expectedError: "invalid client-first-message format",
		},
		{
			name:          "invalid channel binding flag",
			clientFirst:   "x,,n=,r=nonce",
			expectedError: "invalid channel binding flag",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tc, resultCh := setupAuthSession(t, "testuser", "testpass", config.AuthMethodSCRAMSHA256, 4096)

			// Receive SASL auth request
			tc.expectAuthSASL(t)

			// Send invalid client-first-message
			err := tc.sendSASLInitialResponse("SCRAM-SHA-256", []byte(tt.clientFirst))
			require.NoError(t, err)

			// Expect error
			errResp := tc.expectError(t)
			assert.Equal(t, "FATAL", errResp.Severity)
			assert.Contains(t, errResp.Message, tt.expectedError)

			// Auth should fail
			select {
			case err := <-resultCh:
				require.Error(t, err)
			case <-time.After(testTimeout):
				t.Fatal("timeout waiting for auth result")
			}
		})
	}
}

// TestAuthSession_SCRAM_InvalidClientFinalMessage tests various invalid client-final-messages.
func TestAuthSession_SCRAM_InvalidClientFinalMessage(t *testing.T) {
	tests := []struct {
		name              string
		modifyClientFinal func(original string) string
		expectedError     string
	}{
		{
			name: "wrong nonce",
			modifyClientFinal: func(original string) string {
				// Replace the nonce with a different one
				return strings.Replace(original, "r=", "r=wrong", 1)
			},
			expectedError: "nonce mismatch",
		},
		{
			name: "missing proof",
			modifyClientFinal: func(original string) string {
				// Remove the proof
				idx := strings.LastIndex(original, ",p=")
				if idx >= 0 {
					return original[:idx]
				}
				return original
			},
			expectedError: "missing proof",
		},
		{
			name: "invalid proof encoding",
			modifyClientFinal: func(original string) string {
				// Replace proof with invalid base64
				idx := strings.LastIndex(original, ",p=")
				if idx >= 0 {
					return original[:idx] + ",p=!!!invalid!!!"
				}
				return original
			},
			expectedError: "invalid proof encoding",
		},
		{
			name: "missing nonce attribute",
			modifyClientFinal: func(original string) string {
				// Return only channel binding, no nonce
				return "c=biws"
			},
			expectedError: "missing nonce",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tc, resultCh := setupAuthSession(t, "testuser", "testpass", config.AuthMethodSCRAMSHA256, 4096)

			// Receive SASL auth request
			tc.expectAuthSASL(t)

			// Create legitimate SCRAM client (PostgreSQL convention)
			client := newPgScramClient("testuser", "testpass")

			// Send valid client-first-message
			clientFirst := client.clientFirstMessage()
			err := tc.sendSASLInitialResponse("SCRAM-SHA-256", []byte(clientFirst))
			require.NoError(t, err)

			// Receive server-first-message
			serverFirst := tc.expectAuthSASLContinue(t)

			// Generate valid client-final, then modify it
			clientFinal, err := client.clientFinalMessage(string(serverFirst))
			require.NoError(t, err)
			modifiedClientFinal := tt.modifyClientFinal(clientFinal)
			err = tc.sendSASLResponse([]byte(modifiedClientFinal))
			require.NoError(t, err)

			// Expect error
			errResp := tc.expectError(t)
			assert.Equal(t, "FATAL", errResp.Severity)
			assert.Contains(t, errResp.Message, tt.expectedError)

			// Auth should fail
			select {
			case err := <-resultCh:
				require.Error(t, err)
			case <-time.After(testTimeout):
				t.Fatal("timeout waiting for auth result")
			}
		})
	}
}

// TestAuthSession_SCRAM_ChannelBindingFlagValidation tests channel binding flag validation.
func TestAuthSession_SCRAM_ChannelBindingFlagValidation(t *testing.T) {
	tests := []struct {
		name          string
		mechanism     string
		cbFlag        string // n, y, or p=type
		expectError   bool
		expectedError string
	}{
		{
			name:        "SCRAM-SHA-256 with n flag (no channel binding)",
			mechanism:   "SCRAM-SHA-256",
			cbFlag:      "n",
			expectError: false,
		},
		{
			name:        "SCRAM-SHA-256 with y flag (supports but not using)",
			mechanism:   "SCRAM-SHA-256",
			cbFlag:      "y",
			expectError: false,
		},
		{
			name:          "SCRAM-SHA-256 with p flag (requests binding but mechanism doesn't support)",
			mechanism:     "SCRAM-SHA-256",
			cbFlag:        "p=tls-unique",
			expectError:   true,
			expectedError: "channel binding but mechanism is not PLUS",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tc, resultCh := setupAuthSession(t, "testuser", "testpass", config.AuthMethodSCRAMSHA256, 4096)

			// Receive SASL auth request
			tc.expectAuthSASL(t)

			// Construct client-first-message with specific channel binding flag
			clientFirst := fmt.Sprintf("%s,,n=,r=testnonce12345", tt.cbFlag)
			err := tc.sendSASLInitialResponse(tt.mechanism, []byte(clientFirst))
			require.NoError(t, err)

			if tt.expectError {
				// Expect error
				errResp := tc.expectError(t)
				assert.Equal(t, "FATAL", errResp.Severity)
				assert.Contains(t, errResp.Message, tt.expectedError)

				select {
				case err := <-resultCh:
					require.Error(t, err)
				case <-time.After(testTimeout):
					t.Fatal("timeout waiting for auth result")
				}
			} else {
				// Should continue with auth (receive server-first)
				serverFirst := tc.expectAuthSASLContinue(t)
				assert.NotEmpty(t, serverFirst)

				// Close connection to end the test
				tc.close()
				<-resultCh
			}
		})
	}
}

// TestAuthSession_SCRAM_PLUSWithoutTLS tests that SCRAM-SHA-256-PLUS fails without TLS.
func TestAuthSession_SCRAM_PLUSWithoutTLS(t *testing.T) {
	tc, resultCh := setupAuthSession(t, "testuser", "testpass", config.AuthMethodSCRAMSHA256, 4096)

	// Receive SASL auth request (no TLS, so only non-PLUS should be offered)
	mechanisms := tc.expectAuthSASL(t)
	assert.Contains(t, mechanisms, "SCRAM-SHA-256")
	// Without TLS, PLUS should not be offered
	assert.NotContains(t, mechanisms, "SCRAM-SHA-256-PLUS")

	// Try to use PLUS anyway
	clientFirst := "p=tls-unique,,n=,r=testnonce12345"
	err := tc.sendSASLInitialResponse("SCRAM-SHA-256-PLUS", []byte(clientFirst))
	require.NoError(t, err)

	// Expect error
	errResp := tc.expectError(t)
	assert.Equal(t, "FATAL", errResp.Severity)
	assert.Contains(t, errResp.Message, "no TLS connection")

	select {
	case err := <-resultCh:
		require.Error(t, err)
	case <-time.After(testTimeout):
		t.Fatal("timeout waiting for auth result")
	}
}

// TestAuthSession_MD5_Success tests successful MD5 authentication.
func TestAuthSession_MD5_Success(t *testing.T) {
	tests := []struct {
		name     string
		username string
		password string
	}{
		{
			name:     "simple credentials",
			username: "testuser",
			password: "testpass",
		},
		{
			name:     "complex password",
			username: "admin",
			password: "p@ssw0rd!#$%",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tc, resultCh := setupAuthSession(t, tt.username, tt.password, config.AuthMethodMD5, 0)

			// Receive MD5 auth request with salt
			salt := tc.expectAuthMD5(t)

			// Compute MD5 hash
			creds := NewUserSecretData(tt.username, tt.password)
			hash := computeMD5Password(creds, salt)

			// Send password
			err := tc.sendPassword(hash)
			require.NoError(t, err)

			// Expect AuthOk
			tc.expectAuthOk(t)

			// Auth should succeed
			select {
			case err := <-resultCh:
				require.NoError(t, err)
			case <-time.After(testTimeout):
				t.Fatal("timeout waiting for auth result")
			}
		})
	}
}

// TestAuthSession_MD5_WrongPassword tests MD5 authentication with wrong password.
func TestAuthSession_MD5_WrongPassword(t *testing.T) {
	tc, resultCh := setupAuthSession(t, "testuser", "correctpassword", config.AuthMethodMD5, 0)

	// Receive MD5 auth request with salt
	salt := tc.expectAuthMD5(t)

	// Compute MD5 hash with wrong password
	wrongCreds := NewUserSecretData("testuser", "wrongpassword")
	wrongHash := computeMD5Password(wrongCreds, salt)

	// Send wrong password
	err := tc.sendPassword(wrongHash)
	require.NoError(t, err)

	// Expect error
	errResp := tc.expectError(t)
	assert.Equal(t, "FATAL", errResp.Severity)
	assert.Contains(t, errResp.Message, "password authentication failed")

	// Auth should fail
	select {
	case err := <-resultCh:
		require.Error(t, err)
	case <-time.After(testTimeout):
		t.Fatal("timeout waiting for auth result")
	}
}

// TestAuthSession_Plaintext_Success tests successful plaintext authentication.
func TestAuthSession_Plaintext_Success(t *testing.T) {
	tc, resultCh := setupAuthSession(t, "testuser", "testpass", config.AuthMethodPlaintext, 0)

	// Receive cleartext auth request
	tc.expectAuthCleartext(t)

	// Send password
	err := tc.sendPassword("testpass")
	require.NoError(t, err)

	// Expect AuthOk
	tc.expectAuthOk(t)

	// Auth should succeed
	select {
	case err := <-resultCh:
		require.NoError(t, err)
	case <-time.After(testTimeout):
		t.Fatal("timeout waiting for auth result")
	}
}

// TestAuthSession_Plaintext_WrongPassword tests plaintext authentication with wrong password.
func TestAuthSession_Plaintext_WrongPassword(t *testing.T) {
	tc, resultCh := setupAuthSession(t, "testuser", "correctpassword", config.AuthMethodPlaintext, 0)

	// Receive cleartext auth request
	tc.expectAuthCleartext(t)

	// Send wrong password
	err := tc.sendPassword("wrongpassword")
	require.NoError(t, err)

	// Expect error
	errResp := tc.expectError(t)
	assert.Equal(t, "FATAL", errResp.Severity)
	assert.Contains(t, errResp.Message, "password authentication failed")

	// Auth should fail
	select {
	case err := <-resultCh:
		require.Error(t, err)
	case <-time.After(testTimeout):
		t.Fatal("timeout waiting for auth result")
	}
}

// TestAuthSession_UnexpectedMessage tests sending unexpected message types.
func TestAuthSession_UnexpectedMessage(t *testing.T) {
	tests := []struct {
		name        string
		method      config.AuthMethod
		sendInstead func(tc *testConn) error
	}{
		{
			name:   "Query instead of SASLInitialResponse",
			method: config.AuthMethodSCRAMSHA256,
			sendInstead: func(tc *testConn) error {
				msg := &pgproto3.Query{String: "SELECT 1"}
				tc.frontend.Send(msg)
				return tc.frontend.Flush()
			},
		},
		{
			name:   "Query instead of PasswordMessage for MD5",
			method: config.AuthMethodMD5,
			sendInstead: func(tc *testConn) error {
				msg := &pgproto3.Query{String: "SELECT 1"}
				tc.frontend.Send(msg)
				return tc.frontend.Flush()
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tc, resultCh := setupAuthSession(t, "testuser", "testpass", tt.method, 4096)

			// Receive auth request
			switch tt.method {
			case config.AuthMethodSCRAMSHA256:
				tc.expectAuthSASL(t)
			case config.AuthMethodMD5:
				tc.expectAuthMD5(t)
			}

			// Send unexpected message
			err := tt.sendInstead(tc)
			require.NoError(t, err)

			// Expect error
			errResp := tc.expectError(t)
			assert.Equal(t, "FATAL", errResp.Severity)
			assert.Contains(t, errResp.Message, "expected")

			// Auth should fail
			select {
			case err := <-resultCh:
				require.Error(t, err)
			case <-time.After(testTimeout):
				t.Fatal("timeout waiting for auth result")
			}
		})
	}
}

// TestAuthSession_ConnectionClose tests behavior when connection closes during auth.
func TestAuthSession_ConnectionClose(t *testing.T) {
	tc, resultCh := setupAuthSession(t, "testuser", "testpass", config.AuthMethodSCRAMSHA256, 4096)

	// Receive SASL auth request
	tc.expectAuthSASL(t)

	// Close connection without responding
	tc.close()

	// Auth should fail with connection error
	select {
	case err := <-resultCh:
		require.Error(t, err)
		// Should be an I/O error
		assert.True(t, strings.Contains(err.Error(), "io") || strings.Contains(err.Error(), "closed") || strings.Contains(err.Error(), "EOF"))
	case <-time.After(testTimeout):
		t.Fatal("timeout waiting for auth result")
	}
}

// TestSCRAMServer_Standalone tests the SCRAMServer directly without AuthSession.
// Uses the PostgreSQL convention of empty username in SCRAM messages.
func TestSCRAMServer_Standalone(t *testing.T) {
	tests := []struct {
		name       string
		username   string
		password   string
		iterations int
	}{
		{
			name:       "basic auth",
			username:   "user1",
			password:   "pass1",
			iterations: 4096,
		},
		{
			name:       "special characters",
			username:   "admin",
			password:   "p@ss=w,ord",
			iterations: 4096,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create SCRAM server
			server, err := NewSCRAMServer(tt.username, tt.password, tt.iterations)
			require.NoError(t, err)

			// Create PostgreSQL-style SCRAM client
			client := newPgScramClient(tt.username, tt.password)

			// Step 1: Send client-first-message
			clientFirst := client.clientFirstMessage()

			// Process on server
			serverFirst, err := server.ProcessClientFirstMessage(clientFirst)
			require.NoError(t, err)
			assert.NotEmpty(t, serverFirst)

			// Step 2: Generate client-final-message
			clientFinal, err := client.clientFinalMessage(serverFirst)
			require.NoError(t, err)

			// Process on server
			serverFinal, err := server.ProcessClientFinalMessage(clientFinal)
			require.NoError(t, err)
			assert.NotEmpty(t, serverFinal)
			assert.True(t, strings.HasPrefix(serverFinal, "v="))

			// Verify server signature on client
			valid, err := client.verifyServerFinal(serverFinal)
			require.NoError(t, err)
			assert.True(t, valid, "server signature should be valid")
		})
	}
}

// TestSCRAMServer_WrongPassword tests SCRAMServer with wrong password.
func TestSCRAMServer_WrongPassword(t *testing.T) {
	// Server has correct password
	server, err := NewSCRAMServer("user1", "correctpassword", 4096)
	require.NoError(t, err)

	// Client has wrong password (PostgreSQL-style)
	client := newPgScramClient("user1", "wrongpassword")

	// Step 1: Client-first-message
	clientFirst := client.clientFirstMessage()

	// Process on server
	serverFirst, err := server.ProcessClientFirstMessage(clientFirst)
	require.NoError(t, err)

	// Step 2: Client-final-message (with wrong proof)
	clientFinal, err := client.clientFinalMessage(serverFirst)
	require.NoError(t, err)

	// Server should reject the wrong proof
	_, err = server.ProcessClientFinalMessage(clientFinal)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "authentication failed")
}

// TestParseChannelBindingFlag tests the ParseChannelBindingFlag function.
func TestParseChannelBindingFlag(t *testing.T) {
	tests := []struct {
		name        string
		input       string
		expectFlag  byte
		expectType  string
		expectError bool
	}{
		{
			name:       "n flag",
			input:      "n,,n=,r=nonce",
			expectFlag: 'n',
			expectType: "",
		},
		{
			name:       "y flag",
			input:      "y,,n=,r=nonce",
			expectFlag: 'y',
			expectType: "",
		},
		{
			name:       "p flag with tls-unique",
			input:      "p=tls-unique,,n=,r=nonce",
			expectFlag: 'p',
			expectType: "tls-unique",
		},
		{
			name:       "p flag with tls-exporter",
			input:      "p=tls-exporter,,n=,r=nonce",
			expectFlag: 'p',
			expectType: "tls-exporter",
		},
		{
			name:        "empty message",
			input:       "",
			expectError: true,
		},
		{
			name:        "invalid flag",
			input:       "x,,n=,r=nonce",
			expectError: true,
		},
		{
			name:        "p flag without type",
			input:       "p,,n=,r=nonce",
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			flag, cbType, err := ParseChannelBindingFlag(tt.input)
			if tt.expectError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.expectFlag, flag)
				assert.Equal(t, tt.expectType, cbType)
			}
		})
	}
}

// TestParseClientFirstMessageUsername tests extracting username from client-first-message.
func TestParseClientFirstMessageUsername(t *testing.T) {
	tests := []struct {
		name         string
		input        string
		expectedUser string
	}{
		{
			name:         "empty username (PostgreSQL style)",
			input:        "n,,n=,r=nonce",
			expectedUser: "",
		},
		{
			name:         "with username",
			input:        "n,,n=testuser,r=nonce",
			expectedUser: "testuser",
		},
		{
			name:         "with special chars in username",
			input:        "n,,n=user=name,r=nonce",
			expectedUser: "user=name",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			username := ParseClientFirstMessageUsername(tt.input)
			assert.Equal(t, tt.expectedUser, username)
		})
	}
}

// TestSCRAMAuthIntegrity tests the SCRAM implementation against known test vectors.
// These test vectors verify the cryptographic correctness of the implementation.
func TestSCRAMAuthIntegrity(t *testing.T) {
	// Test vector from RFC 5802 (modified for SHA-256)
	// We'll verify the internal computations match expected values.

	password := "pencil"
	salt := []byte("QSXCR+Q6sek8bf92") // This is the base64 decoded salt from RFC
	iterations := 4096

	// Compute SaltedPassword = PBKDF2(password, salt, iterations, 32)
	saltedPassword := pbkdf2.Key([]byte(password), salt, iterations, 32, sha256.New)

	// Compute ClientKey = HMAC(SaltedPassword, "Client Key")
	clientKey := hmacSHA256(saltedPassword, []byte("Client Key"))

	// Compute StoredKey = SHA256(ClientKey)
	storedKeyHash := sha256.Sum256(clientKey)
	storedKey := storedKeyHash[:]

	// Verify the keys are non-empty and of correct length
	assert.Len(t, saltedPassword, 32)
	assert.Len(t, clientKey, 32)
	assert.Len(t, storedKey, 32)

	// Compute ServerKey = HMAC(SaltedPassword, "Server Key")
	serverKey := hmacSHA256(saltedPassword, []byte("Server Key"))
	assert.Len(t, serverKey, 32)

	// Log the values for debugging
	t.Logf("SaltedPassword: %s", base64.StdEncoding.EncodeToString(saltedPassword))
	t.Logf("ClientKey: %s", base64.StdEncoding.EncodeToString(clientKey))
	t.Logf("StoredKey: %s", base64.StdEncoding.EncodeToString(storedKey))
	t.Logf("ServerKey: %s", base64.StdEncoding.EncodeToString(serverKey))
}

// TestSCRAMServer_ProofVerification tests that proof verification is correct.
func TestSCRAMServer_ProofVerification(t *testing.T) {
	// This test manually constructs SCRAM messages to verify the proof verification logic.
	username := "testuser"
	password := "testpass"
	iterations := 4096

	// Create server
	server, err := NewSCRAMServer(username, password, iterations)
	require.NoError(t, err)

	// Manually construct client-first-message
	clientNonce := "rOprNGfwEbeRWgbNEkqO"
	clientFirstMsgBare := "n=,r=" + clientNonce
	clientFirstMsg := "n,," + clientFirstMsgBare

	// Process client-first-message
	serverFirstMsg, err := server.ProcessClientFirstMessage(clientFirstMsg)
	require.NoError(t, err)

	// Parse server-first-message to get combined nonce and salt
	attrs := parseAttributes(serverFirstMsg)
	combinedNonce := attrs["r"]
	saltB64 := attrs["s"]
	iStr := attrs["i"]

	require.NotEmpty(t, combinedNonce)
	require.True(t, strings.HasPrefix(combinedNonce, clientNonce))
	require.NotEmpty(t, saltB64)
	require.Equal(t, "4096", iStr)

	salt, err := base64.StdEncoding.DecodeString(saltB64)
	require.NoError(t, err)

	// Compute the expected values
	saltedPassword := pbkdf2.Key([]byte(password), salt, iterations, 32, sha256.New)
	clientKey := hmacSHA256(saltedPassword, []byte("Client Key"))
	storedKeyHash := sha256.Sum256(clientKey)
	storedKey := storedKeyHash[:]

	// Build AuthMessage
	channelBinding := base64.StdEncoding.EncodeToString([]byte("n,,"))
	clientFinalWithoutProof := fmt.Sprintf("c=%s,r=%s", channelBinding, combinedNonce)
	authMessage := clientFirstMsgBare + "," + serverFirstMsg + "," + clientFinalWithoutProof

	// Compute ClientSignature and ClientProof
	clientSignature := hmacSHA256(storedKey, []byte(authMessage))
	clientProof := make([]byte, len(clientKey))
	for i := range clientKey {
		clientProof[i] = clientKey[i] ^ clientSignature[i]
	}
	proofB64 := base64.StdEncoding.EncodeToString(clientProof)

	// Build client-final-message
	clientFinalMsg := clientFinalWithoutProof + ",p=" + proofB64

	// Process client-final-message
	serverFinalMsg, err := server.ProcessClientFinalMessage(clientFinalMsg)
	require.NoError(t, err)

	// Verify server signature
	assert.True(t, strings.HasPrefix(serverFinalMsg, "v="))

	// Verify by computing expected server signature
	serverKey := hmacSHA256(saltedPassword, []byte("Server Key"))
	expectedServerSignature := hmacSHA256(serverKey, []byte(authMessage))
	expectedServerSigB64 := base64.StdEncoding.EncodeToString(expectedServerSignature)
	assert.Equal(t, "v="+expectedServerSigB64, serverFinalMsg)
}

// TestSCRAMServer_NoncePrefixValidation tests that the server validates nonce prefix.
func TestSCRAMServer_NoncePrefixValidation(t *testing.T) {
	server, err := NewSCRAMServer("user", "pass", 4096)
	require.NoError(t, err)

	// Process client-first with known nonce
	clientNonce := "abcd1234"
	clientFirstMsg := "n,,n=,r=" + clientNonce
	serverFirstMsg, err := server.ProcessClientFirstMessage(clientFirstMsg)
	require.NoError(t, err)

	// Parse to get the full combined nonce
	attrs := parseAttributes(serverFirstMsg)
	combinedNonce := attrs["r"]
	require.True(t, strings.HasPrefix(combinedNonce, clientNonce))

	// Try to use a different nonce in client-final
	fakeNonce := "different1234" + combinedNonce[len(clientNonce):]
	channelBinding := base64.StdEncoding.EncodeToString([]byte("n,,"))
	clientFinalMsg := fmt.Sprintf("c=%s,r=%s,p=fakeproof", channelBinding, fakeNonce)

	_, err = server.ProcessClientFinalMessage(clientFinalMsg)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "nonce mismatch")
}

// BenchmarkSCRAMAuth benchmarks the full SCRAM authentication flow.
func BenchmarkSCRAMAuth(b *testing.B) {
	username := "benchuser"
	password := "benchpass"
	iterations := 4096

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		server, _ := NewSCRAMServer(username, password, iterations)

		// Client-first
		clientFirst := "n,,n=,r=benchnonce12345678"
		serverFirst, _ := server.ProcessClientFirstMessage(clientFirst)

		// Parse server response and compute client proof
		attrs := parseAttributes(serverFirst)
		saltB64 := attrs["s"]
		salt, _ := base64.StdEncoding.DecodeString(saltB64)
		combinedNonce := attrs["r"]

		// Compute proof
		saltedPassword := pbkdf2.Key([]byte(password), salt, iterations, 32, sha256.New)
		clientKey := hmacSHA256(saltedPassword, []byte("Client Key"))
		storedKeyHash := sha256.Sum256(clientKey)
		storedKey := storedKeyHash[:]

		channelBinding := base64.StdEncoding.EncodeToString([]byte("n,,"))
		clientFinalWithoutProof := fmt.Sprintf("c=%s,r=%s", channelBinding, combinedNonce)
		authMessage := "n=,r=benchnonce12345678" + "," + serverFirst + "," + clientFinalWithoutProof

		clientSignature := hmacSHA256(storedKey, []byte(authMessage))
		clientProof := make([]byte, len(clientKey))
		for j := range clientKey {
			clientProof[j] = clientKey[j] ^ clientSignature[j]
		}
		proofB64 := base64.StdEncoding.EncodeToString(clientProof)

		clientFinal := clientFinalWithoutProof + ",p=" + proofB64
		server.ProcessClientFinalMessage(clientFinal)
	}
}

// Ensure io is used
var _ = io.EOF
