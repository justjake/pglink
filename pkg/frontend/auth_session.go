package frontend

import (
	"crypto/rand"
	"crypto/tls"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"net"

	"github.com/cybergarage/go-sasl/sasl/gss"
	"github.com/cybergarage/go-sasl/sasl/scram"
	"github.com/rueian/pgbroker/message"
	"github.com/rueian/pgbroker/proxy"
)

// AuthState represents the current state of authentication.
type AuthState int

const (
	// AuthStateInit is the initial state before authentication begins.
	AuthStateInit AuthState = iota
	// AuthStateWaitingForPassword is waiting for a cleartext or MD5 password.
	AuthStateWaitingForPassword
	// AuthStateSASLInit is waiting for SASL initial response.
	AuthStateSASLInit
	// AuthStateSASL is waiting for SASL final response.
	AuthStateSASL
	// AuthStateComplete means authentication succeeded.
	AuthStateComplete
	// AuthStateFailed means authentication failed.
	AuthStateFailed
)

// AuthSession manages the authentication state for a client connection.
type AuthSession struct {
	// State is the current authentication state.
	State AuthState

	// Method is the authentication method being used.
	Method AuthMethod

	// Credentials holds the expected credentials for verification.
	Credentials UserSecretData

	// TLSState holds the TLS connection state for channel binding.
	// This should be set when using SCRAM-SHA-256-PLUS.
	TLSState *tls.ConnectionState

	// MD5Salt is the salt used for MD5 authentication.
	MD5Salt [4]byte

	// scramServer is the go-sasl SCRAM server instance.
	scramServer *scram.Server

	// serverFirstMessage stores the server's first SCRAM message.
	serverFirstMessage string

	// serverFinalMessage stores the server's final SCRAM message (contains signature).
	serverFinalMessage *scram.Message

	// channelBindingData stores the channel binding data for SCRAM-SHA-256-PLUS.
	channelBindingData []byte

	// channelBindingType stores the type of channel binding being used.
	channelBindingType ChannelBindingType

	// clientGs2Header stores the client's GS2 header for channel binding verification.
	clientGs2Header *gss.Header

	// Error holds any authentication error.
	Error error
}

// NewAuthSession creates a new AuthSession for the given credentials.
func NewAuthSession(creds UserSecretData, method AuthMethod) (*AuthSession, error) {
	session := &AuthSession{
		State:       AuthStateInit,
		Method:      method,
		Credentials: creds,
	}

	// Initialize method-specific state
	switch method {
	case AuthMethodMD5:
		salt := make([]byte, 4)
		if _, err := rand.Read(salt); err != nil {
			return nil, fmt.Errorf("failed to generate MD5 salt: %w", err)
		}
		copy(session.MD5Salt[:], salt)

	case AuthMethodSCRAMSHA256, AuthMethodSCRAMSHA256Plus:
		// SCRAM state is initialized when the first message arrives
	}

	return session, nil
}

// SetTLSState sets the TLS connection state for channel binding.
// This must be called before authentication starts if using SCRAM-SHA-256-PLUS.
func (s *AuthSession) SetTLSState(state *tls.ConnectionState) error {
	s.TLSState = state

	if state != nil && (s.Method == AuthMethodSCRAMSHA256Plus || s.Method == AuthMethodSCRAMSHA256) {
		data, cbType, err := getChannelBindingData(state)
		if err != nil {
			return fmt.Errorf("failed to get channel binding data: %w", err)
		}
		s.channelBindingData = data
		s.channelBindingType = cbType
	}

	return nil
}

// AuthRequest returns the authentication request message to send to the client.
// It also updates ctx.AuthPhase appropriately for SASL authentication.
func (s *AuthSession) AuthRequest(ctx *proxy.Ctx) message.Reader {
	switch s.Method {
	case AuthMethodPlain:
		s.State = AuthStateWaitingForPassword
		return &message.AuthenticationCleartextPassword{ID: 3}

	case AuthMethodMD5:
		s.State = AuthStateWaitingForPassword
		return &message.AuthenticationMD5Password{
			ID:   5,
			Salt: s.MD5Salt[:],
		}

	case AuthMethodSCRAMSHA256:
		s.State = AuthStateSASLInit
		ctx.AuthPhase = proxy.PhaseSASLInit
		return &message.AuthenticationSASL{
			ID:         10,
			Mechanisms: []string{scramSASLMechanismSHA256},
		}

	case AuthMethodSCRAMSHA256Plus:
		s.State = AuthStateSASLInit
		ctx.AuthPhase = proxy.PhaseSASLInit
		// Offer both mechanisms; PLUS is preferred
		mechanisms := []string{scramSASLMechanismSHA256Plus}
		if s.TLSState == nil {
			// Fallback to non-PLUS if no TLS
			mechanisms = []string{scramSASLMechanismSHA256}
		}
		return &message.AuthenticationSASL{
			ID:         10,
			Mechanisms: mechanisms,
		}

	default:
		s.State = AuthStateFailed
		s.Error = fmt.Errorf("unsupported auth method: %s", s.Method)
		return nil
	}
}

// SendAuthRequest writes the authentication request to the client connection.
func (s *AuthSession) SendAuthRequest(ctx *proxy.Ctx, conn net.Conn) error {
	req := s.AuthRequest(ctx)
	if req == nil {
		return s.Error
	}
	_, err := io.Copy(conn, req.Reader())
	return err
}

// HandlePasswordMessage processes a password message from the client.
// This is used for both cleartext and MD5 authentication.
func (s *AuthSession) HandlePasswordMessage(ctx *proxy.Ctx, msg *message.PasswordMessage) (*message.PasswordMessage, error) {
	if s.State != AuthStateWaitingForPassword {
		s.State = AuthStateFailed
		s.Error = errors.New("unexpected password message")
		return nil, s.Error
	}

	var valid bool
	switch s.Method {
	case AuthMethodPlain:
		valid = msg.Password == s.Credentials.Password()

	case AuthMethodMD5:
		expected := computeMD5Password(s.Credentials, s.MD5Salt)
		valid = msg.Password == expected

	default:
		s.State = AuthStateFailed
		s.Error = fmt.Errorf("password message not valid for auth method: %s", s.Method)
		return nil, s.Error
	}

	if valid {
		s.State = AuthStateComplete
		return nil, nil // Don't forward to backend - we handled auth ourselves
	}

	s.State = AuthStateFailed
	s.Error = errors.New("password authentication failed")
	return nil, s.Error
}

// initSCRAMServer initializes the SCRAM server with go-sasl.
func (s *AuthSession) initSCRAMServer(mechanism string) error {
	// Create credential store
	credStore := newCredentialStore(s.Credentials)

	// Create SCRAM server with options
	opts := []scram.ServerOption{
		scram.WithServerCredentialStore(credStore),
		scram.WithServerHashFunc(scram.HashSHA256()),
		scram.WithServerIterationCount(4096),
	}

	server, err := scram.NewServer(opts...)
	if err != nil {
		return fmt.Errorf("failed to create SCRAM server: %w", err)
	}
	s.scramServer = server

	return nil
}

// HandleSASLInitialResponse processes the SASL initial response (client-first-message).
func (s *AuthSession) HandleSASLInitialResponse(ctx *proxy.Ctx, msg *message.SASLInitialResponse) (*message.SASLInitialResponse, error) {
	if s.State != AuthStateSASLInit {
		s.State = AuthStateFailed
		s.Error = errors.New("unexpected SASL initial response")
		return nil, s.Error
	}

	// Validate mechanism
	mechanism := msg.Mechanism
	if mechanism != scramSASLMechanismSHA256 && mechanism != scramSASLMechanismSHA256Plus {
		s.State = AuthStateFailed
		s.Error = fmt.Errorf("unsupported SASL mechanism: %s", mechanism)
		return nil, s.Error
	}

	// If client requests PLUS but we don't have TLS, reject
	if mechanism == scramSASLMechanismSHA256Plus && s.TLSState == nil {
		s.State = AuthStateFailed
		s.Error = errors.New("channel binding requested but no TLS connection")
		return nil, s.Error
	}

	// Initialize SCRAM server
	if err := s.initSCRAMServer(mechanism); err != nil {
		s.State = AuthStateFailed
		s.Error = err
		return nil, s.Error
	}

	// Parse client-first-message
	clientFirstPayload := string(msg.Response.DataBytes())

	// Parse to extract the GS2 header for channel binding verification
	parsedMsg, err := scram.NewMessageFromStringWithHeader(clientFirstPayload)
	if err != nil {
		s.State = AuthStateFailed
		s.Error = fmt.Errorf("failed to parse client-first-message: %w", err)
		return nil, s.Error
	}

	// Store GS2 header for channel binding verification in final message
	s.clientGs2Header = parsedMsg.Header

	// Verify username matches
	username, hasUsername := parsedMsg.Username()
	if !hasUsername {
		s.State = AuthStateFailed
		s.Error = errors.New("client-first-message missing username")
		return nil, s.Error
	}
	if username != s.Credentials.Username() {
		s.State = AuthStateFailed
		s.Error = fmt.Errorf("SCRAM username mismatch: expected %q, got %q", s.Credentials.Username(), username)
		return nil, s.Error
	}

	// Validate channel binding flag
	if parsedMsg.HasHeader() {
		cbFlag := parsedMsg.CBFlag()
		if mechanism == scramSASLMechanismSHA256Plus {
			// For PLUS, client must use 'p' flag
			if cbFlag != gss.ClientSupportsUsedCBSFlag {
				s.State = AuthStateFailed
				s.Error = fmt.Errorf("SCRAM-SHA-256-PLUS requires channel binding, got flag: %c", cbFlag)
				return nil, s.Error
			}
		} else {
			// For non-PLUS, client should use 'n' or 'y'
			if cbFlag == gss.ClientSupportsUsedCBSFlag {
				s.State = AuthStateFailed
				s.Error = errors.New("client requests channel binding but mechanism is not PLUS")
				return nil, s.Error
			}
		}
	}

	// Process client-first-message using go-sasl
	serverResp, err := s.scramServer.FirstMessageFrom(parsedMsg)
	if err != nil {
		s.State = AuthStateFailed
		s.Error = fmt.Errorf("failed to process client-first-message: %w", err)
		return nil, s.Error
	}

	s.serverFirstMessage = serverResp.String()
	s.State = AuthStateSASL
	ctx.AuthPhase = proxy.PhaseSASL

	return nil, nil // Don't forward to backend - we handled auth ourselves
}

// SASLContinue returns the SASL continue message (server-first-message) to send to the client.
func (s *AuthSession) SASLContinue() message.Reader {
	if s.State != AuthStateSASL || s.scramServer == nil {
		return nil
	}

	return &message.AuthenticationSASLContinue{
		ID:   11,
		Data: []byte(s.serverFirstMessage),
	}
}

// SendSASLContinue writes the SASL continue message to the client connection.
func (s *AuthSession) SendSASLContinue(conn net.Conn) error {
	msg := s.SASLContinue()
	if msg == nil {
		return errors.New("not in SASL continue state")
	}
	_, err := io.Copy(conn, msg.Reader())
	return err
}

// HandleSASLResponse processes the SASL response (client-final-message).
func (s *AuthSession) HandleSASLResponse(ctx *proxy.Ctx, msg *message.SASLResponse) (*message.SASLResponse, error) {
	if s.State != AuthStateSASL {
		s.State = AuthStateFailed
		s.Error = errors.New("unexpected SASL response")
		return nil, s.Error
	}

	// Parse client-final-message
	clientFinalPayload := string(msg.Data)
	clientFinalMsg, err := scram.NewMessageFromString(clientFinalPayload)
	if err != nil {
		s.State = AuthStateFailed
		s.Error = fmt.Errorf("failed to parse client-final-message: %w", err)
		return nil, s.Error
	}

	// Verify channel binding
	if err := s.verifyChannelBinding(clientFinalMsg); err != nil {
		s.State = AuthStateFailed
		s.Error = err
		return nil, s.Error
	}

	// Verify client proof using go-sasl - this returns the server-final-message
	serverFinalMsg, err := s.scramServer.FinalMessageFrom(clientFinalMsg)
	if err != nil {
		s.State = AuthStateFailed
		s.Error = fmt.Errorf("SCRAM authentication failed: %w", err)
		return nil, s.Error
	}

	// Store server final message for later use in SASLFinal
	s.serverFinalMessage = serverFinalMsg

	s.State = AuthStateComplete
	ctx.AuthPhase = proxy.PhaseOK

	return nil, nil // Don't forward to backend - we handled auth ourselves
}

// verifyChannelBinding verifies the channel binding data in the client-final-message.
func (s *AuthSession) verifyChannelBinding(clientFinalMsg *scram.Message) error {
	// Get channel binding data from client-final-message
	cbData, hasCB := clientFinalMsg.ChannelBindingData()
	if !hasCB {
		return errors.New("client-final-message missing channel binding data")
	}

	// Decode the base64-encoded channel binding data
	clientCBBytes, err := base64.StdEncoding.DecodeString(cbData)
	if err != nil {
		return fmt.Errorf("invalid channel binding data encoding: %w", err)
	}

	// Build expected channel binding data
	// Format: gs2-header + channel-binding-data
	var expectedCB []byte

	if s.clientGs2Header != nil {
		cbFlag := s.clientGs2Header.CBFlag()
		switch cbFlag {
		case gss.ClientSupportsUsedCBSFlag: // 'p'
			// Channel binding is used
			if s.channelBindingData == nil {
				return errors.New("channel binding requested but no TLS data available")
			}

			// Build expected: gs2-header + cb-data
			gs2Header := s.clientGs2Header.String()
			expectedCB = append([]byte(gs2Header), s.channelBindingData...)

		case gss.ClientDoesNotSupportCBSFlag: // 'n'
			// No channel binding
			gs2Header := s.clientGs2Header.String()
			expectedCB = []byte(gs2Header)

		case gss.ClientSupportsCBSFlag: // 'y'
			// Client supports CB but not used
			gs2Header := s.clientGs2Header.String()
			expectedCB = []byte(gs2Header)

		default:
			return fmt.Errorf("invalid channel binding flag: %c", cbFlag)
		}
	} else {
		// No GS2 header stored, assume "n,,"
		expectedCB = []byte("n,,")
	}

	// Compare using constant-time comparison
	if !constantTimeCompare(clientCBBytes, expectedCB) {
		return errors.New("channel binding verification failed")
	}

	return nil
}

// constantTimeCompare compares two byte slices in constant time.
func constantTimeCompare(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	var result byte
	for i := range a {
		result |= a[i] ^ b[i]
	}
	return result == 0
}

// SASLFinal returns the SASL final message (server-final-message) to send to the client.
func (s *AuthSession) SASLFinal() message.Reader {
	if s.State != AuthStateComplete || s.serverFinalMessage == nil {
		return nil
	}

	// The server final message is already formatted by go-sasl
	return &message.AuthenticationSASLFinal{
		ID:   12,
		Data: []byte(s.serverFinalMessage.String()),
	}
}

// SendSASLFinal writes the SASL final message to the client connection.
func (s *AuthSession) SendSASLFinal(conn net.Conn) error {
	msg := s.SASLFinal()
	if msg == nil {
		return errors.New("not in SASL final state")
	}
	_, err := io.Copy(conn, msg.Reader())
	return err
}

// AuthOK returns the authentication OK message.
func (s *AuthSession) AuthOK() message.Reader {
	return &message.AuthenticationOk{ID: 0}
}

// SendAuthOK writes the authentication OK message to the client connection.
func (s *AuthSession) SendAuthOK(conn net.Conn) error {
	_, err := io.Copy(conn, s.AuthOK().Reader())
	return err
}

// IsComplete returns true if authentication is complete and successful.
func (s *AuthSession) IsComplete() bool {
	return s.State == AuthStateComplete
}

// IsFailed returns true if authentication has failed.
func (s *AuthSession) IsFailed() bool {
	return s.State == AuthStateFailed
}

// ErrorResponse returns an ErrorResponse message for authentication failure.
func (s *AuthSession) ErrorResponse() *message.ErrorResponse {
	errMsg := "authentication failed"
	if s.Error != nil {
		errMsg = s.Error.Error()
	}

	return &message.ErrorResponse{
		Fields: []message.ErrorField{
			{Type: 'S', Value: "FATAL"},
			{Type: 'V', Value: "FATAL"},
			{Type: 'C', Value: "28P01"}, // invalid_password
			{Type: 'M', Value: errMsg},
		},
	}
}

// SendError writes an authentication error response to the client connection.
func (s *AuthSession) SendError(conn net.Conn) error {
	_, err := io.Copy(conn, s.ErrorResponse().Reader())
	return err
}
