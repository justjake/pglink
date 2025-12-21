package frontend

import (
	"crypto/rand"
	"crypto/subtle"
	"crypto/tls"
	"encoding/base64"
	"errors"
	"fmt"

	"github.com/cybergarage/go-sasl/sasl/gss"
	"github.com/cybergarage/go-sasl/sasl/scram"
	"github.com/jackc/pgerrcode"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/justjake/pglink/pkg/config"
)

// AuthSession manages client authentication using the PostgreSQL protocol.
type AuthSession struct {
	frontend    *pgproto3.Backend
	credentials UserSecretData
	method      config.AuthMethod
	tlsState    *tls.ConnectionState

	// MD5 state
	md5Salt [4]byte

	// SCRAM state
	scramIterations    int
	scramServer        *scram.Server
	channelBindingData []byte
	channelBindingType ChannelBindingType
	clientGs2Header    *gss.Header
}

// NewAuthSession creates a new AuthSession.
func NewAuthSession(
	frontend *pgproto3.Backend,
	credentials UserSecretData,
	method config.AuthMethod,
	tlsState *tls.ConnectionState,
	scramIterations int,
) (*AuthSession, error) {
	s := &AuthSession{
		frontend:        frontend,
		credentials:     credentials,
		method:          method,
		tlsState:        tlsState,
		scramIterations: scramIterations,
	}

	// Initialize method-specific state
	switch method {
	case config.AuthMethodMD5:
		salt := make([]byte, 4)
		if _, err := rand.Read(salt); err != nil {
			return nil, fmt.Errorf("failed to generate MD5 salt: %w", err)
		}
		copy(s.md5Salt[:], salt)

	case config.AuthMethodSCRAMSHA256, config.AuthMethodSCRAMSHA256Plus:
		if tlsState != nil {
			data, cbType, err := getChannelBindingData(tlsState)
			if err != nil {
				return nil, fmt.Errorf("failed to get channel binding data: %w", err)
			}
			s.channelBindingData = data
			s.channelBindingType = cbType
		}
	}

	return s, nil
}

// Run executes the full authentication flow.
// It sends the appropriate auth request, handles client responses,
// and returns nil on success or an error on failure.
func (s *AuthSession) Run() error {
	switch s.method {
	case config.AuthMethodPlaintext:
		return s.runPlainAuth()
	case config.AuthMethodMD5:
		return s.runMD5Auth()
	case config.AuthMethodSCRAMSHA256, config.AuthMethodSCRAMSHA256Plus:
		return s.runSCRAMAuth()
	default:
		return fmt.Errorf("unsupported auth method: %s", s.method)
	}
}

// runPlainAuth handles cleartext password authentication.
func (s *AuthSession) runPlainAuth() error {
	if err := s.frontend.SetAuthType(pgproto3.AuthTypeCleartextPassword); err != nil {
		return fmt.Errorf("failed to set auth type: %w", err)
	}
	s.frontend.Send(&pgproto3.AuthenticationCleartextPassword{})

	msg, err := s.frontend.Receive()
	if err != nil {
		return fmt.Errorf("failed to receive password: %w", err)
	}

	pwMsg, ok := msg.(*pgproto3.PasswordMessage)
	if !ok {
		return s.authError(fmt.Errorf("expected PasswordMessage, got %T", msg))
	}

	if subtle.ConstantTimeCompare([]byte(pwMsg.Password), []byte(s.credentials.Password())) != 1 {
		return s.authError(errors.New("password authentication failed"))
	}

	return s.authSuccess()
}

// runMD5Auth handles MD5 password authentication.
func (s *AuthSession) runMD5Auth() error {
	if err := s.frontend.SetAuthType(pgproto3.AuthTypeMD5Password); err != nil {
		return fmt.Errorf("failed to set auth type: %w", err)
	}
	s.frontend.Send(&pgproto3.AuthenticationMD5Password{Salt: s.md5Salt})

	msg, err := s.frontend.Receive()
	if err != nil {
		return fmt.Errorf("failed to receive password: %w", err)
	}

	pwMsg, ok := msg.(*pgproto3.PasswordMessage)
	if !ok {
		return s.authError(fmt.Errorf("expected PasswordMessage, got %T", msg))
	}

	expected := computeMD5Password(s.credentials, s.md5Salt)
	if subtle.ConstantTimeCompare([]byte(pwMsg.Password), []byte(expected)) != 1 {
		return s.authError(errors.New("password authentication failed"))
	}

	return s.authSuccess()
}

// runSCRAMAuth handles SCRAM-SHA-256 authentication (with or without channel binding).
func (s *AuthSession) runSCRAMAuth() error {
	if err := s.frontend.SetAuthType(pgproto3.AuthTypeSASL); err != nil {
		return fmt.Errorf("failed to set auth type: %w", err)
	}

	// Send SASL authentication request
	if s.tlsState == nil {
		// No TLS, can only offer non-PLUS
		s.frontend.Send(&pgproto3.AuthenticationSASL{
			AuthMechanisms: []string{scramSASLMechanismSHA256},
		})
	} else {
		// TLS available: offer both PLUS (preferred) and non-PLUS (fallback)
		s.frontend.Send(&pgproto3.AuthenticationSASL{
			AuthMechanisms: []string{scramSASLMechanismSHA256Plus, scramSASLMechanismSHA256},
		})
	}

	// Step 1: Receive SASL initial response (client-first-message)
	msg, err := s.frontend.Receive()
	if err != nil {
		return fmt.Errorf("failed to receive SASL initial response: %w", err)
	}

	initMsg, ok := msg.(*pgproto3.SASLInitialResponse)
	if !ok {
		return s.authError(fmt.Errorf("expected SASLInitialResponse, got %T", msg))
	}

	// Validate mechanism
	mechanism := initMsg.AuthMechanism
	if mechanism != scramSASLMechanismSHA256 && mechanism != scramSASLMechanismSHA256Plus {
		return s.authError(fmt.Errorf("unsupported SASL mechanism: %s", mechanism))
	}

	if mechanism == scramSASLMechanismSHA256Plus && s.tlsState == nil {
		return s.authError(errors.New("channel binding requested but no TLS connection"))
	}

	// Initialize SCRAM server
	if err := s.initSCRAMServer(); err != nil {
		return s.authError(err)
	}

	// Parse client-first-message
	clientFirstMsg, err := scram.NewMessageFromStringWithHeader(string(initMsg.Data))
	if err != nil {
		return s.authError(fmt.Errorf("failed to parse client-first-message: %w", err))
	}

	// Store GS2 header for channel binding verification
	s.clientGs2Header = clientFirstMsg.Header

	// Verify username
	username, hasUsername := clientFirstMsg.Username()
	if !hasUsername {
		return s.authError(errors.New("client-first-message missing username"))
	}
	if username != s.credentials.Username() {
		return s.authError(fmt.Errorf("SCRAM username mismatch: expected %q, got %q",
			s.credentials.Username(), username))
	}

	// Validate channel binding flag
	if err := s.validateChannelBindingFlag(clientFirstMsg, mechanism); err != nil {
		return s.authError(err)
	}

	// Generate server-first-message
	serverFirstMsg, err := s.scramServer.FirstMessageFrom(clientFirstMsg)
	if err != nil {
		return s.authError(fmt.Errorf("failed to process client-first-message: %w", err))
	}

	// Step 2: Send SASL continue (server-first-message)
	s.frontend.Send(&pgproto3.AuthenticationSASLContinue{
		Data: []byte(serverFirstMsg.String()),
	})

	// Step 3: Receive SASL response (client-final-message)
	msg, err = s.frontend.Receive()
	if err != nil {
		return fmt.Errorf("failed to receive SASL response: %w", err)
	}

	respMsg, ok := msg.(*pgproto3.SASLResponse)
	if !ok {
		return s.authError(fmt.Errorf("expected SASLResponse, got %T", msg))
	}

	// Parse and verify client-final-message
	clientFinalMsg, err := scram.NewMessageFromString(string(respMsg.Data))
	if err != nil {
		return s.authError(fmt.Errorf("failed to parse client-final-message: %w", err))
	}

	if err := s.verifyChannelBinding(clientFinalMsg); err != nil {
		return s.authError(err)
	}

	// Verify client proof
	serverFinalMsg, err := s.scramServer.FinalMessageFrom(clientFinalMsg)
	if err != nil {
		return s.authError(fmt.Errorf("SCRAM authentication failed: %w", err))
	}

	// Step 4: Send SASL final (server-final-message)
	s.frontend.Send(&pgproto3.AuthenticationSASLFinal{
		Data: []byte(serverFinalMsg.String()),
	})

	return s.authSuccess()
}

// initSCRAMServer initializes the SCRAM server.
func (s *AuthSession) initSCRAMServer() error {
	server, err := scram.NewServer(
		scram.WithServerCredentialStore(&s.credentials),
		scram.WithServerHashFunc(scram.HashSHA256()),
		scram.WithServerIterationCount(s.scramIterations),
	)
	if err != nil {
		return fmt.Errorf("failed to create SCRAM server: %w", err)
	}
	s.scramServer = server
	return nil
}

// validateChannelBindingFlag validates the channel binding flag in the client message.
func (s *AuthSession) validateChannelBindingFlag(msg *scram.Message, mechanism string) error {
	if !msg.HasHeader() {
		return nil
	}

	cbFlag := msg.CBFlag()
	if mechanism == scramSASLMechanismSHA256Plus {
		if cbFlag != gss.ClientSupportsUsedCBSFlag {
			return fmt.Errorf("SCRAM-SHA-256-PLUS requires channel binding, got flag: %c", cbFlag)
		}
	} else {
		if cbFlag == gss.ClientSupportsUsedCBSFlag {
			return errors.New("client requests channel binding but mechanism is not PLUS")
		}
	}
	return nil
}

// verifyChannelBinding verifies the channel binding data in the client-final-message.
func (s *AuthSession) verifyChannelBinding(clientFinalMsg *scram.Message) error {
	cbData, hasCB := clientFinalMsg.ChannelBindingData()
	if !hasCB {
		return errors.New("client-final-message missing channel binding data")
	}

	clientCBBytes, err := base64.StdEncoding.DecodeString(cbData)
	if err != nil {
		return fmt.Errorf("invalid channel binding data encoding: %w", err)
	}

	var expectedCB []byte
	if s.clientGs2Header != nil {
		gs2Header := s.clientGs2Header.String()
		switch s.clientGs2Header.CBFlag() {
		case gss.ClientSupportsUsedCBSFlag: // 'p' - channel binding used
			if s.channelBindingData == nil {
				return errors.New("channel binding requested but no TLS data available")
			}
			expectedCB = append([]byte(gs2Header), s.channelBindingData...)
		case gss.ClientDoesNotSupportCBSFlag, gss.ClientSupportsCBSFlag: // 'n' or 'y'
			expectedCB = []byte(gs2Header)
		default:
			return fmt.Errorf("invalid channel binding flag: %c", s.clientGs2Header.CBFlag())
		}
	} else {
		expectedCB = []byte("n,,")
	}

	if subtle.ConstantTimeCompare(clientCBBytes, expectedCB) != 1 {
		return errors.New("channel binding verification failed")
	}

	return nil
}

// authSuccess sends AuthenticationOk and returns nil.
func (s *AuthSession) authSuccess() error {
	s.frontend.Send(&pgproto3.AuthenticationOk{})
	return nil
}

// authError sends an error response and returns the error.
func (s *AuthSession) authError(err error) error {
	s.frontend.Send(&pgproto3.ErrorResponse{
		Severity: "FATAL",
		Code:     pgerrcode.InvalidPassword,
		Message:  err.Error(),
	})
	return err
}
