package frontend

import (
	"crypto/rand"
	"crypto/subtle"
	"crypto/tls"
	"errors"
	"fmt"

	"github.com/jackc/pgerrcode"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/justjake/pglink/pkg/config"
)

// AuthSession manages client authentication using the PostgreSQL protocol.
type AuthSession struct {
	frontend    *Frontend
	credentials UserSecretData
	method      config.AuthMethod
	tlsState    *tls.ConnectionState

	// MD5 state
	md5Salt [4]byte

	// SCRAM state
	scramIterations    int
	channelBindingData []byte
	channelBindingType ChannelBindingType
}

// NewAuthSession creates a new AuthSession.
func NewAuthSession(
	frontend *Frontend,
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
	if err := s.frontend.Flush(); err != nil {
		return fmt.Errorf("failed to flush auth request: %w", err)
	}

	msg, err := s.frontend.Receive()
	if err != nil {
		return fmt.Errorf("failed to receive password: %w", err)
	}

	pwMsg, ok := msg.Client().(*pgproto3.PasswordMessage)
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
	if err := s.frontend.Flush(); err != nil {
		return fmt.Errorf("failed to flush auth request: %w", err)
	}

	msg, err := s.frontend.Receive()
	if err != nil {
		return fmt.Errorf("failed to receive password: %w", err)
	}

	pwMsg, ok := msg.Client().(*pgproto3.PasswordMessage)
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
	if err := s.frontend.Flush(); err != nil {
		return fmt.Errorf("failed to flush SASL auth request: %w", err)
	}

	// Step 1: Receive SASL initial response (client-first-message)
	msg, err := s.frontend.Receive()
	if err != nil {
		return fmt.Errorf("failed to receive SASL initial response: %w", err)
	}

	initMsg, ok := msg.Client().(*pgproto3.SASLInitialResponse)
	if !ok {
		return s.authError(fmt.Errorf("expected SASLInitialResponse, got %T", msg))
	}

	// Validate mechanism
	mechanism := initMsg.AuthMechanism
	if mechanism != scramSASLMechanismSHA256 && mechanism != scramSASLMechanismSHA256Plus {
		return s.authError(fmt.Errorf("unsupported SASL mechanism: %s", mechanism))
	}

	usePLUS := mechanism == scramSASLMechanismSHA256Plus
	if usePLUS && s.tlsState == nil {
		return s.authError(errors.New("channel binding requested but no TLS connection"))
	}

	// Parse client-first-message
	clientFirstMsg := string(initMsg.Data)

	// Validate channel binding flag
	cbFlag, cbType, err := ParseChannelBindingFlag(clientFirstMsg)
	if err != nil {
		return s.authError(fmt.Errorf("invalid channel binding: %w", err))
	}

	if usePLUS {
		if cbFlag != 'p' {
			return s.authError(fmt.Errorf("SCRAM-SHA-256-PLUS requires channel binding, got flag: %c", cbFlag))
		}
		// Verify channel binding type is supported
		if cbType != "tls-exporter" && cbType != "tls-unique" {
			return s.authError(fmt.Errorf("unsupported channel binding type: %s", cbType))
		}
	} else {
		if cbFlag == 'p' {
			return s.authError(errors.New("client requests channel binding but mechanism is not PLUS"))
		}
	}

	// Create SCRAM server (with or without channel binding)
	var scramServer interface {
		ProcessClientFirstMessage(string) (string, error)
		ProcessClientFinalMessage(string) (string, error)
	}

	if usePLUS {
		scramServer, err = NewSCRAMServerPlus(
			s.credentials.Username(),
			s.credentials.Password(),
			s.scramIterations,
			s.channelBindingData,
		)
	} else {
		scramServer, err = NewSCRAMServer(
			s.credentials.Username(),
			s.credentials.Password(),
			s.scramIterations,
		)
	}
	if err != nil {
		return s.authError(fmt.Errorf("failed to create SCRAM server: %w", err))
	}

	// Generate server-first-message
	serverFirstMsg, err := scramServer.ProcessClientFirstMessage(clientFirstMsg)
	if err != nil {
		return s.authError(fmt.Errorf("failed to process client-first-message: %w", err))
	}

	// Step 2: Send SASL continue (server-first-message)
	s.frontend.Send(&pgproto3.AuthenticationSASLContinue{
		Data: []byte(serverFirstMsg),
	})
	if err := s.frontend.Flush(); err != nil {
		return fmt.Errorf("failed to flush SASL continue: %w", err)
	}

	// Update auth type so pgproto3 expects SASLResponse (not SASLInitialResponse)
	if err := s.frontend.SetAuthType(pgproto3.AuthTypeSASLContinue); err != nil {
		return fmt.Errorf("failed to set auth type: %w", err)
	}

	// Step 3: Receive SASL response (client-final-message)
	msg, err = s.frontend.Receive()
	if err != nil {
		return fmt.Errorf("failed to receive SASL response: %w", err)
	}

	respMsg, ok := msg.Client().(*pgproto3.SASLResponse)
	if !ok {
		return s.authError(fmt.Errorf("expected SASLResponse, got %T", msg))
	}

	// Process client-final-message and verify proof
	clientFinalMsg := string(respMsg.Data)
	serverFinalMsg, err := scramServer.ProcessClientFinalMessage(clientFinalMsg)
	if err != nil {
		return s.authError(fmt.Errorf("SCRAM authentication failed: %w", err))
	}

	// Step 4: Send SASL final (server-final-message)
	s.frontend.Send(&pgproto3.AuthenticationSASLFinal{
		Data: []byte(serverFinalMsg),
	})
	if err := s.frontend.Flush(); err != nil {
		return fmt.Errorf("failed to flush SASL final: %w", err)
	}

	return s.authSuccess()
}

// authSuccess sends AuthenticationOk and returns nil.
func (s *AuthSession) authSuccess() error {
	s.frontend.Send(&pgproto3.AuthenticationOk{})
	if err := s.frontend.Flush(); err != nil {
		return fmt.Errorf("failed to flush auth ok: %w", err)
	}
	return nil
}

// authError sends an error response and returns the error.
func (s *AuthSession) authError(err error) error {
	s.frontend.Send(&pgproto3.ErrorResponse{
		Severity: "FATAL",
		Code:     pgerrcode.InvalidPassword,
		Message:  err.Error(),
	})
	_ = s.frontend.Flush() // Best effort flush on error
	return err
}
