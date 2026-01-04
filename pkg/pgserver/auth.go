package pgserver

import (
	"context"
	"crypto/rand"
	"crypto/subtle"
	"crypto/tls"
	"fmt"
	"slices"

	"github.com/jackc/pgerrcode"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/justjake/pglink/pkg/pgserver/scram"
	"github.com/justjake/pglink/pkg/pgwire"
)

// PasswordAuthorizer should verify that `conn`'s specified user is valid for the given database.
// If so, return the credentials for the requested user.
// Otherwise, return an error.
type PasswordAuthorizer func(ctx context.Context, conn *UnauthorizedConn) (pgwire.UserSecretData, error)

// PasswordAuthenticator provides implementations of the following password-based auth methods:
// - [PasswordAuthenticator.CleartextPassword]
// - [PasswordAuthenticator.MD5Password]
// - [PasswordAuthenticator.SASL]
type PasswordAuthenticator struct {
	PasswordAuthorizer  PasswordAuthorizer
	SASLMechanisms      []pgwire.SASLMechanism
	SCRAMIterationCount int
}

// TODO: context cancellation handling.

// CleartextPassword authenticates a connection using cleartext password authentication.
// This is insecure unless the connection is protected by TLS.
func (a *PasswordAuthenticator) CleartextPassword(ctx context.Context, conn *UnauthorizedConn) (*AuthorizedConn, error) {
	if err := conn.Frontend.SetAuthType(pgproto3.AuthTypeCleartextPassword); err != nil {
		return nil, fmt.Errorf("failed to set auth type: %w", err)
	}

	if err := conn.Send(ctx, &pgproto3.AuthenticationCleartextPassword{}); err != nil {
		return nil, err
	}

	msg, err := pgwire.Expect[*pgwire.ClientPasswordMessage](conn.Receive(ctx))
	if err != nil {
		return nil, err
	}

	creds, err := a.PasswordAuthorizer(ctx, conn)
	if err != nil {
		return nil, pgwire.NewErr(pgwire.ErrorFatal, pgerrcode.InvalidPassword, "password authentication failed", err)
	}
	if subtle.ConstantTimeCompare([]byte(msg.Parse().Password), []byte(creds.Password())) != 1 {
		return nil, pgwire.NewErr(pgwire.ErrorFatal, pgerrcode.InvalidPassword, "password authentication failed", nil)
	}

	return a.authSuccess(ctx, &AuthorizedConn{
		FrontendConn:   conn.FrontendConn,
		User:           creds.Username(),
		Database:       pgwire.ParameterStatuses(conn.StartupMessage.Parameters).Database(),
		StartupMessage: conn.StartupMessage,
	})
}

// MD5Password authenticates a connection using MD5-hashed password.
// Each attempt uses a random salt, which provides some protection.
// Should only be used if passwords are long random strings, or the connection is protected by TLS.
func (a *PasswordAuthenticator) MD5Password(ctx context.Context, conn *UnauthorizedConn) (*AuthorizedConn, error) {
	if err := conn.Frontend.SetAuthType(pgproto3.AuthTypeMD5Password); err != nil {
		return nil, fmt.Errorf("failed to set auth type: %w", err)
	}

	var salt [4]byte
	if _, err := rand.Read(salt[:]); err != nil {
		return nil, fmt.Errorf("failed to generate salt: %w", err)
	}

	if err := conn.Send(ctx, &pgproto3.AuthenticationMD5Password{Salt: salt}); err != nil {
		return nil, err
	}

	msg, err := pgwire.Expect[*pgwire.ClientPasswordMessage](conn.Receive(ctx))
	if err != nil {
		return nil, err
	}

	creds, err := a.PasswordAuthorizer(ctx, conn)
	if err != nil {
		return nil, pgwire.NewErr(pgwire.ErrorFatal, pgerrcode.InvalidPassword, "password authentication failed", err)
	}

	if subtle.ConstantTimeCompare([]byte(msg.Parse().Password), []byte(pgwire.MD5Password(creds, salt))) != 1 {
		return nil, pgwire.NewErr(pgwire.ErrorFatal, pgerrcode.InvalidPassword, "password authentication failed", nil)
	}

	return a.authSuccess(ctx, &AuthorizedConn{
		FrontendConn:   conn.FrontendConn,
		User:           creds.Username(),
		Database:       pgwire.ParameterStatuses(conn.StartupMessage.Parameters).Database(),
		StartupMessage: conn.StartupMessage,
	})
}

// SASL authenticates a connection using SASL password authentication (SCRAM-SHA-256 and SCRAM-SHA-256-PLUS).
func (a *PasswordAuthenticator) SASL(ctx context.Context, conn *UnauthorizedConn) (*AuthorizedConn, error) {
	if err := conn.Frontend.SetAuthType(pgproto3.AuthTypeSASL); err != nil {
		return nil, fmt.Errorf("failed to set auth type: %w", err)
	}

	// Use TLS state from UnauthorizedConn (set during connection setup)
	connState := conn.TLSState

	mechanisms, err := a.getSASLMechanisms(connState)
	if err != nil {
		return nil, err
	}

	if err := conn.Send(ctx, &pgproto3.AuthenticationSASL{AuthMechanisms: mechanisms}); err != nil {
		return nil, err
	}

	clientFirstMsg, err := pgwire.Expect[*pgwire.ClientSASLInitialResponse](conn.Receive(ctx))
	if err != nil {
		return nil, err
	}

	mechanism := clientFirstMsg.Parse().AuthMechanism
	if !slices.Contains(mechanisms, mechanism) {
		err := pgwire.NewErr(pgwire.ErrorFatal, pgerrcode.InvalidPassword, "unsupported SASL mechanism", nil)
		err.Detail = fmt.Sprintf("supported mechanisms: %v", mechanisms)
		return nil, err
	}

	cbFlag, cbTypeStr, err := scram.ParseChannelBindingFlag(string(clientFirstMsg.Parse().Data))
	if err != nil {
		return nil, pgwire.NewErr(pgwire.ErrorFatal, pgerrcode.InvalidPassword, "invalid channel binding", err)
	}

	if mechanism == pgwire.SASLMechanismSCRAMSHA256Plus {
		if connState == nil {
			return nil, pgwire.NewErr(pgwire.ErrorFatal, pgerrcode.InvalidPassword, "SCRAM-SHA-256-PLUS requested but no TLS connection", nil)
		}

		if cbFlag != scram.SupportedChannelBindingFlag {
			err := pgwire.NewErr(pgwire.ErrorFatal, pgerrcode.InvalidPassword, "SCRAM-SHA-256-PLUS requires channel binding", fmt.Errorf("got flag: %c", cbFlag))
			err.Detail = fmt.Sprintf("expected flag: %c, got: %c", scram.SupportedChannelBindingFlag, cbFlag)
			return nil, err
		}

		// Validate channel binding type (now supports tls-server-end-point, tls-exporter, tls-unique)
		if _, ok := scram.ParseChannelBindingType(cbTypeStr); !ok {
			return nil, pgwire.NewErr(pgwire.ErrorFatal, pgerrcode.InvalidPassword, "unsupported channel binding type", fmt.Errorf("got type: %s", cbTypeStr))
		}
	} else if cbFlag == 'p' {
		return nil, pgwire.NewErr(pgwire.ErrorFatal, pgerrcode.InvalidPassword, "channel binding requested but mechanism is not PLUS", fmt.Errorf("got flag: %c", cbFlag))
	}

	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	creds, err := a.PasswordAuthorizer(ctx, conn)
	if err != nil {
		return nil, pgwire.NewErr(pgwire.ErrorFatal, pgerrcode.InvalidPassword, "password authentication failed", err)
	}

	var scramServer scram.Server
	if mechanism == pgwire.SASLMechanismSCRAMSHA256Plus {
		// Parse the channel binding type (already validated above)
		cbType, _ := scram.ParseChannelBindingType(cbTypeStr)

		// Compute channel binding data using the requested type
		var cbData []byte
		cbData, err = scram.ChannelBindingData(connState, conn.ServerTLSCertificate, cbType)
		if err != nil {
			return nil, pgwire.NewErr(pgwire.ErrorFatal, pgerrcode.InvalidPassword, "failed to compute channel binding data", err)
		}
		scramServer, err = scram.NewSCRAMServerPlus(creds, a.scramIterationCount(), cbData)
	} else {
		scramServer, err = scram.NewSCRAMServer(creds, a.scramIterationCount())
	}
	if err != nil {
		return nil, pgwire.NewErr(pgwire.ErrorFatal, pgerrcode.InvalidPassword, "failed to create SCRAM server", err)
	}

	serverFirstMsg, err := scramServer.ProcessClientFirstMessage(string(clientFirstMsg.Parse().Data))
	if err != nil {
		return nil, pgwire.NewErr(pgwire.ErrorFatal, pgerrcode.InvalidPassword, "failed to process client-first-message", err)
	}

	if err := conn.Send(ctx, &pgproto3.AuthenticationSASLContinue{
		Data: []byte(serverFirstMsg),
	}); err != nil {
		return nil, fmt.Errorf("failed to send SASL continue: %w", err)
	}

	// Update auth type so pgproto3 expects SASLResponse (not SASLInitialResponse)
	if err := conn.Frontend.SetAuthType(pgproto3.AuthTypeSASLContinue); err != nil {
		return nil, fmt.Errorf("failed to set auth type to SASLContinue: %w", err)
	}

	clientFinalMsg, err := pgwire.Expect[*pgwire.ClientSASLResponse](conn.Receive(ctx))
	if err != nil {
		return nil, err
	}

	serverFinalMsg, err := scramServer.ProcessClientFinalMessage(string(clientFinalMsg.Parse().Data))
	if err != nil {
		return nil, pgwire.NewErr(pgwire.ErrorFatal, pgerrcode.InvalidPassword, "SCRAM authentication failed", err)
	}

	if err := conn.Send(ctx, &pgproto3.AuthenticationSASLFinal{
		Data: []byte(serverFinalMsg),
	}); err != nil {
		return nil, fmt.Errorf("failed to send SASL final: %w", err)
	}

	return a.authSuccess(ctx, &AuthorizedConn{
		FrontendConn:   conn.FrontendConn,
		User:           creds.Username(),
		Database:       pgwire.ParameterStatuses(conn.StartupMessage.Parameters).Database(),
		StartupMessage: conn.StartupMessage,
	})
}

func (a *PasswordAuthenticator) authSuccess(ctx context.Context, conn *AuthorizedConn) (*AuthorizedConn, error) {
	sendErr := conn.Send(ctx, &pgproto3.AuthenticationOk{})
	if sendErr != nil {
		return nil, sendErr
	}
	return conn, nil
}

func (a *PasswordAuthenticator) getSASLMechanisms(connState *tls.ConnectionState) ([]pgwire.SASLMechanism, error) {
	configured := a.SASLMechanisms
	if configured == nil {
		configured = pgwire.SASLMechanisms
	}

	result := configured
	if connState == nil {
		// No TLS, can only offer non-PLUS
		result = make([]pgwire.SASLMechanism, 0, 1)
		for _, mechanism := range configured {
			if mechanism != pgwire.SASLMechanismSCRAMSHA256Plus {
				result = append(result, mechanism)
			}
		}
	}

	if len(result) == 0 {
		return nil, fmt.Errorf("no SASL mechanism for connection (allowed: %v)", configured)
	}

	return result, nil
}

func (a *PasswordAuthenticator) scramIterationCount() int {
	if a.SCRAMIterationCount > 0 {
		return a.SCRAMIterationCount
	}
	return scram.DefaultSCRAMIterationCount
}
