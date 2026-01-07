package pgwire

//go:generate stringer -type=AuthType

import (
	"fmt"

	"github.com/jackc/pgx/v5/pgproto3"
)

// AuthType is a subtype of [MsgServerAuth] messages.
type AuthType uint32

const (
	AuthTypeOk                AuthType = pgproto3.AuthTypeOk
	AuthTypeCleartextPassword AuthType = pgproto3.AuthTypeCleartextPassword
	AuthTypeMD5Password       AuthType = pgproto3.AuthTypeMD5Password
	AuthTypeGSS               AuthType = pgproto3.AuthTypeGSS
	AuthTypeGSSContinue       AuthType = pgproto3.AuthTypeGSSCont
	AuthTypeSASL              AuthType = pgproto3.AuthTypeSASL
	AuthTypeSASLContinue      AuthType = pgproto3.AuthTypeSASLContinue
	AuthTypeSASLFinal         AuthType = pgproto3.AuthTypeSASLFinal
)

// AuthType is a subtype of [MsgServerAuth] messages.
// Use AuthType to decide which parse method to use.
func (m Authentication) AuthType() (AuthType, error) {
	parser, err := m.Msg().BodyParser()
	if err != nil {
		return 0, err
	}
	t, err := parser.ReadUint32()
	return AuthType(t), err
}

func (m Authentication) Parse() (pgproto3.AuthenticationResponseMessage, error) {
	authType, err := m.AuthType()
	if err != nil {
		return nil, err
	}

	switch authType {
	case AuthTypeOk:
		return escaping(m.ParseOk())
	case AuthTypeCleartextPassword:
		return escaping(m.ParseCleartextPassword())
	case AuthTypeGSS:
		return nil, fmt.Errorf("GSS authentication is not supported")
	case AuthTypeGSSContinue:
		return nil, fmt.Errorf("GSSContinue authentication is not supported")
	case AuthTypeMD5Password:
		return escaping(m.ParseMD5Password())
	case AuthTypeSASL:
		return escaping(m.ParseSASL())
	case AuthTypeSASLContinue:
		return escaping(m.ParseSASLContinue())
	case AuthTypeSASLFinal:
		return escaping(m.ParseSASLFinal())
	default:
		return nil, fmt.Errorf("unexpected pgwire.AuthType: %#v", authType)
	}
}

// ParseOk parses an AuthenticationOk message (AuthType = 0).
func (m Authentication) ParseOk() (out pgproto3.AuthenticationOk, err error) {
	return DecodeMsg[pgproto3.AuthenticationOk](m.Msg())
}

// ParseCleartextPassword parses an AuthenticationCleartextPassword message (AuthType = 3).
func (m Authentication) ParseCleartextPassword() (out pgproto3.AuthenticationCleartextPassword, err error) {
	return DecodeMsg[pgproto3.AuthenticationCleartextPassword](m.Msg())
}

// ParseMD5Password parses an AuthenticationMD5Password message (AuthType = 5).
func (m Authentication) ParseMD5Password() (out pgproto3.AuthenticationMD5Password, err error) {
	return DecodeMsg[pgproto3.AuthenticationMD5Password](m.Msg())
}

// ParseSASL parses an AuthenticationSASL message (AuthType = 10).
func (m Authentication) ParseSASL() (out pgproto3.AuthenticationSASL, err error) {
	return DecodeMsg[pgproto3.AuthenticationSASL](m.Msg())
}

// ParseSASLContinue parses an AuthenticationSASLContinue message (AuthType = 11).
func (m Authentication) ParseSASLContinue() (out pgproto3.AuthenticationSASLContinue, err error) {
	return DecodeMsg[pgproto3.AuthenticationSASLContinue](m.Msg())
}

// ParseSASLFinal parses an AuthenticationSASLFinal message (AuthType = 12).
func (m Authentication) ParseSASLFinal() (out pgproto3.AuthenticationSASLFinal, err error) {
	return DecodeMsg[pgproto3.AuthenticationSASLFinal](m.Msg())
}

func escaping[T any](t T, err error) (*T, error) {
	if err != nil {
		return nil, err
	}
	return &t, nil
}
