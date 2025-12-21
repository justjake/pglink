package frontend

import (
	"crypto/md5"
	"crypto/tls"
	"fmt"

	"github.com/cybergarage/go-sasl/sasl/auth"
)

// AuthMethod represents the authentication method to use.
type AuthMethod int

const (
	AuthMethodPlain AuthMethod = iota
	AuthMethodMD5
	AuthMethodSCRAMSHA256
	AuthMethodSCRAMSHA256Plus
)

func (m AuthMethod) String() string {
	switch m {
	case AuthMethodPlain:
		return "plain"
	case AuthMethodMD5:
		return "md5"
	case AuthMethodSCRAMSHA256:
		return "scram-sha-256"
	case AuthMethodSCRAMSHA256Plus:
		return "scram-sha-256-plus"
	default:
		return "unknown"
	}
}

const (
	scramSASLMechanismSHA256     = "SCRAM-SHA-256"
	scramSASLMechanismSHA256Plus = "SCRAM-SHA-256-PLUS"
)

// UserSecretData holds user credentials securely.
// The password is never printed in logs or string representations.
type UserSecretData struct {
	username string
	password string
}

// NewUserSecretData creates a new UserSecretData with the given username and password.
func NewUserSecretData(username, password string) UserSecretData {
	return UserSecretData{
		username: username,
		password: password,
	}
}

// Username returns the username.
func (u UserSecretData) Username() string {
	return u.username
}

// Password returns the password.
// Use this method only when the password is actually needed for authentication.
func (u UserSecretData) Password() string {
	return u.password
}

// String returns a safe string representation that never includes the password.
func (u UserSecretData) String() string {
	return fmt.Sprintf("UserSecretData{username: %q, password: [REDACTED]}", u.username)
}

// GoString returns a safe string for %#v formatting that never includes the password.
func (u UserSecretData) GoString() string {
	return fmt.Sprintf("UserSecretData{username: %q, password: [REDACTED]}", u.username)
}

// Format implements fmt.Formatter to ensure the password is never printed.
func (u UserSecretData) Format(f fmt.State, verb rune) {
	switch verb {
	case 'v':
		if f.Flag('+') || f.Flag('#') {
			_, _ = fmt.Fprintf(f, "UserSecretData{username: %q, password: [REDACTED]}", u.username)
		} else {
			_, _ = fmt.Fprintf(f, "{%s [REDACTED]}", u.username)
		}
	case 's':
		_, _ = fmt.Fprintf(f, "{%s [REDACTED]}", u.username)
	default:
		_, _ = fmt.Fprintf(f, "{%s [REDACTED]}", u.username)
	}
}

// MarshalJSON returns a JSON representation that never includes the password.
func (u UserSecretData) MarshalJSON() ([]byte, error) {
	return []byte(fmt.Sprintf(`{"username":%q,"password":"[REDACTED]"}`, u.username)), nil
}

// MarshalText returns a text representation that never includes the password.
func (u UserSecretData) MarshalText() ([]byte, error) {
	return []byte(u.String()), nil
}

// computeMD5Password computes the MD5 password hash.
// Format: "md5" + md5(md5(password + user) + salt)
func computeMD5Password(creds UserSecretData, salt [4]byte) string {
	// First hash: md5(password + user)
	h1 := md5.New()
	h1.Write([]byte(creds.Password()))
	h1.Write([]byte(creds.Username()))
	inner := fmt.Sprintf("%x", h1.Sum(nil))

	// Second hash: md5(inner + salt)
	h2 := md5.New()
	h2.Write([]byte(inner))
	h2.Write(salt[:])
	return "md5" + fmt.Sprintf("%x", h2.Sum(nil))
}

// credentialStore implements auth.CredentialStore for SCRAM authentication.
type credentialStore struct {
	creds UserSecretData
}

// newCredentialStore creates a new credential store for the given credentials.
func newCredentialStore(creds UserSecretData) *credentialStore {
	return &credentialStore{creds: creds}
}

// LookupCredential implements auth.CredentialStore.
func (cs *credentialStore) LookupCredential(q auth.Query) (auth.Credential, bool, error) {
	// Verify username matches
	if q.Username() != cs.creds.Username() {
		return nil, false, nil
	}

	// Return credential with the password
	cred := auth.NewCredential(
		auth.WithCredentialUsername(cs.creds.Username()),
		auth.WithCredentialPassword(cs.creds.Password()),
	)
	return cred, true, nil
}

// ChannelBindingType represents the type of channel binding to use.
type ChannelBindingType int

const (
	// ChannelBindingNone means no channel binding is used.
	ChannelBindingNone ChannelBindingType = iota
	// ChannelBindingTLSUnique uses tls-unique channel binding (deprecated in TLS 1.3).
	ChannelBindingTLSUnique
	// ChannelBindingTLSExporter uses tls-exporter channel binding (TLS 1.3+).
	ChannelBindingTLSExporter
)

// getChannelBindingData extracts channel binding data from a TLS connection state.
// For TLS 1.3+, it uses tls-exporter; for earlier versions, it uses tls-unique.
func getChannelBindingData(tlsState *tls.ConnectionState) ([]byte, ChannelBindingType, error) {
	if tlsState == nil {
		return nil, ChannelBindingNone, nil
	}

	// TLS 1.3 uses tls-exporter
	if tlsState.Version >= tls.VersionTLS13 {
		// The label and context are per RFC 9266
		data, err := tlsState.ExportKeyingMaterial("EXPORTER-Channel-Binding", nil, 32)
		if err != nil {
			return nil, ChannelBindingNone, fmt.Errorf("failed to export keying material: %w", err)
		}
		return data, ChannelBindingTLSExporter, nil
	}

	// TLS 1.2 and earlier use tls-unique (the finished message)
	// Note: tls-unique is not available in Go's TLS package directly
	// We need to use the TLSUnique field from ConnectionState
	if len(tlsState.TLSUnique) > 0 {
		return tlsState.TLSUnique, ChannelBindingTLSUnique, nil
	}

	return nil, ChannelBindingNone, nil
}
