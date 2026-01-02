package pgwire

import (
	"crypto/md5"
	"errors"
	"fmt"
)

type SASLMechanism = string

const (
	SASLMechanismSCRAMSHA256     SASLMechanism = "SCRAM-SHA-256"
	SASLMechanismSCRAMSHA256Plus SASLMechanism = "SCRAM-SHA-256-PLUS"
)

var SASLMechanisms = []SASLMechanism{
	SASLMechanismSCRAMSHA256Plus,
	SASLMechanismSCRAMSHA256,
}

// UserSecretData holds user credentials.
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
	return u.String()
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

// errUserSecretDataMarshal is returned when attempting to marshal UserSecretData.
var errUserSecretDataMarshal = errors.New("UserSecretData must not be serialized")

// MarshalJSON always returns an error to prevent accidental serialization of credentials.
func (u UserSecretData) MarshalJSON() ([]byte, error) {
	return nil, errUserSecretDataMarshal
}

// MarshalText always returns an error to prevent accidental serialization of credentials.
func (u UserSecretData) MarshalText() ([]byte, error) {
	return nil, errUserSecretDataMarshal
}

// MD5Password computes the MD5 password hash in PostgreSQL format.
// Format: "md5" + md5(md5(password + user) + salt)
func MD5Password(creds UserSecretData, salt [4]byte) string {
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
