package config

import (
	"encoding/json/jsontext"
	"encoding/json/v2"
	"iter"
	"strings"
)

// ServerConfig configures a single server instance.
type ServerConfig struct {
	Listen               []ListenAddr    `json:"listen"`
	Database             string          `json:"database"`
	Users                []UserConfig    `json:"users"`
	Backend              BackendConfig   `json:"backend"`
	TrackExtraParameters map[string]bool `json:"track_extra_parameters,omitempty"`
}

// UserConfig configures authentication credentials for a user.
type UserConfig struct {
	Username SecretRef `json:"username"`
	Password SecretRef `json:"password"`
}

// BackendConfig configures the backend PostgreSQL server to proxy to.
type BackendConfig struct {
	Host                     string              `json:"host"`
	Port                     uint16              `json:"port"`
	MaxConnections           uint                `json:"max_connections"`
	DefaultStartupParameters PgStartupParameters `json:"default_startup_parameters,omitempty"`
}

// PgStartupParameters is a map of PostgreSQL startup parameters
// that preserves insertion order (i.e., the order from the JSON file).
type PgStartupParameters struct {
	keys   []string
	values map[string]string
}

// All returns an iterator over parameters in insertion order.
func (p *PgStartupParameters) All() iter.Seq2[string, string] {
	return func(yield func(string, string) bool) {
		for _, k := range p.keys {
			if !yield(k, p.values[k]) {
				return
			}
		}
	}
}

// UnmarshalJSON parses a JSON object, preserving key order from the file.
func (p *PgStartupParameters) UnmarshalJSON(data []byte) error {
	p.keys = nil
	p.values = make(map[string]string)

	dec := jsontext.NewDecoder(strings.NewReader(string(data)))
	tok, err := dec.ReadToken()
	if err != nil || tok.Kind() != '{' {
		return err
	}

	for dec.PeekKind() != '}' {
		keyTok, err := dec.ReadToken()
		if err != nil {
			return err
		}
		key := keyTok.String()

		valTok, err := dec.ReadToken()
		if err != nil {
			return err
		}
		val := valTok.String()

		p.keys = append(p.keys, key)
		p.values[key] = val
	}
	return nil
}

// MarshalJSON serializes parameters in insertion order.
func (p PgStartupParameters) MarshalJSON() ([]byte, error) {
	var b strings.Builder
	b.WriteByte('{')
	for i, k := range p.keys {
		if i > 0 {
			b.WriteByte(',')
		}
		keyBytes, _ := json.Marshal(k)
		valBytes, _ := json.Marshal(p.values[k])
		b.Write(keyBytes)
		b.WriteByte(':')
		b.Write(valBytes)
	}
	b.WriteByte('}')
	return []byte(b.String()), nil
}

// ListenAddr is a network address suitable for net.Listen.
// It normalizes JSON input formats like "5432", ":5432", or "127.0.0.1:5432"
// into the "host:port" format expected by Go's net package.
type ListenAddr string

// UnmarshalJSON parses a listen address string and normalizes it.
func (l *ListenAddr) UnmarshalJSON(data []byte) error {
	var s string
	if err := json.Unmarshal(data, &s); err != nil {
		return err
	}
	*l = ListenAddr(normalizeListenAddr(s))
	return nil
}

// String returns the normalized address string.
func (l ListenAddr) String() string {
	return string(l)
}

// normalizeListenAddr converts various address formats to "host:port".
// Accepts: "5432", ":5432", "127.0.0.1:5432"
func normalizeListenAddr(s string) string {
	if !strings.Contains(s, ":") {
		// Just a port number like "5432"
		return ":" + s
	}
	return s
}
