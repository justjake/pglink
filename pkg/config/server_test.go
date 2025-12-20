package config

import (
	"encoding/json/v2"
	"testing"
)

func TestPgStartupParameters_RoundTrip(t *testing.T) {
	tests := []struct {
		name string
		json string
	}{
		{
			name: "empty",
			json: `{}`,
		},
		{
			name: "single param",
			json: `{"application_name":"pglink"}`,
		},
		{
			name: "multiple params preserve order",
			json: `{"zebra":"1","apple":"2","mango":"3"}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var p PgStartupParameters
			if err := json.Unmarshal([]byte(tt.json), &p); err != nil {
				t.Fatalf("Unmarshal failed: %v", err)
			}

			got, err := json.Marshal(p)
			if err != nil {
				t.Fatalf("Marshal failed: %v", err)
			}

			if string(got) != tt.json {
				t.Errorf("round-trip mismatch:\n  input:  %s\n  output: %s", tt.json, string(got))
			}
		})
	}
}

func TestListenAddr_RoundTrip(t *testing.T) {
	tests := []struct {
		name     string
		json     string
		expected string // normalized output
	}{
		{
			name:     "port only normalizes",
			json:     `"5432"`,
			expected: `":5432"`,
		},
		{
			name:     "colon port unchanged",
			json:     `":5432"`,
			expected: `":5432"`,
		},
		{
			name:     "host and port unchanged",
			json:     `"127.0.0.1:5432"`,
			expected: `"127.0.0.1:5432"`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var l ListenAddr
			if err := json.Unmarshal([]byte(tt.json), &l); err != nil {
				t.Fatalf("Unmarshal failed: %v", err)
			}

			got, err := json.Marshal(l)
			if err != nil {
				t.Fatalf("Marshal failed: %v", err)
			}

			if string(got) != tt.expected {
				t.Errorf("round-trip mismatch:\n  input:    %s\n  expected: %s\n  got:      %s", tt.json, tt.expected, string(got))
			}
		})
	}
}

func TestUserConfig_RoundTrip(t *testing.T) {
	input := `{"username":{"aws_secret_arn":"arn:aws:secretsmanager:us-east-1:123:secret:db","key":"user"},"password":{"aws_secret_arn":"arn:aws:secretsmanager:us-east-1:123:secret:db","key":"pass"}}`

	var u UserConfig
	if err := json.Unmarshal([]byte(input), &u); err != nil {
		t.Fatalf("Unmarshal failed: %v", err)
	}

	got, err := json.Marshal(u)
	if err != nil {
		t.Fatalf("Marshal failed: %v", err)
	}

	if string(got) != input {
		t.Errorf("round-trip mismatch:\n  input:  %s\n  output: %s", input, string(got))
	}
}

func TestBackendConfig_RoundTrip(t *testing.T) {
	input := `{"host":"db.example.com","port":5432,"max_connections":100,"default_startup_parameters":{"application_name":"pglink","timezone":"UTC"}}`

	var b BackendConfig
	if err := json.Unmarshal([]byte(input), &b); err != nil {
		t.Fatalf("Unmarshal failed: %v", err)
	}

	got, err := json.Marshal(b)
	if err != nil {
		t.Fatalf("Marshal failed: %v", err)
	}

	if string(got) != input {
		t.Errorf("round-trip mismatch:\n  input:  %s\n  output: %s", input, string(got))
	}
}

func TestServerConfig_RoundTrip(t *testing.T) {
	input := `{"listen":[":5432",":6432"],"database":"mydb","users":[{"username":{"aws_secret_arn":"arn:aws:secretsmanager:us-east-1:123:secret:db","key":"user"},"password":{"aws_secret_arn":"arn:aws:secretsmanager:us-east-1:123:secret:db","key":"pass"}}],"backend":{"host":"db.example.com","port":5432,"max_connections":100,"default_startup_parameters":{"application_name":"pglink"}},"track_extra_parameters":{"TimeZone":true,"client_encoding":true}}`

	var s ServerConfig
	if err := json.Unmarshal([]byte(input), &s); err != nil {
		t.Fatalf("Unmarshal failed: %v", err)
	}

	got, err := json.Marshal(s)
	if err != nil {
		t.Fatalf("Marshal failed: %v", err)
	}

	if string(got) != input {
		t.Errorf("round-trip mismatch:\n  input:  %s\n  output: %s", input, string(got))
	}
}
