package config

import (
	"encoding/json/v2"
	"testing"
)

func TestParseConfig_ServerListen(t *testing.T) {
	tests := []struct {
		name     string
		json     string
		expected []ListenAddr
	}{
		{
			name:     "port only",
			json:     `{"servers": [{"listen": ["5432"]}]}`,
			expected: []ListenAddr{":5432"},
		},
		{
			name:     "colon port",
			json:     `{"servers": [{"listen": [":5432"]}]}`,
			expected: []ListenAddr{":5432"},
		},
		{
			name:     "host and port",
			json:     `{"servers": [{"listen": ["127.0.0.1:5432"]}]}`,
			expected: []ListenAddr{"127.0.0.1:5432"},
		},
		{
			name:     "multiple addresses",
			json:     `{"servers": [{"listen": ["5432", ":6432", "192.168.1.1:7432"]}]}`,
			expected: []ListenAddr{":5432", ":6432", "192.168.1.1:7432"},
		},
		{
			name:     "empty listen",
			json:     `{"servers": [{"listen": []}]}`,
			expected: []ListenAddr{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg, err := ParseConfig(tt.json)
			if err != nil {
				t.Fatalf("ParseConfig failed: %v", err)
			}

			if len(cfg.Servers) != 1 {
				t.Fatalf("expected 1 server, got %d", len(cfg.Servers))
			}

			got := cfg.Servers[0].Listen
			if len(got) != len(tt.expected) {
				t.Fatalf("expected %d listen addresses, got %d", len(tt.expected), len(got))
			}

			for i, addr := range got {
				if addr != tt.expected[i] {
					t.Errorf("listen[%d]: expected %q, got %q", i, tt.expected[i], addr)
				}
			}
		})
	}
}

func TestListenAddr_String(t *testing.T) {
	addr := ListenAddr(":5432")
	if addr.String() != ":5432" {
		t.Errorf("expected %q, got %q", ":5432", addr.String())
	}
}

func TestParseConfig_MultipleServers(t *testing.T) {
	json := `{
		"servers": [
			{"listen": ["5432"]},
			{"listen": ["6432", "7432"]}
		]
	}`

	cfg, err := ParseConfig(json)
	if err != nil {
		t.Fatalf("ParseConfig failed: %v", err)
	}

	if len(cfg.Servers) != 2 {
		t.Fatalf("expected 2 servers, got %d", len(cfg.Servers))
	}

	if len(cfg.Servers[0].Listen) != 1 || cfg.Servers[0].Listen[0] != ":5432" {
		t.Errorf("server 0: expected [\":5432\"], got %v", cfg.Servers[0].Listen)
	}

	if len(cfg.Servers[1].Listen) != 2 {
		t.Errorf("server 1: expected 2 addresses, got %d", len(cfg.Servers[1].Listen))
	}
}

func TestJSONV2MapIterationOrder(t *testing.T) {
	input := `{"zebra": "1", "apple": "2", "mango": "3", "banana": "4"}`
	expected := []string{"zebra", "apple", "mango", "banana"}

	var m map[string]string
	if err := json.Unmarshal([]byte(input), &m); err != nil {
		t.Fatalf("Unmarshal failed: %v", err)
	}

	var got []string
	for k := range m {
		got = append(got, k)
	}

	t.Logf("Input order:     %v", expected)
	t.Logf("Iteration order: %v", got)

	match := len(got) == len(expected)
	if match {
		for i := range got {
			if got[i] != expected[i] {
				match = false
				break
			}
		}
	}

	if match {
		t.Log("json/v2 DOES preserve input order")
	} else {
		t.Log("json/v2 does NOT preserve input order")
	}
}
