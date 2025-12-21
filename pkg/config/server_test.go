package config

import (
	"encoding/json/v2"
	"testing"
)

func ptr[T any](v T) *T { return &v }

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
	input := `{"host":"db.example.com","port":5432,"database":"mydb","pool_max_conns":100,"pool_max_conn_lifetime":"1h","default_startup_parameters":{"application_name":"pglink","timezone":"UTC"}}`

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

func TestDatabaseConfig_RoundTrip(t *testing.T) {
	// Note: Database field has json:"-" so it's not included in JSON
	input := `{"users":[{"username":{"aws_secret_arn":"arn:aws:secretsmanager:us-east-1:123:secret:db","key":"user"},"password":{"aws_secret_arn":"arn:aws:secretsmanager:us-east-1:123:secret:db","key":"pass"}}],"backend":{"host":"db.example.com","port":5432,"database":"mydb","pool_max_conns":100,"default_startup_parameters":{"application_name":"pglink"}},"track_extra_parameters":["TimeZone","client_encoding"]}`

	var s DatabaseConfig
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

func TestConfig_RoundTrip(t *testing.T) {
	// Note: databases is a map, and Database field has json:"-"
	// Database-from-map-key behavior is tested in TestParseConfig_MultipleDatabases
	input := `{"listen":[":5432",":6432"],"databases":{"mydb":{"users":[],"backend":{"host":"db.example.com","port":5432,"database":"mydb","pool_max_conns":100}}}}`

	var c Config
	if err := json.Unmarshal([]byte(input), &c); err != nil {
		t.Fatalf("Unmarshal failed: %v", err)
	}

	got, err := json.Marshal(c)
	if err != nil {
		t.Fatalf("Marshal failed: %v", err)
	}

	if string(got) != input {
		t.Errorf("round-trip mismatch:\n  input:  %s\n  output: %s", input, string(got))
	}
}

func TestBackendConfig_PoolConfig(t *testing.T) {
	tests := []struct {
		name   string
		config BackendConfig
	}{
		{
			name: "minimal",
			config: BackendConfig{
				Host:         "localhost",
				Database:     "postgres",
				PoolMaxConns: 10,
			},
		},
		{
			name: "with port",
			config: BackendConfig{
				Host:         "db.example.com",
				Port:         ptr[uint16](5432),
				Database:     "mydb",
				PoolMaxConns: 10,
			},
		},
		{
			name: "with pool settings",
			config: BackendConfig{
				Host:                "db.example.com",
				Port:                ptr[uint16](5432),
				Database:            "mydb",
				PoolMaxConns:        100,
				PoolMinIdleConns:        ptr[int32](10),
				PoolMaxConnLifetime: ptr("1h"),
				PoolMaxConnIdleTime: ptr("30m"),
			},
		},
		{
			name: "with ssl",
			config: BackendConfig{
				Host:         "db.example.com",
				Port:         ptr[uint16](5432),
				Database:     "mydb",
				PoolMaxConns: 10,
				SSLMode:      ptr("require"),
			},
		},
		{
			name: "with connect timeout",
			config: BackendConfig{
				Host:           "db.example.com",
				Database:       "mydb",
				PoolMaxConns:   10,
				ConnectTimeout: ptr("5"),
			},
		},
		{
			name: "with startup parameters",
			config: BackendConfig{
				Host:         "db.example.com",
				Database:     "mydb",
				PoolMaxConns: 10,
				DefaultStartupParameters: PgStartupParameters{
					keys:   []string{"application_name", "timezone"},
					values: map[string]string{"application_name": "pglink", "timezone": "UTC"},
				},
			},
		},
		{
			name: "full config",
			config: BackendConfig{
				Host:                      "db.example.com",
				Port:                      ptr[uint16](5432),
				Database:                  "production",
				ConnectTimeout:            ptr("10"),
				SSLMode:                   ptr("verify-full"),
				PoolMaxConns:              50,
				PoolMinIdleConns:              ptr[int32](5),
				PoolMaxConnLifetime:       ptr("1h"),
				PoolMaxConnLifetimeJitter: ptr("5m"),
				PoolMaxConnIdleTime:       ptr("10m"),
				PoolHealthCheckPeriod:     ptr("1m"),
				DefaultStartupParameters: PgStartupParameters{
					keys:   []string{"application_name"},
					values: map[string]string{"application_name": "pglink"},
				},
			},
		},
		{
			name: "host with spaces",
			config: BackendConfig{
				Host:         "my database server",
				Database:     "my database",
				PoolMaxConns: 10,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			connStr := tt.config.PoolConfigString()
			t.Logf("connection string: %s", connStr)

			cfg, err := tt.config.PoolConfig()
			if err != nil {
				t.Fatalf("PoolConfig failed: %v", err)
			}

			if cfg.ConnConfig.Host != tt.config.Host {
				t.Errorf("host mismatch: got %q, want %q", cfg.ConnConfig.Host, tt.config.Host)
			}

			if tt.config.Port != nil && cfg.ConnConfig.Port != *tt.config.Port {
				t.Errorf("port mismatch: got %d, want %d", cfg.ConnConfig.Port, *tt.config.Port)
			}

			if cfg.ConnConfig.Database != tt.config.Database {
				t.Errorf("database mismatch: got %q, want %q", cfg.ConnConfig.Database, tt.config.Database)
			}

			if cfg.MaxConns != tt.config.PoolMaxConns {
				t.Errorf("max_conns mismatch: got %d, want %d", cfg.MaxConns, tt.config.PoolMaxConns)
			}

			if tt.config.PoolMinIdleConns != nil && cfg.MinIdleConns != *tt.config.PoolMinIdleConns {
				t.Errorf("min_idle_conns mismatch: got %d, want %d", cfg.MinIdleConns, *tt.config.PoolMinIdleConns)
			}
		})
	}
}
