package config

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestParseConfig_Listen(t *testing.T) {
	tests := []struct {
		name     string
		json     string
		expected ListenAddr
	}{
		{
			name:     "port only",
			json:     `{"listen": "5432"}`,
			expected: ":5432",
		},
		{
			name:     "colon port",
			json:     `{"listen": ":5432"}`,
			expected: ":5432",
		},
		{
			name:     "host and port",
			json:     `{"listen": "127.0.0.1:5432"}`,
			expected: "127.0.0.1:5432",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg, err := ParseConfig(tt.json)
			if err != nil {
				t.Fatalf("ParseConfig failed: %v", err)
			}

			if cfg.Listen != tt.expected {
				t.Errorf("listen: expected %q, got %q", tt.expected, cfg.Listen)
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

func TestParseConfig_MultipleDatabases(t *testing.T) {
	jsonStr := `{
		"listen": ":5432",
		"databases": {
			"db1": {},
			"db2": {}
		}
	}`

	cfg, err := ParseConfig(jsonStr)
	if err != nil {
		t.Fatalf("ParseConfig failed: %v", err)
	}

	if len(cfg.Databases) != 2 {
		t.Fatalf("expected 2 databases, got %d", len(cfg.Databases))
	}

	if cfg.Databases["db1"] == nil {
		t.Error("expected database \"db1\" to exist")
	} else if cfg.Databases["db1"].Database != "db1" {
		t.Errorf("database db1: expected database \"db1\", got %q", cfg.Databases["db1"].Database)
	}

	if cfg.Databases["db2"] == nil {
		t.Error("expected database \"db2\" to exist")
	} else if cfg.Databases["db2"].Database != "db2" {
		t.Errorf("database db2: expected database \"db2\", got %q", cfg.Databases["db2"].Database)
	}
}

func TestReadConfigFile(t *testing.T) {
	tests := []struct {
		name    string
		file    string
		wantErr bool
	}{
		{
			name: "minimal config",
			file: "minimal.json",
		},
		{
			name: "full config",
			file: "full.json",
		},
		{
			name: "multiple databases",
			file: "multiple_databases.json",
		},
		{
			name: "env secrets",
			file: "env_secrets.json",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			path := filepath.Join("testdata", tt.file)
			cfg, err := ReadConfigFile(path)
			if (err != nil) != tt.wantErr {
				t.Fatalf("ReadConfigFile() error = %v, wantErr %v", err, tt.wantErr)
			}
			if err != nil {
				return
			}

			if cfg.Listen == "" {
				t.Error("expected a listen address")
			}
			if len(cfg.Databases) == 0 {
				t.Error("expected at least one database")
			}

			t.Logf("loaded config with listen=%s and %d databases",
				cfg.Listen, len(cfg.Databases))
		})
	}
}

func TestConfig_Validate(t *testing.T) {
	ctx := context.Background()

	// Create a SecretCache without AWS client (only works for insecure_value and env_var)
	secrets := NewSecretCache(nil)

	tests := []struct {
		name        string
		file        string
		setup       func()
		cleanup     func()
		wantErr     bool
		errContains string
	}{
		{
			name: "minimal config validates",
			file: "minimal.json",
		},
		{
			name: "full config validates",
			file: "full.json",
		},
		{
			name: "multiple databases validates",
			file: "multiple_databases.json",
		},
		{
			name: "env secrets with vars set",
			file: "env_secrets.json",
			setup: func() {
				os.Setenv("DB_USERNAME", "testuser")
				os.Setenv("DB_PASSWORD", "testpass")
			},
			cleanup: func() {
				os.Unsetenv("DB_USERNAME")
				os.Unsetenv("DB_PASSWORD")
			},
		},
		{
			name:        "env secrets without vars fails",
			file:        "env_secrets.json",
			wantErr:     true,
			errContains: "environment variable",
		},
		{
			name:        "invalid backend fails",
			file:        "invalid_backend.json",
			wantErr:     true,
			errContains: "connect_timeout",
		},
		{
			name:        "invalid secret ref fails",
			file:        "invalid_secret_ref.json",
			wantErr:     true,
			errContains: "must have one of",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.setup != nil {
				tt.setup()
			}
			if tt.cleanup != nil {
				defer tt.cleanup()
			}

			path := filepath.Join("testdata", tt.file)
			cfg, err := ReadConfigFile(path)
			if err != nil {
				t.Fatalf("ReadConfigFile() error = %v", err)
			}

			err = cfg.Validate(ctx, os.DirFS(cfg.Dir()), secrets, nil)
			if (err != nil) != tt.wantErr {
				t.Fatalf("Validate() error = %v, wantErr %v", err, tt.wantErr)
			}

			if tt.errContains != "" && err != nil {
				if !strings.Contains(err.Error(), tt.errContains) {
					t.Errorf("error %q does not contain %q", err.Error(), tt.errContains)
				}
			}

			if err != nil {
				t.Logf("validation error (expected): %v", err)
			}
		})
	}
}

func TestConfig_Validate_AccumulatesErrors(t *testing.T) {
	ctx := context.Background()
	secrets := NewSecretCache(nil)

	// Create a config with multiple errors
	cfg := &Config{
		Listen: ":5432",
		Databases: map[string]*DatabaseConfig{
			"db1": {
				Database: "db1",
				Users: []UserConfig{
					{
						Username: SecretRef{EnvVar: "MISSING_VAR_1"},
						Password: SecretRef{EnvVar: "MISSING_VAR_2"},
					},
				},
				Backend: BackendConfig{
					Host:           "localhost",
					Database:       "db1",
					ConnectTimeout: ptr("invalid"),
				},
			},
			"db2": {
				Database: "db2",
				Users: []UserConfig{
					{
						Username: SecretRef{EnvVar: "MISSING_VAR_3"},
						Password: SecretRef{InsecureValue: "ok"},
					},
				},
				Backend: BackendConfig{
					Host:     "localhost",
					Database: "db2",
				},
			},
		},
	}

	err := cfg.Validate(ctx, os.DirFS("."), secrets, nil)
	if err == nil {
		t.Fatal("expected validation errors")
	}

	errStr := err.Error()
	t.Logf("accumulated errors: %v", err)

	// Should have backend error for first server
	if !strings.Contains(errStr, "databases[db1]") || !strings.Contains(errStr, "backend") {
		t.Error("expected error for databases[db1] backend")
	}

	// Should have secret errors
	if !strings.Contains(errStr, "MISSING_VAR_1") {
		t.Error("expected error for MISSING_VAR_1")
	}
	if !strings.Contains(errStr, "MISSING_VAR_2") {
		t.Error("expected error for MISSING_VAR_2")
	}
	if !strings.Contains(errStr, "MISSING_VAR_3") {
		t.Error("expected error for MISSING_VAR_3")
	}
}

func TestDatabaseConfig_Validate(t *testing.T) {
	ctx := context.Background()
	secrets := NewSecretCache(nil)

	tests := []struct {
		name        string
		config      DatabaseConfig
		wantErr     bool
		errContains string
	}{
		{
			name: "valid config",
			config: DatabaseConfig{
				Database: "mydb",
				Users: []UserConfig{
					{Username: SecretRef{InsecureValue: "user1"}, Password: SecretRef{InsecureValue: "pass1"}},
					{Username: SecretRef{InsecureValue: "user2"}, Password: SecretRef{InsecureValue: "pass2"}},
				},
				Backend: BackendConfig{
					Host:             "localhost",
					Database:         "mydb",
					PoolMaxConns:     20,
					PoolMinIdleConns: ptr[int32](5),
				},
			},
			wantErr: false,
		},
		{
			name: "pool_max_conns required",
			config: DatabaseConfig{
				Database: "mydb",
				Users:    []UserConfig{},
				Backend: BackendConfig{
					Host:         "localhost",
					Database:     "mydb",
					PoolMaxConns: 0,
				},
			},
			wantErr:     true,
			errContains: "pool_max_conns is required",
		},
		{
			name: "pool_min_idle_conns * users exceeds max",
			config: DatabaseConfig{
				Database: "mydb",
				Users: []UserConfig{
					{Username: SecretRef{InsecureValue: "user1"}, Password: SecretRef{InsecureValue: "pass1"}},
					{Username: SecretRef{InsecureValue: "user2"}, Password: SecretRef{InsecureValue: "pass2"}},
					{Username: SecretRef{InsecureValue: "user3"}, Password: SecretRef{InsecureValue: "pass3"}},
				},
				Backend: BackendConfig{
					Host:             "localhost",
					Database:         "mydb",
					PoolMaxConns:     10,
					PoolMinIdleConns: ptr[int32](5), // 5 * 3 = 15 > 10
				},
			},
			wantErr:     true,
			errContains: "exceeds backend.pool_max_conns",
		},
		{
			name: "pool_min_idle_conns * users equals max is ok",
			config: DatabaseConfig{
				Database: "mydb",
				Users: []UserConfig{
					{Username: SecretRef{InsecureValue: "user1"}, Password: SecretRef{InsecureValue: "pass1"}},
					{Username: SecretRef{InsecureValue: "user2"}, Password: SecretRef{InsecureValue: "pass2"}},
				},
				Backend: BackendConfig{
					Host:             "localhost",
					Database:         "mydb",
					PoolMaxConns:     10,
					PoolMinIdleConns: ptr[int32](5), // 5 * 2 = 10 == 10
				},
			},
			wantErr: false,
		},
		{
			name: "duplicate usernames",
			config: DatabaseConfig{
				Database: "mydb",
				Users: []UserConfig{
					{Username: SecretRef{InsecureValue: "same_user"}, Password: SecretRef{InsecureValue: "pass1"}},
					{Username: SecretRef{InsecureValue: "different_user"}, Password: SecretRef{InsecureValue: "pass2"}},
					{Username: SecretRef{InsecureValue: "same_user"}, Password: SecretRef{InsecureValue: "pass3"}},
				},
				Backend: BackendConfig{
					Host:         "localhost",
					Database:     "mydb",
					PoolMaxConns: 10,
				},
			},
			wantErr:     true,
			errContains: "duplicate username",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.config.Validate(ctx, secrets)
			if (err != nil) != tt.wantErr {
				t.Fatalf("Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
			if tt.errContains != "" && err != nil {
				if !strings.Contains(err.Error(), tt.errContains) {
					t.Errorf("error %q does not contain %q", err.Error(), tt.errContains)
				}
			}
		})
	}
}

func TestConfig_PoolConfigFromFile(t *testing.T) {
	path := filepath.Join("testdata", "full.json")
	cfg, err := ReadConfigFile(path)
	if err != nil {
		t.Fatalf("ReadConfigFile() error = %v", err)
	}

	server := cfg.Databases["production"]
	if server == nil {
		t.Fatal("expected server \"production\" to exist")
	}

	backend := server.Backend
	poolCfg, err := backend.PoolConfig()
	if err != nil {
		t.Fatalf("PoolConfig() error = %v", err)
	}

	// Verify the pool config has expected values
	if poolCfg.ConnConfig.Host != "db.example.com" {
		t.Errorf("host = %q, want %q", poolCfg.ConnConfig.Host, "db.example.com")
	}
	if poolCfg.ConnConfig.Port != 5432 {
		t.Errorf("port = %d, want %d", poolCfg.ConnConfig.Port, 5432)
	}
	if poolCfg.ConnConfig.Database != "production" {
		t.Errorf("database = %q, want %q", poolCfg.ConnConfig.Database, "production")
	}
	if poolCfg.MaxConns != 100 {
		t.Errorf("max_conns = %d, want %d", poolCfg.MaxConns, 100)
	}
	if poolCfg.MinIdleConns != 10 {
		t.Errorf("min_idle_conns = %d, want %d", poolCfg.MinIdleConns, 10)
	}

	// Verify startup parameters were applied
	if poolCfg.ConnConfig.RuntimeParams["application_name"] != "pglink" {
		t.Errorf("application_name = %q, want %q",
			poolCfg.ConnConfig.RuntimeParams["application_name"], "pglink")
	}
	if poolCfg.ConnConfig.RuntimeParams["timezone"] != "UTC" {
		t.Errorf("timezone = %q, want %q",
			poolCfg.ConnConfig.RuntimeParams["timezone"], "UTC")
	}
}

func TestConfig_Validate_WithMockAWS(t *testing.T) {
	ctx := context.Background()

	// Create mock AWS client with test secrets
	mock := NewMockSecretsManagerClient()
	mock.Secrets["arn:aws:secretsmanager:us-east-1:123456789:secret:db-creds"] = `{"username":"prod_user","password":"prod_pass123"}`

	secrets := NewSecretCache(mock)

	path := filepath.Join("testdata", "aws_secrets.json")
	cfg, err := ReadConfigFile(path)
	if err != nil {
		t.Fatalf("ReadConfigFile() error = %v", err)
	}

	// Validation should succeed with mock
	err = cfg.Validate(ctx, os.DirFS(cfg.Dir()), secrets, nil)
	if err != nil {
		t.Fatalf("Validate() error = %v", err)
	}

	// Verify the mock was called
	if len(mock.Calls) != 1 {
		t.Errorf("expected 1 AWS call, got %d", len(mock.Calls))
	}

	// Verify we can retrieve the secrets
	server := cfg.Databases["production"]
	if server == nil {
		t.Fatal("expected server \"production\" to exist")
	}
	user := server.Users[0]
	username, err := secrets.Get(ctx, user.Username)
	if err != nil {
		t.Fatalf("Get username error = %v", err)
	}
	if username != "prod_user" {
		t.Errorf("username = %q, want %q", username, "prod_user")
	}

	password, err := secrets.Get(ctx, user.Password)
	if err != nil {
		t.Fatalf("Get password error = %v", err)
	}
	if password != "prod_pass123" {
		t.Errorf("password = %q, want %q", password, "prod_pass123")
	}
}

func TestConfig_Validate_AWSSecretMissing(t *testing.T) {
	ctx := context.Background()

	// Create mock AWS client WITHOUT the required secret
	mock := NewMockSecretsManagerClient()
	secrets := NewSecretCache(mock)

	path := filepath.Join("testdata", "aws_secrets.json")
	cfg, err := ReadConfigFile(path)
	if err != nil {
		t.Fatalf("ReadConfigFile() error = %v", err)
	}

	// Validation should fail
	err = cfg.Validate(ctx, os.DirFS(cfg.Dir()), secrets, nil)
	if err == nil {
		t.Fatal("expected validation error for missing AWS secret")
	}

	if !strings.Contains(err.Error(), "secret not found") {
		t.Errorf("error %q does not contain 'secret not found'", err.Error())
	}

	t.Logf("validation error (expected): %v", err)
}
