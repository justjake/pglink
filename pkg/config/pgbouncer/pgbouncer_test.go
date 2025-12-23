package pgbouncer

import (
	"context"
	"strings"
	"testing"

	"github.com/justjake/pglink/pkg/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGenerateConfig(t *testing.T) {
	ctx := context.Background()

	// Create a mock secret cache that doesn't need AWS
	secrets := config.NewSecretCache(nil)

	// Create a test config
	port := uint16(15432)
	cfg := &config.Config{
		Listen: ":16432",
		Databases: map[string]*config.DatabaseConfig{
			"test_db": {
				Database: "test_db",
				Users: []config.UserConfig{
					{
						Username: config.SecretRef{InsecureValue: "testuser"},
						Password: config.SecretRef{InsecureValue: "testpass"},
					},
					{
						Username: config.SecretRef{InsecureValue: "admin"},
						Password: config.SecretRef{InsecureValue: "adminpass"},
					},
				},
				Backend: config.BackendConfig{
					Host:         "localhost",
					Port:         &port,
					Database:     "mydb",
					PoolMaxConns: 10,
				},
			},
		},
	}

	result, err := GenerateConfig(ctx, cfg, secrets, 16433)
	require.NoError(t, err)

	// Check INI content
	t.Logf("INI:\n%s", result.INI)
	assert.Contains(t, result.INI, "[databases]")
	// Database connection string should NOT include user credentials - pgbouncer passes through client creds
	assert.Contains(t, result.INI, "test_db = host=localhost port=15432 dbname=mydb")
	assert.Contains(t, result.INI, "[pgbouncer]")
	assert.Contains(t, result.INI, "listen_port = 16433")
	assert.Contains(t, result.INI, "pool_mode = transaction")
	assert.Contains(t, result.INI, "auth_type = plain")
	assert.Contains(t, result.INI, "default_pool_size = 10")

	// Check userlist content - plain text passwords for backend authentication
	t.Logf("Userlist:\n%s", result.Userlist)
	assert.Contains(t, result.Userlist, `"testuser" "testpass"`)
	assert.Contains(t, result.Userlist, `"admin" "adminpass"`)
}

func TestMd5Password(t *testing.T) {
	// Test known MD5 password hash
	// PostgreSQL format: md5 + md5(password + username)
	result := md5Password("testuser", "testpass")

	// Verify it starts with "md5"
	assert.True(t, strings.HasPrefix(result, "md5"))

	// Verify it's the right length (md5 + 32 hex chars)
	assert.Len(t, result, 35)
}

func TestGenerateConfig_MultipleDatabases(t *testing.T) {
	ctx := context.Background()
	secrets := config.NewSecretCache(nil)

	port1 := uint16(15432)
	port2 := uint16(15433)
	cfg := &config.Config{
		Listen: ":16432",
		Databases: map[string]*config.DatabaseConfig{
			"db1": {
				Database: "db1",
				Users: []config.UserConfig{
					{
						Username: config.SecretRef{InsecureValue: "user1"},
						Password: config.SecretRef{InsecureValue: "pass1"},
					},
				},
				Backend: config.BackendConfig{
					Host:         "host1",
					Port:         &port1,
					Database:     "backend1",
					PoolMaxConns: 5,
				},
			},
			"db2": {
				Database: "db2",
				Users: []config.UserConfig{
					{
						Username: config.SecretRef{InsecureValue: "user2"},
						Password: config.SecretRef{InsecureValue: "pass2"},
					},
				},
				Backend: config.BackendConfig{
					Host:         "host2",
					Port:         &port2,
					Database:     "backend2",
					PoolMaxConns: 15,
				},
			},
		},
	}

	result, err := GenerateConfig(ctx, cfg, secrets, 16433)
	require.NoError(t, err)

	// Both databases should be in config (without user credentials - pgbouncer passes through client creds)
	assert.Contains(t, result.INI, "db1 = host=host1 port=15432 dbname=backend1")
	assert.Contains(t, result.INI, "db2 = host=host2 port=15433 dbname=backend2")

	// Pool size should be max of all databases
	assert.Contains(t, result.INI, "default_pool_size = 15")

	// Both users should be in userlist with plain text passwords
	assert.Contains(t, result.Userlist, `"user1" "pass1"`)
	assert.Contains(t, result.Userlist, `"user2" "pass2"`)
}

func TestGenerateConfig_DeduplicatesUsers(t *testing.T) {
	ctx := context.Background()
	secrets := config.NewSecretCache(nil)

	port := uint16(5432)
	cfg := &config.Config{
		Listen: ":16432",
		Databases: map[string]*config.DatabaseConfig{
			"db1": {
				Database: "db1",
				Users: []config.UserConfig{
					{
						Username: config.SecretRef{InsecureValue: "shared_user"},
						Password: config.SecretRef{InsecureValue: "pass1"},
					},
				},
				Backend: config.BackendConfig{
					Host:         "localhost",
					Port:         &port,
					Database:     "backend1",
					PoolMaxConns: 10,
				},
			},
			"db2": {
				Database: "db2",
				Users: []config.UserConfig{
					{
						// Same user in different database
						Username: config.SecretRef{InsecureValue: "shared_user"},
						Password: config.SecretRef{InsecureValue: "pass1"},
					},
				},
				Backend: config.BackendConfig{
					Host:         "localhost",
					Port:         &port,
					Database:     "backend2",
					PoolMaxConns: 10,
				},
			},
		},
	}

	result, err := GenerateConfig(ctx, cfg, secrets, 16433)
	require.NoError(t, err)

	// User should only appear once in userlist
	count := strings.Count(result.Userlist, `"shared_user"`)
	assert.Equal(t, 1, count, "user should only appear once in userlist")
}
