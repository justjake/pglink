// Package pgbouncer generates PgBouncer configuration files from pglink config.
// This is useful for benchmarking pglink against PgBouncer.
package pgbouncer

import (
	"context"
	"crypto/md5"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/justjake/pglink/pkg/config"
)

// Config holds the generated PgBouncer configuration.
type Config struct {
	// INI is the main pgbouncer.ini config content.
	INI string

	// Userlist is the userlist.txt content with user credentials.
	Userlist string

	// ListenPort is the port PgBouncer will listen on.
	ListenPort int
}

// GenerateConfig creates a PgBouncer configuration equivalent to the given pglink config.
// The secrets parameter is used to resolve usernames and passwords.
func GenerateConfig(ctx context.Context, cfg *config.Config, secrets *config.SecretCache, listenPort int) (*Config, error) {
	var databases strings.Builder
	var users strings.Builder
	seenUsers := make(map[string]bool)

	// Build [databases] section
	for dbName, dbCfg := range cfg.Databases {
		// Build connection string for this database
		// PgBouncer will pass through client credentials to the backend
		connStr := buildConnString(&dbCfg.Backend)
		databases.WriteString(fmt.Sprintf("%s = %s\n", dbName, connStr))

		// Collect users for userlist.txt
		for _, userCfg := range dbCfg.Users {
			username, err := secrets.Get(ctx, userCfg.Username)
			if err != nil {
				return nil, fmt.Errorf("database %s: failed to get username: %w", dbName, err)
			}
			password, err := secrets.Get(ctx, userCfg.Password)
			if err != nil {
				return nil, fmt.Errorf("database %s: failed to get password for user %s: %w", dbName, username, err)
			}

			// Only add each user once to userlist
			if !seenUsers[username] {
				seenUsers[username] = true
				// Store plain text password - pgbouncer needs this to authenticate to backend
				users.WriteString(fmt.Sprintf("%q %q\n", username, password))
			}
		}
	}

	// Build the INI file
	ini := buildINI(cfg, listenPort, databases.String())

	return &Config{
		INI:        ini,
		Userlist:   users.String(),
		ListenPort: listenPort,
	}, nil
}

// WriteToDir writes the PgBouncer configuration files to the specified directory.
// Creates pgbouncer.ini and userlist.txt files.
func (c *Config) WriteToDir(dir string) error {
	// Create directory if it doesn't exist
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("failed to create directory: %w", err)
	}

	// Write pgbouncer.ini
	iniPath := filepath.Join(dir, "pgbouncer.ini")
	if err := os.WriteFile(iniPath, []byte(c.INI), 0644); err != nil {
		return fmt.Errorf("failed to write pgbouncer.ini: %w", err)
	}

	// Write userlist.txt
	userlistPath := filepath.Join(dir, "userlist.txt")
	if err := os.WriteFile(userlistPath, []byte(c.Userlist), 0600); err != nil {
		return fmt.Errorf("failed to write userlist.txt: %w", err)
	}

	return nil
}

// buildConnString creates a PgBouncer-compatible connection string from a BackendConfig.
// PgBouncer will pass through client credentials to the backend.
func buildConnString(backend *config.BackendConfig) string {
	var parts []string

	// Host
	parts = append(parts, fmt.Sprintf("host=%s", backend.Host))

	// Port (default 5432)
	port := 5432
	if backend.Port != nil {
		port = int(*backend.Port)
	}
	parts = append(parts, fmt.Sprintf("port=%d", port))

	// Database
	parts = append(parts, fmt.Sprintf("dbname=%s", backend.Database))

	return strings.Join(parts, " ")
}

// buildINI creates the pgbouncer.ini content.
func buildINI(cfg *config.Config, listenPort int, databases string) string {
	var b strings.Builder

	// [databases] section
	b.WriteString("[databases]\n")
	b.WriteString(databases)
	b.WriteString("\n")

	// [pgbouncer] section
	b.WriteString("[pgbouncer]\n")

	// Listen settings - parse from pglink config
	listenAddr := "127.0.0.1"
	if cfg.Listen != "" {
		addr := string(cfg.Listen)
		if host, _, err := splitHostPort(addr); err == nil && host != "" {
			listenAddr = host
		}
	}
	b.WriteString(fmt.Sprintf("listen_addr = %s\n", listenAddr))
	b.WriteString(fmt.Sprintf("listen_port = %d\n", listenPort))

	// Authentication - use plain text to allow password passthrough to backend
	b.WriteString("auth_type = plain\n")
	b.WriteString("auth_file = userlist.txt\n")

	// Pool mode - transaction pooling like pglink
	b.WriteString("pool_mode = transaction\n")

	// Connection limits - find max from all databases
	maxPoolConns := int32(0)
	for _, dbCfg := range cfg.Databases {
		if dbCfg.Backend.PoolMaxConns > maxPoolConns {
			maxPoolConns = dbCfg.Backend.PoolMaxConns
		}
	}
	if maxPoolConns == 0 {
		maxPoolConns = 20 // default
	}

	// PgBouncer settings
	b.WriteString(fmt.Sprintf("default_pool_size = %d\n", maxPoolConns))
	b.WriteString(fmt.Sprintf("max_client_conn = %d\n", cfg.GetMaxClientConnections()))

	// Disable server-side prepared statements for transaction pooling compatibility
	b.WriteString("max_prepared_statements = 0\n")

	// Logging
	b.WriteString("log_connections = 0\n")
	b.WriteString("log_disconnections = 0\n")
	b.WriteString("log_pooler_errors = 1\n")

	// Admin access (disabled for benchmarks)
	b.WriteString("admin_users = \n")
	b.WriteString("stats_users = \n")

	// Unix socket disabled
	b.WriteString("unix_socket_dir = \n")

	return b.String()
}

// md5Password creates an MD5 password hash in PostgreSQL format.
// Format: "md5" + md5(password + username)
func md5Password(username, password string) string {
	h := md5.New()
	io.WriteString(h, password)
	io.WriteString(h, username)
	return fmt.Sprintf("md5%x", h.Sum(nil))
}

// splitHostPort splits an address into host and port, handling edge cases.
func splitHostPort(addr string) (string, string, error) {
	// Handle addresses like ":5432" (empty host)
	if strings.HasPrefix(addr, ":") {
		return "", addr[1:], nil
	}

	// Find the last colon (to handle IPv6)
	lastColon := strings.LastIndex(addr, ":")
	if lastColon == -1 {
		// No port specified
		return addr, "", nil
	}

	return addr[:lastColon], addr[lastColon+1:], nil
}
