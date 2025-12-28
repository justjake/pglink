package e2e

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"os/exec"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

// DockerComposeStartTimeout is how long to wait for docker compose services.
const DockerComposeStartTimeout = 2 * time.Minute

// BackendHealthCheckInterval is how often to check backend health.
const BackendHealthCheckInterval = 500 * time.Millisecond

// EnsureDockerCompose starts docker compose if not already running.
// Docker compose runs from the main repo directory so containers are shared
// across all worktrees.
// If observable is true, includes the observability profile.
func EnsureDockerCompose(ctx context.Context, mainRepoDir string, observable bool, logger *slog.Logger) error {
	// Check if containers are already running
	if IsDockerComposeRunning(ctx, mainRepoDir) {
		if logger != nil {
			logger.Info("docker compose already running")
		}
		return nil
	}

	if logger != nil {
		logger.Info("starting docker compose", "dir", mainRepoDir, "observable", observable)
	}

	// Build docker compose command
	args := []string{"compose", "up", "-d", "--wait"}
	if observable {
		args = []string{"compose", "--profile", "observability", "up", "-d", "--wait"}
	}

	cmd := exec.CommandContext(ctx, "docker", args...)
	cmd.Dir = mainRepoDir

	output, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("docker compose up failed: %w\nOutput: %s", err, string(output))
	}

	if logger != nil {
		logger.Info("docker compose started")
	}
	return nil
}

// IsDockerComposeRunning checks if all required containers are running.
func IsDockerComposeRunning(ctx context.Context, mainRepoDir string) bool {
	cmd := exec.CommandContext(ctx, "docker", "compose", "ps", "--format", "{{.State}}")
	cmd.Dir = mainRepoDir
	output, err := cmd.Output()
	if err != nil {
		return false
	}

	// Check that we have running containers
	lines := strings.Split(strings.TrimSpace(string(output)), "\n")
	runningCount := 0
	for _, line := range lines {
		if strings.Contains(line, "running") {
			runningCount++
		}
	}

	// We need all 3 containers (alpha, bravo, charlie) running
	return runningCount >= 3
}

// WaitForBackends waits for all backend databases to accept connections.
func WaitForBackends(ctx context.Context, logger *slog.Logger) error {
	ctx, cancel := context.WithTimeout(ctx, DockerComposeStartTimeout)
	defer cancel()

	var lastErr error
	for _, b := range PredefinedBackends {
		if err := WaitForBackend(ctx, b.Name, b.Port, logger); err != nil {
			lastErr = fmt.Errorf("backend %s: %w", b.Name, err)
		}
	}

	if lastErr != nil {
		return lastErr
	}

	if logger != nil {
		logger.Info("all backends healthy")
	}
	return nil
}

// WaitForBackend waits for a single backend to be ready.
func WaitForBackend(ctx context.Context, name string, port int, logger *slog.Logger) error {
	connStr := fmt.Sprintf("postgres://postgres:postgres@localhost:%d/postgres?sslmode=disable", port)

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		conn, err := pgx.Connect(ctx, connStr)
		if err == nil {
			_ = conn.Close(ctx)
			if logger != nil {
				logger.Info("backend ready", "name", name, "port", port)
			}
			return nil
		}

		if logger != nil {
			logger.Debug("waiting for backend", "name", name, "port", port, "error", err)
		}
		time.Sleep(BackendHealthCheckInterval)
	}
}

// FindFreePort finds an available TCP port.
func FindFreePort() (int, error) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return 0, err
	}
	port := listener.Addr().(*net.TCPAddr).Port
	if err := listener.Close(); err != nil {
		return 0, err
	}
	return port, nil
}

// WaitForTCPPort waits for a TCP port to accept connections.
func WaitForTCPPort(ctx context.Context, addr string, logger *slog.Logger) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		conn, err := net.DialTimeout("tcp", addr, time.Second)
		if err == nil {
			_ = conn.Close()
			if logger != nil {
				logger.Info("service ready", "addr", addr)
			}
			return nil
		}

		if logger != nil {
			logger.Debug("waiting for service", "addr", addr, "error", err)
		}
		time.Sleep(100 * time.Millisecond)
	}
}
