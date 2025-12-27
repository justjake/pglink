// Package e2e provides shared utilities for end-to-end testing and benchmarking.
package e2e

import (
	"fmt"
	"hash/fnv"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
)

// IsWorktree returns true if the given path is a git worktree (not the main repo).
// In a worktree, .git is a file containing "gitdir: ..." rather than a directory.
func IsWorktree(path string) bool {
	gitPath := filepath.Join(path, ".git")
	info, err := os.Stat(gitPath)
	if err != nil {
		return false
	}
	// Main repo: .git is a directory
	// Worktree: .git is a file containing "gitdir: ..."
	return !info.IsDir()
}

// MainWorktreePath returns the path to the main repository from any worktree.
// If the given path is already the main repo, it returns the path unchanged.
func MainWorktreePath(fromPath string) (string, error) {
	absPath, err := filepath.Abs(fromPath)
	if err != nil {
		return "", fmt.Errorf("failed to get absolute path: %w", err)
	}

	if !IsWorktree(absPath) {
		return absPath, nil
	}

	// Read .git file to find main repo
	gitFilePath := filepath.Join(absPath, ".git")
	data, err := os.ReadFile(gitFilePath)
	if err != nil {
		return "", fmt.Errorf("failed to read .git file: %w", err)
	}

	// Parse "gitdir: /path/to/main/.git/worktrees/name"
	content := strings.TrimSpace(string(data))
	if !strings.HasPrefix(content, "gitdir: ") {
		return "", fmt.Errorf("unexpected .git file format: %s", content)
	}

	gitDir := strings.TrimPrefix(content, "gitdir: ")
	// gitDir is like /path/to/main/.git/worktrees/worktree-name
	// We need to extract /path/to/main

	// Find ".git/worktrees" in the path and extract the main repo path
	parts := strings.Split(gitDir, string(filepath.Separator))
	for i, part := range parts {
		if part == ".git" && i+1 < len(parts) && parts[i+1] == "worktrees" {
			// Main repo path is everything before .git
			mainPath := strings.Join(parts[:i], string(filepath.Separator))
			if !filepath.IsAbs(mainPath) {
				mainPath = string(filepath.Separator) + mainPath
			}
			return mainPath, nil
		}
	}

	return "", fmt.Errorf("could not determine main worktree path from gitdir: %s", gitDir)
}

// WorktreePortOffset returns a consistent port offset (0-99) based on the worktree path.
// This allows multiple worktrees to run benchmarks/tests simultaneously without port conflicts.
// For the main repo, this returns 0.
func WorktreePortOffset(worktreePath string) int {
	absPath, err := filepath.Abs(worktreePath)
	if err != nil {
		return 0
	}

	if !IsWorktree(absPath) {
		return 0 // Main repo uses base ports
	}

	// Hash the worktree path for consistent port offset
	h := fnv.New32a()
	h.Write([]byte(absPath))
	// Use modulo 100 to get offset 0-99
	// Start from 1 to ensure worktrees don't conflict with main repo (offset 0)
	return 1 + int(h.Sum32()%99)
}

// ApplyPortOffset applies the worktree port offset to a base port.
func ApplyPortOffset(basePort, offset int) int {
	return basePort + offset
}

// WorktreeName returns the name of the worktree from its path.
// For the main repo, returns "main".
func WorktreeName(worktreePath string) string {
	absPath, err := filepath.Abs(worktreePath)
	if err != nil {
		return "unknown"
	}

	if !IsWorktree(absPath) {
		return "main"
	}

	// Extract worktree name from path (last component)
	return filepath.Base(absPath)
}

// BuildWorktreeBinary builds the pglink binary from a worktree source,
// outputting to the runner's worktree at out/worktrees/<label>/pglink.
// Returns the path to the built binary.
func BuildWorktreeBinary(sourceWorktree, runnerWorktree, label string) (string, error) {
	// Ensure paths are absolute
	sourceAbs, err := filepath.Abs(sourceWorktree)
	if err != nil {
		return "", fmt.Errorf("failed to get source path: %w", err)
	}
	runnerAbs, err := filepath.Abs(runnerWorktree)
	if err != nil {
		return "", fmt.Errorf("failed to get runner path: %w", err)
	}

	// Output path in runner's worktree
	outputDir := filepath.Join(runnerAbs, "out", "worktrees", label)
	outputPath := filepath.Join(outputDir, "pglink")

	// Create output directory
	if err := os.MkdirAll(outputDir, 0755); err != nil {
		return "", fmt.Errorf("failed to create output directory: %w", err)
	}

	// Build from the source worktree
	cmd := exec.Command("go", "build", "-o", outputPath, "./cmd/pglink")
	cmd.Dir = sourceAbs
	cmd.Env = append(os.Environ(),
		"GOOS="+runtime.GOOS,
		"GOARCH="+runtime.GOARCH,
	)

	output, err := cmd.CombinedOutput()
	if err != nil {
		return "", fmt.Errorf("build failed: %w\nOutput: %s", err, string(output))
	}

	return outputPath, nil
}

// CurrentWorktreePath returns the path to the current working directory's worktree root.
// It walks up the directory tree looking for .git.
func CurrentWorktreePath() (string, error) {
	cwd, err := os.Getwd()
	if err != nil {
		return "", err
	}

	// Walk up looking for .git
	dir := cwd
	for {
		gitPath := filepath.Join(dir, ".git")
		if _, err := os.Stat(gitPath); err == nil {
			return dir, nil
		}

		parent := filepath.Dir(dir)
		if parent == dir {
			return "", fmt.Errorf("not in a git repository")
		}
		dir = parent
	}
}
