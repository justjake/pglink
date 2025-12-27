package e2e

import (
	"fmt"
	"os/exec"
	"path/filepath"
	"strings"

	"go.opentelemetry.io/otel/attribute"
)

// GitMetadata captures git state for a worktree/target.
// Used both at runner level and per-target level.
type GitMetadata struct {
	SHA          string `json:"sha"`           // Full commit SHA
	ShortSHA     string `json:"short_sha"`     // First 7 chars of SHA
	Branch       string `json:"branch"`        // Current branch name
	Dirty        bool   `json:"dirty"`         // True if uncommitted changes
	WorktreePath string `json:"worktree_path"` // Absolute path to worktree
}

// GetGitMetadata captures git state for a directory.
func GetGitMetadata(dir string) (*GitMetadata, error) {
	absDir, err := filepath.Abs(dir)
	if err != nil {
		return nil, fmt.Errorf("failed to get absolute path: %w", err)
	}

	meta := &GitMetadata{
		WorktreePath: absDir,
	}

	// Get full SHA
	sha, err := gitCommand(absDir, "rev-parse", "HEAD")
	if err != nil {
		return nil, fmt.Errorf("failed to get HEAD SHA: %w", err)
	}
	meta.SHA = sha
	if len(sha) >= 7 {
		meta.ShortSHA = sha[:7]
	} else {
		meta.ShortSHA = sha
	}

	// Get branch name
	branch, err := gitCommand(absDir, "rev-parse", "--abbrev-ref", "HEAD")
	if err != nil {
		// Not fatal - might be detached HEAD
		meta.Branch = "HEAD"
	} else {
		meta.Branch = branch
	}

	// Check if dirty (uncommitted changes)
	status, err := gitCommand(absDir, "status", "--porcelain")
	if err != nil {
		// Not fatal - assume clean
		meta.Dirty = false
	} else {
		meta.Dirty = len(strings.TrimSpace(status)) > 0
	}

	return meta, nil
}

// gitCommand runs a git command in the specified directory and returns trimmed output.
func gitCommand(dir string, args ...string) (string, error) {
	cmd := exec.Command("git", args...)
	cmd.Dir = dir
	output, err := cmd.Output()
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(string(output)), nil
}

// OTELAttributes returns git metadata as OTEL resource attributes.
func (g *GitMetadata) OTELAttributes() []attribute.KeyValue {
	attrs := []attribute.KeyValue{
		attribute.String("git.sha", g.SHA),
		attribute.String("git.short_sha", g.ShortSHA),
		attribute.String("git.branch", g.Branch),
		attribute.Bool("git.dirty", g.Dirty),
	}
	return attrs
}

// PrometheusLabels returns git metadata as Prometheus labels.
// Note: Prometheus labels should be short and stable.
func (g *GitMetadata) PrometheusLabels() map[string]string {
	return map[string]string{
		"git_sha":    g.ShortSHA,
		"git_branch": g.Branch,
	}
}

// AttrString returns git metadata formatted for CLI flag passing.
// Format: "git.sha=abc1234,git.branch=main,git.dirty=false"
func (g *GitMetadata) AttrString() string {
	return fmt.Sprintf("git.sha=%s,git.branch=%s,git.dirty=%t",
		g.ShortSHA, g.Branch, g.Dirty)
}

// PromLabelsString returns git metadata formatted for Prometheus CLI flag passing.
// Format: "git_sha=abc1234,git_branch=main"
func (g *GitMetadata) PromLabelsString() string {
	return fmt.Sprintf("git_sha=%s,git_branch=%s",
		g.ShortSHA, g.Branch)
}

// String returns a human-readable summary of the git metadata.
func (g *GitMetadata) String() string {
	dirty := ""
	if g.Dirty {
		dirty = " (dirty)"
	}
	return fmt.Sprintf("%s@%s%s", g.Branch, g.ShortSHA, dirty)
}

// GetGitDiff returns the output of git diff for a directory.
func GetGitDiff(dir string) (string, error) {
	return gitCommand(dir, "diff")
}

// GetGitStatus returns the output of git status for a directory.
func GetGitStatus(dir string) (string, error) {
	return gitCommand(dir, "status", "--short")
}
