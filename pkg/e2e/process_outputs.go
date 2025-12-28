package e2e

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

// OutputFile wraps a file with path information for later reference.
type OutputFile struct {
	*os.File
	AbsPath string // Full path: /path/to/output/pgbench.pglink.1.pgbench_x.out
	RelPath string // Relative to output dir: pgbench.pglink.1.pgbench_x.out
}

// ProcessOutputs holds stdout/stderr files for a process.
type ProcessOutputs struct {
	Stdout *OutputFile
	Stderr *OutputFile
}

// Close closes both output files.
func (p *ProcessOutputs) Close() error {
	var errs []error
	if p.Stdout != nil {
		if err := p.Stdout.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	if p.Stderr != nil {
		if err := p.Stderr.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// OpenProcessOutputs creates output files for a process.
//
// Naming convention:
//   - With cases: <processType>.<target>.<index>.<cases>.out
//   - Without cases: <processType>.<target>.<index>.out
//
// Parameters:
//   - processType: "pgbench", "go-test", "pglink", "pgbouncer"
//   - target: "direct", "pglink", "pgbouncer", etc.
//   - index: 1-based index for multiple invocations
//   - cases: comma-separated case names, or "" for daemon processes
//
// Examples:
//   - pgbench.pglink.1.pgbench_tpcb.out (with case)
//   - go-test.pglink.1.simple,connect.out (with cases)
//   - pglink.pglink.1.out (daemon, no cases)
func OpenProcessOutputs(outputDir, processType, target string, index int, cases string) (*ProcessOutputs, error) {
	if outputDir == "" {
		// No-op if no output dir - return empty struct with nil files
		return &ProcessOutputs{}, nil
	}

	var baseName string
	if cases != "" {
		baseName = fmt.Sprintf("%s.%s.%d.%s", processType, target, index, cases)
	} else {
		baseName = fmt.Sprintf("%s.%s.%d", processType, target, index)
	}

	stdoutRelPath := baseName + ".out"
	stderrRelPath := baseName + ".err"
	stdoutPath := filepath.Join(outputDir, stdoutRelPath)
	stderrPath := filepath.Join(outputDir, stderrRelPath)

	stdout, err := os.Create(stdoutPath)
	if err != nil {
		return nil, fmt.Errorf("failed to create stdout file %s: %w", stdoutPath, err)
	}

	stderr, err := os.Create(stderrPath)
	if err != nil {
		_ = stdout.Close()
		return nil, fmt.Errorf("failed to create stderr file %s: %w", stderrPath, err)
	}

	return &ProcessOutputs{
		Stdout: &OutputFile{File: stdout, AbsPath: stdoutPath, RelPath: stdoutRelPath},
		Stderr: &OutputFile{File: stderr, AbsPath: stderrPath, RelPath: stderrRelPath},
	}, nil
}
