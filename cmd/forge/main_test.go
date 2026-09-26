package main

import (
	"bytes"
	"strings"
	"testing"
)

// TestRunWithNoArgsPrintsUsage pins the CLI contract for a missing command:
// a usage line on stderr and the conventional exit code 2 (the shell's "misuse"
// code). Without an explicit branch this would fall through to validation and
// report exit 1, which connotes a runtime failure rather than bad input.
func TestRunWithNoArgsPrintsUsage(t *testing.T) {
	var stdout, stderr bytes.Buffer

	code := run(nil, &stdout, &stderr)

	if code != 2 {
		t.Errorf("run() = %d, want 2", code)
	}
	if stderr.Len() == 0 {
		t.Error("stderr is empty, want a usage message")
	}
	if stdout.Len() != 0 {
		t.Errorf("stdout = %q, want empty", stdout.String())
	}
}

// TestRunPropagatesCommandExitCode pins that the CLI reports the command's own
// exit code, not a generic 1. Tools compose by exit code, so "sh -c 'exit 3'"
// must yield 3.
func TestRunPropagatesCommandExitCode(t *testing.T) {
	var stdout, stderr bytes.Buffer

	code := run([]string{"sh", "-c", "exit 3"}, &stdout, &stderr)

	if code != 3 {
		t.Errorf("run() = %d, want 3", code)
	}
	if stderr.Len() == 0 {
		t.Error("stderr is empty, want failure detail")
	}
}

// TestRunExecutesCommand pins the CLI happy path. run() returns an exit code
// rather than calling os.Exit directly, which is what makes the command line
// testable: the test supplies a bytes.Buffer where main supplies os.Stdout.
func TestRunExecutesCommand(t *testing.T) {
	var stdout, stderr bytes.Buffer

	code := run([]string{"echo", "hello"}, &stdout, &stderr)

	if code != 0 {
		t.Fatalf("run() = %d, want 0 (stderr: %s)", code, stderr.String())
	}
	if got := strings.TrimSpace(stdout.String()); got != "hello" {
		t.Errorf("stdout = %q, want %q", got, "hello")
	}
}
