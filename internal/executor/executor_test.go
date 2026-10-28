package executor_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/Dushmilan/Forge/internal/executor"
	"github.com/Dushmilan/Forge/internal/job"
)

// TestExecutorRunCapturesStdout pins the happy path: a real command runs to
// completion and its standard output is returned on the Result value.
//
// The test calls a REAL process ("echo") rather than a mock. Real behavior is
// the thing we care about here, and a mock would only prove the mock works.
func TestExecutorRunCapturesStdout(t *testing.T) {
	e := executor.Executor{}

	res, err := e.Run(context.Background(), job.Job{ID: "j1", Command: []string{"echo", "hello"}})
	if err != nil {
		t.Fatalf("Run() error = %v, want nil", err)
	}

	// echo appends a newline; trim it so the assertion is about content, not
	// line-ending trivia.
	if got := strings.TrimSpace(res.Stdout); got != "hello" {
		t.Errorf("Stdout = %q, want %q", got, "hello")
	}
	if res.ExitCode != 0 {
		t.Errorf("ExitCode = %d, want 0", res.ExitCode)
	}
}

// TestExecutorRunMissingProgramIsNotAnExitError locks in the distinction
// between a command that STARTED and failed (an *exec.ExitError) and one that
// never started because the binary is missing (an *exec.Error wrapping
// exec.ErrNotFound). The design already yields this; the test keeps a future
// refactor from collapsing the two kinds into one.
func TestExecutorRunMissingProgramIsNotAnExitError(t *testing.T) {
	e := executor.Executor{}

	_, err := e.Run(context.Background(), job.Job{ID: "j3", Command: []string{"forge-no-such-binary-xyz"}})
	if err == nil {
		t.Fatal("Run() error = nil, want an error")
	}

	var exitErr *exec.ExitError
	if errors.As(err, &exitErr) {
		t.Errorf("Run() error = %v, want a start failure, not *exec.ExitError", err)
	}
	if !errors.Is(err, exec.ErrNotFound) {
		t.Errorf("Run() error = %v, want it to wrap exec.ErrNotFound", err)
	}
}

// TestExecutorImplementsRunner verifies the seam. Runner is the small interface
// callers program against; Executor satisfies it implicitly, with no
// "implements" declaration, so it can be swapped for a fake in tests or the
// HTTP layer at V1.
func TestExecutorImplementsRunner(t *testing.T) {
	var r executor.Runner = executor.Executor{}

	res, err := r.Run(context.Background(), job.Job{ID: "j-iface", Command: []string{"echo", "via-interface"}})
	if err != nil {
		t.Fatalf("Run() error = %v, want nil", err)
	}
	if got := strings.TrimSpace(res.Stdout); got != "via-interface" {
		t.Errorf("Stdout = %q, want %q", got, "via-interface")
	}
}

// TestExecutorRunRejectsInvalidJob pins the guard: Run must validate before it
// indexes into Command. An empty Job would otherwise panic with an
// index-out-of-range, and a job runner must never take down its host that way.
func TestExecutorRunRejectsInvalidJob(t *testing.T) {
	e := executor.Executor{}

	_, err := e.Run(context.Background(), job.Job{}) // no ID, no command
	if err == nil {
		t.Fatal("Run() error = nil, want an error")
	}
	if !errors.Is(err, job.ErrInvalidJob) {
		t.Errorf("Run() error = %v, want it to wrap job.ErrInvalidJob", err)
	}
}

// TestExecutorRunNonZeroExitIsAnError pins the failure path. "false" is a real
// program that exits with status 1.
//
// The point of the test is the error TYPE: a command that ran and failed is an
// *exec.ExitError, which carries the exit status. errors.As unwraps the error
// chain to find that concrete type — the counterpart to errors.Is for sentinels.
func TestExecutorRunNonZeroExitIsAnError(t *testing.T) {
	e := executor.Executor{}

	res, err := e.Run(context.Background(), job.Job{ID: "j2", Command: []string{"false"}})
	if err == nil {
		t.Fatal("Run() error = nil, want an error for a non-zero exit")
	}

	var exitErr *exec.ExitError
	if !errors.As(err, &exitErr) {
		t.Fatalf("Run() error = %T (%v), want it to wrap *exec.ExitError", err, err)
	}
	if res.ExitCode != 1 {
		t.Errorf("ExitCode = %d, want 1", res.ExitCode)
	}
}

/*
TestExecutorRunCancelledBeforeStart proves a dead context is checked before
the process is created: a cancelled job must never touch the OS.
*/
func TestExecutorRunCancelledBeforeStart(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	res, err := (executor.Executor{}).Run(ctx, job.Job{ID: "j-cancel", Command: []string{"echo", "hi"}})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Run() error = %v, want wrapped context.Canceled", err)
	}
	if res.Stdout != "" {
		t.Errorf("Stdout = %q, want empty — the process must not start", res.Stdout)
	}
}

/*
TestExecutorRunKillsRunningProcess pins the headline: exec.CommandContext
kills the child when ctx ends. The marker file proves the process actually
started before we cancel, so the test cannot pass by refusing to launch it.
`exec sleep 10` replaces the shell, so no grandchild survives the kill.
*/
func TestExecutorRunKillsRunningProcess(t *testing.T) {
	marker := filepath.Join(t.TempDir(), "started")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan error, 1)
	go func() {
		_, err := (executor.Executor{}).Run(ctx, job.Job{
			ID:      "j-kill",
			Command: []string{"sh", "-c", fmt.Sprintf("touch %s; exec sleep 10", strconv.Quote(marker))},
		})
		done <- err
	}()

	deadline := time.Now().Add(5 * time.Second)
	for {
		if _, err := os.Stat(marker); err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("command never started; cannot test the kill path")
		}
		time.Sleep(time.Millisecond) // poll interval only; the deadline is the guarantee
	}

	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Run() error = %v, want wrapped context.Canceled", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after cancel — the process was not killed")
	}
}

/*
TestExecutorRunDeadlineExceeded pins the timeout flavour of the same path:
WithTimeout's cause is DeadlineExceeded, and callers must be able to tell it
apart from an explicit cancel with errors.Is.
*/
func TestExecutorRunDeadlineExceeded(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	_, err := (executor.Executor{}).Run(ctx, job.Job{ID: "j-timeout", Command: []string{"sleep", "10"}})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Run() error = %v, want wrapped context.DeadlineExceeded", err)
	}
}
