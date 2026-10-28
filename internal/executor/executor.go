// Package executor runs a job.Job to completion on this machine.
//
// Version 0 is deliberately synchronous: one job, start to finish, no
// goroutines. Concurrency (V2/V3), cancellation (V4) and sandboxing (V8) layer
// on top of this type later.
package executor

import (
	"bytes"
	"context"
	"fmt"
	"os/exec"

	"github.com/Dushmilan/Forge/internal/job"
)

// Result is what happened when a Job ran. It is a plain value: callers can
// copy it, log it, or serialize it without touching the process that produced it.
type Result struct {
	ExitCode int
	Stdout   string
	Stderr   string
}

// Executor runs jobs. It holds no state in V0; the struct exists so later
// versions can give it configuration (timeouts, worker counts, sandboxes)
// without every caller having to change the call shape.
type Executor struct{}

// Runner is the seam callers depend on. Go's guidance is "accept interfaces,
// return concrete types": keep one small method here, leave Executor a concrete
// struct, and anything with a matching Run can be substituted — a fake in tests,
// the HTTP layer at V1.
//
// The context is the first parameter because every Run is cancellable: it
// carries the job's deadline and the DELETE signal into os/exec.
type Runner interface {
	Run(ctx context.Context, j job.Job) (Result, error)
}

// Compile-time proof Executor satisfies Runner.
var _ Runner = Executor{}

// Run executes j and returns its Result.
//
// The command is run directly, not through a shell, so j.Command is the program
// followed by its arguments. Stdout and stderr are captured into separate
// buffers because a job runner needs to tell the two streams apart.
func (e Executor) Run(ctx context.Context, j job.Job) (Result, error) {
	// Validate first: it turns a malformed job into an ordinary error value
	// instead of an index-out-of-range panic a few lines below.
	if err := j.Validate(); err != nil {
		return Result{}, err
	}
	/*
		Check the context BEFORE starting anything. exec.CommandContext would
		refuse too, but returning ctx.Err() ourselves makes the cause visible
		without a process launch — and a cancelled job must not touch the OS.
	*/
	if err := ctx.Err(); err != nil {
		return Result{}, fmt.Errorf("job %q: %w", j.ID, err)
	}

	var stdout, stderr bytes.Buffer

	// CommandContext ties the child's life to ctx: when ctx is done it kills
	// the process. That is the whole cancellation mechanism at this layer.
	cmd := exec.CommandContext(ctx, j.Command[0], j.Command[1:]...)
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	err := cmd.Run()
	res := Result{Stdout: stdout.String(), Stderr: stderr.String()}

	// ProcessState is non-nil once the process actually started, even if it
	// exited non-zero. It stays nil when the process never started (e.g. the
	// binary is missing), which is what distinguishes the two failure kinds.
	if cmd.ProcessState != nil {
		res.ExitCode = cmd.ProcessState.ExitCode()
	}

	// Wrap so the returned error carries the job id while still preserving the
	// original *exec.ExitError inside the chain for errors.As to find.
	if err != nil {
		/*
			A killed process reports "signal: killed", which says nothing about
			WHY. Prefer ctx.Err() so the error chain carries Canceled or
			DeadlineExceeded and callers can branch with errors.Is.
		*/
		if cerr := ctx.Err(); cerr != nil {
			err = cerr
		}
		return res, fmt.Errorf("job %q: %w", j.ID, err)
	}

	return res, nil
}
