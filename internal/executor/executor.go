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
	"os"
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

// Executor runs jobs. It holds configuration only; V0 needed none, V8 adds the
// sandbox parent, and later versions can add more without changing Run's shape.
type Executor struct {
	// SandboxRoot is the parent directory for per-run sandboxes. The zero value
	// uses os.TempDir(): Executor{} stays a working Runner, so callers never
	// have to know the sandbox exists.
	SandboxRoot string
}

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

	/*
		Create the sandbox only after validation and the context check: a job
		that cannot run must leave no directory behind. MkdirTemp appends a
		random suffix to the pattern, so the job id never becomes part of a
		path — a hostile id cannot escape the root by choosing its own name.
	*/
	root := e.SandboxRoot
	if root == "" {
		// The zero value means the ordinary temp directory, NOT "no sandbox":
		// Executor{} must keep working without callers opting in.
		root = os.TempDir()
	}
	dir, err := os.MkdirTemp(root, "forge-job-")
	if err != nil {
		return Result{}, fmt.Errorf("job %q: sandbox: %w", j.ID, err)
	}
	/*
		defer runs after cmd.Run has returned, so no live child has files
		deleted from under it; it also covers every error return after this
		point, which is what makes cleanup total rather than success-only.
	*/
	defer os.RemoveAll(dir)

	var stdout, stderr bytes.Buffer

	// CommandContext ties the child's life to ctx: when ctx is done it kills
	// the process. That is the whole cancellation mechanism at this layer.
	cmd := exec.CommandContext(ctx, j.Command[0], j.Command[1:]...)
	// Dir is the child's working directory: the fresh sandbox, never the
	// parent process's cwd, so concurrent jobs cannot see each other's files.
	cmd.Dir = dir
	/*
		cmd.Env REPLACES inheritance: this is every variable the child gets, so
		forge's environment cannot leak into a job by omission. PATH is copied
		explicitly because exec.LookPath resolves the program through it and the
		child's own `sh -c` needs it to find commands.
	*/
	cmd.Env = []string{"PATH=" + os.Getenv("PATH")}
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	err = cmd.Run()
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
