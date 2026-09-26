// Package executor runs a job.Job to completion on this machine.
//
// Version 0 is deliberately synchronous: one job, start to finish, no
// goroutines. Concurrency (V2/V3), cancellation (V4) and sandboxing (V8) layer
// on top of this type later.
package executor

import (
	"bytes"
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
type Runner interface {
	Run(job.Job) (Result, error)
}

// Compile-time proof Executor satisfies Runner.
var _ Runner = Executor{}

// Run executes j and returns its Result.
//
// The command is run directly, not through a shell, so j.Command is the program
// followed by its arguments. Stdout and stderr are captured into separate
// buffers because a job runner needs to tell the two streams apart.
func (e Executor) Run(j job.Job) (Result, error) {
	// Validate first: it turns a malformed job into an ordinary error value
	// instead of an index-out-of-range panic a few lines below.
	if err := j.Validate(); err != nil {
		return Result{}, err
	}

	var stdout, stderr bytes.Buffer

	cmd := exec.Command(j.Command[0], j.Command[1:]...)
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
		return res, fmt.Errorf("job %q: %w", j.ID, err)
	}

	return res, nil
}
