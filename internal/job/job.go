// Package job holds Forge's core domain type: a Job and its lifecycle status.
// It knows nothing about execution, storage, or transport — those arrive in
// later versions and depend on this package, never the other way around.
package job

import (
	"errors"
	"fmt"
)

// ErrInvalidJob is the sentinel (a package-level error value) returned, wrapped,
// whenever a Job fails validation. Sentinels let callers ask "was this an
// invalid job?" with errors.Is, instead of matching on error text.
var ErrInvalidJob = errors.New("invalid job")

// Status is a named type over int describing where a Job is in its lifecycle.
//
// Making it a named type (rather than a bare int) means the compiler refuses
// to let a Status be confused with any other integer — a cheap form of safety.
type Status int

// iota numbers these constants 0, 1, 2, 3. StatusPending is deliberately the
// zero value, so a freshly declared Job is already "pending" with no extra work.
const (
	StatusPending Status = iota // starts at 0
	StatusRunning
	StatusSucceeded
	StatusFailed
)

// String makes Status satisfy fmt.Stringer. Go interfaces are satisfied
// implicitly: writing this method is the ONLY thing needed for fmt to print
// "running" instead of "1" — there is no "implements" keyword to add.
func (s Status) String() string {
	switch s {
	case StatusPending:
		return "pending"
	case StatusRunning:
		return "running"
	case StatusSucceeded:
		return "succeeded"
	case StatusFailed:
		return "failed"
	default:
		// int(s) formats the underlying number. Using %v here instead would
		// re-enter String() through fmt's Stringer path and recurse forever.
		return fmt.Sprintf("Status(%d)", int(s))
	}
}

// Compile-time proof that Status implements fmt.Stringer. If the method set
// ever drifts, this line stops the build — no runtime check required.
var _ fmt.Stringer = Status(0)

// Job is a unit of work: what to run, plus the state it is in.
//
// Command is a []string (program followed by its arguments), NOT a shell
// string. exec does not invoke a shell, so "echo hi" would be looked up as one
// program literally named "echo hi" and fail. The slice form removes all shell
// quoting and injection ambiguity.
type Job struct {
	ID      string
	Command []string
}

// Validate reports whether the Job has enough to be run. It returns a nil error
// on success; on failure it returns an error that WRAPS ErrInvalidJob. Wrapping
// with %w keeps the sentinel recognisable to errors.Is while adding the
// human-readable "which rule failed" detail on top.
func (j Job) Validate() error {
	if j.ID == "" {
		return fmt.Errorf("job: missing id: %w", ErrInvalidJob)
	}
	if len(j.Command) == 0 {
		return fmt.Errorf("job %q: missing command: %w", j.ID, ErrInvalidJob)
	}
	if j.Command[0] == "" {
		return fmt.Errorf("job %q: empty program name: %w", j.ID, ErrInvalidJob)
	}
	return nil
}
