// Package job holds Forge's core domain type: a Job and its lifecycle status.
// It knows nothing about execution, storage, or transport — those arrive in
// later versions and depend on this package, never the other way around.
package job

import (
	"encoding/json"
	"errors"
	"fmt"
)

// ErrInvalidJob is the sentinel (a package-level error value) returned, wrapped,
// whenever a Job fails validation. Sentinels let callers ask "was this an
// invalid job?" with errors.Is, instead of matching on error text.
var ErrInvalidJob = errors.New("invalid job")

/*
ErrInvalidTransition is returned, wrapped, when a caller tries to move a
Status somewhere the lifecycle does not allow. Like ErrInvalidJob it is a
sentinel so errors.Is can answer "was this a state-machine violation?"
without matching text.
*/
var ErrInvalidTransition = errors.New("job: invalid status transition")

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
	StatusCancelled // appended: existing iota values must never shift
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
	case StatusCancelled:
		return "cancelled"
	default:
		// int(s) formats the underlying number. Using %v here instead would
		// re-enter String() through fmt's Stringer path and recurse forever.
		return fmt.Sprintf("Status(%d)", int(s))
	}
}

// MarshalJSON renders a Status as its String() name ("succeeded", not 2).
// This is json.Marshaler: encoding/json checks for this method before the
// default int encoding, so the wire format changes without touching any
// caller. Files and API responses stay readable years later.
func (s Status) MarshalJSON() ([]byte, error) {
	return json.Marshal(s.String())
}

// UnmarshalJSON decodes a Status from its string name. The pointer receiver
// is required: encoding/json only calls UnmarshalJSON on an addressable
// value, and this method must write back through *s. It returns an error for
// unknown names rather than leaving the zero value — a corrupt stored file
// should fail loudly, never load as "pending" by accident.
func (s *Status) UnmarshalJSON(data []byte) error {
	var name string
	if err := json.Unmarshal(data, &name); err != nil {
		return fmt.Errorf("job: status must be a string: %w", err)
	}
	switch name {
	case "pending":
		*s = StatusPending
	case "running":
		*s = StatusRunning
	case "succeeded":
		*s = StatusSucceeded
	case "failed":
		*s = StatusFailed
	case "cancelled":
		*s = StatusCancelled
	default:
		return fmt.Errorf("job: unknown status %q", name)
	}
	return nil
}

// Compile-time proof that Status implements fmt.Stringer. If the method set
// ever drifts, this line stops the build — no runtime check required.
var _ fmt.Stringer = Status(0)

/*
Priority orders pending records. Like Status it is a named type over int, so
the compiler refuses to let a priority be confused with any other number —
a count, an index, an attempt.
*/
type Priority int

/*
iota - 1 is the whole point of this block. PriorityNormal must sit at zero:
a store file written before V7 has no priority field, and a client that never
mentions priority sends none, so BOTH mean "normal" with no special case
anywhere. The other levels fall out around it, and the numeric order
(low < normal < high < critical) is exactly what the queue's Less relies on.
*/
const (
	PriorityLow      Priority = iota - 1 // -1
	PriorityNormal                       //  0
	PriorityHigh                         //  1
	PriorityCritical                     //  2
)

// String makes Priority satisfy fmt.Stringer, exactly as Status does.
func (p Priority) String() string {
	switch p {
	case PriorityLow:
		return "low"
	case PriorityNormal:
		return "normal"
	case PriorityHigh:
		return "high"
	case PriorityCritical:
		return "critical"
	default:
		// int(p) keeps fmt from re-entering String() through its Stringer
		// path, which would recurse forever.
		return fmt.Sprintf("Priority(%d)", int(p))
	}
}

// MarshalJSON writes the name rather than the number, so the store file and
// the API stay readable and a renumbering cannot silently reorder jobs.
func (p Priority) MarshalJSON() ([]byte, error) {
	return json.Marshal(p.String())
}

// UnmarshalJSON accepts a known name. An unknown one is an error, not
// "normal": a typo in a stored file must fail loudly instead of quietly
// reordering the queue.
func (p *Priority) UnmarshalJSON(data []byte) error {
	var name string
	if err := json.Unmarshal(data, &name); err != nil {
		return fmt.Errorf("job: priority must be a string: %w", err)
	}
	switch name {
	case "low":
		*p = PriorityLow
	case "normal":
		*p = PriorityNormal
	case "high":
		*p = PriorityHigh
	case "critical":
		*p = PriorityCritical
	default:
		return fmt.Errorf("job: unknown priority %q", name)
	}
	return nil
}

// Compile-time proof that Priority implements fmt.Stringer.
var _ fmt.Stringer = Priority(0)

// MaxResourcesPerJob bounds one claim. The store's file is the whole database,
// so one job may not ask for an unbounded list of names.
const MaxResourcesPerJob = 8

/*
ValidateResources checks the SHAPE of a resource claim: non-empty names, no
duplicates, a bounded count. Availability is not job's business — the resource
pool answers that and the API asks it. Duplicates are rejected because a job
that asks for one unit twice would wait for itself forever.
*/
func ValidateResources(names []string) error {
	if len(names) > MaxResourcesPerJob {
		return fmt.Errorf("job: %d resources claimed, limit %d: %w",
			len(names), MaxResourcesPerJob, ErrInvalidJob)
	}
	seen := make(map[string]struct{}, len(names))
	for _, name := range names {
		if name == "" {
			return fmt.Errorf("job: empty resource name: %w", ErrInvalidJob)
		}
		if _, dup := seen[name]; dup {
			return fmt.Errorf("job: resource %q claimed twice: %w", name, ErrInvalidJob)
		}
		seen[name] = struct{}{}
	}
	return nil
}

/*
Terminal reports whether a status can still change. A DELETE may only act on
a non-terminal job; this is the state machine's own answer, not a list of
strings duplicated in the API.
*/
func (s Status) Terminal() bool {
	switch s {
	case StatusSucceeded, StatusFailed, StatusCancelled:
		return true
	default:
		return false
	}
}

/*
transitions is the state machine as data. A map of legal destinations beats a
switch in every call site: one place to read the lifecycle, one place to test
it. Same→same is handled by CanTransition, not listed here, because an
idempotent re-save is true for every status.
*/
var transitions = map[Status]map[Status]bool{
	StatusPending: {
		StatusRunning:   true,
		StatusCancelled: true,
		StatusFailed:    true, // an exhausted budget fails before any attempt starts
	},
	StatusRunning: {
		StatusSucceeded: true,
		StatusFailed:    true,
		StatusCancelled: true,
		StatusPending:   true, // recovery: interrupted by a restart
	},
}

/*
CanTransition reports whether s may become to. Reading a missing map key
returns the zero value (false) — Go makes "not allowed" the default, which is
the safe direction for a state machine.
*/
func (s Status) CanTransition(to Status) bool {
	if s == to {
		return true
	}
	return transitions[s][to]
}

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
