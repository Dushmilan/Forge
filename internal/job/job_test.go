package job_test

import (
	"encoding/json"
	"errors"
	"testing"

	"github.com/Dushmilan/Forge/internal/job"
)

// TestStatusString pins the human-readable name of every Job status.
// A table of cases is the idiomatic Go shape when one behavior has many inputs:
// the loop and assertions are written once, each case is one line.
func TestStatusString(t *testing.T) {
	cases := []struct {
		name   string
		status job.Status
		want   string
	}{
		{"pending", job.StatusPending, "pending"},
		{"running", job.StatusRunning, "running"},
		{"succeeded", job.StatusSucceeded, "succeeded"},
		{"failed", job.StatusFailed, "failed"},
		{"cancelled", job.StatusCancelled, "cancelled"},
	}

	for _, tc := range cases {
		// t.Run gives each case its own named subtest, so a failure points at
		// exactly which input broke without stopping the others.
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.status.String(); got != tc.want {
				t.Errorf("Status.String() = %q, want %q", got, tc.want)
			}
		})
	}
}

// TestJobValidate pins the rules for a Job worth running. Invalid input is a
// normal outcome in Go — not an exception — so the test checks both that an
// error is returned and that it wraps the ErrInvalidJob sentinel, which lets
// callers branch on the KIND of failure with errors.Is instead of string matching.
func TestJobValidate(t *testing.T) {
	cases := []struct {
		name    string
		job     job.Job
		wantErr bool
	}{
		{"valid", job.Job{ID: "job-1", Command: []string{"echo", "hi"}}, false},
		{"missing id", job.Job{Command: []string{"echo", "hi"}}, true},
		{"missing command", job.Job{ID: "job-1"}, true},
		{"blank program name", job.Job{ID: "job-1", Command: []string{""}}, true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.job.Validate()

			if !tc.wantErr {
				if err != nil {
					t.Fatalf("Validate() = %v, want nil", err)
				}
				return
			}

			if err == nil {
				t.Fatal("Validate() = nil, want an error")
			}
			if !errors.Is(err, job.ErrInvalidJob) {
				t.Errorf("Validate() error = %v, want it to wrap ErrInvalidJob", err)
			}
		})
	}
}

// TestStatusJSONRoundTrip pins Status to its string name across JSON, so a
// persisted job reads "succeeded" instead of the opaque iota 2. It also pins
// that only known names decode — a typo in a stored file must be an error,
// not a silent Status(0).
func TestStatusJSONRoundTrip(t *testing.T) {
	tests := []struct {
		name   string
		status job.Status
		want   string
	}{
		{name: "pending", status: job.StatusPending, want: `"pending"`},
		{name: "running", status: job.StatusRunning, want: `"running"`},
		{name: "succeeded", status: job.StatusSucceeded, want: `"succeeded"`},
		{name: "failed", status: job.StatusFailed, want: `"failed"`},
		{name: "cancelled", status: job.StatusCancelled, want: `"cancelled"`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b, err := json.Marshal(tt.status)
			if err != nil {
				t.Fatalf("Marshal(%v) error: %v", tt.status, err)
			}
			if string(b) != tt.want {
				t.Errorf("Marshal(%v) = %s, want %s", tt.status, b, tt.want)
			}

			var back job.Status
			if err := json.Unmarshal(b, &back); err != nil {
				t.Fatalf("Unmarshal(%s) error: %v", b, err)
			}
			if back != tt.status {
				t.Errorf("round trip = %v, want %v", back, tt.status)
			}
		})
	}
}

// TestStatusUnmarshalRejectsUnknown pins that decoding is strict: an
// unrecognized name (and a bare number, which would decode against the old
// int representation) is an error, not a zero-value Status.
func TestStatusUnmarshalRejectsUnknown(t *testing.T) {
	for _, in := range []string{`"weird"`, `2`} {
		var s job.Status
		if err := json.Unmarshal([]byte(in), &s); err == nil {
			t.Errorf("Unmarshal(%s) = nil error, want error", in)
		}
	}
}

/*
TestStatusTerminal pins the state machine's final states. The API uses it to
answer 409 for DELETE on a job that already ended — a cancelled job is as
final as a succeeded one.
*/
func TestStatusTerminal(t *testing.T) {
	tests := []struct {
		status job.Status
		want   bool
	}{
		{job.StatusPending, false},
		{job.StatusRunning, false},
		{job.StatusSucceeded, true},
		{job.StatusFailed, true},
		{job.StatusCancelled, true},
	}
	for _, tt := range tests {
		if got := tt.status.Terminal(); got != tt.want {
			t.Errorf("%v.Terminal() = %v, want %v", tt.status, got, tt.want)
		}
	}
}

/*
TestStatusCanTransition pins the lifecycle as one table. same→same is legal
because a worker re-saves running on every attempt; terminal statuses accept
nothing different; recovery is the only edge that moves running backwards.
*/
func TestStatusCanTransition(t *testing.T) {
	tests := []struct {
		name string
		from job.Status
		to   job.Status
		want bool
	}{
		{"pending to running", job.StatusPending, job.StatusRunning, true},
		{"pending to cancelled", job.StatusPending, job.StatusCancelled, true},
		{"pending to failed", job.StatusPending, job.StatusFailed, true},
		{"pending to succeeded", job.StatusPending, job.StatusSucceeded, false},
		{"running to succeeded", job.StatusRunning, job.StatusSucceeded, true},
		{"running to failed", job.StatusRunning, job.StatusFailed, true},
		{"running to cancelled", job.StatusRunning, job.StatusCancelled, true},
		{"running to pending", job.StatusRunning, job.StatusPending, true},
		{"running to itself", job.StatusRunning, job.StatusRunning, true},
		{"succeeded to running", job.StatusSucceeded, job.StatusRunning, false},
		{"failed to pending", job.StatusFailed, job.StatusPending, false},
		{"cancelled to succeeded", job.StatusCancelled, job.StatusSucceeded, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.from.CanTransition(tt.to); got != tt.want {
				t.Errorf("%v.CanTransition(%v) = %v, want %v", tt.from, tt.to, got, tt.want)
			}
		})
	}
}

/*
TestPriorityNormalIsZero pins the compatibility rule the whole version rests
on: the zero value must mean "normal", so an absent field and a V6 file both
decode without a special case.
*/
func TestPriorityNormalIsZero(t *testing.T) {
	var p job.Priority
	if p != job.PriorityNormal {
		t.Errorf("zero Priority = %q, want normal", p)
	}
}

/*
TestPriorityOrdering pins the direction container/heap will depend on: the
numeric order is low < normal < high < critical. Less compares these values.
*/
func TestPriorityOrdering(t *testing.T) {
	if !(job.PriorityLow < job.PriorityNormal &&
		job.PriorityNormal < job.PriorityHigh &&
		job.PriorityHigh < job.PriorityCritical) {
		t.Errorf("priority values are not ordered low < normal < high < critical")
	}
}

/*
TestPriorityJSONRoundTrip pins the wire form: readable names, and an unknown
name is an error rather than a silent "normal".
*/
func TestPriorityJSONRoundTrip(t *testing.T) {
	for _, tt := range []struct {
		p    job.Priority
		name string
	}{
		{job.PriorityLow, `"low"`},
		{job.PriorityNormal, `"normal"`},
		{job.PriorityHigh, `"high"`},
		{job.PriorityCritical, `"critical"`},
	} {
		b, err := json.Marshal(tt.p)
		if err != nil {
			t.Fatalf("Marshal(%v): %v", tt.p, err)
		}
		if string(b) != tt.name {
			t.Errorf("Marshal(%v) = %s, want %s", tt.p, b, tt.name)
		}
		var got job.Priority
		if err := json.Unmarshal(b, &got); err != nil {
			t.Fatalf("Unmarshal(%s): %v", b, err)
		}
		if got != tt.p {
			t.Errorf("round trip %s = %v, want %v", b, got, tt.p)
		}
	}
}

func TestPriorityUnknownNameFails(t *testing.T) {
	var p job.Priority
	if err := json.Unmarshal([]byte(`"urgent"`), &p); err == nil {
		t.Fatal("Unmarshal(urgent) = nil, want an error")
	}
}

/*
TestValidateResources pins the claim's shape — not its availability, which
belongs to the pool. Duplicates matter: ["gpu","gpu"] with capacity 1 would
wait for itself forever.
*/
func TestValidateResources(t *testing.T) {
	for _, tt := range []struct {
		name    string
		names   []string
		wantErr bool
	}{
		{"none", nil, false},
		{"one", []string{"gpu"}, false},
		{"two", []string{"gpu", "db"}, false},
		{"empty name", []string{""}, true},
		{"empty among good", []string{"gpu", ""}, true},
		{"duplicate", []string{"gpu", "gpu"}, true},
		{"too many", []string{"r1", "r2", "r3", "r4", "r5", "r6", "r7", "r8", "r9"}, true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			err := job.ValidateResources(tt.names)
			if tt.wantErr && !errors.Is(err, job.ErrInvalidJob) {
				t.Errorf("ValidateResources(%v) = %v, want an ErrInvalidJob", tt.names, err)
			}
			if !tt.wantErr && err != nil {
				t.Errorf("ValidateResources(%v) = %v, want nil", tt.names, err)
			}
		})
	}
}
