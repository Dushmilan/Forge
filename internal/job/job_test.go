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
