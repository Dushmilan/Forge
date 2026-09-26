package job_test

import (
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
