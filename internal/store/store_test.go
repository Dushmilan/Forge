package store_test

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/Dushmilan/Forge/internal/executor"
	"github.com/Dushmilan/Forge/internal/job"
	"github.com/Dushmilan/Forge/internal/store"
)

// TestSaveGetRoundTrip pins the store's whole reason to exist: what Save
// writes, a *fresh* Store instance (newly constructed, as after a process
// restart) reads back unchanged. The nested directory that does not exist yet
// pins that Save creates it — production starts with no ./data.
func TestSaveGetRoundTrip(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "nested", "data")
	s := store.New(dir)

	want := store.Record{
		ID:      "abc123",
		Command: []string{"echo", "hi"},
		Status:  job.StatusSucceeded,
		Result:  executor.Result{ExitCode: 0, Stdout: "hi\n"},
	}
	if err := s.Save(want); err != nil {
		t.Fatalf("Save() error: %v", err)
	}

	got, err := store.New(dir).Get("abc123")
	if err != nil {
		t.Fatalf("Get() error: %v", err)
	}
	if got.ID != want.ID || got.Status != want.Status || got.Result.Stdout != want.Result.Stdout {
		t.Errorf("Get() = %+v, want %+v", got, want)
	}

	// The file on disk must name the status as a string — this is what
	// consumes Task 1's MarshalJSON and pins the storage format.
	b, err := os.ReadFile(filepath.Join(dir, "jobs.json"))
	if err != nil {
		t.Fatalf("read jobs.json: %v", err)
	}
	if !bytes.Contains(b, []byte(`"status": "succeeded"`)) {
		t.Errorf("jobs.json = %s, want status as string", b)
	}
}

// TestGetMissing wraps the sentinel: callers distinguish "no such job"
// (404 later) from "the disk is on fire" (500) with errors.Is, so the
// identity must survive the message formatting.
func TestGetMissing(t *testing.T) {
	s := store.New(t.TempDir())

	_, err := s.Get("ghost")
	if !errors.Is(err, store.ErrNotFound) {
		t.Errorf("Get(missing) = %v, want ErrNotFound", err)
	}
}

// TestCorruptFileFailsLoudly pins the failure mode for a jobs.json that a
// crash left half-written (Review Focus 5): both operations surface an
// error, neither panics, and Save does NOT clobber what it cannot read.
func TestCorruptFileFailsLoudly(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "jobs.json"), []byte("{not json"), 0o644); err != nil {
		t.Fatal(err)
	}
	s := store.New(dir)

	if _, err := s.Get("any"); err == nil || errors.Is(err, store.ErrNotFound) {
		t.Errorf("Get(corrupt) = %v, want a decode error (not ErrNotFound)", err)
	}
	if err := s.Save(store.Record{ID: "new"}); err == nil {
		t.Error("Save(corrupt) = nil, want a decode error — silent overwrite loses data")
	}
}

// TestConcurrentSaves pins the mutex's reason to exist at the store level
// (Review Focus 4): without it, racing read-modify-write cycles lose
// records — and -race flags the data race. Five goroutines, five records,
// all five must survive.
func TestConcurrentSaves(t *testing.T) {
	s := store.New(t.TempDir())

	var wg sync.WaitGroup
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done() // defer inside the goroutine runs before wg.Done returns up the stack
			id := string(rune('a' + i))
			if err := s.Save(store.Record{ID: id, Status: job.StatusSucceeded}); err != nil {
				t.Errorf("Save(%s) error: %v", id, err)
			}
		}(i)
	}
	wg.Wait() // the channel-based join: Wait blocks until every Done has fired

	for i := 0; i < 5; i++ {
		id := string(rune('a' + i))
		if _, err := s.Get(id); err != nil {
			t.Errorf("Get(%s) after concurrent saves: %v", id, err)
		}
	}
}

/*
TestSaveRejectsIllegalTransition pins the state machine at the write: a
finished job may not run again, the error wraps the sentinel, and the file is
untouched — a rejected write must not partially apply.
*/
func TestSaveRejectsIllegalTransition(t *testing.T) {
	dir := t.TempDir()
	s := store.New(dir)
	if err := s.Save(store.Record{ID: "done", Status: job.StatusSucceeded}); err != nil {
		t.Fatalf("seed Save() error: %v", err)
	}
	before, err := os.ReadFile(filepath.Join(dir, "jobs.json"))
	if err != nil {
		t.Fatal(err)
	}

	err = s.Save(store.Record{ID: "done", Status: job.StatusRunning})
	if !errors.Is(err, job.ErrInvalidTransition) {
		t.Fatalf("Save(succeeded→running) = %v, want wrapped ErrInvalidTransition", err)
	}

	after, err := os.ReadFile(filepath.Join(dir, "jobs.json"))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(before, after) {
		t.Errorf("jobs.json changed after a rejected transition:\nbefore %s\nafter  %s", before, after)
	}
	if got, _ := store.New(dir).Get("done"); got.Status != job.StatusSucceeded {
		t.Errorf("status = %v, want succeeded", got.Status)
	}
}

/*
TestSaveAllowsRecoveryTransition pins the one backwards edge: running →
pending is how a restart requeues an interrupted job. Without it, recovery
could not write its own repair.
*/
func TestSaveAllowsRecoveryTransition(t *testing.T) {
	s := store.New(t.TempDir())
	if err := s.Save(store.Record{ID: "orphan", Status: job.StatusRunning, Attempts: 1}); err != nil {
		t.Fatalf("seed Save() error: %v", err)
	}
	if err := s.Save(store.Record{ID: "orphan", Status: job.StatusPending, Attempts: 1}); err != nil {
		t.Fatalf("Save(running→pending) = %v, want nil", err)
	}
	if got, _ := s.Get("orphan"); got.Status != job.StatusPending {
		t.Errorf("status = %v, want pending", got.Status)
	}
}

/*
TestHeartbeatUpdatesOnlyTheTimestamp pins the field-ownership rule: the
heartbeat writer touches LastHeartbeat and nothing else. It also proves the
record must be running — a finished job is not alive, so a late tick is
rejected with a sentinel instead of resurrecting it.
*/
func TestHeartbeatUpdatesOnlyTheTimestamp(t *testing.T) {
	s := store.New(t.TempDir())
	if err := s.Save(store.Record{ID: "live", Status: job.StatusRunning, Attempts: 1, Err: "boom"}); err != nil {
		t.Fatal(err)
	}

	at := time.Date(2026, 10, 30, 12, 0, 0, 0, time.UTC)
	if err := s.Heartbeat("live", at); err != nil {
		t.Fatalf("Heartbeat() error: %v", err)
	}

	got, err := s.Get("live")
	if err != nil {
		t.Fatal(err)
	}
	if got.LastHeartbeat == nil || !got.LastHeartbeat.Equal(at) {
		t.Errorf("LastHeartbeat = %v, want %v", got.LastHeartbeat, at)
	}
	if got.Status != job.StatusRunning || got.Attempts != 1 || got.Err != "boom" {
		t.Errorf("Heartbeat changed more than the timestamp: %+v", got)
	}

	if err := s.Save(store.Record{ID: "live", Status: job.StatusSucceeded}); err != nil {
		t.Fatalf("Save() error: %v", err)
	}
	if err := s.Heartbeat("live", at.Add(time.Second)); !errors.Is(err, store.ErrNotRunning) {
		t.Errorf("Heartbeat(finished) = %v, want ErrNotRunning", err)
	}
	if got, _ := s.Get("live"); got.LastHeartbeat == nil || !got.LastHeartbeat.Equal(at) {
		t.Errorf("a rejected heartbeat changed the timestamp: %v", got.LastHeartbeat)
	}
}

/*
TestSavePreservesHeartbeat pins the other half of ownership: the worker saves
whole records and never knows the heartbeat, so Save must keep the stored
value. Otherwise every attempt boundary would rewind liveness to nil.
*/
func TestSavePreservesHeartbeat(t *testing.T) {
	s := store.New(t.TempDir())
	if err := s.Save(store.Record{ID: "live", Status: job.StatusRunning}); err != nil {
		t.Fatal(err)
	}
	at := time.Date(2026, 10, 30, 12, 0, 0, 0, time.UTC)
	if err := s.Heartbeat("live", at); err != nil {
		t.Fatal(err)
	}

	// The worker's snapshot predates the heartbeat: nil heartbeat, same status.
	if err := s.Save(store.Record{ID: "live", Status: job.StatusRunning, Attempts: 2}); err != nil {
		t.Fatal(err)
	}
	got, _ := s.Get("live")
	if got.LastHeartbeat == nil || !got.LastHeartbeat.Equal(at) {
		t.Errorf("Save rewound LastHeartbeat to %v, want %v", got.LastHeartbeat, at)
	}
	if got.Attempts != 2 {
		t.Errorf("Attempts = %d, want the save's value", got.Attempts)
	}
}

/*
TestHeartbeatMissingID pins the difference between "not running" and "not
there": recovery and the API answer 404 for the latter, so the sentinels must
stay distinguishable.
*/
func TestHeartbeatMissingID(t *testing.T) {
	if err := store.New(t.TempDir()).Heartbeat("ghost", time.Now()); !errors.Is(err, store.ErrNotFound) {
		t.Errorf("Heartbeat(missing) = %v, want ErrNotFound", err)
	}
}

/*
TestListIsOrderedByID pins recovery's input: a deterministic snapshot. Map
iteration is randomized in Go, so without sorting, recovery would repair
records in a different order on every start — untestable and surprising.
*/
func TestListIsOrderedByID(t *testing.T) {
	s := store.New(t.TempDir())
	for _, id := range []string{"c", "a", "b"} {
		if err := s.Save(store.Record{ID: id, Status: job.StatusPending}); err != nil {
			t.Fatal(err)
		}
	}
	got, err := s.List()
	if err != nil {
		t.Fatalf("List() error: %v", err)
	}
	if len(got) != 3 || got[0].ID != "a" || got[1].ID != "b" || got[2].ID != "c" {
		t.Errorf("List() = %+v, want ids a,b,c", got)
	}
}
