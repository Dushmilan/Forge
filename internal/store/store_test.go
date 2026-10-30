package store_test

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
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

/*
TestSaveNeverOverwritesHeartbeat pins the ownership rule with the value that
actually occurs after recovery: the worker's snapshot can carry an OLDER,
non-nil heartbeat taken from the requeued record. Save must still keep the
stored value; otherwise every attempt boundary rewinds liveness to the crash
timestamp, and only Heartbeat may ever advance it.
*/
func TestSaveNeverOverwritesHeartbeat(t *testing.T) {
	s := store.New(t.TempDir())
	if err := s.Save(store.Record{ID: "live", Status: job.StatusRunning}); err != nil {
		t.Fatal(err)
	}
	newer := time.Date(2026, 10, 30, 12, 0, 1, 0, time.UTC)
	if err := s.Heartbeat("live", newer); err != nil {
		t.Fatal(err)
	}

	older := newer.Add(-time.Minute)
	if err := s.Save(store.Record{ID: "live", Status: job.StatusRunning, Attempts: 2, LastHeartbeat: &older}); err != nil {
		t.Fatal(err)
	}
	got, _ := s.Get("live")
	if got.LastHeartbeat == nil || !got.LastHeartbeat.Equal(newer) {
		t.Errorf("LastHeartbeat = %v, want the stored %v", got.LastHeartbeat, newer)
	}
}

/*
TestNullFileFailsLoudly pins that a `null` document is corruption, not an
empty store: encoding/json happily leaves the map nil, and the next Save would
panic assigning into it. Startup must reject it instead.
*/
func TestNullFileFailsLoudly(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "jobs.json"), []byte("null"), 0o644); err != nil {
		t.Fatal(err)
	}
	s := store.New(dir)

	if _, err := s.List(); err == nil {
		t.Error("List(null) = nil error, want a corruption error")
	}
	if _, err := s.Get("any"); err == nil {
		t.Error("Get(null) = nil error, want a corruption error")
	}
	if err := s.Save(store.Record{ID: "new", Status: job.StatusPending}); err == nil {
		t.Error("Save(null) = nil, want an error instead of a panic")
	}
}

/*
TestCreateDedupesByKey pins the headline: the second submission with a key is
not a second job. It returns the stored record, reports created=false, and
leaves the file byte-identical — a replay must not rewrite anything.
*/
func TestCreateDedupesByKey(t *testing.T) {
	dir := t.TempDir()
	s := store.New(dir)

	first, created, err := s.Create(store.Record{
		ID: "first", Command: []string{"echo", "hi"}, Status: job.StatusPending, IdempotencyKey: "k1",
	})
	if err != nil || !created {
		t.Fatalf("Create(first) = %v, created=%v, want created", err, created)
	}

	before, err := os.ReadFile(filepath.Join(dir, "jobs.json"))
	if err != nil {
		t.Fatal(err)
	}
	second, created, err := s.Create(store.Record{
		ID: "second", Command: []string{"echo", "hi"}, Status: job.StatusPending, IdempotencyKey: "k1",
	})
	if err != nil {
		t.Fatalf("Create(second) error: %v", err)
	}
	if created {
		t.Fatal("Create(second) created a record, want a replay")
	}
	if second.ID != first.ID {
		t.Errorf("replay id = %q, want %q", second.ID, first.ID)
	}

	after, err := os.ReadFile(filepath.Join(dir, "jobs.json"))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(before, after) {
		t.Errorf("a replay rewrote jobs.json:\nbefore %s\nafter  %s", before, after)
	}
}

/*
TestCreateKeyRules pins the key's scope: distinct keys are distinct jobs, and
an empty key means "no dedupe" — the V1–V5 behaviour every old client gets.
*/
func TestCreateKeyRules(t *testing.T) {
	s := store.New(t.TempDir())

	for _, tt := range []struct {
		id, key string
	}{
		{"a", "k1"},
		{"b", "k2"},
		{"c", ""},
		{"d", ""},
	} {
		_, created, err := s.Create(store.Record{
			ID: tt.id, Command: []string{"true"}, Status: job.StatusPending, IdempotencyKey: tt.key,
		})
		if err != nil || !created {
			t.Fatalf("Create(%s, key=%q) = %v, created=%v, want created", tt.id, tt.key, err, created)
		}
	}

	got, err := s.List()
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 4 {
		t.Errorf("stored %d records, want 4 (empty keys never dedupe)", len(got))
	}
}

/*
TestCreateKeySurvivesRestart pins durability: the index is derived from disk,
so a new process must find the same key. Without this, a crash would reset
idempotency to "everything is new again".
*/
func TestCreateKeySurvivesRestart(t *testing.T) {
	dir := t.TempDir()
	first, _, err := store.New(dir).Create(store.Record{
		ID: "first", Command: []string{"true"}, Status: job.StatusPending, IdempotencyKey: "k1",
	})
	if err != nil {
		t.Fatal(err)
	}

	got, created, err := store.New(dir).Create(store.Record{
		ID: "second", Command: []string{"true"}, Status: job.StatusPending, IdempotencyKey: "k1",
	})
	if err != nil {
		t.Fatalf("Create after restart: %v", err)
	}
	if created || got.ID != first.ID {
		t.Errorf("after restart: created=%v id=%q, want replay of %q", created, got.ID, first.ID)
	}
}

/*
TestSavePreservesIdempotencyKey pins the second half of field ownership: a
worker saves a record it built from a queue snapshot, and that snapshot must
not be able to erase the client's key.
*/
func TestSavePreservesIdempotencyKey(t *testing.T) {
	s := store.New(t.TempDir())
	if _, _, err := s.Create(store.Record{
		ID: "keyed", Command: []string{"true"}, Status: job.StatusPending, IdempotencyKey: "k1",
	}); err != nil {
		t.Fatal(err)
	}

	// The worker's view: same record, no key, after an attempt.
	if err := s.Save(store.Record{
		ID: "keyed", Command: []string{"true"}, Status: job.StatusRunning, Attempts: 1,
	}); err != nil {
		t.Fatal(err)
	}
	got, _ := s.Get("keyed")
	if got.IdempotencyKey != "k1" {
		t.Errorf("IdempotencyKey = %q, want k1 after a worker save", got.IdempotencyKey)
	}
}

/*
TestCreateRejectsDuplicateKeysInFile pins honesty over convenience: a store
that already contains two records for one key is corrupt, and guessing which
one "wins" would silently lose a job.
*/
func TestCreateRejectsDuplicateKeysInFile(t *testing.T) {
	dir := t.TempDir()
	body := `{
  "a": {"id": "a", "command": ["true"], "status": "pending", "idempotency_key": "k1"},
  "b": {"id": "b", "command": ["true"], "status": "pending", "idempotency_key": "k1"}
}`
	if err := os.WriteFile(filepath.Join(dir, "jobs.json"), []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}

	_, _, err := store.New(dir).Create(store.Record{
		ID: "c", Command: []string{"true"}, Status: job.StatusPending, IdempotencyKey: "k2",
	})
	if err == nil {
		t.Fatal("Create(corrupt keys) = nil, want an error")
	}
	if !strings.Contains(err.Error(), `"a"`) || !strings.Contains(err.Error(), `"b"`) {
		t.Errorf("error = %v, want it to name both ids", err)
	}
}

/*
TestConcurrentCreatesOneWinner pins Review Focus 1: N clients retrying the
same key at once must produce one record. The mutex is the guarantee; -race is
the proof that the derived index is not read outside it.
*/
func TestConcurrentCreatesOneWinner(t *testing.T) {
	s := store.New(t.TempDir())

	const n = 10
	type result struct {
		rec     store.Record
		created bool
		err     error
	}
	out := make(chan result, n)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			rec, created, err := s.Create(store.Record{
				ID:             fmt.Sprintf("j%d", i),
				Command:        []string{"true"},
				Status:         job.StatusPending,
				IdempotencyKey: "same",
			})
			out <- result{rec, created, err}
		}(i)
	}
	wg.Wait()
	close(out)

	var winners int
	var id string
	results := make([]result, 0, n)
	for r := range out {
		results = append(results, r)
	}
	for _, r := range results {
		if r.err != nil {
			t.Fatalf("Create error: %v", r.err)
		}
		if r.created {
			winners++
			id = r.rec.ID
		}
	}
	if winners != 1 {
		t.Fatalf("%d creates won, want exactly 1", winners)
	}
	// Every loser must have been told which record won: that is what the API
	// returns to the retrying client.
	for _, r := range results {
		if !r.created && r.rec.ID != id {
			t.Errorf("loser returned id %q, want the winner %q", r.rec.ID, id)
		}
	}

	all, err := s.List()
	if err != nil {
		t.Fatal(err)
	}
	if len(all) != 1 || all[0].ID != id {
		t.Errorf("store holds %+v, want only the winner %s", all, id)
	}
}

/*
TestCreateRejectsExistingID pins Create as admission only: it never updates.
Without this, an id collision could overwrite a record, keep the old key in
storage, and report the incoming key as created — response, queue and file
disagreeing at once.
*/
func TestCreateRejectsExistingID(t *testing.T) {
	for _, tc := range []struct {
		name string
		key  string
	}{
		{"keyed", "old"},
		{"unkeyed", ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			s := store.New(dir)
			if _, _, err := s.Create(store.Record{
				ID: "x", Command: []string{"one"}, Status: job.StatusPending, IdempotencyKey: tc.key,
			}); err != nil {
				t.Fatal(err)
			}
			before, err := os.ReadFile(filepath.Join(dir, "jobs.json"))
			if err != nil {
				t.Fatal(err)
			}

			_, created, err := s.Create(store.Record{
				ID: "x", Command: []string{"two"}, Status: job.StatusPending, IdempotencyKey: "new",
			})
			if err == nil || created {
				t.Fatalf("Create(existing id) = created=%v err=%v, want a refusal", created, err)
			}

			after, err := os.ReadFile(filepath.Join(dir, "jobs.json"))
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(before, after) {
				t.Errorf("a refused Create rewrote jobs.json")
			}
			got, _ := s.Get("x")
			if got.Command[0] != "one" || got.IdempotencyKey != tc.key {
				t.Errorf("stored record = %+v, want the original untouched", got)
			}
		})
	}
}

/*
TestSaveCannotCreateKeyedRecord pins the other half of admission: Save is the
update path. Letting it introduce a new keyed record would bypass Create's
uniqueness check and duplicate a key without anyone editing the file.
*/
func TestSaveCannotCreateKeyedRecord(t *testing.T) {
	s := store.New(t.TempDir())
	if _, _, err := s.Create(store.Record{
		ID: "a", Command: []string{"true"}, Status: job.StatusPending, IdempotencyKey: "k1",
	}); err != nil {
		t.Fatal(err)
	}

	err := s.Save(store.Record{
		ID: "b", Command: []string{"true"}, Status: job.StatusPending, IdempotencyKey: "k1",
	})
	if err == nil {
		t.Fatal("Save created a second record with an existing key")
	}
	if _, gerr := s.Get("b"); !errors.Is(gerr, store.ErrNotFound) {
		t.Errorf("Get(b) = %v, want ErrNotFound — the refused save wrote nothing", gerr)
	}
}
