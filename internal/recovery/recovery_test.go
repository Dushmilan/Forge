package recovery_test

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/Dushmilan/Forge/internal/job"
	"github.com/Dushmilan/Forge/internal/recovery"
	"github.com/Dushmilan/Forge/internal/store"
)

func seed(t *testing.T, st *store.Store, recs ...store.Record) {
	t.Helper()
	for _, rec := range recs {
		if err := st.Save(rec); err != nil {
			t.Fatalf("seed %s: %v", rec.ID, err)
		}
	}
}

/*
TestRecoverRepairsOrphans pins the startup table: an interrupted running job
with budget left is requeued at its persisted attempt count, a pending record
is requeued unchanged, an exhausted one fails, and terminal records are
untouched.
*/
func TestRecoverRepairsOrphans(t *testing.T) {
	st := store.New(t.TempDir())
	seed(t, st,
		store.Record{ID: "a-pending", Status: job.StatusPending},
		store.Record{ID: "b-running", Status: job.StatusRunning, Attempts: 1},
		store.Record{ID: "c-spent", Status: job.StatusRunning, Attempts: 3},
		store.Record{ID: "d-done", Status: job.StatusSucceeded, Attempts: 1},
	)

	got, err := recovery.Recover(st, recovery.Config{MaxAttempts: 3})
	if err != nil {
		t.Fatalf("Recover() error: %v", err)
	}

	if len(got) != 2 || got[0].ID != "a-pending" || got[1].ID != "b-running" {
		t.Fatalf("requeue = %+v, want a-pending then b-running", got)
	}
	if got[1].Attempts != 1 {
		t.Errorf("requeued Attempts = %d, want 1 — recovery must not reset the budget", got[1].Attempts)
	}

	b, _ := st.Get("b-running")
	if b.Status != job.StatusPending {
		t.Errorf("b-running status = %v, want pending", b.Status)
	}
	if b.Err == "" {
		t.Error("b-running Err is empty, want the interruption recorded")
	}

	c, _ := st.Get("c-spent")
	if c.Status != job.StatusFailed {
		t.Errorf("c-spent status = %v, want failed", c.Status)
	}

	d, _ := st.Get("d-done")
	if d.Status != job.StatusSucceeded || d.Err != "" {
		t.Errorf("d-done = %+v, want untouched", d)
	}
}

/*
TestRecoverIsIdempotent pins that a second start with nothing to repair does
not rewrite or requeue anything: recovery must be safe to run on every boot.
*/
func TestRecoverIsIdempotent(t *testing.T) {
	st := store.New(t.TempDir())
	seed(t, st, store.Record{ID: "done", Status: job.StatusSucceeded})

	got, err := recovery.Recover(st, recovery.Config{MaxAttempts: 3})
	if err != nil {
		t.Fatalf("Recover() error: %v", err)
	}
	if len(got) != 0 {
		t.Errorf("requeue = %+v, want none", got)
	}
}

/*
TestRecoverFailsLoudlyOnCorruptStore pins honesty at startup: unknown state
must be an error, never an empty queue that silently serves.
*/
func TestRecoverFailsLoudlyOnCorruptStore(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "jobs.json"), []byte("{not json"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := recovery.Recover(store.New(dir), recovery.Config{MaxAttempts: 3}); err == nil {
		t.Fatal("Recover(corrupt) = nil, want an error")
	}
}

/*
TestRecoverPreservesHeartbeatOnRequeue pins the field-ownership rule through
recovery: the crash timestamp stays on the requeued record as evidence of
when the previous process last saw it alive.
*/
func TestRecoverPreservesHeartbeatOnRequeue(t *testing.T) {
	st := store.New(t.TempDir())
	at := time.Date(2026, 10, 30, 12, 0, 0, 0, time.UTC)
	seed(t, st, store.Record{ID: "beat", Status: job.StatusRunning, Attempts: 1, LastHeartbeat: &at})

	if _, err := recovery.Recover(st, recovery.Config{MaxAttempts: 3}); err != nil {
		t.Fatalf("Recover() error: %v", err)
	}
	got, _ := st.Get("beat")
	if got.LastHeartbeat == nil || !got.LastHeartbeat.Equal(at) {
		t.Errorf("LastHeartbeat = %v, want %v", got.LastHeartbeat, at)
	}
}
