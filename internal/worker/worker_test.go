package worker_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Dushmilan/Forge/internal/executor"
	"github.com/Dushmilan/Forge/internal/job"
	"github.com/Dushmilan/Forge/internal/store"
	"github.com/Dushmilan/Forge/internal/worker"
)

// runnerFunc lets a test turn a function into an executor.Runner: Go
// interfaces are satisfied implicitly, so no struct or mock framework is
// needed for a one-method seam.
type runnerFunc func(context.Context, job.Job) (executor.Result, error)

func (f runnerFunc) Run(ctx context.Context, j job.Job) (executor.Result, error) { return f(ctx, j) }

func newWorker(t *testing.T, r executor.Runner, cfg worker.Config) (*worker.Worker, *store.Store) {
	t.Helper()
	st := store.New(t.TempDir())
	return worker.New(st, r, cfg, io.Discard), st
}

func mustGet(t *testing.T, st *store.Store, id string) store.Record {
	t.Helper()
	rec, err := st.Get(id)
	if err != nil {
		t.Fatalf("Get(%s): %v", id, err)
	}
	return rec
}

/*
TestHandleRetriesUntilSuccess pins the retry loop: a failed attempt is not
final, and Attempts records how many executions actually started.
*/
func TestHandleRetriesUntilSuccess(t *testing.T) {
	calls := 0
	r := runnerFunc(func(context.Context, job.Job) (executor.Result, error) {
		calls++
		if calls == 1 {
			return executor.Result{ExitCode: 1, Stderr: "boom"}, errors.New("exit status 1")
		}
		return executor.Result{Stdout: "ok\n"}, nil
	})
	w, st := newWorker(t, r, worker.Config{MaxAttempts: 3, Backoff: time.Millisecond, Timeout: time.Second})

	w.Handle(context.Background(), store.Record{ID: "j1", Command: []string{"true"}, Status: job.StatusPending})

	rec := mustGet(t, st, "j1")
	if rec.Status != job.StatusSucceeded {
		t.Errorf("status = %v, want succeeded", rec.Status)
	}
	if rec.Attempts != 2 {
		t.Errorf("Attempts = %d, want 2", rec.Attempts)
	}
	if rec.Result.Stdout != "ok\n" {
		t.Errorf("stdout = %q, want the successful attempt's output", rec.Result.Stdout)
	}
}

/*
TestHandleExhaustsAttempts pins the bound: MaxAttempts is exactly how many
times the runner is called, and the last failure is what gets persisted.
*/
func TestHandleExhaustsAttempts(t *testing.T) {
	calls := 0
	r := runnerFunc(func(context.Context, job.Job) (executor.Result, error) {
		calls++
		return executor.Result{ExitCode: 1}, errors.New("nope")
	})
	w, st := newWorker(t, r, worker.Config{MaxAttempts: 3, Backoff: time.Millisecond, Timeout: time.Second})

	w.Handle(context.Background(), store.Record{ID: "j2", Command: []string{"false"}, Status: job.StatusPending})

	if calls != 3 {
		t.Errorf("runner called %d times, want exactly 3", calls)
	}
	rec := mustGet(t, st, "j2")
	if rec.Status != job.StatusFailed {
		t.Errorf("status = %v, want failed", rec.Status)
	}
	if rec.Attempts != 3 {
		t.Errorf("Attempts = %d, want 3", rec.Attempts)
	}
	if rec.Err != "nope" {
		t.Errorf("Err = %q, want the last failure", rec.Err)
	}
}

/*
TestHandleSuccessRunsOnce pins that a healthy job pays no retry cost.
*/
func TestHandleSuccessRunsOnce(t *testing.T) {
	calls := 0
	r := runnerFunc(func(context.Context, job.Job) (executor.Result, error) {
		calls++
		return executor.Result{Stdout: "hello\n"}, nil
	})
	w, st := newWorker(t, r, worker.Config{MaxAttempts: 3, Backoff: time.Millisecond, Timeout: time.Second})

	w.Handle(context.Background(), store.Record{ID: "j3", Command: []string{"echo"}, Status: job.StatusPending})

	if calls != 1 {
		t.Errorf("runner called %d times, want 1", calls)
	}
	if rec := mustGet(t, st, "j3"); rec.Status != job.StatusSucceeded || rec.Attempts != 1 {
		t.Errorf("record = %+v, want succeeded after 1 attempt", rec)
	}
}

/*
TestHandleMarksRunningMidFlight pins the live state a GET serves while the
runner is busy: running, not pending.
*/
func TestHandleMarksRunningMidFlight(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	r := runnerFunc(func(context.Context, job.Job) (executor.Result, error) {
		close(started)
		<-release
		return executor.Result{Stdout: "done\n"}, nil
	})
	w, st := newWorker(t, r, worker.Config{MaxAttempts: 1, Timeout: time.Second})
	rec := store.Record{ID: "j4", Command: []string{"slow"}, Status: job.StatusPending}
	if err := st.Save(rec); err != nil {
		t.Fatal(err)
	}

	done := make(chan struct{})
	go func() { w.Handle(context.Background(), rec); close(done) }()

	<-started
	if got := mustGet(t, st, "j4"); got.Status != job.StatusRunning {
		t.Errorf("mid-flight status = %v, want running", got.Status)
	}
	close(release)
	<-done
	if got := mustGet(t, st, "j4"); got.Status != job.StatusSucceeded {
		t.Errorf("final status = %v, want succeeded", got.Status)
	}
}

/*
TestHandleSaveFailureGoesToStderr pins that a worker with nowhere to answer
still does not swallow a storage failure, and never runs the job when the
running-save failed.
*/
func TestHandleSaveFailureGoesToStderr(t *testing.T) {
	blocker := filepath.Join(t.TempDir(), "not-a-dir")
	if err := os.WriteFile(blocker, []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	st := store.New(filepath.Join(blocker, "data"))
	called := false
	r := runnerFunc(func(context.Context, job.Job) (executor.Result, error) {
		called = true
		return executor.Result{}, nil
	})
	var stderr bytes.Buffer
	w := worker.New(st, r, worker.Config{MaxAttempts: 1, Timeout: time.Second}, &stderr)

	w.Handle(context.Background(), store.Record{ID: "j5", Command: []string{"true"}, Status: job.StatusPending})

	if !strings.Contains(stderr.String(), "j5") {
		t.Errorf("stderr = %q, want it to name the job", stderr.String())
	}
	if called {
		t.Error("runner ran even though marking the job running failed")
	}
}

/*
TestHandleConcurrentSaveFailuresRaceFree keeps V3's race guarantee after the
handler moved package: N goroutines sharing one Worker must not race on an
arbitrary io.Writer (bytes.Buffer is not goroutine-safe).
*/
func TestHandleConcurrentSaveFailuresRaceFree(t *testing.T) {
	blocker := filepath.Join(t.TempDir(), "not-a-dir")
	if err := os.WriteFile(blocker, []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	st := store.New(filepath.Join(blocker, "data"))
	r := runnerFunc(func(context.Context, job.Job) (executor.Result, error) { return executor.Result{}, nil })
	var stderr bytes.Buffer
	w := worker.New(st, r, worker.Config{MaxAttempts: 1, Timeout: time.Second}, &stderr)

	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		id := fmt.Sprintf("j-%d", i)
		go func() {
			defer wg.Done()
			w.Handle(context.Background(), store.Record{ID: id, Command: []string{"true"}, Status: job.StatusPending})
		}()
	}
	wg.Wait()

	for i := 0; i < 8; i++ {
		want := fmt.Sprintf("j-%d", i)
		if !strings.Contains(stderr.String(), want) {
			t.Errorf("stderr missing %q; got %q", want, stderr.String())
		}
	}
}

// waitDone carries one completion signal; helper for the cancel tests.
func waitDone(t *testing.T, done <-chan struct{}) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Handle did not return; cancellation did not interrupt it")
	}
}

/*
TestCancelWhileQueuedNeverRuns pins the tombstone: DELETE can arrive while
the record is still sitting in the channel buffer. Claim must see the
tombstone, skip execution, and persist cancelled.
*/
func TestCancelWhileQueuedNeverRuns(t *testing.T) {
	called := false
	r := runnerFunc(func(context.Context, job.Job) (executor.Result, error) {
		called = true
		return executor.Result{}, nil
	})
	w, st := newWorker(t, r, worker.Config{MaxAttempts: 3, Backoff: time.Hour, Timeout: time.Minute})

	w.Cancel("queued") // no running entry yet: plant a tombstone
	w.Handle(context.Background(), store.Record{ID: "queued", Command: []string{"true"}, Status: job.StatusPending})

	if called {
		t.Error("runner ran a job cancelled while queued")
	}
	rec := mustGet(t, st, "queued")
	if rec.Status != job.StatusCancelled {
		t.Errorf("status = %v, want cancelled", rec.Status)
	}
	if rec.Attempts != 0 {
		t.Errorf("Attempts = %d, want 0 — no attempt started", rec.Attempts)
	}
}

/*
TestCancelInterruptsBackoff pins Review Focus 2: with a one-hour backoff, a
cancel must still make Handle return immediately. The assertion is a channel
receive with a deadline, not a sleep.
*/
func TestCancelInterruptsBackoff(t *testing.T) {
	failed := make(chan struct{})
	r := runnerFunc(func(context.Context, job.Job) (executor.Result, error) {
		close(failed)
		return executor.Result{ExitCode: 1}, errors.New("boom")
	})
	w, st := newWorker(t, r, worker.Config{MaxAttempts: 5, Backoff: time.Hour, Timeout: time.Hour})

	done := make(chan struct{})
	go func() {
		w.Handle(context.Background(), store.Record{ID: "j-backoff", Command: []string{"false"}, Status: job.StatusPending})
		close(done)
	}()

	<-failed
	w.Cancel("j-backoff")
	waitDone(t, done)

	rec := mustGet(t, st, "j-backoff")
	if rec.Status != job.StatusCancelled {
		t.Errorf("status = %v, want cancelled", rec.Status)
	}
	if rec.Attempts != 1 {
		t.Errorf("Attempts = %d, want 1 — no retry after cancel", rec.Attempts)
	}
}

/*
TestCancelKillsRunningAttempt pins the live path: Cancel finds the registry
entry and stops the context the runner is blocked on.
*/
func TestCancelKillsRunningAttempt(t *testing.T) {
	entered := make(chan struct{})
	r := runnerFunc(func(ctx context.Context, _ job.Job) (executor.Result, error) {
		close(entered)
		<-ctx.Done()
		return executor.Result{}, ctx.Err()
	})
	w, st := newWorker(t, r, worker.Config{MaxAttempts: 3, Backoff: time.Hour, Timeout: time.Hour})

	done := make(chan struct{})
	go func() {
		w.Handle(context.Background(), store.Record{ID: "j-run", Command: []string{"sleep"}, Status: job.StatusPending})
		close(done)
	}()

	<-entered
	w.Cancel("j-run")
	waitDone(t, done)

	rec := mustGet(t, st, "j-run")
	if rec.Status != job.StatusCancelled {
		t.Errorf("status = %v, want cancelled", rec.Status)
	}
	if rec.Err != "job cancelled" {
		t.Errorf("Err = %q, want %q", rec.Err, "job cancelled")
	}
}

/*
TestHandleTimeoutCancelsWithoutRetry pins Review Focus 4: the deadline is a
cancellation, not a failure. The message names the timeout and Attempts
stays 1.
*/
func TestHandleTimeoutCancelsWithoutRetry(t *testing.T) {
	r := runnerFunc(func(ctx context.Context, _ job.Job) (executor.Result, error) {
		<-ctx.Done()
		return executor.Result{}, ctx.Err()
	})
	w, st := newWorker(t, r, worker.Config{MaxAttempts: 3, Backoff: time.Millisecond, Timeout: 10 * time.Millisecond})

	w.Handle(context.Background(), store.Record{ID: "j-timeout", Command: []string{"sleep"}, Status: job.StatusPending})

	rec := mustGet(t, st, "j-timeout")
	if rec.Status != job.StatusCancelled {
		t.Errorf("status = %v, want cancelled", rec.Status)
	}
	if rec.Attempts != 1 {
		t.Errorf("Attempts = %d, want 1", rec.Attempts)
	}
	if !strings.Contains(rec.Err, "timed out") {
		t.Errorf("Err = %q, want it to name the timeout", rec.Err)
	}
}

/*
TestShutdownDuringBackoffStopsRetrying pins Review Focus 3's other half:
server shutdown stops the retry loop but is not a cancellation — the last
failure is what gets persisted.
*/
func TestShutdownDuringBackoffStopsRetrying(t *testing.T) {
	failed := make(chan struct{})
	r := runnerFunc(func(context.Context, job.Job) (executor.Result, error) {
		close(failed)
		return executor.Result{ExitCode: 1}, errors.New("boom")
	})
	w, st := newWorker(t, r, worker.Config{MaxAttempts: 5, Backoff: time.Hour, Timeout: time.Hour})

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		w.Handle(ctx, store.Record{ID: "j-stop", Command: []string{"false"}, Status: job.StatusPending})
		close(done)
	}()

	<-failed
	cancel()
	waitDone(t, done)

	rec := mustGet(t, st, "j-stop")
	if rec.Status != job.StatusFailed {
		t.Errorf("status = %v, want failed", rec.Status)
	}
	if rec.Attempts != 1 {
		t.Errorf("Attempts = %d, want 1 — shutdown stops retries", rec.Attempts)
	}
}

/*
TestShutdownDoesNotKillRunningAttempt pins V3's contract after V4's
cancellation machinery: the lifecycle context stops new work, but the
execution context is detached from it, so an in-flight attempt finishes.
*/
func TestShutdownDoesNotKillRunningAttempt(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	r := runnerFunc(func(context.Context, job.Job) (executor.Result, error) {
		close(started)
		<-release
		return executor.Result{Stdout: "done\n"}, nil
	})
	w, st := newWorker(t, r, worker.Config{MaxAttempts: 3, Backoff: time.Millisecond, Timeout: time.Minute})

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		w.Handle(ctx, store.Record{ID: "j-finish", Command: []string{"sleep"}, Status: job.StatusPending})
		close(done)
	}()

	<-started
	cancel()
	select {
	case <-done:
		t.Fatal("Handle returned while the attempt was still running — shutdown killed it")
	case <-time.After(50 * time.Millisecond):
	}

	close(release)
	waitDone(t, done)
	if rec := mustGet(t, st, "j-finish"); rec.Status != job.StatusSucceeded {
		t.Errorf("status = %v, want succeeded", rec.Status)
	}
}

/*
TestCancelAfterSuccessLeavesRecord pins that a late DELETE never corrupts a
finished record: the API's store read decides 409, and the worker does not
write on Cancel.
*/
func TestCancelAfterSuccessLeavesRecord(t *testing.T) {
	r := runnerFunc(func(context.Context, job.Job) (executor.Result, error) {
		return executor.Result{Stdout: "ok\n"}, nil
	})
	w, st := newWorker(t, r, worker.Config{MaxAttempts: 1, Timeout: time.Second})

	w.Handle(context.Background(), store.Record{ID: "j-done", Command: []string{"true"}, Status: job.StatusPending})
	w.Cancel("j-done")

	if rec := mustGet(t, st, "j-done"); rec.Status != job.StatusSucceeded {
		t.Errorf("status = %v, want succeeded — Cancel must not write", rec.Status)
	}
}

/*
TestHandleResumesAttemptBudget pins crash recovery's cost: a record recovered
at Attempts=1 spends that attempt and continues at 2. Without resuming, a job
that kills the process would restart its budget on every restart — a crash
loop with no bound.
*/
func TestHandleResumesAttemptBudget(t *testing.T) {
	calls := 0
	r := runnerFunc(func(context.Context, job.Job) (executor.Result, error) {
		calls++
		return executor.Result{Stdout: "ok\n"}, nil
	})
	w, st := newWorker(t, r, worker.Config{MaxAttempts: 3, Backoff: time.Millisecond, Timeout: time.Second})
	if err := st.Save(store.Record{ID: "j-resume", Command: []string{"true"}, Status: job.StatusPending, Attempts: 1}); err != nil {
		t.Fatal(err)
	}

	w.Handle(context.Background(), store.Record{ID: "j-resume", Command: []string{"true"}, Status: job.StatusPending, Attempts: 1})

	if calls != 1 {
		t.Errorf("runner called %d times, want 1 — the recovered attempt already ran", calls)
	}
	rec := mustGet(t, st, "j-resume")
	if rec.Attempts != 2 {
		t.Errorf("Attempts = %d, want 2", rec.Attempts)
	}
	if rec.Status != job.StatusSucceeded {
		t.Errorf("status = %v, want succeeded", rec.Status)
	}
}

/*
TestHandleExhaustedBudgetRunsNothing pins the bound at the handler: even if a
record reaches a worker with its budget spent, it must not execute again.
*/
func TestHandleExhaustedBudgetRunsNothing(t *testing.T) {
	called := false
	r := runnerFunc(func(context.Context, job.Job) (executor.Result, error) {
		called = true
		return executor.Result{}, nil
	})
	w, st := newWorker(t, r, worker.Config{MaxAttempts: 2, Backoff: time.Millisecond, Timeout: time.Second})
	if err := st.Save(store.Record{ID: "j-spent", Command: []string{"true"}, Status: job.StatusPending, Attempts: 2}); err != nil {
		t.Fatal(err)
	}

	w.Handle(context.Background(), store.Record{ID: "j-spent", Command: []string{"true"}, Status: job.StatusPending, Attempts: 2})

	if called {
		t.Error("runner ran a record whose attempts were exhausted")
	}
	rec := mustGet(t, st, "j-spent")
	if rec.Status != job.StatusFailed {
		t.Errorf("status = %v, want failed", rec.Status)
	}
	if rec.Attempts != 2 {
		t.Errorf("Attempts = %d, want 2 — no attempt started", rec.Attempts)
	}
}
