# V4 — Retries + Cancellation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Give every job a bounded lifetime — retry failed attempts with exponential backoff, cancel it from the API or a timeout, and kill the child process through `context`.

**Architecture:** `executor.Runner` takes a context and runs through `exec.CommandContext`. A new `internal/worker` package replaces `cmd/forge.newJobHandler`: it owns retry policy, per-job timeouts, and an in-memory claim/cancel registry (`running` cancel funcs + `cancelled` tombstones). `internal/api` gains `DELETE /jobs/{id}` backed by a `Canceller` seam. `queue.Run` passes its lifecycle context to the handler; `cmd/forge` adds `-max-attempts`, `-retry-backoff`, `-job-timeout`.

**Tech Stack:** Go 1.22 standard library only (`context`, `time`, `sync`, `os/exec`). Zero new dependencies.

**Spec:** `docs/versions/V4.md`

## Global Constraints

- **TDD strictly:** one failing test at a time; run it, watch it fail, implement the minimum, watch it pass. Never write implementation before the test that demands it; never refactor while red.
- **Standard library only.** No new module dependencies.
- **No `time.Sleep` as synchronization.** Timer waits use a `select` with a deadline on a channel that must fire; polling uses deadline-bounded observation of stored state, as V1–V3 already do.
- **Comment convention:** block comments (`/* … */`, `/** … **/` inline) explaining the Go concept, per AGENTS.md rule 4. Do not rewrite comments in files this version does not otherwise touch.
- **Three lifetimes stay distinct:** server shutdown stops dequeuing and retrying but never kills a running attempt; `DELETE`/timeout kill the attempt; each attempt is one `exec.Run`.
- **Only the worker writes statuses.** `DELETE` never writes to the store; it cancels a live context or plants a tombstone. This is what keeps cancellation free of a store compare-and-swap race.
- **Before every commit:** `gofmt -l .` empty, `go vet ./...` clean, `go test ./... -race` green.
- **Working-tree pre-flight:** `docs/versions/V4.md` is committed before Task 1.
- House test style: external test packages (`package queue_test`, `package api_test`, `package worker_test`); `cmd/forge` tests stay in `package main`.

## Review Focus

Spec-implied failure modes most likely to bite a user:

1. **Cancel arrives while the job is still buffered in the channel** → the job must never run and must end `cancelled`. Owned by Task 5 (tombstone test) and Task 6 (202 without enqueue).
2. **Cancel arrives during backoff** → the worker must return immediately, not sleep out a long delay, and must not attempt again. Owned by Task 5.
3. **Ctrl-C while a job runs** → V3's contract must survive: the attempt finishes, `runServe` joins the pool, status is final. Owned by Task 5 (unit) and Task 7 (existing e2e still green).
4. **Timeout is a cancellation, not a failure** → no retry, status `cancelled`, error names the timeout. Owned by Tasks 5 and 7.
5. **Test assumes DELETE cancels synchronously** → 202 means "requested"; the record may still read `running` until the worker observes the cancelled context. Owned by Tasks 6 and 7 (poll, never assert immediately).

---

### Task 1: Executor accepts a context

**Files:**
- Modify: `internal/executor/executor.go`
- Test: `internal/executor/executor_test.go`
- Modify call sites: `cmd/forge/main.go` (one-shot `run`)

**Interfaces:**
- Consumes: `job.Job`, `job.Validate`
- Produces: `func (e Executor) Run(ctx context.Context, j job.Job) (Result, error)` and `Runner.Run(ctx context.Context, j job.Job) (Result, error)`

- [ ] **Step 1: Update existing tests and add the cancellation tests**

Change every `e.Run(job...)` / `r.Run(job...)` in `internal/executor/executor_test.go` to pass `context.Background()`, add `"context"`, `"fmt"`, `"os"`, `"path/filepath"`, `"strconv"`, `"time"` to imports, and append:

```go
/*
TestExecutorRunCancelledBeforeStart proves a dead context is checked before
the process is created: a cancelled job must never touch the OS.
*/
func TestExecutorRunCancelledBeforeStart(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	res, err := (executor.Executor{}).Run(ctx, job.Job{ID: "j-cancel", Command: []string{"echo", "hi"}})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Run() error = %v, want wrapped context.Canceled", err)
	}
	if res.Stdout != "" {
		t.Errorf("Stdout = %q, want empty — the process must not start", res.Stdout)
	}
}

/*
TestExecutorRunKillsRunningProcess pins the headline: exec.CommandContext
kills the child when ctx ends. The marker file proves the process actually
started before we cancel, so the test cannot pass by refusing to launch it.
`exec sleep 10` replaces the shell, so no grandchild survives the kill.
*/
func TestExecutorRunKillsRunningProcess(t *testing.T) {
	marker := filepath.Join(t.TempDir(), "started")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan error, 1)
	go func() {
		_, err := (executor.Executor{}).Run(ctx, job.Job{
			ID:      "j-kill",
			Command: []string{"sh", "-c", fmt.Sprintf("touch %s; exec sleep 10", strconv.Quote(marker))},
		})
		done <- err
	}()

	deadline := time.Now().Add(5 * time.Second)
	for {
		if _, err := os.Stat(marker); err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("command never started; cannot test the kill path")
		}
		time.Sleep(time.Millisecond) // poll interval only; the deadline is the guarantee
	}

	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Run() error = %v, want wrapped context.Canceled", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after cancel — the process was not killed")
	}
}

/*
TestExecutorRunDeadlineExceeded pins the timeout flavour of the same path:
WithTimeout's cause is DeadlineExceeded, and callers must be able to tell it
apart from an explicit cancel with errors.Is.
*/
func TestExecutorRunDeadlineExceeded(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	_, err := (executor.Executor{}).Run(ctx, job.Job{ID: "j-timeout", Command: []string{"sleep", "10"}})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Run() error = %v, want wrapped context.DeadlineExceeded", err)
	}
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test ./internal/executor/ -run 'TestExecutor' -v`
Expected: FAIL to build — `not enough arguments in call to e.Run` / `undefined: context`.

- [ ] **Step 3: Write minimal implementation**

In `internal/executor/executor.go`, add `"context"` to imports, change the interface and method:

```go
// Runner is the seam callers depend on. The context is the first parameter
// because every Run is cancellable: it carries the job's deadline and the
// DELETE signal into os/exec.
type Runner interface {
	Run(ctx context.Context, j job.Job) (Result, error)
}

func (e Executor) Run(ctx context.Context, j job.Job) (Result, error) {
	if err := j.Validate(); err != nil {
		return Result{}, err
	}
	/*
		Check the context BEFORE starting anything. exec.CommandContext would
		refuse too, but returning ctx.Err() ourselves makes the cause visible
		without a process launch — and a cancelled job must not touch the OS.
	*/
	if err := ctx.Err(); err != nil {
		return Result{}, fmt.Errorf("job %q: %w", j.ID, err)
	}

	var stdout, stderr bytes.Buffer

	// CommandContext ties the child's life to ctx: when ctx is done it kills
	// the process. That is the whole cancellation mechanism at this layer.
	cmd := exec.CommandContext(ctx, j.Command[0], j.Command[1:]...)
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	err := cmd.Run()
	res := Result{Stdout: stdout.String(), Stderr: stderr.String()}

	if cmd.ProcessState != nil {
		res.ExitCode = cmd.ProcessState.ExitCode()
	}

	if err != nil {
		/*
			A killed process reports "signal: killed", which says nothing about
			WHY. Prefer ctx.Err() so the error chain carries Canceled or
			DeadlineExceeded and callers can branch with errors.Is.
		*/
		if cerr := ctx.Err(); cerr != nil {
			err = cerr
		}
		return res, fmt.Errorf("job %q: %w", j.ID, err)
	}

	return res, nil
}
```

Update `cmd/forge/main.go`'s one-shot call to `e.Run(context.Background(), j)` (add `"context"` — already imported).

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./internal/executor/ -race -v && go build ./...`
Expected: PASS, build clean.

- [ ] **Step 5: Commit**

```bash
git add internal/executor/executor.go internal/executor/executor_test.go cmd/forge/main.go
git commit -m "feat(executor): run with a context and kill on cancel"
```

---

### Task 2: `cancelled` status and `Terminal`

**Files:**
- Modify: `internal/job/job.go`
- Test: `internal/job/job_test.go`

**Interfaces:**
- Produces: `job.StatusCancelled`; `func (s Status) Terminal() bool`

- [ ] **Step 1: Write the failing tests**

Add `{"cancelled", job.StatusCancelled, "cancelled"}` to both status tables in `TestStatusString` and `TestStatusJSONRoundTrip`. Append:

```go
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
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/job/ -run 'TestStatus' -v`
Expected: FAIL to build — `undefined: job.StatusCancelled`, `tt.status.Terminal undefined`.

- [ ] **Step 3: Write minimal implementation**

In `internal/job/job.go`:

```go
const (
	StatusPending Status = iota
	StatusRunning
	StatusSucceeded
	StatusFailed
	StatusCancelled // appended: existing iota values must never shift
)

// String: add
	case StatusCancelled:
		return "cancelled"

// UnmarshalJSON: add
	case "cancelled":
		*s = StatusCancelled

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
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./internal/job/ -race -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/job/job.go internal/job/job_test.go
git commit -m "feat(job): add cancelled status and Terminal"
```

---

### Task 3: Queue hands its context to the handler

**Files:**
- Modify: `internal/queue/queue.go`, `internal/queue/queue_test.go`, `internal/api/server_test.go`
- Test: `internal/queue/queue_test.go` (append)

**Interfaces:**
- Produces: `func (q *Queue) Run(ctx context.Context, workers int, handler func(context.Context, store.Record))`

- [ ] **Step 1: Add the failing test and update call sites**

In `internal/queue/queue_test.go`, change every handler literal from `func(rec store.Record)` / `func(store.Record)` to `func(_ context.Context, rec store.Record)`. In `internal/api/server_test.go` do the same for the two `q.Run` closures. Append:

```go
/*
TestRunPassesLifecycleContext pins context propagation: the handler receives
the SAME context Run was given, so cancellation and shutdown decisions reach
the worker without a global or a struct field.
*/
func TestRunPassesLifecycleContext(t *testing.T) {
	q := queue.New()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	got := make(chan context.Context, 1)
	go q.Run(ctx, 1, func(hctx context.Context, _ store.Record) { got <- hctx })
	mustEnqueue(t, ctx, q, store.Record{ID: "ctx"})

	select {
	case hctx := <-got:
		if hctx != ctx {
			t.Errorf("handler context = %p, want the Run context %p", hctx, ctx)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("handler never received the record")
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/queue/ -run TestRunPassesLifecycleContext -v`
Expected: FAIL to build — the closures no longer match `func(store.Record)`.

- [ ] **Step 3: Write minimal implementation**

In `internal/queue/queue.go`, change both signatures and the dispatch:

```go
func (q *Queue) Run(ctx context.Context, workers int, handler func(context.Context, store.Record)) {
	...
			handler(ctx, rec) // the worker sees the same lifecycle signal Run does
```

Update the doc comment: the handler receives the lifecycle context.

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./... -race`
Expected: PASS (API tests compile against the new handler type).

- [ ] **Step 5: Commit**

```bash
git add internal/queue/queue.go internal/queue/queue_test.go internal/api/server_test.go
git commit -m "feat(queue): pass the lifecycle context to handlers"
```

---

### Task 4: Worker package — retries, attempts, save reporting

**Files:**
- Create: `internal/worker/worker.go`
- Test: `internal/worker/worker_test.go`
- Modify: `internal/store/store.go` (`Record.Attempts`)
- Modify: `cmd/forge/main.go` and `cmd/forge/main_test.go` (handler + its tests move out)

**Interfaces:**
- Consumes: `store.Store`, `executor.Runner` (context-aware), `job.Status*`
- Produces: `worker.Config{MaxAttempts int; Backoff, Timeout time.Duration}`, `worker.New(st *store.Store, exec executor.Runner, cfg Config, stderr io.Writer) *Worker`, `(*Worker).Handle(ctx context.Context, rec store.Record)`

- [ ] **Step 1: Write the failing tests**

Create `internal/worker/worker_test.go` with the helper and the retry tests. Add `Attempts int` to `store.Record` only in Step 3, so Step 2 fails on the missing field too.

```go
package worker_test

import (
	"context"
	"errors"
	"io"
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
```

Append the migrated handler tests:

```go
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
```

Imports needed for these: `bytes`, `fmt`, `os`, `path/filepath`, `strings`, `sync`.

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test ./internal/worker/ -v`
Expected: FAIL to build — package does not exist / `undefined: worker.New`.

- [ ] **Step 3: Write minimal implementation**

Add `Attempts` to `store.Record`:

```go
	// Attempts counts executions started. A retried job ends with more than
	// one; a job cancelled before it ever started stays at zero.
	Attempts int `json:"attempts,omitempty"`
```

Create `internal/worker/worker.go`:

```go
/*
Package worker executes queued records: it marks a job running, runs it
through the executor seam, retries failed attempts with backoff, and records
the outcome. V4 adds the job's lifetime — retries, a deadline, and a cancel
signal — on top of V2/V3's worker loop.
*/
package worker

import (
	"context"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/Dushmilan/Forge/internal/executor"
	"github.com/Dushmilan/Forge/internal/job"
	"github.com/Dushmilan/Forge/internal/store"
)

// Config is the retry/timeout policy. All durations are per job; Backoff is
// the first delay and doubles after every failed attempt.
type Config struct {
	MaxAttempts int
	Backoff     time.Duration
	Timeout     time.Duration
}

// Worker is safe for concurrent use by N queue workers: the registry below is
// the only shared mutable state, and every access goes through its mutex.
type Worker struct {
	st     *store.Store
	exec   executor.Runner
	cfg    Config
	stderr io.Writer

	errMu sync.Mutex // serializes stderr writes from concurrent workers
}

// New wires a Worker. cfg is expected to be valid (runServe validates flags
// before constructing it); tests construct explicit configs.
func New(st *store.Store, exec executor.Runner, cfg Config, stderr io.Writer) *Worker {
	return &Worker{st: st, exec: exec, cfg: cfg, stderr: stderr}
}

/*
Handle runs one record through the retry loop. It is the queue's handler
shape: the lifecycle ctx comes from Queue.Run and is only consulted between
attempts, never handed to the executor — V3's shutdown contract says a
running job finishes.
*/
func (w *Worker) Handle(ctx context.Context, rec store.Record) {
	for attempt := 1; attempt <= w.cfg.MaxAttempts; attempt++ {
		rec.Attempts = attempt
		rec.Status = job.StatusRunning
		if !w.save(rec) {
			return
		}

		res, err := w.exec.Run(ctx, job.Job{ID: rec.ID, Command: rec.Command})
		rec.Result = res
		if err == nil {
			rec.Status = job.StatusSucceeded
			rec.Err = ""
			w.save(rec)
			return
		}

		rec.Err = err.Error()
		if attempt == w.cfg.MaxAttempts {
			rec.Status = job.StatusFailed
			w.save(rec)
			return
		}

		if !w.wait(ctx, w.cfg.Backoff<<(attempt-1)) {
			rec.Status = job.StatusFailed
			w.save(rec)
			return
		}
	}
}

/*
wait sleeps for d, but wakes early when the lifecycle context ends: shutdown
must not be delayed by a backoff timer. defer timer.Stop is the cleanup that
prevents a fired timer's resources from lingering when the ctx branch wins.
*/
func (w *Worker) wait(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}

/*
save persists rec and reports failure to stderr. The mutex is the same
reasoning as V3: N workers share this Writer and an arbitrary io.Writer is
not goroutine-safe.
*/
func (w *Worker) save(rec store.Record) bool {
	if err := w.st.Save(rec); err != nil {
		w.errMu.Lock()
		defer w.errMu.Unlock()
		fmt.Fprintf(w.stderr, "forge: save job %s: %v\n", rec.ID, err)
		return false
	}
	return true
}
```

Remove `newJobHandler` and `TestJobHandler*` + `fakeRunner` from `cmd/forge` (the tests now live in `internal/worker`). In `runServe`, temporarily pass `(&worker.Worker{})`? No — wire it now:

```go
w := worker.New(st, e, worker.Config{MaxAttempts: 3, Backoff: 100 * time.Millisecond, Timeout: 30 * time.Second}, stderr)
go func() {
	q.Run(ctx, *workers, w.Handle)
	close(workerDone)
}()
srv := &http.Server{Addr: *addr, Handler: api.New(ctx, q, st).Handler()}
```

(Task 7 replaces the literals with flags; keep this compile step honest.)

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./... -race`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/worker internal/store/store.go cmd/forge
git commit -m "feat(worker): retry jobs with backoff and record attempts"
```

---

### Task 5: Cancellation registry, timeout, and shutdown semantics

**Files:**
- Modify: `internal/worker/worker.go`
- Test: `internal/worker/worker_test.go`

**Interfaces:**
- Produces: `func (w *Worker) Cancel(id string)`; `Handle` claims/tombstones as described in the spec.

- [ ] **Step 1: Write the failing tests**

Append to `internal/worker/worker_test.go`:

```go
// runT carries one completion signal; helper for the cancel tests.
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
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test ./internal/worker/ -race -v`
Expected: FAIL to build — `w.Cancel undefined`; cancellation tests then fail once it compiles (retry continues, statuses wrong).

- [ ] **Step 3: Write minimal implementation**

Replace `Handle` and add the registry to `Worker`:

```go
type Worker struct {
	st     *store.Store
	exec   executor.Runner
	cfg    Config
	stderr io.Writer

	errMu sync.Mutex // serializes stderr writes

	// mu guards the two maps that make cancellation reach a specific record:
	// running holds a live attempt's CancelFunc; cancelled holds tombstones
	// for records still buffered in the queue channel.
	mu        sync.Mutex
	running   map[string]context.CancelFunc
	cancelled map[string]struct{}
}

func New(...) *Worker {
	return &Worker{
		st: st, exec: exec, cfg: cfg, stderr: stderr,
		running:   make(map[string]context.CancelFunc),
		cancelled: make(map[string]struct{}),
	}
}

// Handle: claim first; a tombstoned record never reaches the runner.
func (w *Worker) Handle(ctx context.Context, rec store.Record) {
	execCtx, release, ok := w.claim(ctx, rec.ID)
	if !ok {
		rec.Status = job.StatusCancelled
		rec.Err = "job cancelled before it started"
		w.save(rec)
		return
	}
	defer release() // unregister only after the final save: Cancel must see the entry until then

	for attempt := 1; attempt <= w.cfg.MaxAttempts; attempt++ {
		rec.Attempts = attempt
		rec.Status = job.StatusRunning
		if !w.save(rec) {
			return
		}

		res, err := w.exec.Run(execCtx, job.Job{ID: rec.ID, Command: rec.Command})
		rec.Result = res
		if err == nil {
			rec.Status = job.StatusSucceeded
			rec.Err = ""
			w.save(rec)
			return
		}

		/*
			A completed execution context is the authoritative reason the
			run ended early. Treat it as cancellation, never as a failed
			attempt to retry — and never retry a timed-out job.
		*/
		if cerr := execCtx.Err(); cerr != nil {
			w.finishCancelled(rec, cerr)
			return
		}

		rec.Err = err.Error()
		if attempt == w.cfg.MaxAttempts {
			rec.Status = job.StatusFailed
			w.save(rec)
			return
		}

		if !w.wait(ctx, execCtx, w.cfg.Backoff<<(attempt-1)) {
			if cerr := execCtx.Err(); cerr != nil {
				w.finishCancelled(rec, cerr)
			} else {
				rec.Status = job.StatusFailed
				w.save(rec)
			}
			return
		}
	}
}

/*
claim builds the per-job execution context and registers its cancel func
atomically with the tombstone check. context.WithoutCancel is the key: the
attempt keeps the server context's values but not its cancellation, so
Ctrl-C cannot kill a running job (V3), while WithTimeout and Cancel can.
*/
func (w *Worker) claim(parent context.Context, id string) (context.Context, func(), bool) {
	execCtx, cancel := context.WithTimeout(context.WithoutCancel(parent), w.cfg.Timeout)

	w.mu.Lock()
	if _, cancelled := w.cancelled[id]; cancelled {
		delete(w.cancelled, id)
		w.mu.Unlock()
		cancel()
		return nil, nil, false
	}
	w.running[id] = cancel
	w.mu.Unlock()

	release := func() {
		w.mu.Lock()
		delete(w.running, id)
		w.mu.Unlock()
		cancel()
	}
	return execCtx, release, true
}

/*
Cancel is called by the API. If the record already claimed the ID, it stops
the attempt; otherwise it plants a tombstone so the worker skips the job
when it is finally dequeued. It never writes the store — only the worker
goroutine owns status transitions, which is what keeps cancel/complete free
of a compare-and-swap race.
*/
func (w *Worker) Cancel(id string) {
	w.mu.Lock()
	cancel, running := w.running[id]
	if !running {
		w.cancelled[id] = struct{}{}
	}
	w.mu.Unlock()
	if running {
		cancel()
	}
}

// finishCancelled records the cancel/timeout cause in the record.
func (w *Worker) finishCancelled(rec store.Record, cause error) {
	rec.Status = job.StatusCancelled
	if errors.Is(cause, context.DeadlineExceeded) {
		rec.Err = fmt.Sprintf("job timed out after %s", w.cfg.Timeout)
	} else {
		rec.Err = "job cancelled"
	}
	w.save(rec)
}

// wait also watches execCtx so a DELETE/timeout interrupts the timer.
func (w *Worker) wait(ctx, execCtx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-execCtx.Done():
		return false
	case <-t.C:
		return true
	}
}
```

Add `"errors"` to imports.

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./internal/worker/ -race -v`
Expected: PASS, zero race warnings.

- [ ] **Step 5: Commit**

```bash
git add internal/worker
git commit -m "feat(worker): cancel running and queued jobs, enforce timeouts"
```

---

### Task 6: `DELETE /jobs/{id}`

**Files:**
- Modify: `internal/api/server.go`, `internal/api/server_test.go`
- Modify: `cmd/forge/main.go` (constructor call; flags in Task 7)

**Interfaces:**
- Consumes: `Canceller` implementation from Task 5.
- Produces: `api.Canceller` interface; `api.New(ctx, q, st, canceller)`; route `DELETE /jobs/{id}`.

- [ ] **Step 1: Write the failing tests and update the helper**

In `internal/api/server_test.go`, add to imports nothing new. Change `newServer`:

```go
type fakeCanceller struct {
	mu  sync.Mutex
	ids []string
}

func (f *fakeCanceller) Cancel(id string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.ids = append(f.ids, id)
}

func (f *fakeCanceller) cancelled() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.ids...)
}

func newServer(t *testing.T) (*api.Server, *store.Store, *queue.Queue, *fakeCanceller) {
	t.Helper()
	st := store.New(t.TempDir())
	q := queue.New()
	c := &fakeCanceller{}
	return api.New(context.Background(), q, st, c), st, q, c
}
```

Update every `srv, st, q :=` / `srv, st, _ :=` / `srv, _, _ :=` to the four-value form. Update `TestMethodAndPathFallbacks`'s "wrong method" case to `httptest.NewRequest(http.MethodPatch, "/jobs/x", nil)` — DELETE is now a real route. Update the two direct `api.New` calls to pass `&fakeCanceller{}`.

Append:

```go
/*
TestDeleteJobPending pins the happy path: DELETE on a non-terminal job asks
the canceller and answers 202 with the record as it stands. Cancellation is
asynchronous — the body may still say pending.
*/
func TestDeleteJobPending(t *testing.T) {
	srv, st, _, c := newServer(t)
	if err := st.Save(store.Record{ID: "job1", Command: []string{"sleep", "10"}, Status: job.StatusPending}); err != nil {
		t.Fatal(err)
	}

	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodDelete, "/jobs/job1", nil))

	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want 202 (body: %s)", rec.Code, rec.Body.String())
	}
	if got := c.cancelled(); len(got) != 1 || got[0] != "job1" {
		t.Errorf("canceller saw %v, want [job1]", got)
	}
	var body store.Record
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("body is not JSON: %v", err)
	}
	if body.ID != "job1" {
		t.Errorf("body id = %q, want job1", body.ID)
	}
}

/*
TestDeleteJobTerminal409 pins the state machine: a finished job cannot be
cancelled, and the canceller is never consulted.
*/
func TestDeleteJobTerminal409(t *testing.T) {
	for _, status := range []job.Status{job.StatusSucceeded, job.StatusFailed, job.StatusCancelled} {
		t.Run(status.String(), func(t *testing.T) {
			srv, st, _, c := newServer(t)
			if err := st.Save(store.Record{ID: "done", Command: []string{"true"}, Status: status}); err != nil {
				t.Fatal(err)
			}

			rec := httptest.NewRecorder()
			srv.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodDelete, "/jobs/done", nil))

			if rec.Code != http.StatusConflict {
				t.Fatalf("status = %d, want 409", rec.Code)
			}
			if got := c.cancelled(); len(got) != 0 {
				t.Errorf("canceller saw %v, want no call for a terminal job", got)
			}
		})
	}
}

/*
TestDeleteJobMissing404 pins the not-found contract.
*/
func TestDeleteJobMissing404(t *testing.T) {
	srv, _, _, _ := newServer(t)

	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodDelete, "/jobs/ghost", nil))

	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", rec.Code)
	}
}

/*
TestDeleteJobRunning202 pins that a running record is cancellable too.
*/
func TestDeleteJobRunning202(t *testing.T) {
	srv, st, _, c := newServer(t)
	if err := st.Save(store.Record{ID: "run", Command: []string{"sleep", "10"}, Status: job.StatusRunning}); err != nil {
		t.Fatal(err)
	}

	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodDelete, "/jobs/run", nil))

	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want 202", rec.Code)
	}
	if got := c.cancelled(); len(got) != 1 || got[0] != "run" {
		t.Errorf("canceller saw %v, want [run]", got)
	}
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test ./internal/api/ -run TestDelete -v`
Expected: FAIL to build — `too many arguments in call to api.New` / no DELETE route (405).

- [ ] **Step 3: Write minimal implementation**

In `internal/api/server.go`:

```go
/*
Canceller is the seam between HTTP and execution (the worker implements it).
Defined here, where it is consumed, and deliberately tiny: the API only asks
"stop this job", never how.
*/
type Canceller interface {
	Cancel(id string)
}

type Server struct {
	q   *queue.Queue
	st  *store.Store
	ctx context.Context
	c   Canceller
}

func New(ctx context.Context, q *queue.Queue, st *store.Store, c Canceller) *Server {
	return &Server{q: q, st: st, ctx: ctx, c: c}
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /jobs/{id}", s.getJob)
	mux.HandleFunc("POST /jobs", s.createJob)
	mux.HandleFunc("DELETE /jobs/{id}", s.cancelJob)
	return mux
}

/*
cancelJob asks the worker to stop a job. Cancellation is asynchronous: the
worker owns the status write, so this answers 202 and leaves the current
record in the body. A terminal job is a conflict — nothing left to cancel.
*/
func (s *Server) cancelJob(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	id := r.PathValue("id")
	rec, err := s.st.Get(id)
	switch {
	case errors.Is(err, store.ErrNotFound):
		writeErr(w, http.StatusNotFound, err)
		return
	case err != nil:
		writeErr(w, http.StatusInternalServerError, err)
		return
	}
	if rec.Status.Terminal() {
		writeErr(w, http.StatusConflict, fmt.Errorf("api: job %s is already %s", id, rec.Status))
		return
	}

	s.c.Cancel(id)

	w.WriteHeader(http.StatusAccepted)
	_ = json.NewEncoder(w).Encode(rec)
}
```

In `cmd/forge/main.go`, `api.New(ctx, q, st, w).Handler()` (the worker from Task 4 is in scope).

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./internal/api/ ./cmd/forge/ -race -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/api cmd/forge/main.go
git commit -m "feat(api): DELETE /jobs/{id} cancels pending and running jobs"
```

---

### Task 7: CLI flags, wiring, and end-to-end lifetime tests

**Files:**
- Modify: `cmd/forge/main.go` (`runServe`)
- Test: `cmd/forge/main_test.go`

**Interfaces:**
- Consumes: `worker.Config`, `worker.New`, `(*Worker).Handle`, `(*Worker).Cancel`
- Produces: `forge serve -max-attempts N -retry-backoff D -job-timeout D`

- [ ] **Step 1: Write the failing tests**

Append to `cmd/forge/main_test.go`:

```go
/*
TestRunServeInvalidLifetimeFlags pins the usage-error contract for the V4
knobs. Each value would make the worker nonsensical (zero attempts, an
immediate backoff loop, an already-expired deadline), so all exit 2.
*/
func TestRunServeInvalidLifetimeFlags(t *testing.T) {
	tests := []struct {
		flag, val, wantSub string
	}{
		{"-max-attempts", "0", "-max-attempts must be >= 1"},
		{"-max-attempts", "-2", "-max-attempts must be >= 1"},
		{"-max-attempts", "nope", "invalid value"},
		{"-retry-backoff", "0s", "-retry-backoff must be > 0"},
		{"-retry-backoff", "-1s", "-retry-backoff must be > 0"},
		{"-retry-backoff", "nope", "invalid value"},
		{"-job-timeout", "0s", "-job-timeout must be > 0"},
		{"-job-timeout", "-1s", "-job-timeout must be > 0"},
		{"-job-timeout", "nope", "invalid value"},
	}
	for _, tt := range tests {
		t.Run(tt.flag+"="+tt.val, func(t *testing.T) {
			var stdout, stderr bytes.Buffer
			code := runServe(context.Background(), []string{tt.flag, tt.val}, &stdout, &stderr)
			if code != 2 {
				t.Errorf("runServe(%s %s) = %d, want 2", tt.flag, tt.val, code)
			}
			if !strings.Contains(stderr.String(), tt.wantSub) {
				t.Errorf("stderr = %q, want it to contain %q", stderr.String(), tt.wantSub)
			}
		})
	}
}

// serveAddr starts runServe on an ephemeral port and returns its address plus
// the exit-code channel. Tests wait on the startup line — no sleeps.
func serveAddr(t *testing.T, ctx context.Context, args ...string) (string, <-chan int) {
	t.Helper()
	out := newLineWriter()
	code := make(chan int, 1)
	full := append([]string{"-addr", "127.0.0.1:0", "-data", t.TempDir()}, args...)
	go func() { code <- runServe(ctx, full, out, io.Discard) }()

	select {
	case line := <-out.ch:
		addr := strings.TrimPrefix(line, "listening on ")
		if addr == line {
			t.Fatalf("unexpected startup line %q", line)
		}
		return addr, code
	case <-time.After(10 * time.Second):
		t.Fatal("runServe never printed its listening address")
		return "", nil
	}
}

// waitStatus polls GET until want, returning the record. The deadline bounds
// the observation; the poll interval is not the guarantee.
func waitStatus(t *testing.T, addr, id string, want job.Status) store.Record {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		r, err := http.Get("http://" + addr + "/jobs/" + id)
		if err != nil {
			t.Fatalf("GET %s: %v", id, err)
		}
		var got store.Record
		decodeErr := json.NewDecoder(r.Body).Decode(&got)
		r.Body.Close()
		if decodeErr != nil {
			t.Fatalf("GET %s body: %v", id, decodeErr)
		}
		if got.Status == want {
			return got
		}
		if time.Now().After(deadline) {
			t.Fatalf("job %s never reached %v; last status %v (%s)", id, want, got.Status, got.Err)
		}
		time.Sleep(time.Millisecond)
	}
}

func postJob(t *testing.T, addr, command string) store.Record {
	t.Helper()
	resp, err := http.Post("http://"+addr+"/jobs", "application/json", strings.NewReader(command))
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	defer resp.Body.Close()
	var rec store.Record
	if err := json.NewDecoder(resp.Body).Decode(&rec); err != nil {
		t.Fatalf("decode POST: %v", err)
	}
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("POST status = %d, want 202", resp.StatusCode)
	}
	return rec
}

/*
TestRunServeRetriesFailedJob pins the V4 headline end to end: a failing
command runs exactly MaxAttempts times and is stored failed.
*/
func TestRunServeRetriesFailedJob(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	addr, code := serveAddr(t, ctx, "-workers", "1", "-max-attempts", "2", "-retry-backoff", "1ms", "-job-timeout", "5s")

	rec := postJob(t, addr, `{"command": ["sh", "-c", "exit 1"]}`)
	got := waitStatus(t, addr, rec.ID, job.StatusFailed)
	if got.Attempts != 2 {
		t.Errorf("Attempts = %d, want 2", got.Attempts)
	}

	cancel()
	select {
	case c := <-code:
		if c != 0 {
			t.Errorf("runServe() = %d, want 0", c)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("runServe did not shut down")
	}
}

/*
TestRunServeDeleteCancelsRunningJob pins the DELETE path with a real child
process: the sleep is killed and the record ends cancelled.
*/
func TestRunServeDeleteCancelsRunningJob(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	addr, code := serveAddr(t, ctx, "-workers", "1", "-job-timeout", "30s")

	rec := postJob(t, addr, `{"command": ["sleep", "10"]}`)
	waitStatus(t, addr, rec.ID, job.StatusRunning)

	req, err := http.NewRequest(http.MethodDelete, "http://"+addr+"/jobs/"+rec.ID, nil)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("DELETE: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("DELETE status = %d, want 202", resp.StatusCode)
	}

	got := waitStatus(t, addr, rec.ID, job.StatusCancelled)
	if got.Err != "job cancelled" {
		t.Errorf("Err = %q, want %q", got.Err, "job cancelled")
	}

	cancel()
	select {
	case <-code:
	case <-time.After(10 * time.Second):
		t.Fatal("runServe did not shut down after the job was cancelled")
	}
}

/*
TestRunServeJobTimeoutCancels pins the deadline path: no client action, the
job stops itself and is stored cancelled with a timeout message.
*/
func TestRunServeJobTimeoutCancels(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	addr, code := serveAddr(t, ctx, "-workers", "1", "-job-timeout", "100ms")

	rec := postJob(t, addr, `{"command": ["sleep", "10"]}`)
	got := waitStatus(t, addr, rec.ID, job.StatusCancelled)
	if !strings.Contains(got.Err, "timed out") {
		t.Errorf("Err = %q, want a timeout message", got.Err)
	}

	cancel()
	select {
	case <-code:
	case <-time.After(10 * time.Second):
		t.Fatal("runServe did not shut down")
	}
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test ./cmd/forge/ -run 'TestRunServeInvalidLifetimeFlags|TestRunServeRetries|TestRunServeDelete|TestRunServeJobTimeout' -v`
Expected: FAIL — `flag provided but not defined: -max-attempts`.

- [ ] **Step 3: Write minimal implementation**

In `runServe`, add the flags and validation, then build the worker from them:

```go
	maxAttempts := fs.Int("max-attempts", 3, "attempts per job")
	retryBackoff := fs.Duration("retry-backoff", 100*time.Millisecond, "first retry delay (doubles per attempt)")
	jobTimeout := fs.Duration("job-timeout", 30*time.Second, "per-job deadline")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if *workers < 1 {
		fmt.Fprintln(stderr, "forge: -workers must be >= 1")
		return 2
	}
	if *maxAttempts < 1 {
		fmt.Fprintln(stderr, "forge: -max-attempts must be >= 1")
		return 2
	}
	if *retryBackoff <= 0 {
		fmt.Fprintln(stderr, "forge: -retry-backoff must be > 0")
		return 2
	}
	if *jobTimeout <= 0 {
		fmt.Fprintln(stderr, "forge: -job-timeout must be > 0")
		return 2
	}

	var e executor.Executor
	st := store.New(*data)
	q := queue.New()
	w := worker.New(st, e, worker.Config{
		MaxAttempts: *maxAttempts,
		Backoff:     *retryBackoff,
		Timeout:     *jobTimeout,
	}, stderr)

	workerDone := make(chan struct{})
	go func() {
		q.Run(ctx, *workers, w.Handle)
		close(workerDone)
	}()
	srv := &http.Server{
		Addr:    *addr,
		Handler: api.New(ctx, q, st, w).Handler(),
	}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./... -race` and `go vet ./...`
Expected: PASS everywhere. `TestRunServeFinishesRunningJobOnShutdown` and `TestRunServeRunsJobsConcurrently` must still pass — the V3 contract survived.

- [ ] **Step 5: Commit**

```bash
git add cmd/forge
git commit -m "feat(forge): add retry, backoff, and timeout flags to serve"
```

---

### Task 8: Learning entry, README, tag v4

**Files:**
- Modify: `docs/LEARNING.md`
- Modify: `README.md`

- [ ] **Step 1: Append the V4 learning entry**

```markdown
## V4 — Retries + cancellation

- **Concept:**
  - `context` as three distinct lifetimes: the server's (shutdown), the
    job's (DELETE/timeout), and the attempt's. `context.WithoutCancel`
    detaches execution from shutdown — V3's "running jobs finish" survives —
    while `WithTimeout` and the registered `CancelFunc` still stop it.
  - `exec.CommandContext` is the whole cancellation mechanism at the process
    boundary: the child is killed when the context ends, and preferring
    `ctx.Err()` over "signal: killed" keeps `errors.Is` useful.
  - `time.NewTimer` inside a `select` with `ctx.Done()`: backoff is a wait
    that can be interrupted, never a `time.Sleep` that ignores shutdown.
  - A mutex-guarded claim registry (cancel funcs + tombstones) makes
    "cancel a queued job" deterministic without a store compare-and-swap:
    only the worker goroutine writes statuses.
  - `defer` as the cleanup ledger: `cancel()`, `timer.Stop()`, registry
    release — each one runs on every return path, including cancellation.
- **What I built:** context-aware `executor.Runner`; `worker` package with
  retries, exponential backoff, timeouts, and cancel-by-id; `StatusCancelled`;
  `DELETE /jobs/{id}`; `-max-attempts`, `-retry-backoff`, `-job-timeout`.
- **What surprised me:** _(you)_
- **Idiom worth remembering:** _(you)_
- **Next time:** _(you)_
```

- [ ] **Step 2: Update `README.md`**

1. Status line → `**Status:** V0–V4 are done. V5–V11 are planned.`
2. Server quick start: show `-max-attempts 3 -retry-backoff 100ms -job-timeout 30s`, and document `curl -X DELETE localhost:8080/jobs/<id>`.
3. Add `## How it works (V4)` after V3: the three lifetimes, the retry loop, and the registry/tombstone idea.
4. Ladder table: V4 row `⏳` → `✅`.
5. Project layout: add `internal/worker/`.

Use this section:

```markdown
## How it works (V4)

V4 gives each job a lifetime. A failed attempt is retried with exponential
backoff until `-max-attempts`; every job runs under `-job-timeout`; and
`DELETE /jobs/{id}` stops a job — running or still queued:

```
POST /jobs ──▶ queue ──▶ worker.Handle ──▶ executor.Run(ctx, job)
                              │                  │
                              │        exec.CommandContext kills on cancel
                              │
                 registry: running[id] = cancel func
                           cancelled[id] = tombstone (still in channel)
```

- [`internal/worker`](internal/worker) — claim/cancel registry, retry loop,
  `time.NewTimer` backoff that wakes on cancellation.
- [`internal/executor`](internal/executor) — takes a `context.Context` and
  wraps `ctx.Err()` so `errors.Is(err, context.Canceled)` works.
- [`internal/api`](internal/api) — `DELETE /jobs/{id}` answers 202 (409 if the
  job already ended); cancellation is asynchronous, so poll `GET`.
```

- [ ] **Step 3: Full verification**

```bash
gofmt -l .
go vet ./...
go test ./... && go test ./... -race
```

- [ ] **Step 4: Commit and tag**

```bash
git add docs/LEARNING.md README.md
git commit -m "docs(v4): learning entry, README describes retries and cancellation"
git tag v4
```

- [ ] **Step 5: Confirm the ladder**

Run: `git tag` shows `v0 v1 v2 v3 v4`; `go test ./... -race` green.
