# V2 — Queue + Worker Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Move job execution off the HTTP request path: `POST /jobs` answers 202 immediately with a pending record, and one worker goroutine drains a channel-backed queue, making job status observable live.

**Architecture:** One new package — `internal/queue` — owns a buffered channel (capacity 64) and a single-goroutine `select` worker loop. `internal/api` loses its executor dependency and enqueues instead of running; the worker's handler (mark running → execute → persist outcome) is a closure over the store and V0's `Runner` seam, wired in `cmd/forge` under the same context that shuts the server down. `internal/job`, `internal/store`, and `internal/executor` are untouched.

**Tech Stack:** Go 1.22 standard library only (`context`, channels, `select`, `httptest`). Zero new dependencies.

**Spec:** `docs/versions/V2.md`

## Global Constraints

- **TDD strictly:** one failing test at a time; run it, watch it fail, implement minimum, watch it pass. Never write implementation before the test that demands it; never refactor while red.
- **Standard library only.** No new module dependencies.
- **No `time.Sleep` as synchronization in tests.** Synchronize with channels, `context`, or `httptest`. A `select` with `time.After` is allowed as a *deadline* on a channel that must fire, and as a *negative* check ("this must NOT happen within the window"). The single exception is the deadline-bounded status poll in Task 6's e2e test, which observes state instead of guessing timing.
- **Comment convention:** new and modified code uses block comments (`/* … */`, `/** … **/` for short inline notes) per the amended AGENTS.md rule 4. Doc comments start with the declaration name and teach *why* in Go terms. Do not rewrite comments in files this version doesn't otherwise touch.
- **Packages extended, not rewritten:** `internal/job`, `internal/store`, `internal/executor` are untouched by V2. `internal/api` loses its executor field; `cmd/forge` gains the handler and wiring.
- **One context, one shutdown story:** the `signal.NotifyContext` context from `runServe` is passed to both `http.Server` (via Shutdown) and `queue.Run`. The worker finishes the job it holds, drops the rest — no drain loop (V4+ territory).
- **The API never runs jobs and never sees the handler.** `internal/api` imports `queue` and `store` only — no `os/exec`, no `executor`.
- **Before every commit:** `gofmt -l .` empty, `go vet ./...` clean, `go test ./... -race` green.
- **Working-tree pre-flight:** tree is clean at plan time; `docs/versions/V2.md` is already committed.
- House test style: external test packages (`package queue_test`, `package api_test`); `cmd/forge` tests stay in `package main`.

## Review Focus

Spec-implied inputs no single deliverable line calls out, most likely to bite first:

1. **POST racing server shutdown** → the request's context is dead, the worker may be gone; must answer **503** with a JSON error body and enqueue nothing, never block forever. Owned by Task 5.
2. **Ctrl-C while a job is mid-flight** → the running job completes and its final status persists; queued records stay `pending`. Owned by Task 3 (queue-level pin) and Task 6 (e2e).
3. **GET while the worker is executing** → the record must read `"running"`, not jump pending → final. This is the whole point of "live status". Owned by Task 4.
4. **FIFO order** → records run in submission order; the buffer is a queue, not a set. Owned by Task 1.
5. **Store failure at POST** → **500** and nothing enqueued (a 500'd submission must never reach the worker). Owned by Task 5.

---

### Task 1: Queue delivery

The queue is V2's core: a buffered channel drained by one goroutine in a `select` loop. This task makes an enqueued record reach the handler exactly once, in FIFO order.

**Files:**
- Create: `internal/queue/queue.go`
- Test: `internal/queue/queue_test.go`

**Interfaces:**
- Consumes: `store.Record` (plain struct from `internal/store`, used as the channel payload — the queue moves whole records, so the worker never needs a second lookup).
- Produces: `const queue.Capacity = 64`, `func queue.New() *Queue`, `func (q *Queue) Enqueue(rec store.Record)` (Task 2 changes this signature to take a context), `func (q *Queue) Run(ctx context.Context, handler func(store.Record))`.

- [ ] **Step 1: Write the failing test**

Create `internal/queue/queue_test.go`:

```go
package queue_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/Dushmilan/Forge/internal/queue"
	"github.com/Dushmilan/Forge/internal/store"
)

// TestRunDeliversInOrder pins the queue's contract (Review Focus 4): every
// enqueued record reaches the handler exactly once, in FIFO order. The
// deadline is a safety net on a channel that MUST fire — the test fails if
// delivery doesn't happen, and cannot flake because it never guesses how
// long the goroutine needs.
func TestRunDeliversInOrder(t *testing.T) {
	q := queue.New()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	got := make(chan string, 3)
	runDone := make(chan struct{})
	go func() {
		q.Run(ctx, func(rec store.Record) { got <- rec.ID })
		close(runDone)
	}()

	for _, id := range []string{"a", "b", "c"} {
		q.Enqueue(store.Record{ID: id})
	}

	var ids []string
	for i := 0; i < 3; i++ {
		select {
		case id := <-got:
			ids = append(ids, id)
		case <-time.After(2 * time.Second):
			t.Fatalf("deadline waiting for handler; got %v so far", ids)
		}
	}
	if strings.Join(ids, ",") != "a,b,c" {
		t.Errorf("delivery order = %v, want [a b c]", ids)
	}

	// Exactly once: nothing else may arrive after the three we enqueued.
	select {
	case id := <-got:
		t.Fatalf("handler received a duplicate/extra record: %q", id)
	default:
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/queue/ -run TestRunDeliversInOrder -v`
Expected: FAIL to build — no `internal/queue` package yet ("no required module provides package github.com/Dushmilan/Forge/internal/queue").

- [ ] **Step 3: Write minimal implementation**

Create `internal/queue/queue.go`:

```go
/*
Package queue moves job execution off the request path: Enqueue buffers a
record on a channel, Run drains it on one worker goroutine.

The whole package is V2's idea in a few lines — the queue IS the channel,
the worker IS a goroutine in a select loop. Rebuilding a queue from a
slice + mutex + sync.Cond is the classic pre-Go mistake; a buffered chan
is already a thread-safe FIFO.
*/
package queue

import (
	"context"

	"github.com/Dushmilan/Forge/internal/store"
)

// Capacity is the queue's fixed buffer size. 64 is a decision, not a knob:
// making it configurable is V7's scheduling story, and YAGNI says wait for
// the need.
const Capacity = 64

// Queue is an in-memory FIFO of records waiting to run. The channel is the
// entire state — no slice, no mutex. The zero value is not usable; call New.
type Queue struct {
	jobs chan store.Record
}

// New returns an empty Queue with Capacity slots of buffer.
func New() *Queue {
	return &Queue{jobs: make(chan store.Record, Capacity)}
}

// Enqueue blocks until the queue has room: a full buffer makes the caller
// wait. That is backpressure — overload flows to whoever is submitting the
// job, and memory stays bounded by Capacity.
func (q *Queue) Enqueue(rec store.Record) {
	q.jobs <- rec
}

// Run drains the queue on the goroutine it is started on — callers must
// write `go q.Run(...)`. Each record is handed to handler, then the loop
// re-selects, so between jobs it can also notice ctx cancellation and
// return. Records still buffered when that happens are dropped; the API
// already saved them as pending, and they stay that way.
func (q *Queue) Run(ctx context.Context, handler func(store.Record)) {
	for {
		select {
		case <-ctx.Done():
			return
		case rec := <-q.jobs:
			handler(rec)
		}
	}
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./internal/queue/ -run TestRunDeliversInOrder -v`
Expected: PASS

- [ ] **Step 5: Full gate + commit**

Run: `gofmt -l .` (empty), `go vet ./...` (clean), `go test ./... -race` (green).

```bash
git add internal/queue/queue.go internal/queue/queue_test.go
git commit -m "feat(queue): buffered channel queue drained by one worker goroutine"
```

---

### Task 2: Enqueue respects cancellation (no shutdown deadlock)

Once the worker can exit, a send against a dead receiver with a full buffer blocks forever — and `http.Server.Shutdown` waits on that blocked handler. `Enqueue` must select on `ctx.Done()`. This task pins both the post-cancel error and the backpressure contract.

**Files:**
- Modify: `internal/queue/queue.go` (`Enqueue` signature change)
- Test: `internal/queue/queue_test.go` (append)

**Interfaces:**
- Consumes: `queue.New`, `queue.Capacity`, `Queue.Run` from Task 1.
- Produces: `func (q *Queue) Enqueue(ctx context.Context, rec store.Record) error` — returns `nil` on success, a wrapped `context.Canceled`/`DeadlineExceeded` when the send is abandoned. The API's 503 path (Task 5) tests this with `errors.Is`.

- [ ] **Step 1: Write the failing test**

Append to `internal/queue/queue_test.go`:

```go
// TestEnqueueAfterCancelReturnsError pins the no-deadlock guarantee: once
// ctx is cancelled the worker may be gone, so a send could block forever —
// Enqueue must notice the cancellation instead. The buffer is filled to
// capacity first, because a send into free buffer space succeeds even with
// no worker alive.
func TestEnqueueAfterCancelReturnsError(t *testing.T) {
	q := queue.New()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	for i := 0; i < queue.Capacity; i++ {
		q.Enqueue(store.Record{ID: strconv.Itoa(i)})
	}
	cancel()

	errDone := make(chan error, 1)
	go func() { errDone <- q.Enqueue(ctx, store.Record{ID: "late"}) }()
	select {
	case err := <-errDone:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Enqueue after cancel = %v, want wrapped context.Canceled", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Enqueue blocked after ctx cancel — shutdown deadlock")
	}
}

// TestEnqueueBackpressure pins the blocking-send contract: a full queue
// makes Enqueue wait, and freeing a slot releases it. The handler is gated
// so the worker holds one record; the buffer then fills to Capacity, and
// the overflow Enqueue must sit blocked until the worker consumes a slot.
func TestEnqueueBackpressure(t *testing.T) {
	q := queue.New()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	gate := make(chan struct{})
	defer close(gate) // frees any handler still waiting, on the way out
	go q.Run(ctx, func(rec store.Record) { <-gate })

	mustEnqueue(t, ctx, q, store.Record{ID: "held"}) // the worker takes this one and blocks on gate
	for i := 0; i < queue.Capacity; i++ {
		mustEnqueue(t, ctx, q, store.Record{ID: strconv.Itoa(i)}) // fills the buffer
	}

	blocked := make(chan error, 1)
	go func() { blocked <- q.Enqueue(ctx, store.Record{ID: "overflow"}) }()
	select {
	case err := <-blocked:
		t.Fatalf("Enqueue returned %v with a full queue — no backpressure", err)
	case <-time.After(50 * time.Millisecond):
		// good: the overflow send is still waiting
	}

	gate <- struct{}{} // free "held"; the worker loops, takes a buffered record, and opens a slot

	select {
	case err := <-blocked:
		if err != nil {
			t.Fatalf("unblocked Enqueue = %v, want nil", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Enqueue never completed after a slot was freed")
	}
}

// mustEnqueue enqueues or fails the test — test plumbing so the happy
// paths above stay readable.
func mustEnqueue(t *testing.T, ctx context.Context, q *queue.Queue, rec store.Record) {
	t.Helper()
	if err := q.Enqueue(ctx, rec); err != nil {
		t.Fatalf("Enqueue(%s): %v", rec.ID, err)
	}
}
```

Add the `errors` and `strconv` imports to the file's import block.

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/queue/ -run 'TestEnqueueAfterCancelReturnsError|TestEnqueueBackpressure' -v`
Expected: FAIL to build — `q.Enqueue(ctx, …) too many arguments` (current `Enqueue` takes one argument).

- [ ] **Step 3: Write minimal implementation**

Replace `Enqueue` in `internal/queue/queue.go` (and update Task 1's call site in `TestRunDeliversInOrder` to `q.Enqueue(ctx, store.Record{ID: id})`, checking the error with `t.Fatalf`):

```go
/*
Enqueue hands rec to the worker via select over two cases: the send, and
ctx.Done(). While the worker lives, a full buffer makes the caller wait —
backpressure. After cancellation the send case may never succeed (the
receiver is gone), so the ctx case is what keeps a late POST from
blocking http.Server.Shutdown forever. ctx.Err is wrapped so callers can
still ask errors.Is(err, context.Canceled).
*/
func (q *Queue) Enqueue(ctx context.Context, rec store.Record) error {
	select {
	case q.jobs <- rec:
		return nil
	case <-ctx.Done():
		return fmt.Errorf("queue: enqueue: %w", ctx.Err())
	}
}
```

Add `fmt` to the file's imports.

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./internal/queue/ -v`
Expected: PASS — all three queue tests (delivery, cancel, backpressure).

- [ ] **Step 5: Full gate + commit**

Run: `gofmt -l .` (empty), `go vet ./...` (clean), `go test ./... -race` (green).

```bash
git add internal/queue/queue.go internal/queue/queue_test.go
git commit -m "feat(queue): Enqueue selects on ctx.Done to avoid shutdown deadlock"
```

---

### Task 3: Shutdown — finish current, drop queued

The spec's shutdown contract: on cancel a running job completes, buffered records are never handed to the handler, and `Run` returns only after the in-flight handler has returned.

**Files:**
- Modify: none (Task 1's `Run` already has the right shape — these tests pin it)
- Test: `internal/queue/queue_test.go` (append)

**Interfaces:**
- Consumes: `queue.New`, `Queue.Enqueue(ctx, rec) error`, `Queue.Run` — all as of Task 2.

- [ ] **Step 1: Write the failing test**

Append to `internal/queue/queue_test.go`:

```go
// TestRunReturnsWhenIdle pins: an idle worker exits promptly on cancel —
// there is nothing to finish.
func TestRunReturnsWhenIdle(t *testing.T) {
	q := queue.New()
	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan struct{})
	go func() {
		q.Run(ctx, func(store.Record) {})
		close(done)
	}()

	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("idle Run did not return after ctx cancel")
	}
}

// TestRunFinishesCurrentAndDropsQueued pins the shutdown contract (Review
// Focus 2): after cancel, the record being handled runs to completion, the
// record still buffered is never delivered, and Run returns only after the
// in-flight handler has returned.
func TestRunFinishesCurrentAndDropsQueued(t *testing.T) {
	q := queue.New()
	ctx, cancel := context.WithCancel(context.Background())

	entered := make(chan struct{}, 2) // handler-entry signal, one slot per job
	gate := make(chan struct{})       // blocks the handler until the test frees it
	handled := make(chan string, 2)
	runDone := make(chan struct{})
	go func() {
		q.Run(ctx, func(rec store.Record) {
			entered <- struct{}{}
			<-gate
			handled <- rec.ID
		})
		close(runDone)
	}()

	mustEnqueue(t, ctx, q, store.Record{ID: "in-flight"})
	select { // wait until the worker has actually picked "in-flight" up
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("worker never picked up the first record")
	}
	mustEnqueue(t, ctx, q, store.Record{ID: "queued"}) // buffer; the worker is busy

	cancel()
	select { // finish-current: Run must NOT return while the handler runs
	case <-runDone:
		t.Fatal("Run returned while a handler was still running")
	case <-time.After(50 * time.Millisecond):
	}

	gate <- struct{}{} // free the in-flight handler

	select {
	case <-runDone:
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return after the in-flight handler finished")
	}
	if id := <-handled; id != "in-flight" {
		t.Errorf("handler saw %q, want in-flight", id)
	}
	select { // drop-queued: "queued" must never have been delivered
	case id := <-handled:
		t.Fatalf("buffered record was delivered after cancel: %q", id)
	default:
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/queue/ -run 'TestRunReturnsWhenIdle|TestRunFinishesCurrentAndDropsQueued' -v`
Expected: PASS if Task 1's implementation is intact — this is a *pin* task: the loop's `select` already finishes the current handler before re-selecting. Record the pass honestly; if it fails, Task 1's loop was changed and must be fixed before continuing.

- [ ] **Step 3: No implementation needed**

Task 1's loop is exactly this contract: `handler(rec)` runs to completion, then the `for` re-selects, sees `ctx.Done()`, and returns. Verify by reading `internal/queue/queue.go`: if any version of the loop returns without completing the current `handler(rec)` call, stop and fix it.

- [ ] **Step 4: Full gate + commit**

Run: `gofmt -l .` (empty), `go vet ./...` (clean), `go test ./... -race` (green).

```bash
git add internal/queue/queue_test.go
git commit -m "test(queue): pin shutdown contract — finish current, drop queued"
```

---

### Task 4: Worker handler in cmd/forge

The handler is the worker's per-record function: mark running, execute through V0's `Runner` seam, persist the outcome — V1's honesty rules moved off the request path. It lives in `package main` (wiring-level, per the spec), testable with a fake runner.

**Files:**
- Modify: `cmd/forge/main.go` (add `newJobHandler` near `runServe`)
- Test: `cmd/forge/main_test.go` (append; `package main`)

**Interfaces:**
- Consumes: `executor.Runner` (V0 seam: `Run(job.Job) (executor.Result, error)`), `store.Store.Save/Get`, `store.Record`, `job.Job`, `job.StatusPending/Running/Succeeded/Failed`.
- Produces: `func newJobHandler(st *store.Store, exec executor.Runner) func(store.Record)` — the closure Task 5 passes to `q.Run`.

- [ ] **Step 1: Write the failing test**

Append to `cmd/forge/main_test.go` (add imports `errors`, `github.com/Dushmilan/Forge/internal/executor`, `github.com/Dushmilan/Forge/internal/job`, `github.com/Dushmilan/Forge/internal/store`; `time` is already imported):

```go
// fakeRunner is V0's Runner seam doing what fakes do best: it signals when
// a run starts and blocks until the test releases it, turning "did the
// handler mark the job running?" into a channel question — no sleeps.
type fakeRunner struct {
	started chan struct{}
	release chan struct{}
	res     executor.Result
	err     error
}

func (f *fakeRunner) Run(j job.Job) (executor.Result, error) {
	f.started <- struct{}{}
	<-f.release
	return f.res, f.err
}

// TestJobHandlerRunsAndPersists pins the success path: pending in, running
// saved on the way, succeeded with the executor's result out.
func TestJobHandlerRunsAndPersists(t *testing.T) {
	st := store.New(t.TempDir())
	fake := &fakeRunner{
		started: make(chan struct{}, 1),
		release: make(chan struct{}, 1),
		res:     executor.Result{ExitCode: 0, Stdout: "hello\n"},
	}
	fake.release <- struct{}{} // the single run proceeds immediately

	pending := store.Record{ID: "j1", Command: []string{"echo", "hello"}, Status: job.StatusPending}
	if err := st.Save(pending); err != nil {
		t.Fatal(err)
	}

	newJobHandler(st, fake)(pending)

	stored, err := st.Get("j1")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if stored.Status != job.StatusSucceeded {
		t.Errorf("status = %v, want succeeded", stored.Status)
	}
	if stored.Result.Stdout != "hello\n" {
		t.Errorf("stdout = %q, want %q", stored.Result.Stdout, "hello\n")
	}
}

// TestJobHandlerMarksRunningMidFlight pins Review Focus 3: while the
// executor is busy, the stored record reads "running" — the live status a
// GET would serve — not "pending" and not the final result.
func TestJobHandlerMarksRunningMidFlight(t *testing.T) {
	st := store.New(t.TempDir())
	fake := &fakeRunner{
		started: make(chan struct{}, 1),
		release: make(chan struct{}, 1),
	}
	pending := store.Record{ID: "j1", Command: []string{"slow"}, Status: job.StatusPending}
	if err := st.Save(pending); err != nil {
		t.Fatal(err)
	}

	handlerDone := make(chan struct{})
	go func() {
		newJobHandler(st, fake)(pending)
		close(handlerDone)
	}()

	select { // the executor has the job; the record must say running NOW
	case <-fake.started:
	case <-time.After(2 * time.Second):
		t.Fatal("executor never started")
	}
	stored, err := st.Get("j1")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if stored.Status != job.StatusRunning {
		t.Errorf("mid-flight status = %v, want running", stored.Status)
	}

	fake.release <- struct{}{}
	select {
	case <-handlerDone:
	case <-time.After(2 * time.Second):
		t.Fatal("handler never finished after release")
	}
	stored, err = st.Get("j1")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if stored.Status != job.StatusSucceeded {
		t.Errorf("final status = %v, want succeeded", stored.Status)
	}
}

// TestJobHandlerRunFailureIsData pins V1's honesty rule on the worker path:
// a failed run is an OUTCOME — status failed, message kept in the record's
// error field — not an exception.
func TestJobHandlerRunFailureIsData(t *testing.T) {
	st := store.New(t.TempDir())
	fake := &fakeRunner{
		started: make(chan struct{}, 1),
		release: make(chan struct{}, 1),
		res:     executor.Result{ExitCode: 3},
		err:     errors.New("exit status 3"),
	}
	fake.release <- struct{}{}

	pending := store.Record{ID: "j1", Command: []string{"sh", "-c", "exit 3"}, Status: job.StatusPending}
	if err := st.Save(pending); err != nil {
		t.Fatal(err)
	}

	newJobHandler(st, fake)(pending)

	stored, err := st.Get("j1")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if stored.Status != job.StatusFailed {
		t.Errorf("status = %v, want failed", stored.Status)
	}
	if stored.Result.ExitCode != 3 {
		t.Errorf("exit code = %d, want 3", stored.Result.ExitCode)
	}
	if stored.Err != "exit status 3" {
		t.Errorf("error field = %q, want the failure message as data", stored.Err)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./cmd/forge/ -run TestJobHandler -v`
Expected: FAIL to build — `undefined: newJobHandler`.

- [ ] **Step 3: Write minimal implementation**

Add to `cmd/forge/main.go` (before `runServe`):

```go
/*
newJobHandler builds the worker's per-record function: mark running,
execute through V0's Runner seam, persist the outcome. It is V1's honesty
rules moved off the request path — a failed run is data (status failed +
error message), never an exception.

A Save failure inside the worker has no client to answer and no logger
yet (V9); the record simply stays at its last saved status, which is the
most honest thing pre-observability Forge can say.
*/
func newJobHandler(st *store.Store, exec executor.Runner) func(store.Record) {
	return func(rec store.Record) {
		rec.Status = job.StatusRunning
		if err := st.Save(rec); err != nil {
			return
		}

		res, err := exec.Run(job.Job{ID: rec.ID, Command: rec.Command})
		rec.Result = res
		if err != nil {
			rec.Status = job.StatusFailed
			rec.Err = err.Error()
		} else {
			rec.Status = job.StatusSucceeded
		}
		_ = st.Save(rec)
	}
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./cmd/forge/ -run TestJobHandler -v`
Expected: PASS — all three handler tests.

- [ ] **Step 5: Full gate + commit**

Run: `gofmt -l .` (empty), `go vet ./...` (clean), `go test ./... -race` (green).

```bash
git add cmd/forge/main.go cmd/forge/main_test.go
git commit -m "feat(forge): worker handler marks running, executes, persists outcome"
```

---

### Task 5: API goes async (202 + pending + 503)

`Server` drops its executor and holds the queue; `POST` validates, saves a pending record, enqueues, answers 202 — and answers 503 when the hand-off is abandoned. `cmd/forge` wires the queue and the Task 4 handler into the server. This is the version's headline.

**Files:**
- Modify: `internal/api/server.go` (package doc, `Server`, `New`, `createJob` tail; remove `executor` import, add `queue`)
- Modify: `cmd/forge/main.go` (`runServe` wiring)
- Test: `internal/api/server_test.go` (rewrite the POST tests + helper)

**Interfaces:**
- Consumes: `queue.New() *queue.Queue`, `Queue.Enqueue(ctx, rec) error`, `Queue.Run` (Tasks 1–2); `newJobHandler` (Task 4); `store.Record{Status: job.StatusPending}`.
- Produces: `func api.New(q *queue.Queue, st *store.Store) *Server` — the signature `runServe` and every api test use from here on.

- [ ] **Step 1: Write the failing tests**

In `internal/api/server_test.go`:

1. Update `newServer` (add the `context` and `queue` imports; remove the `executor` import — after this task no test in the package names a `Runner`):

```go
// newServer builds a Server over a temp-dir store and a fresh queue. No
// executor: as of V2 the API hands jobs off, it never runs them.
func newServer(t *testing.T) (*api.Server, *store.Store, *queue.Queue) {
	t.Helper()
	st := store.New(t.TempDir())
	q := queue.New()
	return api.New(q, st), st, q
}
```

2. Replace `TestPostJobRunsAndPersists` with:

```go
// TestPostJobEnqueuesAndPersists pins the V2 headline: POST answers 202
// with the PENDING record, that record is persisted BEFORE the response,
// and it reaches a worker — the request itself never waits for the run.
func TestPostJobEnqueuesAndPersists(t *testing.T) {
	srv, st, q := newServer(t)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/jobs",
		strings.NewReader(`{"command": ["echo", "hello"]}`))
	srv.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want 202 (body: %s)", rec.Code, rec.Body.String())
	}
	var body store.Record
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("body is not JSON: %v", err)
	}
	if body.ID == "" {
		t.Fatal("response has no id — the server must generate one")
	}
	if body.Status != job.StatusPending {
		t.Errorf("status = %v, want pending", body.Status)
	}

	stored, err := st.Get(body.ID)
	if err != nil {
		t.Fatalf("persisted record not found: %v", err)
	}
	if stored.Status != job.StatusPending {
		t.Errorf("stored status = %v, want pending", stored.Status)
	}

	// The record reached the queue: a worker started now receives it.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	seen := make(chan store.Record, 1)
	go q.Run(ctx, func(r store.Record) { seen <- r })
	select {
	case got := <-seen:
		if got.ID != body.ID {
			t.Errorf("worker saw id %q, want %q", got.ID, body.ID)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("enqueued record never reached a worker")
	}
}
```

3. Replace `TestPostJobFailedCommand` with `TestPostJobRejections`-style coverage of the 400 path only (failed *runs* are now the worker's story, pinned in Task 4) — delete `TestPostJobFailedCommand` entirely; `TestPostJobRejections` already pins every 400.

4. Replace `TestPostJobStoreFailure500` with (new signature; keep the blocker-file trick):

```go
// TestPostJobStoreFailure500 pins Review Focus 5: a store that cannot
// save gets 500, and the submission never reaches the queue — a 500'd
// POST must not secretly run the job anyway.
func TestPostJobStoreFailure500(t *testing.T) {
	blocker := filepath.Join(t.TempDir(), "not-a-dir")
	if err := os.WriteFile(blocker, []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	q := queue.New()
	srv := api.New(q, store.New(filepath.Join(blocker, "data")))

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/jobs",
		strings.NewReader(`{"command": ["true"]}`))
	srv.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500 (body: %s)", rec.Code, rec.Body.String())
	}
	var body map[string]string
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Errorf("body is not JSON: %v", err)
	}

	// Nothing was enqueued: a worker started now must see an empty queue.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	seen := make(chan store.Record, 1)
	go q.Run(ctx, func(r store.Record) { seen <- r })
	select {
	case got := <-seen:
		t.Fatalf("a 500'd submission reached the worker: %+v", got)
	case <-time.After(50 * time.Millisecond):
	}
}
```

5. Add the shutdown-race test (Review Focus 1):

```go
// TestPostJobShutdown503 pins Review Focus 1: a POST whose request context
// is already dead (server shutting down) gets 503 + JSON error instead of
// blocking forever, and the pending record it saved stays pending —
// nothing is enqueued.
func TestPostJobShutdown503(t *testing.T) {
	dir := t.TempDir()
	q := queue.New()
	srv := api.New(q, store.New(dir))

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // as if the server were already shutting down
	req := httptest.NewRequest(http.MethodPost, "/jobs",
		strings.NewReader(`{"command": ["echo", "late"]}`)).WithContext(ctx)
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503 (body: %s)", rec.Code, rec.Body.String())
	}
	var body map[string]string
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("body is not JSON: %v", err)
	}

	// The saved record is the only one, and it stayed pending. Read the
	// store file directly — the store has no List, and inventing one for
	// a test is V-something-else's problem.
	b, err := os.ReadFile(filepath.Join(dir, "jobs.json"))
	if err != nil {
		t.Fatalf("read store file: %v", err)
	}
	var m map[string]store.Record
	if err := json.Unmarshal(b, &m); err != nil {
		t.Fatalf("decode store file: %v", err)
	}
	if len(m) != 1 {
		t.Fatalf("stored %d records, want exactly the one pending save", len(m))
	}
	for _, got := range m {
		if got.Status != job.StatusPending {
			t.Errorf("record status = %v, want pending", got.Status)
		}
	}

	// And it was never handed off: a worker now sees an empty queue.
	wctx, wcancel := context.WithCancel(context.Background())
	defer wcancel()
	seen := make(chan store.Record, 1)
	go q.Run(wctx, func(r store.Record) { seen <- r })
	select {
	case got := <-seen:
		t.Fatalf("the 503'd record reached the worker: %+v", got)
	case <-time.After(50 * time.Millisecond):
	}
}
```

6. `TestConcurrentRequestsRaceFree`: update the `newServer` destructuring to three values and change the POST's expected status check if it asserts one (it currently doesn't); the POST body/behavior needs no other change — it now exercises concurrent enqueue + GET, which is the race window that matters.

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test ./internal/api/ -v`
Expected: FAIL to build — `api.New(q, st) - too many arguments` / `unknown: queue` (current `New` still takes an executor).

- [ ] **Step 3: Write minimal implementation**

In `internal/api/server.go`:

1. Replace the package doc with:

```go
/*
Package api exposes Forge over HTTP: submit a job, fetch a job.

Handlers never run jobs — POST saves a pending record, hands it to the
queue, and answers 202 immediately; the worker goroutine started by
cmd/forge does the running. The request path stays fast and this package
never touches os/exec.
*/
```

2. Replace `Server` and `New` with (drop the `executor` import, add `queue`):

```go
/*
Server is the HTTP front end. It holds only the two collaborators every
handler needs: the queue jobs are handed to, and the store their records
live in. It never runs anything — V2 moved execution to the worker.
*/
type Server struct {
	q  *queue.Queue
	st *store.Store
}

/*
New wires the server. The queue is V2's seam: POST enqueues, the worker
dequeues, and this package cannot tell the difference between a live
worker and a test's — which is exactly why the tests need no executor.
*/
func New(q *queue.Queue, st *store.Store) *Server {
	return &Server{q: q, st: st}
}
```

3. Replace `createJob`'s steps 4–5 (the `s.exec.Run` block through the final `Save`) with:

```go
	// Step 4: persist the job's intent BEFORE answering. A pending record
	// on disk is what makes 202 honest: even if the process dies before
	// the worker picks the job up, the submission is not lost.
	rec := store.Record{ID: id, Command: in.Command, Status: job.StatusPending}
	if err := s.st.Save(rec); err != nil {
		writeErr(w, http.StatusInternalServerError, err)
		return
	}

	// Step 5: hand off to the worker. Enqueue selects on r.Context(): when
	// the server is shutting down and the worker may already be gone, a
	// send could block forever — cancellation turns the hand-off into an
	// honest 503 instead of a hung request.
	if err := s.q.Enqueue(r.Context(), rec); err != nil {
		writeErr(w, http.StatusServiceUnavailable, err)
		return
	}

	w.WriteHeader(http.StatusAccepted)
	_ = json.NewEncoder(w).Encode(rec)
```

Update the `createJob` doc comment's opening from "the V1 core" to describe the V2 flow: "decode → identify → validate → persist pending → enqueue → 202".

4. In `cmd/forge/main.go`'s `runServe`, replace the server construction:

```go
	var e executor.Executor
	st := store.New(*data)
	q := queue.New()
	/*
		One context governs server AND worker: the same signal that stops
		accepting requests also stops the worker loop. The worker finishes
		the job it holds and drops the rest — no drain, no sleep.
	*/
	go q.Run(ctx, newJobHandler(st, e))
	srv := &http.Server{
		Addr:    *addr,
		Handler: api.New(q, st).Handler(),
	}
```

Add the `queue` import to `main.go`.

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./... -v`
Expected: PASS everywhere — the queue, handler, rewritten API tests, and `cmd/forge` tests (including the untouched `TestRunServeServesThenShutsDown`, which never asserted a POST status).

- [ ] **Step 5: Full gate + commit**

Run: `gofmt -l .` (empty), `go vet ./...` (clean), `go test ./... -race` (green).

```bash
git add internal/api/server.go internal/api/server_test.go cmd/forge/main.go
git commit -m "feat(api): POST answers 202 and enqueues; worker runs the job"
```

---

### Task 6: Serve end-to-end — 202 out, terminal status on its own

The wiring is proven only by a real server: POST returns 202 without waiting, the record reaches a terminal status without anyone asking, and Ctrl-C still exits 0.

**Files:**
- Modify: none of production code — this task proves Task 5's wiring
- Test: `cmd/forge/main_test.go` (append)

**Interfaces:**
- Consumes: `runServe` as wired in Task 5; `newLineWriter` test helper (already in `main_test.go`); `store.Record` JSON shape over HTTP.

- [ ] **Step 1: Write the failing test**

Append to `cmd/forge/main_test.go` (add imports `encoding/json`, `github.com/Dushmilan/Forge/internal/job`, `github.com/Dushmilan/Forge/internal/store`; `net/http`, `context`, `io`, `strings`, `time` are already imported):

```go
// TestRunServeRunsJobsInBackground pins the V2 headline end to end: a real
// POST answers 202 without waiting, and the record reaches a terminal
// status on its own. The GET poll is deadline-bounded observation, not a
// guessed sleep: it cannot flake on timing, only bound the wait — and it
// reads the same status field a client would.
func TestRunServeRunsJobsInBackground(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	out := newLineWriter()
	code := make(chan int, 1)
	go func() {
		code <- runServe(ctx, []string{"-addr", "127.0.0.1:0", "-data", t.TempDir()}, out, io.Discard)
	}()

	var addr string
	select {
	case line := <-out.ch:
		addr = strings.TrimPrefix(line, "listening on ")
		if addr == line {
			t.Fatalf("unexpected startup line %q", line)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("runServe never printed its listening address")
	}

	resp, err := http.Post("http://"+addr+"/jobs", "application/json",
		strings.NewReader(`{"command": ["echo", "hello"]}`))
	if err != nil {
		t.Fatalf("POST failed: %v", err)
	}
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("POST status = %d, want 202", resp.StatusCode)
	}
	var rec store.Record
	if err := json.NewDecoder(resp.Body).Decode(&rec); err != nil {
		t.Fatalf("POST body is not JSON: %v", err)
	}
	resp.Body.Close()

	deadline := time.Now().Add(10 * time.Second)
	for {
		r, err := http.Get("http://" + addr + "/jobs/" + rec.ID)
		if err != nil {
			t.Fatalf("GET failed: %v", err)
		}
		var got store.Record
		if err := json.NewDecoder(r.Body).Decode(&got); err != nil {
			r.Body.Close()
			t.Fatalf("GET body is not JSON: %v", err)
		}
		r.Body.Close()
		switch got.Status {
		case job.StatusSucceeded:
			if got.Result.Stdout != "hello\n" {
				t.Errorf("stdout = %q, want %q", got.Result.Stdout, "hello\n")
			}
		case job.StatusFailed:
			t.Fatalf("job failed: %+v", got)
		default:
			if time.Now().After(deadline) {
				t.Fatalf("job never finished; last status %v", got.Status)
			}
			time.Sleep(time.Millisecond) // poll interval only; the deadline is the guarantee
			continue
		}
		break
	}

	cancel()
	select {
	case c := <-code:
		if c != 0 {
			t.Errorf("runServe() = %d, want 0 on clean shutdown", c)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("runServe did not shut down after cancel")
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./cmd/forge/ -run TestRunServeRunsJobsInBackground -v`
Expected: FAIL if the wiring is wrong; if it PASSES, Task 5's wiring is already proven end to end — record the pass honestly and still run the full suite before committing. (A compile error here means Task 5 left `runServe` unwired; go back to Task 5.)

- [ ] **Step 3: Fix wiring if red**

If the test failed: `runServe` must start `go q.Run(ctx, newJobHandler(st, e))` with the *same* ctx the HTTP server shuts down on. Compare with Task 5 Step 3's wiring block; the failure mode is almost always a different (derived) context for the worker.

- [ ] **Step 4: Full gate + commit**

Run: `gofmt -l .` (empty), `go vet ./...` (clean), `go test ./... -race` (green — run the whole suite twice if the e2e touched timing nerves).

```bash
git add cmd/forge/main_test.go
git commit -m "test(forge): serve e2e — 202 out, job completes on the worker"
```

---

### Task 7: Learning entry, README, tag v2

The version isn't done until `docs/LEARNING.md` has its entry, the README stops describing V1's synchronous world, and the tag exists.

**Files:**
- Modify: `docs/LEARNING.md` (append)
- Modify: `README.md` (server quick start, "How it works" section, ladder, layout)

**Interfaces:**
- Consumes: everything from Tasks 1–6, as shipped.

- [ ] **Step 1: Append the V2 entry to `docs/LEARNING.md`**

After the V1 entry:

```markdown
## V2 — Queue + worker

- **Concept:**
  - A buffered `chan` IS a thread-safe FIFO queue — the "queue + mutex +
    condition variable" reflex is the pre-Go design; the channel replaces
    all three.
  - The worker loop: `for { select { case <-ctx.Done(): return; case rec := <-ch: handle(rec) } }` —
    one goroutine, two wakeup reasons, no polling.
  - Blocking channel sends as backpressure, and putting the send *inside*
    a `select` with `ctx.Done()` so an enqueue can't deadlock shutdown.
  - Testing goroutines honestly: gates (channels the handler waits on) and
    deadlines instead of `time.Sleep`.
- **What I built:** `internal/queue` (channel + single-goroutine loop), an
  async API (`POST /jobs` answers 202 with a pending record; `GET` observes
  `pending → running → final` live), and `forge serve` wiring one context
  through both server and worker.
- **What surprised me:** _(you)_
- **Idiom worth remembering:** _(you)_
- **Next time:** _(you)_
```

- [ ] **Step 2: Update `README.md`**

1. `**Status:** V0 and V1 are done.` → `**Status:** V0–V2 are done.`
2. In the server quick start, the curl block becomes:

```sh
curl -X POST localhost:8080/jobs -d '{"command": ["echo", "hello"]}'   # 202 + pending record
curl localhost:8080/jobs/<id>                                          # 200 + record (poll for final status)
```

and the paragraph under it becomes: "Jobs run asynchronously (V2): the POST returns 202 with a `pending` record as soon as it is queued, one worker goroutine runs it, and `GET` shows the record moving `pending → running → succeeded|failed`. Every submitted job is persisted to `<data>/jobs.json`."

3. Add a `## How it works (V2)` section after the V1 one:

```markdown
## How it works (V2)

V2 moves execution off the request path. The API's only job is to persist
and enqueue; one worker goroutine drains the queue:

```
POST /jobs ──▶ api.Server ──▶ store.Save (pending) ──▶ queue.Enqueue ──▶ 202
                                    ▲                       │
                 worker goroutine: running ─▶ Runner ─▶ final Save
                                    └──── queue.Run(ctx, handler)
```

- [`internal/queue`](internal/queue) — a buffered channel (capacity 64) and
  a one-goroutine `select` loop; a full queue blocks the sender
  (backpressure), and cancellation ends the loop without a shutdown
  deadlock.
```

4. Ladder table: the V2 row's Status cell `⏳` → `✅`.
5. Project layout: add `│   ├── queue/             # channel + worker loop (V2)` after the `store/` line.

- [ ] **Step 3: Full verification**

Run and paste results into the commit body:
```bash
go test ./... && go test ./... -race && go vet ./... && gofmt -l .
```

- [ ] **Step 4: Commit and tag**

```bash
git add docs/LEARNING.md README.md
git commit -m "docs(v2): learning entry, README describes the async server"
git tag v2
```

- [ ] **Step 5: Confirm the ladder**

Run: `git tag` shows `v0 v1 v2`; `git log --oneline` shows the V2 slice commits. Earlier versions' tests still pass (they are part of the full suite that just went green).
