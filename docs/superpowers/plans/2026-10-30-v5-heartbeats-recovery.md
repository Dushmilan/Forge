# V5 — Heartbeats + Failure Recovery Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Survive a restart — record liveness while a job runs, rebuild the queue from disk on startup, repair orphaned `running` records, and make the status lifecycle an enforced state machine.

**Architecture:** `internal/job` owns a transition table (`Status.CanTransition`); `internal/store` enforces it in its locked read-modify-write, gains a `LastHeartbeat *time.Time` field with a single writer (`Heartbeat`), and a `List` for recovery. `internal/worker` resumes at the persisted attempt count and refreshes the heartbeat from a ticker goroutine it joins before saving. A new `internal/recovery` package scans the store once at startup, repairs orphans, and returns the records `runServe` must enqueue.

**Tech Stack:** Go 1.22 standard library only (`time`, `sync`, `errors`, `sort`). Zero new dependencies.

**Spec:** `docs/versions/V5.md`

## Global Constraints

- **TDD strictly:** one failing test at a time; run it, watch it fail, implement the minimum, watch it pass. Never write implementation before the test that demands it; never refactor while red.
- **Standard library only.** No new module dependencies.
- **No `time.Sleep` as synchronization.** Ticker-driven tests poll the store to a deadline or wait on channels.
- **Comment convention:** block comments (`/* … */`, `/** … **/` inline) explaining the Go concept, per AGENTS.md rule 4. Do not rewrite comments in code this version does not otherwise touch.
- **Field ownership:** `Save` never clears `LastHeartbeat`; only `Heartbeat(id, at)` writes it. The worker always saves records with a nil heartbeat.
- **State machine:** `store.Save` rejects an illegal transition and writes nothing. Creating a record that does not yet exist is a seed, not a transition.
- **Only the worker writes statuses while jobs run**; recovery writes them only before the pool starts.
- **Before every commit:** `gofmt -l .` empty, `go vet ./...` clean, `go test ./... -race` green.
- House test style: external test packages (`package store_test`, `package worker_test`, `package recovery_test`); `cmd/forge` tests stay in `package main`.

## File Structure

| File | Responsibility |
|---|---|
| `internal/job/job.go` | domain type, status, transition table |
| `internal/store/store.go` | persistence, transitions, heartbeat field, listing |
| `internal/worker/worker.go` | attempt loop, heartbeat goroutine, cancellation registry |
| `internal/recovery/recovery.go` (new) | startup reconciliation of orphaned records |
| `cmd/forge/main.go` | flag, recovery → pool → requeue wiring |
| `README.md`, `docs/LEARNING.md` | version docs |

## Review Focus

1. **A heartbeat racing the final save** → the terminal status must win, and no heartbeat may land afterwards. Owned by Task 5.
2. **A worker save rewinding `LastHeartbeat` to nil** → liveness must survive whole-record saves. Owned by Task 3.
3. **An illegal transition silently applied** → the file must be untouched and the error must wrap `job.ErrInvalidTransition`. Owned by Task 2.
4. **A crash loop** → a job that kills the process every time must exhaust its persisted budget and end `failed`, not restart forever. Owned by Task 4 (resume) and Task 6 (exhausted → failed).
5. **Recovery set larger than the queue buffer** → startup must not deadlock; start the pool before enqueuing. Owned by Task 7.

---

### Task 1: Status transition table

**Files:**
- Modify: `internal/job/job.go`
- Test: `internal/job/job_test.go`

**Interfaces:**
- Produces: `job.ErrInvalidTransition error`, `func (s Status) CanTransition(to Status) bool`

- [ ] **Step 1: Write the failing test**

Append to `internal/job/job_test.go`:

```go
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
```

- [ ] **Step 2: Run it and watch it fail**

Run: `go test ./internal/job -run TestStatusCanTransition -v`
Expected: compile error, `tt.from.CanTransition undefined`.

- [ ] **Step 3: Implement the table**

In `internal/job/job.go`, after `ErrInvalidJob`:

```go
// ErrInvalidTransition is returned, wrapped, when a caller tries to move a
// Status somewhere the lifecycle does not allow. Like ErrInvalidJob it is a
// sentinel so errors.Is can answer "was this a state-machine violation?"
// without matching text.
var ErrInvalidTransition = errors.New("job: invalid status transition")

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
```

- [ ] **Step 4: Run the test**

Run: `go test ./internal/job -race`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/job
git commit -m "feat(job): add the status transition table"
```

---

### Task 2: Store enforces transitions

**Files:**
- Modify: `internal/store/store.go`
- Test: `internal/store/store_test.go`

**Interfaces:**
- Consumes: `job.Status.CanTransition`, `job.ErrInvalidTransition`
- Produces: `store.Save` rejects illegal transitions with a wrapped `job.ErrInvalidTransition`

- [ ] **Step 1: Write the failing test**

Append to `internal/store/store_test.go`:

```go
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
```

- [ ] **Step 2: Run and watch them fail**

Run: `go test ./internal/store -run 'TestSaveRejectsIllegalTransition|TestSaveAllowsRecoveryTransition' -v`
Expected: FAIL — the illegal transition is accepted (first test), second passes already.

- [ ] **Step 3: Enforce in `Save`**

In `internal/store/store.go`, replace the assignment in `Save` with:

```go
	/*
		The state machine is checked where the transition becomes durable.
		Reading the stored record inside the same lock as the write is what
		makes the check atomic: no caller can slip a different status in
		between validation and persistence. A record that does not exist yet
		is a seed, not a transition — V1's tests and direct test setup save
		terminal records first.
	*/
	if cur, ok := m[r.ID]; ok {
		if !cur.Status.CanTransition(r.Status) {
			return fmt.Errorf("store: job %q: %w: %s -> %s",
				r.ID, job.ErrInvalidTransition, cur.Status, r.Status)
		}
	}
	m[r.ID] = r
```

- [ ] **Step 4: Run the package**

Run: `go test ./internal/store -race`
Expected: PASS (all existing store tests still green).

- [ ] **Step 5: Commit**

```bash
git add internal/store
git commit -m "feat(store): enforce the status state machine on save"
```

---

### Task 3: Heartbeat field, targeted writer, and listing

**Files:**
- Modify: `internal/store/store.go`
- Test: `internal/store/store_test.go`

**Interfaces:**
- Produces:
  - `store.Record.LastHeartbeat *time.Time`
  - `func (s *Store) Heartbeat(id string, at time.Time) error`
  - `func (s *Store) List() ([]Record, error)`
  - `store.ErrNotRunning error`

- [ ] **Step 1: Write the failing tests**

Append to `internal/store/store_test.go`:

```go
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
```

- [ ] **Step 2: Run and watch them fail**

Run: `go test ./internal/store -run 'TestHeartbeat|TestSavePreservesHeartbeat|TestListIsOrderedByID' -v`
Expected: compile errors for `LastHeartbeat`, `Heartbeat`, `List`, `ErrNotRunning`.

- [ ] **Step 3: Implement**

In `internal/store/store.go`:

```go
// ErrNotRunning is returned by Heartbeat when the record exists but is no
// longer running: the caller's tick arrived after the job ended. It is a
// sentinel because "stop heartbeating" is a normal outcome, not a failure.
var ErrNotRunning = errors.New("store: job not running")
```

Add to `Record`:

```go
	/*
		A pointer, not time.Time: nil means "this job has never run" and is
		omitted from JSON, while a zero time.Time would serialize as
		0001-01-01T00:00:00Z on every pending job. encode/json follows the
		pointer, so the field is both honest and absent when unknown.
	*/
	LastHeartbeat *time.Time `json:"last_heartbeat,omitempty"`
```

In `Save`, extend the existing-record branch so the heartbeat survives:

```go
	if cur, ok := m[r.ID]; ok {
		if !cur.Status.CanTransition(r.Status) {
			return fmt.Errorf("store: job %q: %w: %s -> %s",
				r.ID, job.ErrInvalidTransition, cur.Status, r.Status)
		}
		/*
			Field ownership: the worker persists whole records but never
			knows the heartbeat; Heartbeat is the only writer. Taking the
			stored value when the incoming one is nil is what keeps a
			whole-record save from rewinding liveness — the same reason the
			store exists instead of two goroutines racing on one struct.
		*/
		if r.LastHeartbeat == nil {
			r.LastHeartbeat = cur.LastHeartbeat
		}
	}
```

Extract the write into one helper and add the two methods:

```go
/*
writeLocked persists the whole map. The suffix documents the locking contract:
every caller already holds s.mu, so two writers can never interleave a read,
a modify, and a write. tmp+rename is the atomicity: a crash leaves the old
file, never a half-written one.
*/
func (s *Store) writeLocked(m map[string]Record) error {
	b, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return fmt.Errorf("store: encode: %w", err)
	}
	tmp := s.path() + ".tmp"
	if err := os.WriteFile(tmp, b, 0o644); err != nil {
		return fmt.Errorf("store: write temp: %w", err)
	}
	if err := os.Rename(tmp, s.path()); err != nil {
		return fmt.Errorf("store: rename: %w", err)
	}
	return nil
}

/*
Heartbeat advances one record's liveness without touching status, result, or
attempts. It exists so the heartbeat goroutine and the worker never write the
same fields: two writers, one mutex, no lost update.
*/
func (s *Store) Heartbeat(id string, at time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	m, err := s.load()
	if err != nil {
		return err
	}
	r, ok := m[id]
	if !ok {
		return fmt.Errorf("store: job %q: %w", id, ErrNotFound)
	}
	if r.Status != job.StatusRunning {
		return fmt.Errorf("store: job %q is %s: %w", id, r.Status, ErrNotRunning)
	}
	r.LastHeartbeat = &at
	m[id] = r
	return s.writeLocked(m)
}

/*
List returns a deterministic snapshot. sort.Slice is deliberate: Go randomizes
map iteration, and recovery must repair records in the same order every run.
*/
func (s *Store) List() ([]Record, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	m, err := s.load()
	if err != nil {
		return nil, err
	}
	out := make([]Record, 0, len(m))
	for _, r := range m {
		out = append(out, r)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out, nil
}
```

Replace `Save`'s inline marshal/write/rename block with `return s.writeLocked(m)` after `m[r.ID] = r`. Add `"sort"` and `"time"` to imports.

- [ ] **Step 4: Run the package**

Run: `go test ./internal/store -race`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/store
git commit -m "feat(store): add heartbeat timestamps and listing"
```

---

### Task 4: Worker resumes the persisted attempt budget

**Files:**
- Modify: `internal/worker/worker.go`
- Test: `internal/worker/worker_test.go`

**Interfaces:**
- Consumes: `store.Record.Attempts`
- Produces: `Handle` starts at `rec.Attempts + 1`; exhausted records end `failed` with no runner call

- [ ] **Step 1: Write the failing tests**

Append to `internal/worker/worker_test.go`:

```go
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
```

- [ ] **Step 2: Run and watch them fail**

Run: `go test ./internal/worker -run 'TestHandleResumesAttemptBudget|TestHandleExhaustedBudgetRunsNothing' -v`
Expected: FAIL — `Attempts` comes back 1 and the exhausted record still runs.

- [ ] **Step 3: Implement**

In `Handle`, after `defer release()`:

```go
	/*
		The retry budget is persisted, not per-process: recovery requeues an
		interrupted job with its Attempts intact, so the next attempt must
		continue the count. A record that arrives already exhausted is a bug
		in recovery or a stale queue entry; fail it rather than run it.
	*/
	if rec.Attempts >= w.cfg.MaxAttempts {
		rec.Status = job.StatusFailed
		rec.Err = fmt.Sprintf("job exhausted %d attempt(s)", w.cfg.MaxAttempts)
		w.save(rec)
		return
	}
```

and change the loop header:

```go
	for attempt := rec.Attempts + 1; attempt <= w.cfg.MaxAttempts; attempt++ {
```

- [ ] **Step 4: Run the package**

Run: `go test ./internal/worker -race`
Expected: PASS (all V2–V4 worker tests unchanged).

- [ ] **Step 5: Commit**

```bash
git add internal/worker
git commit -m "feat(worker): resume the persisted attempt budget"
```

---

### Task 5: Per-attempt heartbeat goroutine

**Files:**
- Modify: `internal/worker/worker.go`
- Test: `internal/worker/worker_test.go`

**Interfaces:**
- Consumes: `store.Heartbeat`, `store.ErrNotRunning`
- Produces: `worker.Config.Heartbeat time.Duration` (zero disables)

- [ ] **Step 1: Write the failing tests**

Append to `internal/worker/worker_test.go`:

```go
/*
TestHandleHeartbeatsWhileRunning pins the ticker: a job that blocks must keep
its record visibly alive. The runner waits until the test has observed a
heartbeat, so the synchronization is the store itself — no sleep.
*/
func TestHandleHeartbeatsWhileRunning(t *testing.T) {
	beating := make(chan struct{})
	var once sync.Once
	r := runnerFunc(func(context.Context, job.Job) (executor.Result, error) {
		<-beating
		return executor.Result{Stdout: "ok\n"}, nil
	})
	w, st := newWorker(t, r, worker.Config{
		MaxAttempts: 1,
		Timeout:     time.Second,
		Heartbeat:   5 * time.Millisecond,
	})

	done := make(chan struct{})
	go func() {
		w.Handle(context.Background(), store.Record{ID: "j-beat", Command: []string{"slow"}, Status: job.StatusPending})
		close(done)
	}()

	deadline := time.After(2 * time.Second)
	for {
		rec, err := st.Get("j-beat")
		if err == nil && rec.LastHeartbeat != nil {
			once.Do(func() { close(beating) })
			break
		}
		select {
		case <-deadline:
			t.Fatal("no heartbeat observed before the deadline")
		default:
		}
	}

	waitDone(t, done)
	if rec := mustGet(t, st, "j-beat"); rec.Status != job.StatusSucceeded {
		t.Errorf("status = %v, want succeeded", rec.Status)
	}
}

/*
TestHandleStopsHeartbeatBeforeFinalSave pins Review Focus 1: the ticker
goroutine must be joined before the terminal save. After Handle returns, the
timestamp must freeze — a late tick may only ever touch a running record.
*/
func TestHandleStopsHeartbeatBeforeFinalSave(t *testing.T) {
	r := runnerFunc(func(context.Context, job.Job) (executor.Result, error) {
		return executor.Result{Stdout: "ok\n"}, nil
	})
	w, st := newWorker(t, r, worker.Config{
		MaxAttempts: 1,
		Timeout:     time.Second,
		Heartbeat:   time.Millisecond,
	})

	w.Handle(context.Background(), store.Record{ID: "j-stopbeat", Command: []string{"true"}, Status: job.StatusPending})
	first := mustGet(t, st, "j-stopbeat")

	// Several intervals later, a leaked goroutine would have advanced the
	// timestamp. The timer is a deadline, not a sleep used for ordering.
	select {
	case <-time.After(20 * time.Millisecond):
	}
	second := mustGet(t, st, "j-stopbeat")
	if first.LastHeartbeat == nil {
		t.Fatal("no heartbeat recorded")
	}
	if !second.LastHeartbeat.Equal(*first.LastHeartbeat) {
		t.Errorf("heartbeat advanced after Handle returned: %v -> %v", first.LastHeartbeat, second.LastHeartbeat)
	}
	if second.Status != job.StatusSucceeded {
		t.Errorf("status = %v, want succeeded", second.Status)
	}
}

/*
TestHandleSurvivesHeartbeatWriteFailure pins that liveness is best-effort: a
failing heartbeat is reported once and must not fail the job or leak the
goroutine.
*/
func TestHandleSurvivesHeartbeatWriteFailure(t *testing.T) {
	blocker := filepath.Join(t.TempDir(), "not-a-dir")
	if err := os.WriteFile(blocker, []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	st := store.New(filepath.Join(blocker, "data"))
	r := runnerFunc(func(context.Context, job.Job) (executor.Result, error) {
		return executor.Result{}, nil
	})
	var stderr bytes.Buffer
	w := worker.New(st, r, worker.Config{MaxAttempts: 1, Timeout: time.Second, Heartbeat: time.Millisecond}, &stderr)

	w.Handle(context.Background(), store.Record{ID: "j-hbfail", Command: []string{"true"}, Status: job.StatusPending})

	if !strings.Contains(stderr.String(), "j-hbfail") {
		t.Errorf("stderr = %q, want the heartbeat failure reported", stderr.String())
	}
}

/*
TestHeartbeatRacesFinalSave drives a fast ticker against a fast runner under
-race. The terminal status must be the last word; the store's mutex plus field
ownership is what makes that true.
*/
func TestHeartbeatRacesFinalSave(t *testing.T) {
	r := runnerFunc(func(context.Context, job.Job) (executor.Result, error) {
		return executor.Result{}, nil
	})
	w, st := newWorker(t, r, worker.Config{MaxAttempts: 1, Timeout: time.Second, Heartbeat: time.Microsecond})

	for i := 0; i < 20; i++ {
		id := fmt.Sprintf("j-race-%d", i)
		w.Handle(context.Background(), store.Record{ID: id, Command: []string{"true"}, Status: job.StatusPending})
		rec := mustGet(t, st, id)
		if rec.Status != job.StatusSucceeded {
			t.Fatalf("%s status = %v, want succeeded", id, rec.Status)
		}
	}
}
```

Note: the third test's failed save path means the heartbeat never fires, so it is weak. Keep it anyway as the error-path pin; the first test covers the happy path.

- [ ] **Step 2: Run and watch them fail**

Run: `go test ./internal/worker -run 'TestHandleHeartbeats|TestHandleStopsHeartbeat|TestHeartbeatRaces' -v`
Expected: FAIL — `Heartbeat` is not a field of `worker.Config`.

- [ ] **Step 3: Implement**

Add to `Config`:

```go
	// Heartbeat is how often a running attempt refreshes LastHeartbeat.
	// Zero disables it; runServe always passes a positive value.
	Heartbeat time.Duration
```

In `Handle`, around the runner call:

```go
		stopHeartbeat := w.startHeartbeat(rec.ID)
		res, err := w.exec.Run(execCtx, job.Job{ID: rec.ID, Command: rec.Command})
		stopHeartbeat() // join before the outcome save: no tick may follow the final status
		rec.Result = res
```

Add the two methods:

```go
/*
startHeartbeat runs a ticker goroutine that refreshes the record's liveness
until the returned stop function is called. The returned function closes the
stop channel AND waits on done: stopping without joining would let one more
tick land after the caller has saved the final status. Every goroutine gets
a stop path, and every stop path joins.
*/
func (w *Worker) startHeartbeat(id string) func() {
	if w.cfg.Heartbeat <= 0 {
		return func() {}
	}

	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		t := time.NewTicker(w.cfg.Heartbeat)
		defer t.Stop() // a ticker is a runtime resource; defer is its ledger entry
		for {
			select {
			case <-stop:
				return
			case <-t.C:
				if err := w.st.Heartbeat(id, time.Now()); err != nil {
					/*
						ErrNotRunning/ErrNotFound mean the record already ended;
						that is the normal way a heartbeat retires, not a fault.
						Anything else is reported once and ends the goroutine:
						retrying a broken disk every tick would only spam stderr.
					*/
					if !errors.Is(err, store.ErrNotRunning) && !errors.Is(err, store.ErrNotFound) {
						w.report(fmt.Errorf("heartbeat job %s: %w", id, err))
					}
					return
				}
			}
		}
	}()

	return func() {
		close(stop)
		<-done
	}
}

/*
report serializes writes to the shared stderr Writer. N workers call this
concurrently, and an arbitrary io.Writer is not goroutine-safe — V3's rule,
now shared by save and heartbeat failures.
*/
func (w *Worker) report(err error) {
	w.errMu.Lock()
	defer w.errMu.Unlock()
	fmt.Fprintf(w.stderr, "forge: %v\n", err)
}
```

Change `save` to use `w.report(fmt.Errorf("save job %s: %w", rec.ID, err))` (preserving the "forge: save job <id>: ..." shape).

- [ ] **Step 4: Run the package**

Run: `go test ./internal/worker -race`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/worker
git commit -m "feat(worker): heartbeat running attempts from a joined ticker"
```

---

### Task 6: Recovery package

**Files:**
- Create: `internal/recovery/recovery.go`
- Create: `internal/recovery/recovery_test.go`

**Interfaces:**
- Consumes: `store.List`, `store.Save`, `store.Record`
- Produces: `recovery.Config{MaxAttempts int}`, `recovery.Recover(st *store.Store, cfg Config) ([]store.Record, error)`

- [ ] **Step 1: Write the failing tests**

Create `internal/recovery/recovery_test.go`:

```go
package recovery_test

import (
	"errors"
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
TestRecoverFailsLoudlyOnCorruptStore pins Review Focus-style honesty: unknown
startup state must be an error, never an empty queue that silently serves.
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
```

Remove the unused `errors` import if the linter complains (it is only referenced if you use `errors.Is` — either assert the error wraps nothing here or drop it).

- [ ] **Step 2: Run and watch them fail**

Run: `go test ./internal/recovery -v`
Expected: FAIL — package does not exist.

- [ ] **Step 3: Implement**

Create `internal/recovery/recovery.go`:

```go
/*
Package recovery reconciles the store with a fresh process. A crash leaves
records asserting states no goroutine is working on any more: running jobs
with a dead worker, pending jobs that were still buffered in the channel.
Recovery turns that snapshot back into a runnable queue before the pool
starts, so startup never depends on what the previous process happened to
have in memory.
*/
package recovery

import (
	"fmt"

	"github.com/Dushmilan/Forge/internal/job"
	"github.com/Dushmilan/Forge/internal/store"
)

// Config is the policy a restart uses to judge interrupted work. MaxAttempts
// is the current process's budget; a restart with a smaller budget may fail a
// job the previous process would have retried.
type Config struct {
	MaxAttempts int
}

/*
Recover repairs orphaned records and returns the ones the caller must enqueue.
A running record is an orphan by definition: this process just started and is
the only one allowed to run jobs. There is deliberately no heartbeat-age check
— comparing timestamps only becomes meaningful when a second process could
still own the record (V11); pretending to enforce a lease now would add
machinery with no honest failure mode to catch.
*/
func Recover(st *store.Store, cfg Config) ([]store.Record, error) {
	records, err := st.List()
	if err != nil {
		return nil, fmt.Errorf("recovery: list: %w", err)
	}

	var requeue []store.Record
	for _, rec := range records {
		switch rec.Status {
		case job.StatusPending:
			// Already runnable; recovery only needs to hand it to the queue.
			requeue = append(requeue, rec)

		case job.StatusRunning:
			/*
				The interrupted attempt was counted when it started, so a
				restart costs one attempt rather than resetting the budget.
				Exhausted records fail here: a job that kills the process
				every time must not crash-loop forever.
			*/
			if rec.Attempts >= cfg.MaxAttempts {
				rec.Status = job.StatusFailed
				rec.Err = fmt.Sprintf("job interrupted by forge restart; attempts exhausted (%d/%d)", rec.Attempts, cfg.MaxAttempts)
			} else {
				rec.Status = job.StatusPending
				rec.Err = fmt.Sprintf("job interrupted by forge restart after attempt %d", rec.Attempts)
				requeue = append(requeue, rec)
			}
			/*
				Save enforces the state machine, so this repair is only
				possible because running→pending is an explicit edge. A
				repair that the store refused would be a bug worth surfacing,
				hence the wrapped error instead of a skipped record.
			*/
			if err := st.Save(rec); err != nil {
				return nil, fmt.Errorf("recovery: save %s: %w", rec.ID, err)
			}
		}
	}
	return requeue, nil
}
```

- [ ] **Step 4: Run the package**

Run: `go test ./internal/recovery -race`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/recovery
git commit -m "feat(recovery): requeue interrupted jobs and fail exhausted ones"
```

---

### Task 7: Wire recovery and the heartbeat flag into `serve`

**Files:**
- Modify: `cmd/forge/main.go`
- Test: `cmd/forge/main_test.go`

**Interfaces:**
- Consumes: `recovery.Recover`, `recovery.Config`, `worker.Config.Heartbeat`
- Produces: `-heartbeat-interval` flag; startup order recover → pool → enqueue

- [ ] **Step 1: Write the failing tests**

Append to `cmd/forge/main_test.go`:

Read the existing `TestServeRejectsBadFlags` table and add these rows (match its exact struct field names):

```go
		{"-heartbeat-interval", "0", "-heartbeat-interval must be > 0"},
		{"-heartbeat-interval", "-1s", "-heartbeat-interval must be > 0"},
		{"-heartbeat-interval", "nope", "invalid value"},
```

Add a recovery e2e test. It seeds the store directly, then starts `serve`:

```go
/*
TestServeRecoversInterruptedJob pins V5's headline: a record left running by
a dead process is requeued and finishes after a restart, at the next attempt.
The seed is the crash: jobs.json written before serve exists.
*/
func TestServeRecoversInterruptedJob(t *testing.T) {
	data := t.TempDir()
	st := store.New(data)
	if err := st.Save(store.Record{
		ID:       "orphan",
		Command:  []string{"echo", "recovered"},
		Status:   job.StatusRunning,
		Attempts: 1,
	}); err != nil {
		t.Fatal(err)
	}
	if err := st.Save(store.Record{
		ID:      "queued",
		Command: []string{"echo", "queued"},
		Status:  job.StatusPending,
	}); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	addr, code := serveAddr(t, ctx, "-data", data, "-max-attempts", "3", "-retry-backoff", "1ms", "-job-timeout", "5s")

	for _, tt := range []struct {
		id       string
		attempts int
	}{
		{"orphan", 2},
		{"queued", 1},
	} {
		rec := pollJob(t, addr, tt.id)
		if rec.Status != job.StatusSucceeded {
			t.Errorf("%s status = %v, want succeeded", tt.id, rec.Status)
		}
		if rec.Attempts != tt.attempts {
			t.Errorf("%s Attempts = %d, want %d", tt.id, rec.Attempts, tt.attempts)
		}
	}

	cancel()
	waitServe(t, code)
}
```

Use the helpers already present in `main_test.go` (`serveAddr`, `pollJob`, `waitServe`, or their local equivalents). If `serveAddr` always injects `-data t.TempDir()`, add a variant `serveAddrData(t, ctx, data, args...)` that takes the directory instead — do not duplicate the whole server harness. Inspect lines 520–600 before writing and reuse the existing names.

- [ ] **Step 2: Run and watch them fail**

Run: `go test ./cmd/forge -run 'TestServeRejectsBadFlags|TestServeRecoversInterruptedJob' -v`
Expected: FAIL — unknown flag `-heartbeat-interval`; and `-data` passed twice overrides the temp dir (fix the helper in step 3 if needed).

- [ ] **Step 3: Implement**

In `runServe`, add the flag:

```go
	heartbeat := fs.Duration("heartbeat-interval", time.Second, "how often a running job refreshes its heartbeat")
```

Validate before wiring anything:

```go
	if *heartbeat <= 0 {
		fmt.Fprintln(stderr, "forge: -heartbeat-interval must be > 0")
		return 2 // a non-positive ticker interval panics time.NewTicker
	}
```

Pass it to the worker:

```go
	w := worker.New(st, e, worker.Config{
		MaxAttempts: *maxAttempts,
		Backoff:     *retryBackoff,
		Timeout:     *jobTimeout,
		Heartbeat:   *heartbeat,
	}, stderr)
```

Run recovery **before** the pool and enqueue after it starts:

```go
	/*
		Recover before any worker exists: the repair must be the only writer
		while it runs, and the queue must never see a half-repaired record.
		Starting the pool before enqueuing is the other half: a recovery set
		larger than the queue's 64-slot buffer would otherwise block the
		startup goroutine forever with no receiver.
	*/
	requeue, err := recovery.Recover(st, recovery.Config{MaxAttempts: *maxAttempts})
	if err != nil {
		fmt.Fprintf(stderr, "forge: %v\n", err)
		return 1
	}

	workerDone := make(chan struct{})
	go func() {
		q.Run(ctx, *workers, w.Handle)
		close(workerDone)
	}()

	if len(requeue) > 0 {
		fmt.Fprintf(stderr, "forge: recovered %d job(s)\n", len(requeue))
		for _, rec := range requeue {
			if err := q.Enqueue(ctx, rec); err != nil {
				fmt.Fprintf(stderr, "forge: requeue job %s: %v\n", rec.ID, err)
			}
		}
	}
```

Add the import `"github.com/Dushmilan/Forge/internal/recovery"` and keep the existing `srv` wiring below.

- [ ] **Step 4: Run the tests**

Run: `go test ./cmd/forge -race`
Expected: PASS, including the V1–V4 e2e tests.

- [ ] **Step 5: Commit**

```bash
git add cmd/forge
git commit -m "feat(forge): recover interrupted jobs on serve startup"
```

---

### Task 8: Docs and tag

**Files:**
- Modify: `README.md`
- Modify: `docs/LEARNING.md`

- [ ] **Step 1: README**

- Change the status line to `V0–V5 are done.`
- Add `-heartbeat-interval 1s` to the `serve` example and the flag sentence.
- Add a `## How it works (V5)` section: startup recovery table and the field-ownership rule (heartbeat writer vs whole-record saves), with a short ASCII flow.
- Note at-least-once semantics and that `running` records are reclaimed unconditionally at startup.

- [ ] **Step 2: LEARNING.md**

Add a `## V5 — Heartbeats + failure recovery` entry in the V3/V4 format:

- **Concept:** tickers and joining the goroutine that owns one; `time.Time` through JSON with pointer fields; a transition table as executable domain knowledge; startup reconciliation; field ownership as the alternative to one giant lock.
- **What I built:** `Status.CanTransition` + `ErrInvalidTransition`; `LastHeartbeat`, `store.Heartbeat`, `store.List`; worker attempt resumption and per-attempt heartbeat; `internal/recovery`; `-heartbeat-interval`.
- **What surprised me:** _(you)_
- **Idiom worth remembering:** _(you)_
- **Next time:** _(you)_

- [ ] **Step 3: Full verification**

```bash
gofmt -l .
go vet ./...
go test ./... -race
```
Expected: no output from `gofmt`, no vet findings, all packages green.

- [ ] **Step 4: Commit and tag**

```bash
git add README.md docs/LEARNING.md
git commit -m "docs(v5): learning entry and README describe heartbeats and recovery"
git tag v5
```

- [ ] **Step 5: Confirm earlier versions still pass**

Run: `go test ./... -race -count=1`
Expected: PASS.

---

## Self-Review

- **Spec coverage:** transition table (T1/T2), heartbeat field + writer + List (T3), attempt resumption (T4), heartbeat ticker + join + race (T5), recovery table + honesty (T6), flag + startup order + e2e (T7), docs/tag (T8). Every spec deliverable maps to a task.
- **Placeholders:** none; each test and implementation step carries code.
- **Type consistency:** `LastHeartbeat *time.Time`, `Heartbeat(id string, at time.Time) error`, `List() ([]Record, error)`, `CanTransition(Status) bool`, `Config.Heartbeat time.Duration`, `recovery.Config{MaxAttempts int}`, `Recover(*store.Store, Config) ([]Record, error)` — used consistently from T1 through T7.
- **Review Focus:** heartbeat/final-save race (T5), heartbeat rewind (T3), illegal transition (T2), crash loop (T4/T6), recovery-buffer deadlock (T7).
