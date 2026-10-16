# V3 — Concurrent Workers Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace V2's single worker goroutine with a `sync.WaitGroup`-managed pool of `N` concurrent workers draining the shared queue channel, configurable via `forge serve -workers`.

**Architecture:** `queue.Queue.Run(ctx, workers, handler)` spawns `workers` goroutines running the single-worker `select` loop (`q.work`) and joins them with a `sync.WaitGroup`. `cmd/forge` adds `-workers` (default 4, rejecting `< 1` with exit 2) and guards `newJobHandler`'s `stderr` writes with a `sync.Mutex` so concurrent worker save failures are race-free. `internal/job`, `internal/executor`, `internal/store`, and `internal/api` production code remain untouched.

**Tech Stack:** Go 1.22 standard library only (`sync`, `sync.WaitGroup`, `sync.Mutex`, `context`, channels). Zero new dependencies.

**Spec:** `docs/versions/V3.md`

## Global Constraints

- **TDD strictly:** one failing test at a time; run it, watch it fail, implement minimum, watch it pass. Never write implementation before the test that demands it; never refactor while red.
- **Standard library only.** No new module dependencies.
- **No `time.Sleep` as synchronization in tests.** Synchronize with channels, barriers, or `context`. A `select` with `time.After` is allowed as a *deadline* on a channel that must fire, and as a *negative* check ("this must NOT happen within the window"). The two bounded status polls in the e2e test observe state instead of guessing timing.
- **Comment convention:** new and modified code uses block comments (`/* … */`, `/** … **/` for short inline notes) per AGENTS.md rule 4. Doc comments start with the declaration name and teach *why* in Go terms. Do not rewrite comments in files this version doesn't otherwise touch.
- **Packages extended, not rewritten:** `internal/job`, `internal/executor`, `internal/store`, and `internal/api` production code are untouched by V3. `internal/queue` and `cmd/forge` change.
- **One context, one shutdown story:** the `signal.NotifyContext` context is passed to both `http.Server` (via Shutdown) and `queue.Run`. `Queue.Run` blocks on its `WaitGroup`, so every in-flight worker finishes before `runServe` returns.
- **Before every commit:** `gofmt -l .` empty, `go vet ./...` clean, `go test ./... -race` green.
- **Working-tree pre-flight:** `docs/versions/V3.md` is committed before Task 1.
- House test style: external test packages (`package queue_test`, `package api_test`); `cmd/forge` tests stay in `package main`.

## Review Focus

Spec-implied inputs and concurrency hazards most likely to bite first:

1. **Slow job blocking fast jobs (head-of-line blocking)** → with `workers >= 2`, a job stuck in the handler must not prevent later jobs from being dequeued and completed by sibling workers. Owned by Task 1.
2. **Partial shutdown join** → when 2 workers are mid-flight at `ctx` cancel and 1 finishes early, `Run` must NOT return until the *second* worker also finishes (`wg.Wait()` waits for all `N`, not the first). Owned by Task 2.
3. **Concurrent `stderr` writes on `Save` failure** → `newJobHandler` takes an arbitrary `io.Writer` (e.g., `*bytes.Buffer` in tests); two workers failing `st.Save` simultaneously race on `stderr` without a mutex. Owned by Task 3.
4. **Non-positive `-workers` flag (`0`, `-3`)** → starting a server with `0` workers would accept jobs with 202 and silently never run them; `runServe` must reject `-workers < 1` at startup with exit code `2`. Owned by Task 4.
5. **`wg.Add` placement race** → calling `wg.Add(1)` inside the child goroutine instead of before `go func()` lets `wg.Wait()` return before any worker starts when `ctx` cancels quickly. Owned by Task 1 (`TestRunReturnsWhenIdle` + the implementation).

---

### Task 1: Worker pool fan-out in `internal/queue`

Upgrade `Queue.Run` to take `workers int`, spawn `workers` goroutines reading from `q.jobs`, and wait for all of them with `sync.WaitGroup`. Update every `q.Run` caller to the new signature, and prove both simultaneous execution (barrier test) and head-of-line bypass.

**Files:**
- Modify: `internal/queue/queue.go`
- Modify: `internal/queue/queue_test.go`
- Modify: `internal/api/server_test.go`
- Modify: `cmd/forge/main.go`

**Interfaces:**
- Consumes: `store.Record`
- Produces: `func (q *Queue) Run(ctx context.Context, workers int, handler func(store.Record))`

- [ ] **Step 1: Write the failing tests and update existing `q.Run` call sites**

In `internal/queue/queue_test.go`, update the existing `q.Run(ctx, ...)` calls in `TestRunDeliversInOrder`, `TestEnqueueBackpressure`, `TestRunReturnsWhenIdle`, and `TestRunFinishesCurrentAndDropsQueued` to pass `1` as the second argument (`q.Run(ctx, 1, ...)`). Then append:

```go
/*
TestRunExecutesConcurrently proves that Run(ctx, 3, ...) actually spawns 3
concurrent workers: 3 enqueued jobs all enter the handler before ANY of
them is allowed to return. A single worker (or a loop that forgot `go`)
would deadlock on the barrier.
*/
func TestRunExecutesConcurrently(t *testing.T) {
	const workers = 3
	q := queue.New()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	arrived := make(chan string, workers)
	release := make(chan struct{})
	runDone := make(chan struct{})
	go func() {
		q.Run(ctx, workers, func(rec store.Record) {
			arrived <- rec.ID
			<-release
		})
		close(runDone)
	}()

	for i := 0; i < workers; i++ {
		mustEnqueue(t, ctx, q, store.Record{ID: strconv.Itoa(i)})
	}

	// All 3 workers must be inside the handler at once before we release them.
	for i := 0; i < workers; i++ {
		select {
		case <-arrived:
		case <-time.After(2 * time.Second):
			t.Fatalf("only %d of %d workers entered handler concurrently", i, workers)
		}
	}
	close(release)

	cancel()
	select {
	case <-runDone:
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return after cancel")
	}
}

/*
TestRunFastJobsBypassSlowWorker pins Review Focus 1 (why V3 exists): when
worker 1 is occupied by a slow job, worker 2 still drains subsequent fast
jobs to completion instead of stalling behind the slow one.
*/
func TestRunFastJobsBypassSlowWorker(t *testing.T) {
	q := queue.New()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	slowEntered := make(chan struct{})
	slowGate := make(chan struct{})
	defer close(slowGate)
	fastDone := make(chan string, 2)

	go q.Run(ctx, 2, func(rec store.Record) {
		if rec.ID == "slow" {
			close(slowEntered)
			<-slowGate
			return
		}
		fastDone <- rec.ID
	})

	mustEnqueue(t, ctx, q, store.Record{ID: "slow"})
	select {
	case <-slowEntered:
	case <-time.After(2 * time.Second):
		t.Fatal("slow job never entered handler")
	}

	mustEnqueue(t, ctx, q, store.Record{ID: "fast-1"})
	mustEnqueue(t, ctx, q, store.Record{ID: "fast-2"})

	for i := 0; i < 2; i++ {
		select {
		case <-fastDone:
		case <-time.After(2 * time.Second):
			t.Fatalf("fast job %d blocked behind slow job", i+1)
		}
	}
}
```

Update the four `q.Run` call sites in `internal/api/server_test.go` to `q.Run(..., 1, ...)` and the one in `cmd/forge/main.go` (inside `runServe`) to `q.Run(ctx, 1, newJobHandler(st, e, stderr))` so the whole workspace compiles against the new signature.

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test ./internal/queue/ -run 'TestRunExecutesConcurrently|TestRunFastJobsBypassSlowWorker' -v`
Expected: FAIL to build — `too many arguments in call to q.Run`.

- [ ] **Step 3: Write minimal implementation**

In `internal/queue/queue.go`, add `"sync"` to the imports, replace `Run`, and move the loop body into a `work` method:

```go
/*
Run drains the queue across a pool of worker goroutines and blocks until
every worker has returned.

sync.WaitGroup is the fan-out/join primitive: wg.Add(1) MUST run on the
spawning goroutine before `go`, never inside the child goroutine — otherwise
wg.Wait() could race ahead of the child's Add and return before the worker
even starts.

Competing consumers need no extra lock: a Go channel already guarantees
each record is received by exactly one worker.
*/
func (q *Queue) Run(ctx context.Context, workers int, handler func(store.Record)) {
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done() /** decrements the counter when the worker returns **/
			q.work(ctx, handler)
		}()
	}
	wg.Wait()
}

/*
work is one worker goroutine's lifecycle: pull a record, run handler to
completion, then re-select. Between jobs it notices ctx cancellation and
returns; records still buffered stay pending.
*/
func (q *Queue) work(ctx context.Context, handler func(store.Record)) {
	for {
		select {
		case <-ctx.Done():
			return
		case rec := <-q.jobs:
			/*
				Both select cases can be ready at once (cancel raced a
				delivery), and select chooses pseudo-randomly between ready
				cases — so a cancelled worker must re-check before dispatching,
				or a buffered record could still reach the handler after
				shutdown. This inner non-blocking select is that priority
				rule: cancellation always wins over delivery.
			*/
			select {
			case <-ctx.Done():
				return
			default:
			}
			handler(rec)
		}
	}
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./... -race`
Expected: PASS across all packages.

- [ ] **Step 5: Full gate + commit**

Run: `gofmt -l .` (empty), `go vet ./...` (clean), `go test ./... -race` (green).

```bash
git add docs/versions/V3.md internal/queue/queue.go internal/queue/queue_test.go internal/api/server_test.go cmd/forge/main.go
git commit -m "feat(queue): fan out N workers in Run and join with WaitGroup"
```

---

### Task 2: Multi-worker shutdown join and N-worker backpressure

Pin the pool's shutdown and backpressure contracts with multiple workers.

**Files:**
- Modify: none (pins Task 1's `WaitGroup` + channel behavior)
- Test: `internal/queue/queue_test.go` (append)

**Interfaces:**
- Consumes: `queue.New`, `queue.Capacity`, `Queue.Enqueue`, `Queue.Run` from Task 1.

- [ ] **Step 1: Write the tests**

Append to `internal/queue/queue_test.go`:

```go
/*
TestRunFinishesAllCurrentWorkersAndDropsQueued pins Review Focus 2: when N
workers are mid-flight at cancellation, Run must wait for EVERY in-flight
worker to finish (not just the first one), while still dropping any record
left in the channel buffer.
*/
func TestRunFinishesAllCurrentWorkersAndDropsQueued(t *testing.T) {
	q := queue.New()
	ctx, cancel := context.WithCancel(context.Background())

	entered := make(chan string, 3)
	gate1 := make(chan struct{})
	gate2 := make(chan struct{})
	handled := make(chan string, 3)
	runDone := make(chan struct{})

	go func() {
		q.Run(ctx, 2, func(rec store.Record) {
			entered <- rec.ID
			switch rec.ID {
			case "in-flight-1":
				<-gate1
			case "in-flight-2":
				<-gate2
			}
			handled <- rec.ID
		})
		close(runDone)
	}()

	mustEnqueue(t, ctx, q, store.Record{ID: "in-flight-1"})
	mustEnqueue(t, ctx, q, store.Record{ID: "in-flight-2"})
	for i := 0; i < 2; i++ {
		select {
		case <-entered:
		case <-time.After(2 * time.Second):
			t.Fatalf("only %d of 2 workers picked up a record", i)
		}
	}
	mustEnqueue(t, ctx, q, store.Record{ID: "queued"})

	cancel()

	// Release worker 1 only — Run must still wait on worker 2.
	close(gate1)
	select {
	case <-runDone:
		t.Fatal("Run returned while worker 2 was still mid-flight")
	case <-time.After(50 * time.Millisecond):
	}

	// Release worker 2 — now wg.Wait() can complete.
	close(gate2)
	select {
	case <-runDone:
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return after both workers finished")
	}

	close(handled)
	var got []string
	for id := range handled {
		got = append(got, id)
	}
	if len(got) != 2 {
		t.Fatalf("handled %v, want 2 in-flight records only", got)
	}
	for _, id := range got {
		if id == "queued" {
			t.Fatalf("buffered record %q was delivered after cancel", id)
		}
	}
}

/*
TestEnqueueBackpressureMultipleWorkers pins that total in-system capacity
with W workers is W (in-flight) + Capacity (buffered), and freeing a single
worker slot unblocks a waiting Enqueue.
*/
func TestEnqueueBackpressureMultipleWorkers(t *testing.T) {
	const workers = 2
	q := queue.New()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	entered := make(chan struct{}, workers)
	gate := make(chan struct{})
	defer close(gate)
	go q.Run(ctx, workers, func(store.Record) {
		entered <- struct{}{}
		<-gate
	})

	for i := 0; i < workers; i++ {
		mustEnqueue(t, ctx, q, store.Record{ID: "held-" + strconv.Itoa(i)})
		select {
		case <-entered:
		case <-time.After(2 * time.Second):
			t.Fatalf("worker %d never took its record", i)
		}
	}
	for i := 0; i < queue.Capacity; i++ {
		mustEnqueue(t, ctx, q, store.Record{ID: strconv.Itoa(i)})
	}

	blocked := make(chan error, 1)
	go func() { blocked <- q.Enqueue(ctx, store.Record{ID: "overflow"}) }()
	select {
	case err := <-blocked:
		t.Fatalf("Enqueue returned %v with all workers busy and buffer full", err)
	case <-time.After(50 * time.Millisecond):
	}

	gate <- struct{}{} /** free one worker; it pulls from the buffer and opens one slot **/

	select {
	case err := <-blocked:
		if err != nil {
			t.Fatalf("unblocked Enqueue = %v, want nil", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Enqueue never completed after one worker freed a slot")
	}
}
```

- [ ] **Step 2: Run tests to verify they pass**

Run: `go test ./internal/queue/ -race -v`
Expected: PASS — pins the multi-worker `WaitGroup` join and N-worker backpressure.

- [ ] **Step 3: Full gate + commit**

Run: `gofmt -l .` (empty), `go vet ./...` (clean), `go test ./... -race` (green).

```bash
git add internal/queue/queue_test.go
git commit -m "test(queue): pin multi-worker shutdown join and N-worker backpressure"
```

---

### Task 3: Race-safe `newJobHandler` under concurrent workers

`N` workers now call the handler closure concurrently. `st.Save` is already serialized by `store.Store`'s mutex, but `fmt.Fprintf(stderr, ...)` on a `Save` failure is not — if two workers fail to save at once and `stderr` is an unsynchronized `io.Writer`, `-race` fires.

**Files:**
- Modify: `cmd/forge/main.go` (`newJobHandler`)
- Test: `cmd/forge/main_test.go` (append)

**Interfaces:**
- Consumes: `store.Store`, `executor.Runner`, `io.Writer`
- Produces: `func newJobHandler(st *store.Store, exec executor.Runner, stderr io.Writer) func(store.Record)` (same signature, now safe for concurrent calls).

- [ ] **Step 1: Write the failing test**

Append to `cmd/forge/main_test.go` (add `"fmt"` to the import block; `sync`, `bytes`, `os`, `path/filepath`, `strings` are already imported):

```go
/*
TestJobHandlerConcurrentSaveFailuresRaceFree pins Review Focus 3: when
multiple worker goroutines share one handler closure and st.Save fails
concurrently, their stderr writes must not race on the io.Writer.
*/
func TestJobHandlerConcurrentSaveFailuresRaceFree(t *testing.T) {
	blocker := filepath.Join(t.TempDir(), "not-a-dir")
	if err := os.WriteFile(blocker, []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	st := store.New(filepath.Join(blocker, "data"))
	fake := &fakeRunner{
		started: make(chan struct{}, 8),
		release: make(chan struct{}),
	}
	close(fake.release)

	var stderr bytes.Buffer
	h := newJobHandler(st, fake, &stderr)

	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		id := fmt.Sprintf("j-%d", i)
		go func() {
			defer wg.Done()
			h(store.Record{ID: id, Command: []string{"true"}, Status: job.StatusPending})
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

- [ ] **Step 2: Run test with `-race` to verify it fails**

Run: `go test ./cmd/forge/ -race -run TestJobHandlerConcurrentSaveFailuresRaceFree -v`
Expected: FAIL — `WARNING: DATA RACE` in `bytes.(*Buffer).Write` called from `fmt.Fprintf(stderr, ...)` in `newJobHandler`.

- [ ] **Step 3: Write minimal implementation**

Add `"sync"` to `cmd/forge/main.go`'s imports and guard the `stderr` writes:

```go
/*
newJobHandler builds the worker's per-record function: mark running,
execute through V0's Runner seam, persist the outcome. It is V1's honesty
rules moved off the request path — a failed run is data (status failed +
error message), never an exception.

A Save failure inside the worker has no client to answer and no logger
yet (V9); stderr is the most honest destination pre-observability Forge
has. Because V3 runs N workers against this same closure concurrently and
an arbitrary io.Writer is not goroutine-safe, errMu serializes the stderr
writes just as store.Store's mutex serializes the file writes.
*/
func newJobHandler(st *store.Store, exec executor.Runner, stderr io.Writer) func(store.Record) {
	var errMu sync.Mutex
	reportSaveErr := func(id string, err error) {
		errMu.Lock()
		defer errMu.Unlock()
		fmt.Fprintf(stderr, "forge: save job %s: %v\n", id, err)
	}
	return func(rec store.Record) {
		rec.Status = job.StatusRunning
		if err := st.Save(rec); err != nil {
			reportSaveErr(rec.ID, err)
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
		if err := st.Save(rec); err != nil {
			reportSaveErr(rec.ID, err)
		}
	}
}
```

- [ ] **Step 4: Run test with `-race` to verify it passes**

Run: `go test ./cmd/forge/ -race -run TestJobHandler -v`
Expected: PASS, zero race warnings.

- [ ] **Step 5: Full gate + commit**

Run: `gofmt -l .` (empty), `go vet ./...` (clean), `go test ./... -race` (green).

```bash
git add cmd/forge/main.go cmd/forge/main_test.go
git commit -m "fix(forge): guard concurrent worker stderr writes in newJobHandler"
```

---

### Task 4: `forge serve -workers` flag and concurrent e2e execution

Wire `-workers` (default `4`, minimum `1`) into `runServe` and pass `*workers` to `q.Run`. Prove flag validation and end-to-end concurrent execution.

**Files:**
- Modify: `cmd/forge/main.go` (`runServe`)
- Test: `cmd/forge/main_test.go` (append)

**Interfaces:**
- Consumes: `Queue.Run(ctx, workers, handler)` from Task 1; `newJobHandler` from Task 3.
- Produces: `forge serve -workers N` CLI flag (default 4; `< 1` exits 2).

- [ ] **Step 1: Write the failing tests**

Append to `cmd/forge/main_test.go`:

```go
/*
TestRunServeInvalidWorkersFlag pins Review Focus 4: a worker pool of 0 or
fewer goroutines would accept jobs and never run them, so runServe rejects
-workers < 1 at flag validation time with exit code 2 (usage error).
*/
func TestRunServeInvalidWorkersFlag(t *testing.T) {
	for _, val := range []string{"0", "-1", "not-a-number"} {
		t.Run(val, func(t *testing.T) {
			var stdout, stderr bytes.Buffer
			code := runServe(context.Background(), []string{"-workers", val}, &stdout, &stderr)
			if code != 2 {
				t.Errorf("runServe(-workers %s) = %d, want 2", val, code)
			}
			if stderr.Len() == 0 {
				t.Errorf("runServe(-workers %s) wrote nothing to stderr", val)
			}
		})
	}
}

/*
TestRunServeRunsJobsConcurrently proves V3's headline end to end: when a
slow job ("sleep 1") is already running on forge serve, a fast job
("echo fast") submitted behind it completes BEFORE the slow job finishes,
and shutdown still waits for the slow job to complete cleanly.
*/
func TestRunServeRunsJobsConcurrently(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	out := newLineWriter()
	code := make(chan int, 1)
	go func() {
		code <- runServe(ctx, []string{"-addr", "127.0.0.1:0", "-data", t.TempDir(), "-workers", "2"}, out, io.Discard)
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

	deadline := time.Now().Add(10 * time.Second)

	// 1. Submit the slow job and wait until a worker marks it running.
	slowResp, err := http.Post("http://"+addr+"/jobs", "application/json",
		strings.NewReader(`{"command": ["sleep", "1"]}`))
	if err != nil {
		t.Fatalf("POST slow failed: %v", err)
	}
	var slowRec store.Record
	if err := json.NewDecoder(slowResp.Body).Decode(&slowRec); err != nil {
		t.Fatalf("decode slow POST: %v", err)
	}
	slowResp.Body.Close()

	for {
		r, err := http.Get("http://" + addr + "/jobs/" + slowRec.ID)
		if err != nil {
			t.Fatalf("GET slow failed: %v", err)
		}
		var got store.Record
		_ = json.NewDecoder(r.Body).Decode(&got)
		r.Body.Close()
		if got.Status == job.StatusRunning {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("slow job never reached running; last status %v", got.Status)
		}
		time.Sleep(time.Millisecond) // poll interval; the deadline is the guarantee
	}

	// 2. Submit the fast job while worker 1 is busy with sleep 1.
	fastResp, err := http.Post("http://"+addr+"/jobs", "application/json",
		strings.NewReader(`{"command": ["echo", "fast"]}`))
	if err != nil {
		t.Fatalf("POST fast failed: %v", err)
	}
	var fastRec store.Record
	if err := json.NewDecoder(fastResp.Body).Decode(&fastRec); err != nil {
		t.Fatalf("decode fast POST: %v", err)
	}
	fastResp.Body.Close()

	for {
		r, err := http.Get("http://" + addr + "/jobs/" + fastRec.ID)
		if err != nil {
			t.Fatalf("GET fast failed: %v", err)
		}
		var got store.Record
		_ = json.NewDecoder(r.Body).Decode(&got)
		r.Body.Close()
		if got.Status == job.StatusSucceeded {
			if got.Result.Stdout != "fast\n" {
				t.Errorf("fast stdout = %q, want %q", got.Result.Stdout, "fast\n")
			}
			break
		}
		if got.Status == job.StatusFailed {
			t.Fatalf("fast job failed: %+v", got)
		}
		if time.Now().After(deadline) {
			t.Fatalf("fast job never finished; last status %v", got.Status)
		}
		time.Sleep(time.Millisecond) // poll interval; the deadline is the guarantee
	}

	// The slow job must STILL be running — proving the fast job overtook it.
	r, err := http.Get("http://" + addr + "/jobs/" + slowRec.ID)
	if err != nil {
		t.Fatalf("GET slow check failed: %v", err)
	}
	var slowNow store.Record
	_ = json.NewDecoder(r.Body).Decode(&slowNow)
	r.Body.Close()
	if slowNow.Status != job.StatusRunning {
		t.Errorf("slow job status = %v, want running while fast job already finished", slowNow.Status)
	}

	cancel()
	select {
	case c := <-code:
		if c != 0 {
			t.Errorf("runServe() = %d, want 0", c)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("runServe did not shut down after cancel")
	}
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test ./cmd/forge/ -run 'TestRunServeInvalidWorkersFlag|TestRunServeRunsJobsConcurrently' -v`
Expected: FAIL — `flag provided but not defined: -workers`.

- [ ] **Step 3: Write minimal implementation**

In `cmd/forge/main.go`'s `runServe`, add the flag, validate it, and pass it to `q.Run`:

```go
	workers := fs.Int("workers", 4, "number of worker goroutines")
	if err := fs.Parse(args); err != nil {
		return 2 // flag misuse is a usage error, like no-args in run
	}
	if *workers < 1 {
		fmt.Fprintln(stderr, "forge: -workers must be >= 1")
		return 2
	}
```

and:

```go
	workerDone := make(chan struct{})
	go func() {
		q.Run(ctx, *workers, newJobHandler(st, e, stderr))
		close(workerDone) // closing (not just returning) lets the shutdown path JOIN
	}()
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./cmd/forge/ -race -v`
Expected: PASS across all `cmd/forge` tests.

- [ ] **Step 5: Full gate + commit**

Run: `gofmt -l .` (empty), `go vet ./...` (clean), `go test ./... -race` (green).

```bash
git add cmd/forge/main.go cmd/forge/main_test.go
git commit -m "feat(forge): add -workers flag to serve and run N-worker pool"
```

---

### Task 5: Learning entry, README, tag v3

The version isn't done until `docs/LEARNING.md` has its entry, the README describes the worker pool, and the tag exists.

**Files:**
- Modify: `docs/LEARNING.md` (append)
- Modify: `README.md` (status, server quick start, "How it works (V3)", ladder)

**Interfaces:**
- Consumes: everything from Tasks 1–4, as shipped.

- [ ] **Step 1: Append the V3 entry to `docs/LEARNING.md`**

After the V2 entry:

```markdown
## V3 — Concurrent workers

- **Concept:**
  - Competing consumers on a single channel: spawning `N` goroutines that
    all receive from `q.jobs` turns a single worker into a worker pool with
    zero extra locking on the queue — the channel runtime hands each record
    to exactly one receiver.
  - `sync.WaitGroup` for fan-out/join: `wg.Add(1)` on the parent goroutine
    *before* `go func()`, `defer wg.Done()` inside the child, and
    `wg.Wait()` to block until the last in-flight worker finishes.
  - The race detector (`-race`) and shared collaborators: once `N` workers
    call the same handler closure simultaneously, every shared side effect
    (`store.Store` file updates, `stderr` error writes) must be synchronized.
  - Proving concurrency without `time.Sleep`: a channel barrier that only
    opens when all `N` workers have entered the handler at once.
- **What I built:** A `WaitGroup`-backed worker pool in `internal/queue`
  (`Queue.Run(ctx, workers, handler)`), race-safe `stderr` reporting in
  `newJobHandler`, and `-workers` (default 4) on `forge serve`.
- **What surprised me:** _(you)_
- **Idiom worth remembering:** _(you)_
- **Next time:** _(you)_
```

- [ ] **Step 2: Update `README.md`**

1. Status line → `**Status:** V0–V3 are done. V4–V11 are planned.`
2. Server quick start: add `-workers 4`, and note a pool of worker goroutines (`-workers`, default 4) drains the queue concurrently.
3. Add a `## How it works (V3)` section after V2 showing `N` workers reading from `queue` and joined by `sync.WaitGroup`.
4. Ladder table: the V3 row's Status cell `⏳` → `✅`.

- [ ] **Step 3: Full verification**

Run and paste results into the commit body:
```bash
go test ./... && go test ./... -race && go vet ./... && gofmt -l .
```

- [ ] **Step 4: Commit and tag**

```bash
git add docs/LEARNING.md README.md
git commit -m "docs(v3): learning entry, README describes the worker pool"
git tag v3
```

- [ ] **Step 5: Confirm the ladder**

Run: `git tag` shows `v0 v1 v2 v3`; `go test ./... -race` green (earlier versions' tests still pass as part of the full suite).
