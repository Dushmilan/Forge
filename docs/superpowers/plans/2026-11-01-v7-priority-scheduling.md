# V7 — Priority and Resource Scheduling Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Pending jobs run by priority instead of arrival order, and a job that declares a scarce resource waits until that resource is free — without ever running two holders at once.

**Architecture:** `internal/queue` stops being a channel and becomes a mutex-guarded `container/heap` with a one-slot wakeup channel and a `Capacity`-token semaphore for backpressure. `internal/resource` adds a per-name semaphore pool; `worker.Handle` acquires a claim after the budget check and releases it with `defer`. Priority and the resource claim persist on `store.Record` and are validated at admission in `api`.

**Tech Stack:** Go 1.22 standard library only (`container/heap`, `slices`, `sync`, `context`). Zero new dependencies.

**Spec:** `docs/versions/V7.md`

## Global Constraints

- **TDD strictly:** one failing test at a time; run it, watch it fail, implement the minimum, watch it pass. Never write implementation before the test that demands it; never refactor while red.
- **Standard library only.** No new module dependencies. Weighted resource units are explicitly out of scope.
- **No `time.Sleep` as synchronization.** Bounded windows are used only where an absence is asserted, matching the existing suite. The queue is proven with channels and gates.
- **Comment convention:** block comments (`/* … */`) explaining the Go concept, per AGENTS.md rule 4.
- **`PriorityNormal` must be the zero value.** A V6 file and a client that omits the field both mean `normal`; no special case anywhere.
- **`omitempty` compatibility:** a `normal`, resource-free record must serialize byte-identically to V6.
- **Interfaces are declared where consumed:** `api.Resources` and `worker.Acquirer`. Neither `resource` nor `queue` imports its consumer.
- **Field ownership:** `Save` never writes `Priority` or `Resources` on an existing record, exactly as it never writes `IdempotencyKey`.
- **Before every commit:** `gofmt -l .` empty, `go vet ./...` clean, `go test ./... -race` green.
- House test style: external test packages (`package queue_test`, `package resource_test`, `package api_test`) except for unexported types, which get `*_internal_test.go` in the package (see `internal/worker/backoff_internal_test.go`).

## File Structure

| File | Responsibility |
|---|---|
| `internal/job/job.go` | `Priority` type and its JSON/String, resource-claim shape validation |
| `internal/store/store.go` | `Record.Priority`, `Record.Resources`, ownership on save |
| `internal/queue/queue.go` | `Queue` over a heap: slots, notify, pop, worker loop |
| `internal/queue/heap.go` | `item`, `pendingHeap`, `heap.Interface` |
| `internal/resource/resource.go` | `Pool`: one semaphore per resource name |
| `internal/worker/worker.go` | `Acquirer` seam, acquire/release around a job |
| `internal/api/server.go` | body fields, admission checks, whole-intent replay |
| `cmd/forge/main.go` | `-resources` parsing and wiring |
| `*_test.go` alongside each | behavior for each layer, e2e in `cmd/forge` |
| `README.md`, `docs/LEARNING.md` | version docs |

## Review Focus

1. **A heap that is not stable silently breaks FIFO within a priority** → two `normal` jobs submitted in order must run in that order. Owned by Task 3 (heap `Less`) and Task 4 (behavioral test).
2. **A worker that drains the whole heap while its peers sleep** → after every pop, if items remain, the queue must re-signal. Owned by Task 4.
3. **A cancelled acquire leaking a token** → capacity is still configured but no job can ever hold it again. Owned by Task 5.
4. **Two jobs claiming `["gpu","db"]` and `["db","gpu"]`** → must not deadlock; a global acquisition order is the fix. Owned by Task 5.
5. **A `DELETE` while a job waits for a resource** → the job must end `cancelled` with `attempts` unchanged, not hang or report a failed attempt. Owned by Task 6.
6. **A replay changing priority under the same key** → one key names one intent. Owned by Task 7.
7. **A resource name nobody configured** → must be refused at admission (400), never block a worker forever. Owned by Task 7.

---

### Task 1: `job.Priority` and resource-claim shape

**Files:**
- Modify: `internal/job/job.go`
- Test: `internal/job/job_test.go`

**Interfaces:**
- Produces:
  - `type Priority int`
  - `job.PriorityLow` (-1), `job.PriorityNormal` (0), `job.PriorityHigh` (1), `job.PriorityCritical` (2)
  - `func (p Priority) String() string`
  - `func (p Priority) MarshalJSON() ([]byte, error)`
  - `func (p *Priority) UnmarshalJSON(data []byte) error`
  - `const MaxResourcesPerJob = 8`
  - `func ValidateResources(names []string) error` (wraps `ErrInvalidJob`)

- [ ] **Step 1: Write the failing tests**

Append to `internal/job/job_test.go`:

```go
/*
TestPriorityNormalIsZero pins the compatibility rule the whole version rests
on: the zero value must mean "normal", so an absent field and a V6 file both
decode without a special case.
*/
func TestPriorityNormalIsZero(t *testing.T) {
	var p job.Priority
	if p != job.PriorityNormal {
		t.Errorf("zero Priority = %q, want normal", p)
	}
}

/*
TestPriorityOrdering pins the direction container/heap will depend on: the
numeric order is low < normal < high < critical. Less compares these values.
*/
func TestPriorityOrdering(t *testing.T) {
	if !(job.PriorityLow < job.PriorityNormal &&
		job.PriorityNormal < job.PriorityHigh &&
		job.PriorityHigh < job.PriorityCritical) {
		t.Errorf("priority values are not ordered low < normal < high < critical")
	}
}

/*
TestPriorityJSONRoundTrip pins the wire form: readable names, and an unknown
name is an error rather than a silent "normal".
*/
func TestPriorityJSONRoundTrip(t *testing.T) {
	for _, tt := range []struct {
		p    job.Priority
		name string
	}{
		{job.PriorityLow, `"low"`},
		{job.PriorityNormal, `"normal"`},
		{job.PriorityHigh, `"high"`},
		{job.PriorityCritical, `"critical"`},
	} {
		b, err := json.Marshal(tt.p)
		if err != nil {
			t.Fatalf("Marshal(%v): %v", tt.p, err)
		}
		if string(b) != tt.name {
			t.Errorf("Marshal(%v) = %s, want %s", tt.p, b, tt.name)
		}
		var got job.Priority
		if err := json.Unmarshal(b, &got); err != nil {
			t.Fatalf("Unmarshal(%s): %v", b, err)
		}
		if got != tt.p {
			t.Errorf("round trip %s = %v, want %v", b, got, tt.p)
		}
	}
}

func TestPriorityUnknownNameFails(t *testing.T) {
	var p job.Priority
	if err := json.Unmarshal([]byte(`"urgent"`), &p); err == nil {
		t.Fatal("Unmarshal(urgent) = nil, want an error")
	}
}

/*
TestValidateResources pins the claim's shape — not its availability, which
belongs to the pool. Duplicates matter: ["gpu","gpu"] with capacity 1 would
wait for itself forever.
*/
func TestValidateResources(t *testing.T) {
	for _, tt := range []struct {
		name    string
		names   []string
		wantErr bool
	}{
		{"none", nil, false},
		{"one", []string{"gpu"}, false},
		{"two", []string{"gpu", "db"}, false},
		{"empty name", []string{""}, true},
		{"empty among good", []string{"gpu", ""}, true},
		{"duplicate", []string{"gpu", "gpu"}, true},
		{"too many", []string{"r1", "r2", "r3", "r4", "r5", "r6", "r7", "r8", "r9"}, true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			err := job.ValidateResources(tt.names)
			if tt.wantErr && !errors.Is(err, job.ErrInvalidJob) {
				t.Errorf("ValidateResources(%v) = %v, want an ErrInvalidJob", tt.names, err)
			}
			if !tt.wantErr && err != nil {
				t.Errorf("ValidateResources(%v) = %v, want nil", tt.names, err)
			}
		})
	}
}
```

- [ ] **Step 2: Run and watch them fail**

Run: `go test ./internal/job -run 'TestPriority|TestValidateResources' -v`
Expected: compile errors — `undefined: job.Priority`, `undefined: job.ValidateResources`.

- [ ] **Step 3: Implement**

In `internal/job/job.go`, next to `Status`:

```go
// Priority orders pending records. A named int for the same reason Status is
// one: the compiler keeps it from being confused with any other number.
type Priority int

/*
iota - 1 is the point of this block: Normal must be the zero value so that a
record written before V7, and a client that never mentions priority, both mean
"normal" with no special case. The other values fall out around it.
*/
const (
	PriorityLow      Priority = iota - 1 // -1
	PriorityNormal                       //  0
	PriorityHigh                         //  1
	PriorityCritical                     //  2
)

func (p Priority) String() string { /* low/normal/high/critical, default Priority(%d) */ }

// MarshalJSON renders the name, exactly like Status.MarshalJSON.
func (p Priority) MarshalJSON() ([]byte, error) { return json.Marshal(p.String()) }

// UnmarshalJSON accepts a known name. The pointer receiver is required so the
// decoded value can be written back through it.
func (p *Priority) UnmarshalJSON(data []byte) error { /* decode string, switch, error on unknown */ }

var _ fmt.Stringer = Priority(0)

// MaxResourcesPerJob bounds one claim. The store's file is the whole database.
const MaxResourcesPerJob = 8

/*
ValidateResources checks the SHAPE of a resource claim: non-empty names, no
duplicates, a bounded count. Availability is not job's business — the pool
answers that, and the API asks it. Duplicates are rejected because a job that
asks for one unit twice waits for itself.
*/
func ValidateResources(names []string) error
```

- [ ] **Step 4: Run the package**

Run: `go test ./internal/job -race`
Expected: PASS, including V0's tests.

- [ ] **Step 5: Commit**

```bash
git add internal/job
git commit -m "feat(job): add priority levels and resource-claim validation"
```

---

### Task 2: Persist scheduling intent

**Files:**
- Modify: `internal/store/store.go`
- Test: `internal/store/store_test.go`

**Interfaces:**
- Consumes: `job.Priority` (Task 1).
- Produces: `store.Record.Priority job.Priority` (`json:"priority,omitempty"`), `store.Record.Resources []string` (`json:"resources,omitempty"`); `Save` preserves both on an existing record.

- [ ] **Step 1: Write the failing tests**

```go
/*
TestRecordSchedulingFieldsRoundTrip pins durability: a claim survives the file,
because recovery has to requeue an interrupted gpu job with its claim intact.
*/
func TestRecordSchedulingFieldsRoundTrip(t *testing.T) {
	s := store.New(t.TempDir())
	if _, _, err := s.Create(store.Record{
		ID: "train", Command: []string{"true"}, Status: job.StatusPending,
		Priority: job.PriorityHigh, Resources: []string{"gpu"},
	}); err != nil {
		t.Fatal(err)
	}
	got, err := s.Get("train")
	if err != nil {
		t.Fatal(err)
	}
	if got.Priority != job.PriorityHigh || !slices.Equal(got.Resources, []string{"gpu"}) {
		t.Errorf("record = priority %v resources %v, want high [gpu]", got.Priority, got.Resources)
	}
}

/*
TestNormalRecordOmitsSchedulingFields pins Review Focus: a plain job must still
look exactly like a V6 job on disk, or every existing store gains churn on the
next save.
*/
func TestNormalRecordOmitsSchedulingFields(t *testing.T) {
	dir := t.TempDir()
	s := store.New(dir)
	if _, _, err := s.Create(store.Record{
		ID: "plain", Command: []string{"true"}, Status: job.StatusPending,
	}); err != nil {
		t.Fatal(err)
	}
	b, err := os.ReadFile(filepath.Join(dir, "jobs.json"))
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(b, []byte("priority")) || bytes.Contains(b, []byte("resources")) {
		t.Errorf("jobs.json = %s, want no priority/resources keys for a normal job", b)
	}
}

/*
TestOldFileLoadsAsNormal pins compatibility in the other direction: a file
written by V6 has no scheduling fields and must load unchanged.
*/
func TestOldFileLoadsAsNormal(t *testing.T) {
	dir := t.TempDir()
	body := `{"old": {"id": "old", "command": ["true"], "status": "pending"}}`
	if err := os.WriteFile(filepath.Join(dir, "jobs.json"), []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
	got, err := store.New(dir).Get("old")
	if err != nil {
		t.Fatal(err)
	}
	if got.Priority != job.PriorityNormal || len(got.Resources) != 0 {
		t.Errorf("old record = %v %v, want normal and no resources", got.Priority, got.Resources)
	}
}

/*
TestSavePreservesSchedulingFields pins field ownership: the worker saves whole
records, and a stale snapshot must not downgrade a high-priority gpu job.
*/
func TestSavePreservesSchedulingFields(t *testing.T) {
	s := store.New(t.TempDir())
	if _, _, err := s.Create(store.Record{
		ID: "train", Command: []string{"true"}, Status: job.StatusPending,
		Priority: job.PriorityCritical, Resources: []string{"gpu"},
	}); err != nil {
		t.Fatal(err)
	}
	if err := s.Save(store.Record{
		ID: "train", Command: []string{"true"}, Status: job.StatusRunning, Attempts: 1,
	}); err != nil {
		t.Fatal(err)
	}
	got, _ := s.Get("train")
	if got.Priority != job.PriorityCritical || !slices.Equal(got.Resources, []string{"gpu"}) {
		t.Errorf("after save = %v %v, want critical [gpu]", got.Priority, got.Resources)
	}
}

/*
TestUnknownPriorityInFileFails pins the loud-corruption rule: a hand-edited
"urgent" must not silently run at normal priority.
*/
func TestUnknownPriorityInFileFails(t *testing.T) {
	dir := t.TempDir()
	body := `{"a": {"id": "a", "command": ["true"], "status": "pending", "priority": "urgent"}}`
	if err := os.WriteFile(filepath.Join(dir, "jobs.json"), []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := store.New(dir).Get("a"); err == nil {
		t.Fatal("Get(corrupt priority) = nil error, want a decode error")
	}
}
```

- [ ] **Step 2: Run and watch them fail**

Run: `go test ./internal/store -run 'TestRecordScheduling|TestNormalRecord|TestOldFile|TestSavePreservesScheduling|TestUnknownPriority' -v`
Expected: FAIL — unknown fields `Priority`/`Resources`.

- [ ] **Step 3: Implement**

Add to `Record`, after `IdempotencyKey`:

```go
	/*
		Priority and Resources are admission-time intent, like the key. Both
		omitempty: a normal, resource-free job serializes exactly as it did in
		V6, so old files stay byte-identical.
	*/
	Priority  job.Priority `json:"priority,omitempty"`
	Resources []string     `json:"resources,omitempty"`
```

Add both to the ownership block in `insertLocked`, next to the heartbeat and
key:

```go
		r.Priority = cur.Priority
		r.Resources = cur.Resources
```

- [ ] **Step 4: Run the package**

Run: `go test ./internal/store -race`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/store
git commit -m "feat(store): persist priority and resource claims"
```

---

### Task 3: The priority heap

**Files:**
- Create: `internal/queue/heap.go`
- Test: `internal/queue/heap_internal_test.go` (`package queue`)

**Interfaces:**
- Consumes: `job.Priority` (Task 1).
- Produces (unexported, within `package queue`):
  - `type item struct { rec store.Record; seq uint64 }`
  - `type pendingHeap []item` implementing `heap.Interface`
  - `var _ heap.Interface = (*pendingHeap)(nil)`

- [ ] **Step 1: Write the failing tests**

`internal/queue/heap_internal_test.go`:

```go
/*
TestPendingHeapLess pins the policy in one place: higher priority is "less"
(container/heap is a min-heap), and equal priorities fall back to arrival
order. A heap is not stable, so the sequence is what makes FIFO true.
*/
func TestPendingHeapLess(t *testing.T) {
	h := pendingHeap{
		{rec: store.Record{Priority: job.PriorityLow}, seq: 1},
		{rec: store.Record{Priority: job.PriorityHigh}, seq: 2},
		{rec: store.Record{Priority: job.PriorityHigh}, seq: 3},
	}
	if !h.Less(1, 0) {
		t.Error("high should sort before low")
	}
	if !h.Less(1, 2) {
		t.Error("earlier sequence should sort before later at equal priority")
	}
	if h.Less(2, 1) {
		t.Error("later sequence must not sort before earlier")
	}
}

/*
TestPendingHeapPopsBestFirst pushes out of order and drains through
container/heap, proving the type satisfies the interface the functions expect.
*/
func TestPendingHeapPopsBestFirst(t *testing.T) {
	want := []string{"crit", "high-1", "high-2", "normal", "low"}
	h := &pendingHeap{}
	heap.Push(h, item{rec: store.Record{ID: "normal", Priority: job.PriorityNormal}, seq: 4})
	heap.Push(h, item{rec: store.Record{ID: "low", Priority: job.PriorityLow}, seq: 5})
	heap.Push(h, item{rec: store.Record{ID: "high-1", Priority: job.PriorityHigh}, seq: 2})
	heap.Push(h, item{rec: store.Record{ID: "crit", Priority: job.PriorityCritical}, seq: 1})
	heap.Push(h, item{rec: store.Record{ID: "high-2", Priority: job.PriorityHigh}, seq: 3})

	for _, id := range want {
		it := heap.Pop(h).(item)
		if it.rec.ID != id {
			t.Fatalf("popped %q, want %q", it.rec.ID, id)
		}
	}
	if h.Len() != 0 {
		t.Errorf("%d items left, want 0", h.Len())
	}
}
```

- [ ] **Step 2: Run and watch them fail**

Run: `go test ./internal/queue -run 'TestPendingHeap' -v`
Expected: compile errors — `undefined: pendingHeap`.

- [ ] **Step 3: Implement `internal/queue/heap.go`**

The signatures and bodies are pinned by `heap.Interface`; write:

```go
type item struct {
	rec store.Record
	seq uint64 // arrival order; the record cannot carry this
}

type pendingHeap []item

func (h pendingHeap) Len() int           { return len(h) }
func (h pendingHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h pendingHeap) Less(i, j int) bool { /* priority desc, then seq asc */ }

func (h *pendingHeap) Push(x any) { *h = append(*h, x.(item)) }

func (h *pendingHeap) Pop() any {
	old := *h
	n := len(old)
	it := old[n-1]
	old[n-1] = item{} // clear the slot: the convention that matters once items hold pointers
	*h = old[:n-1]
	return it
}

var _ heap.Interface = (*pendingHeap)(nil)
```

Add a doc comment above `Less` explaining that `container/heap` is a min-heap,
so "higher priority" must report itself as less.

- [ ] **Step 4: Run the package**

Run: `go test ./internal/queue -race`
Expected: PASS — the new tests pass and every V2/V3 test still passes, because
the old channel is still in use.

- [ ] **Step 5: Commit**

```bash
git add internal/queue
git commit -m "feat(queue): add a priority heap"
```

---

### Task 4: Order pending records by priority

**Files:**
- Modify: `internal/queue/queue.go` (whole file)
- Test: `internal/queue/queue_test.go`

**Interfaces:**
- Consumes: `pendingHeap` (Task 3).
- Produces: `queue.New() *Queue` and `(*Queue).Run(ctx, workers, handler)` keep
  their exact signatures; `Enqueue` keeps its signature and cancellation
  contract.

- [ ] **Step 1: Write the failing tests**

```go
/*
TestRunServesHighestPriorityFirst pins the headline. With one worker the
delivery order is the run order, so this is priority made observable.
*/
func TestRunServesHighestPriorityFirst(t *testing.T) {
	q := queue.New()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mustEnqueue(t, ctx, q, store.Record{ID: "low", Priority: job.PriorityLow})
	mustEnqueue(t, ctx, q, store.Record{ID: "normal", Priority: job.PriorityNormal})
	mustEnqueue(t, ctx, q, store.Record{ID: "high", Priority: job.PriorityHigh})

	order := make(chan string, 3)
	done := make(chan struct{})
	go func() {
		q.Run(ctx, 1, func(_ context.Context, r store.Record) { order <- r.ID })
		close(done)
	}()

	for _, want := range []string{"high", "normal", "low"} {
		select {
		case got := <-order:
			if got != want {
				t.Fatalf("ran %q, want %q", got, want)
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("never ran %q", want)
		}
	}
	cancel()
	<-done
}

/*
TestRunIsFIFOWithinPriority pins Review Focus 1: a heap is not stable, so
without the sequence tie-break equal priorities come out in heap shape order
and this test fails intermittently. Enqueue enough to make that likely.
*/
func TestRunIsFIFOWithinPriority(t *testing.T) {
	q := queue.New()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	const n = 20
	for i := 0; i < n; i++ {
		mustEnqueue(t, ctx, q, store.Record{ID: strconv.Itoa(i), Priority: job.PriorityNormal})
	}

	order := make(chan string, n)
	go q.Run(ctx, 1, func(_ context.Context, r store.Record) { order <- r.ID })

	for i := 0; i < n; i++ {
		select {
		case got := <-order:
			if got != strconv.Itoa(i) {
				t.Fatalf("ran %q at position %d, want %q", got, i, strconv.Itoa(i))
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("only %d records ran", i)
		}
	}
}

/*
TestPushWakesEveryIdleWorker pins Review Focus 2: with two records and two
workers, both handlers must be entered at once. A worker that drains the heap
alone while its peer sleeps fails the gate below.
*/
func TestPushWakesEveryIdleWorker(t *testing.T) {
	q := queue.New()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	gate := make(chan struct{})
	entered := make(chan string, 2)
	go q.Run(ctx, 2, func(ctx context.Context, r store.Record) {
		entered <- r.ID
		select {
		case <-gate:
		case <-ctx.Done():
		}
	})

	mustEnqueue(t, ctx, q, store.Record{ID: "a"})
	mustEnqueue(t, ctx, q, store.Record{ID: "b"})

	for i := 0; i < 2; i++ {
		select {
		case <-entered:
		case <-time.After(2 * time.Second):
			t.Fatalf("only %d worker(s) entered a handler; the second was never woken", i)
		}
	}
	close(gate)
}
```

- [ ] **Step 2: Run and watch them fail**

Run: `go test ./internal/queue -run 'TestRunServesHighest|TestRunIsFIFO|TestPushWakes' -v`
Expected: FAIL — `TestRunServesHighestPriorityFirst` sees `low` first (the
channel is FIFO).

- [ ] **Step 3: Rewrite `internal/queue/queue.go`**

Keep `Capacity` and the package doc; replace the internals:

```go
const Capacity = 64

type Queue struct {
	mu      sync.Mutex
	pending pendingHeap
	seq     uint64
	notify  chan struct{} // capacity 1: a coalescing "something was pushed"
	slots   chan struct{} // Capacity tokens: one per record allowed to be pending
}

func New() *Queue
func (q *Queue) Enqueue(ctx context.Context, rec store.Record) error
func (q *Queue) Run(ctx context.Context, workers int, handler func(context.Context, store.Record))
func (q *Queue) work(ctx context.Context, handler func(context.Context, store.Record))
func (q *Queue) pushLocked(rec store.Record)
func (q *Queue) pop() (store.Record, bool)
```

Behavior the tests pin, and the algorithm that satisfies it:

- `New` fills `slots` with `Capacity` tokens and makes `notify` with capacity 1.
- `Enqueue` keeps V2's explicit cancellation priority check **first**, then
  selects between a slot and `ctx.Done()`. On a slot: `pushLocked` under the
  mutex (increment `seq`, `heap.Push`), then a non-blocking send on `notify`.
  The returned error still wraps `ctx.Err()`.
- `pop` takes the mutex, returns `false` on an empty heap, otherwise
  `heap.Pop`s, puts a token back in `slots` (never blocks: this record held
  one), and — still under the lock — re-signals `notify` if items remain
  (non-blocking). That re-signal is Review Focus 2.
- `work` loops: `pop` and, if a record came back, re-check `ctx.Done()` before
  handing it over (V2's "cancellation wins over delivery" rule, unchanged),
  run the handler, then pop again. Only when the heap is empty does it block on
  `select { <-ctx.Done(): return; <-q.notify: }`.
- `Run` keeps its `WaitGroup` fan-out/join exactly as it is.

Update the `Capacity` comment: the fixed buffer is still a decision, but the
scheduling story V2 promised turned out to be ordering, not size.

- [ ] **Step 4: Run the package**

Run: `go test ./internal/queue -race -count=5`
Expected: PASS, including every V2/V3 test unchanged. `-count=5` because two of
these tests are the kind that pass once by luck.

- [ ] **Step 5: Commit**

```bash
git add internal/queue
git commit -m "feat(queue): order pending records by priority"
```

---

### Task 5: The resource pool

**Files:**
- Create: `internal/resource/resource.go`
- Test: `internal/resource/resource_test.go`

**Interfaces:**
- Produces:
  - `type Pool struct`
  - `func New(caps map[string]int) *Pool`
  - `func (p *Pool) Configured(name string) bool`
  - `func (p *Pool) Acquire(ctx context.Context, names []string) (release func(), err error)`
  - Unknown name error: `fmt.Errorf("resource: %q is not configured", name)`
  - `func (p *Pool) sortedUnique(names []string) ([]string, error)` (unexported helper, rejects duplicates)

- [ ] **Step 1: Write the failing tests**

```go
/*
TestAcquireRelease pins the semaphore: taking a unit makes it unavailable, and
the release closure returns it exactly once.
*/
func TestAcquireRelease(t *testing.T) {
	p := resource.New(map[string]int{"gpu": 1})
	release, err := p.Acquire(context.Background(), []string{"gpu"})
	if err != nil {
		t.Fatal(err)
	}
	blocked := make(chan struct{})
	go func() {
		defer close(blocked)
		r, err := p.Acquire(context.Background(), []string{"gpu"})
		if err == nil {
			r()
		}
	}()
	select {
	case <-blocked:
		t.Fatal("second acquire returned while the unit was held")
	case <-time.After(50 * time.Millisecond):
	}
	release()
	select {
	case <-blocked:
	case <-time.After(2 * time.Second):
		t.Fatal("release did not wake the waiter")
	}
}

/*
TestAcquireCancelledReturnsEveryHeldUnit pins Review Focus 3: after a partial
acquisition is abandoned, the units already taken must be free again. A leak
here leaves capacity configured and unreachable.
*/
func TestAcquireCancelledReturnsEveryHeldUnit(t *testing.T) {
	p := resource.New(map[string]int{"a": 1, "b": 1})
	hold, err := p.Acquire(context.Background(), []string{"b"})
	if err != nil {
		t.Fatal(err)
	}
	defer hold()

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() {
		// a is taken first (sorted order), then b blocks.
		_, err := p.Acquire(ctx, []string{"a", "b"})
		errCh <- err
	}()

	cancel()
	select {
	case err := <-errCh:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Acquire = %v, want context.Canceled", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("cancelled acquire did not return")
	}

	// a must be free again, or the next acquire blocks forever.
	ctx2, cancel2 := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel2()
	release, err := p.Acquire(ctx2, []string{"a"})
	if err != nil {
		t.Fatalf("a was not returned after cancellation: %v", err)
	}
	release()
}

/*
TestAcquireOrdersGlobally pins Review Focus 4: opposite name order must not
deadlock. Both goroutines take a then b because Acquire sorts, so one waits at
its first acquire instead of each holding one.
*/
func TestAcquireOrdersGlobally(t *testing.T) {
	p := resource.New(map[string]int{"a": 1, "b": 1})
	var wg sync.WaitGroup
	done := make(chan struct{})
	for _, names := range [][]string{{"a", "b"}, {"b", "a"}} {
		wg.Add(1)
		go func(names []string) {
			defer wg.Done()
			release, err := p.Acquire(context.Background(), names)
			if err != nil {
				t.Errorf("Acquire(%v): %v", names, err)
				return
			}
			release()
		}(names)
	}
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("acquiring opposite orders deadlocked")
	}
}

func TestAcquireRejectsUnknownAndDuplicate(t *testing.T) {
	p := resource.New(map[string]int{"gpu": 1})
	if _, err := p.Acquire(context.Background(), []string{"tpu"}); err == nil {
		t.Error("Acquire(tpu) = nil, want an error for an unconfigured name")
	}
	if _, err := p.Acquire(context.Background(), []string{"gpu", "gpu"}); err == nil {
		t.Error("Acquire(duplicate) = nil, want an error (it would wait for itself)")
	}
}

func TestAcquireEmptyIsNoop(t *testing.T) {
	p := resource.New(nil)
	release, err := p.Acquire(context.Background(), nil)
	if err != nil {
		t.Fatalf("Acquire(nil) = %v, want nil", err)
	}
	release() // must not panic or block
}

func TestConfigured(t *testing.T) {
	p := resource.New(map[string]int{"gpu": 2})
	if !p.Configured("gpu") {
		t.Error("Configured(gpu) = false, want true")
	}
	if p.Configured("tpu") {
		t.Error("Configured(tpu) = true, want false")
	}
}
```

- [ ] **Step 2: Run and watch them fail**

Run: `go test ./internal/resource -v`
Expected: compile errors — the package does not exist.

- [ ] **Step 3: Implement `internal/resource/resource.go`**

```go
// Package resource gates execution on scarce capacity: one counting
// semaphore per named resource.
package resource

type Pool struct {
	gates map[string]chan struct{}
}
```

- `New` builds a buffered channel per name, pre-filled with `n` tokens. The map
  is never written after construction, which is why `Pool` needs no mutex —
  immutability *is* the synchronization. Say that in the doc comment.
- `Configured` is a map lookup.
- `Acquire` clones and sorts the names (global order), rejects duplicates and
  unknown names **before taking anything**, then takes one token per name with
  `select { case <-gate: ...; case <-ctx.Done(): ... }`. On a context error it
  sends back every token taken so far and wraps `ctx.Err()`.
- The returned closure sends each held token back. Document that it must be
  called exactly once — `defer` gives that for free.

- [ ] **Step 4: Run the package**

Run: `go test ./internal/resource -race -count=3`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/resource
git commit -m "feat(resource): add a per-name semaphore pool"
```

---

### Task 6: Acquire around a job

**Files:**
- Modify: `internal/worker/worker.go`
- Test: `internal/worker/worker_test.go`

**Interfaces:**
- Consumes: nothing concrete — the seam is the interface.
- Produces:
  - `type Acquirer interface { Acquire(ctx context.Context, names []string) (release func(), err error) }`
  - `func New(st *store.Store, exec executor.Runner, cfg Config, acq Acquirer, stderr io.Writer) *Worker`
  - `*resource.Pool` satisfies `Acquirer` implicitly (Task 5).

- [ ] **Step 1: Write the failing tests**

Add a stub next to the existing test helpers:

```go
// stubAcquirer records the calls and can block until its context ends, which
// is how "cancelled while waiting for a resource" is tested without sleeping.
type stubAcquirer struct {
	mu       sync.Mutex
	calls    int
	held     bool
	err      error
	blockOn  string
	acquired chan struct{}
	release  chan struct{} // closed to let a blocked acquire proceed
}

func (a *stubAcquirer) Acquire(ctx context.Context, names []string) (func(), error) {
	a.mu.Lock()
	a.calls++
	a.mu.Unlock()
	if a.err != nil {
		return nil, a.err
	}
	if len(names) > 0 && names[0] == a.blockOn {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-a.release:
		}
	}
	a.mu.Lock()
	a.held = true
	a.mu.Unlock()
	return func() {
		a.mu.Lock()
		a.held = false
		a.mu.Unlock()
	}, nil
}

func (a *stubAcquirer) count() int { a.mu.Lock(); defer a.mu.Unlock(); return a.calls }
```

Tests (each drives `Handle` directly; reuse the existing store/executor helpers):

```go
/*
TestHandleHoldsResourcesWhileRunning pins the ordering the spec fixes: the
claim is taken before the attempt starts and released only after it ends.
*/
func TestHandleHoldsResourcesWhileRunning(t *testing.T)
// executor closure asserts acq.held is true when it runs;
// after Handle returns, assert acq.held is false.

/*
TestHandleAcquiresOnceAcrossRetries pins "three attempts are one job": the
claim is taken once, not once per attempt.
*/
func TestHandleAcquiresOnceAcrossRetries(t *testing.T)
// executor fails twice, succeeds on the third; expect count() == 1.

/*
TestHandleCancelledWhileWaiting pins Review Focus 5: a DELETE during the gate
ends the record cancelled with attempts still 0.
*/
func TestHandleCancelledWhileWaiting(t *testing.T)
// acq blocks; run Handle in a goroutine; w.Cancel(id); expect
// status cancelled, Attempts 0, and no executor call.

/*
TestHandleExhaustedNeverAcquires pins the budget check's position: a record
that cannot run must not take capacity.
*/
func TestHandleExhaustedNeverAcquires(t *testing.T)

/*
TestHandleUnknownResourceFails pins "terminal, not retried": an error from the
gate that is not cancellation cannot be fixed by trying again.
*/
func TestHandleUnknownResourceFails(t *testing.T)
// acq.err = errors.New(`resource: "tpu" is not configured`);
// expect failed after the FIRST attempt (executor never runs).
```

- [ ] **Step 2: Run and watch them fail**

Run: `go test ./internal/worker -run 'TestHandleHolds|TestHandleAcquiresOnce|TestHandleCancelledWhile|TestHandleExhaustedNever|TestHandleUnknown' -v`
Expected: compile errors — `New` takes four arguments, no `Acquirer`.

- [ ] **Step 3: Implement**

- Add the `Acquirer` interface above `Worker`, documented as the seam between a
  job and the capacity it needs, defined where it is consumed.
- Add `acq Acquirer` to `Worker`; take it as the fourth parameter of `New`.
- In `Handle`, after the `rec.Attempts >= w.cfg.MaxAttempts` check and before
  the loop:

```go
	/*
		The claim is taken once per job, not once per attempt: three tries at
		one gpu job are one claim on the gpu. Acquiring under execCtx means a
		DELETE interrupts a job that is waiting, and a job that waits past its
		deadline times out rather than starting late.
	*/
	release, err := w.acq.Acquire(execCtx, rec.Resources)
	if err != nil {
		/*
			A context error means the wait was cancelled or timed out; anything
			else (e.g. a resource this process does not configure) cannot be
			fixed by another attempt.
		*/
		if execCtx.Err() != nil {
			w.finishCancelled(rec, execCtx.Err())
			return
		}
		rec.Status = job.StatusFailed
		rec.Err = err.Error()
		w.save(rec)
		return
	}
	defer release() // V4's cleanup ledger now covers capacity too
```

- Update every call site of `worker.New` in the package's tests to pass a stub
  (or `resource.New(nil)` where a test does not care) and in `cmd/forge/main.go`
  for now; Task 8 wires the real pool.

- [ ] **Step 4: Run the package**

Run: `go test ./internal/worker -race`
Expected: PASS, including V4/V5's retry, heartbeat, and cancellation tests.

- [ ] **Step 5: Commit**

```bash
git add internal/worker
git commit -m "feat(worker): acquire resources around an attempt"
```

---

### Task 7: Admit priority and resource claims

**Files:**
- Modify: `internal/api/server.go`
- Test: `internal/api/server_test.go`

**Interfaces:**
- Consumes: `job.ValidateResources` (Task 1), `store.Record.Priority` /
  `Resources` (Task 2).
- Produces:
  - `type Resources interface { Configured(name string) bool }`
  - `func New(ctx context.Context, q *queue.Queue, st *store.Store, c Canceller, res Resources) *Server`
  - `POST /jobs` accepts `"priority"` (string) and `"resources"` (`[]string`).

- [ ] **Step 1: Write the failing tests**

In the api test helper, construct the server with
`resource.New(map[string]int{"gpu": 1})` (add a second helper if a test needs
no resources).

```go
/*
TestPostJobEchoesSchedulingIntent pins the happy path: both fields survive
decode, admission, and the response, and they are persisted.
*/
func TestPostJobEchoesSchedulingIntent(t *testing.T)
// POST {"command":["true"],"priority":"high","resources":["gpu"]}
// expect 202, body priority high and resources [gpu], and the stored record equal.

/*
TestPostJobNormalOmitsPriorityOnTheWire pins the compatibility rule at the API
boundary: an omitted priority is normal and the response has no priority key.
*/
func TestPostJobNormalOmitsPriorityOnTheWire(t *testing.T)
// assert !bytes.Contains(body, []byte(`"priority"`))

/*
TestPostJobRejectsUnrunnableClaims pins Review Focus 7 plus the admission
table: unknown resource, duplicate name, too many names, unknown priority —
each 400, each storing nothing.
*/
func TestPostJobRejectsUnrunnableClaims(t *testing.T)
// table:
// `{"command":["true"],"resources":["tpu"]}`                 -> 400
// `{"command":["true"],"resources":["gpu","gpu"]}`           -> 400
// `{"command":["true"],"resources":["r1"..."r9"]}`           -> 400
// `{"command":["true"],"priority":"urgent"}`                 -> 400
// for each: len(store.List()) == 0

/*
TestPostJobReplayComparesWholeIntent pins Review Focus 6: a key names one
intent, and priority is part of it.
*/
func TestPostJobReplayComparesWholeIntent(t *testing.T)
// first: high + gpu -> 202
// second: same key + same command + priority low -> 409, one stored record
// third:  same key + same command + priority high (no resources is a change) -> 409
// fourth: same key, identical body -> 200 with the original id
```

- [ ] **Step 2: Run and watch them fail**

Run: `go test ./internal/api -run 'TestPostJobEchoes|TestPostJobNormal|TestPostJobRejects|TestPostJobReplayCompares' -v`
Expected: FAIL — the extra fields are ignored, unknown claims still get 202.

- [ ] **Step 3: Implement**

- Add the `Resources` interface and a `res` field; add the parameter to `New`.
- Decode into a named struct so the replay comparison has one type:

```go
type submitRequest struct {
	Command   []string      `json:"command"`
	Priority  job.Priority  `json:"priority"`
	Resources []string      `json:"resources"`
}
```

- After the trailing-data check, before generating an id: call
  `job.ValidateResources(in.Resources)` and, for each name,
  `s.res.Configured(name)`; both failures are 400 naming the offending value.
- Sort the claim (`slices.Sort(in.Resources)`) before storing, so
  `["gpu","db"]` and `["db","gpu"]` name the same intent and the stored record
  is canonical.
- Put `Priority` and `Resources` on the `store.Record` built for `Create`.
- Replace the replay comparison with a `sameIntent(stored store.Record, in submitRequest) bool`
  helper comparing `Command`, `Priority`, and `Resources` (all via
  `slices.Equal` / `==`). The 409 message now names the intent, not just the
  command.
- Update `newServer` helpers and `cmd/forge/main.go`'s construction (pass
  `resource.New(nil)` there for now; Task 8 replaces it).

- [ ] **Step 4: Run the package**

Run: `go test ./internal/api -race`
Expected: PASS, including V1–V6 API tests.

- [ ] **Step 5: Commit**

```bash
git add internal/api
git commit -m "feat(api): admit priority and resource claims"
```

---

### Task 8: `-resources` on `forge serve`

**Files:**
- Modify: `cmd/forge/main.go`
- Test: `cmd/forge/main_test.go`

**Interfaces:**
- Consumes: `resource.New` (Task 5), `worker.New` (Task 6), `api.New` (Task 7).
- Produces: `func parseResources(spec string) (map[string]int, error)`.

- [ ] **Step 1: Write the failing tests**

```go
func TestParseResources(t *testing.T) {
	for _, tt := range []struct {
		spec    string
		want    map[string]int
		wantErr bool
	}{
		{"", nil, false},
		{"gpu=1", map[string]int{"gpu": 1}, false},
		{"gpu=2,db=4", map[string]int{"gpu": 2, "db": 4}, false},
		{"gpu", nil, true},        // no '='
		{"gpu=x", nil, true},      // not a number
		{"gpu=0", nil, true},      // zero capacity is unreachable capacity
		{"gpu=-1", nil, true},     // negative is nonsense
		{"gpu=1,gpu=2", nil, true}, // duplicate name
		{"=1", nil, true},         // empty name
		{",", nil, true},          // empty entries
	} {
		t.Run(tt.spec, func(t *testing.T) {
			got, err := parseResources(tt.spec)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("parseResources(%q) = %v, want an error", tt.spec, got)
				}
				return
			}
			if err != nil {
				t.Fatalf("parseResources(%q): %v", tt.spec, err)
			}
			if !maps.Equal(got, tt.want) {
				t.Errorf("parseResources(%q) = %v, want %v", tt.spec, got, tt.want)
			}
		})
	}
}

/*
TestRunServeInvalidResourcesExits2 pins the CLI contract: a bad capacity spec is
a usage error, not a server that starts with no limits.
*/
func TestRunServeInvalidResourcesExits2(t *testing.T)
// run([]string{"serve", "-resources", "gpu=0"}, io.Discard, io.Discard) == 2
```

- [ ] **Step 2: Run and watch them fail**

Run: `go test ./cmd/forge -run 'TestParseResources|TestRunServeInvalidResources' -v`
Expected: compile error — `undefined: parseResources`.

- [ ] **Step 3: Implement**

- `parseResources` splits on `,`, each entry on `=` (exactly two parts), parses
  an int with `strconv.Atoi`, and rejects empty names, non-positive values, and
  repeated names. Return `nil` for the empty spec so `resource.New(nil)` is the
  no-resources case.
- Add `resources := fs.String("resources", "", "resource capacities, e.g. gpu=2,db=4")`
  and validate it after `fs.Parse`, printing the error and returning 2.
- Wire the pool once: `pool := resource.New(caps)`, then pass it to
  `worker.New(...)` and `api.New(...)`. Both already accept it.

- [ ] **Step 4: Run the package**

Run: `go test ./cmd/forge -race`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add cmd/forge
git commit -m "feat(forge): parse -resources capacities"
```

---

### Task 9: End to end

**Files:**
- Test: `cmd/forge/main_test.go`

- [ ] **Step 1: Write the failing tests**

```go
/*
TestRunServePriorityBeatsArrival pins the headline end to end. A blocker job
holds the only worker while two more are submitted; when the blocker is
released, priority decides the rest. The gate file, not a sleep, is what makes
the ordering deterministic.
*/
func TestRunServePriorityBeatsArrival(t *testing.T) {
	dir := t.TempDir()
	gate := filepath.Join(dir, "gate")
	log := filepath.Join(dir, "order")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	addr, code := serveAddrData(t, ctx, t.TempDir(), "-workers", "1", "-job-timeout", "10s")

	blocker := fmt.Sprintf(`{"command":["sh","-c","while [ ! -f %s ]; do sleep 0.01; done; echo blocker >> %s"]}`, gate, log)
	low := fmt.Sprintf(`{"command":["sh","-c","echo low >> %s"]}`, log)
	high := fmt.Sprintf(`{"command":["sh","-c","echo high >> %s"]}`, log)

	// Fill the worker first so both others are still pending when priority matters.
	blockerRec := postJob(t, addr, blocker)
	waitStatus(t, addr, blockerRec.ID, job.StatusRunning)
	postPriority(t, addr, low, "low")
	postPriority(t, addr, high, "high")

	if err := os.WriteFile(gate, nil, 0o644); err != nil {
		t.Fatal(err)
	}
	waitStatus(t, addr, blockerRec.ID, job.StatusSucceeded)
	// wait for the queue to drain, then read the order
	waitFor(t, func() bool { return countLines(t, log) == 3 })
	if got := readLines(t, log); !slices.Equal(got, []string{"blocker", "high", "low"}) {
		t.Errorf("run order = %v, want blocker, high, low", got)
	}
	cancel()
	<-code
}

/*
TestRunServeResourceCapacitySerializes pins the semaphore end to end: two gpu
jobs with capacity 1 must not interleave their start/end markers.
*/
func TestRunServeResourceCapacitySerializes(t *testing.T) {
	dir := t.TempDir()
	log := filepath.Join(dir, "log")
	body := fmt.Sprintf(`{"command":["sh","-c","echo start >> %s; sleep 0.1; echo end >> %s"],"resources":["gpu"]}`, log, log)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	addr, code := serveAddrData(t, ctx, t.TempDir(), "-workers", "2", "-resources", "gpu=1", "-job-timeout", "10s")

	a := postJob(t, addr, body)
	b := postJob(t, addr, body)
	waitStatus(t, addr, a.ID, job.StatusSucceeded)
	waitStatus(t, addr, b.ID, job.StatusSucceeded)

	if got := readLines(t, log); !slices.Equal(got, []string{"start", "end", "start", "end"}) {
		t.Errorf("log = %v, want start,end,start,end (no overlap)", got)
	}
	cancel()
	<-code
}

/*
TestRunServeRejectsUnknownResource pins the admission table at the process
boundary: no capacity configured, so the claim never becomes a job.
*/
func TestRunServeRejectsUnknownResource(t *testing.T)
// serve with no -resources, POST resources:["gpu"] -> 400, store empty.
```

Add the small helpers the tests need (`postPriority`, `countLines`,
`readLines`, `waitFor`); `waitFor` polls with a `time.After` deadline and is
the only place a sleep-adjacent construct appears.

- [ ] **Step 2: Run and watch them fail**

Run: `go test ./cmd/forge -run 'TestRunServePriority|TestRunServeResourceCapacity|TestRunServeRejectsUnknown' -v`
Expected: FAIL — priority is ignored (low runs first) and both gpu jobs overlap.

- [ ] **Step 3: Implement**

No production change should be needed if Tasks 4–8 are correct. Fix the owning
layer, not the test.

- [ ] **Step 4: Run the whole suite, repeatedly**

```bash
go test ./... -race -count=3
```

Expected: PASS three times — the ordering tests are the ones that fail
intermittently if the sequence tie-break or the re-signal is wrong.

- [ ] **Step 5: Commit**

```bash
git add cmd/forge
git commit -m "test(forge): pin priority order and resource serialization end to end"
```

---

### Task 10: Docs and tag

**Files:**
- Modify: `README.md`
- Modify: `docs/LEARNING.md`

- [ ] **Step 1: README**

- Status line → `**Status:** V0–V7 are done, tagged `v0`–`v7`.` followed by the
  next rung (V8 — sandboxed execution).
- Server example gains `-resources gpu=2,db=4`.
- Add one `curl` showing the body fields:

```sh
curl -X POST localhost:8080/jobs \
  -d '{"command": ["train"], "priority": "high", "resources": ["gpu"]}'
```

- Add a `## How it works (V7)` section: the channel-to-heap replacement, the
  `Less` policy (priority, then sequence), the notify channel, the `slots`
  semaphore that preserves backpressure, the resource pool and why acquisition
  is ordered, and the known limit (a waiting job holds a worker).
- Ladder row for V7 → ✅.

- [ ] **Step 2: LEARNING.md**

Add a `## V7 — Priority and resource scheduling` entry in the established
format:

- **Concept:** `container/heap` is an interface you implement; a min-heap is
  turned into a priority queue by inverting `Less`; an unstable heap needs a
  sequence tie-break for FIFO; a buffered channel is a counting semaphore;
  consistent acquisition order prevents circular wait; interfaces declared
  where consumed.
- **What I built:** `pendingHeap`, the heap-backed `Queue` with `slots` and
  `notify`, `job.Priority`, `internal/resource`, the `worker.Acquirer` gate,
  the admission checks, and `-resources`.
- **What surprised me:** _(you)_
- **Idiom worth remembering:** _(you)_
- **Next time:** _(you)_

- [ ] **Step 3: Full verification**

```bash
gofmt -l .
go vet ./...
go test ./... -race -count=1
```

- [ ] **Step 4: Commit**

```bash
git add README.md docs/LEARNING.md
git commit -m "docs(v7): learning entry and README describe scheduling"
```

- [ ] **Step 5: Tag after review**

Do not tag yet. Tag `v7` only after the whole-branch review is clean.

---

## Self-Review

- **Spec coverage:** `job.Priority` + `ValidateResources` (T1), persisted
  fields + ownership (T2), `heap.Interface` (T3), heap queue + notify + slots +
  backpressure (T4), pool + ordering + partial release (T5), `Acquirer` gate +
  cancel-while-waiting + terminal unknown (T6), admission table + whole-intent
  replay + normalization (T7), `-resources` (T8), e2e priority and capacity
  (T9), docs/tag (T10).
- **Placeholders:** none. Reflection lines in LEARNING are deliberately the
  learner's, matching V0–V6.
- **Type consistency:** `Priority`, `MaxResourcesPerJob`, `ValidateResources`,
  `PendingHeap`/`item`, `Queue.slots`/`notify`/`pop`/`pushLocked`,
  `resource.New`/`Configured`/`Acquire`, `worker.Acquirer`, `api.Resources`,
  `submitRequest`/`sameIntent`, `parseResources` — one spelling each across
  tasks.
- **Review Focus:** FIFO within priority (T3/T4), worker drain starvation (T4),
  leaked token on cancel (T5), lock-order deadlock (T5), cancel during the gate
  (T6), replay intent (T7), unconfigured resource (T7/T9).
