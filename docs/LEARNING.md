# Learning Log

One entry per version. The point is to record what *I* learned about Go, not
what the code does. Write it after the version's tests are green.

Template:

```
## Vn — <theme>
- **Concept:**
- **What I built:**
- **What surprised me:**
- **Idiom worth remembering:**
- **Next time:**
```

---

## V0 — Single-process executor

- **Concept:**
  - Named types + `iota` enums, and `fmt.Stringer` (interfaces are satisfied
    *implicitly* — no `implements` keyword).
  - Errors as values: sentinels (`errors.New`), wrapping with `%w`, and
    inspecting with `errors.Is` (sentinels) vs `errors.As` (concrete types).
  - `os/exec` for real subprocesses; distinguishing a start failure from a
    non-zero exit.
  - Implicit interfaces again with `io.Writer`, which is what makes `main`
    testable.
- **What I built:** `internal/job` (Job, Status, Validate), `internal/executor`
  (Executor, Result, Runner seam), and a testable `cmd/forge` entrypoint that
  prints usage, runs one command, and propagates its exit code.
- **What surprised me:** _(you)_
- **Idiom worth remembering:** _(you)_
- **Next time:** _(you)_

## V1 — API + persistent jobs

- **Concept:**
  - `net/http` handlers and the Go 1.22 method+pattern mux
    (`"GET /jobs/{id}"`, `r.PathValue`) — routing without a dependency.
  - `encoding/json` round trips: `json.Marshaler`/`json.Unmarshaler` on a
    named type, pointer receivers for decode, and why errors become strings
    to cross a JSON boundary.
  - File persistence: read-modify-write under a mutex, atomic write via
    temp file + `os.Rename`, `fs.ErrNotExist` as "empty, not broken".
  - `crypto/rand` for server-generated IDs; `httptest` and `t.TempDir()`
    for tests that never touch the network or the real disk.
  - `context` + `signal.NotifyContext` to shut a server down without
    sleeping — channels as the synchronization primitive.
- **What I built:** `internal/store` (JSON file store), `internal/api`
  (POST /jobs runs synchronously and persists; GET /jobs/{id} reads it
  back), and a `forge serve` subcommand that hosts them until interrupted.
- **What surprised me:** _(you)_
- **Idiom worth remembering:** _(you)_
- **Next time:** _(you)_

## V2 — Queue + worker

- **Concept:**
  - A buffered `chan` IS a thread-safe FIFO queue — the "queue + mutex +
    condition variable" reflex is the pre-Go design; the channel replaces
    all three.
  - The worker loop: `for { select { case <-ctx.Done(): return; case rec := <-ch: handle(rec) } }` —
    one goroutine, two wakeup reasons, no polling.
  - Blocking channel sends as backpressure, and putting the send *inside*
    a `select` with `ctx.Done()` so an enqueue can't deadlock shutdown.
  - `select` chooses pseudo-randomly between simultaneously-ready cases —
    so "cancellation wins over delivery" needs an explicit re-check, it is
    not the default.
  - Testing goroutines honestly: gates (channels the handler waits on) and
    deadlines instead of `time.Sleep`.
- **What I built:** `internal/queue` (channel + single-goroutine loop), an
  async API (`POST /jobs` answers 202 with a pending record; `GET` observes
  `pending → running → final` live), and `forge serve` wiring one context
  through both server and worker.
- **What surprised me:** _(you)_
- **Idiom worth remembering:** _(you)_
- **Next time:** _(you)_

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
- **What I built:** context-aware `executor.Runner`; a `worker` package with
  retries, exponential backoff, timeouts, and cancel-by-id; `StatusCancelled`;
  `DELETE /jobs/{id}`; `-max-attempts`, `-retry-backoff`, `-job-timeout`.
- **What surprised me:** _(you)_
- **Idiom worth remembering:** _(you)_
- **Next time:** _(you)_
