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

## V5 — Heartbeats + failure recovery

- **Concept:**
  - `time.Ticker` is only half a heartbeat: the goroutine that owns the
    ticker must also be stopped and **joined** before the record's final
    save. Stopping without waiting leaves one more tick that can land after
    the terminal status; the join is what makes "the last write wins" true.
  - Persisted time is a data-model decision. `*time.Time` distinguishes
    "never ran" (nil, omitted from JSON) from the zero instant, which
    `time.Time` would silently serialize as `0001-01-01T00:00:00Z` on every
    pending job.
  - Field ownership beats one big lock: the worker writes whole records and
    the ticker writes one timestamp, so `Save` preserves a stored
    `LastHeartbeat` instead of letting a stale snapshot rewind it. Two
    writers, two fields, one mutex — no lost update.
  - A state machine is more useful as a table than as prose:
    `Status.CanTransition` makes the lifecycle executable, and enforcing it
    inside `store.Save`'s locked read-modify-write makes an illegal move
    fail instead of corrupting the file.
  - Startup reconciliation is its own operation: a `running` record at boot
    is an orphan by definition, because this process owns the store. The
    interrupted attempt was already counted, so recovery resumes the budget
    rather than resetting it — a crash costs one attempt, not the whole job.
- **What I built:** the `Status.CanTransition` table and
  `job.ErrInvalidTransition`; `Record.LastHeartbeat`, `store.Heartbeat`, and
  `store.List`; a heartbeat ticker joined before every outcome save; attempt
  resumption in `worker.Handle`; the `internal/recovery` startup pass; and
  `-heartbeat-interval` on `forge serve`.
- **What surprised me:** _(you)_
- **Idiom worth remembering:** _(you)_
- **Next time:** _(you)_

## V6 — Idempotency

- **Concept:**
  - A `map` is a *derived index*, not necessarily stored state. The key → id
    lookup is rebuilt from the durable records on every admission, inside the
    lock that already serializes writes. Storing the index would add a second
    structure that can disagree with the records; deriving it means one file,
    one atomic rename, and no migration.
  - Check-and-insert is atomic only if the check and the write share the same
    critical section. `Create` holds `s.mu` across read → index → insert, so
    two concurrent retries cannot both observe "key absent" and both write.
    A try-lock or a retry loop would be a workaround for a lock held too
    narrowly.
  - `omitempty` is a compatibility tool: a field that is absent from the JSON
    is absent from every file written before this version, so old stores load
    unchanged.
  - "Exactly-once" is a claim about a boundary, not a system. V6 makes
    *creation* at-most-once per key; V5 keeps *execution* at-least-once.
    Saying which boundary a guarantee covers is part of the design.
  - A retry must never re-enqueue. The server cannot distinguish "never
    queued" from "queued, response lost", so re-enqueueing trades a stall for
    a double run. Recovery — not retry — is what repairs a lost enqueue.
  - Validation must precede admission. Otherwise a malformed submission binds
    its key to nothing, and the client's corrected retry is told its job
    already exists.
- **What I built:** `Record.IdempotencyKey`, `store.Create` with a derived
  key index and one shared `insertLocked`, the `Idempotency-Key` contract on
  `POST /jobs` (202 new / 200 replay / 409 mismatch / 400 oversize), and
  end-to-end replay tests that count the command's side effect rather than
  trusting its status.
- **What surprised me:** _(you)_
- **Idiom worth remembering:** _(you)_
- **Next time:** _(you)_
