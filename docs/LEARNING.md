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
