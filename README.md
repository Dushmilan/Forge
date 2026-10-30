# Forge

A job-execution engine built one Go version at a time — learning the language
by building it. Each version (V0 → V11) introduces one set of core Go
concepts: structs and interfaces, then HTTP, channels, `sync`, `context`,
`container/heap`, `os/exec`, `slog`, pprof, and beyond.

**Status:** V0–V5 are done. V6–V11 are planned.

## Quick start

Requires Go 1.22+.

```sh
go build ./cmd/forge
./forge echo hello        # -> hello, exit 0
./forge sh -c 'exit 3'    # -> error on stderr, exit 3
```

`forge` propagates the executed command's own exit code, so it composes like
any other Unix tool. Called with no arguments it prints usage and exits 2.

### As a server (V1)

```sh
go build ./cmd/forge
./forge serve -addr :8080 -data ./data -workers 4 \
  -max-attempts 3 -retry-backoff 100ms -job-timeout 30s \
  -heartbeat-interval 1s
```

```sh
curl -X POST localhost:8080/jobs -d '{"command": ["echo", "hello"]}'   # 202 + pending record
curl localhost:8080/jobs/<id>                                          # 200 + record (poll for final status)
curl -X DELETE localhost:8080/jobs/<id>                                # 202: cancel queued or running
```

Jobs run asynchronously (V2): the POST returns 202 with a `pending` record
as soon as it is queued, and `GET` shows the record moving
`pending → running → succeeded|failed|cancelled` as a worker runs it. As of V3
a pool of worker goroutines (`-workers`, default 4) drains the queue
concurrently, so a slow job no longer blocks the ones behind it. As of V4 a
failed attempt is retried with exponential backoff up to `-max-attempts`, every
job runs under `-job-timeout`, and `DELETE` cancels a queued or running job.
As of V5 a running job refreshes `last_heartbeat` every `-heartbeat-interval`,
and a restart repairs what the previous process left behind: interrupted jobs
are requeued (spending the attempt they had started), leftover pending jobs run
again, and a job that has exhausted its budget is failed instead of
crash-looping. Recovery is at-least-once — a job interrupted after its side
effect can run again; V6 is the idempotency story.
Every submitted job is persisted to `<data>/jobs.json`.

Run the tests:

```sh
go test ./... -race
```

## How it works (V0)

V0 is deliberately single-process and synchronous — no concurrency, no
persistence, no HTTP. Data flows one way:

```
args ──▶ cmd/forge ──▶ job.Job ──▶ executor.Executor ──▶ Result + error
         (builds a        (validates     (runs it via os/exec,
          Job, prints      its fields)    captures stdout/exit)
          the result)
```

- [`internal/job`](internal/job) — the `Job` struct, a `Status` enum, and
  validation that returns wrapped errors (`fmt.Errorf` with `%w`).
- [`internal/executor`](internal/executor) — `Executor.Run` executes a job's
  command with `os/exec`, distinguishing "failed to start" from "exited
  non-zero".
- [`cmd/forge`](cmd/forge) — a testable CLI entrypoint: `main` only calls
  `os.Exit`, everything else lives in `run(args, stdout, stderr)`.

## How it works (V1)

`forge serve` adds an HTTP face and a memory. `POST /jobs` decodes the
command, runs it **synchronously** (the request blocks — there is no queue
yet, and V2's whole point is that this stops being true), persists a
`Record`, and answers 201; `GET /jobs/{id}` reads it back:

```
POST /jobs ──▶ api.Server ──▶ executor.Runner ──▶ store.Save ──▶ 201 + Record
GET /jobs/{id} ──────────────▶ store.Get ──────────────────────▶ 200 / 404
```

- [`internal/store`](internal/store) — one JSON file, atomic writes
  (temp file + `os.Rename`), a mutex because `net/http` serves each
  request on its own goroutine.
- [`internal/api`](internal/api) — handlers on Go 1.22's method+pattern
  mux; errors are always JSON (`{"error": "…"}`).

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

## How it works (V3)

V3 turns that one worker into a pool. `-workers N` starts `N` goroutines
that all receive from the same channel: a Go channel hands each record to
exactly one receiver, so the workers compete for jobs with no extra lock.
`sync.WaitGroup` makes `Run` block until the last in-flight worker finishes,
so Ctrl-C still lets running jobs complete:

```
POST /jobs ──▶ api.Server ──▶ queue.Enqueue ──▶ ┌─ worker 1 ─┐
                                               ├─ worker 2 ─┤──▶ handler
                                               └─ worker N ─┘
                 queue.Run(ctx, workers, handler) ──▶ wg.Wait() on shutdown
```

- [`internal/queue`](internal/queue) — `Run(ctx, workers, handler)` fans out
  `N` goroutines over the channel and joins them with a `sync.WaitGroup`.
- [`cmd/forge`](cmd/forge) — `serve -workers N` (default 4); the job handler
  serializes its `stderr` error writes so concurrent workers stay `-race`
  clean (the handler itself moved to `internal/worker` in V4).

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

## How it works (V5)

V5 gives the process a memory. The store is the source of truth across a
restart, so `forge serve` starts by reconciling it with reality:

```
                 startup
                    │
        recovery.Recover(store)
                    │
   pending ─────────┼──▶ requeue
   running, budget ─┼──▶ pending + interruption note ──▶ requeue
   running, spent ──┼──▶ failed
   terminal ────────┴──▶ untouched
                    │
              start worker pool, then enqueue the recovered records
```

- [`internal/recovery`](internal/recovery) — the startup table above. A
  `running` record is an orphan by definition: this process just started and
  is the only one allowed to run jobs. Attempts are persisted, so a crash
  costs one attempt and a job that kills the process ends `failed` instead of
  looping forever.
- [`internal/store`](internal/store) — `LastHeartbeat *time.Time` (nil means
  "never ran"; JSON omits it), `Heartbeat(id, at)` as the single writer, and
  `List()` for a deterministic snapshot. `Save` preserves a stored heartbeat,
  so a worker's whole-record save can never rewind liveness.
- [`internal/worker`](internal/worker) — a `time.Ticker` goroutine refreshes
  liveness while an attempt runs; it is stopped and joined before the final
  save, so no tick can land after the terminal status.
- [`internal/job`](internal/job) — `Status.CanTransition` is an explicit
  transition table, and `store.Save` rejects anything it does not allow,
  including the recovery-only `running → pending` edge.

## The ladder

| Ver | Theme                          | Go concepts introduced                          | Status |
|-----|--------------------------------|--------------------------------------------------|--------|
| V0  | Single-process executor        | structs, interfaces, error handling, `testing`   | ✅     |
| V1  | API + persistent jobs          | `net/http`, handlers, JSON, file/SQL persistence | ✅     |
| V2  | Queue + worker                 | channels, goroutines, basic worker loop          | ✅     |
| V3  | Concurrent workers             | `sync`, `WaitGroup`, worker pool, race detector  | ✅     |
| V4  | Retries + cancellation         | `context`, timers, backoff, `defer`              | ✅     |
| V5  | Heartbeats + failure recovery  | tickers, timeouts, state machines                | ✅     |
| V6  | Idempotency                    | maps, keys, dedupe, atomicity                    | ⏳     |
| V7  | Priority/resource scheduling   | `container/heap`, semaphores, interfaces         | ⏳     |
| V8  | Sandboxed execution            | `os/exec`, processes, signals, filesystem        | ⏳     |
| V9  | Observability                  | `slog`, `expvar`, `pprof`, metrics               | ⏳     |
| V10 | Load testing + profiling       | `testing.B`, pprof, escape analysis              | ⏳     |
| V11 | Horizontal scaling             | distributed state, coordination                  | ⏳     |

Versions are built strictly in order — no concept is pulled forward from a
later rung.

## Project layout

```
Forge/
├── AGENTS.md              # rules for how this repo is built (TDD, the ladder)
├── go.mod                 # module github.com/Dushmilan/Forge
├── cmd/forge/main.go      # CLI + `forge serve` front end
├── internal/
│   ├── job/               # Job, Status, validation, JSON marshaling
│   ├── executor/          # synchronous command execution
│   ├── store/             # JSON file persistence (V1)
│   ├── queue/             # channel + worker loop (V2)
│   ├── worker/            # retries, backoff, timeouts, cancellation (V4)
│   ├── recovery/          # startup reconciliation of orphaned records (V5)
│   └── api/               # HTTP handlers (V1, async since V2)
└── docs/
    ├── LEARNING.md        # one entry per version: what Go concept clicked
    └── versions/V0.md …   # spec for each version
```

## Learn more

- [`docs/LEARNING.md`](docs/LEARNING.md) — the learning log: what each version
  taught, in the author's words.
- [`docs/versions/`](docs/versions) — the spec for each version (concept,
  deliverables, test checklist, out of scope).
- [`AGENTS.md`](AGENTS.md) — the development process: vertical slices, TDD,
  and the definition of done for every version.
