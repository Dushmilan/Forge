# Forge

A job-execution engine built one Go version at a time — learning the language
by building it. Each version (V0 → V11) introduces one set of core Go
concepts: structs and interfaces, then HTTP, channels, `sync`, `context`,
`container/heap`, `os/exec`, `slog`, pprof, and beyond.

**Status:** V0–V2 are done. V3–V11 are planned.

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
./forge serve -addr :8080 -data ./data
```

```sh
curl -X POST localhost:8080/jobs -d '{"command": ["echo", "hello"]}'   # 202 + pending record
curl localhost:8080/jobs/<id>                                          # 200 + record (poll for final status)
```

Jobs run asynchronously (V2): the POST returns 202 with a `pending` record
as soon as it is queued, one worker goroutine runs it, and `GET` shows the
record moving `pending → running → succeeded|failed`. Every submitted job
is persisted to `<data>/jobs.json`.

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

## The ladder

| Ver | Theme                          | Go concepts introduced                          | Status |
|-----|--------------------------------|--------------------------------------------------|--------|
| V0  | Single-process executor        | structs, interfaces, error handling, `testing`   | ✅     |
| V1  | API + persistent jobs          | `net/http`, handlers, JSON, file/SQL persistence | ✅     |
| V2  | Queue + worker                 | channels, goroutines, basic worker loop          | ✅     |
| V3  | Concurrent workers             | `sync`, `WaitGroup`, worker pool, race detector  | ⏳     |
| V4  | Retries + cancellation         | `context`, timers, backoff, `defer`              | ⏳     |
| V5  | Heartbeats + failure recovery  | tickers, timeouts, state machines                | ⏳     |
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
