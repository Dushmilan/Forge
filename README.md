# Forge

A job-execution engine built one Go version at a time — learning the language
by building it. Each version (V0 → V11) introduces one set of core Go
concepts: structs and interfaces, then HTTP, channels, `sync`, `context`,
`container/heap`, `os/exec`, `slog`, pprof, and beyond.

**Status:** V0 (single-process executor) is done. V1–V11 are planned.

## Quick start

Requires Go 1.22+.

```sh
go build ./cmd/forge
./forge echo hello        # -> hello, exit 0
./forge sh -c 'exit 3'    # -> error on stderr, exit 3
```

`forge` propagates the executed command's own exit code, so it composes like
any other Unix tool. Called with no arguments it prints usage and exits 2.

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

## The ladder

| Ver | Theme                          | Go concepts introduced                          | Status |
|-----|--------------------------------|--------------------------------------------------|--------|
| V0  | Single-process executor        | structs, interfaces, error handling, `testing`   | ✅     |
| V1  | API + persistent jobs          | `net/http`, handlers, JSON, file/SQL persistence | ⏳     |
| V2  | Queue + worker                 | channels, goroutines, basic worker loop          | ⏳     |
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
├── cmd/forge/main.go      # CLI front end
├── internal/
│   ├── job/               # Job, Status, validation
│   └── executor/          # synchronous command execution
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
