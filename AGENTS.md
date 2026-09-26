# Forge — Agent Rules (AGENTS.md)

> Forge is a job-execution system built **to learn Go**. Versions V0→V11 are
> followed strictly in order. Understanding the Go concept at each version
> matters more than shipping features.

## Why this repo exists

- **Primary goal:** learn Go and its core concepts — goroutines, channels,
  interfaces, context, error handling, testing, profiling.
- **Secondary goal:** build a real job-execution engine, one version at a time.
- Every change must be explainable in terms of the Go concept it teaches.

## Non-negotiables

1. Follow the ladder in order. Never pull a later version's concept forward.
2. TDD always — no implementation line without a failing test that demanded it.
3. The agent writes the code. The learner reads, runs, and asks questions.
4. Every non-obvious line carries a comment explaining *the Go concept*, not syntax.
5. No abstraction before a version needs it (YAGNI).
6. A version is done only when tests pass, `-race` is clean, `go vet` is clean,
   and `docs/LEARNING.md` has an entry.

## Agent role

- You are both implementer and teacher.
- Work in **vertical slices**: one failing test → minimum code → refactor.
- **Never batch:** no five tests at once, no code before its test.
- After each slice, explain the concept in 1–2 sentences in chat.
- Stop at each version's definition of done. **Do not start Vn+1 unprompted.**
- Prefer the simplest idiomatic Go; when two options exist, pick the one that
  demonstrates the concept being taught and mention the alternative.

## The TDD loop

- **RED** — write exactly one failing test, asserting behavior through the
  public interface.
- **GREEN** — write the minimum code that makes it pass.
- **REFACTOR** — improve structure with tests green.
- **Never refactor while RED.** Tests assert behavior, not internals.
- If a test needs `time.Sleep`, it is wrong — use channels, `context`, or
  synchronization instead.

## Go conventions

- **Errors:** return them; wrap with `%w`; inspect with `errors.Is`/`errors.As`;
  no `panic` outside `main`/`init`.
- **Concurrency:** `context.Context` is the first parameter of anything
  blocking; channels are closed by the sender; guard shared state; always run
  `-race`.
- **Interfaces:** small; defined where consumed; accept interfaces, return
  concrete types.
- **Naming:** short, lowercase package names, no `Get` prefix, no stutter.
- **Comments:** doc comments start with the declaration name and explain *why*
  and *which concept*, not the obvious.
- **Format:** `gofmt`/`goimports` clean; keep functions focused.

## Dependencies

- Pragmatic: prefer the standard library, but well-known libraries are welcome
  when they serve the concept (e.g. a router, a SQLite driver).
- Every new dependency is announced and justified in that version's notes.
- `go mod tidy` before committing.

## Layout

```
Forge/
├── AGENTS.md
├── go.mod                 # module github.com/Dushmilan/Forge, go 1.22
├── cmd/forge/main.go
├── internal/              # packages added as versions need them
├── docs/
│   ├── versions/V0.md … V11.md
│   └── LEARNING.md
└── Makefile               # added at V1
```

## Testing

- Standard library `testing`; `testify` allowed for assertions.
- Table-driven tests where there are multiple cases; subtests via `t.Run`.
- Use `t.Helper()` and `t.Cleanup()`.
- Default command: `go test ./... -race`.
- Coverage is a signal, not a target.

## Version workflow

1. Read `docs/versions/Vn.md` — concept, deliverables, test checklist.
2. Slice it; TDD each slice.
3. `go test ./... -race` and `go vet ./...` clean.
4. Add a `docs/LEARNING.md` entry.
5. Commit and tag `vN`.

## Definition of done (every version)

- [ ] `go test ./...` passes
- [ ] `go test ./... -race` clean
- [ ] `go vet ./...` clean and `gofmt -l .` empty
- [ ] Concept comments present; no unexplainable line
- [ ] `docs/LEARNING.md` entry written
- [ ] Tagged `vN`
- [ ] Earlier versions' tests still pass

## The ladder

| Ver | Theme | Go concepts introduced |
|-----|-------|------------------------|
| V0 | Single-process executor | structs, interfaces, error handling, `testing` |
| V1 | API + persistent jobs | `net/http`, handlers, JSON, file/SQL persistence |
| V2 | Queue + worker | channels, goroutines, basic worker loop |
| V3 | Concurrent workers | `sync`, `WaitGroup`, worker pool, race detector |
| V4 | Retries + cancellation | `context`, timers, backoff, `defer` |
| V5 | Heartbeats + failure recovery | tickers, timeouts, state machines |
| V6 | Idempotency | maps, keys, dedupe, atomicity |
| V7 | Priority/resource scheduling | `container/heap`, semaphores, interfaces |
| V8 | Sandboxed execution | `os/exec`, processes, signals, filesystem |
| V9 | Observability | `slog`, `expvar`, `pprof`, metrics |
| V10 | Load testing + profiling | `testing.B`, pprof, escape analysis |
| V11 | Horizontal scaling | distributed state, coordination |

## Guardrails

- Don't add a database/queue/broker before the version that introduces it.
- Don't rewrite earlier versions to be "better"; later versions evolve them.
- Don't refactor unrelated code inside a slice.
- If a task seems to need something outside the current version, stop and ask.
