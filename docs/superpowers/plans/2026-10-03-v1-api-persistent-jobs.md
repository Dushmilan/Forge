# V1 — API + Persistent Jobs Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Give Forge an HTTP API that runs submitted jobs synchronously and persists the outcome to a JSON file, plus a `forge serve` subcommand to host it.

**Architecture:** Two new packages — `internal/store` (one JSON file, mutex-guarded, atomic rename) and `internal/api` (handlers on Go 1.22's pattern-based `net/http` mux) — wired together by a new `serve` branch in `cmd/forge`. V0's `internal/job` and `internal/executor` are consumed unchanged through the existing `Runner` seam; V1 adds a JSON-marshaling pair to `job.Status` so persisted status reads as `"succeeded"`, not `2`.

**Tech Stack:** Go 1.22 standard library only (`net/http`, `encoding/json`, `crypto/rand`, `flag`, `sync`, `httptest`, `os/exec` via V0). Zero new dependencies.

**Spec:** `docs/versions/V1.md`

## Global Constraints

- **TDD strictly:** one failing test at a time; run it, watch it fail, implement minimum, watch it pass. Never write implementation before the test that demands it; never refactor while red.
- **Standard library only.** No new module dependencies (spec: "Zero new dependencies").
- **No `time.Sleep` in tests.** Synchronize with channels, `context`, `httptest` responses, or `sync.WaitGroup`. A `select` with `time.After` is allowed only as a *deadline* on a channel that must fire, never as a substitute for waiting.
- **Comments teach the Go concept.** Every non-obvious line explains *why* in Go terms; doc comments start with the declaration name.
- **V0 packages are extended, not rewritten.** `internal/executor` and its `Runner` interface are untouched; `internal/job` only gains `MarshalJSON`/`UnmarshalJSON` on `Status`.
- **Execution stays synchronous in the handler.** No goroutines for job execution (that is V2's story). The only goroutine in V1 bridges `context` → `http.Server.Shutdown`.
- **Working-tree pre-flight:** `internal/job/job.go` carries an uncommitted learner edit (the `// starts at 0` comment on `StatusPending`). Task 1 commits that file — the edit rides along; do not revert it. `README.md` is untracked and gets committed in Task 6.
- **Before every commit:** `gofmt -l .` empty, `go vet ./...` clean, `go test ./... -race` green.
- House test style: external test packages (`package store_test`, `package job_test`, `package api_test`…); `main` tests stay in `package main`.

## Review Focus

Spec-implied inputs no single deliverable line calls out, most likely to bite first:

1. **Malformed JSON request body** (`not json`, truncated) → must answer **400** with a JSON `{"error":…}` body, not a panic or 500. Owned by Task 4.
2. **Command whose binary doesn't exist** → an *outcome*, not a transport error: **201**, `status: "failed"`, error text persisted. Not 400, not 500. Owned by Task 4.
3. **Wrong method on a known path** (`DELETE /jobs/x`) and **unknown path** (`/nope`) → Go 1.22 mux answers 405/404; pin that so nobody "fixes" it into a 200. Owned by Task 3.
4. **Concurrent requests** (POST while GETs) → `-race` must stay silent; this is why the store mutex exists. Owned by Task 4 (API level) and Task 2 (store level).
5. **Corrupt/truncated `jobs.json`** (crash mid-write) → `Get`/`Save` return an error; no panic, no silent data loss by overwriting. Owned by Task 2.

---

### Task 1: Status JSON serialization

`job.Status` currently marshals as an int. A persisted job that reads
`"status": 2` is opaque, and V1's API promises human-readable outcomes. This
task makes `Status` round-trip through its string name.

**Files:**
- Modify: `internal/job/job.go` (add two methods after `String()`)
- Test: `internal/job/job_test.go` (append; file already exists, `package job_test`)

**Interfaces:**
- Consumes: `Status`, `StatusPending/Running/Succeeded/Failed`, `String()` — all already in `internal/job`.
- Produces: `func (s Status) MarshalJSON() ([]byte, error)` and `func (s *Status) UnmarshalJSON(data []byte) error`. Task 2's `store.Record` relies on both to persist `Status` as a JSON string.

- [ ] **Step 1: Write the failing test**

Append to `internal/job/job_test.go`:

```go
// TestStatusJSONRoundTrip pins Status to its string name across JSON, so a
// persisted job reads "succeeded" instead of the opaque iota 2. It also pins
// that only known names decode — a typo in a stored file must be an error,
// not a silent Status(0).
func TestStatusJSONRoundTrip(t *testing.T) {
	tests := []struct {
		name   string
		status job.Status
		want   string
	}{
		{name: "pending", status: job.StatusPending, want: `"pending"`},
		{name: "running", status: job.StatusRunning, want: `"running"`},
		{name: "succeeded", status: job.StatusSucceeded, want: `"succeeded"`},
		{name: "failed", status: job.StatusFailed, want: `"failed"`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b, err := json.Marshal(tt.status)
			if err != nil {
				t.Fatalf("Marshal(%v) error: %v", tt.status, err)
			}
			if string(b) != tt.want {
				t.Errorf("Marshal(%v) = %s, want %s", tt.status, b, tt.want)
			}

			var back job.Status
			if err := json.Unmarshal(b, &back); err != nil {
				t.Fatalf("Unmarshal(%s) error: %v", b, err)
			}
			if back != tt.status {
				t.Errorf("round trip = %v, want %v", back, tt.status)
			}
		})
	}
}

// TestStatusUnmarshalRejectsUnknown pins that decoding is strict: an
// unrecognized name (and a bare number, which would decode against the old
// int representation) is an error, not a zero-value Status.
func TestStatusUnmarshalRejectsUnknown(t *testing.T) {
	for _, in := range []string{`"weird"`, `2`} {
		var s job.Status
		if err := json.Unmarshal([]byte(in), &s); err == nil {
			t.Errorf("Unmarshal(%s) = nil error, want error", in)
		}
	}
}
```

Add `"encoding/json"` to the file's import block alongside `"testing"`.

- [ ] **Step 2: Run the tests to verify they fail**

Run: `go test ./internal/job/ -run 'StatusJSON|StatusUnmarshal' -v`
Expected: FAIL — `json: unsupported type: job.Status` from `Marshal` (no `MarshalJSON` yet).

- [ ] **Step 3: Write the minimal implementation**

Append to `internal/job/job.go` (after `String()`), keeping the existing learner edit intact:

```go
// MarshalJSON renders a Status as its String() name ("succeeded", not 2).
// This is json.Marshaler: encoding/json checks for this method before the
// default int encoding, so the wire format changes without touching any
// caller. Files and API responses stay readable years later.
func (s Status) MarshalJSON() ([]byte, error) {
	return json.Marshal(s.String())
}

// UnmarshalJSON decodes a Status from its string name. The pointer receiver
// is required: encoding/json only calls UnmarshalJSON on an addressable
// value, and this method must write back through *s. It returns an error for
// unknown names rather than leaving the zero value — a corrupt stored file
// should fail loudly, never load as "pending" by accident.
func (s *Status) UnmarshalJSON(data []byte) error {
	var name string
	if err := json.Unmarshal(data, &name); err != nil {
		return fmt.Errorf("job: status must be a string: %w", err)
	}
	switch name {
	case "pending":
		*s = StatusPending
	case "running":
		*s = StatusRunning
	case "succeeded":
		*s = StatusSucceeded
	case "failed":
		*s = StatusFailed
	default:
		return fmt.Errorf("job: unknown status %q", name)
	}
	return nil
}
```

Add `"encoding/json"` to `job.go`'s imports.

- [ ] **Step 4: Run the tests to verify they pass**

Run: `go test ./internal/job/ -race`
Expected: PASS (all of `job_test.go`, old and new).

- [ ] **Step 5: Format, vet, commit**

```bash
gofmt -l .          # expect empty
go vet ./...
go test ./... -race # whole suite still green — includes the learner's "// starts at 0" edit
git add internal/job/job.go internal/job/job_test.go
git commit -m "feat(job): marshal Status as its string name"
```

---

### Task 2: JSON file store

**Files:**
- Create: `internal/store/store.go`
- Test: `internal/store/store_test.go`

**Interfaces:**
- Consumes: `job.Status` JSON methods (Task 1), `executor.Result`, `job.Status`.
- Produces (Task 3/4 depend on these exact names):
  - `var ErrNotFound = errors.New("store: not found")`
  - `type Record struct { ID string; Command []string; Status job.Status; Result executor.Result; Err string }` with JSON tags `id`, `command`, `status`, `result`, `error` (`result` no omitempty — structs never omit), `error` with `omitempty`.
  - `func New(dir string) *Store`
  - `func (s *Store) Save(r Record) error`
  - `func (s *Store) Get(id string) (Record, error)`

- [ ] **Step 1 (cycle A): Write the failing round-trip test**

Create `internal/store/store_test.go`:

```go
package store_test

import (
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/Dushmilan/Forge/internal/executor"
	"github.com/Dushmilan/Forge/internal/job"
	"github.com/Dushmilan/Forge/internal/store"
)

// TestSaveGetRoundTrip pins the store's whole reason to exist: what Save
// writes, a *fresh* Store instance (newly constructed, as after a process
// restart) reads back unchanged. The nested directory that does not exist yet
// pins that Save creates it — production starts with no ./data.
func TestSaveGetRoundTrip(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "nested", "data")
	s := store.New(dir)

	want := store.Record{
		ID:      "abc123",
		Command: []string{"echo", "hi"},
		Status:  job.StatusSucceeded,
		Result:  executor.Result{ExitCode: 0, Stdout: "hi\n"},
	}
	if err := s.Save(want); err != nil {
		t.Fatalf("Save() error: %v", err)
	}

	got, err := store.New(dir).Get("abc123")
	if err != nil {
		t.Fatalf("Get() error: %v", err)
	}
	if got.ID != want.ID || got.Status != want.Status || got.Result.Stdout != want.Result.Stdout {
		t.Errorf("Get() = %+v, want %+v", got, want)
	}

	// The file on disk must name the status as a string — this is what
	// consumes Task 1's MarshalJSON and pins the storage format.
	b, err := os.ReadFile(filepath.Join(dir, "jobs.json"))
	if err != nil {
		t.Fatalf("read jobs.json: %v", err)
	}
	if !bytes.Contains(b, []byte(`"status": "succeeded"`)) {
		t.Errorf("jobs.json = %s, want status as string", b)
	}
}
```

Add `"bytes"` to the import block.

- [ ] **Step 2 (cycle A): Run to verify it fails**

Run: `go test ./internal/store/`
Expected: FAIL to compile — `no required module provides package github.com/Dushmilan/Forge/internal/store`.

- [ ] **Step 3 (cycle A): Write the minimal implementation**

Create `internal/store/store.go`:

```go
// Package store persists job records in a single JSON file.
//
// V1 storage is deliberately dumb: one file, one mutex, atomic renames.
// Databases and concurrent-writer stories belong to later versions — the
// ladder pulls them in when a real need does.
package store

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sync"

	"github.com/Dushmilan/Forge/internal/executor"
	"github.com/Dushmilan/Forge/internal/job"
)

// ErrNotFound is the sentinel callers test with errors.Is. Wrapping it with
// the id keeps the message human-readable while preserving identity — the
// same %w idiom V0's job.ErrInvalidJob uses.
var ErrNotFound = errors.New("store: not found")

// Record is a Job's persisted life story: what was asked for, where it ended
// up, and what came back. Err is a string (not error) because it must
// survive JSON: errors don't round-trip, their messages do.
type Record struct {
	ID      string          `json:"id"`
	Command []string        `json:"command"`
	Status  job.Status      `json:"status"`
	Result  executor.Result `json:"result,omitempty"`
	Err     string          `json:"error,omitempty"`
}

// Store reads and writes <dir>/jobs.json. The zero construction is New;
// the mutex makes the read-modify-write cycle atomic across net/http's
// per-request goroutines (see Save).
type Store struct {
	mu  sync.Mutex
	dir string
}

// New returns a Store rooted at dir. It does not touch the disk yet — a
// missing directory is created on first Save, and a missing file reads as
// an empty store, so constructing a Store can never fail.
func New(dir string) *Store {
	return &Store{dir: dir}
}

// path is the one file this Store owns.
func (s *Store) path() string {
	return filepath.Join(s.dir, "jobs.json")
}

// load reads the current map. A missing file is not an error: it means
// nothing has been saved yet, so an empty map is the honest answer. A
// *corrupt* file IS an error — overwriting data we cannot read would lose
// it silently.
func (s *Store) load() (map[string]Record, error) {
	b, err := os.ReadFile(s.path())
	if errors.Is(err, fs.ErrNotExist) {
		return map[string]Record{}, nil
	}
	if err != nil {
		return nil, fmt.Errorf("store: read %s: %w", s.path(), err)
	}
	var m map[string]Record
	if err := json.Unmarshal(b, &m); err != nil {
		return nil, fmt.Errorf("store: decode %s: %w", s.path(), err)
	}
	return m, nil
}

// Save merges r into the file.
//
// The mutex serializes the whole read-modify-write: net/http runs each
// request on its own goroutine, so two concurrent Saves would each read the
// old map and the loser's record would vanish. This is data-race
// correctness, not an early taste of V3's worker concurrency.
//
// The write is atomic on POSIX: marshal to jobs.json.tmp, then rename over
// the destination. A crash mid-write leaves the old file intact instead of
// a half-written one.
func (s *Store) Save(r Record) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := os.MkdirAll(s.dir, 0o755); err != nil {
		return fmt.Errorf("store: create %s: %w", s.dir, err)
	}
	m, err := s.load()
	if err != nil {
		return err
	}
	m[r.ID] = r

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

// Get returns the record for id, or a wrapped ErrNotFound.
func (s *Store) Get(id string) (Record, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	m, err := s.load()
	if err != nil {
		return Record{}, err
	}
	r, ok := m[id]
	if !ok {
		return Record{}, fmt.Errorf("store: job %q: %w", id, ErrNotFound)
	}
	return r, nil
}
```

- [ ] **Step 4 (cycle A): Run to verify it passes**

Run: `go test ./internal/store/ -v`
Expected: PASS `TestSaveGetRoundTrip`.

- [ ] **Step 5 (cycle B): Write the failing not-found test**

Append to `store_test.go`:

```go
// TestGetMissing wraps the sentinel: callers distinguish "no such job"
// (404 later) from "the disk is on fire" (500) with errors.Is, so the
// identity must survive the message formatting.
func TestGetMissing(t *testing.T) {
	s := store.New(t.TempDir())

	_, err := s.Get("ghost")
	if !errors.Is(err, store.ErrNotFound) {
		t.Errorf("Get(missing) = %v, want ErrNotFound", err)
	}
}
```

- [ ] **Step 6 (cycle B): Run to verify it fails**

Run: `go test ./internal/store/ -run TestGetMissing -v`
Expected: FAIL — `Get(missing) = store: not found, want ErrNotFound` (not wrapped yet).

- [ ] **Step 7 (cycle B): Minimal implementation**

Change `Get`'s return line in `store.go`:

```go
	return Record{}, fmt.Errorf("store: job %q: %w", id, ErrNotFound)
```

- [ ] **Step 8 (cycle B): Run to verify it passes**

Run: `go test ./internal/store/ -race`
Expected: PASS.

- [ ] **Step 9 (cycle C): Write the failing corrupt-file test**

Append to `store_test.go`:

```go
// TestCorruptFileFailsLoudly pins the failure mode for a jobs.json that a
// crash left half-written (Review Focus 5): both operations surface an
// error, neither panics, and Save does NOT clobber what it cannot read.
func TestCorruptFileFailsLoudly(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "jobs.json"), []byte("{not json"), 0o644); err != nil {
		t.Fatal(err)
	}
	s := store.New(dir)

	if _, err := s.Get("any"); err == nil || errors.Is(err, store.ErrNotFound) {
		t.Errorf("Get(corrupt) = %v, want a decode error (not ErrNotFound)", err)
	}
	if err := s.Save(store.Record{ID: "new"}); err == nil {
		t.Error("Save(corrupt) = nil, want a decode error — silent overwrite loses data")
	}
}
```

- [ ] **Step 10 (cycle C): Run to verify it fails**

Run: `go test ./internal/store/ -run TestCorruptFile -v`
Expected: FAIL — `load` currently returns an empty map on decode failure (it does not distinguish), so `Get` reports `ErrNotFound` and `Save` succeeds.

- [ ] **Step 11 (cycle C): Minimal implementation**

The `load` function in `store.go` already returns a decode error — if Step 10 fails, the discrepancy is there: ensure the `json.Unmarshal` failure branch returns the wrapped error (shown in Step 3's full listing):

```go
	var m map[string]Record
	if err := json.Unmarshal(b, &m); err != nil {
		return nil, fmt.Errorf("store: decode %s: %w", s.path(), err)
	}
```

(If Step 3 was written verbatim this cycle may already pass; confirm and continue — green is green.)

- [ ] **Step 12 (cycle C): Run to verify it passes**

Run: `go test ./internal/store/ -race`
Expected: PASS.

- [ ] **Step 13 (cycle D): Write the failing concurrency test**

Append to `store_test.go`:

```go
// TestConcurrentSaves pins the mutex's reason to exist at the store level
// (Review Focus 4): without it, racing read-modify-write cycles lose
// records — and -race flags the data race. Five goroutines, five records,
// all five must survive.
func TestConcurrentSaves(t *testing.T) {
	s := store.New(t.TempDir())

	var wg sync.WaitGroup
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done() // defer inside the goroutine runs before wg.Done returns up the stack
			id := string(rune('a' + i))
			if err := s.Save(store.Record{ID: id, Status: job.StatusSucceeded}); err != nil {
				t.Errorf("Save(%s) error: %v", id, err)
			}
		}(i)
	}
	wg.Wait() // the channel-based join: Wait blocks until every Done has fired

	for i := 0; i < 5; i++ {
		id := string(rune('a' + i))
		if _, err := s.Get(id); err != nil {
			t.Errorf("Get(%s) after concurrent saves: %v", id, err)
		}
	}
}
```

- [ ] **Step 14 (cycle D): Run — this one may already pass, and that's fine**

Run: `go test ./internal/store/ -run TestConcurrentSaves -race -count=5`
Expected: PASS — Step 3's `sync.Mutex` already serializes the
read-modify-write cycles. This test is a *pin*: from now on, anyone who
removes the lock gets a loud failure here instead of a lost record in
production. If it fails, the mutex is missing from `Save`/`Get` — add it.

- [ ] **Step 15 (cycle D): Verify, format, commit**

```bash
gofmt -l .          # expect empty
go vet ./...
go test ./... -race
git add internal/store/
git commit -m "feat(store): persist job records as an atomically-written JSON file"
```

---

### Task 3: GET /jobs/{id} handler

**Files:**
- Create: `internal/api/server.go`
- Test: `internal/api/server_test.go`

**Interfaces:**
- Consumes: `store.New/Save/Get/Record/ErrNotFound` (Task 2), `executor.Runner`, `job.Job/Status`.
- Produces (Task 4/5 depend on these exact names):
  - `func New(exec executor.Runner, st *store.Store) *Server`
  - `func (s *Server) Handler() http.Handler`
  - unexported: `func (s *Server) getJob(w http.ResponseWriter, r *http.Request)`, `func writeErr(w http.ResponseWriter, code int, err error)`

- [ ] **Step 1: Write the failing tests**

Create `internal/api/server_test.go`:

```go
package api_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/Dushmilan/Forge/internal/api"
	"github.com/Dushmilan/Forge/internal/executor"
	"github.com/Dushmilan/Forge/internal/job"
	"github.com/Dushmilan/Forge/internal/store"
)

// newServer builds a Server over a temp-dir store. executor.Runner is V0's
// seam — the API never names the concrete Executor, which is exactly why it
// exists.
func newServer(t *testing.T) (*api.Server, *store.Store) {
	t.Helper()
	st := store.New(t.TempDir())
	return api.New(executor.Executor{}, st), st
}

// TestGetJobFound pins the happy path: 200, JSON content type, and a body
// whose id matches the path.
func TestGetJobFound(t *testing.T) {
	srv, st := newServer(t)
	if err := st.Save(store.Record{ID: "job1", Command: []string{"true"}, Status: job.StatusSucceeded}); err != nil {
		t.Fatal(err)
	}

	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/jobs/job1", nil))

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 (body: %s)", rec.Code, rec.Body.String())
	}
	if ct := rec.Header().Get("Content-Type"); ct != "application/json" {
		t.Errorf("Content-Type = %q, want application/json", ct)
	}
	var body struct {
		ID string `json:"id"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("body is not JSON: %v", err)
	}
	if body.ID != "job1" {
		t.Errorf("body id = %q, want job1", body.ID)
	}
}

// TestGetJobMissing pins the 404 contract: JSON error body carrying the
// id, so a client can tell WHICH job was missing.
func TestGetJobMissing(t *testing.T) {
	srv, _ := newServer(t)

	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/jobs/ghost", nil))

	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "ghost") {
		t.Errorf("body = %s, want it to name the missing id", rec.Body.String())
	}
	var body map[string]string
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Errorf("body is not JSON: %v", err)
	}
}

// TestMethodAndPathFallbacks pins the mux's own routing behavior
// (Review Focus 3): 405 for a known path with the wrong verb, 404 for an
// unknown path. Pinned so nobody later "fixes" these into 200s.
func TestMethodAndPathFallbacks(t *testing.T) {
	srv, _ := newServer(t)
	h := srv.Handler()

	tests := []struct {
		name string
		req  *http.Request
		want int
	}{
		{"wrong method", httptest.NewRequest(http.MethodDelete, "/jobs/x", nil), http.StatusMethodNotAllowed},
		{"unknown path", httptest.NewRequest(http.MethodGet, "/nope", nil), http.StatusNotFound},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, tt.req)
			if rec.Code != tt.want {
				t.Errorf("status = %d, want %d", rec.Code, tt.want)
			}
		})
	}
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `go test ./internal/api/`
Expected: FAIL to compile — `no required module provides package github.com/Dushmilan/Forge/internal/api`.

- [ ] **Step 3: Write the minimal implementation**

Create `internal/api/server.go`:

```go
// Package api exposes Forge over HTTP: submit a job, fetch a job.
//
// Handlers are synchronous — POST runs the job before responding. That is
// V1's honest shape; V2 replaces the inline run with a queue, and this
// package's interface does not change.
package api

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"

	"github.com/Dushmilan/Forge/internal/executor"
	"github.com/Dushmilan/Forge/internal/store"
)

// Server is the HTTP front end. It holds only the two collaborators every
// handler needs; config (timeouts, sandboxes) arrives in later versions
// without changing New's callers.
type Server struct {
	exec executor.Runner
	st   *store.Store
}

// New wires the server. exec is V0's Runner interface, not the concrete
// Executor — accept interfaces, return concrete types, and tests (or V2's
// queue) can substitute whatever has a Run method.
func New(exec executor.Runner, st *store.Store) *Server {
	return &Server{exec: exec, st: st}
}

// Handler returns the routes on a Go 1.22 method+pattern mux: the verb is
// part of the pattern, so routing mistakes become 405s from the standard
// library instead of hand-written if-chains. {id} is captured into
// r.PathValue("id") — no third-party router needed.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /jobs/{id}", s.getJob)
	return mux
}

// getJob answers with the stored record, or 404 when the sentinel says so.
// The errors.Is branch separates "no such job" from "storage broke" — the
// reason the store wraps ErrNotFound with %w.
func (s *Server) getJob(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	rec, err := s.st.Get(r.PathValue("id"))
	switch {
	case errors.Is(err, store.ErrNotFound):
		writeErr(w, http.StatusNotFound, err)
	case err != nil:
		writeErr(w, http.StatusInternalServerError, err)
	default:
		// Encoding into the response cannot fail for our own Record type;
		// the header is already sent, so there is nothing to recover — ignore
		// it deliberately rather than pretend we can still change the status.
		_ = json.NewEncoder(w).Encode(rec)
	}
}

// writeErr renders an error as the API's one error shape: a JSON object
// with an "error" key. Clients (and tests) get JSON on every failure path,
// never bare text.
func writeErr(w http.ResponseWriter, code int, err error) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
}
```

- [ ] **Step 4: Run to verify it passes**

Run: `go test ./internal/api/ -race -v`
Expected: PASS — `TestGetJobFound`, `TestGetJobMissing`, `TestMethodAndPathFallbacks` (and its two subtests).

- [ ] **Step 5: Format, commit**

```bash
gofmt -l .
go vet ./...
go test ./... -race
git add internal/api/
git commit -m "feat(api): GET /jobs/{id} serves persisted records"
```

---

### Task 4: POST /jobs — run synchronously, persist, respond

**Files:**
- Modify: `internal/api/server.go` (register route in `Handler`, add `createJob`, `newID`)
- Test: `internal/api/server_test.go` (append)

**Interfaces:**
- Consumes: `Server.exec` (`executor.Runner`), `Server.st`, `job.Job`, `job.Validate`, `writeErr` (Task 3), `store.Record` (Task 2).
- Produces: `POST /jobs` route; unexported `func (s *Server) createJob(w http.ResponseWriter, r *http.Request)` and `func newID() (string, error)`. Task 5 hosts this over a network but adds no new API surface.

- [ ] **Step 1 (cycle A): Write the failing happy-path test**

Append to `internal/api/server_test.go`:

```go
// TestPostJobRunsAndPersists pins the V1 core: a valid submission runs
// synchronously, answers 201 with the full record, and that record is
// retrievable afterwards — the response body and the stored record are the
// same job.
func TestPostJobRunsAndPersists(t *testing.T) {
	srv, st := newServer(t)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/jobs",
		strings.NewReader(`{"command": ["echo", "hello"]}`))
	srv.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusCreated {
		t.Fatalf("status = %d, want 201 (body: %s)", rec.Code, rec.Body.String())
	}
	var body store.Record
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("body is not JSON: %v", err)
	}
	if body.ID == "" {
		t.Fatal("response has no id — the server must generate one")
	}
	if body.Status != job.StatusSucceeded {
		t.Errorf("status = %v, want succeeded", body.Status)
	}
	if body.Result.Stdout != "hello\n" {
		t.Errorf("stdout = %q, want %q", body.Result.Stdout, "hello\n")
	}

	stored, err := st.Get(body.ID)
	if err != nil {
		t.Fatalf("persisted record not found: %v", err)
	}
	if stored.ID != body.ID || stored.Result.Stdout != body.Result.Stdout {
		t.Errorf("stored = %+v, want to match response %+v", stored, body)
	}
}
```

- [ ] **Step 2 (cycle A): Run to verify it fails**

Run: `go test ./internal/api/ -run TestPostJob -v`
Expected: FAIL — 404 (`POST /jobs` not registered on the mux).

- [ ] **Step 3 (cycle A): Write the minimal implementation**

Add to `Handler()` in `server.go`:

```go
	mux.HandleFunc("POST /jobs", s.createJob)
```

Add the imports `"crypto/rand"`, `"encoding/hex"`, and `"github.com/Dushmilan/Forge/internal/job"` to `server.go`.

Append to `server.go`:

```go
// createJob is the V1 core: decode → identify → validate → run → persist →
// respond, in that order, all inside one HTTP request. Each step answers
// with the status code its failure actually deserves.
func (s *Server) createJob(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	// Step 1: the body must be the expected JSON shape. A body that will
	// not decode is a client's mistake (400), never a 500 and never a
	// panic — json.Decoder returns the problem as an ordinary error.
	var in struct {
		Command []string `json:"command"`
	}
	if err := json.NewDecoder(r.Body).Decode(&in); err != nil {
		writeErr(w, http.StatusBadRequest, fmt.Errorf("api: bad request body: %w", err))
		return
	}

	// Step 2: identity comes from the server. crypto/rand gives 16 bytes of
	// OS entropy; hex doubles it to a collision-proof-enough URL token.
	// Client-supplied ids would need uniqueness semantics — V6's story.
	id, err := newID()
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err)
		return
	}

	// Step 3: validation reuses V0's rule set. With a generated id the only
	// way to fail here is an empty command — still a 400, still nothing run.
	j := job.Job{ID: id, Command: in.Command}
	if err := j.Validate(); err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}

	// Step 4: run it. Blocking here IS V1 — the client waits because there
	// is no queue yet. V2's whole point is that this line stops blocking.
	res, runErr := s.exec.Run(j)

	// Step 5: a command that failed is an OUTCOME, not a transport error.
	// The job ran; the answer is 201 with status "failed" and the message
	// kept as data. Only a broken store justifies 500.
	rec := store.Record{ID: id, Command: j.Command, Status: job.StatusSucceeded, Result: res}
	if runErr != nil {
		rec.Status = job.StatusFailed
		rec.Err = runErr.Error()
	}
	if err := s.st.Save(rec); err != nil {
		writeErr(w, http.StatusInternalServerError, err)
		return
	}

	w.WriteHeader(http.StatusCreated)
	_ = json.NewEncoder(w).Encode(rec)
}

// newID returns 32 hex characters (16 random bytes). It returns an error
// instead of panicking because AGENTS.md reserves panic for main/init —
// entropy failure is rare but it is an I/O condition, and callers answer
// 500 like any other server-side failure.
func newID() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", fmt.Errorf("api: generate id: %w", err)
	}
	return hex.EncodeToString(b[:]), nil
}
```

- [ ] **Step 4 (cycle A): Run to verify it passes**

Run: `go test ./internal/api/ -race -v`
Expected: PASS — new `TestPostJobRunsAndPersists`, all Task 3 tests still green.

- [ ] **Step 5 (cycle B): Write the failing rejection tests**

Append to `internal/api/server_test.go`:

```go
// TestPostJobRejections pins the client-error contract (Review Focus 1
// and the spec's "400 and nothing written"): every malformed or invalid
// submission gets 400 + JSON error, and the store stays untouched.
func TestPostJobRejections(t *testing.T) {
	tests := []struct {
		name string
		body string
	}{
		{name: "not json at all", body: `not json`},
		{name: "truncated json", body: `{"command": ["echo"`},
		{name: "empty object", body: `{}`},
		{name: "empty command", body: `{"command": []}`},
		{name: "null command", body: `{"command": null}`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			srv, _ := newServer(t)

			rec := httptest.NewRecorder()
			req := httptest.NewRequest(http.MethodPost, "/jobs", strings.NewReader(tt.body))
			srv.Handler().ServeHTTP(rec, req)

			if rec.Code != http.StatusBadRequest {
				t.Fatalf("status = %d, want 400 (body: %s)", rec.Code, rec.Body.String())
			}
			var body map[string]string
			if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
				t.Errorf("body is not JSON: %v", err)
			}
		})
	}
}

// TestPostJobFailedCommand pins Review Focus 2: a command that exits
// non-zero (or never starts) is a 201 with status "failed" and the error
// preserved as data — not a 400, not a 500.
func TestPostJobFailedCommand(t *testing.T) {
	tests := []struct {
		name     string
		command  string
		wantCode int
	}{
		{name: "exits 3", command: `["sh", "-c", "exit 3"]`, wantCode: 3},
		{name: "binary missing", command: `["definitely-not-a-real-binary-xyz"]`, wantCode: 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			srv, _ := newServer(t)

			rec := httptest.NewRecorder()
			body := `{"command": ` + tt.command + `}`
			req := httptest.NewRequest(http.MethodPost, "/jobs", strings.NewReader(body))
			srv.Handler().ServeHTTP(rec, req)

			if rec.Code != http.StatusCreated {
				t.Fatalf("status = %d, want 201 (body: %s)", rec.Code, rec.Body.String())
			}
			var got store.Record
			if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
				t.Fatalf("body is not JSON: %v", err)
			}
			if got.Status != job.StatusFailed {
				t.Errorf("status = %v, want failed", got.Status)
			}
			if got.Result.ExitCode != tt.wantCode {
				t.Errorf("exit code = %d, want %d", got.Result.ExitCode, tt.wantCode)
			}
			if got.Err == "" {
				t.Error("error field empty — the failure message must persist as data")
			}
		})
	}
}

// TestPostJobStoreFailure500 pins the spec's 500 rule: when the job ran but
// the store rejects it, the client learns the save failed instead of
// hearing nothing. A file where a directory should be makes MkdirAll (and
// thus Save) fail deterministically — no fault injection needed.
func TestPostJobStoreFailure500(t *testing.T) {
	blocker := filepath.Join(t.TempDir(), "not-a-dir")
	if err := os.WriteFile(blocker, []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	srv := api.New(executor.Executor{}, store.New(filepath.Join(blocker, "data")))

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/jobs",
		strings.NewReader(`{"command": ["true"]}`))
	srv.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500 (body: %s)", rec.Code, rec.Body.String())
	}
	var body map[string]string
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Errorf("body is not JSON: %v", err)
	}
}
```

Add `"os"` and `"path/filepath"` to the test file's imports.

- [ ] **Step 6 (cycle B): Run the rejection tests**

Run: `go test ./internal/api/ -run 'TestPostJobRejections|TestPostJobFailed|TestPostJobStore' -v`
Expected: PASS — cycle A's `createJob` already branches on every case
(decoder → 400, `Validate` → 400, `runErr` → failed record, `Save` error →
500). That is the point of this cycle: these tests *pin* the contract so a
future refactor cannot quietly loosen it. If any assertion fails, read it,
make the minimum change to `createJob` that satisfies it (Step 7), and do
not restructure anything that is green.

- [ ] **Step 7 (cycle B): Fix only what the failing tests demanded**

No production code change needed if Step 6 was fully green. If a specific
assertion failed, change only the `createJob` branch it names.

- [ ] **Step 8 (cycle B): Run the whole package to verify it passes**

Run: `go test ./internal/api/ -race`
Expected: PASS.

- [ ] **Step 9 (cycle C): Write the failing concurrency test**

Append to `internal/api/server_test.go`:

```go
// TestConcurrentRequestsRaceFree pins Review Focus 4 at the API level: a
// real httptest server, one POST running a command while three GETs hammer
// a known id. If the store mutex were missing, -race would fire here —
// net/http serves every request on its own goroutine by design.
func TestConcurrentRequestsRaceFree(t *testing.T) {
	srv, st := newServer(t)
	if err := st.Save(store.Record{ID: "hot", Command: []string{"true"}, Status: job.StatusSucceeded}); err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(srv.Handler())
	t.Cleanup(ts.Close) // t.Cleanup runs after the test, even on failure

	var wg sync.WaitGroup
	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			resp, err := http.Get(ts.URL + "/jobs/hot")
			if err != nil {
				t.Errorf("GET error: %v", err)
				return
			}
			resp.Body.Close()
		}()
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		resp, err := http.Post(ts.URL+"/jobs", "application/json",
			strings.NewReader(`{"command": ["echo", "x"]}`))
		if err != nil {
			t.Errorf("POST error: %v", err)
			return
		}
		resp.Body.Close()
	}()
	wg.Wait()
}
```

Add `"sync"` and `"net/http/httptest"` — `httptest` is already imported from Task 3; add `"sync"` only.

- [ ] **Step 10 (cycle C): Run — expected to pass, and stay passing**

Run: `go test ./internal/api/ -run TestConcurrentRequests -race -count=5`
Expected: PASS on first run (the mutex landed in Task 2). A DATA RACE here means the store lock was lost — go fix `internal/store`. The test's job is to pin the property; confirm and move on.

- [ ] **Step 11: Format, commit**

```bash
gofmt -l .
go vet ./...
go test ./... -race
git add internal/api/
git commit -m "feat(api): POST /jobs runs and persists synchronously"
```

---

### Task 5: `forge serve` subcommand

**Files:**
- Modify: `cmd/forge/main.go` (serve branch in `run`, new `runServe`)
- Test: `cmd/forge/main_test.go` (append)

**Interfaces:**
- Consumes: `api.New/Handler`, `store.New`, `executor.Executor`, `run(args, stdout, stderr)` (V0).
- Produces: `func runServe(ctx context.Context, args []string, stdout, stderr io.Writer) int` — flags `-addr` (default `:8080`) and `-data` (default `data`); prints `listening on <addr>` to stdout on success.

- [ ] **Step 1: Write the failing test**

Append to `cmd/forge/main_test.go`:

```go
// lineWriter collects Write output and emits each complete line on ch, so a
// test can wait for "listening on …" without sleeping: it blocks on a
// channel receive, which is synchronization, not polling.
type lineWriter struct {
	mu  sync.Mutex
	buf []byte
	ch  chan string
}

func newLineWriter() *lineWriter {
	return &lineWriter{ch: make(chan string, 8)}
}

func (w *lineWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.buf = append(w.buf, p...)
	for {
		i := bytes.IndexByte(w.buf, '\n')
		if i < 0 {
			break
		}
		w.ch <- string(w.buf[:i])
		w.buf = w.buf[i+1:]
	}
	return len(p), nil
}

// TestRunServeServesThenShutsDown pins the whole serve lifecycle against
// Review Focus-style reality: it binds a real port (:0 = pick any free
// one), answers a real HTTP request, and exits 0 when ctx is cancelled —
// with no time.Sleep anywhere. Cancellation is the synchronization.
func TestRunServeServesThenShutsDown(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	out := newLineWriter()
	code := make(chan int, 1)
	go func() {
		code <- runServe(ctx, []string{"-addr", "127.0.0.1:0", "-data", t.TempDir()}, out, io.Discard)
	}()

	// Block until the server announces its address — a channel receive, not
	// a sleep, so the test cannot flake on timing.
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

	// A real request proves the API is wired into the server, not just
	// that a socket opened. 404 for a job nobody created.
	resp, err := http.Get("http://" + addr + "/jobs/ghost")
	if err != nil {
		t.Fatalf("GET failed: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("status = %d, want 404", resp.StatusCode)
	}

	cancel()
	select {
	case c := <-code:
		if c != 0 {
			t.Errorf("runServe() = %d, want 0 on clean shutdown", c)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("runServe did not shut down after cancel")
	}
}

// TestRunServeBadFlag pins that flag misuse exits 2 (usage), like the
// no-args CLI path — not 1, which means "something ran and failed".
func TestRunServeBadFlag(t *testing.T) {
	var stdout, stderr bytes.Buffer
	if code := runServe(context.Background(), []string{"-bogus"}, &stdout, &stderr); code != 2 {
		t.Errorf("runServe(-bogus) = %d, want 2", code)
	}
}
```

Add these imports to `main_test.go`: `"context"`, `"io"`, `"net/http"`, `"sync"`, `"time"` (keep existing `"bytes"`, `"strings"`, `"testing"`).

- [ ] **Step 2: Run to verify it fails**

Run: `go test ./cmd/forge/ -run TestRunServe -v`
Expected: FAIL to compile — `undefined: runServe`.

- [ ] **Step 3: Write the minimal implementation**

Add to `cmd/forge/main.go` — at the top of `run`, before the no-args check:

```go
	// The serve subcommand is recognized by name BEFORE the args are
	// treated as a job command, so `forge serve` starts a server instead of
	// trying to execute a program named "serve".
	if len(args) > 0 && args[0] == "serve" {
		// signal.NotifyContext translates OS signals (Ctrl-C, SIGTERM) into
		// ctx cancellation — context is the one knob every blocking call in
		// Go already understands.
		ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
		defer stop() // stop deregisters the signal handlers on the way out
		return runServe(ctx, args[1:], stdout, stderr)
	}
```

Add the imports `context`, `errors`, `flag`, `net`, `net/http`,
`os/signal`, `syscall`, `time`, and the two new project packages (`api`,
`store`; `executor` is already imported).

Append `runServe` to `main.go`:

```go
// runServe parses serve flags, wires store + API into an http.Server, and
// serves until ctx is cancelled. It returns an exit code rather than
// calling os.Exit, keeping main the only place that terminates the process
// — the same discipline as run.
func runServe(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	// A FlagSet (not the global flag.CommandLine) means serve's flags are
	// parsed in isolation and testable — the global one would panic on a
	// second Parse.
	fs := flag.NewFlagSet("serve", flag.ContinueOnError)
	fs.SetOutput(stderr)
	addr := fs.String("addr", ":8080", "address to listen on")
	data := fs.String("data", "data", "directory holding jobs.json")
	if err := fs.Parse(args); err != nil {
		return 2 // flag misuse is a usage error, like no-args in run
	}

	var e executor.Executor
	srv := &http.Server{
		Addr:    *addr,
		Handler: api.New(e, store.New(*data)).Handler(),
	}

	// Listen ourselves instead of srv.ListenAndServe() so we can print the
	// REAL address: with -addr ":0" the OS picked an ephemeral port that
	// only the listener knows.
	ln, err := net.Listen("tcp", *addr)
	if err != nil {
		fmt.Fprintf(stderr, "forge: %v\n", err)
		return 1
	}
	fmt.Fprintf(stdout, "listening on %s\n", ln.Addr())

	// Serve in a goroutine; the main goroutine selects between the two
	// outcomes. This is the ONLY goroutine V1 adds: it exists to bridge
	// ctx → Shutdown, not to run jobs (that is V2).
	serveErr := make(chan error, 1)
	go func() { serveErr <- srv.Serve(ln) }()

	select {
	case <-ctx.Done():
		// Shutdown stops accepting and waits for in-flight requests; a
		// deadline caps the wait so a stuck handler cannot hang shutdown
		// forever.
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdownCtx)
		// Belt and braces: if Shutdown raced ahead of Serve registering the
		// listener, closing it ourselves still guarantees Serve returns.
		// net.ErrClosed (from that close) and http.ErrServerClosed (from
		// Shutdown) both mean "we meant to stop" — not a failure.
		_ = ln.Close()
		<-serveErr // join: don't return until the serving goroutine has
		return 0
	case err := <-serveErr:
		if err != nil && !errors.Is(err, http.ErrServerClosed) && !errors.Is(err, net.ErrClosed) {
			fmt.Fprintf(stderr, "forge: %v\n", err)
			return 1
		}
		return 0
	}
}
```

- [ ] **Step 4: Run to verify it passes**

Run: `go test ./cmd/forge/ -race -v`
Expected: PASS — `TestRunServeServesThenShutsDown`, `TestRunServeBadFlag`, and all three V0 CLI tests unchanged.

- [ ] **Step 5: Manual smoke test**

```bash
go build ./cmd/forge
./forge serve -addr 127.0.0.1:0 -data /tmp/forge-smoke &
# note the printed "listening on 127.0.0.1:PORT", then:
curl -s -X POST http://127.0.0.1:PORT/jobs -d '{"command": ["echo", "hi"]}'
curl -s http://127.0.0.1:PORT/jobs/<id-from-above>
kill -INT %1   # must exit promptly with 0
```

Expected: POST prints a JSON record with `"status": "succeeded"`; GET returns it; the process exits on SIGINT without a hang.

- [ ] **Step 6: Format, commit**

```bash
gofmt -l .
go vet ./...
go test ./... -race
git add cmd/forge/
git commit -m "feat(cmd): forge serve hosts the API until interrupted"
```

---

### Task 6: Documentation, definition of done, tag v1

**Files:**
- Modify: `docs/LEARNING.md` (append V1 entry)
- Modify: `README.md` (untracked → add, quick start + endpoints + roadmap)
- Test: none (docs task; verification is the full DoD battery)

**Interfaces:**
- Consumes: everything from Tasks 1–5 as shipped.
- Produces: a tagged `v1` and a README that matches reality.

- [ ] **Step 1: Append the V1 entry to `docs/LEARNING.md`**

After the V0 entry, following the file's template:

```markdown
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
```

- [ ] **Step 2: Update `README.md`**

Make these edits (README is currently untracked; this task adds it):

1. Roadmap table: change the V1 row from ⏳ to ✅.
2. Quick start: after the V0 examples, add:

```markdown
### As a server

```bash
go build ./cmd/forge
./forge serve -addr :8080 -data ./data
```

```bash
curl -X POST localhost:8080/jobs -d '{"command": ["echo", "hello"]}'   # 201 + record
curl localhost:8080/jobs/<id>                                           # 200 + record
```

Jobs run synchronously: the POST response arrives when the command finishes.
```

3. In the "what it does today" prose, mention both modes (CLI and server).

- [ ] **Step 3: Run the full definition-of-done battery**

```bash
gofmt -l .                # expect empty
go vet ./...              # expect clean
go test ./... -race       # expect green, including V0's tests
go build ./cmd/forge      # expect no output
```

Expected: all four silent/success. **Do not tag if any fails** — fix first (in the task that owns the failure).

- [ ] **Step 4: Commit and tag**

```bash
git add README.md docs/LEARNING.md
git commit -m "docs(v1): learning entry, README server quick start"
git tag v1
git log --oneline        # confirm the v1 history
```

Note: `internal/job/job.go`'s learner comment edit rode along with Task 1's commit — nothing left uncommitted afterward except anything the learner adds next.

- [ ] **Step 5: Update the roadmap in the README's Status section**

Verify the README's status line says V1 is done (checked in Step 2's table edit) — if the README has a separate "current state" sentence, sync it: `V0 and V1 complete; V2 (queue + workers) next.`

```bash
git add README.md
git commit -m "docs: sync README status after v1"
```

(Skip this step if Step 2 already left the status consistent — one commit, not two.)
