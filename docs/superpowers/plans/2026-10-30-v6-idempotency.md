# V6 — Idempotent Submission Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A retried `POST /jobs` carrying the same `Idempotency-Key` returns the original job instead of creating and running a second one — atomically, durably, and after a restart.

**Architecture:** `internal/store` gains `Record.IdempotencyKey` and an admission method `Create`, which derives a key→id map from the loaded records inside the existing mutex and writes only when the key is new. `Save` keeps the stored key, like it keeps `LastHeartbeat`. `internal/api` reads the header, validates before binding the key, and answers 202 (new), 200 (replay), 409 (same key, different command), 400 (oversize key).

**Tech Stack:** Go 1.22 standard library only (`slices`, `sync`, `errors`). Zero new dependencies.

**Spec:** `docs/versions/V6.md`

## Global Constraints

- **TDD strictly:** one failing test at a time; run it, watch it fail, implement the minimum, watch it pass. Never write implementation before the test that demands it; never refactor while red.
- **Standard library only.** No new module dependencies.
- **No `time.Sleep` as synchronization.** Bounded observation windows are used only where an absence is being asserted, matching the existing suite.
- **Comment convention:** block comments (`/* … */`) explaining the Go concept, per AGENTS.md rule 4.
- **Field ownership:** `Save` never writes `IdempotencyKey` on an existing record; only `Create` sets one, and only for a new record.
- **API 409 policy lives in `api`**, not `store`; the store only knows a key already names a record.
- **Before every commit:** `gofmt -l .` empty, `go vet ./...` clean, `go test ./... -race` green.
- House test style: external test packages (`package store_test`, `package api_test`); `cmd/forge` tests stay in `package main`.

## File Structure

| File | Responsibility |
|---|---|
| `internal/store/store.go` | record field, keyed admission, shared locked insert |
| `internal/api/server.go` | header, status contract, command comparison |
| `internal/store/store_test.go` | dedupe, restart, ownership, corruption, concurrency |
| `internal/api/server_test.go` | 202/200/409/400, no re-enqueue, concurrent POSTs |
| `cmd/forge/main_test.go` | end-to-end replay and restart |
| `README.md`, `docs/LEARNING.md` | version docs |

## Review Focus

1. **Two concurrent duplicates both creating a job** → the check and the insert must share one critical section. Owned by Task 3.
2. **A worker save erasing the key** → identity must survive whole-record saves. Owned by Task 2.
3. **A replay enqueueing a second run** → the API must return before the enqueue path on `created=false`. Owned by Task 4.
4. **A failed submission burning its key** → validation precedes `Create`. Owned by Task 4.
5. **A hand-edited file with duplicate keys silently picking a winner** → `Create` errors naming both ids. Owned by Task 2.

---

### Task 1: `Create` — keyed admission

**Files:**
- Modify: `internal/store/store.go`
- Test: `internal/store/store_test.go`

**Interfaces:**
- Produces:
  - `store.Record.IdempotencyKey string` (`json:"idempotency_key,omitempty"`)
  - `func (s *Store) Create(r Record) (Record, bool, error)`

- [ ] **Step 1: Write the failing tests**

Append to `internal/store/store_test.go`:

```go
/*
TestCreateDedupesByKey pins the headline: the second submission with a key is
not a second job. It returns the stored record, reports created=false, and
leaves the file byte-identical — a replay must not rewrite anything.
*/
func TestCreateDedupesByKey(t *testing.T) {
	dir := t.TempDir()
	s := store.New(dir)

	first, created, err := s.Create(store.Record{
		ID: "first", Command: []string{"echo", "hi"}, Status: job.StatusPending, IdempotencyKey: "k1",
	})
	if err != nil || !created {
		t.Fatalf("Create(first) = %v, created=%v, want created", err, created)
	}

	before, err := os.ReadFile(filepath.Join(dir, "jobs.json"))
	if err != nil {
		t.Fatal(err)
	}
	second, created, err := s.Create(store.Record{
		ID: "second", Command: []string{"echo", "hi"}, Status: job.StatusPending, IdempotencyKey: "k1",
	})
	if err != nil {
		t.Fatalf("Create(second) error: %v", err)
	}
	if created {
		t.Fatal("Create(second) created a record, want a replay")
	}
	if second.ID != first.ID {
		t.Errorf("replay id = %q, want %q", second.ID, first.ID)
	}

	after, err := os.ReadFile(filepath.Join(dir, "jobs.json"))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(before, after) {
		t.Errorf("a replay rewrote jobs.json:\nbefore %s\nafter  %s", before, after)
	}
}

/*
TestCreateKeyRules pins the key's scope: distinct keys are distinct jobs, and
an empty key means "no dedupe" — the V1–V5 behaviour every old client gets.
*/
func TestCreateKeyRules(t *testing.T) {
	s := store.New(t.TempDir())

	for _, tt := range []struct {
		id, key string
	}{
		{"a", "k1"},
		{"b", "k2"},
		{"c", ""},
		{"d", ""},
	} {
		_, created, err := s.Create(store.Record{
			ID: tt.id, Command: []string{"true"}, Status: job.StatusPending, IdempotencyKey: tt.key,
		})
		if err != nil || !created {
			t.Fatalf("Create(%s, key=%q) = %v, created=%v, want created", tt.id, tt.key, err, created)
		}
	}

	got, err := s.List()
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 4 {
		t.Errorf("stored %d records, want 4 (empty keys never dedupe)", len(got))
	}
}

/*
TestCreateKeySurvivesRestart pins durability: the index is derived from disk,
so a new process must find the same key. Without this, a crash would reset
idempotency to "everything is new again".
*/
func TestCreateKeySurvivesRestart(t *testing.T) {
	dir := t.TempDir()
	first, _, err := store.New(dir).Create(store.Record{
		ID: "first", Command: []string{"true"}, Status: job.StatusPending, IdempotencyKey: "k1",
	})
	if err != nil {
		t.Fatal(err)
	}

	got, created, err := store.New(dir).Create(store.Record{
		ID: "second", Command: []string{"true"}, Status: job.StatusPending, IdempotencyKey: "k1",
	})
	if err != nil {
		t.Fatalf("Create after restart: %v", err)
	}
	if created || got.ID != first.ID {
		t.Errorf("after restart: created=%v id=%q, want replay of %q", created, got.ID, first.ID)
	}
}
```

- [ ] **Step 2: Run and watch them fail**

Run: `go test ./internal/store -run 'TestCreate' -v`
Expected: compile errors — `Unknown field IdempotencyKey`, `s.Create undefined`.

- [ ] **Step 3: Implement**

Add to `Record`:

```go
	/*
		IdempotencyKey is the client's identity for this submission. Opaque
		bytes: no trimming, no normalization. omitempty keeps every file
		written before V6 byte-identical, so old stores load unchanged.
	*/
	IdempotencyKey string `json:"idempotency_key,omitempty"`
```

Add the admission path and the two helpers:

```go
/*
Create inserts r unless a stored record already carries its key. It is the
admission path (POST /jobs asks "may this become a record?"); Save stays the
update path for records that already exist.

The check and the insert share s.mu, so two concurrent duplicates cannot both
observe "key absent" and both write: the loser sees the winner's record. The
index is derived from the loaded map rather than stored beside it, which keeps
jobs.json a single atomic write at the cost of an O(n) scan per keyed submit.
*/
func (s *Store) Create(r Record) (Record, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := os.MkdirAll(s.dir, 0o755); err != nil {
		return Record{}, false, fmt.Errorf("store: create %s: %w", s.dir, err)
	}
	m, err := s.load()
	if err != nil {
		return Record{}, false, err
	}
	if r.IdempotencyKey != "" {
		ids, err := keyIndex(m)
		if err != nil {
			return Record{}, false, err
		}
		if id, ok := ids[r.IdempotencyKey]; ok {
			return m[id], false, nil
		}
	}
	if err := s.insertLocked(m, r); err != nil {
		return Record{}, false, err
	}
	return r, true, nil
}

/*
keyIndex derives key → id from the records. Building a map is what makes a
duplicate visible: two records claiming one key is corruption, and reporting
both ids is the only answer that does not silently drop a job.
*/
func keyIndex(m map[string]Record) (map[string]string, error) {
	ids := make(map[string]string, len(m))
	for id, rec := range m {
		if rec.IdempotencyKey == "" {
			continue
		}
		if other, ok := ids[rec.IdempotencyKey]; ok {
			return nil, fmt.Errorf("store: duplicate idempotency key %q on jobs %s and %s",
				rec.IdempotencyKey, other, id)
		}
		ids[rec.IdempotencyKey] = id
	}
	return ids, nil
}

/*
insertLocked validates and stores r, then writes once. Save and Create both
funnel through here so the state machine and the field-ownership rules exist
in exactly one place.
*/
func (s *Store) insertLocked(m map[string]Record, r Record) error {
	if cur, ok := m[r.ID]; ok {
		if !cur.Status.CanTransition(r.Status) {
			return fmt.Errorf("store: job %q: %w: %s -> %s",
				r.ID, job.ErrInvalidTransition, cur.Status, r.Status)
		}
		/*
			Field ownership: Save carries whole records from the worker and
			recovery, so it must never write the two fields it does not own.
			A stale snapshot that predates the heartbeat or the key would
			otherwise erase liveness or identity.
		*/
		r.LastHeartbeat = cur.LastHeartbeat
		r.IdempotencyKey = cur.IdempotencyKey
	}
	m[r.ID] = r
	return s.writeLocked(m)
}
```

Replace `Save`'s inline check-and-write with:

```go
	m, err := s.load()
	if err != nil {
		return err
	}
	return s.insertLocked(m, r)
```

- [ ] **Step 4: Run the package**

Run: `go test ./internal/store -race`
Expected: PASS, including V1–V5 store tests.

- [ ] **Step 5: Commit**

```bash
git add internal/store
git commit -m "feat(store): admit jobs idempotently by key"
```

---

### Task 2: Ownership and corruption

**Files:**
- Modify: `internal/store/store.go` (only if a test fails)
- Test: `internal/store/store_test.go`

- [ ] **Step 1: Write the failing tests**

```go
/*
TestSavePreservesIdempotencyKey pins the second half of field ownership: a
worker saves a record it built from a queue snapshot, and that snapshot must
not be able to erase the client's key.
*/
func TestSavePreservesIdempotencyKey(t *testing.T) {
	s := store.New(t.TempDir())
	if _, _, err := s.Create(store.Record{
		ID: "keyed", Command: []string{"true"}, Status: job.StatusPending, IdempotencyKey: "k1",
	}); err != nil {
		t.Fatal(err)
	}

	// The worker's view: same record, no key, after an attempt.
	if err := s.Save(store.Record{
		ID: "keyed", Command: []string{"true"}, Status: job.StatusRunning, Attempts: 1,
	}); err != nil {
		t.Fatal(err)
	}
	got, _ := s.Get("keyed")
	if got.IdempotencyKey != "k1" {
		t.Errorf("IdempotencyKey = %q, want k1 after a worker save", got.IdempotencyKey)
	}
}

/*
TestCreateRejectsDuplicateKeysInFile pins honesty over convenience: a store
that already contains two records for one key is corrupt, and guessing which
one "wins" would silently lose a job.
*/
func TestCreateRejectsDuplicateKeysInFile(t *testing.T) {
	dir := t.TempDir()
	body := `{
  "a": {"id": "a", "command": ["true"], "status": "pending", "idempotency_key": "k1"},
  "b": {"id": "b", "command": ["true"], "status": "pending", "idempotency_key": "k1"}
}`
	if err := os.WriteFile(filepath.Join(dir, "jobs.json"), []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}

	_, _, err := store.New(dir).Create(store.Record{
		ID: "c", Command: []string{"true"}, Status: job.StatusPending, IdempotencyKey: "k2",
	})
	if err == nil {
		t.Fatal("Create(corrupt keys) = nil, want an error")
	}
	if !strings.Contains(err.Error(), "a") || !strings.Contains(err.Error(), "b") {
		t.Errorf("error = %v, want it to name both ids", err)
	}
}
```

Add `"strings"` to the test imports if absent.

- [ ] **Step 2: Run and watch them fail**

Run: `go test ./internal/store -run 'TestSavePreservesIdempotencyKey|TestCreateRejectsDuplicateKeys' -v`
Expected: both PASS already if Task 1 was implemented exactly as specified. If so, that is the point of these tests — they pin behavior the implementation already has; record that they passed on first run, then continue. If the duplicate test fails, fix `keyIndex` before proceeding.

- [ ] **Step 3: Commit**

```bash
git add internal/store
git commit -m "test(store): pin key ownership and duplicate-key rejection"
```

---

### Task 3: Concurrent admissions

**Files:**
- Test: `internal/store/store_test.go`

- [ ] **Step 1: Write the failing test**

```go
/*
TestConcurrentCreatesOneWinner pins Review Focus 1: N clients retrying the
same key at once must produce one record. The mutex is the guarantee; -race is
the proof that the derived index is not read outside it.
*/
func TestConcurrentCreatesOneWinner(t *testing.T) {
	s := store.New(t.TempDir())

	const n = 10
	type result struct {
		rec     store.Record
		created bool
		err     error
	}
	out := make(chan result, n)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			rec, created, err := s.Create(store.Record{
				ID:             fmt.Sprintf("j%d", i),
				Command:        []string{"true"},
				Status:         job.StatusPending,
				IdempotencyKey: "same",
			})
			out <- result{rec, created, err}
		}(i)
	}
	wg.Wait()
	close(out)

	var winners int
	var id string
	for r := range out {
		if r.err != nil {
			t.Fatalf("Create error: %v", r.err)
		}
		if r.created {
			winners++
			id = r.rec.ID
		}
	}
	if winners != 1 {
		t.Fatalf("%d creates won, want exactly 1", winners)
	}

	all, err := s.List()
	if err != nil {
		t.Fatal(err)
	}
	if len(all) != 1 || all[0].ID != id {
		t.Errorf("store holds %+v, want only the winner %s", all, id)
	}
}
```

Add `"fmt"` to the test imports if absent.

- [ ] **Step 2: Run it**

Run: `go test ./internal/store -run TestConcurrentCreatesOneWinner -race -v`
Expected: PASS. If it fails with more than one winner, the index is being read outside the lock — move it inside, never around, `s.mu`.

- [ ] **Step 3: Commit**

```bash
git add internal/store
git commit -m "test(store): pin one winner under concurrent keyed creates"
```

---

### Task 4: The HTTP contract

**Files:**
- Modify: `internal/api/server.go`
- Test: `internal/api/server_test.go`

**Interfaces:**
- Consumes: `store.Create`
- Produces: `Idempotency-Key` handling on `POST /jobs`

- [ ] **Step 1: Write the failing tests**

```go
/*
TestPostJobIdempotentReplay pins the headline contract: the first POST creates
(202), the second with the same key replays (200), both name one job, and only
ONE record reaches a worker — a replay must not run the work again.
*/
func TestPostJobIdempotentReplay(t *testing.T) {
	srv, st, q, _ := newServer(t)
	h := srv.Handler()
	body := `{"command": ["echo", "hello"]}`

	first := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/jobs", strings.NewReader(body))
	req.Header.Set("Idempotency-Key", "k1")
	h.ServeHTTP(first, req)
	if first.Code != http.StatusAccepted {
		t.Fatalf("first status = %d, want 202 (body: %s)", first.Code, first.Body.String())
	}

	second := httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodPost, "/jobs", strings.NewReader(body))
	req.Header.Set("Idempotency-Key", "k1")
	h.ServeHTTP(second, req)
	if second.Code != http.StatusOK {
		t.Fatalf("replay status = %d, want 200 (body: %s)", second.Code, second.Body.String())
	}

	var a, b store.Record
	if err := json.Unmarshal(first.Body.Bytes(), &a); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(second.Body.Bytes(), &b); err != nil {
		t.Fatal(err)
	}
	if a.ID != b.ID {
		t.Errorf("replay id = %q, want the original %q", b.ID, a.ID)
	}

	all, err := st.List()
	if err != nil {
		t.Fatal(err)
	}
	if len(all) != 1 {
		t.Errorf("store holds %d records, want 1", len(all))
	}

	// A replay must not enqueue: one worker sees one record, then nothing.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	seen := make(chan store.Record, 2)
	go q.Run(ctx, 1, func(_ context.Context, r store.Record) { seen <- r })
	select {
	case got := <-seen:
		if got.ID != a.ID {
			t.Errorf("worker saw %q, want %q", got.ID, a.ID)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("the created record never reached a worker")
	}
	select {
	case got := <-seen:
		t.Fatalf("the replay reached a worker too: %+v", got)
	case <-time.After(50 * time.Millisecond):
	}
}

/*
TestPostJobKeyReuseDifferentCommand409 pins the mismatch rule: a key names one
intent. Reusing it for other work is a client bug, and no second job is made.
*/
func TestPostJobKeyReuseDifferentCommand409(t *testing.T) {
	srv, st, _, _ := newServer(t)
	h := srv.Handler()

	first := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/jobs", strings.NewReader(`{"command": ["echo", "one"]}`))
	req.Header.Set("Idempotency-Key", "k1")
	h.ServeHTTP(first, req)
	if first.Code != http.StatusAccepted {
		t.Fatalf("first status = %d, want 202", first.Code)
	}

	second := httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodPost, "/jobs", strings.NewReader(`{"command": ["echo", "two"]}`))
	req.Header.Set("Idempotency-Key", "k1")
	h.ServeHTTP(second, req)
	if second.Code != http.StatusConflict {
		t.Fatalf("status = %d, want 409 (body: %s)", second.Code, second.Body.String())
	}

	all, err := st.List()
	if err != nil {
		t.Fatal(err)
	}
	if len(all) != 1 {
		t.Errorf("store holds %d records, want 1", len(all))
	}
}

/*
TestPostJobKeyLengthLimit pins the one key rule: opaque bytes, capped so a
client cannot grow jobs.json without bound.
*/
func TestPostJobKeyLengthLimit(t *testing.T) {
	srv, st, _, _ := newServer(t)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/jobs", strings.NewReader(`{"command": ["true"]}`))
	req.Header.Set("Idempotency-Key", strings.Repeat("x", 256))
	srv.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400 (body: %s)", rec.Code, rec.Body.String())
	}
	all, err := st.List()
	if err != nil {
		t.Fatal(err)
	}
	if len(all) != 0 {
		t.Errorf("stored %d records for an oversize key, want none", len(all))
	}
}

/*
TestPostJobInvalidBodyDoesNotBurnKey pins Review Focus 4: validation runs
before admission, so a malformed attempt leaves the key unused and a valid
retry still creates the job.
*/
func TestPostJobInvalidBodyDoesNotBurnKey(t *testing.T) {
	srv, st, _, _ := newServer(t)

	bad := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/jobs", strings.NewReader(`{"command": []}`))
	req.Header.Set("Idempotency-Key", "k1")
	srv.Handler().ServeHTTP(bad, req)
	if bad.Code != http.StatusBadRequest {
		t.Fatalf("bad status = %d, want 400", bad.Code)
	}

	good := httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodPost, "/jobs", strings.NewReader(`{"command": ["true"]}`))
	req.Header.Set("Idempotency-Key", "k1")
	srv.Handler().ServeHTTP(good, req)
	if good.Code != http.StatusAccepted {
		t.Fatalf("retry status = %d, want 202 (body: %s)", good.Code, good.Body.String())
	}

	all, err := st.List()
	if err != nil {
		t.Fatal(err)
	}
	if len(all) != 1 {
		t.Errorf("store holds %d records, want the one valid job", len(all))
	}
}

/*
TestPostJobConcurrentDuplicatesOneJob pins the client-visible form of Review
Focus 1: twenty simultaneous retries of one key yield exactly one 202 and one
record; every response names the same id.
*/
func TestPostJobConcurrentDuplicatesOneJob(t *testing.T) {
	srv, st, _, _ := newServer(t)
	ts := httptest.NewServer(srv.Handler())
	t.Cleanup(ts.Close)

	const n = 20
	ids := make(chan string, n)
	codes := make(chan int, n)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			req, err := http.NewRequest(http.MethodPost, ts.URL+"/jobs",
				strings.NewReader(`{"command": ["true"]}`))
			if err != nil {
				t.Errorf("NewRequest: %v", err)
				return
			}
			req.Header.Set("Idempotency-Key", "burst")
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				t.Errorf("POST: %v", err)
				return
			}
			defer resp.Body.Close()
			var rec store.Record
			if err := json.NewDecoder(resp.Body).Decode(&rec); err != nil {
				t.Errorf("decode: %v", err)
				return
			}
			ids <- rec.ID
			codes <- resp.StatusCode
		}()
	}
	wg.Wait()
	close(ids)
	close(codes)

	var accepted int
	for code := range codes {
		if code == http.StatusAccepted {
			accepted++
		}
	}
	if accepted != 1 {
		t.Errorf("%d responses were 202, want exactly 1", accepted)
	}
	var want string
	for id := range ids {
		if want == "" {
			want = id
		}
		if id != want {
			t.Errorf("response id = %q, want %q", id, want)
		}
	}
	all, err := st.List()
	if err != nil {
		t.Fatal(err)
	}
	if len(all) != 1 {
		t.Errorf("store holds %d records, want 1", len(all))
	}
}
```

- [ ] **Step 2: Run and watch them fail**

Run: `go test ./internal/api -run 'TestPostJobIdempotent|TestPostJobKey|TestPostJobInvalidBody|TestPostJobConcurrentDuplicates' -v`
Expected: FAIL — the replay is a fresh 202 with a new id, and the oversize key is accepted.

- [ ] **Step 3: Implement**

In `createJob`, before decoding:

```go
	/*
		The key is opaque bytes: no trimming, no case folding. Missing or
		empty means "no dedupe", which keeps every client written before V6
		working unchanged. The cap exists so a client cannot grow jobs.json
		without bound.
	*/
	key := r.Header.Get("Idempotency-Key")
	if len(key) > maxIdempotencyKeyLen {
		writeErr(w, http.StatusBadRequest,
			fmt.Errorf("api: Idempotency-Key must be at most %d bytes", maxIdempotencyKeyLen))
		return
	}
```

with, next to `newID`:

```go
// maxIdempotencyKeyLen bounds one record's key. Opaque bytes, but not
// unbounded: the store's file is the whole database.
const maxIdempotencyKeyLen = 255
```

Replace the `Save` call with the admission path:

```go
	rec := store.Record{ID: id, Command: in.Command, Status: job.StatusPending, IdempotencyKey: key}
	stored, created, err := s.st.Create(rec)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err)
		return
	}
	if !created {
		/*
			A replay is the same intent, not a second submission. A different
			command under one key is a client bug: answer 409 and leave the
			stored job alone. The 200 says "nothing new was accepted"; the
			body's status says where the original job stands.

			Return here — before Enqueue. Re-enqueueing could run the job
			twice when the first enqueue succeeded but its response was lost,
			and V5's startup recovery already repairs a record that was
			persisted but never queued.
		*/
		if !slices.Equal(stored.Command, in.Command) {
			writeErr(w, http.StatusConflict,
				fmt.Errorf("api: Idempotency-Key %q was used with a different command", key))
			return
		}
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(stored)
		return
	}
```

Then enqueue `rec` and answer 202 exactly as before. Add `"slices"` to the imports.

- [ ] **Step 4: Run the package**

Run: `go test ./internal/api -race`
Expected: PASS, including V1–V5 API tests.

- [ ] **Step 5: Commit**

```bash
git add internal/api
git commit -m "feat(api): dedupe POST /jobs by Idempotency-Key"
```

---

### Task 5: End-to-end replay, including a restart

**Files:**
- Test: `cmd/forge/main_test.go`

- [ ] **Step 1: Write the failing tests**

Add a helper next to `postJob`:

```go
// postKeyed submits with an Idempotency-Key and returns the status code and
// the record the server answered with.
func postKeyed(t *testing.T, addr, key, command string) (int, store.Record) {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, "http://"+addr+"/jobs", strings.NewReader(command))
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Idempotency-Key", key)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	defer resp.Body.Close()
	var rec store.Record
	if err := json.NewDecoder(resp.Body).Decode(&rec); err != nil {
		t.Fatalf("POST body: %v", err)
	}
	return resp.StatusCode, rec
}

/*
TestRunServeReplaysIdempotentSubmission pins the contract end to end: the
command's side effect happens once, and the retry gets 200 with the original
id. Counting lines in a marker file proves execution, which the status alone
cannot.
*/
func TestRunServeReplaysIdempotentSubmission(t *testing.T) {
	marker := filepath.Join(t.TempDir(), "runs")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	addr, code := serveAddrData(t, ctx, t.TempDir(), "-workers", "1", "-job-timeout", "5s")

	body := `{"command": ["sh", "-c", "echo run >> ` + marker + `"]}`
	firstCode, first := postKeyed(t, addr, "once", body)
	if firstCode != http.StatusAccepted {
		t.Fatalf("first status = %d, want 202", firstCode)
	}
	got := waitStatus(t, addr, first.ID, job.StatusSucceeded)

	secondCode, second := postKeyed(t, addr, "once", body)
	if secondCode != http.StatusOK {
		t.Fatalf("replay status = %d, want 200", secondCode)
	}
	if second.ID != got.ID {
		t.Errorf("replay id = %q, want %q", second.ID, first.ID)
	}

	b, err := os.ReadFile(marker)
	if err != nil {
		t.Fatalf("read marker: %v", err)
	}
	if lines := strings.Count(string(b), "run\n") ; lines != 1 {
		t.Errorf("command ran %d time(s), want 1", lines)
	}

	cancel()
	select {
	case c := <-code:
		if c != 0 {
			t.Errorf("runServe() = %d, want 0", c)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("runServe did not shut down")
	}
}

/*
TestRunServeReplaySurvivesRestart pins durability at the process boundary: a
new serve over the same data directory answers the retry with the original
terminal record and does not run the command again.
*/
func TestRunServeReplaySurvivesRestart(t *testing.T) {
	data := t.TempDir()
	marker := filepath.Join(t.TempDir(), "runs")
	body := `{"command": ["sh", "-c", "echo run >> ` + marker + `"]}`

	ctx1, cancel1 := context.WithCancel(context.Background())
	addr1, code1 := serveAddrData(t, ctx1, data, "-workers", "1", "-job-timeout", "5s")
	firstCode, first := postKeyed(t, addr1, "once", body)
	if firstCode != http.StatusAccepted {
		t.Fatalf("first status = %d, want 202", firstCode)
	}
	done := waitStatus(t, addr1, first.ID, job.StatusSucceeded)
	cancel1()
	select {
	case <-code1:
	case <-time.After(10 * time.Second):
		t.Fatal("first serve did not shut down")
	}

	ctx2, cancel2 := context.WithCancel(context.Background())
	defer cancel2()
	addr2, code2 := serveAddrData(t, ctx2, data, "-workers", "1", "-job-timeout", "5s")
	defer func() { cancel2(); <-code2 }()

	secondCode, second := postKeyed(t, addr2, "once", body)
	if secondCode != http.StatusOK {
		t.Fatalf("replay after restart = %d, want 200", secondCode)
	}
	if second.ID != done.ID || second.Status != job.StatusSucceeded {
		t.Errorf("replay = %+v, want the original succeeded record %s", second, done.ID)
	}

	b, err := os.ReadFile(marker)
	if err != nil {
		t.Fatalf("read marker: %v", err)
	}
	if lines := strings.Count(string(b), "run\n"); lines != 1 {
		t.Errorf("command ran %d time(s) across the restart, want 1", lines)
	}
}
```

- [ ] **Step 2: Run and watch them fail**

Run: `go test ./cmd/forge -run 'TestRunServeReplay' -v`
Expected: FAIL — the retry answers 202 with a different id, and the marker holds two lines.

- [ ] **Step 3: Implement**

No production change should be needed if Tasks 1 and 4 are correct; the CLI already goes through the API. If the test fails, the bug is in a lower layer — fix it there, not in `cmd/forge`.

- [ ] **Step 4: Run the package**

Run: `go test ./cmd/forge -race`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add cmd/forge
git commit -m "test(forge): pin idempotent replay end to end"
```

---

### Task 6: Docs and tag

**Files:**
- Modify: `README.md`
- Modify: `docs/LEARNING.md`

- [ ] **Step 1: README**

- Status line → `V0–V6 are done. V7–V11 are planned.`
- Ladder row for V6 → ✅.
- Add to the server example, under the curl block:

```sh
curl -X POST localhost:8080/jobs \
  -H 'Idempotency-Key: order-42' \
  -d '{"command": ["echo", "hello"]}'   # 202; retry the same key -> 200, same id
```

- Add a `## How it works (V6)` section: the header, the 202/200/409 table, the
  derived key index inside the store lock, and the creation-vs-execution
  guarantee with the V5 at-least-once caveat.
- Note that `Save` never writes the key (same ownership rule as the heartbeat).

- [ ] **Step 2: LEARNING.md**

Add a `## V6 — Idempotency` entry in the V3–V5 format:

- **Concept:** a derived index (`map[string]string`) over durable state inside
  the lock that already serializes writes; check-and-insert as one critical
  section; `omitempty` as a compatibility tool; creation-at-most-once versus
  execution-at-least-once; a replay that must not enqueue.
- **What I built:** `Record.IdempotencyKey`, `store.Create`, the shared
  `insertLocked`, the `Idempotency-Key` contract on `POST /jobs`, and the
  end-to-end replay tests.
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
git commit -m "docs(v6): learning entry and README describe idempotent submission"
```

- [ ] **Step 5: Tag after review**

Do not tag yet. Tag `v6` only after the whole-branch review is clean.

---

## Self-Review

- **Spec coverage:** record field + `Create` (T1), ownership + corruption (T2), concurrency (T3), HTTP contract incl. 409/400/validation order/no re-enqueue (T4), e2e + restart (T5), docs/tag (T6).
- **Placeholders:** none; every step carries code.
- **Type consistency:** `Create(Record) (Record, bool, error)`, `IdempotencyKey string`, `maxIdempotencyKeyLen`, `keyIndex`, `insertLocked` — used consistently across tasks.
- **Review Focus:** concurrent duplicates (T3/T4), key erased by save (T2), replay enqueues (T4), failed submission burns key (T4), duplicate keys in file (T2).
