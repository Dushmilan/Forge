package api_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Dushmilan/Forge/internal/api"
	"github.com/Dushmilan/Forge/internal/job"
	"github.com/Dushmilan/Forge/internal/queue"
	"github.com/Dushmilan/Forge/internal/store"
)

// fakeCanceller records the ids the API asked to stop. Cancellation itself
// belongs to the worker; the API test only pins the hand-off.
type fakeCanceller struct {
	mu  sync.Mutex
	ids []string
}

func (f *fakeCanceller) Cancel(id string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.ids = append(f.ids, id)
}

func (f *fakeCanceller) cancelled() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.ids...)
}

// newServer builds a Server over a temp-dir store, a fresh queue, and a fake
// canceller. No executor: as of V2 the API hands jobs off, it never runs them.
func newServer(t *testing.T) (*api.Server, *store.Store, *queue.Queue, *fakeCanceller) {
	t.Helper()
	st := store.New(t.TempDir())
	q := queue.New()
	c := &fakeCanceller{}
	return api.New(context.Background(), q, st, c), st, q, c
}

// TestGetJobFound pins the happy path: 200, JSON content type, and a body
// whose id matches the path.
func TestGetJobFound(t *testing.T) {
	srv, st, _, _ := newServer(t)
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
	srv, _, _, _ := newServer(t)

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
	srv, _, _, _ := newServer(t)
	h := srv.Handler()

	tests := []struct {
		name string
		req  *http.Request
		want int
	}{
		{"wrong method", httptest.NewRequest(http.MethodPatch, "/jobs/x", nil), http.StatusMethodNotAllowed},
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

// TestPostJobEnqueuesAndPersists pins the V2 headline: POST answers 202
// with the PENDING record, that record is persisted BEFORE the response,
// and it reaches a worker — the request itself never waits for the run.
func TestPostJobEnqueuesAndPersists(t *testing.T) {
	srv, st, q, _ := newServer(t)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/jobs",
		strings.NewReader(`{"command": ["echo", "hello"]}`))
	srv.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want 202 (body: %s)", rec.Code, rec.Body.String())
	}
	var body store.Record
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("body is not JSON: %v", err)
	}
	if body.ID == "" {
		t.Fatal("response has no id — the server must generate one")
	}
	if body.Status != job.StatusPending {
		t.Errorf("status = %v, want pending", body.Status)
	}

	stored, err := st.Get(body.ID)
	if err != nil {
		t.Fatalf("persisted record not found: %v", err)
	}
	if stored.Status != job.StatusPending {
		t.Errorf("stored status = %v, want pending", stored.Status)
	}

	// The record reached the queue: a worker started now receives it.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	seen := make(chan store.Record, 1)
	go q.Run(ctx, 1, func(_ context.Context, r store.Record) { seen <- r })
	select {
	case got := <-seen:
		if got.ID != body.ID {
			t.Errorf("worker saw id %q, want %q", got.ID, body.ID)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("enqueued record never reached a worker")
	}
}

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
			srv, _, _, _ := newServer(t)

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

// TestPostJobStoreFailure500 pins Review Focus 5: a store that cannot
// save gets 500, and the submission never reaches the queue — a 500'd
// POST must not secretly run the job anyway. A file where a directory
// should be makes MkdirAll (and thus Save) fail deterministically.
func TestPostJobStoreFailure500(t *testing.T) {
	blocker := filepath.Join(t.TempDir(), "not-a-dir")
	if err := os.WriteFile(blocker, []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	q := queue.New()
	srv := api.New(context.Background(), q, store.New(filepath.Join(blocker, "data")), &fakeCanceller{})

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

	// Nothing was enqueued: a worker started now must see an empty queue.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	seen := make(chan store.Record, 1)
	go q.Run(ctx, 1, func(_ context.Context, r store.Record) { seen <- r })
	select {
	case got := <-seen:
		t.Fatalf("a 500'd submission reached the worker: %+v", got)
	case <-time.After(50 * time.Millisecond):
	}
}

// TestPostJobCancelledRequest503 pins the client-disconnect half of the
// no-deadlock guarantee: a POST whose REQUEST context is already dead gets
// 503 + JSON error instead of blocking forever, and the pending record it
// saved stays pending — nothing is enqueued. (The server-shutdown half has
// its own test below: http.Server.Shutdown does NOT cancel request
// contexts, so the two cancellations are genuinely different things.)
func TestPostJobCancelledRequest503(t *testing.T) {
	dir := t.TempDir()
	q := queue.New()
	srv := api.New(context.Background(), q, store.New(dir), &fakeCanceller{})

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // as if the server were already shutting down
	req := httptest.NewRequest(http.MethodPost, "/jobs",
		strings.NewReader(`{"command": ["echo", "late"]}`)).WithContext(ctx)
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503 (body: %s)", rec.Code, rec.Body.String())
	}
	var body map[string]string
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("body is not JSON: %v", err)
	}

	// The saved record is the only one, and it stayed pending. Read the
	// store file directly — the store has no List, and inventing one for
	// a test is V-something-else's problem.
	b, err := os.ReadFile(filepath.Join(dir, "jobs.json"))
	if err != nil {
		t.Fatalf("read store file: %v", err)
	}
	var m map[string]store.Record
	if err := json.Unmarshal(b, &m); err != nil {
		t.Fatalf("decode store file: %v", err)
	}
	if len(m) != 1 {
		t.Fatalf("stored %d records, want exactly the one pending save", len(m))
	}
	for _, got := range m {
		if got.Status != job.StatusPending {
			t.Errorf("record status = %v, want pending", got.Status)
		}
	}

	// And it was never handed off: a worker now sees an empty queue.
	wctx, wcancel := context.WithCancel(context.Background())
	defer wcancel()
	seen := make(chan store.Record, 1)
	go q.Run(wctx, 1, func(_ context.Context, r store.Record) { seen <- r })
	select {
	case got := <-seen:
		t.Fatalf("the 503'd record reached the worker: %+v", got)
	case <-time.After(50 * time.Millisecond):
	}
}

// TestPostBlockedEnqueueGets503OnServerShutdown pins Review Focus 1 for a
// REAL server shutdown: http.Server.Shutdown does not cancel request
// contexts, so a POST blocked on a full queue must be released by the
// server's lifecycle context, or it hangs until the Shutdown deadline and
// gets its connection dropped instead of an answer.
func TestPostBlockedEnqueueGets503OnServerShutdown(t *testing.T) {
	st := store.New(t.TempDir())
	q := queue.New()
	sctx, scancel := context.WithCancel(context.Background())
	srv := api.New(sctx, q, st, &fakeCanceller{})

	gate := make(chan struct{})
	defer close(gate) // frees the worker's handler on the way out
	wctx, wcancel := context.WithCancel(context.Background())
	defer wcancel()
	go q.Run(wctx, 1, func(context.Context, store.Record) { <-gate }) // the worker takes one job and holds it

	for i := 0; i < queue.Capacity+1; i++ { // fill: one held + a full buffer
		if err := q.Enqueue(wctx, store.Record{ID: strconv.Itoa(i)}); err != nil {
			t.Fatalf("fill Enqueue(%d): %v", i, err)
		}
	}

	resp := make(chan *httptest.ResponseRecorder, 1)
	go func() { // this POST blocks in Enqueue: the queue is full
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPost, "/jobs",
			strings.NewReader(`{"command": ["true"]}`))
		srv.Handler().ServeHTTP(rec, req)
		resp <- rec
	}()

	scancel() // the server shuts down while that POST is blocked

	select {
	case rec := <-resp:
		if rec.Code != http.StatusServiceUnavailable {
			t.Fatalf("status = %d, want 503 (body: %s)", rec.Code, rec.Body.String())
		}
		var body map[string]string
		if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
			t.Errorf("body is not JSON: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("blocked POST never got its 503 after server shutdown")
	}
}

// TestConcurrentRequestsRaceFree pins Review Focus 4 at the API level: a
// real httptest server, one POST running a command while three GETs hammer
// a known id. If the store mutex were missing, -race would fire here —
// net/http serves every request on its own goroutine by design.
func TestConcurrentRequestsRaceFree(t *testing.T) {
	srv, st, _, _ := newServer(t)
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

/*
TestDeleteJobPending pins the happy path: DELETE on a non-terminal job asks
the canceller and answers 202 with the record as it stands. Cancellation is
asynchronous — the body may still say pending.
*/
func TestDeleteJobPending(t *testing.T) {
	srv, st, _, c := newServer(t)
	if err := st.Save(store.Record{ID: "job1", Command: []string{"sleep", "10"}, Status: job.StatusPending}); err != nil {
		t.Fatal(err)
	}

	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodDelete, "/jobs/job1", nil))

	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want 202 (body: %s)", rec.Code, rec.Body.String())
	}
	if got := c.cancelled(); len(got) != 1 || got[0] != "job1" {
		t.Errorf("canceller saw %v, want [job1]", got)
	}
	var body store.Record
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("body is not JSON: %v", err)
	}
	if body.ID != "job1" {
		t.Errorf("body id = %q, want job1", body.ID)
	}
}

/*
TestDeleteJobTerminal409 pins the state machine: a finished job cannot be
cancelled, and the canceller is never consulted.
*/
func TestDeleteJobTerminal409(t *testing.T) {
	for _, status := range []job.Status{job.StatusSucceeded, job.StatusFailed, job.StatusCancelled} {
		t.Run(status.String(), func(t *testing.T) {
			srv, st, _, c := newServer(t)
			if err := st.Save(store.Record{ID: "done", Command: []string{"true"}, Status: status}); err != nil {
				t.Fatal(err)
			}

			rec := httptest.NewRecorder()
			srv.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodDelete, "/jobs/done", nil))

			if rec.Code != http.StatusConflict {
				t.Fatalf("status = %d, want 409", rec.Code)
			}
			if got := c.cancelled(); len(got) != 0 {
				t.Errorf("canceller saw %v, want no call for a terminal job", got)
			}
		})
	}
}

/*
TestDeleteJobMissing404 pins the not-found contract.
*/
func TestDeleteJobMissing404(t *testing.T) {
	srv, _, _, _ := newServer(t)

	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodDelete, "/jobs/ghost", nil))

	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", rec.Code)
	}
}

/*
TestDeleteJobRunning202 pins that a running record is cancellable too.
*/
func TestDeleteJobRunning202(t *testing.T) {
	srv, st, _, c := newServer(t)
	if err := st.Save(store.Record{ID: "run", Command: []string{"sleep", "10"}, Status: job.StatusRunning}); err != nil {
		t.Fatal(err)
	}

	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodDelete, "/jobs/run", nil))

	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want 202", rec.Code)
	}
	if got := c.cancelled(); len(got) != 1 || got[0] != "run" {
		t.Errorf("canceller saw %v, want [run]", got)
	}
}

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
	srv, st, q, _ := newServer(t)
	h := srv.Handler()

	first := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/jobs", strings.NewReader(`{"command": ["echo", "one"]}`))
	req.Header.Set("Idempotency-Key", "k1")
	h.ServeHTTP(first, req)
	if first.Code != http.StatusAccepted {
		t.Fatalf("first status = %d, want 202", first.Code)
	}
	var original store.Record
	if err := json.Unmarshal(first.Body.Bytes(), &original); err != nil {
		t.Fatal(err)
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
		t.Fatalf("store holds %d records, want 1", len(all))
	}
	// A refused reuse must leave the original exactly as it was: the key
	// still names the first intent, command included.
	if all[0].ID != original.ID || !slices.Equal(all[0].Command, original.Command) || all[0].Status != original.Status {
		t.Errorf("stored record = %+v, want the original %+v untouched", all[0], original)
	}

	// And only the original submission is queued: one worker sees one job.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	seen := make(chan store.Record, 2)
	go q.Run(ctx, 1, func(_ context.Context, r store.Record) { seen <- r })
	select {
	case got := <-seen:
		if got.ID != original.ID || !slices.Equal(got.Command, original.Command) {
			t.Errorf("worker saw %+v, want the original %+v", got, original)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("the original submission never reached a worker")
	}
	select {
	case got := <-seen:
		t.Fatalf("the 409'd submission reached a worker too: %+v", got)
	case <-time.After(50 * time.Millisecond):
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
	type result struct {
		code int
		id   string
	}
	out := make(chan result, n)
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
			out <- result{resp.StatusCode, rec.ID}
		}()
	}
	wg.Wait()
	close(out)

	results := make([]result, 0, n)
	for r := range out {
		results = append(results, r)
	}
	if len(results) != n {
		t.Fatalf("got %d responses, want %d", len(results), n)
	}

	var accepted, replayed int
	var want string
	for _, r := range results {
		if r.id == "" {
			t.Error("a response carried no id")
		}
		if want == "" {
			want = r.id
		}
		if r.id != want {
			t.Errorf("response id = %q, want %q", r.id, want)
		}
		switch r.code {
		case http.StatusAccepted:
			accepted++
		case http.StatusOK:
			replayed++
		default:
			t.Errorf("status = %d, want 200 or 202", r.code)
		}
	}
	if accepted != 1 || replayed != n-1 {
		t.Errorf("got %d accepted and %d replayed, want 1 and %d", accepted, replayed, n-1)
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
TestPostJobTrailingJSONRejected pins "the whole body is the document": one
decoded value followed by garbage is a malformed request, not a valid job.
It must not create a record or bind the key, so a corrected retry still works.
*/
func TestPostJobTrailingJSONRejected(t *testing.T) {
	srv, st, _, _ := newServer(t)
	h := srv.Handler()

	bad := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/jobs",
		strings.NewReader(`{"command": ["echo", "hi"]} garbage`))
	req.Header.Set("Idempotency-Key", "k1")
	h.ServeHTTP(bad, req)
	if bad.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400 (body: %s)", bad.Code, bad.Body.String())
	}
	all, err := st.List()
	if err != nil {
		t.Fatal(err)
	}
	if len(all) != 0 {
		t.Fatalf("stored %d records for a malformed body, want none", len(all))
	}

	good := httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodPost, "/jobs", strings.NewReader(`{"command": ["echo", "hi"]}`))
	req.Header.Set("Idempotency-Key", "k1")
	h.ServeHTTP(good, req)
	if good.Code != http.StatusAccepted {
		t.Fatalf("retry status = %d, want 202 — the key must not be burned", good.Code)
	}
}
