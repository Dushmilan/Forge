package api_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Dushmilan/Forge/internal/api"
	"github.com/Dushmilan/Forge/internal/job"
	"github.com/Dushmilan/Forge/internal/queue"
	"github.com/Dushmilan/Forge/internal/store"
)

// newServer builds a Server over a temp-dir store and a fresh queue. No
// executor: as of V2 the API hands jobs off, it never runs them.
func newServer(t *testing.T) (*api.Server, *store.Store, *queue.Queue) {
	t.Helper()
	st := store.New(t.TempDir())
	q := queue.New()
	return api.New(q, st), st, q
}

// TestGetJobFound pins the happy path: 200, JSON content type, and a body
// whose id matches the path.
func TestGetJobFound(t *testing.T) {
	srv, st, _ := newServer(t)
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
	srv, _, _ := newServer(t)

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
	srv, _, _ := newServer(t)
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

// TestPostJobEnqueuesAndPersists pins the V2 headline: POST answers 202
// with the PENDING record, that record is persisted BEFORE the response,
// and it reaches a worker — the request itself never waits for the run.
func TestPostJobEnqueuesAndPersists(t *testing.T) {
	srv, st, q := newServer(t)

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
	go q.Run(ctx, func(r store.Record) { seen <- r })
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
			srv, _, _ := newServer(t)

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
	srv := api.New(q, store.New(filepath.Join(blocker, "data")))

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
	go q.Run(ctx, func(r store.Record) { seen <- r })
	select {
	case got := <-seen:
		t.Fatalf("a 500'd submission reached the worker: %+v", got)
	case <-time.After(50 * time.Millisecond):
	}
}

// TestPostJobShutdown503 pins Review Focus 1: a POST whose request context
// is already dead (server shutting down) gets 503 + JSON error instead of
// blocking forever, and the pending record it saved stays pending —
// nothing is enqueued.
func TestPostJobShutdown503(t *testing.T) {
	dir := t.TempDir()
	q := queue.New()
	srv := api.New(q, store.New(dir))

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
	go q.Run(wctx, func(r store.Record) { seen <- r })
	select {
	case got := <-seen:
		t.Fatalf("the 503'd record reached the worker: %+v", got)
	case <-time.After(50 * time.Millisecond):
	}
}

// TestConcurrentRequestsRaceFree pins Review Focus 4 at the API level: a
// real httptest server, one POST running a command while three GETs hammer
// a known id. If the store mutex were missing, -race would fire here —
// net/http serves every request on its own goroutine by design.
func TestConcurrentRequestsRaceFree(t *testing.T) {
	srv, st, _ := newServer(t)
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
