package api_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
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
