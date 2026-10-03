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
