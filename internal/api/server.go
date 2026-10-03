// Package api exposes Forge over HTTP: submit a job, fetch a job.
//
// Handlers are synchronous — POST runs the job before responding. That is
// V1's honest shape; V2 replaces the inline run with a queue, and this
// package's interface does not change.
package api

import (
	"encoding/json"
	"errors"
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
