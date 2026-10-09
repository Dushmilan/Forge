/*
Package api exposes Forge over HTTP: submit a job, fetch a job.

Handlers are synchronous — POST runs the job before responding. That is
V1's honest shape; V2 replaces the inline run with a queue, and this
package's interface does not change.
*/
package api

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"

	"github.com/Dushmilan/Forge/internal/executor"
	"github.com/Dushmilan/Forge/internal/job"
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
	mux.HandleFunc("POST /jobs", s.createJob)
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
