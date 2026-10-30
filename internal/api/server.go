/*
Package api exposes Forge over HTTP: submit a job, fetch a job, cancel a job.

Handlers never run jobs — POST saves a pending record, hands it to the
queue, and answers 202 immediately; the worker goroutine started by
cmd/forge does the running. The request path stays fast and this package
never touches os/exec.
*/
package api

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"slices"

	"github.com/Dushmilan/Forge/internal/job"
	"github.com/Dushmilan/Forge/internal/queue"
	"github.com/Dushmilan/Forge/internal/store"
)

/*
Server is the HTTP front end. It holds only the collaborators every
handler needs: the queue jobs are handed to, the store their records
live in, the server's lifecycle context, and the canceller V4's DELETE
route asks to stop a job.
*/
type Server struct {
	q   *queue.Queue
	st  *store.Store
	ctx context.Context
	c   Canceller
}

/*
Canceller is the seam between HTTP and execution (the worker implements it).
Defined here, where it is consumed, and deliberately tiny: the API only asks
"stop this job", never how.
*/
type Canceller interface {
	Cancel(id string)
}

/*
New wires the server. The queue is V2's seam: POST enqueues, the worker
dequeues, and this package cannot tell the difference between a live
worker and a test's — which is exactly why the tests need no executor.

ctx is the SERVER's lifecycle context (cancelled when the server shuts
down). It is not a request's context: http.Server.Shutdown waits for
active requests but never cancels them, so Enqueue needs both signals —
see createJob.
*/
func New(ctx context.Context, q *queue.Queue, st *store.Store, c Canceller) *Server {
	return &Server{q: q, st: st, ctx: ctx, c: c}
}

// Handler returns the routes on a Go 1.22 method+pattern mux: the verb is
// part of the pattern, so routing mistakes become 405s from the standard
// library instead of hand-written if-chains. {id} is captured into
// r.PathValue("id") — no third-party router needed.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /jobs/{id}", s.getJob)
	mux.HandleFunc("POST /jobs", s.createJob)
	mux.HandleFunc("DELETE /jobs/{id}", s.cancelJob)
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

/*
cancelJob asks the worker to stop a job. Cancellation is asynchronous: the
worker owns the status write, so this answers 202 and leaves the current
record in the body. A terminal job is a conflict — nothing left to cancel.
*/
func (s *Server) cancelJob(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	id := r.PathValue("id")
	rec, err := s.st.Get(id)
	switch {
	case errors.Is(err, store.ErrNotFound):
		writeErr(w, http.StatusNotFound, err)
		return
	case err != nil:
		writeErr(w, http.StatusInternalServerError, err)
		return
	}
	if rec.Status.Terminal() {
		writeErr(w, http.StatusConflict, fmt.Errorf("api: job %s is already %s", id, rec.Status))
		return
	}

	s.c.Cancel(id)

	w.WriteHeader(http.StatusAccepted)
	_ = json.NewEncoder(w).Encode(rec)
}

// createJob is the V2 core: decode → identify → validate → persist pending
// → enqueue → 202, in that order, all inside one HTTP request. Each step
// answers with the status code its failure actually deserves. The job
// itself runs later, on the worker — this handler never blocks on it.
func (s *Server) createJob(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

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

	// Step 1: the body must be the expected JSON shape. A body that will
	// not decode is a client's mistake (400), never a 500 and never a
	// panic — json.Decoder returns the problem as an ordinary error.
	var in struct {
		Command []string `json:"command"`
	}
	dec := json.NewDecoder(r.Body)
	if err := dec.Decode(&in); err != nil {
		writeErr(w, http.StatusBadRequest, fmt.Errorf("api: bad request body: %w", err))
		return
	}
	/*
		One Decode reads one JSON value and stops; `{"command": [...]} junk`
		would otherwise be accepted. A second Decode must report io.EOF —
		anything else means there was a second document, or trailing
		garbage, and the whole body was not the request.
	*/
	if err := dec.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		writeErr(w, http.StatusBadRequest, fmt.Errorf("api: bad request body: expected one JSON object"))
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

	// Step 4: persist the job's intent BEFORE answering. A pending record
	// on disk is what makes 202 honest: even if the process dies before
	// the worker picks the job up, the submission is not lost.
	//
	// Create is the admission path, not Save: with a key it refuses to make
	// a second record for the same intent, and the check plus the insert
	// happen under the store's one lock — two concurrent retries cannot
	// both create.
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

	// Step 5: hand off to the worker. Enqueue must respect TWO lifetimes:
	// r.Context() ends when the request does (client disconnect), and the
	// server's lifecycle context ends at shutdown — Shutdown does NOT
	// cancel request contexts, so without the merge a POST blocked on a
	// full queue would hang until the Shutdown deadline and then get its
	// connection dropped instead of an answer. context.AfterFunc (Go 1.21)
	// bridges the second signal into the first: one cancel, two causes.
	ectx, cancel := context.WithCancel(r.Context())
	defer cancel()
	stop := context.AfterFunc(s.ctx, cancel)
	defer stop()
	if err := s.q.Enqueue(ectx, rec); err != nil {
		writeErr(w, http.StatusServiceUnavailable, err)
		return
	}

	w.WriteHeader(http.StatusAccepted)
	_ = json.NewEncoder(w).Encode(rec)
}

// maxIdempotencyKeyLen bounds one record's key. Opaque bytes, but not
// unbounded: the store's file is the whole database.
const maxIdempotencyKeyLen = 255

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
