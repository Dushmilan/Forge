/*
Package worker executes queued records: it marks a job running, runs it
through the executor seam, retries failed attempts with backoff, and records
the outcome. V4 adds the job's lifetime — retries, a deadline, and a cancel
signal — on top of V2/V3's worker loop.
*/
package worker

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"sync"
	"time"

	"github.com/Dushmilan/Forge/internal/executor"
	"github.com/Dushmilan/Forge/internal/job"
	"github.com/Dushmilan/Forge/internal/store"
)

// Config is the retry/timeout policy. All durations are per job; Backoff is
// the first delay and doubles after every failed attempt.
type Config struct {
	MaxAttempts int
	Backoff     time.Duration
	Timeout     time.Duration
}

// Worker is safe for concurrent use by N queue workers. Every shared field
// below is either immutable after New or guarded by a mutex.
type Worker struct {
	st     *store.Store
	exec   executor.Runner
	cfg    Config
	stderr io.Writer

	errMu sync.Mutex // serializes stderr writes from concurrent workers

	/*
		mu guards the two maps that make cancellation reach a specific
		record: running holds a live attempt's CancelFunc; cancelled holds
		tombstones for records still buffered in the queue channel.
	*/
	mu        sync.Mutex
	running   map[string]context.CancelFunc
	cancelled map[string]struct{}
}

// New wires a Worker. cfg is expected to be valid (runServe validates flags
// before constructing it); tests construct explicit configs.
func New(st *store.Store, exec executor.Runner, cfg Config, stderr io.Writer) *Worker {
	return &Worker{
		st:        st,
		exec:      exec,
		cfg:       cfg,
		stderr:    stderr,
		running:   make(map[string]context.CancelFunc),
		cancelled: make(map[string]struct{}),
	}
}

/*
Handle runs one record through the retry loop. It is the queue's handler
shape: the lifecycle ctx comes from Queue.Run and is only consulted between
attempts, never handed to the executor — V3's shutdown contract says a
running job finishes.
*/
func (w *Worker) Handle(ctx context.Context, rec store.Record) {
	execCtx, release, ok := w.claim(ctx, rec.ID)
	if !ok {
		rec.Status = job.StatusCancelled
		rec.Err = "job cancelled before it started"
		w.save(rec)
		return
	}
	defer release() // unregister only after the final save: Cancel must see the entry until then

	for attempt := 1; attempt <= w.cfg.MaxAttempts; attempt++ {
		rec.Attempts = attempt
		rec.Status = job.StatusRunning
		if !w.save(rec) {
			return
		}

		res, err := w.exec.Run(execCtx, job.Job{ID: rec.ID, Command: rec.Command})
		rec.Result = res
		if err == nil {
			rec.Status = job.StatusSucceeded
			rec.Err = ""
			w.save(rec)
			return
		}

		/*
			A completed execution context is the authoritative reason the
			run ended early. Treat it as cancellation, never as a failed
			attempt to retry — and never retry a timed-out job.
		*/
		if cerr := execCtx.Err(); cerr != nil {
			w.finishCancelled(rec, cerr)
			return
		}

		rec.Err = err.Error()
		if attempt == w.cfg.MaxAttempts {
			rec.Status = job.StatusFailed
			w.save(rec)
			return
		}

		if !w.wait(ctx, execCtx, backoffFor(w.cfg.Backoff, attempt)) {
			if cerr := execCtx.Err(); cerr != nil {
				w.finishCancelled(rec, cerr)
			} else {
				rec.Status = job.StatusFailed
				w.save(rec)
			}
			return
		}
	}
}

/*
claim builds the per-job execution context and registers its cancel func
atomically with the tombstone check. context.WithoutCancel is the key: the
attempt keeps the parent's values but not its cancellation, so Ctrl-C cannot
kill a running job (V3), while WithTimeout and Cancel still can.
*/
func (w *Worker) claim(parent context.Context, id string) (context.Context, func(), bool) {
	execCtx, cancel := context.WithTimeout(context.WithoutCancel(parent), w.cfg.Timeout)

	w.mu.Lock()
	if _, cancelled := w.cancelled[id]; cancelled {
		delete(w.cancelled, id)
		w.mu.Unlock()
		cancel()
		return nil, nil, false
	}
	w.running[id] = cancel
	w.mu.Unlock()

	release := func() {
		w.mu.Lock()
		delete(w.running, id)
		w.mu.Unlock()
		cancel()
	}
	return execCtx, release, true
}

/*
Cancel is called by the API. If the record already claimed the ID, it stops
the attempt; otherwise it plants a tombstone so the worker skips the job
when it is finally dequeued. It never writes the store — only the worker
goroutine owns status transitions, which is what keeps cancel/complete free
of a compare-and-swap race.
*/
func (w *Worker) Cancel(id string) {
	w.mu.Lock()
	cancel, running := w.running[id]
	if !running {
		w.cancelled[id] = struct{}{}
	}
	w.mu.Unlock()
	if running {
		cancel()
	}
}

// finishCancelled records the cancel/timeout cause in the record.
func (w *Worker) finishCancelled(rec store.Record, cause error) {
	rec.Status = job.StatusCancelled
	if errors.Is(cause, context.DeadlineExceeded) {
		rec.Err = fmt.Sprintf("job timed out after %s", w.cfg.Timeout)
	} else {
		rec.Err = "job cancelled"
	}
	w.save(rec)
}

/*
backoffFor computes base * 2^(attempt-1) without overflowing time.Duration.
Doubling an int64 wraps negative after ~63 steps; time.NewTimer treats a
non-positive delay as "fire now", so an unbounded shift silently becomes a
hot retry loop. Saturating at the largest Duration keeps the wait long.
*/
func backoffFor(base time.Duration, attempt int) time.Duration {
	if attempt < 1 {
		return base
	}
	const max = time.Duration(math.MaxInt64)
	d := base
	for i := 1; i < attempt; i++ {
		if d > max/2 {
			return max
		}
		d *= 2
	}
	return d
}

/*
wait sleeps for d, but wakes early when either context ends: shutdown must
not be delayed by a backoff timer, and a cancel must interrupt the wait
immediately. defer timer.Stop is the cleanup that prevents a fired timer's
resources from lingering when a context branch wins.
*/
func (w *Worker) wait(ctx, execCtx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-execCtx.Done():
		return false
	case <-t.C:
		/*
			Both cases can be ready at once (the timer fired as shutdown
			landed) and select chooses pseudo-randomly — so the timer must
			re-check the contexts, or one more attempt starts after Ctrl-C.
			This is V2's post-select priority rule in queue.work, applied to
			a timer instead of a channel receive.
		*/
	}
	return ctx.Err() == nil && execCtx.Err() == nil
}

/*
save persists rec and reports failure to stderr. The mutex is the same
reasoning as V3: N workers share this Writer and an arbitrary io.Writer is
not goroutine-safe.
*/
func (w *Worker) save(rec store.Record) bool {
	if err := w.st.Save(rec); err != nil {
		w.errMu.Lock()
		defer w.errMu.Unlock()
		fmt.Fprintf(w.stderr, "forge: save job %s: %v\n", rec.ID, err)
		return false
	}
	return true
}
