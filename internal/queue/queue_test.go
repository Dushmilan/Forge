package queue_test

import (
	"context"
	"errors"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/Dushmilan/Forge/internal/queue"
	"github.com/Dushmilan/Forge/internal/store"
)

// TestRunDeliversInOrder pins the queue's contract (Review Focus 4): every
// enqueued record reaches the handler exactly once, in FIFO order. The
// deadline is a safety net on a channel that MUST fire — the test fails if
// delivery doesn't happen, and cannot flake because it never guesses how
// long the goroutine needs.
func TestRunDeliversInOrder(t *testing.T) {
	q := queue.New()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	got := make(chan string, 3)
	runDone := make(chan struct{})
	go func() {
		q.Run(ctx, 1, func(rec store.Record) { got <- rec.ID })
		close(runDone)
	}()

	for _, id := range []string{"a", "b", "c"} {
		if err := q.Enqueue(ctx, store.Record{ID: id}); err != nil {
			t.Fatalf("Enqueue(%s): %v", id, err)
		}
	}

	var ids []string
	for i := 0; i < 3; i++ {
		select {
		case id := <-got:
			ids = append(ids, id)
		case <-time.After(2 * time.Second):
			t.Fatalf("deadline waiting for handler; got %v so far", ids)
		}
	}
	if strings.Join(ids, ",") != "a,b,c" {
		t.Errorf("delivery order = %v, want [a b c]", ids)
	}

	// Exactly once: nothing else may arrive after the three we enqueued.
	select {
	case id := <-got:
		t.Fatalf("handler received a duplicate/extra record: %q", id)
	default:
	}
}

// TestEnqueueAfterCancelReturnsError pins the no-deadlock guarantee: once
// ctx is cancelled the worker may be gone, so a send could block forever —
// Enqueue must notice the cancellation instead. The buffer is filled to
// capacity first, because a send into free buffer space succeeds even with
// no worker alive.
func TestEnqueueAfterCancelReturnsError(t *testing.T) {
	q := queue.New()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	for i := 0; i < queue.Capacity; i++ {
		if err := q.Enqueue(ctx, store.Record{ID: strconv.Itoa(i)}); err != nil {
			t.Fatalf("fill Enqueue(%d): %v", i, err)
		}
	}
	cancel()

	errDone := make(chan error, 1)
	go func() { errDone <- q.Enqueue(ctx, store.Record{ID: "late"}) }()
	select {
	case err := <-errDone:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Enqueue after cancel = %v, want wrapped context.Canceled", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Enqueue blocked after ctx cancel — shutdown deadlock")
	}
}

// TestEnqueueBackpressure pins the blocking-send contract: a full queue
// makes Enqueue wait, and freeing a slot releases it. The handler is gated
// so the worker holds one record; the buffer then fills to Capacity, and
// the overflow Enqueue must sit blocked until the worker consumes a slot.
func TestEnqueueBackpressure(t *testing.T) {
	q := queue.New()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	gate := make(chan struct{})
	defer close(gate) // frees any handler still waiting, on the way out
	go q.Run(ctx, 1, func(rec store.Record) { <-gate })

	mustEnqueue(t, ctx, q, store.Record{ID: "held"}) // the worker takes this one and blocks on gate
	for i := 0; i < queue.Capacity; i++ {
		mustEnqueue(t, ctx, q, store.Record{ID: strconv.Itoa(i)}) // fills the buffer
	}

	blocked := make(chan error, 1)
	go func() { blocked <- q.Enqueue(ctx, store.Record{ID: "overflow"}) }()
	select {
	case err := <-blocked:
		t.Fatalf("Enqueue returned %v with a full queue — no backpressure", err)
	case <-time.After(50 * time.Millisecond):
		// good: the overflow send is still waiting
	}

	gate <- struct{}{} // free "held"; the worker loops, takes a buffered record, and opens a slot

	select {
	case err := <-blocked:
		if err != nil {
			t.Fatalf("unblocked Enqueue = %v, want nil", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Enqueue never completed after a slot was freed")
	}
}

// mustEnqueue enqueues or fails the test — test plumbing so the happy
// paths above stay readable.
func mustEnqueue(t *testing.T, ctx context.Context, q *queue.Queue, rec store.Record) {
	t.Helper()
	if err := q.Enqueue(ctx, rec); err != nil {
		t.Fatalf("Enqueue(%s): %v", rec.ID, err)
	}
}

// TestRunReturnsWhenIdle pins: an idle worker exits promptly on cancel —
// there is nothing to finish.
func TestRunReturnsWhenIdle(t *testing.T) {
	q := queue.New()
	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan struct{})
	go func() {
		q.Run(ctx, 1, func(store.Record) {})
		close(done)
	}()

	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("idle Run did not return after ctx cancel")
	}
}

// TestRunFinishesCurrentAndDropsQueued pins the shutdown contract (Review
// Focus 2): after cancel, the record being handled runs to completion, the
// record still buffered is never delivered, and Run returns only after the
// in-flight handler has returned.
func TestRunFinishesCurrentAndDropsQueued(t *testing.T) {
	q := queue.New()
	ctx, cancel := context.WithCancel(context.Background())

	entered := make(chan struct{}, 2) // handler-entry signal, one slot per job
	gate := make(chan struct{})       // blocks the handler until the test frees it
	handled := make(chan string, 2)
	runDone := make(chan struct{})
	go func() {
		q.Run(ctx, 1, func(rec store.Record) {
			entered <- struct{}{}
			<-gate
			handled <- rec.ID
		})
		close(runDone)
	}()

	mustEnqueue(t, ctx, q, store.Record{ID: "in-flight"})
	select { // wait until the worker has actually picked "in-flight" up
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("worker never picked up the first record")
	}
	mustEnqueue(t, ctx, q, store.Record{ID: "queued"}) // buffer; the worker is busy

	cancel()
	select { // finish-current: Run must NOT return while the handler runs
	case <-runDone:
		t.Fatal("Run returned while a handler was still running")
	case <-time.After(50 * time.Millisecond):
	}

	gate <- struct{}{} // free the in-flight handler

	select {
	case <-runDone:
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return after the in-flight handler finished")
	}
	if id := <-handled; id != "in-flight" {
		t.Errorf("handler saw %q, want in-flight", id)
	}
	select { // drop-queued: "queued" must never have been delivered
	case id := <-handled:
		t.Fatalf("buffered record was delivered after cancel: %q", id)
	default:
	}
}

/*
TestRunExecutesConcurrently proves that Run(ctx, 3, ...) actually spawns 3
concurrent workers: 3 enqueued jobs all enter the handler before ANY of
them is allowed to return. A single worker (or a loop that forgot `go`)
would deadlock on the barrier.
*/
func TestRunExecutesConcurrently(t *testing.T) {
	const workers = 3
	q := queue.New()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	arrived := make(chan string, workers)
	release := make(chan struct{})
	runDone := make(chan struct{})
	go func() {
		q.Run(ctx, workers, func(rec store.Record) {
			arrived <- rec.ID
			<-release
		})
		close(runDone)
	}()

	for i := 0; i < workers; i++ {
		mustEnqueue(t, ctx, q, store.Record{ID: strconv.Itoa(i)})
	}

	// All 3 workers must be inside the handler at once before we release them.
	for i := 0; i < workers; i++ {
		select {
		case <-arrived:
		case <-time.After(2 * time.Second):
			t.Fatalf("only %d of %d workers entered handler concurrently", i, workers)
		}
	}
	close(release)

	cancel()
	select {
	case <-runDone:
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return after cancel")
	}
}

/*
TestRunFastJobsBypassSlowWorker pins Review Focus 1 (why V3 exists): when
worker 1 is occupied by a slow job, worker 2 still drains subsequent fast
jobs to completion instead of stalling behind the slow one.
*/
func TestRunFastJobsBypassSlowWorker(t *testing.T) {
	q := queue.New()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	slowEntered := make(chan struct{})
	slowGate := make(chan struct{})
	defer close(slowGate)
	fastDone := make(chan string, 2)

	go q.Run(ctx, 2, func(rec store.Record) {
		if rec.ID == "slow" {
			close(slowEntered)
			<-slowGate
			return
		}
		fastDone <- rec.ID
	})

	mustEnqueue(t, ctx, q, store.Record{ID: "slow"})
	select {
	case <-slowEntered:
	case <-time.After(2 * time.Second):
		t.Fatal("slow job never entered handler")
	}

	mustEnqueue(t, ctx, q, store.Record{ID: "fast-1"})
	mustEnqueue(t, ctx, q, store.Record{ID: "fast-2"})

	for i := 0; i < 2; i++ {
		select {
		case <-fastDone:
		case <-time.After(2 * time.Second):
			t.Fatalf("fast job %d blocked behind slow job", i+1)
		}
	}
}

// TestEnqueueAfterCancelWithFreeBufferReturnsError pins the deterministic
// half of the no-deadlock guarantee the API's 503 relies on: with a CANCELLED
// ctx, Enqueue must error even when the buffer has free space. Both select
// cases are ready in that situation, and select chooses pseudo-randomly —
// without an explicit priority check, a shutdown-race POST would slip into
// the buffer ~half the time and get a 202 instead of a 503.
func TestEnqueueAfterCancelWithFreeBufferReturnsError(t *testing.T) {
	q := queue.New()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cancel()

	for i := 0; i < 20; i++ { // repeated: the bug this pins is a coin flip
		errDone := make(chan error, 1)
		go func() { errDone <- q.Enqueue(ctx, store.Record{ID: strconv.Itoa(i)}) }()
		select {
		case err := <-errDone:
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("Enqueue after cancel = %v, want wrapped context.Canceled", err)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("Enqueue blocked after ctx cancel — shutdown deadlock")
		}
	}
}
