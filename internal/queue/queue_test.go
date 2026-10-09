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
		q.Run(ctx, func(rec store.Record) { got <- rec.ID })
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
	go q.Run(ctx, func(rec store.Record) { <-gate })

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
