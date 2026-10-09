package queue_test

import (
	"context"
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
		q.Enqueue(store.Record{ID: id})
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
