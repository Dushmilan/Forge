package queue

import (
	"context"
	"testing"

	"github.com/Dushmilan/Forge/internal/store"
)

/*
TestPopResignalsWhenWorkRemains pins the wakeup handshake that no black-box
test can see: a worker can only sleep after it popped an empty queue, so a
pop that leaves records behind must put a token back on notify or its peers
stay asleep with work waiting. Enqueue coalesces its signals (capacity 1),
which is why the token is drained before the pop.
*/
func TestPopResignalsWhenWorkRemains(t *testing.T) {
	q := New()
	ctx := context.Background()
	if err := q.Enqueue(ctx, store.Record{ID: "a"}); err != nil {
		t.Fatalf("Enqueue a: %v", err)
	}
	if err := q.Enqueue(ctx, store.Record{ID: "b"}); err != nil {
		t.Fatalf("Enqueue b: %v", err)
	}

	select {
	case <-q.notify:
	default:
		t.Fatal("Enqueue did not signal")
	}

	if _, ok := q.pop(); !ok {
		t.Fatal("pop reported empty with two records pending")
	}

	select {
	case <-q.notify:
	default:
		t.Fatal("pop left work behind without re-signalling")
	}
}
