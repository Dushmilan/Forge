package queue

import (
	"container/heap"
	"testing"

	"github.com/Dushmilan/Forge/internal/job"
	"github.com/Dushmilan/Forge/internal/store"
)

/*
TestPendingHeapLess pins the policy in one place: higher priority is "less"
(container/heap is a min-heap), and equal priorities fall back to arrival
order. A heap is not stable, so the sequence is what makes FIFO true.
*/
func TestPendingHeapLess(t *testing.T) {
	h := pendingHeap{
		{rec: store.Record{Priority: job.PriorityLow}, seq: 1},
		{rec: store.Record{Priority: job.PriorityHigh}, seq: 2},
		{rec: store.Record{Priority: job.PriorityHigh}, seq: 3},
	}
	if !h.Less(1, 0) {
		t.Error("high should sort before low")
	}
	if !h.Less(1, 2) {
		t.Error("earlier sequence should sort before later at equal priority")
	}
	if h.Less(2, 1) {
		t.Error("later sequence must not sort before earlier")
	}
}

/*
TestPendingHeapPopsBestFirst pushes out of order and drains through
container/heap, proving the type satisfies the interface those functions
expect — the compile-time assertion alone only proves the method set.
*/
func TestPendingHeapPopsBestFirst(t *testing.T) {
	want := []string{"crit", "high-1", "high-2", "normal", "low"}
	h := &pendingHeap{}
	heap.Push(h, item{rec: store.Record{ID: "normal", Priority: job.PriorityNormal}, seq: 4})
	heap.Push(h, item{rec: store.Record{ID: "low", Priority: job.PriorityLow}, seq: 5})
	heap.Push(h, item{rec: store.Record{ID: "high-1", Priority: job.PriorityHigh}, seq: 2})
	heap.Push(h, item{rec: store.Record{ID: "crit", Priority: job.PriorityCritical}, seq: 1})
	heap.Push(h, item{rec: store.Record{ID: "high-2", Priority: job.PriorityHigh}, seq: 3})

	for _, id := range want {
		it := heap.Pop(h).(item)
		if it.rec.ID != id {
			t.Fatalf("popped %q, want %q", it.rec.ID, id)
		}
	}
	if h.Len() != 0 {
		t.Errorf("%d items left, want 0", h.Len())
	}
}
