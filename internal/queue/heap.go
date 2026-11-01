package queue

import (
	"container/heap"

	"github.com/Dushmilan/Forge/internal/store"
)

/*
item pairs a record with the sequence number it arrived at. The record cannot
carry that number: it is the queue's memory of arrival order, not a property of
the job, and it must not reach the file the client can see.
*/
type item struct {
	rec store.Record
	seq uint64
}

/*
pendingHeap satisfies container/heap.Interface, which is how Go spells
"priority queue": the container is generic and the policy lives in four
methods you write. container/heap maintains the invariant but performs no
comparisons of its own.
*/
type pendingHeap []item

func (h pendingHeap) Len() int { return len(h) }

func (h pendingHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }

/*
Less defines the policy, and the name is a trap worth naming: container/heap
is a MIN-heap, so the item that should come out FIRST must report itself as
less. Higher priority therefore compares with >, and arrivals with an equal
priority fall back to the sequence number. The tie-break is not cosmetic —
heap.Pop reorders elements freely, so without it FIFO within a priority is
only accidentally true.
*/
func (h pendingHeap) Less(i, j int) bool {
	if h[i].rec.Priority != h[j].rec.Priority {
		return h[i].rec.Priority > h[j].rec.Priority
	}
	return h[i].seq < h[j].seq
}

// Push and Pop have pointer receivers because they change the slice header.
// The any parameter and return value are container/heap's signature, not a
// choice: the container has to hold whatever the caller puts in it.
func (h *pendingHeap) Push(x any) { *h = append(*h, x.(item)) }

func (h *pendingHeap) Pop() any {
	old := *h
	n := len(old)
	it := old[n-1]
	old[n-1] = item{} // zero the slot so a popped record is not retained
	*h = old[:n-1]
	return it
}

// Compile-time proof that the type satisfies what heap.Push and heap.Pop want.
var _ heap.Interface = (*pendingHeap)(nil)
