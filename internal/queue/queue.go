/*
Package queue moves job execution off the request path: Enqueue buffers a
record on a channel, Run drains it on one worker goroutine.

The whole package is V2's idea in a few lines — the queue IS the channel,
the worker IS a goroutine in a select loop. Rebuilding a queue from a
slice + mutex + sync.Cond is the classic pre-Go mistake; a buffered chan
is already a thread-safe FIFO.
*/
package queue

import (
	"context"

	"github.com/Dushmilan/Forge/internal/store"
)

// Capacity is the queue's fixed buffer size. 64 is a decision, not a knob:
// making it configurable is V7's scheduling story, and YAGNI says wait for
// the need.
const Capacity = 64

// Queue is an in-memory FIFO of records waiting to run. The channel is the
// entire state — no slice, no mutex. The zero value is not usable; call New.
type Queue struct {
	jobs chan store.Record
}

// New returns an empty Queue with Capacity slots of buffer.
func New() *Queue {
	return &Queue{jobs: make(chan store.Record, Capacity)}
}

// Enqueue blocks until the queue has room: a full buffer makes the caller
// wait. That is backpressure — overload flows to whoever is submitting the
// job, and memory stays bounded by Capacity.
func (q *Queue) Enqueue(rec store.Record) {
	q.jobs <- rec
}

// Run drains the queue on the goroutine it is started on — callers must
// write `go q.Run(...)`. Each record is handed to handler, then the loop
// re-selects, so between jobs it can also notice ctx cancellation and
// return. Records still buffered when that happens are dropped; the API
// already saved them as pending, and they stay that way.
func (q *Queue) Run(ctx context.Context, handler func(store.Record)) {
	for {
		select {
		case <-ctx.Done():
			return
		case rec := <-q.jobs:
			handler(rec)
		}
	}
}
