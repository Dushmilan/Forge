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
	"fmt"
	"sync"

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

/*
Enqueue hands rec to the worker via select over two cases: the send, and
ctx.Done(). While the worker lives, a full buffer makes the caller wait —
backpressure. After cancellation the send case may never succeed (the
receiver is gone), so the ctx case is what keeps a late POST from
blocking http.Server.Shutdown forever. ctx.Err is wrapped so callers can
still ask errors.Is(err, context.Canceled).

Cancellation gets an explicit priority check first: with both cases ready
(cancelled ctx, free buffer) select chooses pseudo-randomly, and a
shutdown-race POST must NOT be able to slip into the buffer half the time
— the API's 503 contract depends on this being deterministic.
*/
func (q *Queue) Enqueue(ctx context.Context, rec store.Record) error {
	select {
	case <-ctx.Done():
		return fmt.Errorf("queue: enqueue: %w", ctx.Err())
	default:
	}
	select {
	case q.jobs <- rec:
		return nil
	case <-ctx.Done():
		return fmt.Errorf("queue: enqueue: %w", ctx.Err())
	}
}

/*
Run drains the queue across a pool of worker goroutines and blocks until
every worker has returned.

sync.WaitGroup is the fan-out/join primitive: wg.Add(1) MUST run on the
spawning goroutine before `go`, never inside the child goroutine — otherwise
wg.Wait() could race ahead of the child's Add and return before the worker
even starts.

Competing consumers need no extra lock: a Go channel already guarantees
each record is received by exactly one worker.
*/
func (q *Queue) Run(ctx context.Context, workers int, handler func(store.Record)) {
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done() /** decrements the counter when the worker returns **/
			q.work(ctx, handler)
		}()
	}
	wg.Wait()
}

/*
work is one worker goroutine's lifecycle: pull a record, run handler to
completion, then re-select. Between jobs it notices ctx cancellation and
returns; records still buffered stay pending.
*/
func (q *Queue) work(ctx context.Context, handler func(store.Record)) {
	for {
		select {
		case <-ctx.Done():
			return
		case rec := <-q.jobs:
			/*
				Both select cases can be ready at once (cancel raced a
				delivery), and select chooses pseudo-randomly between ready
				cases — so a cancelled worker must re-check before dispatching,
				or a buffered record could still reach the handler after
				shutdown. This inner non-blocking select is that priority
				rule: cancellation always wins over delivery.
			*/
			select {
			case <-ctx.Done():
				return
			default:
			}
			handler(rec)
		}
	}
}
