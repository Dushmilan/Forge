/*
Package queue holds records waiting to run and hands them to a pool of worker
goroutines in priority order.

V2 built this on a channel, and for a strict FIFO that is exactly right. V7
breaks the FIFO assumption: a channel cannot reorder what is already in it, so
the buffer becomes a container/heap guarded by a mutex. The channel did not
disappear — it moved. notify is a one-slot channel that wakes a sleeping
worker, and slots is a buffered channel used as a counting semaphore so the
"at most Capacity records pending" contract still holds without a second
queue.

sync.Cond is the other textbook answer and it is still wrong here: a Cond
cannot be selected against ctx.Done(), so a worker blocked on it could not
notice shutdown.
*/
package queue

import (
	"container/heap"
	"context"
	"fmt"
	"sync"

	"github.com/Dushmilan/Forge/internal/store"
)

// Capacity bounds how many records may be pending. 64 is still a decision,
// not a knob: V2 predicted this would become configurable, but the V7 need
// turned out to be ordering, not size.
const Capacity = 64

/*
Queue is an in-memory priority queue of records waiting to run.

mu guards pending and seq; everything else is channel state. The zero value is
not usable; call New.
*/
type Queue struct {
	mu      sync.Mutex
	pending pendingHeap
	// seq stamps arrival order. A heap is not stable, so equal priorities
	// need something that is.
	seq uint64
	// notify holds at most one token: "something is pending". It coalesces
	// because a worker that wakes early will simply pop again; a token per
	// record would let the buffer fill with stale wakeups.
	notify chan struct{}
	// slots is a semaphore with Capacity tokens, one held by each pending
	// record. Enqueue takes one, pop returns one — that is the whole
	// backpressure contract, unchanged from V2.
	slots chan struct{}
}

// New returns an empty Queue with Capacity slots of pending room.
func New() *Queue {
	q := &Queue{
		notify: make(chan struct{}, 1),
		slots:  make(chan struct{}, Capacity),
	}
	for i := 0; i < Capacity; i++ {
		q.slots <- struct{}{}
	}
	return q
}

/*
Enqueue adds rec to the pending set. It blocks while Capacity records are
already pending — backpressure — and returns early if ctx ends first.

Cancellation keeps V2's explicit priority check: with both cases ready
(cancelled ctx, free slot) select picks pseudo-randomly, and the API's 503
contract depends on a shutdown-race POST being refused every time, not half
the time.
*/
func (q *Queue) Enqueue(ctx context.Context, rec store.Record) error {
	select {
	case <-ctx.Done():
		return fmt.Errorf("queue: enqueue: %w", ctx.Err())
	default:
	}
	select {
	case <-q.slots:
	case <-ctx.Done():
		return fmt.Errorf("queue: enqueue: %w", ctx.Err())
	}

	q.mu.Lock()
	q.pushLocked(rec)
	q.mu.Unlock()

	/*
		A non-blocking send: if the token is already there, some worker is
		about to look at the heap anyway. Blocking here would let a slow
		worker stall an HTTP handler for no reason.
	*/
	select {
	case q.notify <- struct{}{}:
	default:
	}
	return nil
}

// pushLocked stamps rec with the next sequence number and inserts it. The
// caller holds q.mu; the suffix says so.
func (q *Queue) pushLocked(rec store.Record) {
	q.seq++
	heap.Push(&q.pending, item{rec: rec, seq: q.seq})
}

/*
pop removes the best record, or reports false when nothing is pending.

Returning the slot here — rather than in Enqueue — is what keeps the counts
honest: a record holds a token for exactly as long as it is pending, and the
token comes back the moment it starts running.
*/
func (q *Queue) pop() (store.Record, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()

	if len(q.pending) == 0 {
		return store.Record{}, false
	}
	it := heap.Pop(&q.pending).(item)

	// This record held one token, so the channel has room and the send
	// cannot block while the mutex is held.
	q.slots <- struct{}{}

	/*
		Wake a peer if work remains. Without this, one worker would drain the
		whole heap while the others slept on the single notify token.
	*/
	if len(q.pending) > 0 {
		select {
		case q.notify <- struct{}{}:
		default:
		}
	}
	return it.rec, true
}

/*
Run drains the queue across a pool of worker goroutines and blocks until
every worker has returned.

sync.WaitGroup is the fan-out/join primitive: wg.Add(1) MUST run on the
spawning goroutine before `go`, never inside the child goroutine — otherwise
wg.Wait() could race ahead of the child's Add and return before the worker
even starts.

The mutex inside pop is what makes competing consumers safe now that the
buffer is not a channel: each pop is one atomic "take the best item".
*/
func (q *Queue) Run(ctx context.Context, workers int, handler func(context.Context, store.Record)) {
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
work is one worker goroutine's lifecycle: take the best pending record, run
handler to completion, then take another. It blocks on notify only when the
heap is empty, and the select there is what lets shutdown reach a worker that
is waiting rather than working.
*/
func (q *Queue) work(ctx context.Context, handler func(context.Context, store.Record)) {
	for {
		rec, ok := q.pop()
		if !ok {
			select {
			case <-ctx.Done():
				return
			case <-q.notify:
				continue
			}
		}
		/*
			Cancellation can race the delivery — the worker may have already
			popped when ctx ended — so it re-checks before dispatching, or a
			record could start running after shutdown. This is V2's rule,
			unchanged: cancellation always wins over delivery.
		*/
		select {
		case <-ctx.Done():
			return
		default:
		}
		handler(ctx, rec)
	}
}
