package worker

import (
	"context"
	"math"
	"testing"
	"time"
)

/*
TestBackoffForSaturates pins the arithmetic: doubling an int64 Duration
overflows after ~63 attempts. A negative or zero delay makes time.NewTimer
fire immediately, turning the tail of a long retry policy into a hot loop.
The helper must saturate at the largest Duration instead.
*/
func TestBackoffForSaturates(t *testing.T) {
	const max = time.Duration(math.MaxInt64)
	tests := []struct {
		name    string
		base    time.Duration
		attempt int
		want    time.Duration
	}{
		{"first attempt", 100 * time.Millisecond, 1, 100 * time.Millisecond},
		{"second attempt", 100 * time.Millisecond, 2, 200 * time.Millisecond},
		{"third attempt", 100 * time.Millisecond, 3, 400 * time.Millisecond},
		{"zero attempt", 100 * time.Millisecond, 0, 100 * time.Millisecond},
		{"saturates at 64", 100 * time.Millisecond, 64, max},
		{"stays saturated", 100 * time.Millisecond, 1000, max},
		{"tiny base saturates", time.Nanosecond, 64, max},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := backoffFor(tt.base, tt.attempt); got != tt.want {
				t.Errorf("backoffFor(%s, %d) = %v, want %v", tt.base, tt.attempt, got, tt.want)
			}
		})
	}
}

/*
TestWaitPrefersShutdownOverReadyTimer pins the select priority rule: when a
context is done AND the timer has expired, select chooses pseudo-randomly.
wait must re-check the contexts after the timer wins, or a shutdown can let
one more attempt start (V2 hit the same trap in queue.work). d = 0 makes the
timer immediately ready, so the old code fails about half the iterations.
*/
func TestWaitPrefersShutdownOverReadyTimer(t *testing.T) {
	w := &Worker{}
	for i := 0; i < 100; i++ {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		if w.wait(ctx, context.Background(), 0) {
			t.Fatalf("iteration %d: wait returned true with the lifecycle context already cancelled", i)
		}
	}
}

/*
TestWaitPrefersJobCancelOverReadyTimer is the execCtx half: DELETE/timeout
must win over an expired backoff timer for the same reason.
*/
func TestWaitPrefersJobCancelOverReadyTimer(t *testing.T) {
	w := &Worker{}
	for i := 0; i < 100; i++ {
		execCtx, cancel := context.WithCancel(context.Background())
		cancel()
		if w.wait(context.Background(), execCtx, 0) {
			t.Fatalf("iteration %d: wait returned true with the job context already cancelled", i)
		}
	}
}
