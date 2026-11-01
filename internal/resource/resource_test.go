package resource_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/Dushmilan/Forge/internal/resource"
)

/*
TestAcquireRelease pins the semaphore itself: taking a unit makes it
unavailable, and the returned closure gives it back. The blocked goroutine is
how the test observes "unavailable" without sleeping long enough to hope.
*/
func TestAcquireRelease(t *testing.T) {
	p := resource.New(map[string]int{"gpu": 1})
	release, err := p.Acquire(context.Background(), []string{"gpu"})
	if err != nil {
		t.Fatal(err)
	}

	blocked := make(chan struct{})
	go func() {
		defer close(blocked)
		r, err := p.Acquire(context.Background(), []string{"gpu"})
		if err == nil {
			r()
		}
	}()

	select {
	case <-blocked:
		t.Fatal("second acquire returned while the unit was held")
	case <-time.After(50 * time.Millisecond):
	}

	release()
	select {
	case <-blocked:
	case <-time.After(2 * time.Second):
		t.Fatal("release did not wake the waiter")
	}
}

/*
TestAcquireCancelledReturnsEveryHeldUnit pins the leak that would be invisible
until it mattered: a cancelled acquisition must give back the units it already
took. A leak leaves capacity configured and permanently unreachable.
*/
func TestAcquireCancelledReturnsEveryHeldUnit(t *testing.T) {
	p := resource.New(map[string]int{"a": 1, "b": 1})
	hold, err := p.Acquire(context.Background(), []string{"b"})
	if err != nil {
		t.Fatal(err)
	}
	defer hold()

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() {
		// Sorted order takes a first, then blocks on b.
		_, err := p.Acquire(ctx, []string{"a", "b"})
		errCh <- err
	}()

	cancel()
	select {
	case err := <-errCh:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Acquire = %v, want context.Canceled", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("cancelled acquire did not return")
	}

	// a must be free again, or the next acquire blocks forever.
	ctx2, cancel2 := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel2()
	release, err := p.Acquire(ctx2, []string{"a"})
	if err != nil {
		t.Fatalf("a was not returned after cancellation: %v", err)
	}
	release()
}

/*
TestAcquireOrdersGlobally pins the deadlock fix: opposite name order must not
let two jobs each hold one unit and wait for the other. Both goroutines take a
first because Acquire sorts, so one waits at its first acquire instead.
*/
func TestAcquireOrdersGlobally(t *testing.T) {
	p := resource.New(map[string]int{"a": 1, "b": 1})
	var wg sync.WaitGroup
	done := make(chan struct{})
	for _, names := range [][]string{{"a", "b"}, {"b", "a"}} {
		wg.Add(1)
		go func(names []string) {
			defer wg.Done()
			release, err := p.Acquire(context.Background(), names)
			if err != nil {
				t.Errorf("Acquire(%v): %v", names, err)
				return
			}
			release()
		}(names)
	}
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("acquiring opposite orders deadlocked")
	}
}

/*
TestAcquireRejectsUnrunnableClaims pins the failures that must happen before
any unit is taken: an unknown name would wait forever on a gate nobody fills,
and a duplicate would wait for the unit it already holds.
*/
func TestAcquireRejectsUnrunnableClaims(t *testing.T) {
	p := resource.New(map[string]int{"gpu": 1})
	if _, err := p.Acquire(context.Background(), []string{"tpu"}); err == nil {
		t.Error("Acquire(tpu) = nil, want an error for an unconfigured name")
	}
	if _, err := p.Acquire(context.Background(), []string{"gpu", "gpu"}); err == nil {
		t.Error("Acquire(duplicate) = nil, want an error")
	}
}

func TestAcquireEmptyIsNoop(t *testing.T) {
	p := resource.New(nil)
	release, err := p.Acquire(context.Background(), nil)
	if err != nil {
		t.Fatalf("Acquire(nil) = %v, want nil", err)
	}
	release() // must not panic or block
}

func TestConfigured(t *testing.T) {
	p := resource.New(map[string]int{"gpu": 2})
	if !p.Configured("gpu") {
		t.Error("Configured(gpu) = false, want true")
	}
	if p.Configured("tpu") {
		t.Error("Configured(tpu) = true, want false")
	}
}

/*
TestAcquireNeverSucceedsOnFinishedContext pins the total cancellation
contract: a context that is already done must never yield capacity. The
repeated calls are the test, not padding — Acquire's select chooses
pseudo-randomly between a free token and a done context, so a single call
would pass half the time on the broken version.
*/
func TestAcquireNeverSucceedsOnFinishedContext(t *testing.T) {
	pool := resource.New(map[string]int{"gpu": 1})
	done, cancel := context.WithCancel(context.Background())
	cancel()

	for i := 0; i < 200; i++ {
		release, err := pool.Acquire(done, []string{"gpu"})
		if err == nil {
			release()
			t.Fatalf("iteration %d: Acquire succeeded on a finished context", i)
		}
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("iteration %d: err = %v, want context.Canceled", i, err)
		}
	}

	// The refusals must not have consumed the unit.
	release, err := pool.Acquire(context.Background(), []string{"gpu"})
	if err != nil {
		t.Fatalf("Acquire after refusals: %v", err)
	}
	release()
}
