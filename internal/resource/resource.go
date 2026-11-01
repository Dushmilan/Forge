/*
Package resource gates execution on scarce capacity.

A resource is a name with a unit count: two gpus, four database slots. A job
that claims one unit of "gpu" waits until a unit is free, so two gpu jobs never
run at once. The unit is deliberately indivisible — V7 teaches acquisition and
release; weighted sharing is a different mechanism and a different version.

The implementation is one buffered channel per name: a channel with n tokens IS
a counting semaphore in Go, and taking a token is one select that can also be
abandoned through its context.
*/
package resource

import (
	"context"
	"fmt"
	"slices"
)

/*
Pool holds one gate per configured resource. The map is written only by New and
only read afterwards, so it needs no mutex — immutability is the
synchronization. The gates themselves are safe for concurrent use by design.
*/
type Pool struct {
	gates map[string]chan struct{}
}

// New builds a pool from name → capacity. A capacity of zero or less is
// dropped: a resource nobody can ever acquire is not a resource, it is a
// permanent stall.
func New(caps map[string]int) *Pool {
	p := &Pool{gates: make(map[string]chan struct{}, len(caps))}
	for name, n := range caps {
		if n <= 0 {
			continue
		}
		g := make(chan struct{}, n)
		for i := 0; i < n; i++ {
			g <- struct{}{}
		}
		p.gates[name] = g
	}
	return p
}

// Configured reports whether name can be acquired at all. The API asks this at
// admission so an unconfigured claim is a 400, not a worker blocked forever.
func (p *Pool) Configured(name string) bool {
	_, ok := p.gates[name]
	return ok
}

/*
Acquire takes one unit of every named resource and returns a closure that
returns them all. On failure nothing is held: units taken before the failure
are released before the error is returned.

The names are sorted first. That single line is what prevents deadlock: if job
A claims ["gpu","db"] and job B claims ["db","gpu"], unordered acquisition lets
each hold one and wait for the other forever. A global order means one of them
always waits at its first acquire, holding nothing.

The returned closure must be called exactly once — a defer in the caller gives
that for free. Calling it twice would mint capacity that does not exist.
*/
func (p *Pool) Acquire(ctx context.Context, names []string) (func(), error) {
	ordered, err := p.sortedUnique(names)
	if err != nil {
		return nil, err
	}

	/*
		held records what has been taken so far so the failure path can give
		it all back. release is safe to call with an empty held slice.
	*/
	var held []string
	release := func() {
		for _, name := range held {
			p.gates[name] <- struct{}{}
		}
		held = nil
	}

	for _, name := range ordered {
		g := p.gates[name]
		select {
		case <-g:
			held = append(held, name)
		case <-ctx.Done():
			release()
			return nil, fmt.Errorf("resource: acquire %q: %w", name, ctx.Err())
		}
	}
	return release, nil
}

/*
sortedUnique validates the claim's shape before any unit is taken and returns
the names in the pool's global order. Duplicates are rejected because
["gpu","gpu"] with capacity 1 would block waiting for a unit the job itself
holds.
*/
func (p *Pool) sortedUnique(names []string) ([]string, error) {
	if len(names) == 0 {
		return nil, nil
	}
	ordered := slices.Clone(names)
	slices.Sort(ordered)
	for i, name := range ordered {
		if !p.Configured(name) {
			return nil, fmt.Errorf("resource: %q is not configured", name)
		}
		if i > 0 && ordered[i-1] == name {
			return nil, fmt.Errorf("resource: %q claimed twice", name)
		}
	}
	return ordered, nil
}
