package resource

import (
	"slices"
	"testing"
)

/*
TestSortedUniqueImposesOneOrder pins the deadlock fix directly. The
behavioural test needs a lucky interleaving to deadlock; the invariant is
simpler than that: whatever order the caller lists, the pool returns the
same global order, so two opposite claims can never each hold a unit the
other wants.
*/
func TestSortedUniqueImposesOneOrder(t *testing.T) {
	p := New(map[string]int{"db": 1, "gpu": 1})

	for _, names := range [][]string{{"gpu", "db"}, {"db", "gpu"}} {
		got, err := p.sortedUnique(names)
		if err != nil {
			t.Fatalf("sortedUnique(%v): %v", names, err)
		}
		if want := []string{"db", "gpu"}; !slices.Equal(got, want) {
			t.Errorf("sortedUnique(%v) = %v, want %v", names, got, want)
		}
	}
}
