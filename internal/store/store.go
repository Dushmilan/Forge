// Package store persists job records in a single JSON file.
//
// V1 storage is deliberately dumb: one file, one mutex, atomic renames.
// Databases and concurrent-writer stories belong to later versions — the
// ladder pulls them in when a real need does.
package store

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sync"

	"github.com/Dushmilan/Forge/internal/executor"
	"github.com/Dushmilan/Forge/internal/job"
)

// ErrNotFound is the sentinel callers test with errors.Is. Wrapping it with
// the id keeps the message human-readable while preserving identity — the
// same %w idiom V0's job.ErrInvalidJob uses.
var ErrNotFound = errors.New("store: not found")

// Record is a Job's persisted life story: what was asked for, where it ended
// up, and what came back. Err is a string (not error) because it must
// survive JSON: errors don't round-trip, their messages do.
type Record struct {
	ID      string          `json:"id"`
	Command []string        `json:"command"`
	Status  job.Status      `json:"status"`
	Result  executor.Result `json:"result,omitempty"`
	Err     string          `json:"error,omitempty"`
	// Attempts counts executions started. A retried job ends with more than
	// one; a job cancelled before it ever started stays at zero.
	Attempts int `json:"attempts,omitempty"`
}

// Store reads and writes <dir>/jobs.json. The zero construction is New;
// the mutex makes the read-modify-write cycle atomic across net/http's
// per-request goroutines (see Save).
type Store struct {
	mu  sync.Mutex
	dir string
}

// New returns a Store rooted at dir. It does not touch the disk yet — a
// missing directory is created on first Save, and a missing file reads as
// an empty store, so constructing a Store can never fail.
func New(dir string) *Store {
	return &Store{dir: dir}
}

// path is the one file this Store owns.
func (s *Store) path() string {
	return filepath.Join(s.dir, "jobs.json")
}

// load reads the current map. A missing file is not an error: it means
// nothing has been saved yet, so an empty map is the honest answer. A
// *corrupt* file IS an error — overwriting data we cannot read would lose
// it silently.
func (s *Store) load() (map[string]Record, error) {
	b, err := os.ReadFile(s.path())
	if errors.Is(err, fs.ErrNotExist) {
		return map[string]Record{}, nil
	}
	if err != nil {
		return nil, fmt.Errorf("store: read %s: %w", s.path(), err)
	}
	var m map[string]Record
	if err := json.Unmarshal(b, &m); err != nil {
		return nil, fmt.Errorf("store: decode %s: %w", s.path(), err)
	}
	return m, nil
}

// Save merges r into the file.
//
// The mutex serializes the whole read-modify-write: net/http runs each
// request on its own goroutine, so two concurrent Saves would each read the
// old map and the loser's record would vanish. This is data-race
// correctness, not an early taste of V3's worker concurrency.
//
// The write is atomic on POSIX: marshal to jobs.json.tmp, then rename over
// the destination. A crash mid-write leaves the old file intact instead of
// a half-written one.
func (s *Store) Save(r Record) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := os.MkdirAll(s.dir, 0o755); err != nil {
		return fmt.Errorf("store: create %s: %w", s.dir, err)
	}
	m, err := s.load()
	if err != nil {
		return err
	}
	/*
		The state machine is checked where the transition becomes durable.
		Reading the stored record inside the same lock as the write is what
		makes the check atomic: no caller can slip a different status in
		between validation and persistence. A record that does not exist yet
		is a seed, not a transition — V1's tests and direct test setup save
		terminal records first.
	*/
	if cur, ok := m[r.ID]; ok {
		if !cur.Status.CanTransition(r.Status) {
			return fmt.Errorf("store: job %q: %w: %s -> %s",
				r.ID, job.ErrInvalidTransition, cur.Status, r.Status)
		}
	}
	m[r.ID] = r

	b, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return fmt.Errorf("store: encode: %w", err)
	}
	tmp := s.path() + ".tmp"
	if err := os.WriteFile(tmp, b, 0o644); err != nil {
		return fmt.Errorf("store: write temp: %w", err)
	}
	if err := os.Rename(tmp, s.path()); err != nil {
		return fmt.Errorf("store: rename: %w", err)
	}
	return nil
}

// Get returns the record for id, or a wrapped ErrNotFound.
func (s *Store) Get(id string) (Record, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	m, err := s.load()
	if err != nil {
		return Record{}, err
	}
	r, ok := m[id]
	if !ok {
		return Record{}, fmt.Errorf("store: job %q: %w", id, ErrNotFound)
	}
	return r, nil
}
