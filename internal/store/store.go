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
	"sort"
	"sync"
	"time"

	"github.com/Dushmilan/Forge/internal/executor"
	"github.com/Dushmilan/Forge/internal/job"
)

// ErrNotFound is the sentinel callers test with errors.Is. Wrapping it with
// the id keeps the message human-readable while preserving identity — the
// same %w idiom V0's job.ErrInvalidJob uses.
var ErrNotFound = errors.New("store: not found")

// ErrNotRunning is returned by Heartbeat when the record exists but is no
// longer running: the caller's tick arrived after the job ended. It is a
// sentinel because "stop heartbeating" is a normal outcome, not a failure.
var ErrNotRunning = errors.New("store: job not running")

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
	/*
		A pointer, not time.Time: nil means "this job has never run" and is
		omitted from JSON, while a zero time.Time would serialize as
		0001-01-01T00:00:00Z on every pending job. encoding/json follows the
		pointer, so the field is both honest and absent when unknown.
	*/
	LastHeartbeat *time.Time `json:"last_heartbeat,omitempty"`
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
		/*
			Field ownership: the worker persists whole records but never
			knows the heartbeat; Heartbeat is the only writer. Taking the
			stored value when the incoming one is nil is what keeps a
			whole-record save from rewinding liveness — the same reason the
			store exists instead of two goroutines racing on one struct.
		*/
		if r.LastHeartbeat == nil {
			r.LastHeartbeat = cur.LastHeartbeat
		}
	}
	m[r.ID] = r

	return s.writeLocked(m)
}

/*
writeLocked persists the whole map. The suffix documents the locking contract:
every caller already holds s.mu, so two writers can never interleave a read,
a modify, and a write. tmp+rename is the atomicity: a crash leaves the old
file, never a half-written one.
*/
func (s *Store) writeLocked(m map[string]Record) error {
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

/*
Heartbeat advances one record's liveness without touching status, result, or
attempts. It exists so the heartbeat goroutine and the worker never write the
same fields: two writers, one mutex, no lost update.
*/
func (s *Store) Heartbeat(id string, at time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	m, err := s.load()
	if err != nil {
		return err
	}
	r, ok := m[id]
	if !ok {
		return fmt.Errorf("store: job %q: %w", id, ErrNotFound)
	}
	if r.Status != job.StatusRunning {
		return fmt.Errorf("store: job %q is %s: %w", id, r.Status, ErrNotRunning)
	}
	r.LastHeartbeat = &at
	m[id] = r
	return s.writeLocked(m)
}

/*
List returns a deterministic snapshot. sort.Slice is deliberate: Go randomizes
map iteration, and recovery must repair records in the same order every run.
*/
func (s *Store) List() ([]Record, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	m, err := s.load()
	if err != nil {
		return nil, err
	}
	out := make([]Record, 0, len(m))
	for _, r := range m {
		out = append(out, r)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out, nil
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
