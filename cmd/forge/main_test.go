package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Dushmilan/Forge/internal/executor"
	"github.com/Dushmilan/Forge/internal/job"
	"github.com/Dushmilan/Forge/internal/store"
)

// TestRunWithNoArgsPrintsUsage pins the CLI contract for a missing command:
// a usage line on stderr and the conventional exit code 2 (the shell's "misuse"
// code). Without an explicit branch this would fall through to validation and
// report exit 1, which connotes a runtime failure rather than bad input.
func TestRunWithNoArgsPrintsUsage(t *testing.T) {
	var stdout, stderr bytes.Buffer

	code := run(nil, &stdout, &stderr)

	if code != 2 {
		t.Errorf("run() = %d, want 2", code)
	}
	if stderr.Len() == 0 {
		t.Error("stderr is empty, want a usage message")
	}
	if stdout.Len() != 0 {
		t.Errorf("stdout = %q, want empty", stdout.String())
	}
}

// TestRunPropagatesCommandExitCode pins that the CLI reports the command's own
// exit code, not a generic 1. Tools compose by exit code, so "sh -c 'exit 3'"
// must yield 3.
func TestRunPropagatesCommandExitCode(t *testing.T) {
	var stdout, stderr bytes.Buffer

	code := run([]string{"sh", "-c", "exit 3"}, &stdout, &stderr)

	if code != 3 {
		t.Errorf("run() = %d, want 3", code)
	}
	if stderr.Len() == 0 {
		t.Error("stderr is empty, want failure detail")
	}
}

// TestRunExecutesCommand pins the CLI happy path. run() returns an exit code
// rather than calling os.Exit directly, which is what makes the command line
// testable: the test supplies a bytes.Buffer where main supplies os.Stdout.
func TestRunExecutesCommand(t *testing.T) {
	var stdout, stderr bytes.Buffer

	code := run([]string{"echo", "hello"}, &stdout, &stderr)

	if code != 0 {
		t.Fatalf("run() = %d, want 0 (stderr: %s)", code, stderr.String())
	}
	if got := strings.TrimSpace(stdout.String()); got != "hello" {
		t.Errorf("stdout = %q, want %q", got, "hello")
	}
}

// lineWriter collects Write output and emits each complete line on ch, so a
// test can wait for "listening on …" without sleeping: it blocks on a
// channel receive, which is synchronization, not polling.
type lineWriter struct {
	mu  sync.Mutex
	buf []byte
	ch  chan string
}

func newLineWriter() *lineWriter {
	return &lineWriter{ch: make(chan string, 8)}
}

func (w *lineWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.buf = append(w.buf, p...)
	for {
		i := bytes.IndexByte(w.buf, '\n')
		if i < 0 {
			break
		}
		w.ch <- string(w.buf[:i])
		w.buf = w.buf[i+1:]
	}
	return len(p), nil
}

// TestRunServeServesThenShutsDown pins the whole serve lifecycle against
// Review Focus-style reality: it binds a real port (:0 = pick any free
// one), answers a real HTTP request, and exits 0 when ctx is cancelled —
// with no time.Sleep anywhere. Cancellation is the synchronization.
func TestRunServeServesThenShutsDown(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	out := newLineWriter()
	code := make(chan int, 1)
	go func() {
		code <- runServe(ctx, []string{"-addr", "127.0.0.1:0", "-data", t.TempDir()}, out, io.Discard)
	}()

	// Block until the server announces its address — a channel receive, not
	// a sleep, so the test cannot flake on timing.
	var addr string
	select {
	case line := <-out.ch:
		addr = strings.TrimPrefix(line, "listening on ")
		if addr == line {
			t.Fatalf("unexpected startup line %q", line)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("runServe never printed its listening address")
	}

	// A real request proves the API is wired into the server, not just
	// that a socket opened. 404 for a job nobody created.
	resp, err := http.Get("http://" + addr + "/jobs/ghost")
	if err != nil {
		t.Fatalf("GET failed: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("status = %d, want 404", resp.StatusCode)
	}

	cancel()
	select {
	case c := <-code:
		if c != 0 {
			t.Errorf("runServe() = %d, want 0 on clean shutdown", c)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("runServe did not shut down after cancel")
	}
}

// TestRunServeBadFlag pins that flag misuse exits 2 (usage), like the
// no-args CLI path — not 1, which means "something ran and failed".
func TestRunServeBadFlag(t *testing.T) {
	var stdout, stderr bytes.Buffer
	if code := runServe(context.Background(), []string{"-bogus"}, &stdout, &stderr); code != 2 {
		t.Errorf("runServe(-bogus) = %d, want 2", code)
	}
}

// fakeRunner is V0's Runner seam doing what fakes do best: it signals when
// a run starts and blocks until the test releases it, turning "did the
// handler mark the job running?" into a channel question — no sleeps.
type fakeRunner struct {
	started chan struct{}
	release chan struct{}
	res     executor.Result
	err     error
}

func (f *fakeRunner) Run(j job.Job) (executor.Result, error) {
	f.started <- struct{}{}
	<-f.release
	return f.res, f.err
}

// TestJobHandlerRunsAndPersists pins the success path: pending in, running
// saved on the way, succeeded with the executor's result out.
func TestJobHandlerRunsAndPersists(t *testing.T) {
	st := store.New(t.TempDir())
	fake := &fakeRunner{
		started: make(chan struct{}, 1),
		release: make(chan struct{}, 1),
		res:     executor.Result{ExitCode: 0, Stdout: "hello\n"},
	}
	fake.release <- struct{}{} // the single run proceeds immediately

	pending := store.Record{ID: "j1", Command: []string{"echo", "hello"}, Status: job.StatusPending}
	if err := st.Save(pending); err != nil {
		t.Fatal(err)
	}

	newJobHandler(st, fake, io.Discard)(pending)

	stored, err := st.Get("j1")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if stored.Status != job.StatusSucceeded {
		t.Errorf("status = %v, want succeeded", stored.Status)
	}
	if stored.Result.Stdout != "hello\n" {
		t.Errorf("stdout = %q, want %q", stored.Result.Stdout, "hello\n")
	}
}

// TestJobHandlerMarksRunningMidFlight pins Review Focus 3: while the
// executor is busy, the stored record reads "running" — the live status a
// GET would serve — not "pending" and not the final result.
func TestJobHandlerMarksRunningMidFlight(t *testing.T) {
	st := store.New(t.TempDir())
	fake := &fakeRunner{
		started: make(chan struct{}, 1),
		release: make(chan struct{}, 1),
	}
	pending := store.Record{ID: "j1", Command: []string{"slow"}, Status: job.StatusPending}
	if err := st.Save(pending); err != nil {
		t.Fatal(err)
	}

	handlerDone := make(chan struct{})
	go func() {
		newJobHandler(st, fake, io.Discard)(pending)
		close(handlerDone)
	}()

	select { // the executor has the job; the record must say running NOW
	case <-fake.started:
	case <-time.After(2 * time.Second):
		t.Fatal("executor never started")
	}
	stored, err := st.Get("j1")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if stored.Status != job.StatusRunning {
		t.Errorf("mid-flight status = %v, want running", stored.Status)
	}

	fake.release <- struct{}{}
	select {
	case <-handlerDone:
	case <-time.After(2 * time.Second):
		t.Fatal("handler never finished after release")
	}
	stored, err = st.Get("j1")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if stored.Status != job.StatusSucceeded {
		t.Errorf("final status = %v, want succeeded", stored.Status)
	}
}

// TestJobHandlerRunFailureIsData pins V1's honesty rule on the worker path:
// a failed run is an OUTCOME — status failed, message kept in the record's
// error field — not an exception.
func TestJobHandlerRunFailureIsData(t *testing.T) {
	st := store.New(t.TempDir())
	fake := &fakeRunner{
		started: make(chan struct{}, 1),
		release: make(chan struct{}, 1),
		res:     executor.Result{ExitCode: 3},
		err:     errors.New("exit status 3"),
	}
	fake.release <- struct{}{}

	pending := store.Record{ID: "j1", Command: []string{"sh", "-c", "exit 3"}, Status: job.StatusPending}
	if err := st.Save(pending); err != nil {
		t.Fatal(err)
	}

	newJobHandler(st, fake, io.Discard)(pending)

	stored, err := st.Get("j1")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if stored.Status != job.StatusFailed {
		t.Errorf("status = %v, want failed", stored.Status)
	}
	if stored.Result.ExitCode != 3 {
		t.Errorf("exit code = %d, want 3", stored.Result.ExitCode)
	}
	if stored.Err != "exit status 3" {
		t.Errorf("error field = %q, want the failure message as data", stored.Err)
	}
}

// TestJobHandlerSaveFailureGoesToStderr pins the spec's "never swallows"
// rule on the worker path: a Save that fails has no client to answer and
// no logger until V9, so the failure must at least land on stderr instead
// of vanishing. A file where a directory should be makes Save fail
// deterministically.
func TestJobHandlerSaveFailureGoesToStderr(t *testing.T) {
	blocker := filepath.Join(t.TempDir(), "not-a-dir")
	if err := os.WriteFile(blocker, []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	st := store.New(filepath.Join(blocker, "data"))
	fake := &fakeRunner{
		started: make(chan struct{}, 1),
		release: make(chan struct{}, 1),
	}
	fake.release <- struct{}{} // released, but the executor must NOT run: the running-save failed first
	var stderr bytes.Buffer

	pending := store.Record{ID: "j1", Command: []string{"true"}, Status: job.StatusPending}
	newJobHandler(st, fake, &stderr)(pending)

	if !strings.Contains(stderr.String(), "j1") {
		t.Errorf("stderr = %q, want it to name the job whose save failed", stderr.String())
	}
	select {
	case <-fake.started:
		t.Fatal("executor ran even though marking the job running failed")
	default:
	}
}

/*
TestJobHandlerConcurrentSaveFailuresRaceFree pins Review Focus 3: when
multiple worker goroutines share one handler closure and st.Save fails
concurrently, their stderr writes must not race on the io.Writer.
*/
func TestJobHandlerConcurrentSaveFailuresRaceFree(t *testing.T) {
	blocker := filepath.Join(t.TempDir(), "not-a-dir")
	if err := os.WriteFile(blocker, []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	st := store.New(filepath.Join(blocker, "data"))
	fake := &fakeRunner{
		started: make(chan struct{}, 8),
		release: make(chan struct{}),
	}
	close(fake.release)

	var stderr bytes.Buffer
	h := newJobHandler(st, fake, &stderr)

	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		id := fmt.Sprintf("j-%d", i)
		go func() {
			defer wg.Done()
			h(store.Record{ID: id, Command: []string{"true"}, Status: job.StatusPending})
		}()
	}
	wg.Wait()

	for i := 0; i < 8; i++ {
		want := fmt.Sprintf("j-%d", i)
		if !strings.Contains(stderr.String(), want) {
			t.Errorf("stderr missing %q; got %q", want, stderr.String())
		}
	}
}

// TestRunServeRunsJobsInBackground pins the V2 headline end to end: a real
// POST answers 202 without waiting, and the record reaches a terminal
// status on its own. The GET poll is deadline-bounded observation, not a
// guessed sleep: it cannot flake on timing, only bound the wait — and it
// reads the same status field a client would.
func TestRunServeRunsJobsInBackground(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	out := newLineWriter()
	code := make(chan int, 1)
	go func() {
		code <- runServe(ctx, []string{"-addr", "127.0.0.1:0", "-data", t.TempDir()}, out, io.Discard)
	}()

	var addr string
	select {
	case line := <-out.ch:
		addr = strings.TrimPrefix(line, "listening on ")
		if addr == line {
			t.Fatalf("unexpected startup line %q", line)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("runServe never printed its listening address")
	}

	resp, err := http.Post("http://"+addr+"/jobs", "application/json",
		strings.NewReader(`{"command": ["echo", "hello"]}`))
	if err != nil {
		t.Fatalf("POST failed: %v", err)
	}
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("POST status = %d, want 202", resp.StatusCode)
	}
	var rec store.Record
	if err := json.NewDecoder(resp.Body).Decode(&rec); err != nil {
		t.Fatalf("POST body is not JSON: %v", err)
	}
	resp.Body.Close()

	deadline := time.Now().Add(10 * time.Second)
	for {
		r, err := http.Get("http://" + addr + "/jobs/" + rec.ID)
		if err != nil {
			t.Fatalf("GET failed: %v", err)
		}
		var got store.Record
		if err := json.NewDecoder(r.Body).Decode(&got); err != nil {
			r.Body.Close()
			t.Fatalf("GET body is not JSON: %v", err)
		}
		r.Body.Close()
		switch got.Status {
		case job.StatusSucceeded:
			if got.Result.Stdout != "hello\n" {
				t.Errorf("stdout = %q, want %q", got.Result.Stdout, "hello\n")
			}
		case job.StatusFailed:
			t.Fatalf("job failed: %+v", got)
		default:
			if time.Now().After(deadline) {
				t.Fatalf("job never finished; last status %v", got.Status)
			}
			time.Sleep(time.Millisecond) // poll interval only; the deadline is the guarantee
			continue
		}
		break
	}

	cancel()
	select {
	case c := <-code:
		if c != 0 {
			t.Errorf("runServe() = %d, want 0 on clean shutdown", c)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("runServe did not shut down after cancel")
	}
}

// TestRunServeFinishesRunningJobOnShutdown pins the spec's shutdown
// contract end to end (Review Focus 2): ctx cancellation while a job is
// RUNNING must not let runServe return until that job has finished and its
// final status is on disk. Without a worker join, runServe returns in
// milliseconds and the record is stuck "running" forever.
func TestRunServeFinishesRunningJobOnShutdown(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	data := t.TempDir()
	out := newLineWriter()
	code := make(chan int, 1)
	go func() {
		code <- runServe(ctx, []string{"-addr", "127.0.0.1:0", "-data", data}, out, io.Discard)
	}()

	var addr string
	select {
	case line := <-out.ch:
		addr = strings.TrimPrefix(line, "listening on ")
		if addr == line {
			t.Fatalf("unexpected startup line %q", line)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("runServe never printed its listening address")
	}

	resp, err := http.Post("http://"+addr+"/jobs", "application/json",
		strings.NewReader(`{"command": ["sleep", "1"]}`))
	if err != nil {
		t.Fatalf("POST failed: %v", err)
	}
	var rec store.Record
	if err := json.NewDecoder(resp.Body).Decode(&rec); err != nil {
		t.Fatalf("POST body is not JSON: %v", err)
	}
	resp.Body.Close()

	// Wait until the record reads "running" — the job is genuinely
	// mid-flight when we cancel, which is the whole point of this test.
	deadline := time.Now().Add(10 * time.Second)
	for {
		r, err := http.Get("http://" + addr + "/jobs/" + rec.ID)
		if err != nil {
			t.Fatalf("GET failed: %v", err)
		}
		var got store.Record
		if err := json.NewDecoder(r.Body).Decode(&got); err != nil {
			r.Body.Close()
			t.Fatalf("GET body is not JSON: %v", err)
		}
		r.Body.Close()
		if got.Status == job.StatusRunning {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("job never reached running; last status %v", got.Status)
		}
		time.Sleep(time.Millisecond) // poll interval only; the deadline is the guarantee
	}

	cancel() // Ctrl-C while the job is running

	// Without the worker join, runServe returns almost immediately — that
	// premature return is the bug this test pins.
	select {
	case c := <-code:
		t.Fatalf("runServe returned %d while the job was still mid-flight", c)
	case <-time.After(200 * time.Millisecond):
		// good: runServe is waiting for the in-flight job
	}

	select { // ...and now it must return, because the job will finish
	case c := <-code:
		if c != 0 {
			t.Errorf("runServe() = %d, want 0 on clean shutdown", c)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("runServe never returned after the in-flight job finished")
	}

	b, err := os.ReadFile(filepath.Join(data, "jobs.json"))
	if err != nil {
		t.Fatalf("read store file: %v", err)
	}
	var m map[string]store.Record
	if err := json.Unmarshal(b, &m); err != nil {
		t.Fatalf("decode store file: %v", err)
	}
	stored, ok := m[rec.ID]
	if !ok {
		t.Fatal("record vanished from the store")
	}
	if stored.Status != job.StatusSucceeded {
		t.Errorf("final status = %v, want succeeded — the job must complete before shutdown returns", stored.Status)
	}
}
