package main

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
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

	newJobHandler(st, fake)(pending)

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
		newJobHandler(st, fake)(pending)
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

	newJobHandler(st, fake)(pending)

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
