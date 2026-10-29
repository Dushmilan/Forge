package main

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

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

/*
TestRunServeInvalidWorkersFlag pins Review Focus 4: a worker pool of 0 or
fewer goroutines would accept jobs and never run them, so runServe rejects
-workers < 1 at flag validation time with exit code 2 (usage error).

Each case also asserts WHY it was rejected. An undefined flag also exits 2,
so "exit 2 + stderr" alone passes even when the flag does not exist — the
substring pins that the numeric cases were rejected by value validation and
the garbage case by the flag parser, not by the flag being missing.
*/
func TestRunServeInvalidWorkersFlag(t *testing.T) {
	tests := []struct {
		val     string
		wantSub string
	}{
		{"0", "-workers must be >= 1"},
		{"-1", "-workers must be >= 1"},
		{"not-a-number", "invalid value"},
	}
	for _, tt := range tests {
		t.Run(tt.val, func(t *testing.T) {
			var stdout, stderr bytes.Buffer
			code := runServe(context.Background(), []string{"-workers", tt.val}, &stdout, &stderr)
			if code != 2 {
				t.Errorf("runServe(-workers %s) = %d, want 2", tt.val, code)
			}
			if !strings.Contains(stderr.String(), tt.wantSub) {
				t.Errorf("stderr = %q, want it to contain %q", stderr.String(), tt.wantSub)
			}
		})
	}
}

/*
TestRunServeRunsJobsConcurrently proves V3's headline end to end: when a
slow job ("sleep 1") is already running on forge serve, a fast job
("echo fast") submitted behind it completes BEFORE the slow job finishes,
and shutdown still waits for the slow job to complete cleanly.
*/
func TestRunServeRunsJobsConcurrently(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	out := newLineWriter()
	code := make(chan int, 1)
	go func() {
		code <- runServe(ctx, []string{"-addr", "127.0.0.1:0", "-data", t.TempDir(), "-workers", "2"}, out, io.Discard)
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

	deadline := time.Now().Add(10 * time.Second)

	// 1. Submit the slow job and wait until a worker marks it running.
	slowResp, err := http.Post("http://"+addr+"/jobs", "application/json",
		strings.NewReader(`{"command": ["sleep", "1"]}`))
	if err != nil {
		t.Fatalf("POST slow failed: %v", err)
	}
	var slowRec store.Record
	if err := json.NewDecoder(slowResp.Body).Decode(&slowRec); err != nil {
		t.Fatalf("decode slow POST: %v", err)
	}
	slowResp.Body.Close()

	for {
		r, err := http.Get("http://" + addr + "/jobs/" + slowRec.ID)
		if err != nil {
			t.Fatalf("GET slow failed: %v", err)
		}
		var got store.Record
		_ = json.NewDecoder(r.Body).Decode(&got)
		r.Body.Close()
		if got.Status == job.StatusRunning {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("slow job never reached running; last status %v", got.Status)
		}
		time.Sleep(time.Millisecond) // poll interval; the deadline is the guarantee
	}

	// 2. Submit the fast job while worker 1 is busy with sleep 1.
	fastResp, err := http.Post("http://"+addr+"/jobs", "application/json",
		strings.NewReader(`{"command": ["echo", "fast"]}`))
	if err != nil {
		t.Fatalf("POST fast failed: %v", err)
	}
	var fastRec store.Record
	if err := json.NewDecoder(fastResp.Body).Decode(&fastRec); err != nil {
		t.Fatalf("decode fast POST: %v", err)
	}
	fastResp.Body.Close()

	for {
		r, err := http.Get("http://" + addr + "/jobs/" + fastRec.ID)
		if err != nil {
			t.Fatalf("GET fast failed: %v", err)
		}
		var got store.Record
		_ = json.NewDecoder(r.Body).Decode(&got)
		r.Body.Close()
		if got.Status == job.StatusSucceeded {
			if got.Result.Stdout != "fast\n" {
				t.Errorf("fast stdout = %q, want %q", got.Result.Stdout, "fast\n")
			}
			break
		}
		if got.Status == job.StatusFailed {
			t.Fatalf("fast job failed: %+v", got)
		}
		if time.Now().After(deadline) {
			t.Fatalf("fast job never finished; last status %v", got.Status)
		}
		time.Sleep(time.Millisecond) // poll interval; the deadline is the guarantee
	}

	// The slow job must STILL be running — proving the fast job overtook it.
	r, err := http.Get("http://" + addr + "/jobs/" + slowRec.ID)
	if err != nil {
		t.Fatalf("GET slow check failed: %v", err)
	}
	var slowNow store.Record
	_ = json.NewDecoder(r.Body).Decode(&slowNow)
	r.Body.Close()
	if slowNow.Status != job.StatusRunning {
		t.Errorf("slow job status = %v, want running while fast job already finished", slowNow.Status)
	}

	cancel()
	select {
	case c := <-code:
		if c != 0 {
			t.Errorf("runServe() = %d, want 0", c)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("runServe did not shut down after cancel")
	}
}
