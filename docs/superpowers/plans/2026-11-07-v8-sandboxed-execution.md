# V8 — Sandboxed Execution Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Give every job run a fresh working directory, a scrubbed environment, and process-group cancellation so grandchildren die with the job and `Run` can never hang on a leaked stream.

**Architecture:** All changes live in `internal/executor`. `Run` grows five lines of `os/exec` configuration — `Dir`, `Env`, `SysProcAttr`, `Cancel`, `WaitDelay` — around the existing command flow; callers see no interface change because `Executor{}` stays a valid `Runner`. `cmd/forge` is untouched.

**Tech Stack:** Go 1.22.2, standard library only (`os`, `os/exec`, `syscall`, `errors`, `io/fs`).

**Spec:** `docs/versions/V8.md`

## Global Constraints

- TDD per AGENTS.md: exactly one failing test before each implementation slice; never refactor while RED; tests assert through the public interface (`Executor.Run`).
- No `time.Sleep` as synchronization — bounded polls with a deadline, matching the existing `TestExecutorRunKillsRunningProcess` style.
- Zero-value `Executor{}` must remain a valid `Runner`; no changes to `cmd/forge`, `internal/worker`, or any other package.
- `job.Job` stays `{ID, Command}` — no new fields.
- Every error path wraps with `%w` so `errors.Is`/`errors.As` keep finding `exec.ErrNotFound`, `*exec.ExitError`, and the context sentinels; the existing "prefer `ctx.Err()` over `signal: killed`" conversion stays.
- `const waitDelay = time.Second` (value fixed by spec).
- Sandbox directory names are random (`os.MkdirTemp` pattern `forge-job-`); the job id never appears in a path.
- Unix-only: `SysProcAttr{Setpgid: true}` and negative-pid `syscall.Kill`. No build tags, no `golang.org/x/sys`.
- Comments explain the Go concept (block `/* … */` style as in the file today), not the syntax.
- Every task ends with `go test ./internal/executor -race` green; the final task ends with `go test ./... -race`, `go vet ./...`, and `gofmt -l .` clean.
- Stdlib only; `go mod tidy` before the final commit.

## Review Focus

Input classes and failure modes the spec implies but a casual implementation gets wrong. Each line is pinned by a test in the task named.

1. **Job id containing `../`** — `job.Validate` allows any non-empty id, so an id like `../../evil` must not influence the sandbox path. Pinned by subtest `hostile id` in Task 1.
2. **Forge environment leaking to the job** — any variable set in the forge process must be invisible to the child. Pinned by `TestExecutorRunScrubsEnvironment` in Task 2.
3. **Zombie grandchild false failure** — a correctly killed grandchild may linger as state `Z` awaiting reap; a test that only accepts "file missing" flakes. Pinned by the assertion shape in Task 4 (missing **or** `Z`).
4. **Escapee holds the pipe → `Run` hangs after cancel** — the exact bug `WaitDelay` exists for; without it the test times out. Pinned by the `escapee holds the pipe after cancel` subtest of `TestExecutorRunWaitDelay` in Task 5 (which also pins the exit-0 `exec.ErrWaitDelay` flavour).
5. **Concurrent workers sharing one `SandboxRoot`** — two simultaneous `Run`s must get distinct directories (race-clean under `-race`). Pinned by subtest `concurrent` in Task 1.

---

### Task 1: Sandbox working directory

**Files:**
- Modify: `internal/executor/executor.go`
- Test: `internal/executor/executor_test.go`

**Interfaces:**
- Consumes: `Executor.Run` flow (validate → `ctx.Err()` → start), `job.Job.Validate`, `job.ErrInvalidJob`.
- Produces: `Executor.SandboxRoot string` — zero value means `os.TempDir()`. Later tasks construct `executor.Executor{SandboxRoot: t.TempDir()}` and rely on `Run` returning the usual error chains while creating/removing exactly one directory per call.

- [ ] **Step 1: Write the failing test**

Add to `internal/executor/executor_test.go` (package `executor_test`). One test function, subtests per scenario:

```go
// TestExecutorRunSandboxDirectory pins V8's room: every run happens in a fresh
// directory under SandboxRoot, the directory is gone when Run returns, and
// nothing about the job — not even a hostile id — chooses its path.
func TestExecutorRunSandboxDirectory(t *testing.T) {
	// success: pwd reports a directory under the root, and the root is empty after.
	t.Run("success", func(t *testing.T) {
		root := t.TempDir()
		res, err := (executor.Executor{SandboxRoot: root}).Run(
			context.Background(), job.Job{ID: "j-cwd", Command: []string{"sh", "-c", "pwd"}})
		if err != nil {
			t.Fatalf("Run() error = %v, want nil", err)
		}
		got := strings.TrimSpace(res.Stdout)
		if !strings.HasPrefix(got, root) {
			t.Errorf("pwd = %q, want a directory under %q", got, root)
		}
		assertDirEmpty(t, root)
	})

	// failure: a non-zero exit still cleans up.
	t.Run("failure", func(t *testing.T) {
		root := t.TempDir()
		if _, err := (executor.Executor{SandboxRoot: root}).Run(
			context.Background(), job.Job{ID: "j-fail", Command: []string{"false"}}); err == nil {
			t.Fatal("Run() error = nil, want an error")
		}
		assertDirEmpty(t, root)
	})

	// cancelled before start: a dead context must not create anything.
	t.Run("cancelled", func(t *testing.T) {
		root := t.TempDir()
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		_, err := (executor.Executor{SandboxRoot: root}).Run(ctx, job.Job{ID: "j-c", Command: []string{"sleep", "60"}})
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Run() error = %v, want wrapped context.Canceled", err)
		}
		assertDirEmpty(t, root)
	})

	// invalid job: rejected before the filesystem is touched.
	t.Run("invalid", func(t *testing.T) {
		root := t.TempDir()
		if _, err := (executor.Executor{SandboxRoot: root}).Run(context.Background(), job.Job{}); !errors.Is(err, job.ErrInvalidJob) {
			t.Fatalf("Run() error = %v, want wrapped job.ErrInvalidJob", err)
		}
		assertDirEmpty(t, root)
	})

	// hostile id: Validate allows it; it must not reach the path.
	t.Run("hostile id", func(t *testing.T) {
		root := t.TempDir()
		res, err := (executor.Executor{SandboxRoot: root}).Run(
			context.Background(), job.Job{ID: "../../evil", Command: []string{"sh", "-c", "pwd"}})
		if err != nil {
			t.Fatalf("Run() error = %v, want nil", err)
		}
		if got := strings.TrimSpace(res.Stdout); !strings.HasPrefix(got, root) {
			t.Errorf("pwd = %q, want a directory under %q", got, root)
		}
		assertDirEmpty(t, root)
	})

	// missing root: configuration error, wrapped, no process started.
	t.Run("missing root", func(t *testing.T) {
		root := filepath.Join(t.TempDir(), "nope")
		_, err := (executor.Executor{SandboxRoot: root}).Run(
			context.Background(), job.Job{ID: "j-noroot", Command: []string{"echo", "x"}})
		if !errors.Is(err, fs.ErrNotExist) {
			t.Fatalf("Run() error = %v, want wrapped fs.ErrNotExist", err)
		}
		var exitErr *exec.ExitError
		if errors.As(err, &exitErr) {
			t.Errorf("Run() error = %v, want a config error, not a process failure", err)
		}
	})

	// concurrent: two runs, one root, distinct directories, both clean after.
	t.Run("concurrent", func(t *testing.T) {
		root := t.TempDir()
		var wg sync.WaitGroup
		for i := 0; i < 2; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				res, err := (executor.Executor{SandboxRoot: root}).Run(
					context.Background(), job.Job{ID: "j-par", Command: []string{"sh", "-c", "pwd"}})
				if err != nil {
					t.Errorf("Run() error = %v, want nil", err)
					return
				}
				if !strings.HasPrefix(strings.TrimSpace(res.Stdout), root) {
					t.Errorf("pwd = %q, want a directory under %q", res.Stdout, root)
				}
			}()
		}
		wg.Wait()
		assertDirEmpty(t, root)
	})
}

// assertDirEmpty fails if root still holds an entry: the sandbox is removed
// when Run returns, on every path.
func assertDirEmpty(t *testing.T, root string) {
	t.Helper()
	entries, err := os.ReadDir(root)
	if err != nil {
		t.Fatalf("ReadDir(%q): %v", root, err)
	}
	if len(entries) != 0 {
		t.Errorf("sandbox root holds %d entries after Run, want 0", len(entries))
	}
}
```

New imports as needed: `io/fs`, `os`, `sync`. (`os/exec` and `filepath` are already imported.)

- [ ] **Step 2: Run the test to verify it fails**

Run: `go test ./internal/executor -run TestExecutorRunSandboxDirectory`
Expected: FAIL to compile — `unknown field 'SandboxRoot' in type 'executor.Executor'`.

- [ ] **Step 3: Implement the sandbox directory in `internal/executor/executor.go`**

- Add the field with a doc comment explaining the concept:
  ```go
  // SandboxRoot is the parent directory for per-run sandboxes. The zero value
  // uses os.TempDir(): Executor{} stays a working Runner, so callers never
  // have to know the sandbox exists.
  SandboxRoot string
  ```
- In `Run`, after the existing `ctx.Err()` check and before `exec.CommandContext`, in this exact order (the order is the spec's rule): resolve `root := e.SandboxRoot`, default `""` to `os.TempDir()`; `dir, err := os.MkdirTemp(root, "forge-job-")`; on error `return Result{}, fmt.Errorf("job %q: sandbox: %w", j.ID, err)`; `defer os.RemoveAll(dir)` (comment: runs after Wait, so no live process has files deleted under it; the random suffix means the job id never picks the path); then `cmd.Dir = dir` on the existing command.
- No other behavior changes: `cmd.Env`, `SysProcAttr`, `Cancel`, `WaitDelay` come in later tasks.

- [ ] **Step 4: Run the test to verify it passes**

Run: `go test ./internal/executor -run TestExecutorRunSandboxDirectory -race`
Expected: PASS, all subtests.

- [ ] **Step 5: Run the whole package and commit**

Run: `go test ./internal/executor -race` — Expected: PASS (all V0 tests too).

```bash
git add internal/executor/executor.go internal/executor/executor_test.go
git commit -m "feat(executor): run every job in a per-run sandbox directory"
```

---

### Task 2: Scrubbed environment

**Files:**
- Modify: `internal/executor/executor.go`
- Test: `internal/executor/executor_test.go`

**Interfaces:**
- Consumes: Task 1's `Run` flow and `SandboxRoot`.
- Produces: `cmd.Env` assignment — every later task's tests inherit the scrubbed environment (commands resolve via the copied `PATH`).

- [ ] **Step 1: Write the failing test**

```go
/*
TestExecutorRunScrubsEnvironment pins replacement, not inheritance: setting
cmd.Env wholesale means forge's variables do not exist for the child, while
PATH survives so binaries still resolve.
*/
func TestExecutorRunScrubsEnvironment(t *testing.T) {
	t.Setenv("FORGE_SANDBOX_LEAK", "sentinel")

	res, err := (executor.Executor{SandboxRoot: t.TempDir()}).Run(
		context.Background(),
		job.Job{ID: "j-env", Command: []string{"sh", "-c",
			`printf '%s|' "${FORGE_SANDBOX_LEAK:-unset}"; command -v sleep >/dev/null && printf ok`}})
	if err != nil {
		t.Fatalf("Run() error = %v, want nil", err)
	}
	if got := strings.TrimSpace(res.Stdout); got != "unset|ok" {
		t.Errorf("Stdout = %q, want %q", got, "unset|ok")
	}
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `go test ./internal/executor -run TestExecutorRunScrubsEnvironment`
Expected: FAIL — `Stdout = "sentinel|ok", want "unset|ok"` (the variable leaked).

- [ ] **Step 3: Implement the env scrub**

In `Run`, next to `cmd.Dir`:

```go
// cmd.Env REPLACES inheritance: this is every variable the child gets, so
// forge's environment cannot leak into a job by omission.
cmd.Env = []string{"PATH=" + os.Getenv("PATH")}
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `go test ./internal/executor -run TestExecutorRunScrubsEnvironment -race`
Expected: PASS.

- [ ] **Step 5: Run the whole package and commit**

Run: `go test ./internal/executor -race` — Expected: PASS.

```bash
git add internal/executor/executor.go internal/executor/executor_test.go
git commit -m "feat(executor): give jobs only PATH, never forge's environment"
```

---

### Task 3: The child leads its own process group

**Files:**
- Modify: `internal/executor/executor.go`
- Test: `internal/executor/executor_test.go`

**Interfaces:**
- Consumes: Task 1's `Run` flow.
- Produces: the invariant `Getpgid(child) == child.pid` that Task 4's group kill depends on.

- [ ] **Step 1: Write the failing test**

```go
/*
TestExecutorRunChildLeadsItsProcessGroup pins the invariant the group kill
depends on: with Setpgid the child's pgid equals its own pid. Without it the
child joins forge's group and kill(-childpid) would address a group that does
not exist.
*/
func TestExecutorRunChildLeadsItsProcessGroup(t *testing.T) {
	pidFile := filepath.Join(t.TempDir(), "pid")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan error, 1)
	go func() {
		_, err := (executor.Executor{SandboxRoot: t.TempDir()}).Run(ctx, job.Job{
			ID:      "j-pgid",
			Command: []string{"sh", "-c", fmt.Sprintf("echo $$ > %s; exec sleep 60", strconv.Quote(pidFile))},
		})
		done <- err
	}()

	pid := pollForPid(t, pidFile) // helper: bounded poll, 5s deadline
	pgid, err := syscall.Getpgid(pid)
	if err != nil {
		t.Fatalf("Getpgid(%d): %v", pid, err)
	}
	if pgid != pid {
		t.Errorf("Getpgid(%d) = %d, want the child to lead its own group", pid, pgid)
	}

	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after cancel")
	}
}
```

Add a shared helper (it is reused by Tasks 4 and 5), plus `pollForFile` if you factor the marker-file wait out of the existing kill test:

```go
// pollForPid waits (bounded, 5s deadline) for a file holding one integer pid.
// Polling with a deadline, not time.Sleep: the deadline is the guarantee.
func pollForPid(t *testing.T, path string) int {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		raw, err := os.ReadFile(path)
		if err == nil {
			if pid, err := strconv.Atoi(strings.TrimSpace(string(raw))); err == nil {
				return pid
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("pid file %q never appeared", path)
		}
		time.Sleep(time.Millisecond) // poll interval only
	}
}
```

New imports: `strconv` (if not present), `syscall`.

- [ ] **Step 2: Run the test to verify it fails**

Run: `go test ./internal/executor -run TestExecutorRunChildLeadsItsProcessGroup`
Expected: FAIL — `Getpgid(pid) = <forge's pgid>, want the child to lead its own group`.

- [ ] **Step 3: Implement `Setpgid`**

Next to the `cmd.Env` line, with a concept comment explaining that this and Task 4's negative-pid kill are an inseparable pair (without `Setpgid` the child inherits forge's group and `kill(-pid)` hits `ESRCH`):

```go
cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `go test ./internal/executor -run TestExecutorRunChildLeadsItsProcessGroup -race`
Expected: PASS.

- [ ] **Step 5: Run the whole package and commit**

Run: `go test ./internal/executor -race` — Expected: PASS.

```bash
git add internal/executor/executor.go internal/executor/executor_test.go
git commit -m "feat(executor): put each job in its own process group"
```

---

### Task 4: Cancel kills the whole group

**Files:**
- Modify: `internal/executor/executor.go`
- Test: `internal/executor/executor_test.go`

**Interfaces:**
- Consumes: Task 3's `Setpgid` invariant and its `pollForPid` helper; the existing `Run` error conversion (`if cerr := ctx.Err(); cerr != nil { err = cerr }`).
- Produces: cancellation semantics that Task 5's `WaitDelay` bounds, plus the `procState(pid string) string` helper Task 5 reuses.

- [ ] **Step 1: Write the failing test**

```go
/*
TestExecutorRunCancelKillsGrandchildren pins V8's headline: cancellation is a
tree operation. The marker file proves the grandchild started, so the test
cannot pass by refusing to launch it. The assertion accepts state Z (dead,
awaiting reap) as well as a missing /proc entry — a correct kill does not
always mean a reaped pid.
*/
func TestExecutorRunCancelKillsGrandchildren(t *testing.T) {
	marker := filepath.Join(t.TempDir(), "gp")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan error, 1)
	go func() {
		_, err := (executor.Executor{SandboxRoot: t.TempDir()}).Run(ctx, job.Job{
			ID:      "j-tree",
			Command: []string{"sh", "-c", fmt.Sprintf("sleep 60 & echo $! > %s; wait", strconv.Quote(marker))},
		})
		done <- err
	}()

	gpid := pollForPid(t, marker)
	cancel()

	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Run() error = %v, want wrapped context.Canceled", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after cancel")
	}

	deadline := time.Now().Add(5 * time.Second)
	for {
		if state := procState(gpid); state == "gone" || state == "Z" {
			return // dead (reaped or awaiting reap)
		}
		if time.Now().After(deadline) {
			t.Fatalf("grandchild %d still running after cancel, state=%s", gpid, procState(gpid))
		}
		time.Sleep(time.Millisecond)
	}
}

// procState reads /proc/<pid>/stat: "gone" when reaped, "Z" when dead but
// unreaped, otherwise the kernel's state letter.
func procState(pid int) string {
	b, err := os.ReadFile(fmt.Sprintf("/proc/%d/stat", pid))
	if err != nil {
		return "gone"
	}
	f := strings.Fields(string(b))
	return f[2]
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `go test ./internal/executor -run TestExecutorRunCancelKillsGrandchildren`
Expected: FAIL — `grandchild <pid> still running after cancel, state=S` (default `CommandContext` kills only `sh`).

- [ ] **Step 3: Implement the group kill**

Override the default cancel (only valid together with Task 3's `Setpgid` — comment both sides):

```go
cmd.Cancel = func() error {
	return syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL) // negative pid = the whole group
}
```

Concept comment: `CommandContext`'s default `Cancel` calls `Process.Kill` on one pid; `os/exec` invokes ours only after `Start` succeeds, which is what makes reading `cmd.Process.Pid` here safe.

- [ ] **Step 4: Run the test to verify it passes**

Run: `go test ./internal/executor -run TestExecutorRunCancelKillsGrandchildren -race`
Expected: PASS.

- [ ] **Step 5: Run the whole package and commit**

Run: `go test ./internal/executor -race` — Expected: PASS, including the pre-existing `TestExecutorRunKillsRunningProcess` (its `errors.Is(context.Canceled)` assertion now passes via our kill).

```bash
git add internal/executor/executor.go internal/executor/executor_test.go
git commit -m "feat(executor): cancel the process group, not just the child"
```

---

### Task 5: `WaitDelay` bounds a leaked stream

**Files:**
- Modify: `internal/executor/executor.go`
- Test: `internal/executor/executor_test.go`

**Interfaces:**
- Consumes: Task 4's group kill and its `procState` helper, Task 3's `pollForPid` helper (the child dies immediately; `WaitDelay` only covers what escapes it).
- Produces: `const waitDelay = time.Second` — referenced by the spec's semantics and the test's timing bound.

- [ ] **Step 1: Write the failing test**

One test function, two subtests — both are the same `WaitDelay` contract (a stream that outlives the child), demanded together before the single-line implementation:

```go
/*
TestExecutorRunWaitDelay pins the pipe bound. Capturing into a bytes.Buffer
means os/exec owns a pipe and a copying goroutine, so Wait blocks until EOF —
and a process that escaped the group (or outlived a successful exit) holds
that EOF hostage. WaitDelay cuts the stream instead. Subtests also pin the
limits honestly: the escapee survives, and an exit-0 with a lingering child
becomes exec.ErrWaitDelay rather than a hang.
*/
func TestExecutorRunWaitDelay(t *testing.T) {
	t.Run("escapee holds the pipe after cancel", func(t *testing.T) {
		if _, err := exec.LookPath("setsid"); err != nil {
			t.Skip("setsid not installed; cannot build an escapee")
		}
		marker := filepath.Join(t.TempDir(), "esc")
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		done := make(chan error, 1)
		go func() {
			_, err := (executor.Executor{SandboxRoot: t.TempDir()}).Run(ctx, job.Job{
				ID:      "j-esc",
				Command: []string{"sh", "-c", fmt.Sprintf("setsid sleep 60 & echo $! > %s; wait", strconv.Quote(marker))},
			})
			done <- err
		}()

		epid := pollForPid(t, marker)
		start := time.Now()
		cancel()

		select {
		case err := <-done:
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("Run() error = %v, want wrapped context.Canceled", err)
			}
			if elapsed := time.Since(start); elapsed > 5*time.Second {
				t.Errorf("Run took %v after cancel, want ~waitDelay (1s)", elapsed)
			}
		case <-time.After(10 * time.Second):
			t.Fatal("Run hung after cancel — the escapee's pipe was never cut")
		}

		if state := procState(epid); state != "R" && state != "S" && state != "Z" {
			t.Errorf("escapee state = %q, want alive — escaping the group is the documented limit", state)
		}
		syscall.Kill(epid, syscall.SIGKILL) // test cleanup: no leaked sleepers
	})

	// The non-cancel flavour: the child exits 0, but a background child keeps
	// the capture pipe open. Without WaitDelay this hangs forever; with it,
	// exec.ErrWaitDelay surfaces because the context is NOT done (so the
	// executor's ctx.Err() preference must not mask it).
	t.Run("exit 0 with a lingering child", func(t *testing.T) {
		marker := filepath.Join(t.TempDir(), "bg")
		done := make(chan error, 1)
		go func() {
			_, err := (executor.Executor{SandboxRoot: t.TempDir()}).Run(context.Background(), job.Job{
				ID:      "j-bg",
				Command: []string{"sh", "-c", fmt.Sprintf("sleep 60 & echo $! > %s; exit 0", strconv.Quote(marker))},
			})
			done <- err
		}()

		select {
		case err := <-done:
			if !errors.Is(err, exec.ErrWaitDelay) {
				t.Fatalf("Run() error = %v, want wrapped exec.ErrWaitDelay", err)
			}
		case <-time.After(10 * time.Second):
			t.Fatal("Run hung on a background child after a successful exit")
		}
		syscall.Kill(pollForPid(t, marker), syscall.SIGKILL) // test cleanup
	})
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `go test ./internal/executor -run TestExecutorRunWaitDelay`
Expected: FAIL — `escapee holds the pipe after cancel: Run hung after cancel — the escapee's pipe was never cut` (10s timeout; `Wait` blocks on the pipe with no `WaitDelay`). The second subtest hangs the same way.

- [ ] **Step 3: Implement `WaitDelay`**

Add the package constant and configure the command:

```go
// waitDelay bounds how long Wait may sit on a capture pipe after the child is
// done: the group is SIGKILLed on cancel, so nothing inside it needs grace —
// this exists only for processes that escaped the group or outlived the exit
// and still hold the stream.
const waitDelay = time.Second
```

```go
cmd.WaitDelay = waitDelay
```

No branch is added for `exec.ErrWaitDelay`: the existing `err != nil` path returns it wrapped with the job id, and the `ctx.Err()` preference is untouched — in this flavour the context is alive, so nothing masks it. `Result.ExitCode` stays 0 (the child did exit 0) while `err` is non-nil; the worker records the attempt failed, per the spec's semantics.

- [ ] **Step 4: Run the test to verify it passes**

Run: `go test ./internal/executor -run TestExecutorRunWaitDelay -race`
Expected: PASS — both subtests return in ~1s each.

- [ ] **Step 5: Run the whole package and commit**

Run: `go test ./internal/executor -race` — Expected: PASS.

```bash
git add internal/executor/executor.go internal/executor/executor_test.go
git commit -m "feat(executor): bound Wait with WaitDelay so escapees cannot hang a run"
```

---

### Task 6: Documentation, full verification, tag

**Files:**
- Modify: `README.md`
- Modify: `docs/LEARNING.md`

**Interfaces:**
- Consumes: Tasks 1–5 (all code complete).
- Produces: nothing programmatically; this is the version's definition of done.

- [ ] **Step 1: Update `README.md`**

- Status line (top): V0–V8 done, tagged `v0`–`v8`; next up V9 — observability.
- Ladder row: `| V8 | Sandboxed execution | os/exec, processes, signals, filesystem | ✅ |`.
- New section `## How it works (V8)` after the V7 section: one short paragraph — each run gets a fresh directory and only `PATH`; cancel signals the process group; `WaitDelay` cuts escapees' streams after a second; and the honest limit, clean room not a jail (no namespaces/cgroups), Unix-only.

- [ ] **Step 2: Add the `docs/LEARNING.md` V8 entry**

Established format (Concept / What I built / What surprised me / Idiom worth remembering / Next time), covering: `cmd.Env` as replacement, the `Setpgid` + negative-pid pair and the `ESRCH` discovery, `WaitDelay` and pipes outliving processes, `MkdirTemp` + `defer` ordering. Written in the author's voice like the V7 entry; draft it, the learner will edit.

- [ ] **Step 3: Full verification**

Run:
```bash
gofmt -l .
go vet ./...
go test ./... -race
go mod tidy && git diff --exit-code go.mod go.sum
```
Expected: `gofmt` prints nothing, vet prints nothing, all packages `ok`, `go.mod`/`go.sum` unchanged (stdlib only).

- [ ] **Step 4: Commit and tag**

```bash
git add README.md docs/LEARNING.md
git commit -m "docs(v8): learning entry and README describe sandboxed execution"
git tag v8
```

- [ ] **Step 5: Confirm earlier versions still pass**

Run: `go test ./... -race` — Expected: every package `ok`, including all V0–V7 suites untouched by this version.
