// Command forge is the Forge command-line front end. In V0 it runs exactly one
// job, given on the command line, and exits.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/Dushmilan/Forge/internal/api"
	"github.com/Dushmilan/Forge/internal/executor"
	"github.com/Dushmilan/Forge/internal/job"
	"github.com/Dushmilan/Forge/internal/store"
)

func main() {
	/**
	main is the only place allowed to call os.Exit; everything testable lives
	in run, which returns the exit code instead.
	**/
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr))
}

/*
run builds a Job from the command-line arguments, executes it, writes the
captured output, and returns the process exit code. Taking io.Writer instead
of *os.File is another implicit interface: os.Stdout and a bytes.Buffer both
satisfy io.Writer, so tests can capture output without touching the real
terminal.
*/
func run(args []string, stdout, stderr io.Writer) int {
	/*
		The serve subcommand is recognized by name BEFORE the args are
		treated as a job command, so `forge serve` starts a server instead of
		trying to execute a program named "serve".
	*/
	if len(args) > 0 && args[0] == "serve" {
		/*
			signal.NotifyContext translates OS signals (Ctrl-C, SIGTERM) into
			ctx cancellation — context is the one knob every blocking call in
			Go already understands.
		*/
		ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
		defer stop() // stop deregisters the signal handlers on the way out
		return runServe(ctx, args[1:], stdout, stderr)
	}

	if len(args) == 0 {
		fmt.Fprintln(stderr, "usage: forge <command> [args...]")
		return 2
	}

	j := job.Job{ID: "cli", Command: args}

	var e executor.Executor
	res, err := e.Run(j)
	if err != nil {
		fmt.Fprintf(stderr, "forge: %v\n", err)
		// Propagate the command's own exit code when it ran and failed, so
		// callers can compose Forge like any other Unix tool. Fall back to 1
		// when the process never started (no exit code to report).
		if res.ExitCode != 0 {
			return res.ExitCode
		}
		return 1
	}

	fmt.Fprint(stdout, res.Stdout)
	return 0
}

// runServe parses serve flags, wires store + API into an http.Server, and
// serves until ctx is cancelled. It returns an exit code rather than
// calling os.Exit, keeping main the only place that terminates the process
// — the same discipline as run.
func runServe(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	// A FlagSet (not the global flag.CommandLine) means serve's flags are
	// parsed in isolation and testable — the global one would panic on a
	// second Parse.
	fs := flag.NewFlagSet("serve", flag.ContinueOnError)
	fs.SetOutput(stderr)
	addr := fs.String("addr", ":8080", "address to listen on")
	data := fs.String("data", "data", "directory holding jobs.json")
	if err := fs.Parse(args); err != nil {
		return 2 // flag misuse is a usage error, like no-args in run
	}

	var e executor.Executor
	srv := &http.Server{
		Addr:    *addr,
		Handler: api.New(e, store.New(*data)).Handler(),
	}

	// Listen ourselves instead of srv.ListenAndServe() so we can print the
	// REAL address: with -addr ":0" the OS picked an ephemeral port that
	// only the listener knows.
	ln, err := net.Listen("tcp", *addr)
	if err != nil {
		fmt.Fprintf(stderr, "forge: %v\n", err)
		return 1
	}
	fmt.Fprintf(stdout, "listening on %s\n", ln.Addr())

	// Serve in a goroutine; the main goroutine selects between the two
	// outcomes. This is the ONLY goroutine V1 adds: it exists to bridge
	// ctx → Shutdown, not to run jobs (that is V2).
	serveErr := make(chan error, 1)
	go func() { serveErr <- srv.Serve(ln) }()

	select {
	case <-ctx.Done():
		// Shutdown stops accepting and waits for in-flight requests; a
		// deadline caps the wait so a stuck handler cannot hang shutdown
		// forever.
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdownCtx)
		// Belt and braces: if Shutdown raced ahead of Serve registering the
		// listener, closing it ourselves still guarantees Serve returns.
		// net.ErrClosed (from that close) and http.ErrServerClosed (from
		// Shutdown) both mean "we meant to stop" — not a failure.
		_ = ln.Close()
		<-serveErr // join: don't return until the serving goroutine has
		return 0
	case err := <-serveErr:
		if err != nil && !errors.Is(err, http.ErrServerClosed) && !errors.Is(err, net.ErrClosed) {
			fmt.Fprintf(stderr, "forge: %v\n", err)
			return 1
		}
		return 0
	}
}
