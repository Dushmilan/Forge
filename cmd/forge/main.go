// Command forge is the Forge command-line front end. In V0 it runs exactly one
// job, given on the command line, and exits.
package main

import (
	"fmt"
	"io"
	"os"

	"github.com/Dushmilan/Forge/internal/executor"
	"github.com/Dushmilan/Forge/internal/job"
)

func main() {
	// main is the only place allowed to call os.Exit; everything testable lives
	// in run, which returns the exit code instead.
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr))
}

// run builds a Job from the command-line arguments, executes it, writes the
// captured output, and returns the process exit code. Taking io.Writer instead
// of *os.File is another implicit interface: os.Stdout and a bytes.Buffer both
// satisfy io.Writer, so tests can capture output without touching the real
// terminal.
func run(args []string, stdout, stderr io.Writer) int {
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
