/*
Package recovery reconciles the store with a fresh process. A crash leaves
records asserting states no goroutine is working on any more: running jobs
with a dead worker, pending jobs that were still buffered in the channel.
Recovery turns that snapshot back into a runnable queue before the pool
starts, so startup never depends on what the previous process happened to
have in memory.
*/
package recovery

import (
	"fmt"

	"github.com/Dushmilan/Forge/internal/job"
	"github.com/Dushmilan/Forge/internal/store"
)

// Config is the policy a restart uses to judge interrupted work. MaxAttempts
// is the current process's budget; a restart with a smaller budget may fail a
// job the previous process would have retried.
type Config struct {
	MaxAttempts int
}

/*
Recover repairs orphaned records and returns the ones the caller must enqueue.
A running record is an orphan by definition: this process just started and is
the only one allowed to run jobs. There is deliberately no heartbeat-age check
— comparing timestamps only becomes meaningful when a second process could
still own the record (V11); pretending to enforce a lease now would add
machinery with no honest failure mode to catch.
*/
func Recover(st *store.Store, cfg Config) ([]store.Record, error) {
	records, err := st.List()
	if err != nil {
		return nil, fmt.Errorf("recovery: list: %w", err)
	}

	var requeue []store.Record
	for _, rec := range records {
		switch rec.Status {
		case job.StatusPending:
			// Already runnable; recovery only needs to hand it to the queue.
			requeue = append(requeue, rec)

		case job.StatusRunning:
			/*
				The interrupted attempt was counted when it started, so a
				restart costs one attempt rather than resetting the budget.
				Exhausted records fail here: a job that kills the process
				every time must not crash-loop forever.
			*/
			if rec.Attempts >= cfg.MaxAttempts {
				rec.Status = job.StatusFailed
				rec.Err = fmt.Sprintf("job interrupted by forge restart; attempts exhausted (%d/%d)", rec.Attempts, cfg.MaxAttempts)
			} else {
				rec.Status = job.StatusPending
				rec.Err = fmt.Sprintf("job interrupted by forge restart after attempt %d", rec.Attempts)
				requeue = append(requeue, rec)
			}
			/*
				Save enforces the state machine, so this repair is only
				possible because running→pending and running→failed are
				explicit edges. A repair the store refused would be a bug
				worth surfacing, hence the wrapped error instead of a
				skipped record.
			*/
			if err := st.Save(rec); err != nil {
				return nil, fmt.Errorf("recovery: save %s: %w", rec.ID, err)
			}
		}
	}
	return requeue, nil
}
