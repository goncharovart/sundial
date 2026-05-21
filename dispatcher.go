package sundial

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"
)

// dispatcher runs the per-tick fetch → claim → execute → record loop.
//
// It is created internally by Scheduler.Run and owns no state of its own
// beyond the in-flight WaitGroup. The Scheduler holds the canonical Job
// registry; dispatcher consults it by name when a DueJob comes back from
// storage. This split lets storage tests stay handler-free and dispatcher
// tests stay storage-free.
type dispatcher struct {
	scheduler *Scheduler
	storage   Storage
	logger    *slog.Logger
	inFlight  sync.WaitGroup
}

// run drives the loop until ctx is cancelled. It returns ctx.Err() (or
// nil if the context was never cancelled, which only happens in tests
// that swap the loop out).
func (d *dispatcher) run(ctx context.Context) error {
	ticker := time.NewTicker(d.scheduler.opts.TickInterval)
	defer ticker.Stop()

	// Run one tick immediately so jobs whose next_fire is in the past
	// at startup do not wait for the first ticker fire.
	d.tick(ctx)

	for {
		select {
		case <-ctx.Done():
			d.drain()
			return ctx.Err()
		case <-ticker.C:
			d.tick(ctx)
		}
	}
}

// tick performs one fetch-and-dispatch cycle. It never returns an error
// up the stack — a transient storage failure should not bring the
// scheduler down, so failures are logged and the loop continues.
func (d *dispatcher) tick(ctx context.Context) {
	now := time.Now().UTC()
	due, err := d.storage.FetchDue(ctx, now, 64)
	if err != nil {
		d.logger.Error("fetch due jobs", "error", err)
		return
	}
	for _, dj := range due {
		d.maybeDispatch(ctx, dj, now)
	}
}

// maybeDispatch tries to claim a single due job and, on success,
// schedules its handler to run concurrently.
func (d *dispatcher) maybeDispatch(ctx context.Context, dj DueJob, now time.Time) {
	job := d.scheduler.lookup(dj.Name)
	if job == nil {
		// The DB knows about this job but this process never
		// registered a handler for it. That is legitimate when
		// multiple binaries share a database — silently ignore.
		return
	}

	nextFire, err := computeNextFire(dj, now)
	if err != nil {
		d.logger.Error("compute next fire", "job", dj.Name, "error", err)
		return
	}
	if nextFire.IsZero() {
		// One-shot job whose firing window has closed.
		return
	}

	claimed, err := d.storage.ClaimJob(ctx, dj.ID, nextFire)
	if err != nil {
		d.logger.Error("claim job", "job", dj.Name, "error", err)
		return
	}
	if !claimed {
		// Another node won the race. That is the happy path under
		// contention — nothing to do.
		return
	}

	d.inFlight.Add(1)
	go d.execute(ctx, job, dj, nextFire)
}

// execute runs a single handler attempt with per-job timeout and
// persists the outcome.
func (d *dispatcher) execute(parent context.Context, job *Job, dj DueJob, claimedFor time.Time) {
	defer d.inFlight.Done()

	ctx := parent
	var cancel context.CancelFunc
	if job.Options.Timeout > 0 {
		ctx, cancel = context.WithTimeout(parent, job.Options.Timeout)
		defer cancel()
	}

	startedAt := time.Now().UTC()
	handlerErr := safeRun(ctx, job.Handler)
	finishedAt := time.Now().UTC()

	outcome := RunSucceeded
	errString := ""
	if handlerErr != nil {
		outcome = RunFailed
		errString = handlerErr.Error()
	}

	rec := RunRecord{
		JobID:      dj.ID,
		FireTime:   dj.NextFireTime,
		StartedAt:  startedAt,
		FinishedAt: finishedAt,
		NodeID:     d.scheduler.opts.NodeID,
		Attempt:    1,
		Outcome:    outcome,
		Error:      errString,
	}
	if err := d.storage.RecordRun(parent, rec); err != nil {
		d.logger.Error("record run",
			"job", dj.Name,
			"outcome", outcome,
			"error", err,
		)
	}

	if handlerErr != nil {
		d.logger.Warn("job failed",
			"job", dj.Name,
			"fire_time", dj.NextFireTime,
			"error", handlerErr,
		)
	} else {
		d.logger.Info("job done",
			"job", dj.Name,
			"fire_time", dj.NextFireTime,
			"duration", finishedAt.Sub(startedAt),
		)
	}

	_ = claimedFor // reserved for future retry-by-attempt logic
}

// drain blocks until all in-flight handlers finish or ShutdownGrace
// elapses, whichever comes first.
func (d *dispatcher) drain() {
	if d.scheduler.opts.ShutdownGrace <= 0 {
		d.inFlight.Wait()
		return
	}
	done := make(chan struct{})
	go func() {
		d.inFlight.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(d.scheduler.opts.ShutdownGrace):
		d.logger.Warn("shutdown grace elapsed with in-flight handlers")
	}
}

// safeRun invokes the handler and converts a panic into an error so a
// single misbehaving job cannot bring the dispatcher down.
func safeRun(ctx context.Context, h HandlerFunc) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("sundial: handler panicked: %v", r)
		}
	}()
	return h(ctx)
}

// computeNextFire derives the next fire time after `from` for a DueJob,
// re-parsing the stored schedule expression. It returns a zero time if
// the schedule has no further fires (the only case today is At() whose
// instant has passed).
func computeNextFire(dj DueJob, from time.Time) (time.Time, error) {
	switch dj.ScheduleKind {
	case "cron":
		s, err := Cron(dj.ScheduleExpr)
		if err != nil {
			return time.Time{}, fmt.Errorf("cron %q: %w", dj.ScheduleExpr, err)
		}
		return s.Next(from), nil

	case "every":
		d, err := time.ParseDuration(dj.ScheduleExpr)
		if err != nil {
			return time.Time{}, fmt.Errorf("duration %q: %w", dj.ScheduleExpr, err)
		}
		s, err := Every(d)
		if err != nil {
			return time.Time{}, err
		}
		return s.Next(from), nil

	case "at":
		t, err := time.Parse(time.RFC3339, dj.ScheduleExpr)
		if err != nil {
			return time.Time{}, fmt.Errorf("at %q: %w", dj.ScheduleExpr, err)
		}
		return At(t).Next(from), nil

	default:
		return time.Time{}, fmt.Errorf("unknown schedule kind %q", dj.ScheduleKind)
	}
}
