package sundial

import (
	"context"
	"fmt"
	"log/slog"
	"math"
	"math/rand"
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
	leader    Leader
	logger    *slog.Logger
	telemetry *telemetry
	inFlight  sync.WaitGroup

	// attempts tracks per-(job, fire-time) execution attempt counts
	// so the retry-with-backoff path can decide whether to give up
	// (dead-letter) or schedule the next retry. The map is kept
	// in-memory only — a process crash mid-retry resets the counter
	// to 1, which is acceptable for the MVP because retries land on
	// real timestamps in storage, so the worst case is one extra
	// duplicate attempt after a crash.
	attemptsMu sync.Mutex
	attempts   map[string]int // keyed by jobID
}

// bumpAttempt is keyed on jobID alone, not on (jobID, fire_time).
// fire_time changes every time ScheduleRetry pulls it back, so a
// per-fire key would always read 1 and the retry budget would never
// run out. Per-job keys give exactly the semantics we want: a stable
// counter while the same job is failing, reset to zero on the first
// successful run after the failure streak.
func (d *dispatcher) bumpAttempt(jobID string) int {
	d.attemptsMu.Lock()
	defer d.attemptsMu.Unlock()
	if d.attempts == nil {
		d.attempts = make(map[string]int)
	}
	d.attempts[jobID]++
	return d.attempts[jobID]
}

func (d *dispatcher) forgetAttempt(jobID string) {
	d.attemptsMu.Lock()
	defer d.attemptsMu.Unlock()
	delete(d.attempts, jobID)
}

// run drives the loop until ctx is cancelled. It returns ctx.Err() (or
// nil if the context was never cancelled, which only happens in tests
// that swap the loop out).
func (d *dispatcher) run(ctx context.Context) error {
	ticker := time.NewTicker(d.scheduler.opts.TickInterval)
	defer ticker.Stop()

	// Best-effort initial leader attempt before the first tick so a
	// LeaderOnly job whose fire is already due doesn't get skipped
	// on the very first cycle when it could have been us.
	d.maybeRenewLeader(ctx)

	var leaderTicker *time.Ticker
	var leaderC <-chan time.Time
	if d.leader != nil {
		leaderTicker = time.NewTicker(d.scheduler.opts.LeaderRenewInterval)
		defer leaderTicker.Stop()
		leaderC = leaderTicker.C
	}

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
		case <-leaderC:
			d.maybeRenewLeader(ctx)
		}
	}
}

// maybeRenewLeader is a single try-acquire attempt for the cluster
// leader role. Postgres releases the advisory lock automatically when
// the leader's connection closes, so a crashed leader naturally
// liberates the role within seconds — non-leaders pick it up here on
// the next renewal tick.
func (d *dispatcher) maybeRenewLeader(ctx context.Context) {
	if d.leader == nil {
		return
	}
	was := d.leader.IsLeader()
	ok, err := d.leader.TryAcquire(ctx, d.scheduler.opts.NodeID)
	if err != nil {
		d.logger.Warn("leader try-acquire", "error", err)
		return
	}
	if ok && !was {
		d.logger.Info("became leader", "node", d.scheduler.opts.NodeID)
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
//
// If the job's actual fire was missed by significantly more than one
// tick interval (e.g. the cluster was down across the window), the
// job's MissedFirePolicy decides what to do:
//
//   - MissedFireSkip: record the miss and jump straight to the next
//     future fire — handler does NOT run for the stale window.
//   - MissedFireRunOnce: run a single catch-up fire and then advance
//     to the next future fire. This is the default.
//   - MissedFireRunAll: run every missed fire in order. Reserved for
//     audit-style jobs; in this MVP commit we implement Skip and
//     RunOnce; RunAll degrades to RunOnce until the dedicated
//     iterator lands in the next milestone.
func (d *dispatcher) maybeDispatch(ctx context.Context, dj DueJob, now time.Time) {
	job := d.scheduler.lookup(dj.Name)
	if job == nil {
		// The DB knows about this job but this process never
		// registered a handler for it. That is legitimate when
		// multiple binaries share a database — silently ignore.
		return
	}

	// LeaderOnly jobs are coordination hints, not fences — the
	// ClaimJob race below would still guarantee single-execution
	// across nodes. But by short-circuiting here we keep the wire
	// traffic of failed claim attempts to a minimum on follower
	// nodes, which matters when the dispatcher has hundreds of jobs.
	if job.Options.LeaderOnly && d.leader != nil && !d.leader.IsLeader() {
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

	// A "miss" is any lag larger than 3× the tick interval — that
	// threshold is conservative enough that ordinary scheduling
	// jitter never trips it, but small enough that an outage of a
	// few seconds is caught.
	missThreshold := 3 * d.scheduler.opts.TickInterval
	lag := now.Sub(dj.NextFireTime)
	missed := lag > missThreshold

	if missed && job.Options.MissedFire == MissedFireSkip {
		// Skip path: advance next_fire forward without executing.
		// Use the next fire after `now`, not after dj.NextFireTime,
		// so we don't immediately re-claim again on the next tick.
		future, ferr := computeNextFire(dj, now)
		if ferr == nil && !future.IsZero() {
			if _, claimErr := d.storage.ClaimJob(ctx, dj.ID, future); claimErr != nil {
				d.logger.Error("skip missed fire", "job", dj.Name, "error", claimErr)
			}
		}
		d.logger.Info("missed fire skipped",
			"job", dj.Name,
			"missed_by", lag,
			"fire_time", dj.NextFireTime,
		)
		return
	}

	if missed && job.Options.MissedFire == MissedFireRunAll {
		// RunAll iterates every missed instant strictly inside
		// (NextFireTime, now), emitting one execution per fire.
		// This is leader-only: in a multi-node cluster only the
		// leader replays the window, otherwise N nodes would
		// each emit the full catch-up volume.
		if d.leader != nil && !d.leader.IsLeader() {
			return
		}
		d.runAllMissed(ctx, job, dj, now)
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
	d.telemetry.claimedInc(ctx, dj.Name)

	if missed {
		d.logger.Info("missed fire recovering (run-once)",
			"job", dj.Name,
			"missed_by", lag,
			"fire_time", dj.NextFireTime,
		)
	}

	// Regular concurrent dispatch — handler runs in its own goroutine
	// so the tick loop can keep claiming other jobs.
	d.inFlight.Add(1)
	go d.execute(ctx, job, dj, nextFire)
}

// execute runs a single handler attempt with per-job timeout and
// persists the outcome.
func (d *dispatcher) execute(parent context.Context, job *Job, dj DueJob, claimedFor time.Time) {
	defer d.inFlight.Done()

	d.telemetry.jobsRunning.Add(parent, 1)
	defer d.telemetry.jobsRunning.Add(parent, -1)

	spanCtx, span := d.telemetry.startRunSpan(
		parent, dj.Name, dj.ID, d.scheduler.opts.NodeID, dj.ScheduleKind, dj.NextFireTime,
	)
	defer span.End()

	ctx := spanCtx
	var cancel context.CancelFunc
	if job.Options.Timeout > 0 {
		ctx, cancel = context.WithTimeout(ctx, job.Options.Timeout)
		defer cancel()
	}

	startedAt := time.Now().UTC()
	d.telemetry.recordLag(spanCtx, dj.Name, dj.NextFireTime, startedAt)

	attempt := d.bumpAttempt(dj.ID)

	handlerErr := safeRun(ctx, job.Handler)
	finishedAt := time.Now().UTC()

	outcome := RunSucceeded
	errString := ""
	if handlerErr != nil {
		outcome = RunFailed
		errString = handlerErr.Error()
		span.RecordError(handlerErr)
	}
	d.telemetry.recordDuration(spanCtx, span, dj.Name, finishedAt.Sub(startedAt), outcome)

	// Retry-with-backoff: on failure, if we have attempts left,
	// pull the row's next_fire back to "now + backoff" so the next
	// tick re-dispatches this same fire. On a successful run (or
	// when the retry budget is exhausted) we forget the counter so
	// the next regular fire starts fresh.
	if handlerErr != nil && attempt < job.Options.Retry.MaxAttempts {
		backoff := computeBackoff(job.Options.Retry, attempt)
		retryAt := time.Now().UTC().Add(backoff)
		if err := d.storage.ScheduleRetry(parent, dj.ID, retryAt); err != nil {
			d.logger.Error("schedule retry",
				"job", dj.Name,
				"attempt", attempt,
				"error", err,
			)
		} else {
			d.logger.Info("job failed, retry scheduled",
				"job", dj.Name,
				"attempt", attempt,
				"backoff", backoff,
				"retry_at", retryAt,
			)
		}
	} else if handlerErr != nil {
		outcome = RunDeadLetter
		d.forgetAttempt(dj.ID)
		d.logger.Warn("job failed and retries exhausted",
			"job", dj.Name,
			"attempts", attempt,
			"error", handlerErr,
		)
	} else {
		d.forgetAttempt(dj.ID)
	}

	rec := RunRecord{
		JobID:      dj.ID,
		FireTime:   dj.NextFireTime,
		StartedAt:  startedAt,
		FinishedAt: finishedAt,
		NodeID:     d.scheduler.opts.NodeID,
		Attempt:    attempt,
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

// runAllMissed replays every missed fire time in the window
// (dj.NextFireTime, now), in order. Each instant goes through the
// usual claim/execute/record pipeline so existing observability
// (spans, run records, retry budget) applies uniformly to catch-up
// fires and live fires. After the iterator drains, the loop falls
// back to the regular cadence — the next ClaimJob on a non-missed
// schedule sets next_fire to a future instant.
//
// The window is capped at maxCatchupInstants to defend against a
// pathological "down for a month at 1s cadence" scenario where the
// iterator would otherwise emit ~2.5M handler calls in one tick.
const maxCatchupInstants = 256

func (d *dispatcher) runAllMissed(ctx context.Context, job *Job, dj DueJob, now time.Time) {
	sched, err := loadScheduleFromDue(dj)
	if err != nil {
		d.logger.Error("missed-fire iterator: load schedule", "job", dj.Name, "error", err)
		return
	}
	instants := iterateSchedule(sched, dj.NextFireTime, now, maxCatchupInstants)
	if len(instants) == 0 {
		// Single catch-up fire even if the iterator yielded nothing
		// — RunOnce semantics, since we know the window was missed.
		d.executeAtFire(ctx, job, dj)
		return
	}
	d.logger.Info("missed fire RunAll replay",
		"job", dj.Name,
		"instants", len(instants),
		"window_start", dj.NextFireTime,
		"window_end", now,
	)
	for _, fireTime := range instants {
		ddj := dj
		ddj.NextFireTime = fireTime
		d.executeAtFire(ctx, job, ddj)
	}
	// After replay, advance to the next future fire so the regular
	// loop does not see the row as missed on the next tick.
	if future := sched.Next(now); !future.IsZero() {
		if _, err := d.storage.ClaimJob(ctx, dj.ID, future); err != nil {
			d.logger.Error("missed-fire RunAll: advance next_fire", "job", dj.Name, "error", err)
		}
	}
}

// executeAtFire is a thin wrapper used by both the regular tick path
// and the RunAll iterator: bump in-flight, run execute() synchronously
// (the iterator MUST serialise — a 5-fire RunAll should not spawn 5
// goroutines that race for the same retry counter).
func (d *dispatcher) executeAtFire(ctx context.Context, job *Job, dj DueJob) {
	d.inFlight.Add(1)
	d.execute(ctx, job, dj, dj.NextFireTime)
}

// loadScheduleFromDue rehydrates a Schedule from the persisted (kind,
// expression) pair. This is the same parsing the dispatcher does in
// computeNextFire — we keep both paths through one helper so adding a
// new schedule kind only requires touching one switch.
func loadScheduleFromDue(dj DueJob) (Schedule, error) {
	switch dj.ScheduleKind {
	case "cron":
		return Cron(dj.ScheduleExpr)
	case "every":
		d, err := time.ParseDuration(dj.ScheduleExpr)
		if err != nil {
			return nil, err
		}
		return Every(d)
	case "at":
		t, err := time.Parse(time.RFC3339, dj.ScheduleExpr)
		if err != nil {
			return nil, err
		}
		return At(t), nil
	default:
		return nil, fmt.Errorf("unknown schedule kind %q", dj.ScheduleKind)
	}
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

// computeBackoff returns the wait between attempt and attempt+1, using
// exponential growth (InitialBackoff × Multiplier^(attempt-1)) clamped
// to MaxBackoff, then jittered by a uniform factor in [0.5, 1.5).
//
// Jitter is the part that matters most under contention — without it,
// a thundering herd of nodes whose handlers fail at the same instant
// would all retry in lockstep and hammer the same upstream over and
// over. The factored jitter spreads retries over a 1× window.
func computeBackoff(p RetryPolicy, attempt int) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	base := float64(p.InitialBackoff) * math.Pow(p.Multiplier, float64(attempt-1))
	if base > float64(p.MaxBackoff) {
		base = float64(p.MaxBackoff)
	}
	// jitter in [0.5, 1.5)
	jitter := 0.5 + rand.Float64() //nolint:gosec // not crypto-sensitive
	return time.Duration(base * jitter)
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
