package sundial

import (
	"context"
	"time"
)

// HandlerFunc is the unit of work Sundial executes when a job fires.
//
// Implementations must respect the context: a cancellation means the
// scheduler is stopping or the per-job timeout has elapsed, and the handler
// should return as quickly as it can. Returning a non-nil error marks the
// run as failed and triggers retry according to JobOptions.
type HandlerFunc func(ctx context.Context) error

// MissedFirePolicy determines what happens to fires that should have run while
// the cluster was unavailable (e.g. during a deploy or a network partition).
//
// Recovery is leader-only: only the node currently holding the leader lock
// runs missed-fire computation, so policies are applied at most once.
type MissedFirePolicy int

const (
	// MissedFireRunOnce executes one catch-up fire when missed fires are
	// detected, regardless of how many were missed. This is the default and
	// the right choice for most idempotent jobs.
	MissedFireRunOnce MissedFirePolicy = iota

	// MissedFireSkip records missed fires for observability and moves on
	// without running them. Use this when running late catch-up is worse than
	// silently missing a window (e.g. user-facing reminders).
	MissedFireSkip

	// MissedFireRunAll executes every missed fire in order. Use only for
	// audit-style jobs that must produce one record per fire-time and where
	// the handler is fast and idempotent.
	MissedFireRunAll
)

// RetryPolicy describes how Sundial retries a failed job execution.
type RetryPolicy struct {
	// MaxAttempts is the total number of attempts including the first one.
	// A value of 1 means "no retries". Zero is treated as the default (3).
	MaxAttempts int

	// InitialBackoff is the delay before the second attempt.
	// Zero is treated as the default (1 second).
	InitialBackoff time.Duration

	// MaxBackoff caps exponential growth of the delay.
	// Zero is treated as the default (5 minutes).
	MaxBackoff time.Duration

	// Multiplier scales the backoff between attempts; the actual delay is
	// computed as InitialBackoff * Multiplier^(attempt-1), clamped to
	// MaxBackoff, then jittered by a uniform factor of [0.5, 1.5).
	// Zero is treated as the default (2.0).
	Multiplier float64
}

// JobOptions configures how a single job is scheduled and executed.
//
// All fields are optional; the zero JobOptions value is safe to use.
type JobOptions struct {
	// Timeout limits how long a single attempt may run. The handler's context
	// is cancelled when this elapses. Zero disables the per-attempt timeout.
	Timeout time.Duration

	// LeaderOnly restricts this job so that only the cluster leader runs it.
	// Use for tasks that must run at most once per fire across the cluster
	// even if multiple nodes would otherwise be eligible.
	LeaderOnly bool

	// MissedFire decides what to do with fires that should have run during
	// downtime. Defaults to MissedFireRunOnce.
	MissedFire MissedFirePolicy

	// Retry describes the retry strategy applied to handler errors.
	// The zero RetryPolicy uses defaults (3 attempts, 1s → 5m exponential
	// backoff, factor 2.0).
	Retry RetryPolicy
}

// Job is a registered scheduled task. Instances are not created by users
// directly — Scheduler.Schedule constructs and returns them.
type Job struct {
	Name     string
	Schedule Schedule
	Handler  HandlerFunc
	Options  JobOptions
}

// withDefaults returns a copy of opts with zero values replaced by the
// package-wide defaults documented on each field.
func (opts JobOptions) withDefaults() JobOptions {
	out := opts
	r := &out.Retry
	if r.MaxAttempts == 0 {
		r.MaxAttempts = 3
	}
	if r.InitialBackoff == 0 {
		r.InitialBackoff = time.Second
	}
	if r.MaxBackoff == 0 {
		r.MaxBackoff = 5 * time.Minute
	}
	if r.Multiplier == 0 {
		r.Multiplier = 2.0
	}
	return out
}
