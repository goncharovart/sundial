package sundial

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Options configures a Scheduler at construction time.
type Options struct {
	// NodeID identifies this scheduler instance in logs, traces, and the
	// sundial_runs.node_id column. Required — pass the pod / hostname.
	NodeID string

	// TickInterval is how often the dispatcher polls for due jobs.
	// Smaller values reduce scheduling lag at the cost of more DB load.
	// Defaults to 1 second; values below 100ms are rejected.
	TickInterval time.Duration

	// ShutdownGrace is how long Run() waits for in-flight handlers to finish
	// after the parent context is cancelled. Defaults to 30 seconds.
	ShutdownGrace time.Duration

	// Schema is the Postgres schema name where Sundial tables live.
	// Defaults to "public".
	Schema string
}

// Errors returned by the public Scheduler API.
var (
	ErrEmptyNodeID       = errors.New("sundial: NodeID is required")
	ErrTickTooSmall      = errors.New("sundial: TickInterval must be ≥ 100ms")
	ErrJobNameTaken      = errors.New("sundial: job name already registered")
	ErrEmptyJobName      = errors.New("sundial: job name is required")
	ErrNilHandler        = errors.New("sundial: handler is required")
	ErrNilSchedule       = errors.New("sundial: schedule is required")
	ErrSchedulerStopped  = errors.New("sundial: scheduler is stopped")
)

// Scheduler is the main runtime entry point.
//
// A Scheduler holds a Postgres pool, a registry of jobs, and (once Run is
// called) a dispatcher goroutine. One process should construct exactly one
// Scheduler; multi-tenancy is achieved by sharing the same database across
// processes, not by running multiple Schedulers per process.
type Scheduler struct {
	pool *pgxpool.Pool
	opts Options

	mu      sync.RWMutex
	jobs    map[string]*Job
	running bool
	stopped bool
}

// New constructs a Scheduler. The pool must already be connected; New does not
// open connections itself. Validation of options happens here so misconfiguration
// surfaces at startup rather than on the first tick.
func New(pool *pgxpool.Pool, opts Options) (*Scheduler, error) {
	if pool == nil {
		return nil, errors.New("sundial: pool is required")
	}
	if opts.NodeID == "" {
		return nil, ErrEmptyNodeID
	}
	if opts.TickInterval == 0 {
		opts.TickInterval = time.Second
	}
	if opts.TickInterval < 100*time.Millisecond {
		return nil, fmt.Errorf("%w: got %s", ErrTickTooSmall, opts.TickInterval)
	}
	if opts.ShutdownGrace == 0 {
		opts.ShutdownGrace = 30 * time.Second
	}
	if opts.Schema == "" {
		opts.Schema = "public"
	}
	return &Scheduler{
		pool: pool,
		opts: opts,
		jobs: make(map[string]*Job),
	}, nil
}

// Schedule registers a job with this Scheduler. It does not persist the job
// to the database — that happens on the first tick or when Run is called.
// Calling Schedule after Stop returns ErrSchedulerStopped.
func (s *Scheduler) Schedule(name string, schedule Schedule, fn HandlerFunc, opts ...JobOption) (*Job, error) {
	if name == "" {
		return nil, ErrEmptyJobName
	}
	if schedule == nil {
		return nil, ErrNilSchedule
	}
	if fn == nil {
		return nil, ErrNilHandler
	}

	jobOpts := JobOptions{}
	for _, apply := range opts {
		apply(&jobOpts)
	}
	jobOpts = jobOpts.withDefaults()

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.stopped {
		return nil, ErrSchedulerStopped
	}
	if _, exists := s.jobs[name]; exists {
		return nil, fmt.Errorf("%w: %q", ErrJobNameTaken, name)
	}
	job := &Job{
		Name:     name,
		Schedule: schedule,
		Handler:  fn,
		Options:  jobOpts,
	}
	s.jobs[name] = job
	return job, nil
}

// Jobs returns a snapshot of currently registered jobs in deterministic
// insertion-independent order (sorted by name). The returned slice is safe
// to modify; the underlying Job pointers are not.
func (s *Scheduler) Jobs() []*Job {
	s.mu.RLock()
	defer s.mu.RUnlock()

	names := make([]string, 0, len(s.jobs))
	for n := range s.jobs {
		names = append(names, n)
	}
	// sort by name for stable output (avoids importing sort by name alone)
	for i := 1; i < len(names); i++ {
		for j := i; j > 0 && names[j-1] > names[j]; j-- {
			names[j-1], names[j] = names[j], names[j-1]
		}
	}
	out := make([]*Job, 0, len(names))
	for _, n := range names {
		out = append(out, s.jobs[n])
	}
	return out
}

// Stop marks the scheduler as stopped so that no new jobs can be registered.
// It does NOT cancel the dispatcher — Run shuts down via its own context.
// Stop is idempotent.
func (s *Scheduler) Stop() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.stopped = true
}

// Run blocks while the dispatcher loop runs, returning when ctx is cancelled
// or a fatal error occurs.
//
// The dispatcher loop is not yet implemented; this method returns immediately
// with a sentinel so callers can compile against the final API.
//
//nolint:revive // intentional pre-MVP stub; will land in a follow-up commit.
func (s *Scheduler) Run(ctx context.Context) error {
	s.mu.Lock()
	if s.running {
		s.mu.Unlock()
		return errors.New("sundial: Run is already in progress")
	}
	s.running = true
	s.mu.Unlock()

	// Real dispatcher loop arrives in feat(dispatcher). For now we honour
	// ctx so callers can integrate with their lifecycle in advance of the
	// real implementation.
	<-ctx.Done()

	s.mu.Lock()
	s.running = false
	s.mu.Unlock()
	return ctx.Err()
}

// JobOption mutates a JobOptions value. Used with Schedule to keep the
// call site terse when defaults are mostly fine.
type JobOption func(*JobOptions)

// WithTimeout sets the per-attempt timeout.
func WithTimeout(d time.Duration) JobOption {
	return func(o *JobOptions) { o.Timeout = d }
}

// WithLeaderOnly restricts the job to run only on the cluster leader.
func WithLeaderOnly() JobOption {
	return func(o *JobOptions) { o.LeaderOnly = true }
}

// WithMissedFire sets the missed-fire recovery policy.
func WithMissedFire(p MissedFirePolicy) JobOption {
	return func(o *JobOptions) { o.MissedFire = p }
}

// WithRetry replaces the retry policy.
func WithRetry(r RetryPolicy) JobOption {
	return func(o *JobOptions) { o.Retry = r }
}
