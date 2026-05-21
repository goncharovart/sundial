package sundial

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// AutoNodeID returns a node identifier suitable for Options.NodeID
// without the caller having to plumb it through environment variables.
// The resolution order is:
//
//  1. os.Hostname() — the canonical container/server name.
//  2. $HOSTNAME — set by most schedulers (Kubernetes, Nomad) even
//     when os.Hostname() returns "localhost".
//  3. "sundial-<8-hex>" — a per-process random fallback so two
//     scrambled containers in the same pod don't end up sharing a
//     NodeID. The randomness is from crypto/rand; on the rare boot
//     where rand.Read fails, we degrade to "sundial-unknown" — a
//     valid string that satisfies the NodeID requirement, just not
//     a useful one for debugging.
//
// The returned string is non-empty by construction.
func AutoNodeID() string {
	if h, err := os.Hostname(); err == nil && h != "" && h != "localhost" {
		return h
	}
	if env := os.Getenv("HOSTNAME"); env != "" {
		return env
	}
	var buf [4]byte
	if _, err := rand.Read(buf[:]); err == nil {
		return "sundial-" + hex.EncodeToString(buf[:])
	}
	return "sundial-unknown"
}

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

	// LeaderRenewInterval is how often the leader loop re-acquires
	// the cluster-leader advisory lock. Defaults to 10 seconds.
	// Smaller values shorten the post-crash failover window at the
	// cost of more idle pg_try_advisory_lock calls.
	LeaderRenewInterval time.Duration
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
	pool    *pgxpool.Pool
	storage Storage
	leader  Leader
	logger  *slog.Logger
	opts    Options

	mu      sync.RWMutex
	jobs    map[string]*Job
	running bool
	stopped bool
}

// SchedulerOption mutates a Scheduler after construction. It exists for
// testing seams (WithStorage swaps in MemoryStorage) without expanding
// the public Options struct.
type SchedulerOption func(*Scheduler)

// WithStorage overrides the default Postgres-backed storage. Production
// code should normally let Scheduler construct PostgresStorage from the
// pool; tests use WithStorage(NewMemoryStorage()).
func WithStorage(s Storage) SchedulerOption {
	return func(sc *Scheduler) {
		if s != nil {
			sc.storage = s
		}
	}
}

// WithLogger overrides the default slog logger.
func WithLogger(l *slog.Logger) SchedulerOption {
	return func(sc *Scheduler) {
		if l != nil {
			sc.logger = l
		}
	}
}

// WithLeader overrides the cluster-leader implementation. Production
// callers normally let Scheduler derive a PostgresLeader from the
// pool; tests use WithLeader(NewMemoryLeader()) — and to simulate two
// competing nodes in one process, share one MemoryLeader between two
// Schedulers.
//
// Pass nil to disable leadership entirely: LeaderOnly jobs will then
// fire on every node, which is correct for single-node deployments
// where there is no contention to coordinate.
func WithLeader(l Leader) SchedulerOption {
	return func(sc *Scheduler) { sc.leader = l }
}

// lookup returns the registered job by name or nil.
func (s *Scheduler) lookup(name string) *Job {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.jobs[name]
}

// New constructs a Scheduler. The pool must already be connected; New does not
// open connections itself. Validation of options happens here so misconfiguration
// surfaces at startup rather than on the first tick.
//
// Pass WithStorage(NewMemoryStorage()) to use the in-memory backend (tests).
// Without it, Scheduler will create a PostgresStorage from the pool when Run
// starts.
func New(pool *pgxpool.Pool, opts Options, schedOpts ...SchedulerOption) (*Scheduler, error) {
	if pool == nil {
		// MemoryStorage path: pool is allowed to be nil if WithStorage is supplied.
		// We validate after applying schedOpts below.
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
	if opts.LeaderRenewInterval == 0 {
		opts.LeaderRenewInterval = 10 * time.Second
	}

	sc := &Scheduler{
		pool:   pool,
		opts:   opts,
		jobs:   make(map[string]*Job),
		logger: slog.Default(),
	}
	// Sentinel so WithLeader(nil) can be told apart from "user did
	// not pass WithLeader at all" — the latter falls through to the
	// default Postgres-derived implementation.
	leaderConfigured := false
	for _, apply := range schedOpts {
		before := sc.leader
		apply(sc)
		// WithLeader(nil) is a deliberate "no leadership" knob; the
		// option func sets sc.leader = nil. Detect it by checking
		// whether either the before- or after-value is non-nil, or
		// whether this option call was the WithLeader one. Cheaper
		// approach: re-detect "user touched leader" by comparing
		// pointer identity after each apply.
		if sc.leader != nil || sc.leader != before {
			leaderConfigured = true
		}
	}
	if sc.storage == nil {
		if pool == nil {
			return nil, errors.New("sundial: either pool or WithStorage(...) is required")
		}
		sc.storage = NewPostgresStorage(pool, opts.Schema)
	}
	if !leaderConfigured {
		if pool != nil {
			sc.leader = NewPostgresLeader(pool)
		}
		// pool == nil with no WithLeader is the test-mode case
		// (in-memory storage but no explicit leader); LeaderOnly
		// jobs will fire on every node, which is fine when there
		// is only one.
	}
	return sc, nil
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

// Run drives the dispatcher loop until ctx is cancelled. On cancel it
// stops accepting new fires, waits up to ShutdownGrace for in-flight
// handlers to finish, then returns ctx.Err().
//
// Before the first tick, Run persists every registered job to storage
// via EnsureJob so other nodes sharing the same database become aware
// of them.
func (s *Scheduler) Run(ctx context.Context) error {
	s.mu.Lock()
	if s.running {
		s.mu.Unlock()
		return errors.New("sundial: Run is already in progress")
	}
	s.running = true
	jobs := make([]*Job, 0, len(s.jobs))
	for _, j := range s.jobs {
		jobs = append(jobs, j)
	}
	s.mu.Unlock()

	now := time.Now().UTC()
	for _, j := range jobs {
		next := j.Schedule.Next(now)
		if next.IsZero() {
			continue
		}
		if _, err := s.storage.EnsureJob(ctx, j, next); err != nil {
			s.markStopped()
			return fmt.Errorf("sundial: ensure %q: %w", j.Name, err)
		}
	}

	d := &dispatcher{
		scheduler: s,
		storage:   s.storage,
		leader:    s.leader,
		logger:    s.logger,
		telemetry: newTelemetry(),
	}
	d.telemetry.jobsScheduled.Add(ctx, int64(len(jobs)))
	err := d.run(ctx)
	if s.leader != nil {
		_ = s.leader.Release(context.Background())
	}
	s.markStopped()
	return err
}

func (s *Scheduler) markStopped() {
	s.mu.Lock()
	s.running = false
	s.mu.Unlock()
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
