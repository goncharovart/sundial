# Sundial — Design Document

Status: **draft**, pre-`v0.1.0`. Subject to change. This document captures the design intent before code, so the public API can be discussed openly.

---

## Problem

Go has solid options at the two ends of the scheduling spectrum:

- **In-process schedulers** (`robfig/cron`, `go-co-op/gocron`) — single-node, in-memory. Restart loses scheduled state. Multi-node deployments fire each job multiple times.
- **Distributed workflow engines** (Temporal, Cadence) — durable, distributed, but heavy: dedicated cluster, complex SDK, mental shift from "cron" to "workflow."

There is a gap in the middle: a library that scales beyond one node, survives restarts, and stays simple to operate on top of a Postgres instance a service already has. `riverqueue/river` covers the queue-first case very well; Sundial covers the **schedule-first** case (cron expressions, predictable fire times, idempotent recurring tasks).

## Goals

1. **Cron semantics first.** Schedule by cron expression (`"0 3 * * *"`) or interval (`5*time.Minute`). Job authors think in fire times, not queue depth.
2. **Postgres-only persistence.** No Redis, no Kafka, no etcd. Reuse the database already in the stack.
3. **Distributed-safe by default.** Multiple nodes running the same code → each scheduled fire executes exactly once.
4. **Survives downtime.** Missed fires during outages are recoverable according to a per-job policy (skip, run-once-catch-up, run-all).
5. **Observable out of the box.** OpenTelemetry spans on every step; standard metrics for lag, queue size, run duration.
6. **Small surface, idiomatic Go.** A handful of types, generics for type-safe job payloads, context propagation everywhere.

## Non-goals

- Replacing Temporal / Cadence. Sundial does not implement workflows, sagas, or activity replay.
- Job queue features (priorities, fan-out, dynamic concurrency per queue). `riverqueue/river` and `hibiken/asynq` already do this well.
- Cross-database support in `v1`. Future versions may add backends (SQLite for embedded use, MySQL on demand), but PG is the reference implementation.

## Core concepts

### Job

A unit of recurring work, identified by a unique name within a Sundial instance.

```go
type Job[T any] struct {
    Name     string
    Schedule Schedule         // cron expression or interval
    Handler  func(ctx context.Context, payload T) error
    Options  JobOptions
}
```

`JobOptions` covers retry policy, timeout, missed-fire policy, leader-only flag.

### Schedule

```go
type Schedule interface {
    Next(after time.Time) time.Time
}
```

Implementations: `Cron(expr string)`, `Every(d time.Duration)`, `At(t time.Time)` (one-shot).

### Scheduler

The runtime. One instance per process. Holds the Postgres pool, the registered jobs, the dispatcher loop.

```go
type Scheduler struct { /* ... */ }

func New(pool *pgxpool.Pool, opts Options) (*Scheduler, error)
func (s *Scheduler) Schedule(name, expr string, fn HandlerFunc, opts ...JobOption) error
func (s *Scheduler) Run(ctx context.Context) error
```

`Run` blocks until `ctx` is cancelled; on cancel it stops accepting new fires, drains in-flight handlers up to a grace period, and returns.

## Concurrency model

Every node runs the same dispatcher loop:

1. **Tick** (default 1s). Query Postgres for jobs with `next_fire_time <= now()`.
2. **Claim.** For each due job, attempt `pg_try_advisory_xact_lock(job_id_hash)` inside a transaction. Lock holders own the fire; others move on.
3. **Execute.** Run the handler with a context derived from the scheduler's root context. Timeout per `JobOptions.Timeout`.
4. **Record outcome.** On success, advance `next_fire_time` to the schedule's next tick. On failure, apply retry policy (record attempt count, set retry-at, or move to dead-letter).
5. **Release.** Transaction commits; advisory lock auto-released.

Leader election (for cluster-wide tasks like missed-fire recovery, vacuum of completed runs) uses a separate, long-held advisory lock on a sentinel key. The current leader publishes a heartbeat every N seconds; followers attempt to acquire on heartbeat timeout.

## Missed-fire recovery

When a node was down across a fire window, the next leader detects it on startup or on the next leader-bound tick:

- For each job, compute the set of fires between `last_run_at` (or `created_at`) and `now()` according to its schedule.
- Apply `JobOptions.MissedFirePolicy`:
  - **Skip** — record fires as missed, do not run.
  - **RunOnce** — run a single catch-up fire (default).
  - **RunAll** — run every missed fire in order (for accounting/audit jobs that must execute all fires).

Recovery is leader-only to avoid duplicate catch-ups.

## Observability

Every job execution is wrapped in a span `sundial.job.run` with attributes:
`job.name`, `job.schedule`, `fire.time`, `node.id`, `attempt`, `status`.

Metrics (OpenTelemetry, `meter:"sundial"`):
- `sundial.jobs.scheduled` (gauge) — currently registered jobs.
- `sundial.jobs.running` (gauge) — currently executing.
- `sundial.jobs.lag_seconds` (histogram) — observed lag between `next_fire_time` and actual start.
- `sundial.jobs.duration_seconds` (histogram).
- `sundial.jobs.failures_total` (counter).

Logs follow Go 1.21+ `slog`. A `slog.Handler` bridge is provided for correlation with the active span (uses `otelslog`).

## Schema

```sql
CREATE TABLE sundial_jobs (
    id              uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    name            text UNIQUE NOT NULL,
    schedule        text NOT NULL,
    schedule_kind   text NOT NULL,  -- 'cron' | 'every' | 'at'
    next_fire_time  timestamptz NOT NULL,
    last_run_at     timestamptz,
    last_status     text,
    payload         jsonb,
    options         jsonb,
    created_at      timestamptz NOT NULL DEFAULT now(),
    updated_at      timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX sundial_jobs_due_idx ON sundial_jobs (next_fire_time);

CREATE TABLE sundial_runs (
    id          uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    job_id      uuid NOT NULL REFERENCES sundial_jobs(id) ON DELETE CASCADE,
    fire_time   timestamptz NOT NULL,
    started_at  timestamptz NOT NULL,
    finished_at timestamptz,
    attempt     int NOT NULL DEFAULT 1,
    status      text NOT NULL,  -- 'running' | 'success' | 'failed' | 'dead-letter'
    error       text,
    node_id     text NOT NULL
);

CREATE INDEX sundial_runs_job_id_idx ON sundial_runs (job_id, fire_time DESC);
```

Migrations live in `migrations/`. Sundial does not auto-migrate by default; the operator runs `sundial migrate up` (or imports the embedded SQL into their own migration tool).

## Open questions

- **API for typed payloads.** Generics work cleanly for handlers, less cleanly when payloads must round-trip JSON through the DB. Sketch in progress.
- **Web UI delivery model.** Embed Templ/HTMX assets in a sidecar binary `sundial-ui`, or expose a JSON read-API and ship a separate UI repo? Likely the latter (smaller blast radius for the core library).
- **Multi-tenant isolation.** `tenant_id` column with index, or rely on schema-per-tenant? Defer to `v0.2`.

## References

- Postgres advisory locks: <https://www.postgresql.org/docs/current/explicit-locking.html#ADVISORY-LOCKS>
- `riverqueue/river` architecture: <https://riverqueue.com/docs/architecture>
- OpenTelemetry semantic conventions for messaging: <https://opentelemetry.io/docs/specs/semconv/messaging/>
- Cron expression handling: <https://github.com/robfig/cron>
