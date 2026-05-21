# Changelog

All notable changes to Sundial are tracked here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this
project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

The public API is considered unstable until **v1.0.0** — breaking
changes between minor versions in the `v0.x` series are allowed but
will be called out under "Changed".

## [Unreleased]

### Added

- **`MissedFireRunAll` iterator** (closes [#1](https://github.com/goncharovart/sundial/issues/1)):
  when a job's last fire is far in the past and the policy is `RunAll`,
  the dispatcher now emits every missed instant strictly inside
  `(last_fire, now)` in order via a new `iterateSchedule` helper.
  Iteration is leader-only (so a multi-node cluster does not multiply
  catch-up volume) and capped at 256 instants per tick (to defend
  against pathological "down for a month at 1s cadence" scenarios).
  Each replayed instant goes through the regular claim → execute →
  record pipeline, so spans, run records, and retry budget all apply
  uniformly to catch-up fires and live fires.

## [v0.1.0] — 2026-05-21

The first tagged release. The dispatcher loop, Postgres backend,
OpenTelemetry instrumentation, leader election, and retry-with-backoff
all land together so the v0.1.0 surface is "everything needed to run
distributed cron in production *except* the missed-fire `RunAll`
iterator and a Web UI."

### Added

- **Schedule API** — `Cron("0 3 * * *")` (5-field, plus `@hourly` /
  `@daily` / `@weekly` / `@monthly` / `@yearly`), `Every(time.Minute)`,
  `At(time.Time)`. Sub-second intervals are rejected; cron seconds are
  intentionally excluded to keep the parser predictable.
- **`Scheduler`** — single entry point per process. `New(pool, opts,
  ...SchedulerOption)` validates options at startup; the dispatcher
  starts on `Run(ctx)` and drains in-flight handlers on cancel up to
  `ShutdownGrace`.
- **`Storage` interface** with two implementations:
  - `PostgresStorage` — production backend. Per-job claim runs inside
    a transaction holding a `pg_try_advisory_xact_lock` keyed on a
    deterministic int64 derived from the job UUID, with `< next_fire`
    semantics on the UPDATE that guarantees only one node advances
    a given fire.
  - `MemoryStorage` — in-process implementation used by tests and by
    consumers running single-node without a database.
- **Dispatcher loop** — tick → fetch due jobs → claim → execute with
  per-job timeout → record outcome. Panic in a handler is recovered
  into a `RunFailed` outcome so a single misbehaving job cannot bring
  the loop down.
- **Missed-fire policies** — `MissedFireSkip` (record the miss, jump
  to the next future fire) and `MissedFireRunOnce` (execute one
  catch-up fire then advance). `MissedFireRunAll` is declared in the
  API surface but degrades to `RunOnce` until the dedicated iterator
  lands (issue [#1](https://github.com/goncharovart/sundial/issues/1)).
- **OpenTelemetry** — six instruments (`sundial.jobs.scheduled`,
  `jobs.running`, `jobs.duration_seconds`, `jobs.lag_seconds`,
  `jobs.failures_total`, `jobs.claimed_total`) plus a
  `sundial.job.run` span per attempt. Pulls from the global
  Tracer/Meter providers so callers integrate Sundial with whichever
  exporter they already ship.
- **Leader election** — `PostgresLeader` holds a session-scoped
  advisory lock on a dedicated sentinel key (`SundialL`); when the
  leader's connection closes, Postgres releases the lock
  automatically and the next renewal tick on another node acquires
  it. `LeaderOnly` jobs short-circuit on follower nodes before the
  claim transaction.
- **Retry with backoff** — per-job `RetryPolicy{MaxAttempts,
  InitialBackoff, MaxBackoff, Multiplier}`. On a failed attempt the
  dispatcher pulls `next_fire` back to `now + backoff` (exponential
  growth × jitter in `[0.5, 1.5)` — the jitter is what protects
  against thundering-herd retries on a shared upstream). On
  exhaustion the outcome flips to `RunDeadLetter`.
- **Runnable example** — `examples/hello` registers three jobs with
  different schedule kinds and runs the real dispatcher. In-memory
  backend by default; `DATABASE_URL` switches to Postgres.
- **Migrations** — `migrations/0001_initial.up.sql` provisions
  `sundial_jobs` and `sundial_runs` tables with the necessary
  indexes.

### Test surface

54 unit tests, no Postgres required. Integration tests against a real
database land under a build tag in [#2](https://github.com/goncharovart/sundial/issues/2).

### Not yet

- `MissedFireRunAll` iterator (issue #1)
- testcontainers-go integration tests (issue #2)
- Web UI
- Multi-tenant `tenant_id` isolation (deferred to v0.2)
