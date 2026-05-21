# Sundial

> Durable, distributed cron scheduler for Go. Postgres-backed. Leader election. OpenTelemetry-instrumented.

[![Go Reference](https://pkg.go.dev/badge/github.com/goncharovart/sundial.svg)](https://pkg.go.dev/github.com/goncharovart/sundial)
[![Go Report Card](https://goreportcard.com/badge/github.com/goncharovart/sundial)](https://goreportcard.com/report/github.com/goncharovart/sundial)
[![CI](https://github.com/goncharovart/sundial/actions/workflows/ci.yml/badge.svg)](https://github.com/goncharovart/sundial/actions/workflows/ci.yml)
[![Release](https://img.shields.io/github/v/release/goncharovart/sundial?sort=semver&display_name=tag&color=blue)](https://github.com/goncharovart/sundial/releases)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

> ⚠️ **Pre-release.** Dispatcher MVP and Postgres backend are in; API freezes at `v0.1.0`. Star/watch to follow development.

---

Sundial schedules and runs background jobs in Go, on top of a Postgres database you already have. It survives restarts, crashes, and network partitions — missed fires are recovered, locked jobs are recoverable by other nodes, and the system stays consistent without a separate queue daemon.

## Why Sundial

The Go ecosystem has good options at both ends of the scheduling
spectrum — `robfig/cron` and `gocron` are single-node in-process, and
Temporal is a full distributed workflow engine. The middle is what's
missing: cron semantics with Postgres durability that scales beyond a
single node, without bringing in Redis, Kafka, or a separate workflow
cluster.

| Capability                            | `robfig/cron` | `gocron` | `asynq` | `riverqueue/river` | **Sundial** |
|---------------------------------------|:-------------:|:--------:|:-------:|:------------------:|:-----------:|
| Cron syntax (`0 3 * * *`)             | ✅            | ✅       | ⚠️ helper | ⚠️ helper        | ✅          |
| Survives restart (persistent state)   | ❌            | ❌ ([#533][gocron533]) | ✅ (Redis) | ✅ (Postgres) | ✅ (Postgres) |
| Distributed across multiple nodes     | ❌            | ❌       | ✅       | ✅                 | ✅          |
| Schedule-first API (you write "@hourly", not "enqueue X") | ✅ | ✅ | ❌ queue-first | ❌ queue-first | ✅ |
| Leader election for cluster chores    | ❌            | ❌       | n/a     | n/a                | ✅ (PG advisory lock) |
| Missed-fire recovery after downtime   | ❌            | ❌       | ⚠️ partial | ⚠️ partial      | ✅ Skip / RunOnce |
| Exponential-backoff retry + jitter    | ❌            | ❌       | ✅       | ✅                 | ✅          |
| Dead-letter outcome on exhaustion     | ❌            | ❌       | ✅       | ✅                 | ✅          |
| OpenTelemetry traces + metrics out of the box | ❌    | ❌       | ⚠️ external | ⚠️ external    | ✅          |
| External dependencies beyond Postgres | none          | none     | Redis   | none               | none        |

[gocron533]: https://github.com/go-co-op/gocron/issues/533

### When to pick what

- **Single-node cron job inside your binary** → `robfig/cron`. Smallest surface.
- **A pile of asynchronous tasks fanned out to workers** → `asynq` (Redis) or `riverqueue/river` (Postgres). Both are *queue-first*; you enqueue work, the system decides when to run it.
- **Cron jobs that must run exactly once across N nodes, survive restart, and report SLO-grade metrics** → Sundial. That's the niche.

If you need long-running, multi-step, retry-with-compensation workflows
(saga, business processes with timeouts and waits) → Temporal /
Cadence / DBOS — Sundial is intentionally not that.

## Quickstart

```go
import (
    "context"
    "github.com/goncharovart/sundial"
    "github.com/jackc/pgx/v5/pgxpool"
)

pool, _ := pgxpool.New(ctx, os.Getenv("DATABASE_URL"))
s, _ := sundial.New(pool, sundial.Options{NodeID: "worker-1"})

s.Schedule("nightly-cleanup", "0 3 * * *", func(ctx context.Context) error {
    return purgeOldRecords(ctx)
})

s.Run(ctx) // blocks until ctx is cancelled
```

That's a fully distributed-safe scheduler: spin up multiple processes with the same code, and exactly one of them will run each job at each fire time.

## Install

```bash
go get github.com/goncharovart/sundial@latest
```

Requirements: **Go 1.25+**, **PostgreSQL 14+**.

## How it works (brief)

- **Job table** in Postgres stores definitions, next-fire times, last-run info.
- A **dispatcher loop** in every node polls due jobs and races to claim them via Postgres advisory locks.
- **Leader election** decides which node owns missed-fire recovery and scheduler-wide coordination (also via advisory locks — no etcd, no Consul).
- **OpenTelemetry** spans cover scheduling, claiming, and job execution; metrics include `sundial.jobs.scheduled`, `sundial.jobs.running`, `sundial.jobs.lag_seconds`.

Detailed design: [docs/design.md](docs/design.md).

## Status & roadmap

The MVP is in. The roadmap to `v0.1.0`:

- [x] Project scaffold, CI, license
- [x] Schedule API — Cron, Every, At
- [x] Storage layer — Postgres (`pg_try_advisory_xact_lock`) and an
      in-memory implementation for tests
- [x] Dispatcher loop with fetch → claim → execute → record
- [x] Graceful shutdown that drains in-flight handlers
- [x] Panic recovery so a single bad handler can't take the loop down
- [x] Runnable example (in-memory by default, Postgres via env)
- [x] Missed-fire recovery policies — Skip, RunOnce, **RunAll** iterator
- [x] OpenTelemetry instrumentation (traces, metrics, lag/duration)
- [x] Leader election via Postgres session-scoped advisory lock
- [x] Job retries with exponential backoff + jitter; dead-letter on exhaust
- [ ] testcontainers-go integration tests against real Postgres (#2)
- [ ] Web UI

Open an issue or [start a discussion](https://github.com/goncharovart/sundial/discussions) — design feedback is especially welcome before the API stabilizes.

## Contributing

Contributors very welcome. See [CONTRIBUTING.md](CONTRIBUTING.md) (will be added with the first PR). For now: file an issue, propose an approach, then send a PR.

## License

MIT. See [LICENSE](LICENSE).
