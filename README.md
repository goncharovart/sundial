# Sundial

> Durable, distributed cron scheduler for Go. Postgres-backed. Leader election. OpenTelemetry-instrumented.

[![Go Reference](https://pkg.go.dev/badge/github.com/goncharovart/sundial.svg)](https://pkg.go.dev/github.com/goncharovart/sundial)
[![Go Report Card](https://goreportcard.com/badge/github.com/goncharovart/sundial)](https://goreportcard.com/report/github.com/goncharovart/sundial)
[![CI](https://github.com/goncharovart/sundial/actions/workflows/ci.yml/badge.svg)](https://github.com/goncharovart/sundial/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

> ⚠️ **Pre-release.** Dispatcher MVP and Postgres backend are in; API freezes at `v0.1.0`. Star/watch to follow development.

---

Sundial schedules and runs background jobs in Go, on top of a Postgres database you already have. It survives restarts, crashes, and network partitions — missed fires are recovered, locked jobs are recoverable by other nodes, and the system stays consistent without a separate queue daemon.

## Why Sundial

Existing Go schedulers stop short of what production needs:

| Capability                           | `robfig/cron` | `go-co-op/gocron` | `hibiken/asynq` | `riverqueue/river` | **Sundial** |
|--------------------------------------|:-------------:|:-----------------:|:---------------:|:------------------:|:-----------:|
| Cron syntax                          | ✅            | ✅                | ⚠️ helper       | ⚠️ helper          | ✅          |
| Persistent jobs (survive restart)    | ❌            | ❌ (open #533)    | ✅ (Redis)      | ✅ (Postgres)      | ✅ (Postgres) |
| Distributed / multi-node             | ❌            | ❌                | ✅              | ✅                 | ✅          |
| Leader election                      | ❌            | ❌                | n/a             | n/a                | ✅ (PG advisory locks) |
| Missed-fire recovery after downtime  | ❌            | ❌                | ⚠️              | ⚠️                 | ✅          |
| Schedule-first API (cron-style)      | ✅            | ✅                | ❌ (queue-first)| ❌ (queue-first)   | ✅          |
| Web UI                               | ❌            | ❌                | ✅              | ✅                 | 🚧 planned  |
| OpenTelemetry traces/metrics built-in| ❌            | ❌                | ⚠️ external     | ⚠️ external        | ✅          |

Sundial is for the case where you want **cron semantics with Postgres durability** and need to **scale beyond a single node** without bringing in Redis, Kafka, or Temporal.

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
- [ ] Missed-fire recovery policies (skip / run-once / run-all)
- [ ] Job retries with exponential backoff + jitter
- [ ] OpenTelemetry instrumentation (traces, metrics, slog correlation)
- [ ] Leader election sentinel lock for cluster-wide tasks
- [ ] Web UI

Open an issue or [start a discussion](https://github.com/goncharovart/sundial/discussions) — design feedback is especially welcome before the API stabilizes.

## Contributing

Contributors very welcome. See [CONTRIBUTING.md](CONTRIBUTING.md) (will be added with the first PR). For now: file an issue, propose an approach, then send a PR.

## License

MIT. See [LICENSE](LICENSE).
