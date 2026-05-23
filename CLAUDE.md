# CLAUDE.md — Sundial

> Project context for AI coding agents. Keep in sync with reality — single source of truth for conventions.

## What this is

`sundial` — **durable, distributed cron scheduler for Go**, built on top of a Postgres database you already have. Per-fire claim via `pg_try_advisory_xact_lock`, leader election via session-scoped advisory lock, missed-fire recovery (Skip / RunOnce / RunAll), retry with exponential backoff + jitter, OpenTelemetry instrumentation.

Niche: the middle between in-process `robfig/cron` (single-node, no durability) and Temporal (heavyweight workflow engine with own cluster). Queue-first solutions (`asynq`, `riverqueue/river`) require reshaping the model — Sundial keeps cron semantics with Postgres durability across N nodes.

## Stack

- **Go 1.25+** (channels, generics, `slices`, `cmp`, modern atomic types)
- **PostgreSQL 14+** (`pg_try_advisory_xact_lock`, `pg_try_advisory_lock`, FOR UPDATE SKIP LOCKED)
- **`pgx/v5`** — Postgres driver (no `lib/pq`, no `sqlx`, no ORM)
- **`robfig/cron/v3`** — only for the cron-expression parser; scheduling logic is Sundial's own
- **`go.opentelemetry.io/otel`** — traces, metrics, logs correlation
- Standard library for everything else (`net/http` for examples, `context`, `time`, `sync`)

## Project layout

```
sundial/
├── cron.go              # Cron parsing (robfig/cron/v3 wrapper)
├── dispatcher.go        # fetch → claim → execute → record loop
├── leader.go            # Postgres session-scoped advisory lock for cluster-wide leader
├── retry.go             # exponential backoff + jitter; dead-letter on exhaust
├── storage_pg.go        # PostgreSQL Storage implementation
├── storage_mem.go       # in-memory Storage for tests
├── instrument.go        # OpenTelemetry spans, metrics, lag/duration
├── auto_nodeid.go       # NodeID resolution chain (Hostname → $HOSTNAME → random hex)
├── *_test.go            # table-driven tests with -race
├── examples/            # runnable examples (in-memory by default, Postgres via env)
├── docs/                # design.md, recipes.md, demo-script.md, internal MAINTAINER_LOG (gitignored)
└── go.mod               # module github.com/goncharovart/sundial
```

No `cmd/`, no `pkg/`, no `internal/`. Sundial is a **library**, not an application — flat layout, `package sundial` at root.

## Build & test

```bash
# All tests with race + count + cover
go test -race -count=1 -cover ./...

# Single test by name
go test -race -run TestLeaderElection ./...

# Tests against real Postgres (testcontainers, slower)
SUNDIAL_PG_TEST=1 go test -race -count=1 ./...

# Vet + format
go vet ./...
gofmt -s -w .
goimports -w .  # if installed

# Module hygiene
go mod tidy
go mod verify
```

## Coding conventions

### Idiomatic Go

- **Accept interfaces, return structs.** `Storage`, `Leader`, `Clock` are interfaces; `PostgresStorage`, `InMemoryStorage`, `RealClock` are structs.
- **Channels for orchestration, mutexes for state.** Dispatcher loop reads from a `<-chan job`; counters and registries live behind `sync.RWMutex`.
- **Errors wrap context.** `fmt.Errorf("claim job %s: %w", jobID, err)` — never bare `return err` at API boundaries.
- **Context propagation everywhere.** Every dispatcher method, storage method, leader method accepts `ctx context.Context` first. Cancellation honored within ≤200ms (one tick).

### Postgres-specific

- **Always use `pg_try_advisory_xact_lock` (transaction-scoped) for per-fire claim**, not `pg_advisory_lock` — we don't want session-orphaned locks if a node dies mid-execution.
- **Use session-scoped `pg_try_advisory_lock` only for leader election** — we WANT it tied to session lifetime (node death → lock released → another node takes over).
- **Index `(next_fire_at, status)` with `WHERE status IN ('pending','retry')`** — partial index for dispatcher polling. Documented in `docs/design.md`.
- **`FOR UPDATE SKIP LOCKED` over advisory locks** when fetching candidate jobs — combined with advisory_lock on claim, it's the only safe pattern under concurrent dispatchers.

### Testing

- **Table-driven with subtests.** Name subtests by scenario: `t.Run("missed fire RunAll replays every instant inside window", ...)`, NOT `t.Run("test 1", ...)`.
- **`-race` clean.** All tests pass `-race -count=10` before commit.
- **Coverage target: 75%+** (currently ~62 unit tests). CI fails below 70%.
- **In-memory Storage for fast unit tests; testcontainers for integration.** Both implementations exercise the same `Storage` interface.

## Pre-commit hook (recommended)

```bash
#!/usr/bin/env bash
# .githooks/pre-commit
set -e
gofmt -s -w .
go vet ./...
go test -race -short -count=1 ./...
golangci-lint run --new-from-rev=HEAD~1 || true
```

Enable: `git config core.hooksPath .githooks`

## What's stable, what isn't (v0.1.x)

| Stable | Unstable (may change in v0.x minor) |
|---|---|
| `Scheduler.Add(cronExpr, handler)` API shape | Internal storage table schema |
| `Storage` interface methods | `instrument.go` metric names |
| `Leader` interface | Default retry backoff curve |
| `MissedFire{Skip,RunOnce,RunAll}` enum | Default leader heartbeat interval |

CHANGELOG records all breaking changes under "Changed" with migration notes.

## When in doubt

- Public API is unstable until v1.0.0 — breaking changes allowed in v0.x but **call out in CHANGELOG**.
- Don't add a dependency to save 30 lines. Stdlib first, then pgx/otel, never anything else without discussion.
- Tests describe behavior, not implementation. If a refactor without behavior change breaks a test, the test was over-specified.
- Idiomatic Go > clever Go. `for range N` over `for i := 0; i < N; i++`. `atomic.Int64` over `atomic.AddInt64`. `errors.Is`/`errors.As` over string-match.

## Related docs

- `README.md` — user-facing overview, comparison table, when-to-pick-what
- `README.ru.md` — Russian translation
- `docs/design.md` — internal architecture, claim-flow, advisory-lock rationale
- `docs/recipes.md` — 7 copy-pasteable usage patterns
- `docs/demo-script.md` — asciinema recording instructions for the README demo
- `CHANGELOG.md` — Keep-a-Changelog format; SemVer
- `CONTRIBUTING.md` — issue → discussion → PR workflow; 5 good-first-issues currently open
