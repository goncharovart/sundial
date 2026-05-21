# Contributing to Sundial

Thanks for considering a contribution — small fixes, missing tests, and
new schedule kinds are all welcome. This document covers the
practicalities; the broader design lives in [docs/design.md](docs/design.md).

## Quick orientation

The codebase is small and intentionally flat:

| File | Purpose |
|------|---------|
| `schedule.go` / `cron.go` | The `Schedule` interface and three implementations |
| `job.go` | `Job`, `JobOptions`, `RetryPolicy`, `MissedFirePolicy` |
| `sundial.go` | `Scheduler` public API + `New` + `Run` |
| `storage.go` | `Storage` interface + `PostgresStorage` + `MemoryStorage` |
| `dispatcher.go` | Tick → fetch → claim → execute → record loop |
| `leader.go` | Cluster-leader election via session-scoped advisory lock |
| `telemetry.go` | OpenTelemetry traces + metrics |
| `migrations/` | Postgres schema |

Tests live next to the file they exercise (`*_test.go`).

## Before you open a PR

1. **Open an issue first** for anything beyond a typo or a doc clarification.
   Coordinating early keeps PRs short and avoids parallel work.
2. **One change per PR.** A bug fix and a refactor in the same PR makes
   review three times longer than it needs to be.
3. **Tests.** Every new behaviour needs a test. The bar is "if the test
   were deleted, the next refactor would break the behaviour silently."
4. **`go fmt`, `go vet`, `golangci-lint run`.** CI runs all three.
5. **Commit message:** imperative present tense, ≤72-char subject,
   wrapped body explaining *why*. Match the style of `git log`.

## Running tests locally

```bash
go test ./...
```

The full suite runs without Postgres — `MemoryStorage` covers the
dispatcher, missed-fire, leader, and retry paths. Integration tests
against a real Postgres land under a build tag in a future commit.

## Schema changes

If you touch the schema, add a new migration file in `migrations/`
(`NNNN_description.up.sql` and `NNNN_description.down.sql`). Never
modify an existing migration — that breaks deploys.

## What we look for in a PR

- **Small surface change.** Public-API breaks need a clear motivation.
- **Comment the *why*, not the *what*.** The `Storage.ClaimJob` doc
  comment is a good model — it explains *why* strict `<` rather than `<=`.
- **Errors are values.** Wrap with `%w`, surface a sentinel when the
  caller might want `errors.Is`.
- **No `interface{}` / `any` in hot paths** unless you've checked
  escape analysis (`go build -gcflags=-m`).
- **Tests describe behaviour**, not implementation: a name like
  `TestClaimJob_RejectsRetryWithSameFireTime` is better than
  `TestClaimJob_2`.

## Reporting bugs

A reproducible test case is the most valuable thing you can attach.
Failing that, include:
- Postgres version (`SELECT version();`)
- Go version (`go version`)
- A short snippet of the call site (Scheduler config + Schedule registration)
- What you expected vs what happened

## License

By contributing you agree your work is released under the project's
[MIT license](LICENSE).
