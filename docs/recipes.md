# Recipes

Common Sundial patterns. Each recipe is a short paragraph that
answers "why does this exist" and a copy-pasteable code block.

The patterns are intentionally tiny. They are not the full picture
for a production deployment — they are the starting point.

---

## 1. Run a job once at a future time and never again

`At(...)` schedules a one-shot fire. Once it has fired, the
schedule's `Next()` returns the zero time and the dispatcher stops
emitting work for it.

```go
package main

import (
    "context"
    "log/slog"
    "time"

    "github.com/jackc/pgx/v5/pgxpool"
    "github.com/goncharovart/sundial"
)

func main() {
    pool, _ := pgxpool.New(context.Background(), os.Getenv("DATABASE_URL"))
    defer pool.Close()

    s, _ := sundial.New(pool, sundial.Options{
        NodeID:       sundial.AutoNodeID(),
        TickInterval: time.Second,
    })

    fireAt := time.Date(2026, 12, 31, 23, 59, 0, 0, time.UTC)
    _, _ = s.Schedule("yearly-roundup", sundial.At(fireAt),
        func(ctx context.Context) error {
            slog.Info("year-end snapshot taken")
            return nil
        },
    )

    _ = s.Run(context.Background())
}
```

**Gotcha:** if your process is down at `fireAt` and stays down past
the firing window, whether the job ever runs is controlled by the
`MissedFire` policy — see recipe #3.

---

## 2. Every minute, but only on the cluster leader

When you have a fleet of N processes sharing one Sundial database,
the per-fire `ClaimJob` already guarantees that only one node runs a
given fire. But "register the same job on every node" still produces
N copies of the `Schedule` row contention on every tick, which is
wasteful for jobs that you *know* should be cluster-wide
singletons (cache warmer, daily report, vacuum).

`WithLeaderOnly()` short-circuits these jobs on follower nodes
before the claim transaction, so only the node holding the
cluster-leader advisory lock even tries.

```go
_, _ = s.Schedule("vacuum-stale-rows", sundial.Cron("0 3 * * *"),
    func(ctx context.Context) error {
        _, err := pool.Exec(ctx, `DELETE FROM events WHERE created_at < now() - interval '90 days'`)
        return err
    },
    sundial.WithLeaderOnly(),
)
```

**Gotcha:** if your `Options.NodeID` is the same string on two
processes (e.g. you forgot to set it and `os.Hostname()` returned
"localhost" on both), the leader lock semantics still work — but
your dashboards will look like one node bouncing between
configurations. Use `sundial.AutoNodeID()`.

---

## 3. Catch-up after a long outage

If your cluster was down across one or more fire times, the
dispatcher classifies those jobs as "missed" on the next tick. What
happens next is controlled per-job:

```go
// Audit job that MUST execute one fire per missed instant.
_, _ = s.Schedule("hourly-audit", sundial.Cron("0 * * * *"),
    auditHandler,
    sundial.WithMissedFire(sundial.MissedFireRunAll),
)

// Notification job — fire once at most after recovery, then forget.
_, _ = s.Schedule("morning-digest", sundial.Cron("0 8 * * *"),
    digestHandler,
    sundial.WithMissedFire(sundial.MissedFireRunOnce),
)

// Health-ping that doesn't matter if missed.
_, _ = s.Schedule("ping-ok", sundial.Every(time.Minute),
    pingHandler,
    sundial.WithMissedFire(sundial.MissedFireSkip),
)
```

**Gotcha 1:** `RunAll` is leader-only by design — replaying a 24-hour
window from N nodes simultaneously would multiply the catch-up
volume by N. The leader replays; followers wait.

**Gotcha 2:** `RunAll` is capped at 256 instants per tick. If you
have a job that fires every second and the cluster was down for an
hour (3600 missed instants), the iterator will replay 256 on the
first tick, then the next 256 on the second tick, and so on. This is
a defensive ceiling, not a feature — if you are doing high-frequency
work that *must* never miss a fire, persist the catch-up state
elsewhere (a `processed_until` column on your domain table is
usually the right answer).

---

## 4. Per-job retry that doesn't hammer a flaky upstream

When a handler fails, Sundial advances `next_fire` to `now + backoff`
and re-claims the same fire on a later tick. Backoff is exponential
with jitter — the jitter is the part that matters most. Without it,
a thundering herd whose handlers all fail at the same instant would
all retry in lockstep and hammer the same upstream over and over.

```go
_, _ = s.Schedule("call-shaky-api", sundial.Every(5*time.Minute),
    apiHandler,
    sundial.WithRetry(sundial.RetryPolicy{
        MaxAttempts:    5,
        InitialBackoff: 1 * time.Second,
        MaxBackoff:     60 * time.Second,
        Multiplier:     2.0,
    }),
)
```

The handler sees a `RunFailed` for each attempt, then a `RunDeadLetter`
outcome on the final exhausted attempt — both visible in `sundial_runs`
and emitted as OpenTelemetry spans.

**Gotcha:** the attempt counter lives in-memory on the dispatcher.
A process crash mid-retry resets it to 1. This is acceptable for the
v0.1 surface because the worst case is one extra duplicate attempt
after a crash; if you need durable attempt counting, persist the
attempt number from the handler itself.

---

## 5. Two cooperating jobs that fan out work

A common pattern: a "producer" job runs every minute, finds N
ready-to-process items, and enqueues per-item work somewhere
(Kafka, NATS, a `work_items` table). A "consumer" job runs on a
faster cadence, drains the queue, and re-enqueues failures with the
producer.

```go
// Producer: scan + enqueue.
_, _ = s.Schedule("scan-pending", sundial.Every(time.Minute),
    func(ctx context.Context) error {
        rows, _ := pool.Query(ctx, `SELECT id FROM orders WHERE state = 'pending' LIMIT 100`)
        defer rows.Close()
        for rows.Next() {
            var id int64
            _ = rows.Scan(&id)
            // enqueue id ...
        }
        return rows.Err()
    },
    sundial.WithLeaderOnly(), // only one producer in the cluster
)

// Consumer: drain queue. Multiple processes can run this concurrently;
// the work_items.locked_at lease is what serialises per-item handling.
_, _ = s.Schedule("drain-queue", sundial.Every(5*time.Second),
    drainHandler,
)
```

**Gotcha:** Sundial does *not* try to be a job queue. The producer
runs the scan; the queue mechanics (Kafka, `SELECT ... FOR UPDATE
SKIP LOCKED`, NATS JetStream, etc.) live in your code. If you want
a Postgres-native queue, look at [River](https://riverqueue.com/) —
it solves the queue problem while Sundial solves the schedule problem.

---

## 6. Wiring OpenTelemetry — minimum config to see lag in Jaeger

Sundial emits spans and metrics through whatever global
TracerProvider / MeterProvider the host process has configured. The
fastest path to a usable trace stream is the OTLP gRPC exporter
pointing at a local Jaeger or Grafana Tempo:

```go
package main

import (
    "context"
    "time"

    "go.opentelemetry.io/otel"
    "go.opentelemetry.io/otel/exporters/otlp/otlptrace"
    "go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
    "go.opentelemetry.io/otel/sdk/resource"
    sdktrace "go.opentelemetry.io/otel/sdk/trace"
    semconv "go.opentelemetry.io/otel/semconv/v1.26.0"

    "github.com/goncharovart/sundial"
)

func main() {
    ctx := context.Background()

    exp, _ := otlptrace.New(ctx,
        otlptracegrpc.NewClient(otlptracegrpc.WithEndpoint("localhost:4317"),
                                 otlptracegrpc.WithInsecure()),
    )
    tp := sdktrace.NewTracerProvider(
        sdktrace.WithBatcher(exp),
        sdktrace.WithResource(resource.NewWithAttributes(
            semconv.SchemaURL,
            semconv.ServiceName("my-service"),
        )),
    )
    otel.SetTracerProvider(tp)
    defer tp.Shutdown(ctx)

    // ...build scheduler and Run as usual.
}
```

In Jaeger you should now see one `sundial.job.run` span per fire,
attributed with `sundial.job.name`, `sundial.job.id`, `sundial.node.id`,
and `sundial.fire_time`. Filter by `sundial.lag_seconds > 1` to find
fires that drifted past their scheduled time.

**Gotcha:** the metric histograms include `sundial.jobs.lag_seconds`
which is `started_at - fire_time`. A growing lag with a flat
`sundial.jobs.running` is the classic "one node fell behind"
signature — easier to spot in Grafana than in any single log line.

---

## 7. Testing your handlers without a real database

`MemoryStorage` is a first-class implementation of `Storage` for
tests. It supports every operation `PostgresStorage` does:

```go
func TestMyHandler(t *testing.T) {
    mem := sundial.NewMemoryStorage()
    s, _ := sundial.New(nil, sundial.Options{
        NodeID:       "test-node",
        TickInterval: 100 * time.Millisecond,
        ShutdownGrace: 200 * time.Millisecond,
    }, sundial.WithStorage(mem))

    var ran bool
    every, _ := sundial.Every(time.Second)
    _, _ = s.Schedule("my-handler", every,
        func(ctx context.Context) error {
            ran = true
            return nil
        },
    )

    // Optionally seed an immediate fire so the dispatcher doesn't
    // wait one cadence on the first tick:
    job := s.Jobs()[0]
    _, _ = mem.EnsureJob(context.Background(), job, time.Now().Add(-time.Second))

    ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
    defer cancel()
    _ = s.Run(ctx)

    if !ran {
        t.Fatal("handler never executed")
    }
}
```

`MemoryStorage` is also useful for running Sundial in a single-process
deployment where you don't actually need a database — the same code
that runs cron in your production cluster runs the same way against
the in-memory backend in your laptop devshell.

---

## Where to go next

- [README](../README.md) — overview, quickstart, status
- [design.md](design.md) — internals: claim semantics, advisory keys, why strict `<`
- [CHANGELOG.md](../CHANGELOG.md) — what shipped in which release
- [Issues labelled `good first issue`](https://github.com/goncharovart/sundial/issues?q=is%3Aissue+is%3Aopen+label%3A%22good+first+issue%22) — pickable contributions
