# examples/hello

A 90-line program that exercises the public Sundial API to register three
jobs with different schedule kinds and then waits for `Ctrl-C` to verify
graceful shutdown.

This example is intentionally tiny and dispatcher-free — it focuses on the
API surface (`Cron`, `Every`, `At`, `Scheduler.Schedule`, `WithLeaderOnly`,
`WithMissedFire`) that downstream callers will see in `v0.1.0`. The actual
dispatcher loop arrives in a follow-up commit; once it lands, this example
gets a few more lines that show the runtime in action.

## Run

```bash
docker run -d --name sundial-pg -p 5432:5432 \
  -e POSTGRES_USER=sundial -e POSTGRES_PASSWORD=sundial -e POSTGRES_DB=sundial \
  postgres:16-alpine

export DATABASE_URL=postgres://sundial:sundial@localhost:5432/sundial?sslmode=disable
go run ./examples/hello
```

Expected output:

```
registered: report-hourly         0 * * * *  (kind=cron)
registered: health-probe          10s        (kind=every)
registered: send-launch-email     2026-...   (kind=at)
INFO scheduler ready (dispatcher loop arrives in a follow-up commit) — Ctrl-C to exit
```

`Ctrl-C` exits cleanly.
