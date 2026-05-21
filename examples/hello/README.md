# examples/hello

A small program that exercises the Sundial public API end-to-end:
registers three jobs with different schedule kinds, drives the
dispatcher loop, prints each fire, and exits cleanly on `Ctrl-C`.

It works two ways:

- **In-memory backend** — no setup. Just `go run ./examples/hello`.
- **Postgres backend** — set `DATABASE_URL` first.

## Run (in-memory)

```bash
go run ./examples/hello
```

Expected output (interleaved with the dispatcher's structured logs):

```
  registered: report-every-12s     12s          (kind=every)
  registered: health-probe-5s      5s           (kind=every)
  registered: send-launch-email    2026-...     (kind=at)
INFO using in-memory backend (set DATABASE_URL for Postgres)
INFO scheduler running — Ctrl-C to exit cleanly
INFO health probe total=1
INFO job done job=health-probe-5s duration=...
INFO health probe total=2
INFO report fired total=1
...
```

## Run (Postgres)

```bash
docker run -d --name sundial-pg -p 5432:5432 \
  -e POSTGRES_USER=sundial -e POSTGRES_PASSWORD=sundial -e POSTGRES_DB=sundial \
  postgres:16-alpine

# Apply schema once
psql "$DATABASE_URL" -f ../../migrations/0001_initial.up.sql

export DATABASE_URL=postgres://sundial:sundial@localhost:5432/sundial?sslmode=disable
go run ./examples/hello
```

`Ctrl-C` drains in-flight handlers up to ShutdownGrace and returns.
