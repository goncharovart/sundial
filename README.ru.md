# Sundial

> Распределённый cron-планировщик задач для Go. PostgreSQL для durability и leader election.

[![Go Reference](https://pkg.go.dev/badge/github.com/goncharovart/sundial.svg)](https://pkg.go.dev/github.com/goncharovart/sundial)
[![Go Report Card](https://goreportcard.com/badge/github.com/goncharovart/sundial)](https://goreportcard.com/report/github.com/goncharovart/sundial)
[![CI](https://github.com/goncharovart/sundial/actions/workflows/ci.yml/badge.svg)](https://github.com/goncharovart/sundial/actions/workflows/ci.yml)
[![codecov](https://codecov.io/gh/goncharovart/sundial/branch/main/graph/badge.svg)](https://codecov.io/gh/goncharovart/sundial)
[![Release](https://img.shields.io/github/v/release/goncharovart/sundial?sort=semver&display_name=tag&color=blue)](https://github.com/goncharovart/sundial/releases)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)
[![README (English)](https://img.shields.io/badge/README-English-blue.svg)](README.md)

> ⚠️ **Pre-release.** Dispatcher MVP и Postgres-бэкенд landed; публичный API стабилизируется в `v0.1.0`. Поставьте star/watch, чтобы следить за развитием.

---

## Зачем нужен Sundial

В Go-экосистеме есть хорошие варианты по двум краям спектра планирования:
**`robfig/cron`** и **`gocron`** — single-node, in-process; **Temporal** — полноценный
распределённый workflow-движок. Чего **не хватает в середине** — это и есть ниша
Sundial: cron-семантика с Postgres-durability, которая масштабируется за пределы
одной ноды, без необходимости приносить Redis, Kafka или отдельный workflow-кластер.

| Возможность                              | `robfig/cron` | `gocron` | `asynq` | `riverqueue/river` | **Sundial** |
|------------------------------------------|:-------------:|:--------:|:-------:|:------------------:|:-----------:|
| Cron-синтаксис (`0 3 * * *`)             | ✅            | ✅       | ⚠️ helper | ⚠️ helper        | ✅          |
| Переживает рестарт (persistent state)    | ❌            | ❌ ([#533][gocron533]) | ✅ (Redis) | ✅ (Postgres) | ✅ (Postgres) |
| Распределён по нескольким нодам          | ❌            | ❌       | ✅       | ✅                 | ✅          |
| Schedule-first API (`@hourly`, не `Enqueue X`) | ✅      | ✅       | ❌ queue-first | ❌ queue-first | ✅ |
| Leader election для cluster-wide задач   | ❌            | ❌       | n/a     | n/a                | ✅ (PG advisory lock) |
| Восстановление после простоя             | ❌            | ❌       | ⚠️ partial | ⚠️ partial      | ✅ Skip / RunOnce / RunAll |
| Retry с экспоненциальным backoff + jitter| ❌            | ❌       | ✅       | ✅                 | ✅          |
| Dead-letter outcome при исчерпании       | ❌            | ❌       | ✅       | ✅                 | ✅          |
| OpenTelemetry traces + metrics из коробки| ❌            | ❌       | ⚠️ external | ⚠️ external    | ✅          |
| Внешние зависимости кроме Postgres       | none          | none     | Redis   | none               | none        |

[gocron533]: https://github.com/go-co-op/gocron/issues/533

### Когда что выбирать

- **Cron-задача внутри одного бинарника** → `robfig/cron`. Минимальная поверхность.
- **Куча асинхронных задач, fanned out на воркеров** → `asynq` (Redis) или
  `riverqueue/river` (Postgres). Оба *queue-first*: вы кладёте работу в очередь,
  система решает когда её выполнить.
- **Cron-задачи которые должны выполняться **ровно один раз** через N нод, переживать
  рестарт, и репортить SLO-grade метрики** → Sundial. Это и есть его ниша.

Если нужны long-running multi-step workflows с компенсациями (saga, бизнес-процессы
с timeout-ами и waits) → Temporal / Cadence / DBOS — Sundial intentionally не это.

---

## Быстрый старт

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

    every, _ := sundial.Every(5 * time.Minute)
    _, _ = s.Schedule("ping", every, func(ctx context.Context) error {
        slog.Info("ping")
        return nil
    })

    _ = s.Run(context.Background())
}
```

Дополнительные паттерны — в [docs/recipes.md](docs/recipes.md):
- Запуск **один раз** в будущем (`At(...)`)
- Cluster-wide singleton через `WithLeaderOnly()`
- Восстановление пропущенных fires (`Skip` / `RunOnce` / `RunAll`)
- Retry с jitter
- Producer/consumer fan-out
- Минимальная конфигурация OTel + Jaeger
- Тестирование handlers через `MemoryStorage`

---

## Архитектура

- **Per-fire claim** через `pg_try_advisory_xact_lock` с детерминированным int64
  ключом, выведенным из UUID задачи. Строгий `<` semantic на UPDATE гарантирует
  что только одна нода продвинет данный fire.
- **Leader election** через session-scoped advisory lock на отдельном sentinel-key
  (`SundialL` в ASCII). При крахе лидера Postgres освобождает блокировку через
  TCP-keepalive timing, следующий renewal tick на другой ноде её получает.
- **Missed-fire iteration** — генерирует все пропущенные instants в (last_fire, now)
  через `Schedule.Next()`, capped at 256 instants на tick для защиты от
  pathological cases.
- **Retry** — exponential backoff (`InitialBackoff × Multiplier^(attempt-1)`)
  clamped to `MaxBackoff`, jittered в `[0.5, 1.5)`. Jitter — главная защита от
  thundering-herd retries на shared upstream.

Подробный design: [docs/design.md](docs/design.md).

---

## Статус и roadmap

MVP готов. Roadmap к `v1.0.0`:

- [x] Project scaffold, CI, license
- [x] Schedule API — Cron, Every, At
- [x] Storage layer — Postgres + in-memory для тестов
- [x] Dispatcher loop с fetch → claim → execute → record
- [x] Graceful shutdown, drain in-flight handlers
- [x] Panic recovery
- [x] Runnable example (in-memory по умолчанию, Postgres через env)
- [x] Missed-fire policies — Skip, RunOnce, **RunAll** iterator
- [x] OpenTelemetry instrumentation (traces, metrics, lag/duration)
- [x] Leader election через session-scoped advisory lock
- [x] Retry с exponential backoff + jitter; dead-letter на exhaust
- [ ] testcontainers-go integration tests against real Postgres ([#2](https://github.com/goncharovart/sundial/issues/2))
- [ ] Web UI

Открытые issues — в [GitHub](https://github.com/goncharovart/sundial/issues),
помеченные `good first issue`. Дизайн-фидбэк на API ещё актуален — `v0.1.x` series
ещё может ломать совместимость.

---

## Установка

```bash
go get github.com/goncharovart/sundial@v0.1.0
```

Требуется Go 1.22+, PostgreSQL 14+ (для Postgres-бэкенда).

---

## Вклад

Welcome — см. [CONTRIBUTING.md](CONTRIBUTING.md). 5+ good-first-issues с
расписанным scope в body.

[Code of Conduct](CODE_OF_CONDUCT.md) основан на Contributor Covenant v2.1.

---

## Лицензия

MIT. См. [LICENSE](LICENSE).
