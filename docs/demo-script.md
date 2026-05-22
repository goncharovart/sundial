# Sundial 60-second demo — asciinema recording script

Этот файл — точный скрипт для записи 60-секундного демо Sundial → GIF в README.

## Setup (один раз)

```powershell
# Install asciinema + agg (Rust CLI converting .cast → .gif)
choco install asciinema
cargo install --git https://github.com/asciinema/agg
```

Использовать **Windows Terminal** с тёмной темой + шрифт 16pt+.

## Запись

```powershell
cd "C:\Users\gonch\OneDrive\Рабочий стол\гитхаб, резюме\02_oss_project\sundial"
asciinema rec demo.cast --idle-time-limit 2 --title "Sundial 60s demo"
```

После `rec` команды — выполнить **точную последовательность ниже** (естественный темп, без спешки):

## Sequence (60 секунд total)

```bash
# ---- 0:00-0:05 — заголовок ----
clear
echo "Sundial — distributed cron scheduler for Go"
echo "Postgres advisory locks · leader election · OpenTelemetry"

# ---- 0:05-0:15 — start in-memory demo ----
echo "$ go run ./examples/hello"
go run ./examples/hello &
SCHEDULER_PID=$!
sleep 8

# Логи появляются автоматически:
#   INFO leader.acquired node=node-1
#   INFO job.claimed name=hello node=node-1
#   INFO job.run name=hello duration=2ms outcome=success

# ---- 0:35-0:45 — graceful shutdown ----
echo ""
echo "$ Ctrl-C — graceful shutdown drains in-flight jobs"
kill -INT $SCHEDULER_PID
sleep 2
echo "Done."
exit
```

## Convert .cast → .gif

```powershell
agg demo.cast demo.gif `
  --rows 24 --cols 100 `
  --font-size 16 --speed 1.5 `
  --theme monokai
```

Размер: ≤2MB, ≤800×500px (GitHub README autoplay limit).

## Embed в README

```markdown
![Sundial demo](docs/demo.gif)
```

Это **multiplier на распространение** — GIF в первом viewport README удваивает stars от Twitter/Reddit (per research 31).

## Альтернатива: Docker compose демо (более полное)

Если хочешь показать distributed leader election:

```bash
# Bring up Postgres + 3 worker'а
docker compose -f deploy/docker-compose.yml up -d
sleep 3

# Apply migrations
migrate -path migrations -database "$DATABASE_URL" up

# Start первый scheduler как leader
go run ./examples/hello &
PID1=$!
sleep 5

# Kill leader → новый leader picks up
kill -9 $PID1
go run ./examples/hello &
sleep 5

# Cleanup
docker compose down -v
```

## После записи

1. Copy `demo.gif` → `docs/demo.gif`
2. Embed в README первый экран
3. Commit:
   ```bash
   git add docs/demo.gif README.md
   git commit -m "docs(demo): add 60s asciinema demo for README first viewport"
   git push
   ```

## Anti-patterns

- ❌ Запись >90 секунд (attention drops)
- ❌ Опечатки во время записи (asciinema сохраняет)
- ❌ Файл >3MB (GitHub README autoplay лимит)
- ❌ `agg --no-loop` (gif должен loop для feed-engagement)
