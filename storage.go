package sundial

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// DueJob is the projection of a row from sundial_jobs that the dispatcher
// reads on each tick. It carries the minimum the dispatcher needs to claim
// and execute a fire without holding the storage interface open.
type DueJob struct {
	ID           string
	Name         string
	NextFireTime time.Time
	ScheduleExpr string
	ScheduleKind string
}

// RunOutcome is the terminal state recorded for a single execution attempt.
type RunOutcome string

const (
	RunSucceeded  RunOutcome = "success"
	RunFailed     RunOutcome = "failed"
	RunDeadLetter RunOutcome = "dead-letter"
)

// RunRecord captures the result of one execution attempt.
type RunRecord struct {
	JobID      string
	FireTime   time.Time
	StartedAt  time.Time
	FinishedAt time.Time
	NodeID     string
	Attempt    int
	Outcome    RunOutcome
	Error      string
}

// Storage-layer sentinel errors. Callers check them with errors.Is.
var (
	ErrJobNotFound = errors.New("sundial: job not found")
	ErrJobLocked   = errors.New("sundial: job is locked by another node")
)

// Storage is the persistence layer the dispatcher depends on.
//
// The Scheduler holds one Storage instance per process. The Postgres
// implementation lives below; an in-memory implementation used by tests
// lives in storage_memory.go alongside this file.
type Storage interface {
	// EnsureJob upserts a job by name and returns its id. The first fire
	// time is computed by the caller and persisted on insert; on update
	// (job re-registered with the same name) the existing next_fire_time
	// is preserved unless callers explicitly reset it.
	EnsureJob(ctx context.Context, j *Job, firstFire time.Time) (jobID string, err error)

	// FetchDue returns up to limit jobs whose next_fire_time is at or
	// before now, oldest fire-time first. The returned slice is a
	// snapshot — by the time the caller reaches ClaimJob, another node
	// may have already claimed the row.
	FetchDue(ctx context.Context, now time.Time, limit int) ([]DueJob, error)

	// ClaimJob attempts to atomically advance the job's next_fire_time
	// from its current value to nextFire, under a Postgres advisory
	// lock keyed on the job id. Returns true on successful claim, false
	// if another node won the race. Releases the lock at the end of the
	// transaction in either case.
	//
	// The caller is responsible for executing the job's handler after a
	// successful claim and for calling RecordRun with the outcome. The
	// advisory lock is intentionally released before handler execution
	// — the dispatcher relies on the advanced next_fire_time, not the
	// lock, to guarantee single-execution semantics.
	ClaimJob(ctx context.Context, jobID string, nextFire time.Time) (claimed bool, err error)

	// RecordRun persists the outcome of one execution attempt to the
	// runs table. It is idempotent on (job_id, fire_time, attempt).
	RecordRun(ctx context.Context, run RunRecord) error
}

// PostgresStorage is the production Storage implementation backed by a
// connected *pgxpool.Pool. The pool's lifecycle is owned by the caller
// (typically the Scheduler), not by PostgresStorage.
type PostgresStorage struct {
	pool   *pgxpool.Pool
	schema string
}

// NewPostgresStorage wraps a pgxpool.Pool. The schema argument lets
// callers isolate Sundial's tables (default "public").
func NewPostgresStorage(pool *pgxpool.Pool, schema string) *PostgresStorage {
	if schema == "" {
		schema = "public"
	}
	return &PostgresStorage{pool: pool, schema: schema}
}

func (s *PostgresStorage) qualify(table string) string {
	return fmt.Sprintf("%s.%s", s.schema, table)
}

// EnsureJob upserts the job row, preserving an existing next_fire_time
// when the job already exists by name.
func (s *PostgresStorage) EnsureJob(ctx context.Context, j *Job, firstFire time.Time) (string, error) {
	if j == nil {
		return "", errors.New("sundial: nil job")
	}
	sql := fmt.Sprintf(`
		INSERT INTO %s (name, schedule, schedule_kind, next_fire_time)
		VALUES ($1, $2, $3, $4)
		ON CONFLICT (name) DO UPDATE
			SET schedule = EXCLUDED.schedule,
			    schedule_kind = EXCLUDED.schedule_kind,
			    updated_at = now()
		RETURNING id::text
	`, s.qualify("sundial_jobs"))

	var id string
	err := s.pool.QueryRow(ctx, sql,
		j.Name,
		j.Schedule.String(),
		j.Schedule.Kind(),
		firstFire.UTC(),
	).Scan(&id)
	if err != nil {
		return "", fmt.Errorf("sundial: ensure job %q: %w", j.Name, err)
	}
	return id, nil
}

// FetchDue returns up to limit jobs whose next fire is at or before now.
func (s *PostgresStorage) FetchDue(ctx context.Context, now time.Time, limit int) ([]DueJob, error) {
	if limit <= 0 {
		limit = 32
	}
	sql := fmt.Sprintf(`
		SELECT id::text, name, next_fire_time, schedule, schedule_kind
		FROM %s
		WHERE next_fire_time <= $1
		ORDER BY next_fire_time
		LIMIT $2
	`, s.qualify("sundial_jobs"))

	rows, err := s.pool.Query(ctx, sql, now.UTC(), limit)
	if err != nil {
		return nil, fmt.Errorf("sundial: fetch due: %w", err)
	}
	defer rows.Close()

	var out []DueJob
	for rows.Next() {
		var d DueJob
		if err := rows.Scan(&d.ID, &d.Name, &d.NextFireTime, &d.ScheduleExpr, &d.ScheduleKind); err != nil {
			return nil, fmt.Errorf("sundial: scan due: %w", err)
		}
		out = append(out, d)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("sundial: iterate due: %w", err)
	}
	return out, nil
}

// ClaimJob runs the claim transaction. It uses a transaction-scoped
// advisory lock keyed on a stable int64 derived from the job id — the
// lock auto-releases on commit/rollback, so callers do not need to
// release it explicitly. The UPDATE only succeeds if next_fire_time
// has not already been advanced past the value we read in FetchDue.
func (s *PostgresStorage) ClaimJob(ctx context.Context, jobID string, nextFire time.Time) (bool, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return false, fmt.Errorf("sundial: begin claim tx: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	// Hash the job id to a deterministic int64 for the advisory key.
	lockKey := advisoryKey(jobID)

	var locked bool
	if err := tx.QueryRow(ctx, `SELECT pg_try_advisory_xact_lock($1)`, lockKey).Scan(&locked); err != nil {
		return false, fmt.Errorf("sundial: advisory lock: %w", err)
	}
	if !locked {
		return false, nil
	}

	// Strict `<` (not `<=`) is required: if next_fire has already been
	// advanced to the same value by another node, RowsAffected would
	// still be 1, hiding the race. With `<` only the first advance wins.
	updateSQL := fmt.Sprintf(`
		UPDATE %s
		SET next_fire_time = $2, updated_at = now()
		WHERE id = $1::uuid AND next_fire_time < $2
	`, s.qualify("sundial_jobs"))

	tag, err := tx.Exec(ctx, updateSQL, jobID, nextFire.UTC())
	if err != nil {
		return false, fmt.Errorf("sundial: advance next_fire: %w", err)
	}
	if tag.RowsAffected() == 0 {
		// Lock held but another tick already advanced — treat as not claimed.
		return false, nil
	}

	if err := tx.Commit(ctx); err != nil {
		return false, fmt.Errorf("sundial: commit claim: %w", err)
	}
	return true, nil
}

// RecordRun inserts a row into sundial_runs.
func (s *PostgresStorage) RecordRun(ctx context.Context, run RunRecord) error {
	sql := fmt.Sprintf(`
		INSERT INTO %s (job_id, fire_time, started_at, finished_at, attempt, status, error, node_id)
		VALUES ($1::uuid, $2, $3, $4, $5, $6, NULLIF($7, ''), $8)
		ON CONFLICT DO NOTHING
	`, s.qualify("sundial_runs"))

	if run.FinishedAt.IsZero() {
		run.FinishedAt = time.Now().UTC()
	}
	if run.StartedAt.IsZero() {
		run.StartedAt = run.FinishedAt
	}
	if run.Attempt < 1 {
		run.Attempt = 1
	}

	_, err := s.pool.Exec(ctx, sql,
		run.JobID,
		run.FireTime.UTC(),
		run.StartedAt.UTC(),
		run.FinishedAt.UTC(),
		run.Attempt,
		string(run.Outcome),
		run.Error,
		run.NodeID,
	)
	if err != nil {
		return fmt.Errorf("sundial: record run: %w", err)
	}
	return nil
}

// advisoryKey derives a stable int64 advisory-lock key from a job id.
// It mixes the bytes with a 64-bit FNV-1a so two ids with the same
// prefix do not collide.
func advisoryKey(id string) int64 {
	const (
		offset = 0xcbf29ce484222325
		prime  = 0x100000001b3
	)
	var h uint64 = offset
	for i := 0; i < len(id); i++ {
		h ^= uint64(id[i])
		h *= prime
	}
	return int64(h)
}

// Ensure PostgresStorage satisfies Storage at compile time.
var _ Storage = (*PostgresStorage)(nil)

// --- MemoryStorage ---------------------------------------------------------

// MemoryStorage is an in-memory Storage implementation used by unit tests.
// It is safe for concurrent use. The dispatcher loop and end-to-end logic
// can be exercised against it without a Postgres dependency.
type MemoryStorage struct {
	mu    sync.Mutex
	jobs  map[string]*memJob
	runs  []RunRecord
	next  int
}

type memJob struct {
	ID           string
	Name         string
	NextFireTime time.Time
	Schedule     string
	ScheduleKind string
	Locked       bool
}

// NewMemoryStorage returns an empty in-memory store.
func NewMemoryStorage() *MemoryStorage {
	return &MemoryStorage{jobs: make(map[string]*memJob)}
}

func (m *MemoryStorage) EnsureJob(_ context.Context, j *Job, firstFire time.Time) (string, error) {
	if j == nil {
		return "", errors.New("sundial: nil job")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if existing, ok := m.jobs[j.Name]; ok {
		existing.Schedule = j.Schedule.String()
		existing.ScheduleKind = j.Schedule.Kind()
		return existing.ID, nil
	}
	m.next++
	id := fmt.Sprintf("mem-job-%d", m.next)
	m.jobs[j.Name] = &memJob{
		ID:           id,
		Name:         j.Name,
		NextFireTime: firstFire.UTC(),
		Schedule:     j.Schedule.String(),
		ScheduleKind: j.Schedule.Kind(),
	}
	return id, nil
}

func (m *MemoryStorage) FetchDue(_ context.Context, now time.Time, limit int) ([]DueJob, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if limit <= 0 {
		limit = 32
	}
	var out []DueJob
	for _, j := range m.jobs {
		if !j.NextFireTime.After(now) {
			out = append(out, DueJob{
				ID:           j.ID,
				Name:         j.Name,
				NextFireTime: j.NextFireTime,
				ScheduleExpr: j.Schedule,
				ScheduleKind: j.ScheduleKind,
			})
		}
	}
	// Stable order by next_fire_time, then name, mirrors the SQL ORDER BY.
	for i := 1; i < len(out); i++ {
		for k := i; k > 0; k-- {
			a, b := out[k-1], out[k]
			if a.NextFireTime.After(b.NextFireTime) || (a.NextFireTime.Equal(b.NextFireTime) && a.Name > b.Name) {
				out[k-1], out[k] = b, a
			} else {
				break
			}
		}
	}
	if len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}

func (m *MemoryStorage) ClaimJob(_ context.Context, jobID string, nextFire time.Time) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	for _, j := range m.jobs {
		if j.ID == jobID {
			if j.Locked {
				return false, nil
			}
			// Strict `Before` matches the SQL `<` semantics: only
			// advance when the requested nextFire is strictly newer
			// than the current value. Equal means another node has
			// already advanced to this same fire — not our claim.
			if !j.NextFireTime.Before(nextFire) {
				return false, nil
			}
			j.NextFireTime = nextFire.UTC()
			return true, nil
		}
	}
	return false, ErrJobNotFound
}

func (m *MemoryStorage) RecordRun(_ context.Context, run RunRecord) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if run.FinishedAt.IsZero() {
		run.FinishedAt = time.Now().UTC()
	}
	if run.StartedAt.IsZero() {
		run.StartedAt = run.FinishedAt
	}
	if run.Attempt < 1 {
		run.Attempt = 1
	}
	m.runs = append(m.runs, run)
	return nil
}

// Runs returns a copy of all recorded runs, oldest first. Tests use it
// to assert outcomes after the dispatcher has executed jobs.
func (m *MemoryStorage) Runs() []RunRecord {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]RunRecord, len(m.runs))
	copy(out, m.runs)
	return out
}

var _ Storage = (*MemoryStorage)(nil)

// Compile-time sanity: silence "unused" complaints if pgx is dropped later.
var _ = pgx.ErrNoRows
