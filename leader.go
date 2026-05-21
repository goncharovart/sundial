package sundial

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Leader holds (or fails to hold) the cluster-wide leader role.
//
// Sundial uses leadership for one purpose: deciding which node runs
// `LeaderOnly` jobs and (in a follow-up commit) which node performs
// missed-fire recovery iteration. Per-job claim races still go through
// the regular Storage.ClaimJob path — the leader does NOT do anything
// non-leaders could not do; it is a coordination hint, not a fence.
//
// The Postgres implementation holds a session-scoped advisory lock on
// a dedicated sentinel key. When the holding node dies, Postgres
// releases the lock automatically on connection close, and the next
// renewal tick on another node will acquire it.
//
// Tests use NewMemoryLeader, which gives the same Try/Release/IsLeader
// shape backed by a mutex.
type Leader interface {
	// TryAcquire attempts to acquire the leader lock. Returns
	// (true, nil) on success or if the caller is already the leader,
	// (false, nil) if another node holds the lock, or (_, err) on a
	// backend failure.
	TryAcquire(ctx context.Context, nodeID string) (bool, error)

	// Release relinquishes leadership. Safe to call when not held;
	// idempotent.
	Release(ctx context.Context) error

	// IsLeader returns the current cached leadership state without
	// touching the backend. Cheap; the dispatcher calls it on every
	// per-job dispatch.
	IsLeader() bool
}

// leaderSentinelKey is the sentinel int64 used for the
// pg_try_advisory_lock that backs leadership. Derived from the ASCII
// bytes of "SundialL" so it does not collide with per-job advisory
// keys (which are FNV-1a of UUID strings).
const leaderSentinelKey int64 = 0x53756e6469616c4c // "SundialL"

// --- PostgresLeader -------------------------------------------------------

// PostgresLeader implements Leader on top of pgxpool by holding a
// session-scoped advisory lock. The lock auto-releases when the
// connection closes, so a crashed leader frees the role within seconds
// (Postgres TCP keepalive timing).
type PostgresLeader struct {
	pool *pgxpool.Pool

	mu     sync.Mutex
	conn   *pgxpool.Conn // pinned while leader; nil otherwise
	held   atomic.Bool
	nodeID string
}

// NewPostgresLeader builds a leader bound to the supplied pool. It
// does not contact the database until TryAcquire is called the first
// time.
func NewPostgresLeader(pool *pgxpool.Pool) *PostgresLeader {
	return &PostgresLeader{pool: pool}
}

// TryAcquire pins a connection from the pool and runs
// pg_try_advisory_lock against the sentinel key. If the lock is
// granted, the connection stays pinned until Release; Postgres holds
// the lock for the lifetime of that session.
func (l *PostgresLeader) TryAcquire(ctx context.Context, nodeID string) (bool, error) {
	if nodeID == "" {
		return false, errors.New("sundial: leader nodeID is required")
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	if l.held.Load() {
		// Already leader on this node. Idempotent.
		return true, nil
	}

	conn, err := l.pool.Acquire(ctx)
	if err != nil {
		return false, fmt.Errorf("sundial: acquire leader conn: %w", err)
	}

	var locked bool
	if err := conn.QueryRow(ctx, `SELECT pg_try_advisory_lock($1)`, leaderSentinelKey).Scan(&locked); err != nil {
		conn.Release()
		return false, fmt.Errorf("sundial: try leader lock: %w", err)
	}
	if !locked {
		conn.Release()
		return false, nil
	}

	l.conn = conn
	l.nodeID = nodeID
	l.held.Store(true)
	return true, nil
}

// Release relinquishes leadership: explicitly unlocks (so other
// sessions don't have to wait for connection-close detection) and
// returns the connection to the pool.
func (l *PostgresLeader) Release(ctx context.Context) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if !l.held.Load() {
		return nil
	}

	conn := l.conn
	l.conn = nil
	l.nodeID = ""
	l.held.Store(false)

	if conn == nil {
		return nil
	}

	var released bool
	err := conn.QueryRow(ctx, `SELECT pg_advisory_unlock($1)`, leaderSentinelKey).Scan(&released)
	conn.Release()
	if err != nil {
		return fmt.Errorf("sundial: release leader lock: %w", err)
	}
	return nil
}

// IsLeader returns the cached leadership flag.
func (l *PostgresLeader) IsLeader() bool { return l.held.Load() }

var _ Leader = (*PostgresLeader)(nil)

// --- MemoryLeader ---------------------------------------------------------

// MemoryLeader is the in-process Leader implementation used by tests
// and by Scheduler when WithStorage(NewMemoryStorage()) is configured.
// A single MemoryLeader value can be shared across multiple Schedulers
// in the same test to simulate two competing nodes.
type MemoryLeader struct {
	mu     sync.Mutex
	holder string // empty means free
	myID   string
}

// NewMemoryLeader returns a fresh in-process leader.
func NewMemoryLeader() *MemoryLeader { return &MemoryLeader{} }

func (l *MemoryLeader) TryAcquire(_ context.Context, nodeID string) (bool, error) {
	if nodeID == "" {
		return false, errors.New("sundial: leader nodeID is required")
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.holder == "" {
		l.holder = nodeID
		l.myID = nodeID
		return true, nil
	}
	if l.holder == nodeID {
		// Same node calling Try again — idempotent re-acquire.
		l.myID = nodeID
		return true, nil
	}
	return false, nil
}

func (l *MemoryLeader) Release(_ context.Context) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.holder == l.myID {
		l.holder = ""
	}
	l.myID = ""
	return nil
}

func (l *MemoryLeader) IsLeader() bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.myID != "" && l.holder == l.myID
}

var _ Leader = (*MemoryLeader)(nil)
