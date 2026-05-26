// Package postgres provides a Postgres-backed Store for goschedule.
//
// Designed for horizontal scaling: N scheduler processes may share one
// Postgres database safely. Correctness rests on three mechanisms:
//
//   - FOR UPDATE SKIP LOCKED in ClaimDue, so concurrent schedulers hand out
//     disjoint job sets.
//   - Conditional UPDATE in AcquireRecurringLease, so exactly one scheduler
//     fires each recurring period.
//   - locked_until visibility timeouts plus RecoverStale, so jobs from a
//     crashed scheduler are recovered by any survivor.
//
// All expiry comparisons (lease_until, locked_until) route through Postgres
// now() rather than caller-supplied time, so schedulers with skewed wall
// clocks still agree on what is expired.
package postgres

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"

	gs "github.com/squall-chua/go-schedule-job"
)

// Store implements goschedule.Store backed by PostgreSQL.
type Store struct {
	pool *pgxpool.Pool
}

// New opens a connection pool against the given Postgres DSN and applies the
// schema. The DSN is in libpq form, e.g.
//
//	postgres://user:pass@host:5432/dbname?sslmode=disable
//
// Connection pool defaults come from pgxpool; tune via the DSN
// (e.g. ?pool_max_conns=20) if needed.
func New(ctx context.Context, dsn string) (*Store, error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("postgres pool: %w", err)
	}
	if _, err := pool.Exec(ctx, schema); err != nil {
		pool.Close()
		return nil, fmt.Errorf("postgres migrate: %w", err)
	}
	return &Store{pool: pool}, nil
}

// Close releases the connection pool.
func (s *Store) Close() { s.pool.Close() }

// Truncate clears all goschedule tables. INTENDED FOR TESTS — destructive in
// production. Idempotent.
func (s *Store) Truncate(ctx context.Context) error {
	_, err := s.pool.Exec(ctx, `TRUNCATE jobs, recurring, workers`)
	if err != nil {
		return fmt.Errorf("postgres truncate: %w", err)
	}
	return nil
}

const upsertJobSQL = `
INSERT INTO jobs (
    id, queue, name, payload, codec_name, priority, run_at, attempt,
    max_attempts, state, timeout_ns, locked_by, locked_until, last_error,
    recurring_id, created_at, updated_at
) VALUES (
    $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17
)
ON CONFLICT (id) DO UPDATE SET
    queue=EXCLUDED.queue,
    name=EXCLUDED.name,
    payload=EXCLUDED.payload,
    codec_name=EXCLUDED.codec_name,
    priority=EXCLUDED.priority,
    run_at=EXCLUDED.run_at,
    attempt=EXCLUDED.attempt,
    max_attempts=EXCLUDED.max_attempts,
    state=EXCLUDED.state,
    timeout_ns=EXCLUDED.timeout_ns,
    locked_by=EXCLUDED.locked_by,
    locked_until=EXCLUDED.locked_until,
    last_error=EXCLUDED.last_error,
    recurring_id=EXCLUDED.recurring_id,
    updated_at=EXCLUDED.updated_at
`

// Save persists or upserts the job, then sends a NOTIFY on
// "goschedule_<queue>" so listeners (see Listen) can wake immediately.
func (s *Store) Save(ctx context.Context, j gs.Job) error {
	state := j.State
	if state == 0 {
		state = gs.StatePending
	}
	maxAttempts := j.MaxAttempts
	if maxAttempts == 0 {
		maxAttempts = 3
	}
	if _, err := s.pool.Exec(ctx, upsertJobSQL,
		string(j.ID), j.Queue, j.Name, j.Payload, j.CodecName,
		int(j.Priority), j.RunAt, j.Attempt,
		maxAttempts, int(state), int64(j.Timeout), j.LockedBy,
		j.LockedUntil, j.LastError, string(j.RecurringID),
		j.CreatedAt, j.UpdatedAt,
	); err != nil {
		return fmt.Errorf("postgres save: %w", err)
	}
	// Best-effort wake of any listener. We deliberately swallow errors —
	// NOTIFY is an optimisation, not part of correctness; the poll fallback
	// will still pick the job up within pollInterval.
	channel := notifyChannel(j.Queue)
	_, _ = s.pool.Exec(ctx, "SELECT pg_notify($1, '')", channel)
	return nil
}

// notifyChannel maps a queue name to a Postgres NOTIFY channel.
// Postgres channel names must be valid identifiers; we use pg_notify() with a
// quoted string so any queue name is safe regardless of characters.
func notifyChannel(queue string) string {
	return "goschedule_" + queue
}
