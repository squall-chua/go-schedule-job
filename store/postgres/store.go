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
	"time"

	"github.com/jackc/pgx/v5"
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

const claimDueSQL = `
WITH due AS (
    SELECT id FROM jobs
    WHERE queue = $1 AND state = $2 AND run_at <= $3
    ORDER BY priority DESC, run_at ASC
    LIMIT $4
    FOR UPDATE SKIP LOCKED
)
UPDATE jobs
SET state = $5, locked_by = $6, locked_until = $7, updated_at = $8
FROM due
WHERE jobs.id = due.id
RETURNING jobs.id, jobs.queue, jobs.name, jobs.payload, jobs.codec_name,
          jobs.priority, jobs.run_at, jobs.attempt, jobs.max_attempts,
          jobs.state, jobs.timeout_ns, jobs.locked_by, jobs.locked_until,
          jobs.last_error, jobs.recurring_id, jobs.created_at, jobs.updated_at
`

func (s *Store) ClaimDue(ctx context.Context, queue string, now time.Time, n int, workerID string, lockUntil time.Time) ([]gs.Job, error) {
	rows, err := s.pool.Query(ctx, claimDueSQL,
		queue, int(gs.StatePending), now, n,
		int(gs.StateClaimed), workerID, lockUntil, now,
	)
	if err != nil {
		return nil, fmt.Errorf("postgres claim: %w", err)
	}
	defer rows.Close()
	var out []gs.Job
	for rows.Next() {
		j, err := scanJob(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, j)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("postgres claim iterate: %w", err)
	}
	// Postgres returns rows in arbitrary order; the SQL spec doesn't guarantee
	// RETURNING preserves ORDER BY of the CTE. Sort to honor the
	// "priority DESC, run_at ASC" contract callers depend on.
	sortJobs(out)
	return out, nil
}

func sortJobs(js []gs.Job) {
	// Insertion sort suffices — claim batches are small (default ~32).
	for i := 1; i < len(js); i++ {
		j := js[i]
		k := i - 1
		for k >= 0 && jobLess(j, js[k]) {
			js[k+1] = js[k]
			k--
		}
		js[k+1] = j
	}
}

// jobLess: higher priority comes first; ties broken by earlier run_at.
func jobLess(a, b gs.Job) bool {
	if a.Priority != b.Priority {
		return a.Priority > b.Priority
	}
	return a.RunAt.Before(b.RunAt)
}

const ackSQL = `DELETE FROM jobs WHERE id = $1 AND state = $2`

func (s *Store) Ack(ctx context.Context, id gs.JobID) error {
	tag, err := s.pool.Exec(ctx, ackSQL, string(id), int(gs.StateClaimed))
	if err != nil {
		return fmt.Errorf("postgres ack: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return gs.ErrJobNotFound
	}
	return nil
}

// scanJob reads a single row from ClaimDue's RETURNING projection. Accepts
// pgx.Rows so it can be shared with future single-row helpers via pgx.Row.
func scanJob(rows pgx.Rows) (gs.Job, error) {
	var j gs.Job
	var id, queue, name, codecName, lockedBy, lastError, recurringID string
	var payload []byte
	var priority, state int16
	var attempt, maxAttempts int32
	var timeoutNs int64
	if err := rows.Scan(
		&id, &queue, &name, &payload, &codecName,
		&priority, &j.RunAt, &attempt, &maxAttempts,
		&state, &timeoutNs, &lockedBy, &j.LockedUntil,
		&lastError, &recurringID, &j.CreatedAt, &j.UpdatedAt,
	); err != nil {
		return j, fmt.Errorf("postgres scan job: %w", err)
	}
	j.ID = gs.JobID(id)
	j.Queue = queue
	j.Name = name
	j.Payload = payload
	j.CodecName = codecName
	j.Priority = gs.Priority(priority)
	j.Attempt = int(attempt)
	j.MaxAttempts = int(maxAttempts)
	j.State = gs.State(state)
	j.Timeout = time.Duration(timeoutNs)
	j.LockedBy = lockedBy
	j.LastError = lastError
	j.RecurringID = gs.JobID(recurringID)
	return j, nil
}
