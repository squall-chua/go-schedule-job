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
	"errors"
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

// Save inserts or updates a job by ID.
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

// ClaimDue claims up to n due jobs from the named queue, ordered by priority then run_at.
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

// Ack marks a claimed job as successfully completed.
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

const failSelectSQL = `SELECT attempt, max_attempts FROM jobs WHERE id = $1 AND state = $2`
const failDeleteSQL = `DELETE FROM jobs WHERE id = $1`
const failRetrySQL = `
UPDATE jobs SET
    attempt = attempt + 1,
    last_error = $1,
    state = $2,
    run_at = $3,
    locked_by = '',
    locked_until = '0001-01-01 00:00:00+00',
    updated_at = $4
WHERE id = $5 AND state = $6
`

// Fail records an attempt failure. The job is re-queued for retry at nextAttemptAt.
func (s *Store) Fail(ctx context.Context, id gs.JobID, errMsg string, nextAttemptAt time.Time) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("postgres fail begin: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	var attempt, maxAttempts int32
	row := tx.QueryRow(ctx, failSelectSQL, string(id), int(gs.StateClaimed))
	if err := row.Scan(&attempt, &maxAttempts); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return gs.ErrJobNotFound
		}
		return fmt.Errorf("postgres fail select: %w", err)
	}
	nextAttempt := int(attempt) + 1
	if maxAttempts > 0 && nextAttempt >= int(maxAttempts) {
		if _, err := tx.Exec(ctx, failDeleteSQL, string(id)); err != nil {
			return fmt.Errorf("postgres fail terminal: %w", err)
		}
	} else {
		if _, err := tx.Exec(ctx, failRetrySQL,
			errMsg, int(gs.StatePending), nextAttemptAt,
			time.Now(), string(id), int(gs.StateClaimed),
		); err != nil {
			return fmt.Errorf("postgres fail retry: %w", err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("postgres fail commit: %w", err)
	}
	return nil
}

const cancelSelectSQL = `SELECT state FROM jobs WHERE id = $1`
const cancelDeleteSQL = `DELETE FROM jobs WHERE id = $1`

// Cancel marks a pending job as cancelled. Returns gs.ErrJobNotFound or gs.ErrJobNotPending if not applicable.
func (s *Store) Cancel(ctx context.Context, id gs.JobID) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("postgres cancel begin: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	var state int16
	row := tx.QueryRow(ctx, cancelSelectSQL, string(id))
	if err := row.Scan(&state); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return gs.ErrJobNotFound
		}
		return fmt.Errorf("postgres cancel select: %w", err)
	}
	if gs.State(state) != gs.StatePending {
		return gs.ErrJobNotPending
	}
	if _, err := tx.Exec(ctx, cancelDeleteSQL, string(id)); err != nil {
		return fmt.Errorf("postgres cancel delete: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("postgres cancel commit: %w", err)
	}
	return nil
}

const rescheduleSelectSQL = `SELECT state FROM jobs WHERE id = $1`
const rescheduleUpdateSQL = `UPDATE jobs SET run_at = $1, updated_at = $2 WHERE id = $3`

// Reschedule changes a pending job's run_at. Returns gs.ErrJobNotFound or gs.ErrJobNotPending if not applicable.
func (s *Store) Reschedule(ctx context.Context, id gs.JobID, newTime time.Time) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("postgres reschedule begin: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	var state int16
	row := tx.QueryRow(ctx, rescheduleSelectSQL, string(id))
	if err := row.Scan(&state); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return gs.ErrJobNotFound
		}
		return fmt.Errorf("postgres reschedule select: %w", err)
	}
	if gs.State(state) != gs.StatePending {
		return gs.ErrJobNotPending
	}
	if _, err := tx.Exec(ctx, rescheduleUpdateSQL, newTime, time.Now(), string(id)); err != nil {
		return fmt.Errorf("postgres reschedule update: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("postgres reschedule commit: %w", err)
	}
	return nil
}

const heartbeatSQL = `
INSERT INTO workers (worker_id, last_heartbeat) VALUES ($1, $2)
ON CONFLICT (worker_id) DO UPDATE SET last_heartbeat = EXCLUDED.last_heartbeat
`

// Heartbeat records a liveness ping for the given worker.
func (s *Store) Heartbeat(ctx context.Context, workerID string, now time.Time) error {
	if _, err := s.pool.Exec(ctx, heartbeatSQL, workerID, now); err != nil {
		return fmt.Errorf("postgres heartbeat: %w", err)
	}
	return nil
}

const recoverStaleSQL = `
UPDATE jobs
SET state = $1, locked_by = '', locked_until = '0001-01-01 00:00:00+00', updated_at = $2
WHERE state = $3
  AND locked_until > '0001-01-01 00:00:00+00'
  AND locked_until < now()
`

// RecoverStale re-queues jobs whose claims have expired and returns the count recovered.
func (s *Store) RecoverStale(ctx context.Context, now time.Time) (int, error) {
	tag, err := s.pool.Exec(ctx, recoverStaleSQL,
		int(gs.StatePending), now, int(gs.StateClaimed),
	)
	if err != nil {
		return 0, fmt.Errorf("postgres recover stale: %w", err)
	}
	return int(tag.RowsAffected()), nil
}

const queueSizeSQL = `SELECT COUNT(*) FROM jobs WHERE queue = $1 AND state = $2`

// QueueSize returns the count of pending jobs in the named queue.
func (s *Store) QueueSize(ctx context.Context, queue string) (int, error) {
	var n int
	row := s.pool.QueryRow(ctx, queueSizeSQL, queue, int(gs.StatePending))
	if err := row.Scan(&n); err != nil {
		return 0, fmt.Errorf("postgres queue size: %w", err)
	}
	return n, nil
}

const upsertRecurringSQL = `
INSERT INTO recurring (
    id, name, queue, payload, codec_name, priority, timeout_ns, max_attempts,
    cron, every_ns, catchup, next_run_at, last_fire_at, lease_until, leased_by
) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)
ON CONFLICT (id) DO UPDATE SET
    name=EXCLUDED.name,
    queue=EXCLUDED.queue,
    payload=EXCLUDED.payload,
    codec_name=EXCLUDED.codec_name,
    priority=EXCLUDED.priority,
    timeout_ns=EXCLUDED.timeout_ns,
    max_attempts=EXCLUDED.max_attempts,
    cron=EXCLUDED.cron,
    every_ns=EXCLUDED.every_ns,
    catchup=EXCLUDED.catchup,
    next_run_at=EXCLUDED.next_run_at,
    last_fire_at=EXCLUDED.last_fire_at
`

const listRecurringSQL = `
SELECT id, name, queue, payload, codec_name, priority, timeout_ns, max_attempts,
       cron, every_ns, catchup, next_run_at, last_fire_at, lease_until, leased_by
FROM recurring
`

const deleteRecurringSQL = `DELETE FROM recurring WHERE id = $1`

const updateRecurringNextRunSQL = `
UPDATE recurring SET next_run_at = $1, last_fire_at = $2 WHERE id = $3
`

// UpsertRecurring inserts or replaces a recurring schedule.
func (s *Store) UpsertRecurring(ctx context.Context, spec gs.RecurringSpec) error {
	if _, err := s.pool.Exec(ctx, upsertRecurringSQL,
		string(spec.ID), spec.Name, spec.Queue, spec.Payload, spec.CodecName,
		int(spec.Priority), int64(spec.Timeout), spec.MaxAttempts,
		spec.Cron, int64(spec.Every), spec.Catchup,
		spec.NextRunAt, spec.LastFireAt,
		spec.LeaseUntil, spec.LeasedBy,
	); err != nil {
		return fmt.Errorf("postgres upsert recurring: %w", err)
	}
	return nil
}

// ListRecurring returns all recurring schedules.
func (s *Store) ListRecurring(ctx context.Context) ([]gs.RecurringSpec, error) {
	rows, err := s.pool.Query(ctx, listRecurringSQL)
	if err != nil {
		return nil, fmt.Errorf("postgres list recurring: %w", err)
	}
	defer rows.Close()
	var out []gs.RecurringSpec
	for rows.Next() {
		var id, name, queue, codec, cron, leasedBy string
		var payload []byte
		var priority int16
		var maxAttempts int32
		var catch bool
		var timeoutNs, everyNs int64
		var nextRunAt, lastFireAt, leaseUntil time.Time
		if err := rows.Scan(
			&id, &name, &queue, &payload, &codec, &priority, &timeoutNs, &maxAttempts,
			&cron, &everyNs, &catch, &nextRunAt, &lastFireAt, &leaseUntil, &leasedBy,
		); err != nil {
			return nil, fmt.Errorf("postgres list recurring scan: %w", err)
		}
		out = append(out, gs.RecurringSpec{
			ID: gs.JobID(id), Name: name, Queue: queue, Payload: payload, CodecName: codec,
			Priority: gs.Priority(priority), Timeout: time.Duration(timeoutNs), MaxAttempts: int(maxAttempts),
			Cron: cron, Every: time.Duration(everyNs), Catchup: catch,
			NextRunAt: nextRunAt, LastFireAt: lastFireAt,
			LeaseUntil: leaseUntil, LeasedBy: leasedBy,
		})
	}
	return out, rows.Err()
}

// DeleteRecurring removes a recurring schedule by ID.
func (s *Store) DeleteRecurring(ctx context.Context, id gs.JobID) error {
	if _, err := s.pool.Exec(ctx, deleteRecurringSQL, string(id)); err != nil {
		return fmt.Errorf("postgres delete recurring: %w", err)
	}
	return nil
}

// UpdateRecurringNextRun records the next firing time and last fire time on a recurring schedule.
func (s *Store) UpdateRecurringNextRun(ctx context.Context, id gs.JobID, nextRunAt, lastFireAt time.Time) error {
	if _, err := s.pool.Exec(ctx, updateRecurringNextRunSQL, nextRunAt, lastFireAt, string(id)); err != nil {
		return fmt.Errorf("postgres update recurring next: %w", err)
	}
	return nil
}

const acquireLeaseSQL = `
UPDATE recurring SET lease_until = $1, leased_by = $2
WHERE id = $3
  AND (lease_until = '0001-01-01 00:00:00+00' OR lease_until < now() OR leased_by = $4)
`

const leaseSpecExistsSQL = `SELECT 1 FROM recurring WHERE id = $1`

// AcquireRecurringLease attempts to claim exclusive responsibility for firing the recurring schedule until leaseUntil. Returns true if the caller holds the lease.
func (s *Store) AcquireRecurringLease(ctx context.Context, specID gs.JobID, leaseUntil time.Time, workerID string) (bool, error) {
	tag, err := s.pool.Exec(ctx, acquireLeaseSQL,
		leaseUntil, workerID, string(specID), workerID,
	)
	if err != nil {
		return false, fmt.Errorf("postgres acquire lease: %w", err)
	}
	if tag.RowsAffected() > 0 {
		return true, nil
	}
	var one int
	if err := s.pool.QueryRow(ctx, leaseSpecExistsSQL, string(specID)).Scan(&one); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			// No such spec — treat as acquirable (matches in-memory store semantics).
			return true, nil
		}
		return false, fmt.Errorf("postgres acquire lease exists: %w", err)
	}
	return false, nil
}

// Compile-time check that *Store satisfies gs.Store.
var _ gs.Store = (*Store)(nil)

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
