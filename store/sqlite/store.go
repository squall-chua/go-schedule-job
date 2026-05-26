// Package sqlite provides a SQLite-backed Store for goschedule.
// Single-node only — uses SQLite's file-level locking; no cross-process
// distribution. Use store/postgres or store/redis for distributed workers.
package sqlite

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	gs "github.com/squall-chua/go-schedule-job"
	_ "modernc.org/sqlite" // pure-Go SQLite driver
)

// Store implements goschedule.Store backed by SQLite.
type Store struct {
	db *sql.DB
}

// New opens or creates a SQLite database at path and applies the schema.
// Use ":memory:" for an in-memory database (note: tests using :memory: must
// share a single Store — separate New() calls produce separate DBs).
func New(path string) (*Store, error) {
	// WAL improves write concurrency; busy_timeout avoids "database is locked"
	// errors under contention.
	dsn := fmt.Sprintf("file:%s?_pragma=journal_mode(wal)&_pragma=busy_timeout(5000)", path)
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("sqlite open: %w", err)
	}
	// SQLite serializes writes at the file level; limit to one writer to keep
	// transactions cleanly serialized and avoid spurious busy errors.
	db.SetMaxOpenConns(1)
	if _, err := db.Exec(schema); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("sqlite migrate: %w", err)
	}
	return &Store{db: db}, nil
}

// Close releases the underlying database handle.
func (s *Store) Close() error { return s.db.Close() }

// --- internal time helpers ---

func toUnixNano(t time.Time) int64 {
	if t.IsZero() {
		return 0
	}
	return t.UnixNano()
}

func fromUnixNano(n int64) time.Time {
	if n == 0 {
		return time.Time{}
	}
	return time.Unix(0, n)
}

const upsertJobSQL = `
INSERT INTO jobs (
    id, queue, name, payload, codec_name, priority, run_at, attempt,
    max_attempts, state, timeout_ns, locked_by, locked_until, last_error,
    recurring_id, created_at, updated_at
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(id) DO UPDATE SET
    queue=excluded.queue,
    name=excluded.name,
    payload=excluded.payload,
    codec_name=excluded.codec_name,
    priority=excluded.priority,
    run_at=excluded.run_at,
    attempt=excluded.attempt,
    max_attempts=excluded.max_attempts,
    state=excluded.state,
    timeout_ns=excluded.timeout_ns,
    locked_by=excluded.locked_by,
    locked_until=excluded.locked_until,
    last_error=excluded.last_error,
    recurring_id=excluded.recurring_id,
    updated_at=excluded.updated_at
`

func (s *Store) Save(ctx context.Context, j gs.Job) error {
	state := j.State
	if state == 0 {
		state = gs.StatePending
	}
	maxAttempts := j.MaxAttempts
	if maxAttempts == 0 {
		maxAttempts = 3
	}
	_, err := s.db.ExecContext(ctx, upsertJobSQL,
		string(j.ID), j.Queue, j.Name, j.Payload, j.CodecName,
		int(j.Priority), toUnixNano(j.RunAt), j.Attempt,
		maxAttempts, int(state), int64(j.Timeout), j.LockedBy,
		toUnixNano(j.LockedUntil), j.LastError, string(j.RecurringID),
		toUnixNano(j.CreatedAt), toUnixNano(j.UpdatedAt),
	)
	if err != nil {
		return fmt.Errorf("sqlite save: %w", err)
	}
	return nil
}

const claimSelectSQL = `
SELECT id, queue, name, payload, codec_name, priority, run_at, attempt,
       max_attempts, state, timeout_ns, locked_by, locked_until, last_error,
       recurring_id, created_at, updated_at
FROM jobs
WHERE queue = ? AND state = ? AND run_at <= ?
ORDER BY priority DESC, run_at ASC
LIMIT ?
`

const claimUpdateSQL = `
UPDATE jobs SET state = ?, locked_by = ?, locked_until = ?, updated_at = ?
WHERE id = ?
`

func (s *Store) ClaimDue(ctx context.Context, queue string, now time.Time, n int, workerID string, lockUntil time.Time) ([]gs.Job, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("sqlite claim begin: %w", err)
	}
	defer tx.Rollback() // safe to call after Commit

	rows, err := tx.QueryContext(ctx, claimSelectSQL, queue, int(gs.StatePending), toUnixNano(now), n)
	if err != nil {
		return nil, fmt.Errorf("sqlite claim select: %w", err)
	}
	var out []gs.Job
	for rows.Next() {
		j, err := scanJob(rows)
		if err != nil {
			rows.Close()
			return nil, err
		}
		out = append(out, j)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("sqlite claim iterate: %w", err)
	}
	rows.Close()

	for i := range out {
		out[i].State = gs.StateClaimed
		out[i].LockedBy = workerID
		out[i].LockedUntil = lockUntil
		out[i].UpdatedAt = now
		if _, err := tx.ExecContext(ctx, claimUpdateSQL,
			int(gs.StateClaimed), workerID, toUnixNano(lockUntil), toUnixNano(now), string(out[i].ID),
		); err != nil {
			return nil, fmt.Errorf("sqlite claim update: %w", err)
		}
	}
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("sqlite claim commit: %w", err)
	}
	return out, nil
}

// --- Ack ---

const ackSQL = `DELETE FROM jobs WHERE id = ? AND state = ?`

func (s *Store) Ack(ctx context.Context, id gs.JobID) error {
	res, err := s.db.ExecContext(ctx, ackSQL, string(id), int(gs.StateClaimed))
	if err != nil {
		return fmt.Errorf("sqlite ack: %w", err)
	}
	rows, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("sqlite ack rows: %w", err)
	}
	if rows == 0 {
		return gs.ErrJobNotFound
	}
	return nil
}

// --- Fail ---

const failSelectSQL = `SELECT attempt, max_attempts FROM jobs WHERE id = ? AND state = ?`
const failDeleteSQL = `DELETE FROM jobs WHERE id = ?`
const failRetrySQL = `
UPDATE jobs SET
    attempt = attempt + 1,
    last_error = ?,
    state = ?,
    run_at = ?,
    locked_by = '',
    locked_until = 0,
    updated_at = ?
WHERE id = ? AND state = ?
`

func (s *Store) Fail(ctx context.Context, id gs.JobID, errMsg string, nextAttemptAt time.Time) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("sqlite fail begin: %w", err)
	}
	defer tx.Rollback()

	var attempt, maxAttempts int
	row := tx.QueryRowContext(ctx, failSelectSQL, string(id), int(gs.StateClaimed))
	if err := row.Scan(&attempt, &maxAttempts); err != nil {
		if err == sql.ErrNoRows {
			return gs.ErrJobNotFound
		}
		return fmt.Errorf("sqlite fail select: %w", err)
	}
	nextAttempt := attempt + 1
	if maxAttempts > 0 && nextAttempt >= maxAttempts {
		if _, err := tx.ExecContext(ctx, failDeleteSQL, string(id)); err != nil {
			return fmt.Errorf("sqlite fail terminal: %w", err)
		}
	} else {
		if _, err := tx.ExecContext(ctx, failRetrySQL,
			errMsg, int(gs.StatePending), toUnixNano(nextAttemptAt),
			toUnixNano(time.Now()), string(id), int(gs.StateClaimed),
		); err != nil {
			return fmt.Errorf("sqlite fail retry: %w", err)
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("sqlite fail commit: %w", err)
	}
	return nil
}

// --- Cancel ---

const cancelSelectSQL = `SELECT state FROM jobs WHERE id = ?`
const cancelDeleteSQL = `DELETE FROM jobs WHERE id = ?`

func (s *Store) Cancel(ctx context.Context, id gs.JobID) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("sqlite cancel begin: %w", err)
	}
	defer tx.Rollback()
	var state int
	row := tx.QueryRowContext(ctx, cancelSelectSQL, string(id))
	if err := row.Scan(&state); err != nil {
		if err == sql.ErrNoRows {
			return gs.ErrJobNotFound
		}
		return fmt.Errorf("sqlite cancel select: %w", err)
	}
	if gs.State(state) != gs.StatePending {
		return gs.ErrJobNotPending
	}
	if _, err := tx.ExecContext(ctx, cancelDeleteSQL, string(id)); err != nil {
		return fmt.Errorf("sqlite cancel delete: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("sqlite cancel commit: %w", err)
	}
	return nil
}

// --- Reschedule ---

const rescheduleSelectSQL = `SELECT state FROM jobs WHERE id = ?`
const rescheduleUpdateSQL = `UPDATE jobs SET run_at = ?, updated_at = ? WHERE id = ?`

func (s *Store) Reschedule(ctx context.Context, id gs.JobID, newTime time.Time) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("sqlite reschedule begin: %w", err)
	}
	defer tx.Rollback()
	var state int
	row := tx.QueryRowContext(ctx, rescheduleSelectSQL, string(id))
	if err := row.Scan(&state); err != nil {
		if err == sql.ErrNoRows {
			return gs.ErrJobNotFound
		}
		return fmt.Errorf("sqlite reschedule select: %w", err)
	}
	if gs.State(state) != gs.StatePending {
		return gs.ErrJobNotPending
	}
	if _, err := tx.ExecContext(ctx, rescheduleUpdateSQL, toUnixNano(newTime), toUnixNano(time.Now()), string(id)); err != nil {
		return fmt.Errorf("sqlite reschedule update: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("sqlite reschedule commit: %w", err)
	}
	return nil
}

// --- Heartbeat / RecoverStale / QueueSize ---

func (s *Store) Heartbeat(_ context.Context, _ string, _ time.Time) error {
	return nil
}

const recoverStaleSQL = `
UPDATE jobs SET state = ?, locked_by = '', locked_until = 0, updated_at = ?
WHERE state = ? AND locked_until > 0 AND locked_until < ?
`

func (s *Store) RecoverStale(ctx context.Context, now time.Time) (int, error) {
	res, err := s.db.ExecContext(ctx, recoverStaleSQL,
		int(gs.StatePending), toUnixNano(now), int(gs.StateClaimed), toUnixNano(now),
	)
	if err != nil {
		return 0, fmt.Errorf("sqlite recover stale: %w", err)
	}
	rows, err := res.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("sqlite recover stale rows: %w", err)
	}
	return int(rows), nil
}

const queueSizeSQL = `SELECT COUNT(*) FROM jobs WHERE queue = ? AND state = ?`

func (s *Store) QueueSize(ctx context.Context, queue string) (int, error) {
	var n int
	row := s.db.QueryRowContext(ctx, queueSizeSQL, queue, int(gs.StatePending))
	if err := row.Scan(&n); err != nil {
		return 0, fmt.Errorf("sqlite queue size: %w", err)
	}
	return n, nil
}

// --- Recurring CRUD ---

const upsertRecurringSQL = `
INSERT INTO recurring (
    id, name, queue, payload, codec_name, priority, timeout_ns, max_attempts,
    cron, every_ns, catchup, next_run_at, last_fire_at, lease_until, leased_by
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(id) DO UPDATE SET
    name=excluded.name,
    queue=excluded.queue,
    payload=excluded.payload,
    codec_name=excluded.codec_name,
    priority=excluded.priority,
    timeout_ns=excluded.timeout_ns,
    max_attempts=excluded.max_attempts,
    cron=excluded.cron,
    every_ns=excluded.every_ns,
    catchup=excluded.catchup,
    next_run_at=excluded.next_run_at,
    last_fire_at=excluded.last_fire_at
`

const listRecurringSQL = `
SELECT id, name, queue, payload, codec_name, priority, timeout_ns, max_attempts,
       cron, every_ns, catchup, next_run_at, last_fire_at, lease_until, leased_by
FROM recurring
`

const deleteRecurringSQL = `DELETE FROM recurring WHERE id = ?`

const updateRecurringNextRunSQL = `
UPDATE recurring SET next_run_at = ?, last_fire_at = ? WHERE id = ?
`

func (s *Store) UpsertRecurring(ctx context.Context, spec gs.RecurringSpec) error {
	catch := 0
	if spec.Catchup {
		catch = 1
	}
	_, err := s.db.ExecContext(ctx, upsertRecurringSQL,
		string(spec.ID), spec.Name, spec.Queue, spec.Payload, spec.CodecName,
		int(spec.Priority), int64(spec.Timeout), spec.MaxAttempts,
		spec.Cron, int64(spec.Every), catch,
		toUnixNano(spec.NextRunAt), toUnixNano(spec.LastFireAt),
		toUnixNano(spec.LeaseUntil), spec.LeasedBy,
	)
	if err != nil {
		return fmt.Errorf("sqlite upsert recurring: %w", err)
	}
	return nil
}

func (s *Store) ListRecurring(ctx context.Context) ([]gs.RecurringSpec, error) {
	rows, err := s.db.QueryContext(ctx, listRecurringSQL)
	if err != nil {
		return nil, fmt.Errorf("sqlite list recurring: %w", err)
	}
	defer rows.Close()
	var out []gs.RecurringSpec
	for rows.Next() {
		var id, name, queue, codec, cron, leasedBy string
		var payload []byte
		var priority, maxAttempts, catch int
		var timeoutNs, everyNs, nextRunAt, lastFireAt, leaseUntil int64
		if err := rows.Scan(
			&id, &name, &queue, &payload, &codec, &priority, &timeoutNs, &maxAttempts,
			&cron, &everyNs, &catch, &nextRunAt, &lastFireAt, &leaseUntil, &leasedBy,
		); err != nil {
			return nil, fmt.Errorf("sqlite list recurring scan: %w", err)
		}
		out = append(out, gs.RecurringSpec{
			ID: gs.JobID(id), Name: name, Queue: queue, Payload: payload, CodecName: codec,
			Priority: gs.Priority(priority), Timeout: time.Duration(timeoutNs), MaxAttempts: maxAttempts,
			Cron: cron, Every: time.Duration(everyNs), Catchup: catch != 0,
			NextRunAt: fromUnixNano(nextRunAt), LastFireAt: fromUnixNano(lastFireAt),
			LeaseUntil: fromUnixNano(leaseUntil), LeasedBy: leasedBy,
		})
	}
	return out, rows.Err()
}

func (s *Store) DeleteRecurring(ctx context.Context, id gs.JobID) error {
	if _, err := s.db.ExecContext(ctx, deleteRecurringSQL, string(id)); err != nil {
		return fmt.Errorf("sqlite delete recurring: %w", err)
	}
	return nil
}

func (s *Store) UpdateRecurringNextRun(ctx context.Context, id gs.JobID, nextRunAt, lastFireAt time.Time) error {
	if _, err := s.db.ExecContext(ctx, updateRecurringNextRunSQL,
		toUnixNano(nextRunAt), toUnixNano(lastFireAt), string(id),
	); err != nil {
		return fmt.Errorf("sqlite update recurring next: %w", err)
	}
	return nil
}

// --- AcquireRecurringLease ---

const acquireLeaseSQL = `
UPDATE recurring SET lease_until = ?, leased_by = ?
WHERE id = ? AND (lease_until = 0 OR lease_until < ? OR leased_by = ?)
`

func (s *Store) AcquireRecurringLease(ctx context.Context, specID gs.JobID, leaseUntil time.Time, workerID string) (bool, error) {
	now := time.Now()
	res, err := s.db.ExecContext(ctx, acquireLeaseSQL,
		toUnixNano(leaseUntil), workerID,
		string(specID), toUnixNano(now), workerID,
	)
	if err != nil {
		return false, fmt.Errorf("sqlite acquire lease: %w", err)
	}
	rows, err := res.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("sqlite acquire lease rows: %w", err)
	}
	return rows > 0, nil
}

// scanJob reads a row matching the SELECT projection used by ClaimDue.
func scanJob(rows *sql.Rows) (gs.Job, error) {
	var j gs.Job
	var id, queue, name, codecName, lockedBy, lastError, recurringID string
	var payload []byte
	var priority, attempt, maxAttempts, state int
	var runAt, timeoutNs, lockedUntil, createdAt, updatedAt int64
	if err := rows.Scan(
		&id, &queue, &name, &payload, &codecName, &priority,
		&runAt, &attempt, &maxAttempts, &state, &timeoutNs,
		&lockedBy, &lockedUntil, &lastError, &recurringID,
		&createdAt, &updatedAt,
	); err != nil {
		return j, fmt.Errorf("sqlite scan job: %w", err)
	}
	j.ID = gs.JobID(id)
	j.Queue = queue
	j.Name = name
	j.Payload = payload
	j.CodecName = codecName
	j.Priority = gs.Priority(priority)
	j.RunAt = fromUnixNano(runAt)
	j.Attempt = attempt
	j.MaxAttempts = maxAttempts
	j.State = gs.State(state)
	j.Timeout = time.Duration(timeoutNs)
	j.LockedBy = lockedBy
	j.LockedUntil = fromUnixNano(lockedUntil)
	j.LastError = lastError
	j.RecurringID = gs.JobID(recurringID)
	j.CreatedAt = fromUnixNano(createdAt)
	j.UpdatedAt = fromUnixNano(updatedAt)
	return j, nil
}
