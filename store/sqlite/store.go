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
