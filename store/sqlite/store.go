// Package sqlite provides a SQLite-backed Store for goschedule.
// Single-node only — uses SQLite's file-level locking; no cross-process
// distribution. Use store/postgres or store/redis for distributed workers.
package sqlite

import (
	"database/sql"
	"fmt"
	"time"

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
