// Package sqlite provides a SQLite-backed Store for goschedule.
// Single-node only — uses SQLite's file-level locking; no cross-process
// distribution. Use store/postgres or store/redis for distributed workers.
package sqlite

import (
	"database/sql"

	_ "modernc.org/sqlite" // pure-Go SQLite driver
)

// Store implements goschedule.Store backed by SQLite.
type Store struct {
	db *sql.DB
}
