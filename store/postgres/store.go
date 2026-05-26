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
