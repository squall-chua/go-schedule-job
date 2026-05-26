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
	"github.com/jackc/pgx/v5/pgxpool"
)

// Store implements goschedule.Store backed by PostgreSQL.
type Store struct {
	pool *pgxpool.Pool
}
