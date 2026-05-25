package goschedule

import (
	"context"
	"errors"
	"time"
)

// ErrJobNotFound is returned by Cancel / Reschedule / Ack / Fail when the id is unknown.
var ErrJobNotFound = errors.New("goschedule: job not found")

// ErrJobNotPending is returned by Reschedule / Cancel when the job has already been claimed or finished.
var ErrJobNotPending = errors.New("goschedule: job no longer pending")

// Store is the persistence boundary. All access from Scheduler goes through it.
// Implementations must be safe for concurrent use.
type Store interface {
	// One-shot lifecycle.
	Save(ctx context.Context, j Job) error
	ClaimDue(ctx context.Context, queue string, now time.Time, n int, workerID string, lockUntil time.Time) ([]Job, error)
	Ack(ctx context.Context, id JobID) error
	Fail(ctx context.Context, id JobID, errMsg string, nextAttemptAt time.Time) error
	Cancel(ctx context.Context, id JobID) error
	Reschedule(ctx context.Context, id JobID, newTime time.Time) error

	// Distributed coordination. In-memory stores may treat these as no-ops.
	Heartbeat(ctx context.Context, workerID string, now time.Time) error
	RecoverStale(ctx context.Context, now time.Time) (recovered int, err error)

	// Recurring schedules.
	UpsertRecurring(ctx context.Context, spec RecurringSpec) error
	ListRecurring(ctx context.Context) ([]RecurringSpec, error)
	DeleteRecurring(ctx context.Context, specID JobID) error
	AcquireRecurringLease(ctx context.Context, specID JobID, leaseUntil time.Time, workerID string) (acquired bool, err error)
	UpdateRecurringNextRun(ctx context.Context, specID JobID, nextRunAt, lastFireAt time.Time) error

	// Observability.
	QueueSize(ctx context.Context, queue string) (int, error)
}
