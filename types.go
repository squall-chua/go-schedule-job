package goschedule

import (
	"context"
	"time"
)

// JobID uniquely identifies a job (or recurring schedule) within a Store.
type JobID string

// Priority controls relative ordering at claim time. Higher runs sooner.
type Priority uint8

const (
	PriorityLow      Priority = 0
	PriorityNormal   Priority = 1
	PriorityHigh     Priority = 2
	PriorityCritical Priority = 3
)

func (p Priority) String() string {
	switch p {
	case PriorityLow:
		return "low"
	case PriorityNormal:
		return "normal"
	case PriorityHigh:
		return "high"
	case PriorityCritical:
		return "critical"
	default:
		return "unknown"
	}
}

// State is a job's lifecycle stage.
type State uint8

const (
	StatePending State = iota
	StateClaimed
	StateSucceeded
	StateFailed
	StateCancelled
)

func (s State) Terminal() bool {
	return s == StateSucceeded || s == StateFailed || s == StateCancelled
}

// Delivery semantics.
type Delivery uint8

const (
	DeliveryAtLeastOnce Delivery = iota
	DeliveryAtMostOnce
)

// Handler executes a job. Returning a non-nil error triggers retry / failure handling.
type Handler func(ctx context.Context, payload []byte) error

// Job is the canonical record passed between Store and Scheduler.
type Job struct {
	ID            JobID
	Name          string
	Queue         string
	Priority      Priority
	Payload       []byte
	CodecName     string // empty when raw []byte
	RunAt         time.Time
	Attempt       int
	MaxAttempts   int
	State         State
	Delivery      Delivery
	Timeout       time.Duration // zero = no per-job timeout
	LockedBy      string
	LockedUntil   time.Time
	LastError     string
	RecurringID   JobID // empty for one-shot jobs; references RecurringSpec.ID
	CreatedAt     time.Time
	UpdatedAt     time.Time
}

// RecurringSpec is the canonical record for a recurring schedule.
type RecurringSpec struct {
	ID          JobID
	Name        string
	Queue       string
	Priority    Priority
	Payload     []byte
	CodecName   string
	Delivery    Delivery
	Timeout     time.Duration
	MaxAttempts int

	// Exactly one of Cron or Every must be set.
	Cron  string
	Every time.Duration

	Catchup    bool
	LeaseUntil time.Time
	LeasedBy   string
	NextRunAt  time.Time
	LastFireAt time.Time
}
