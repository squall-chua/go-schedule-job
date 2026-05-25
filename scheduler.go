package goschedule

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"runtime"
	"sync"
	"time"

	"github.com/google/uuid"
)

// Scheduler is the top-level entry point.
type Scheduler struct {
	mu       sync.RWMutex
	handlers map[string]Handler

	// configuration
	store             Store
	queues            map[string]int
	workerID          string
	heartbeatInterval time.Duration
	visibilityTimeout time.Duration
	shutdownGrace     time.Duration
	hooks             Hooks
	logger            *slog.Logger
	clock             Clock
	defaultBackoff    BackoffStrategy
	pollInterval      time.Duration
}

// JobOption configures one dispatched job.
type JobOption func(*Job)

func WithQueue(name string) JobOption       { return func(j *Job) { j.Queue = name } }
func WithPriority(p Priority) JobOption     { return func(j *Job) { j.Priority = p } }
func WithMaxAttempts(n int) JobOption       { return func(j *Job) { j.MaxAttempts = n } }
func WithTimeout(d time.Duration) JobOption { return func(j *Job) { j.Timeout = d } }
func WithDelivery(d Delivery) JobOption     { return func(j *Job) { j.Delivery = d } }

// withCodec is used by typed dispatch helpers; not exported.
func withCodec(name string) JobOption { return func(j *Job) { j.CodecName = name } }

// SchedulerOption configures the scheduler at construction.
type SchedulerOption func(*Scheduler)

func WithStore(s Store) SchedulerOption { return func(sc *Scheduler) { sc.store = s } }
func WithQueues(m map[string]int) SchedulerOption {
	return func(sc *Scheduler) {
		sc.queues = map[string]int{}
		for k, v := range m {
			sc.queues[k] = v
		}
	}
}
func WithWorkerID(id string) SchedulerOption { return func(sc *Scheduler) { sc.workerID = id } }
func WithHeartbeatInterval(d time.Duration) SchedulerOption {
	return func(sc *Scheduler) { sc.heartbeatInterval = d }
}
func WithVisibilityTimeout(d time.Duration) SchedulerOption {
	return func(sc *Scheduler) { sc.visibilityTimeout = d }
}
func WithShutdownGrace(d time.Duration) SchedulerOption {
	return func(sc *Scheduler) { sc.shutdownGrace = d }
}
func WithHooks(h Hooks) SchedulerOption         { return func(sc *Scheduler) { sc.hooks = h } }
func WithLogger(l *slog.Logger) SchedulerOption { return func(sc *Scheduler) { sc.logger = l } }
func WithClock(c Clock) SchedulerOption         { return func(sc *Scheduler) { sc.clock = c } }
func WithDefaultBackoff(b BackoffStrategy) SchedulerOption {
	return func(sc *Scheduler) { sc.defaultBackoff = b }
}
func WithPollInterval(d time.Duration) SchedulerOption {
	return func(sc *Scheduler) { sc.pollInterval = d }
}

// NewScheduler constructs a scheduler with sensible defaults.
func NewScheduler(opts ...SchedulerOption) (*Scheduler, error) {
	s := &Scheduler{
		handlers:          map[string]Handler{},
		queues:            map[string]int{"default": runtime.NumCPU()},
		workerID:          uuid.NewString(),
		heartbeatInterval: 10 * time.Second,
		visibilityTimeout: 5 * time.Minute,
		shutdownGrace:     30 * time.Second,
		logger:            slog.Default(),
		clock:             RealClock(),
		defaultBackoff:    DefaultBackoff,
		pollInterval:      1 * time.Second,
	}
	for _, opt := range opts {
		opt(s)
	}
	if s.store == nil {
		return nil, errors.New("goschedule: WithStore is required (or use memstore.New() default)")
	}
	if len(s.queues) == 0 {
		return nil, errors.New("goschedule: at least one queue required")
	}
	return s, nil
}

// Register associates a handler with a name. Dispatch by that name routes here.
func (s *Scheduler) Register(name string, h Handler) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.handlers[name] = h
}

// IsRegistered reports whether a handler is registered for name.
func (s *Scheduler) IsRegistered(name string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	_, ok := s.handlers[name]
	return ok
}

func (s *Scheduler) handler(name string) (Handler, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	h, ok := s.handlers[name]
	return h, ok
}

// At schedules name+payload to run at t. opts override defaults.
func (s *Scheduler) At(t time.Time, name string, payload []byte, opts ...JobOption) (JobID, error) {
	if _, ok := s.handler(name); !ok {
		return "", fmt.Errorf("goschedule: no handler registered for %q", name)
	}
	j := Job{
		ID:          JobID(uuid.NewString()),
		Name:        name,
		Queue:       "default",
		Priority:    PriorityNormal,
		Payload:     payload,
		RunAt:       t,
		State:       StatePending,
		MaxAttempts: 3,
		CreatedAt:   s.clock.Now(),
	}
	for _, opt := range opts {
		opt(&j)
	}
	if _, ok := s.queues[j.Queue]; !ok {
		return "", fmt.Errorf("goschedule: unknown queue %q", j.Queue)
	}
	if err := s.store.Save(context.Background(), j); err != nil {
		return "", err
	}
	s.hooks.fireEnqueue(j.ID, j.Name, j.Queue)
	logEnqueue(s.logger, j.ID, j.Name, j.Queue, j.RunAt)
	return j.ID, nil
}

// Now schedules for immediate execution (next dispatch tick).
func (s *Scheduler) Now(name string, payload []byte, opts ...JobOption) (JobID, error) {
	return s.At(s.clock.Now(), name, payload, opts...)
}

// After schedules after a delay.
func (s *Scheduler) After(d time.Duration, name string, payload []byte, opts ...JobOption) (JobID, error) {
	return s.At(s.clock.Now().Add(d), name, payload, opts...)
}
