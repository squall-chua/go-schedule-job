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

// WithQueue routes the job to the named queue. Defaults to "default".
func WithQueue(name string) JobOption { return func(j *Job) { j.Queue = name } }

// WithPriority sets the job's claim-time priority. Higher priorities are claimed first.
func WithPriority(p Priority) JobOption { return func(j *Job) { j.Priority = p } }

// WithMaxAttempts caps how many times the job will be tried before terminal failure. Defaults to 3.
func WithMaxAttempts(n int) JobOption { return func(j *Job) { j.MaxAttempts = n } }

// WithTimeout sets a per-attempt timeout for the handler context. Zero means no timeout.
func WithTimeout(d time.Duration) JobOption { return func(j *Job) { j.Timeout = d } }

// withCodec is used by typed dispatch helpers; not exported.
func withCodec(name string) JobOption { return func(j *Job) { j.CodecName = name } }

// WithCatchup enables catching up missed fires for recurring jobs. Default is false.
func WithCatchup(c bool) JobOption { return func(j *Job) { j.catchup = c } }

// SchedulerOption configures the scheduler at construction.
type SchedulerOption func(*Scheduler)

// WithStore sets the persistence backend. Required when no default is supplied.
func WithStore(s Store) SchedulerOption { return func(sc *Scheduler) { sc.store = s } }

// WithQueues configures named queues and their worker concurrency.
func WithQueues(m map[string]int) SchedulerOption {
	return func(sc *Scheduler) {
		sc.queues = map[string]int{}
		for k, v := range m {
			sc.queues[k] = v
		}
	}
}

// WithWorkerID overrides the per-process worker identifier (default: random UUID).
func WithWorkerID(id string) SchedulerOption { return func(sc *Scheduler) { sc.workerID = id } }

// WithHeartbeatInterval sets how often the janitor heartbeats and runs stale-claim recovery.
func WithHeartbeatInterval(d time.Duration) SchedulerOption {
	return func(sc *Scheduler) { sc.heartbeatInterval = d }
}

// WithVisibilityTimeout sets how long a claimed job is hidden before another scheduler can recover it.
func WithVisibilityTimeout(d time.Duration) SchedulerOption {
	return func(sc *Scheduler) { sc.visibilityTimeout = d }
}

// WithShutdownGrace bounds how long Start blocks after ctx is cancelled while waiting for workers.
func WithShutdownGrace(d time.Duration) SchedulerOption {
	return func(sc *Scheduler) { sc.shutdownGrace = d }
}

// WithHooks installs lifecycle hooks (OnEnqueue/OnStart/OnSuccess/OnFailure/OnRetry).
func WithHooks(h Hooks) SchedulerOption { return func(sc *Scheduler) { sc.hooks = h } }

// WithLogger sets the structured logger used for scheduler events.
func WithLogger(l *slog.Logger) SchedulerOption { return func(sc *Scheduler) { sc.logger = l } }

// WithClock overrides the time source — useful in tests with a fake clock.
func WithClock(c Clock) SchedulerOption { return func(sc *Scheduler) { sc.clock = c } }

// WithDefaultBackoff overrides the retry backoff strategy used when a handler returns an error.
func WithDefaultBackoff(b BackoffStrategy) SchedulerOption {
	return func(sc *Scheduler) { sc.defaultBackoff = b }
}

// WithPollInterval sets the dispatcher tick — the upper bound on dispatch latency between Save and run.
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

// Cancel marks a pending job as cancelled. Returns ErrJobNotFound or ErrJobNotPending if not applicable.
func (s *Scheduler) Cancel(id JobID) error {
	return s.store.Cancel(context.Background(), id)
}

// Reschedule moves a pending job's RunAt to newTime. Returns ErrJobNotFound or ErrJobNotPending if not applicable.
func (s *Scheduler) Reschedule(id JobID, newTime time.Time) error {
	return s.store.Reschedule(context.Background(), id, newTime)
}

// Start boots dispatchers, worker pools, and the janitor. Blocks until ctx is cancelled.
func (s *Scheduler) Start(ctx context.Context) error {
	var wg sync.WaitGroup

	// Per-queue worker channels.
	channels := map[string]chan Job{}
	for queue, concurrency := range s.queues {
		ch := make(chan Job, concurrency)
		channels[queue] = ch

		d := &dispatcher{
			queue:      queue,
			batchSize:  concurrency,
			pollEvery:  s.pollInterval,
			visibility: s.visibilityTimeout,
			workerID:   s.workerID,
			store:      s.store,
			clock:      s.clock,
			logger:     s.logger,
			out:        ch,
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			d.run(ctx)
		}()

		wp := &workerPool{
			concurrency: concurrency,
			in:          ch,
			store:       s.store,
			scheduler:   s,
			clock:       s.clock,
			logger:      s.logger,
		}
		wp.run(ctx) // spawns internal goroutines; they exit on ctx.Done
	}

	_ = channels

	runner := &recurringRunner{
		store:      s.store,
		clock:      s.clock,
		logger:     s.logger,
		workerID:   s.workerID,
		leaseEvery: s.pollInterval,
	}
	wg.Add(1)
	go func() { defer wg.Done(); runner.run(ctx) }()

	jan := &janitor{store: s.store, clock: s.clock, logger: s.logger, workerID: s.workerID, every: s.heartbeatInterval}
	wg.Add(1)
	go func() { defer wg.Done(); jan.run(ctx) }()

	// Wait for ctx cancellation, then grace shutdown.
	<-ctx.Done()
	doneCh := make(chan struct{})
	go func() { wg.Wait(); close(doneCh) }()
	select {
	case <-doneCh:
	case <-time.After(s.shutdownGrace):
	}
	return nil
}
