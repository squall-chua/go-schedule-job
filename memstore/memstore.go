package memstore

import (
	"container/heap"
	"context"
	"sort"
	"sync"
	"time"

	gs "github.com/squall-chua/go-schedule-job"
)

// Store is an in-memory implementation of goschedule.Store. Single-process; not durable.
// Heartbeat, RecoverStale, and AcquireRecurringLease are no-ops (always success).
type Store struct {
	mu        sync.Mutex
	byID      map[gs.JobID]*entry
	queues    map[string]*queueHeap
	recurring map[gs.JobID]gs.RecurringSpec
}

type entry struct {
	job   gs.Job
	index int // index in the queue heap, or -1 if not in the heap
}

// New constructs an empty in-memory Store.
func New() *Store {
	return &Store{
		byID:      map[gs.JobID]*entry{},
		queues:    map[string]*queueHeap{},
		recurring: map[gs.JobID]gs.RecurringSpec{},
	}
}

// --- Heap implementation: ordered by (-priority, runAt). ---

type queueHeap struct {
	items []*entry
}

func (q *queueHeap) Len() int { return len(q.items) }
func (q *queueHeap) Less(i, j int) bool {
	a, b := q.items[i].job, q.items[j].job
	if a.Priority != b.Priority {
		return a.Priority > b.Priority
	}
	return a.RunAt.Before(b.RunAt)
}
func (q *queueHeap) Swap(i, j int) {
	q.items[i], q.items[j] = q.items[j], q.items[i]
	q.items[i].index = i
	q.items[j].index = j
}
func (q *queueHeap) Push(x any) {
	e := x.(*entry)
	e.index = len(q.items)
	q.items = append(q.items, e)
}
func (q *queueHeap) Pop() any {
	n := len(q.items)
	e := q.items[n-1]
	q.items = q.items[:n-1]
	e.index = -1
	return e
}

func (s *Store) heap(queue string) *queueHeap {
	h, ok := s.queues[queue]
	if !ok {
		h = &queueHeap{}
		s.queues[queue] = h
	}
	return h
}

// --- Public methods ---

// Save inserts or updates a job by ID.
func (s *Store) Save(_ context.Context, j gs.Job) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if j.State == 0 {
		j.State = gs.StatePending
	}
	e := &entry{job: j, index: -1}
	if existing, ok := s.byID[j.ID]; ok {
		// Overwrite (treat as upsert). Remove from any heap.
		if existing.index >= 0 {
			heap.Remove(s.heap(existing.job.Queue), existing.index)
		}
	}
	s.byID[j.ID] = e
	if j.State == gs.StatePending {
		heap.Push(s.heap(j.Queue), e)
	}
	return nil
}

// ClaimDue pops up to n due jobs from the named queue, ordered by priority then run_at.
func (s *Store) ClaimDue(_ context.Context, queue string, now time.Time, n int, workerID string, lockUntil time.Time) ([]gs.Job, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	h, ok := s.queues[queue]
	if !ok {
		return nil, nil
	}
	out := make([]gs.Job, 0, n)
	for len(out) < n && h.Len() > 0 {
		top := h.items[0]
		if top.job.RunAt.After(now) {
			break
		}
		heap.Pop(h)
		top.job.State = gs.StateClaimed
		top.job.LockedBy = workerID
		top.job.LockedUntil = lockUntil
		top.job.UpdatedAt = now
		out = append(out, top.job)
	}
	// stable sort to keep deterministic order across equal priorities/runAt (already heap-ordered though).
	sort.SliceStable(out, func(i, j int) bool {
		if out[i].Priority != out[j].Priority {
			return out[i].Priority > out[j].Priority
		}
		return out[i].RunAt.Before(out[j].RunAt)
	})
	return out, nil
}

// Ack marks a claimed job as successfully completed.
func (s *Store) Ack(_ context.Context, id gs.JobID) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := s.byID[id]
	if !ok {
		return gs.ErrJobNotFound
	}
	if e.job.State != gs.StateClaimed {
		return gs.ErrJobNotFound
	}
	e.job.State = gs.StateSucceeded
	delete(s.byID, id)
	return nil
}

// Fail records an attempt failure. The job is re-queued for retry at nextAttemptAt.
func (s *Store) Fail(_ context.Context, id gs.JobID, errMsg string, nextAttemptAt time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := s.byID[id]
	if !ok {
		return gs.ErrJobNotFound
	}
	if e.job.State != gs.StateClaimed {
		return gs.ErrJobNotFound
	}
	e.job.Attempt++
	e.job.LastError = errMsg
	if e.job.MaxAttempts > 0 && e.job.Attempt >= e.job.MaxAttempts {
		e.job.State = gs.StateFailed
		delete(s.byID, id)
		return nil
	}
	e.job.State = gs.StatePending
	e.job.RunAt = nextAttemptAt
	e.job.LockedBy = ""
	e.job.LockedUntil = time.Time{}
	heap.Push(s.heap(e.job.Queue), e)
	return nil
}

// Cancel marks a pending job as cancelled. Returns ErrJobNotFound or ErrJobNotPending if not applicable.
func (s *Store) Cancel(_ context.Context, id gs.JobID) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := s.byID[id]
	if !ok {
		return gs.ErrJobNotFound
	}
	if e.job.State != gs.StatePending {
		return gs.ErrJobNotPending
	}
	if e.index >= 0 {
		heap.Remove(s.heap(e.job.Queue), e.index)
	}
	delete(s.byID, id)
	return nil
}

// Reschedule changes a pending job's run_at. Returns ErrJobNotFound or ErrJobNotPending if not applicable.
func (s *Store) Reschedule(_ context.Context, id gs.JobID, newTime time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := s.byID[id]
	if !ok {
		return gs.ErrJobNotFound
	}
	if e.job.State != gs.StatePending {
		return gs.ErrJobNotPending
	}
	e.job.RunAt = newTime
	if e.index >= 0 {
		heap.Fix(s.heap(e.job.Queue), e.index)
	}
	return nil
}

// UpsertRecurring inserts or replaces a recurring schedule.
func (s *Store) UpsertRecurring(_ context.Context, spec gs.RecurringSpec) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.recurring[spec.ID] = spec
	return nil
}

// ListRecurring returns all recurring schedules.
func (s *Store) ListRecurring(_ context.Context) ([]gs.RecurringSpec, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]gs.RecurringSpec, 0, len(s.recurring))
	for _, r := range s.recurring {
		out = append(out, r)
	}
	return out, nil
}

// DeleteRecurring removes a recurring schedule by ID.
func (s *Store) DeleteRecurring(_ context.Context, id gs.JobID) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.recurring, id)
	return nil
}

// AcquireRecurringLease is a no-op in the single-process memstore and always returns true.
func (s *Store) AcquireRecurringLease(_ context.Context, _ gs.JobID, _ time.Time, _ string) (bool, error) {
	return true, nil
}

// UpdateRecurringNextRun records the next firing time and last fire time on a recurring schedule.
func (s *Store) UpdateRecurringNextRun(_ context.Context, id gs.JobID, nextRunAt, lastFireAt time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if r, ok := s.recurring[id]; ok {
		r.NextRunAt = nextRunAt
		r.LastFireAt = lastFireAt
		s.recurring[id] = r
	}
	return nil
}

// Heartbeat is a no-op for the single-process memstore.
func (s *Store) Heartbeat(_ context.Context, _ string, _ time.Time) error { return nil }

// RecoverStale is a no-op for the single-process memstore.
func (s *Store) RecoverStale(_ context.Context, _ time.Time) (int, error) { return 0, nil }

// QueueSize returns the count of pending jobs in the named queue.
func (s *Store) QueueSize(_ context.Context, queue string) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	h, ok := s.queues[queue]
	if !ok {
		return 0, nil
	}
	return h.Len(), nil
}

var _ gs.Store = (*Store)(nil)
