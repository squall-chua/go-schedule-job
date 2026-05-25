package memstore

import (
	"container/heap"
	"context"
	"sort"
	"sync"
	"time"

	gs "github.com/squallchua/goschedule"
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
