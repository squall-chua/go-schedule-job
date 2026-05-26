package prometheus

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	gs "github.com/squall-chua/go-schedule-job"
)

func TestNewMetrics_AllVecsNonNil(t *testing.T) {
	m := newMetrics()
	if m.enqueued == nil {
		t.Fatal("enqueued nil")
	}
	if m.succeeded == nil {
		t.Fatal("succeeded nil")
	}
	if m.failed == nil {
		t.Fatal("failed nil")
	}
	if m.retried == nil {
		t.Fatal("retried nil")
	}
	if m.duration == nil {
		t.Fatal("duration nil")
	}
	if m.inFlight == nil {
		t.Fatal("inFlight nil")
	}
	if m.queueSize == nil {
		t.Fatal("queueSize nil")
	}

	// Sanity check labels by attempting a WithLabelValues.
	_ = m.enqueued.WithLabelValues("q", "n").Desc()
	_ = m.duration.WithLabelValues("q", "n")
	_ = m.inFlight.WithLabelValues("q").Desc()
	_ = m.queueSize.WithLabelValues("q").Desc()

	// Silence unused import.
	var _ prometheus.Collector = (*prometheus.CounterVec)(nil)
}

func TestNew_StoresArgs(t *testing.T) {
	fs := &fakeStore{}
	c := New(fs, []string{"a", "b"})
	if c == nil {
		t.Fatal("New returned nil")
	}
	if c.store != fs {
		t.Fatal("store not retained")
	}
	if len(c.queues) != 2 || c.queues[0] != "a" || c.queues[1] != "b" {
		t.Fatalf("queues = %v, want [a b]", c.queues)
	}
}

type fakeStore struct {
	queueSizes map[string]int
	queueErr   error
}

func (f *fakeStore) Save(context.Context, gs.Job) error                  { return nil }
func (f *fakeStore) ClaimDue(context.Context, string, time.Time, int, string, time.Time) ([]gs.Job, error) {
	return nil, nil
}
func (f *fakeStore) Ack(context.Context, gs.JobID) error                 { return nil }
func (f *fakeStore) Fail(context.Context, gs.JobID, string, time.Time) error { return nil }
func (f *fakeStore) Cancel(context.Context, gs.JobID) error              { return nil }
func (f *fakeStore) Reschedule(context.Context, gs.JobID, time.Time) error { return nil }
func (f *fakeStore) Heartbeat(context.Context, string, time.Time) error  { return nil }
func (f *fakeStore) RecoverStale(context.Context, time.Time) (int, error) { return 0, nil }
func (f *fakeStore) UpsertRecurring(context.Context, gs.RecurringSpec) error { return nil }
func (f *fakeStore) ListRecurring(context.Context) ([]gs.RecurringSpec, error) { return nil, nil }
func (f *fakeStore) DeleteRecurring(context.Context, gs.JobID) error     { return nil }
func (f *fakeStore) AcquireRecurringLease(context.Context, gs.JobID, time.Time, string) (bool, error) {
	return false, nil
}
func (f *fakeStore) UpdateRecurringNextRun(context.Context, gs.JobID, time.Time, time.Time) error {
	return nil
}
func (f *fakeStore) QueueSize(_ context.Context, q string) (int, error) {
	if f.queueErr != nil {
		return 0, f.queueErr
	}
	return f.queueSizes[q], nil
}

func TestHooks_OnEnqueueIncrements(t *testing.T) {
	c := New(&fakeStore{}, nil)
	h := c.Hooks()

	h.OnEnqueue("id-1", "send-email", "default")
	h.OnEnqueue("id-2", "send-email", "default")
	h.OnEnqueue("id-3", "send-email", "high")

	if got := testutil.ToFloat64(c.metrics.enqueued.WithLabelValues("default", "send-email")); got != 2 {
		t.Fatalf("default/send-email = %v, want 2", got)
	}
	if got := testutil.ToFloat64(c.metrics.enqueued.WithLabelValues("high", "send-email")); got != 1 {
		t.Fatalf("high/send-email = %v, want 1", got)
	}
}

func TestHooks_OnSuccess(t *testing.T) {
	c := New(&fakeStore{}, nil)
	h := c.Hooks()

	// Pre-fill in_flight so the decrement is observable.
	c.metrics.inFlight.WithLabelValues("default").Set(3)

	h.OnSuccess("id-1", "send-email", "default", 1, 50*time.Millisecond)
	h.OnSuccess("id-2", "send-email", "default", 2, 250*time.Millisecond)

	if got := testutil.ToFloat64(c.metrics.succeeded.WithLabelValues("default", "send-email")); got != 2 {
		t.Fatalf("succeeded = %v, want 2", got)
	}
	if got := testutil.ToFloat64(c.metrics.inFlight.WithLabelValues("default")); got != 1 {
		t.Fatalf("in_flight = %v, want 1 (3 - 2)", got)
	}

	// Histogram sample count via CollectAndCount.
	if n := testutil.CollectAndCount(c.metrics.duration); n != 1 {
		t.Fatalf("duration vec series = %d, want 1", n)
	}
}

func TestHooks_OnFailure(t *testing.T) {
	c := New(&fakeStore{}, nil)
	h := c.Hooks()
	c.metrics.inFlight.WithLabelValues("default").Set(2)

	h.OnFailure("id-1", "send-email", "default", 3, errors.New("boom"))

	if got := testutil.ToFloat64(c.metrics.failed.WithLabelValues("default", "send-email")); got != 1 {
		t.Fatalf("failed = %v, want 1", got)
	}
	if got := testutil.ToFloat64(c.metrics.inFlight.WithLabelValues("default")); got != 1 {
		t.Fatalf("in_flight = %v, want 1", got)
	}
}

func TestHooks_OnRetry(t *testing.T) {
	c := New(&fakeStore{}, nil)
	h := c.Hooks()
	c.metrics.inFlight.WithLabelValues("default").Set(1)

	h.OnRetry("id-1", "send-email", "default", 1, errors.New("retry"), time.Now().Add(time.Second))

	if got := testutil.ToFloat64(c.metrics.retried.WithLabelValues("default", "send-email")); got != 1 {
		t.Fatalf("retried = %v, want 1", got)
	}
	if got := testutil.ToFloat64(c.metrics.inFlight.WithLabelValues("default")); got != 0 {
		t.Fatalf("in_flight = %v, want 0", got)
	}
}
