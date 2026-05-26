package prometheus

import (
	"context"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
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
