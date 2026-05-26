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

func TestHooks_OnStart_IncrementsInFlight(t *testing.T) {
	c := New(&fakeStore{}, nil)
	h := c.Hooks()

	h.OnStart("id-1", "send-email", "default", 1)
	h.OnStart("id-2", "send-email", "default", 1)
	h.OnStart("id-3", "send-email", "high", 1)

	if got := testutil.ToFloat64(c.metrics.inFlight.WithLabelValues("default")); got != 2 {
		t.Fatalf("default in_flight = %v, want 2", got)
	}
	if got := testutil.ToFloat64(c.metrics.inFlight.WithLabelValues("high")); got != 1 {
		t.Fatalf("high in_flight = %v, want 1", got)
	}
}

// Verify the full lifecycle balances back to zero.
func TestHooks_InFlight_BalancedLifecycle(t *testing.T) {
	c := New(&fakeStore{}, nil)
	h := c.Hooks()

	h.OnStart("id-1", "send-email", "q", 1)
	h.OnSuccess("id-1", "send-email", "q", 1, 5*time.Millisecond)

	h.OnStart("id-2", "send-email", "q", 1)
	h.OnRetry("id-2", "send-email", "q", 1, errors.New("x"), time.Now())

	h.OnStart("id-3", "send-email", "q", 1)
	h.OnFailure("id-3", "send-email", "q", 3, errors.New("x"))

	if got := testutil.ToFloat64(c.metrics.inFlight.WithLabelValues("q")); got != 0 {
		t.Fatalf("in_flight after balanced lifecycle = %v, want 0", got)
	}
}

func TestDescribe_EmitsAllDescriptors(t *testing.T) {
	c := New(&fakeStore{}, []string{"default"})

	descs := make(chan *prometheus.Desc, 32)
	go func() {
		c.Describe(descs)
		close(descs)
	}()

	var got []string
	for d := range descs {
		got = append(got, d.String())
	}

	// 7 distinct metric families.
	if len(got) < 7 {
		t.Fatalf("Describe emitted %d descs, want >= 7: %v", len(got), got)
	}

	// Sanity: each family appears.
	must := []string{
		"goschedule_jobs_enqueued_total",
		"goschedule_jobs_succeeded_total",
		"goschedule_jobs_failed_total",
		"goschedule_jobs_retried_total",
		"goschedule_job_duration_seconds",
		"goschedule_jobs_in_flight",
		"goschedule_queue_size",
	}
	for _, m := range must {
		found := false
		for _, d := range got {
			if containsName(d, m) {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("missing descriptor %q in %v", m, got)
		}
	}
}

func containsName(desc, name string) bool {
	return len(desc) > 0 && len(name) > 0 && (indexOf(desc, name) >= 0)
}

func indexOf(s, sub string) int {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return i
		}
	}
	return -1
}

func TestCollect_SamplesQueueSize(t *testing.T) {
	fs := &fakeStore{queueSizes: map[string]int{"default": 7, "email": 3}}
	c := New(fs, []string{"default", "email"})

	// Touch in_flight so it shows up.
	c.metrics.inFlight.WithLabelValues("default").Set(2)

	got := testutil.ToFloat64(c.metrics.queueSize.WithLabelValues("default"))
	if got != 0 {
		t.Fatalf("queue_size before Collect = %v, want 0", got)
	}

	metricsCh := make(chan prometheus.Metric, 64)
	go func() {
		c.Collect(metricsCh)
		close(metricsCh)
	}()
	for range metricsCh {
	}

	if v := testutil.ToFloat64(c.metrics.queueSize.WithLabelValues("default")); v != 7 {
		t.Fatalf("queue_size default = %v, want 7", v)
	}
	if v := testutil.ToFloat64(c.metrics.queueSize.WithLabelValues("email")); v != 3 {
		t.Fatalf("queue_size email = %v, want 3", v)
	}
}

func TestCollect_QueueSizeError_DoesNotPanic(t *testing.T) {
	fs := &fakeStore{queueErr: errors.New("db down")}
	c := New(fs, []string{"default"})

	metricsCh := make(chan prometheus.Metric, 32)
	go func() {
		defer close(metricsCh)
		c.Collect(metricsCh)
	}()
	for range metricsCh {
	}
	// queueSize stays at zero — no panic, no metric corruption.
	if v := testutil.ToFloat64(c.metrics.queueSize.WithLabelValues("default")); v != 0 {
		t.Fatalf("queue_size on error = %v, want 0", v)
	}
}
