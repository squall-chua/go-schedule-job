package prometheus

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"

	gs "github.com/squall-chua/go-schedule-job"
	"github.com/squall-chua/go-schedule-job/memstore"
)

func TestIntegration_RealSchedulerFlow(t *testing.T) {
	store := memstore.New()
	c := New(store, []string{"default"})

	sched, err := gs.NewScheduler(
		gs.WithStore(store),
		gs.WithQueues(map[string]int{"default": 2}),
		gs.WithHooks(c.Hooks()),
		gs.WithPollInterval(20*time.Millisecond),
	)
	if err != nil {
		t.Fatalf("NewScheduler: %v", err)
	}

	var succeeded, failed int32
	sched.Register("ok", func(context.Context, []byte) error {
		atomic.AddInt32(&succeeded, 1)
		return nil
	})
	sched.Register("boom", func(context.Context, []byte) error {
		atomic.AddInt32(&failed, 1)
		return errors.New("expected")
	})

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan struct{})
	go func() { _ = sched.Start(ctx); close(done) }()

	for i := 0; i < 5; i++ {
		if _, err := sched.Now("ok", nil); err != nil {
			t.Fatalf("dispatch ok: %v", err)
		}
	}
	if _, err := sched.Now("boom", nil, gs.WithMaxAttempts(1)); err != nil {
		t.Fatalf("dispatch boom: %v", err)
	}

	// Wait for jobs to drain.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if atomic.LoadInt32(&succeeded) == 5 && atomic.LoadInt32(&failed) == 1 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if atomic.LoadInt32(&succeeded) != 5 {
		t.Fatalf("succeeded = %d, want 5", atomic.LoadInt32(&succeeded))
	}
	if atomic.LoadInt32(&failed) != 1 {
		t.Fatalf("failed handler = %d, want 1", atomic.LoadInt32(&failed))
	}

	cancel()
	<-done

	// Register into a private registry and scrape.
	reg := prometheus.NewRegistry()
	reg.MustRegister(c)

	families, err := reg.Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}

	byName := map[string]*dto.MetricFamily{}
	for _, f := range families {
		byName[f.GetName()] = f
	}

	// Counters.
	if v := sumCounter(byName["goschedule_jobs_enqueued_total"]); v != 6 {
		t.Errorf("enqueued = %v, want 6", v)
	}
	if v := sumCounter(byName["goschedule_jobs_succeeded_total"]); v != 5 {
		t.Errorf("succeeded = %v, want 5", v)
	}
	if v := sumCounter(byName["goschedule_jobs_failed_total"]); v != 1 {
		t.Errorf("failed = %v, want 1", v)
	}

	// Histogram has at least 5 samples (one per success).
	if h := byName["goschedule_job_duration_seconds"]; h == nil {
		t.Error("missing job_duration_seconds")
	} else {
		var sum uint64
		for _, m := range h.GetMetric() {
			sum += m.GetHistogram().GetSampleCount()
		}
		if sum < 5 {
			t.Errorf("duration sample count = %d, want >= 5", sum)
		}
	}

	// in_flight balanced back to zero.
	if v := sumGauge(byName["goschedule_jobs_in_flight"]); v != 0 {
		t.Errorf("in_flight = %v, want 0", v)
	}

	// queue_size = 0 (everything drained).
	if v := sumGauge(byName["goschedule_queue_size"]); v != 0 {
		t.Errorf("queue_size = %v, want 0", v)
	}

	// Sanity: serialised output contains the namespace prefix.
	if !strings.Contains(renderFamilies(families), "goschedule_") {
		t.Error("rendered metrics missing goschedule_ namespace")
	}
}

func sumCounter(f *dto.MetricFamily) float64 {
	if f == nil {
		return 0
	}
	var sum float64
	for _, m := range f.GetMetric() {
		sum += m.GetCounter().GetValue()
	}
	return sum
}

func sumGauge(f *dto.MetricFamily) float64 {
	if f == nil {
		return 0
	}
	var sum float64
	for _, m := range f.GetMetric() {
		sum += m.GetGauge().GetValue()
	}
	return sum
}

func renderFamilies(families []*dto.MetricFamily) string {
	var b strings.Builder
	for _, f := range families {
		b.WriteString(f.GetName())
		b.WriteByte('\n')
	}
	return b.String()
}
