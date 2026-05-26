package prometheus

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
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
