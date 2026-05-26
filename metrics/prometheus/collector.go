package prometheus

import (
	"time"

	gs "github.com/squall-chua/go-schedule-job"
)

// Collector is a prometheus.Collector that mirrors a goschedule Scheduler.
//
// It carries no scheduler reference directly; instead the user wires Hooks()
// into NewScheduler(WithHooks(...)) and registers the Collector itself with
// their prometheus.Registry.
type Collector struct {
	store   gs.Store
	queues  []string
	metrics *metrics
}

// New returns a Collector backed by store. queues are the names whose
// queue_size gauge should be sampled at Collect time.
func New(store gs.Store, queues []string) *Collector {
	qs := make([]string, len(queues))
	copy(qs, queues)
	return &Collector{
		store:   store,
		queues:  qs,
		metrics: newMetrics(),
	}
}

// Hooks returns a goschedule.Hooks value that drives counter and gauge
// updates. Pass it to NewScheduler(WithHooks(...)).
func (c *Collector) Hooks() gs.Hooks {
	return gs.Hooks{
		OnEnqueue: func(_ gs.JobID, name, queue string) {
			c.metrics.enqueued.WithLabelValues(queue, name).Inc()
		},
		OnSuccess: func(_ gs.JobID, name, queue string, _ int, d time.Duration) {
			c.metrics.succeeded.WithLabelValues(queue, name).Inc()
			c.metrics.duration.WithLabelValues(queue, name).Observe(d.Seconds())
			c.metrics.inFlight.WithLabelValues(queue).Dec()
		},
	}
}
