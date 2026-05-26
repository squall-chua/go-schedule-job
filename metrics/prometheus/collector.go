package prometheus

import (
	"context"
	"time"

	"github.com/prometheus/client_golang/prometheus"
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
		OnStart: func(_ gs.JobID, _, queue string, _ int) {
			c.metrics.inFlight.WithLabelValues(queue).Inc()
		},
		OnSuccess: func(_ gs.JobID, name, queue string, _ int, d time.Duration) {
			c.metrics.succeeded.WithLabelValues(queue, name).Inc()
			c.metrics.duration.WithLabelValues(queue, name).Observe(d.Seconds())
			c.metrics.inFlight.WithLabelValues(queue).Dec()
		},
		OnFailure: func(_ gs.JobID, name, queue string, _ int, _ error) {
			c.metrics.failed.WithLabelValues(queue, name).Inc()
			c.metrics.inFlight.WithLabelValues(queue).Dec()
		},
		OnRetry: func(_ gs.JobID, name, queue string, _ int, _ error, _ time.Time) {
			c.metrics.retried.WithLabelValues(queue, name).Inc()
			c.metrics.inFlight.WithLabelValues(queue).Dec()
		},
	}
}

// Describe implements prometheus.Collector. It emits the descriptors for every
// metric family the Collector owns.
func (c *Collector) Describe(ch chan<- *prometheus.Desc) {
	c.metrics.enqueued.Describe(ch)
	c.metrics.succeeded.Describe(ch)
	c.metrics.failed.Describe(ch)
	c.metrics.retried.Describe(ch)
	c.metrics.duration.Describe(ch)
	c.metrics.inFlight.Describe(ch)
	c.metrics.queueSize.Describe(ch)
}

// collectTimeout bounds the QueueSize call during a scrape. A scrape that
// blocks too long can stall Prometheus and cascade alerts.
const collectTimeout = 2 * time.Second

// Collect implements prometheus.Collector. It refreshes the queue_size gauge
// by calling Store.QueueSize for each configured queue, then emits every
// metric family.
func (c *Collector) Collect(ch chan<- prometheus.Metric) {
	ctx, cancel := context.WithTimeout(context.Background(), collectTimeout)
	defer cancel()

	for _, q := range c.queues {
		n, err := c.store.QueueSize(ctx, q)
		if err != nil {
			// Skip this queue; do not overwrite a previous good sample.
			continue
		}
		c.metrics.queueSize.WithLabelValues(q).Set(float64(n))
	}

	c.metrics.enqueued.Collect(ch)
	c.metrics.succeeded.Collect(ch)
	c.metrics.failed.Collect(ch)
	c.metrics.retried.Collect(ch)
	c.metrics.duration.Collect(ch)
	c.metrics.inFlight.Collect(ch)
	c.metrics.queueSize.Collect(ch)
}

// Compile-time check that Collector satisfies prometheus.Collector.
var _ prometheus.Collector = (*Collector)(nil)
