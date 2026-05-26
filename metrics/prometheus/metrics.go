package prometheus

import "github.com/prometheus/client_golang/prometheus"

const namespace = "goschedule"

type metrics struct {
	enqueued  *prometheus.CounterVec
	succeeded *prometheus.CounterVec
	failed    *prometheus.CounterVec
	retried   *prometheus.CounterVec
	duration  *prometheus.HistogramVec
	inFlight  *prometheus.GaugeVec
	queueSize *prometheus.GaugeVec
}

func newMetrics() *metrics {
	jobLabels := []string{"queue", "name"}
	queueLabels := []string{"queue"}

	return &metrics{
		enqueued: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "jobs_enqueued_total",
			Help:      "Total jobs enqueued, labelled by queue and handler name.",
		}, jobLabels),
		succeeded: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "jobs_succeeded_total",
			Help:      "Total jobs that completed successfully.",
		}, jobLabels),
		failed: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "jobs_failed_total",
			Help:      "Total jobs that exhausted retries.",
		}, jobLabels),
		retried: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "jobs_retried_total",
			Help:      "Total job attempts that failed and were scheduled for retry.",
		}, jobLabels),
		duration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: namespace,
			Name:      "job_duration_seconds",
			Help:      "Duration of successful job runs, in seconds.",
			Buckets:   prometheus.ExponentialBuckets(0.001, 2, 14),
		}, jobLabels),
		inFlight: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "jobs_in_flight",
			Help:      "Current number of jobs being executed per queue.",
		}, queueLabels),
		queueSize: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "queue_size",
			Help:      "Current number of pending jobs in each queue, sampled at scrape time.",
		}, queueLabels),
	}
}
