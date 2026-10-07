package observability

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// Metrics encapsulates all Prometheus metrics for the Task Orchestrator.
type Metrics struct {
	Registry *prometheus.Registry

	JobsTotal     *prometheus.CounterVec
	JobDuration   *prometheus.HistogramVec
	ActiveWorkers prometheus.Gauge
	QueueDepth    prometheus.Gauge
}

// NewMetrics initializes and registers all Prometheus metrics using an isolated registry.
func NewMetrics(customReg *prometheus.Registry) *Metrics {
	reg := customReg
	if reg == nil {
		reg = prometheus.NewRegistry()
	}

	m := &Metrics{
		Registry: reg,

		JobsTotal: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "task_orchestrator_jobs_total",
				Help: "Total number of processed jobs partitioned by type and terminal status",
			},
			[]string{"type", "status"},
		),

		JobDuration: prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Name:    "task_orchestrator_job_duration_seconds",
				Help:    "Execution duration distribution of jobs in seconds partitioned by type",
				Buckets: []float64{0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0},
			},
			[]string{"type"},
		),

		ActiveWorkers: prometheus.NewGauge(
			prometheus.GaugeOpts{
				Name: "task_orchestrator_active_workers",
				Help: "Current number of worker goroutines actively processing a job",
			},
		),

		QueueDepth: prometheus.NewGauge(
			prometheus.GaugeOpts{
				Name: "task_orchestrator_queue_depth",
				Help: "Total number of tasks currently waiting in queue and Pending Entries List (PEL)",
			},
		),
	}

	reg.MustRegister(
		m.JobsTotal,
		m.JobDuration,
		m.ActiveWorkers,
		m.QueueDepth,
	)

	return m
}

// IncActiveWorkers increments the active workers gauge.
func (m *Metrics) IncActiveWorkers() {
	if m != nil && m.ActiveWorkers != nil {
		m.ActiveWorkers.Inc()
	}
}

// DecActiveWorkers decrements the active workers gauge.
func (m *Metrics) DecActiveWorkers() {
	if m != nil && m.ActiveWorkers != nil {
		m.ActiveWorkers.Dec()
	}
}

// RecordJobCompleted records a successful job execution and its duration.
func (m *Metrics) RecordJobCompleted(jobType string, duration time.Duration) {
	if m == nil {
		return
	}
	m.DecActiveWorkers()
	m.JobsTotal.WithLabelValues(jobType, "completed").Inc()
	m.JobDuration.WithLabelValues(jobType).Observe(duration.Seconds())
}

// RecordJobFailed records a failed job execution and its duration.
func (m *Metrics) RecordJobFailed(jobType string, duration time.Duration) {
	if m == nil {
		return
	}
	m.DecActiveWorkers()
	m.JobsTotal.WithLabelValues(jobType, "failed").Inc()
	m.JobDuration.WithLabelValues(jobType).Observe(duration.Seconds())
}

// RecordJobDLQ records a job moving to the Dead-Letter Queue.
func (m *Metrics) RecordJobDLQ(jobType string) {
	if m == nil {
		return
	}
	m.JobsTotal.WithLabelValues(jobType, "dlq").Inc()
}

// RecordJobRetry records a job scheduled for retry.
func (m *Metrics) RecordJobRetry(jobType string) {
	if m == nil {
		return
	}
	m.JobsTotal.WithLabelValues(jobType, "retry").Inc()
}

// SetQueueDepth updates the queue and PEL depth gauge.
func (m *Metrics) SetQueueDepth(depth float64) {
	if m == nil {
		return
	}
	m.QueueDepth.Set(depth)
}
