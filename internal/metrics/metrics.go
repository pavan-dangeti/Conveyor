// Package metrics holds the Prometheus instruments shared by the API,
// maintenance leader and workers. Instruments live in the process that
// performs the action, so a fleet is observed by scraping every process.
package metrics

import (
	"context"
	"database/sql"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	Transitions = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "conveyor_transitions_total",
		Help: "Job state transitions applied, by event.",
	}, []string{"event"})

	TransitionSeconds = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "conveyor_transition_duration_seconds",
		Help:    "Latency of the SQL statement that applies a transition, by event.",
		Buckets: prometheus.ExponentialBuckets(0.0005, 2, 14),
	}, []string{"event"})

	StartLatency = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "conveyor_enqueue_to_start_seconds",
		Help:    "Time from a job becoming due (run_at) to a worker claiming it, measured on the Postgres clock.",
		Buckets: prometheus.ExponentialBuckets(0.001, 2, 16),
	})

	JobSeconds = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "conveyor_job_duration_seconds",
		Help:    "Handler execution time, by kind and outcome.",
		Buckets: prometheus.ExponentialBuckets(0.001, 2, 18),
	}, []string{"kind", "outcome"})

	LeaderChanges = promauto.NewCounter(prometheus.CounterOpts{
		Name: "conveyor_leader_changes_total",
		Help: "Times this process acquired or lost maintenance leadership.",
	})

	IsLeader = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "conveyor_is_leader",
		Help: "1 while this process holds the maintenance advisory lock.",
	})

	RPCRequests = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "conveyor_rpc_requests_total",
		Help: "API requests by method and gRPC status code.",
	}, []string{"method", "code"})

	RPCSeconds = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "conveyor_rpc_duration_seconds",
		Help:    "API request latency by method.",
		Buckets: prometheus.ExponentialBuckets(0.0005, 2, 14),
	}, []string{"method"})
)

// Observe records one transition statement that changed n rows.
func Observe(event string, start time.Time, n int64) {
	TransitionSeconds.WithLabelValues(event).Observe(time.Since(start).Seconds())
	if n > 0 {
		Transitions.WithLabelValues(event).Add(float64(n))
	}
}

// DBCollector exports gauges read from Postgres at scrape time: queue depth
// by state and the cumulative lease-expiration count. Reading at scrape time
// keeps them correct across many API replicas without coordination.
type DBCollector struct {
	db         *sql.DB
	depth      *prometheus.Desc
	expired    *prometheus.Desc
	leaseTotal *prometheus.Desc
}

func NewDBCollector(db *sql.DB) *DBCollector {
	return &DBCollector{
		db:         db,
		depth:      prometheus.NewDesc("conveyor_queue_depth", "Jobs by queue and state.", []string{"queue", "state"}, nil),
		expired:    prometheus.NewDesc("conveyor_running_expired_lease", "Running jobs whose lease has expired but not yet been reaped.", nil, nil),
		leaseTotal: prometheus.NewDesc("conveyor_lease_expirations", "Cumulative leases reaped by the maintenance leader.", nil, nil),
	}
}

func (c *DBCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.depth
	ch <- c.expired
	ch <- c.leaseTotal
}

func (c *DBCollector) Collect(ch chan<- prometheus.Metric) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	rows, err := c.db.QueryContext(ctx, `SELECT queue, state, count(*) FROM jobs GROUP BY queue, state`)
	if err == nil {
		for rows.Next() {
			var q, s string
			var n float64
			if rows.Scan(&q, &s, &n) == nil {
				ch <- prometheus.MustNewConstMetric(c.depth, prometheus.GaugeValue, n, q, s)
			}
		}
		_ = rows.Close()
	}
	var expired, total float64
	if c.db.QueryRowContext(ctx, `SELECT count(*) FROM jobs WHERE state='running' AND lease_expiry < now()`).Scan(&expired) == nil {
		ch <- prometheus.MustNewConstMetric(c.expired, prometheus.GaugeValue, expired)
	}
	if c.db.QueryRowContext(ctx, `SELECT value FROM counters WHERE name='lease_expirations'`).Scan(&total) == nil {
		ch <- prometheus.MustNewConstMetric(c.leaseTotal, prometheus.CounterValue, total)
	}
}
