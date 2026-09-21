package listwatch

import (
	"sync"

	"github.com/prometheus/client_golang/prometheus"
)

type metrics struct {
	watchFailures        prometheus.Counter
	listLatency          prometheus.Histogram
	watchLatency         prometheus.Histogram
	eventProcessed       prometheus.Counter
	retryCount           prometheus.Counter
	eventsByType         *prometheus.CounterVec
	connectionState      prometheus.Gauge
	watchSessionDuration prometheus.Histogram
	errorsByType         *prometheus.CounterVec
}

var (
	defaultMetrics *metrics
	metricsOnce    sync.Once
)

func newMetrics() *metrics {
	metricsOnce.Do(func() {
		defaultMetrics = &metrics{
			watchFailures: prometheus.NewCounter(prometheus.CounterOpts{
				Name:        "listwatch_watch_failures_total",
				Help:        "Total number of watch operation failures",
				ConstLabels: prometheus.Labels{"component": "listwatch"},
			}),
			listLatency: prometheus.NewHistogram(prometheus.HistogramOpts{
				Name:        "listwatch_list_duration_seconds",
				Help:        "Duration of list operations in seconds",
				ConstLabels: prometheus.Labels{"component": "listwatch"},
				Buckets:     prometheus.DefBuckets,
			}),
			watchLatency: prometheus.NewHistogram(prometheus.HistogramOpts{
				Name:        "listwatch_watch_event_duration_seconds",
				Help:        "Duration of watch event processing in seconds",
				ConstLabels: prometheus.Labels{"component": "listwatch"},
				Buckets:     prometheus.DefBuckets,
			}),
			eventProcessed: prometheus.NewCounter(prometheus.CounterOpts{
				Name:        "listwatch_events_processed_total",
				Help:        "Total number of events processed",
				ConstLabels: prometheus.Labels{"component": "listwatch"},
			}),
			retryCount: prometheus.NewCounter(prometheus.CounterOpts{
				Name:        "listwatch_retry_attempts_total",
				Help:        "Total number of retry attempts",
				ConstLabels: prometheus.Labels{"component": "listwatch"},
			}),
			eventsByType: prometheus.NewCounterVec(
				prometheus.CounterOpts{
					Name:        "listwatch_events_by_type_total",
					Help:        "Total number of events by type (add/modify/delete)",
					ConstLabels: prometheus.Labels{"component": "listwatch"},
				},
				[]string{"event_type"},
			),
			connectionState: prometheus.NewGauge(prometheus.GaugeOpts{
				Name:        "listwatch_connection_state",
				Help:        "Current connection state (1=connected, 0=disconnected)",
				ConstLabels: prometheus.Labels{"component": "listwatch"},
			}),
			watchSessionDuration: prometheus.NewHistogram(prometheus.HistogramOpts{
				Name:        "listwatch_watch_session_duration_seconds",
				Help:        "Duration of watch sessions in seconds",
				ConstLabels: prometheus.Labels{"component": "listwatch"},
				Buckets:     prometheus.DefBuckets,
			}),
			errorsByType: prometheus.NewCounterVec(
				prometheus.CounterOpts{
					Name:        "listwatch_errors_by_type_total",
					Help:        "Total number of errors by type",
					ConstLabels: prometheus.Labels{"component": "listwatch"},
				},
				[]string{"error_type"},
			),
		}

		// Register metrics only once
		prometheus.MustRegister(
			defaultMetrics.watchFailures,
			defaultMetrics.listLatency,
			defaultMetrics.watchLatency,
			defaultMetrics.eventProcessed,
			defaultMetrics.retryCount,
			defaultMetrics.eventsByType,
			defaultMetrics.connectionState,
			defaultMetrics.watchSessionDuration,
			defaultMetrics.errorsByType,
		)
	})

	return defaultMetrics
}
