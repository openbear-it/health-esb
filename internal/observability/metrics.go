package observability

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Metrics holds Prometheus metrics shared across services.
type Metrics struct {
	MessagesProcessedTotal *prometheus.CounterVec
	MessagesFailedTotal    *prometheus.CounterVec
	ProcessingDuration     *prometheus.HistogramVec
	RetryTotal             *prometheus.CounterVec
	DLQTotal               *prometheus.CounterVec
}

// NewMetrics registers and returns the standard set of Prometheus metrics.
func NewMetrics(serviceName string) *Metrics {
	labels := []string{"topic"}
	return &Metrics{
		MessagesProcessedTotal: promauto.NewCounterVec(prometheus.CounterOpts{
			Namespace: "healthesb",
			Subsystem: serviceName,
			Name:      "messages_processed_total",
			Help:      "Total number of messages processed.",
		}, labels),
		MessagesFailedTotal: promauto.NewCounterVec(prometheus.CounterOpts{
			Namespace: "healthesb",
			Subsystem: serviceName,
			Name:      "messages_failed_total",
			Help:      "Total number of messages that failed processing.",
		}, labels),
		ProcessingDuration: promauto.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: "healthesb",
			Subsystem: serviceName,
			Name:      "message_processing_duration_seconds",
			Help:      "Histogram of message processing duration.",
			Buckets:   prometheus.DefBuckets,
		}, labels),
		RetryTotal: promauto.NewCounterVec(prometheus.CounterOpts{
			Namespace: "healthesb",
			Subsystem: serviceName,
			Name:      "retry_total",
			Help:      "Total number of message retries.",
		}, labels),
		DLQTotal: promauto.NewCounterVec(prometheus.CounterOpts{
			Namespace: "healthesb",
			Subsystem: serviceName,
			Name:      "dlq_total",
			Help:      "Total number of messages routed to the dead-letter queue.",
		}, labels),
	}
}
