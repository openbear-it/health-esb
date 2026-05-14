package observability

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"time"

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

	// Phase 3 additions.
	MessageE2EDuration  *prometheus.HistogramVec
	QueueDepthTotal     *prometheus.GaugeVec
	CircuitBreakerState *prometheus.GaugeVec
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
		MessageE2EDuration: promauto.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "healthesb_message_e2e_duration_seconds",
			Help:    "End-to-end message duration from envelope timestamp to handler completion.",
			Buckets: prometheus.DefBuckets,
		}, []string{"source_service", "dest_service", "topic"}),
		QueueDepthTotal: promauto.NewGaugeVec(prometheus.GaugeOpts{
			Name: "healthesb_queue_depth_total",
			Help: "Current number of messages in each RabbitMQ queue.",
		}, []string{"queue_name"}),
		CircuitBreakerState: promauto.NewGaugeVec(prometheus.GaugeOpts{
			Name: "healthesb_circuit_breaker_state",
			Help: "Circuit breaker state (1 = active for that state). Labels: target, state (closed/open/half_open).",
		}, []string{"target", "state"}),
	}
}

// StartQueueDepthPoller starts a background goroutine that polls the RabbitMQ
// Management HTTP API every 15 seconds and updates the QueueDepthTotal gauge.
// mgmtURL should be the base URL, e.g. "http://guest:guest@localhost:15672".
// The goroutine exits when ctx is cancelled.
func (m *Metrics) StartQueueDepthPoller(ctx context.Context, mgmtURL string, logger *slog.Logger) {
	go func() {
		tick := time.NewTicker(15 * time.Second)
		defer tick.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-tick.C:
				if err := m.pollQueueDepths(mgmtURL); err != nil && logger != nil {
					logger.Warn("queue depth poll failed", "err", err)
				}
			}
		}
	}()
}

type rabbitQueue struct {
	Name     string `json:"name"`
	Messages int    `json:"messages"`
}

func (m *Metrics) pollQueueDepths(mgmtURL string) error {
	url := mgmtURL + "/api/queues"
	resp, err := http.Get(url) //nolint:gosec // URL comes from config, not user input
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}

	var queues []rabbitQueue
	if err := json.Unmarshal(body, &queues); err != nil {
		return err
	}

	for _, q := range queues {
		m.QueueDepthTotal.WithLabelValues(q.Name).Set(float64(q.Messages))
	}
	return nil
}
