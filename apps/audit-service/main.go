package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/openbear-it/health-esb/internal/config"
	"github.com/openbear-it/health-esb/internal/events"
	"github.com/openbear-it/health-esb/internal/messaging"
	"github.com/openbear-it/health-esb/internal/observability"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

const serviceName = "audit-service"

// AuditRecord stores a persisted audit log entry (in-memory for demo).
type AuditRecord struct {
	EventID       string    `json:"event_id"`
	EventType     string    `json:"event_type"`
	CorrelationID string    `json:"correlation_id"`
	Source        string    `json:"source"`
	Timestamp     time.Time `json:"timestamp"`
	ReceivedAt    time.Time `json:"received_at"`
}

var (
	auditLog []AuditRecord
	auditMu  sync.RWMutex
)

func main() {
	cfg := config.Load(serviceName)
	logger := observability.NewLogger(cfg.ServiceName, cfg.LogLevel)
	slog.SetDefault(logger)

	wmLogger := watermill.NewSlogLogger(logger)
	metrics := observability.NewMetrics("audit_service")

	pub, err := messaging.NewPublisher(cfg.AMQPUrl, wmLogger)
	if err != nil {
		logger.Error("create publisher", "error", err)
		return
	}
	defer pub.Close()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	allTopics := []string{
		events.TopicCommandPatientAdmit,
		events.TopicPatientAdmitted,
		events.TopicPatientDischarged,
		events.TopicPatientTransferred,
		events.TopicLabResultCreated,
		events.TopicFHIRDocumentCreated,
		events.TopicNotificationSent,
		events.TopicAlertCreated,
	}

	var wg sync.WaitGroup
	for _, topic := range allTopics {
		sub, err := messaging.NewSubscriber(cfg.AMQPUrl, serviceName+"-"+topic, wmLogger)
		if err != nil {
			logger.Warn("create subscriber", "topic", topic, "error", err)
			continue
		}
		msgs, err := sub.Subscribe(ctx, topic)
		if err != nil {
			logger.Warn("subscribe", "topic", topic, "error", err)
			continue
		}
		wg.Add(1)
		go func(t string, ch <-chan *message.Message) {
			defer wg.Done()
			for msg := range ch {
				handleAudit(msg, t, metrics, logger)
			}
		}(topic, msgs)
	}

	// Expose HTTP endpoints for audit log + metrics
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	mux.HandleFunc("/audit", handleAuditHTTP)
	mux.HandleFunc("/health", func(w http.ResponseWriter, _ *http.Request) {
		fmt.Fprintln(w, `{"status":"ok"}`)
	})

	srv := &http.Server{Addr: fmt.Sprintf(":%d", cfg.Port), Handler: mux}
	go func() {
		logger.Info("audit-service HTTP", "port", cfg.Port)
		_ = srv.ListenAndServe()
	}()

	<-ctx.Done()
	shutCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = srv.Shutdown(shutCtx)
	wg.Wait()

	// Write audit log to stdout on exit
	if len(auditLog) > 0 {
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		_ = enc.Encode(auditLog)
	}
}

func handleAudit(msg *message.Message, topic string, m *observability.Metrics, logger *slog.Logger) {
	start := time.Now()
	evt, err := messaging.DecodeEvent(msg)
	if err != nil {
		m.MessagesFailedTotal.WithLabelValues(topic).Inc()
		msg.Nack()
		return
	}

	rec := AuditRecord{
		EventID:       evt.ID,
		EventType:     evt.Type,
		CorrelationID: evt.CorrelationID,
		Source:        evt.Source,
		Timestamp:     evt.Timestamp,
		ReceivedAt:    time.Now().UTC(),
	}

	auditMu.Lock()
	auditLog = append(auditLog, rec)
	auditMu.Unlock()

	m.MessagesProcessedTotal.WithLabelValues(topic).Inc()
	m.ProcessingDuration.WithLabelValues(topic).Observe(time.Since(start).Seconds())
	logger.Info("audit recorded",
		"event_type", evt.Type,
		"correlation_id", evt.CorrelationID,
		"source", evt.Source,
		"duration_ms", time.Since(start).Milliseconds(),
	)

	msg.Ack()
}

func handleAuditHTTP(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	auditMu.RLock()
	defer auditMu.RUnlock()
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	_ = enc.Encode(auditLog)
	
}
