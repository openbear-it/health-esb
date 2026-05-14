package main

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os/signal"
	"syscall"
	"time"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/openbear-it/health-esb/internal/config"
	"github.com/openbear-it/health-esb/internal/events"
	"github.com/openbear-it/health-esb/internal/messaging"
	"github.com/openbear-it/health-esb/internal/observability"
	"github.com/openbear-it/health-esb/internal/resilience"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

const serviceName = "notification-service"

func main() {
	cfg := config.Load(serviceName)
	logger := observability.NewLogger(cfg.ServiceName, cfg.LogLevel)
	slog.SetDefault(logger)

	wmLogger := watermill.NewSlogLogger(logger)
	metrics := observability.NewMetrics("notification_service")

	// Circuit breaker for outbound notification sends (email/SMS adapter).
	breaker := resilience.NewBreaker(resilience.Config{
		Target:       "notification-outbound",
		Threshold:    5,
		ResetTimeout: 30 * time.Second,
		Gauge:        metrics.CircuitBreakerState,
	})

	pub, err := messaging.NewPublisher(cfg.AMQPUrl, wmLogger)
	if err != nil {
		logger.Error("create publisher", "error", err)
		return
	}
	defer pub.Close()

	subAdmit, err := messaging.NewSubscriber(cfg.AMQPUrl, serviceName+"-admit", wmLogger)
	if err != nil {
		logger.Error("create subscriber admit", "error", err)
		return
	}
	subLab, err := messaging.NewSubscriber(cfg.AMQPUrl, serviceName+"-lab", wmLogger)
	if err != nil {
		logger.Error("create subscriber lab", "error", err)
		return
	}
	subDischarge, err := messaging.NewSubscriber(cfg.AMQPUrl, serviceName+"-discharge", wmLogger)
	if err != nil {
		logger.Error("create subscriber discharge", "error", err)
		return
	}
	subTransfer, err := messaging.NewSubscriber(cfg.AMQPUrl, serviceName+"-transfer", wmLogger)
	if err != nil {
		logger.Error("create subscriber transfer", "error", err)
		return
	}
	subAlert, err := messaging.NewSubscriber(cfg.AMQPUrl, serviceName+"-alert", wmLogger)
	if err != nil {
		logger.Error("create subscriber alert", "error", err)
		return
	}

	router, err := messaging.NewRouter(messaging.RouterConfig{ServiceName: serviceName, Logger: wmLogger})
	if err != nil {
		logger.Error("create router", "error", err)
		return
	}

	for _, hSpec := range []struct {
		name string
		sub  message.Subscriber
		in   string
		fn   message.HandlerFunc
	}{
		{"notify-admitted", subAdmit, events.TopicPatientAdmitted, handleAdmitNotification(metrics, breaker, logger)},
		{"notify-lab-result", subLab, events.TopicLabResultCreated, handleLabNotification(metrics, breaker, logger)},
		{"notify-discharged", subDischarge, events.TopicPatientDischarged, handleDischargeNotification(metrics, breaker, logger)},
		{"notify-transferred", subTransfer, events.TopicPatientTransferred, handleTransferNotification(metrics, breaker, logger)},
		{"notify-alert", subAlert, events.TopicAlertCreated, handleAlertNotification(metrics, breaker, logger)},
	} {
		h := router.AddHandler(hSpec.name, hSpec.in, hSpec.sub, events.TopicNotificationSent, pub, hSpec.fn)
		messaging.AddPoisonQueue(h, pub, events.TopicNotificationSent)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	go func() {
		mux := http.NewServeMux()
		mux.Handle("/metrics", promhttp.Handler())
		mux.HandleFunc("/health", func(w http.ResponseWriter, _ *http.Request) {
			fmt.Fprintln(w, `{"status":"ok"}`)
		})
		addr := fmt.Sprintf(":%d", cfg.Port)
		logger.Info("metrics server listening", "addr", addr)
		if err := http.ListenAndServe(addr, mux); err != nil {
			logger.Error("metrics server", "error", err)
		}
	}()

	if err := router.Run(ctx); err != nil {
		logger.Error("router stopped", "error", err)
	}
}

func handleAdmitNotification(m *observability.Metrics, cb resilience.Breaker, logger *slog.Logger) message.HandlerFunc {
	return func(msg *message.Message) ([]*message.Message, error) {
		start := time.Now()
		topic := events.TopicNotificationSent

		inEvt, err := messaging.DecodeEvent(msg)
		if err != nil {
			m.MessagesFailedTotal.WithLabelValues(topic).Inc()
			return nil, err
		}

		payload, err := events.Decode[events.PatientAdmitPayload](inEvt)
		if err != nil {
			m.MessagesFailedTotal.WithLabelValues(topic).Inc()
			return nil, err
		}

		notification := events.NotificationPayload{
			PatientID: payload.PatientID,
			Channel:   "email",
			Message:   fmt.Sprintf("Patient %s %s has been admitted to ward %s.", payload.FirstName, payload.LastName, payload.Ward),
		}

		return publishNotification(msg.Context(), topic, serviceName, inEvt.CorrelationID, notification, m, cb, logger, start)
	}
}

func handleLabNotification(m *observability.Metrics, cb resilience.Breaker, logger *slog.Logger) message.HandlerFunc {
	return func(msg *message.Message) ([]*message.Message, error) {
		start := time.Now()
		topic := events.TopicNotificationSent

		inEvt, err := messaging.DecodeEvent(msg)
		if err != nil {
			m.MessagesFailedTotal.WithLabelValues(topic).Inc()
			return nil, err
		}

		lab, err := events.Decode[events.LabResultPayload](inEvt)
		if err != nil {
			m.MessagesFailedTotal.WithLabelValues(topic).Inc()
			return nil, err
		}

		if !lab.Abnormal {
			return nil, nil
		}

		notification := events.NotificationPayload{
			PatientID: lab.PatientID,
			Channel:   "sms",
			Message:   fmt.Sprintf("ALERT: Abnormal lab result for patient %s — %s: %.2f %s", lab.PatientID, lab.TestName, lab.Value, lab.Unit),
		}

		return publishNotification(msg.Context(), topic, serviceName, inEvt.CorrelationID, notification, m, cb, logger, start)
	}
}

func handleDischargeNotification(m *observability.Metrics, cb resilience.Breaker, logger *slog.Logger) message.HandlerFunc {
	return func(msg *message.Message) ([]*message.Message, error) {
		start := time.Now()
		topic := events.TopicNotificationSent

		inEvt, err := messaging.DecodeEvent(msg)
		if err != nil {
			m.MessagesFailedTotal.WithLabelValues(topic).Inc()
			return nil, err
		}

		payload, err := events.Decode[events.PatientDischargedPayload](inEvt)
		if err != nil {
			m.MessagesFailedTotal.WithLabelValues(topic).Inc()
			return nil, err
		}

		notification := events.NotificationPayload{
			PatientID: payload.PatientID,
			Channel:   "email",
			Message:   fmt.Sprintf("Patient %s %s has been discharged from ward %s. Reason: %s.", payload.FirstName, payload.LastName, payload.Ward, payload.Reason),
		}

		return publishNotification(msg.Context(), topic, serviceName, inEvt.CorrelationID, notification, m, cb, logger, start)
	}
}

func handleTransferNotification(m *observability.Metrics, cb resilience.Breaker, logger *slog.Logger) message.HandlerFunc {
	return func(msg *message.Message) ([]*message.Message, error) {
		start := time.Now()
		topic := events.TopicNotificationSent

		inEvt, err := messaging.DecodeEvent(msg)
		if err != nil {
			m.MessagesFailedTotal.WithLabelValues(topic).Inc()
			return nil, err
		}

		payload, err := events.Decode[events.PatientTransferPayload](inEvt)
		if err != nil {
			m.MessagesFailedTotal.WithLabelValues(topic).Inc()
			return nil, err
		}

		notification := events.NotificationPayload{
			PatientID: payload.PatientID,
			Channel:   "email",
			Message:   fmt.Sprintf("Patient %s %s transferred from %s to %s. Reason: %s.", payload.FirstName, payload.LastName, payload.FromWard, payload.ToWard, payload.Reason),
		}

		return publishNotification(msg.Context(), topic, serviceName, inEvt.CorrelationID, notification, m, cb, logger, start)
	}
}

func handleAlertNotification(m *observability.Metrics, cb resilience.Breaker, logger *slog.Logger) message.HandlerFunc {
	return func(msg *message.Message) ([]*message.Message, error) {
		start := time.Now()
		topic := events.TopicNotificationSent

		inEvt, err := messaging.DecodeEvent(msg)
		if err != nil {
			m.MessagesFailedTotal.WithLabelValues(topic).Inc()
			return nil, err
		}

		payload, err := events.Decode[events.AlertPayload](inEvt)
		if err != nil {
			m.MessagesFailedTotal.WithLabelValues(topic).Inc()
			return nil, err
		}

		channel := "sms"
		if payload.Severity == "low" || payload.Severity == "medium" {
			channel = "email"
		}

		notification := events.NotificationPayload{
			PatientID: payload.PatientID,
			Channel:   channel,
			Message:   fmt.Sprintf("[%s] Alert for patient %s: %s", payload.Severity, payload.PatientID, payload.Message),
		}

		return publishNotification(msg.Context(), topic, serviceName, inEvt.CorrelationID, notification, m, cb, logger, start)
	}
}

func publishNotification(ctx context.Context, topic, source, correlationID string, n events.NotificationPayload, m *observability.Metrics, cb resilience.Breaker, logger *slog.Logger, start time.Time) ([]*message.Message, error) {
	var outMsg *message.Message
	err := cb.Call(ctx, func() error {
		outEvt, err := events.New(topic, source, correlationID, n)
		if err != nil {
			return err
		}
		outMsg, err = messaging.ToMessage(outEvt)
		return err
	})
	if err != nil {
		m.MessagesFailedTotal.WithLabelValues(topic).Inc()
		return nil, err
	}

	m.MessagesProcessedTotal.WithLabelValues(topic).Inc()
	m.ProcessingDuration.WithLabelValues(topic).Observe(time.Since(start).Seconds())
	logger.Info("notification sent",
		"event_type", topic,
		"correlation_id", correlationID,
		"channel", n.Channel,
		"patient_id", n.PatientID,
		"duration_ms", time.Since(start).Milliseconds(),
	)

	return []*message.Message{outMsg}, nil
}
