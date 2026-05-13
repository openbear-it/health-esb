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
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

const serviceName = "notification-service"

func main() {
	cfg := config.Load(serviceName)
	logger := observability.NewLogger(cfg.ServiceName, cfg.LogLevel)
	slog.SetDefault(logger)

	wmLogger := watermill.NewSlogLogger(logger)
	metrics := observability.NewMetrics("notification_service")

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

	messaging.AddPoisonQueue(router, pub, events.TopicNotificationSent)

	router.AddHandler(
		"notify-admitted",
		events.TopicPatientAdmitted,
		subAdmit,
		events.TopicNotificationSent,
		pub,
		handleAdmitNotification(metrics, logger),
	)

	router.AddHandler(
		"notify-lab-result",
		events.TopicLabResultCreated,
		subLab,
		events.TopicNotificationSent,
		pub,
		handleLabNotification(metrics, logger),
	)

	router.AddHandler(
		"notify-discharged",
		events.TopicPatientDischarged,
		subDischarge,
		events.TopicNotificationSent,
		pub,
		handleDischargeNotification(metrics, logger),
	)

	router.AddHandler(
		"notify-transferred",
		events.TopicPatientTransferred,
		subTransfer,
		events.TopicNotificationSent,
		pub,
		handleTransferNotification(metrics, logger),
	)

	router.AddHandler(
		"notify-alert",
		events.TopicAlertCreated,
		subAlert,
		events.TopicNotificationSent,
		pub,
		handleAlertNotification(metrics, logger),
	)

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

func handleAdmitNotification(m *observability.Metrics, logger *slog.Logger) message.HandlerFunc {
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

		return publishNotification(topic, serviceName, inEvt.CorrelationID, notification, m, logger, start)
	}
}

func handleLabNotification(m *observability.Metrics, logger *slog.Logger) message.HandlerFunc {
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

		return publishNotification(topic, serviceName, inEvt.CorrelationID, notification, m, logger, start)
	}
}

func handleDischargeNotification(m *observability.Metrics, logger *slog.Logger) message.HandlerFunc {
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

		return publishNotification(topic, serviceName, inEvt.CorrelationID, notification, m, logger, start)
	}
}

func handleTransferNotification(m *observability.Metrics, logger *slog.Logger) message.HandlerFunc {
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

		return publishNotification(topic, serviceName, inEvt.CorrelationID, notification, m, logger, start)
	}
}

func handleAlertNotification(m *observability.Metrics, logger *slog.Logger) message.HandlerFunc {
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

		return publishNotification(topic, serviceName, inEvt.CorrelationID, notification, m, logger, start)
	}
}

func publishNotification(topic, source, correlationID string, n events.NotificationPayload, m *observability.Metrics, logger *slog.Logger, start time.Time) ([]*message.Message, error) {
	outEvt, err := events.New(topic, source, correlationID, n)
	if err != nil {
		return nil, err
	}

	outMsg, err := messaging.ToMessage(outEvt)
	if err != nil {
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
