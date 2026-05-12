package main

import (
	"context"
	"fmt"
	"log/slog"
	"os/signal"
	"syscall"
	"time"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/openbear-it/health-esb/internal/config"
	"github.com/openbear-it/health-esb/internal/events"
	"github.com/openbear-it/health-esb/internal/messaging"
	"github.com/openbear-it/health-esb/internal/observability"
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

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

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
