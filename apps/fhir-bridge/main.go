package main

import (
	"context"
	"log/slog"
	"os/signal"
	"syscall"
	"time"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/openbear-it/health-esb/internal/config"
	"github.com/openbear-it/health-esb/internal/events"
	"github.com/openbear-it/health-esb/internal/fhir"
	"github.com/openbear-it/health-esb/internal/messaging"
	"github.com/openbear-it/health-esb/internal/observability"
)

const serviceName = "fhir-bridge"

func main() {
	cfg := config.Load(serviceName)
	logger := observability.NewLogger(cfg.ServiceName, cfg.LogLevel)
	slog.SetDefault(logger)

	wmLogger := watermill.NewSlogLogger(logger)
	metrics := observability.NewMetrics("fhir_bridge")

	pub, err := messaging.NewPublisher(cfg.AMQPUrl, wmLogger)
	if err != nil {
		logger.Error("create publisher", "error", err)
		return
	}
	defer pub.Close()

	sub, err := messaging.NewSubscriber(cfg.AMQPUrl, serviceName, wmLogger)
	if err != nil {
		logger.Error("create subscriber", "error", err)
		return
	}

	router, err := messaging.NewRouter(messaging.RouterConfig{ServiceName: serviceName, Logger: wmLogger})
	if err != nil {
		logger.Error("create router", "error", err)
		return
	}

	messaging.AddPoisonQueue(router, pub, events.TopicFHIRDocumentCreated)

	router.AddHandler(
		"fhir-handle-lab-result",
		events.TopicLabResultCreated,
		sub,
		events.TopicFHIRDocumentCreated,
		pub,
		handleLabResult(metrics, logger),
	)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	if err := router.Run(ctx); err != nil {
		logger.Error("router stopped", "error", err)
	}
}

func handleLabResult(m *observability.Metrics, logger *slog.Logger) message.HandlerFunc {
	return func(msg *message.Message) ([]*message.Message, error) {
		start := time.Now()
		topic := events.TopicFHIRDocumentCreated

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

		obs := fhir.FromLabResult(inEvt.CorrelationID, lab)
		doc, err := fhir.ToJSON(obs)
		if err != nil {
			return nil, err
		}

		outEvt, err := events.New(topic, serviceName, inEvt.CorrelationID, events.FHIRDocumentPayload{
			PatientID:    lab.PatientID,
			ResourceType: obs.ResourceType,
			Document:     doc,
		})
		if err != nil {
			return nil, err
		}

		outMsg, err := messaging.ToMessage(outEvt)
		if err != nil {
			return nil, err
		}

		m.MessagesProcessedTotal.WithLabelValues(topic).Inc()
		m.ProcessingDuration.WithLabelValues(topic).Observe(time.Since(start).Seconds())
		logger.Info("event processed",
			"event_type", topic,
			"correlation_id", inEvt.CorrelationID,
			"patient_id", lab.PatientID,
			"resource_type", obs.ResourceType,
			"duration_ms", time.Since(start).Milliseconds(),
		)

		return []*message.Message{outMsg}, nil
	}
}
