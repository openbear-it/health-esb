package main

import (
	"context"
	"fmt"
	"log/slog"
	"math/rand"
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

const serviceName = "lab-service"

func main() {
	cfg := config.Load(serviceName)
	logger := observability.NewLogger(cfg.ServiceName, cfg.LogLevel)
	slog.SetDefault(logger)

	wmLogger := watermill.NewSlogLogger(logger)
	metrics := observability.NewMetrics("lab_service")

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

	messaging.AddPoisonQueue(router, pub, events.TopicLabResultCreated)

	router.AddHandler(
		"lab-handle-admitted",
		events.TopicPatientAdmitted,
		sub,
		events.TopicLabResultCreated,
		pub,
		handlePatientAdmitted(metrics, logger),
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

var labTests = []struct {
	name string
	lo   float64
	hi   float64
	unit string
}{
	{"Hemoglobin", 12.0, 17.5, "g/dL"},
	{"White Blood Cells", 4.5, 11.0, "10^3/uL"},
	{"Platelets", 150, 400, "10^3/uL"},
	{"Glucose", 70, 100, "mg/dL"},
	{"Creatinine", 0.6, 1.2, "mg/dL"},
}

func handlePatientAdmitted(m *observability.Metrics, logger *slog.Logger) message.HandlerFunc {
	return func(msg *message.Message) ([]*message.Message, error) {
		start := time.Now()
		topic := events.TopicLabResultCreated

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

		// Simulate lab processing delay
		time.Sleep(time.Duration(50+rand.Intn(200)) * time.Millisecond)

		test := labTests[rand.Intn(len(labTests))]
		value := test.lo + rand.Float64()*(test.hi-test.lo)*1.3
		abnormal := value < test.lo || value > test.hi

		labPayload := events.LabResultPayload{
			PatientID:   payload.PatientID,
			TestName:    test.name,
			Value:       round(value, 2),
			Unit:        test.unit,
			ReferenceLo: test.lo,
			ReferenceHi: test.hi,
			Abnormal:    abnormal,
		}

		outEvt, err := events.New(topic, serviceName, inEvt.CorrelationID, labPayload)
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
			"patient_id", payload.PatientID,
			"test", test.name,
			"value", fmt.Sprintf("%.2f %s", labPayload.Value, test.unit),
			"abnormal", abnormal,
			"duration_ms", time.Since(start).Milliseconds(),
		)

		return []*message.Message{outMsg}, nil
	}
}

func round(val float64, precision int) float64 {
	ratio := 1.0
	for i := 0; i < precision; i++ {
		ratio *= 10
	}
	return float64(int(val*ratio+0.5)) / ratio
}
