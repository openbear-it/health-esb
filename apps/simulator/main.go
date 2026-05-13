package main

import (
	"context"
	"fmt"
	"log/slog"
	"math/rand"
	"os/signal"
	"syscall"
	"time"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/google/uuid"
	"github.com/openbear-it/health-esb/internal/config"
	"github.com/openbear-it/health-esb/internal/events"
	"github.com/openbear-it/health-esb/internal/messaging"
	"github.com/openbear-it/health-esb/internal/observability"
)

const serviceName = "simulator"

var (
	firstNames = []string{"Alice", "Bob", "Carol", "David", "Eve", "Frank", "Grace", "Hank"}
	lastNames  = []string{"Smith", "Jones", "Brown", "Wilson", "Taylor", "Davis", "Clark", "Hall"}
	wards      = []string{"ICU", "Cardiology", "Oncology", "Pediatrics", "Emergency", "Surgery"}
)

func main() {
	cfg := config.Load(serviceName)
	logger := observability.NewLogger(cfg.ServiceName, cfg.LogLevel)
	slog.SetDefault(logger)

	wmLogger := watermill.NewSlogLogger(logger)

	pub, err := messaging.NewPublisher(cfg.AMQPUrl, wmLogger)
	if err != nil {
		logger.Error("create publisher", "error", err)
		return
	}
	defer pub.Close()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	interval := 2 * time.Second
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	logger.Info("simulator started", "interval", interval)

	for {
		select {
		case <-ctx.Done():
			logger.Info("simulator stopping")
			return
		case <-ticker.C:
			if err := sendAdmission(pub, logger); err != nil {
				logger.Error("send admission", "error", err)
			}
		}
	}
}

func sendAdmission(pub message.Publisher, logger *slog.Logger) error {
	correlationID := uuid.New().String()
	patientID := fmt.Sprintf("P%04d", rand.Intn(9999))

	evt, err := events.New(
		events.TopicCommandPatientAdmit,
		serviceName,
		correlationID,
		events.PatientAdmitPayload{
			PatientID:   patientID,
			FirstName:   firstNames[rand.Intn(len(firstNames))],
			LastName:    lastNames[rand.Intn(len(lastNames))],
			DateOfBirth: randomDOB(),
			Ward:        wards[rand.Intn(len(wards))],
		},
	)
	if err != nil {
		return fmt.Errorf("create event: %w", err)
	}

	if err := messaging.Publish(pub, events.TopicCommandPatientAdmit, evt); err != nil {
		return fmt.Errorf("publish: %w", err)
	}

	logger.Info("admission sent",
		"correlation_id", correlationID,
		"patient_id", patientID,
	)
	
	return nil
}

func randomDOB() string {
	year := 1940 + rand.Intn(65)
	month := 1 + rand.Intn(12)
	day := 1 + rand.Intn(28)
	return fmt.Sprintf("%04d-%02d-%02d", year, month, day)
}
