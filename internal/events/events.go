package events

import (
	"encoding/json"
	"time"
)

// Event is the canonical event model for the health-esb platform.
type Event struct {
	ID            string          `json:"id"`
	Type          string          `json:"type"`
	CorrelationID string          `json:"correlation_id"`
	Timestamp     time.Time       `json:"timestamp"`
	Source        string          `json:"source"`
	Payload       json.RawMessage `json:"payload"`
}

// Topic constants follow dot-notation naming convention.
const (
	TopicCommandPatientAdmit  = "command.patient.admit"
	TopicPatientAdmitted      = "patient.admitted"
	TopicPatientUpdated       = "patient.updated"
	TopicLabResultCreated     = "lab.result.created"
	TopicLabResultValidated   = "lab.result.validated"
	TopicFHIRDocumentCreated  = "fhir.document.created"
	TopicNotificationSent     = "notification.sent"
	TopicAuditEvent           = "audit.event"
)

// DLQTopic returns the dead-letter queue topic for a given topic.
func DLQTopic(topic string) string {
	return topic + ".dlq"
}
