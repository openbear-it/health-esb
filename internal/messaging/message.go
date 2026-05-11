package messaging

import (
	"encoding/json"
	"fmt"

	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/google/uuid"
	"github.com/openbear-it/health-esb/internal/events"
)

// Publish serialises an Event and publishes it to the given topic.
func Publish(pub message.Publisher, topic string, evt *events.Event) error {
	payload, err := json.Marshal(evt)
	if err != nil {
		return fmt.Errorf("marshal event: %w", err)
	}

	msg := message.NewMessage(uuid.New().String(), payload)
	msg.Metadata.Set("correlation_id", evt.CorrelationID)
	msg.Metadata.Set("event_type", evt.Type)

	return pub.Publish(topic, msg)
}

// DecodeEvent decodes an Event from a Watermill message.
func DecodeEvent(msg *message.Message) (*events.Event, error) {
	var evt events.Event
	if err := json.Unmarshal(msg.Payload, &evt); err != nil {
		return nil, fmt.Errorf("unmarshal event: %w", err)
	}
	return &evt, nil
}

// ToMessage serialises an Event into a Watermill message ready for publishing.
func ToMessage(evt *events.Event) (*message.Message, error) {
	payload, err := json.Marshal(evt)
	if err != nil {
		return nil, fmt.Errorf("marshal event: %w", err)
	}
	msg := message.NewMessage(uuid.New().String(), payload)
	msg.Metadata.Set("correlation_id", evt.CorrelationID)
	msg.Metadata.Set("event_type", evt.Type)
	return msg, nil
}
