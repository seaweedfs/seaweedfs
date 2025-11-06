package testutil

import (
	"fmt"
	"os"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/mq/kafka/schema"
	"github.com/segmentio/kafka-go"
)

// MessageGenerator provides utilities for generating test messages
type MessageGenerator struct {
	counter int
}

// NewMessageGenerator creates a new message generator
func NewMessageGenerator() *MessageGenerator {
	return &MessageGenerator{counter: 0}
}

// GenerateKafkaGoMessages generates kafka-go messages for testing
func (m *MessageGenerator) GenerateKafkaGoMessages(count int) []kafka.Message {
	messages := make([]kafka.Message, count)

	for i := 0; i < count; i++ {
		m.counter++
		key := []byte(fmt.Sprintf("test-key-%d", m.counter))
		val := []byte(fmt.Sprintf("{\"value\":\"test-message-%d-generated-at-%d\"}", m.counter, time.Now().Unix()))

		// If schema mode is requested, ensure a test schema exists and wrap with Confluent envelope
		if url := os.Getenv("SCHEMA_REGISTRY_URL"); url != "" {
			subject := "offset-management-value"
			schemaJSON := `{"type":"record","name":"TestRecord","fields":[{"name":"value","type":"string"}]}`
			rc := schema.NewRegistryClient(schema.RegistryConfig{URL: url})
			if _, err := rc.GetLatestSchema(subject); err != nil {
				// Best-effort register schema
				_, _ = rc.RegisterSchema(subject, schemaJSON)
			}
			if latest, err := rc.GetLatestSchema(subject); err == nil {
				val = schema.CreateConfluentEnvelope(schema.FormatAvro, latest.LatestID, nil, val)
			} else {
				// fallback to schema id 1
				val = schema.CreateConfluentEnvelope(schema.FormatAvro, 1, nil, val)
			}
		}

		messages[i] = kafka.Message{Key: key, Value: val}
	}

	return messages
}

// GenerateStringMessages generates string messages for Sarama
func (m *MessageGenerator) GenerateStringMessages(count int) []string {
	messages := make([]string, count)

	for i := 0; i < count; i++ {
		m.counter++
		messages[i] = fmt.Sprintf("test-message-%d-generated-at-%d", m.counter, time.Now().Unix())
	}

	return messages
}

// GenerateKafkaGoMessage generates a single kafka-go message
func (m *MessageGenerator) GenerateKafkaGoMessage(key, value string) kafka.Message {
	if key == "" {
		m.counter++
		key = fmt.Sprintf("test-key-%d", m.counter)
	}
	if value == "" {
		value = fmt.Sprintf("test-message-%d-generated-at-%d", m.counter, time.Now().Unix())
	}

	return kafka.Message{
		Key:   []byte(key),
		Value: []byte(value),
	}
}

// GenerateUniqueTopicName generates a unique topic name for testing
func GenerateUniqueTopicName(prefix string) string {
	if prefix == "" {
		prefix = "test-topic"
	}
	return fmt.Sprintf("%s-%d", prefix, time.Now().UnixNano())
}

// GenerateUniqueGroupID generates a unique consumer group ID for testing
func GenerateUniqueGroupID(prefix string) string {
	if prefix == "" {
		prefix = "test-group"
	}
	return fmt.Sprintf("%s-%d", prefix, time.Now().UnixNano())
}

// ValidateMessageContent validates that consumed messages match expected content
func ValidateMessageContent(expected, actual []string) error {
	if len(expected) != len(actual) {
		return fmt.Errorf("message count mismatch: expected %d, got %d", len(expected), len(actual))
	}

	for i, expectedMsg := range expected {
		if i >= len(actual) {
			return fmt.Errorf("missing message at index %d", i)
		}
		if actual[i] != expectedMsg {
			return fmt.Errorf("message mismatch at index %d: expected %q, got %q", i, expectedMsg, actual[i])
		}
	}

	return nil
}

// ValidateKafkaGoMessageContent validates kafka-go messages
func ValidateKafkaGoMessageContent(expected, actual []kafka.Message) error {
	if len(expected) != len(actual) {
		return fmt.Errorf("message count mismatch: expected %d, got %d", len(expected), len(actual))
	}

	for i, expectedMsg := range expected {
		if i >= len(actual) {
			return fmt.Errorf("missing message at index %d", i)
		}
		if string(actual[i].Key) != string(expectedMsg.Key) {
			return fmt.Errorf("key mismatch at index %d: expected %q, got %q", i, string(expectedMsg.Key), string(actual[i].Key))
		}
		if string(actual[i].Value) != string(expectedMsg.Value) {
			return fmt.Errorf("value mismatch at index %d: expected %q, got %q", i, string(expectedMsg.Value), string(actual[i].Value))
		}
	}

	return nil
}
