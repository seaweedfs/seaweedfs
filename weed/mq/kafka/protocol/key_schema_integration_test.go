package protocol

import (
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/mq/kafka/schema"
)

// TestKeySchemaIntegration tests key schema functionality end-to-end
func TestKeySchemaIntegration(t *testing.T) {
	handler := NewHandlerForUnitTests()

	// Test case 1: Store and retrieve key schema configuration
	topic := "integration-test-topic"
	keySchemaID := uint32(12345)
	keySchemaFormat := schema.FormatAvro
	valueSchemaID := uint32(67890)
	valueSchemaFormat := schema.FormatProtobuf

	// Initially, no schema config should exist
	_, err := handler.getTopicSchemaConfig(topic)
	if err == nil {
		t.Error("Expected error when getting non-existent topic schema config")
	}

	// Store key schema
	err = handler.storeTopicKeySchemaConfig(topic, keySchemaID, keySchemaFormat)
	if err != nil {
		t.Fatalf("Failed to store key schema config: %v", err)
	}

	// Store value schema
	err = handler.storeTopicSchemaConfig(topic, valueSchemaID, valueSchemaFormat)
	if err != nil {
		t.Fatalf("Failed to store value schema config: %v", err)
	}

	// Retrieve and verify combined configuration
	config, err := handler.getTopicSchemaConfig(topic)
	if err != nil {
		t.Fatalf("Failed to get topic schema config: %v", err)
	}

	// Test key schema properties
	if !config.HasKeySchema {
		t.Error("Expected HasKeySchema to be true")
	}

	if config.KeySchemaID != keySchemaID {
		t.Errorf("Expected KeySchemaID %d, got %d", keySchemaID, config.KeySchemaID)
	}

	if config.KeySchemaFormat != keySchemaFormat {
		t.Errorf("Expected KeySchemaFormat %v, got %v", keySchemaFormat, config.KeySchemaFormat)
	}

	// Test value schema properties
	if config.ValueSchemaID != valueSchemaID {
		t.Errorf("Expected ValueSchemaID %d, got %d", valueSchemaID, config.ValueSchemaID)
	}

	if config.ValueSchemaFormat != valueSchemaFormat {
		t.Errorf("Expected ValueSchemaFormat %v, got %v", valueSchemaFormat, config.ValueSchemaFormat)
	}

	// Test legacy accessors for backward compatibility
	if config.SchemaID() != valueSchemaID {
		t.Errorf("Expected SchemaID() to return %d, got %d", valueSchemaID, config.SchemaID())
	}

	if config.SchemaFormat() != valueSchemaFormat {
		t.Errorf("Expected SchemaFormat() to return %v, got %v", valueSchemaFormat, config.SchemaFormat())
	}

	// Test case 2: hasTopicKeySchemaConfig method
	if !handler.hasTopicKeySchemaConfig(topic, keySchemaID, keySchemaFormat) {
		t.Error("Expected hasTopicKeySchemaConfig to return true for stored key schema")
	}

	if handler.hasTopicKeySchemaConfig(topic, keySchemaID+1, keySchemaFormat) {
		t.Error("Expected hasTopicKeySchemaConfig to return false for different schema ID")
	}

	if handler.hasTopicKeySchemaConfig(topic, keySchemaID, schema.FormatJSONSchema) {
		t.Error("Expected hasTopicKeySchemaConfig to return false for different schema format")
	}

	// Test case 3: Topic with only value schema (no key schema)
	valueOnlyTopic := "value-only-topic"

	err = handler.storeTopicSchemaConfig(valueOnlyTopic, valueSchemaID, valueSchemaFormat)
	if err != nil {
		t.Fatalf("Failed to store value-only schema config: %v", err)
	}

	valueOnlyConfig, err := handler.getTopicSchemaConfig(valueOnlyTopic)
	if err != nil {
		t.Fatalf("Failed to get value-only topic schema config: %v", err)
	}

	if valueOnlyConfig.HasKeySchema {
		t.Error("Expected HasKeySchema to be false for value-only topic")
	}

	if valueOnlyConfig.KeySchemaID != 0 {
		t.Errorf("Expected KeySchemaID to be 0 (unset), got %d", valueOnlyConfig.KeySchemaID)
	}

	if valueOnlyConfig.ValueSchemaID != valueSchemaID {
		t.Errorf("Expected ValueSchemaID %d, got %d", valueSchemaID, valueOnlyConfig.ValueSchemaID)
	}

	// Test case 4: Multiple topics with different schema configurations
	anotherTopic := "another-topic"
	anotherKeySchemaID := uint32(11111)
	anotherValueSchemaID := uint32(22222)

	err = handler.storeTopicKeySchemaConfig(anotherTopic, anotherKeySchemaID, schema.FormatJSONSchema)
	if err != nil {
		t.Fatalf("Failed to store another topic's key schema: %v", err)
	}

	err = handler.storeTopicSchemaConfig(anotherTopic, anotherValueSchemaID, schema.FormatJSONSchema)
	if err != nil {
		t.Fatalf("Failed to store another topic's value schema: %v", err)
	}

	// Verify first topic's configuration is unchanged
	originalConfig, err := handler.getTopicSchemaConfig(topic)
	if err != nil {
		t.Fatalf("Failed to get original topic schema config: %v", err)
	}

	if originalConfig.KeySchemaID != keySchemaID {
		t.Errorf("Original topic's KeySchemaID changed, expected %d, got %d", keySchemaID, originalConfig.KeySchemaID)
	}

	// Verify second topic's configuration
	anotherConfig, err := handler.getTopicSchemaConfig(anotherTopic)
	if err != nil {
		t.Fatalf("Failed to get another topic's schema config: %v", err)
	}

	if anotherConfig.KeySchemaID != anotherKeySchemaID {
		t.Errorf("Another topic's KeySchemaID incorrect, expected %d, got %d", anotherKeySchemaID, anotherConfig.KeySchemaID)
	}

	if anotherConfig.KeySchemaFormat != schema.FormatJSONSchema {
		t.Errorf("Another topic's KeySchemaFormat incorrect, expected %v, got %v", schema.FormatJSONSchema, anotherConfig.KeySchemaFormat)
	}
}

// TestTopicSchemaConfigConcurrency tests concurrent access to schema configurations
func TestTopicSchemaConfigConcurrency(t *testing.T) {
	handler := NewHandlerForUnitTests()

	topic := "concurrent-test-topic"
	numGoroutines := 10

	// Test concurrent writes and reads
	done := make(chan bool, numGoroutines*2)

	// Start writers
	for i := 0; i < numGoroutines; i++ {
		go func(id int) {
			defer func() { done <- true }()

			keySchemaID := uint32(1000 + id)
			valueSchemaID := uint32(2000 + id)

			// Store key schema
			err := handler.storeTopicKeySchemaConfig(topic, keySchemaID, schema.FormatAvro)
			if err != nil {
				t.Errorf("Goroutine %d: Failed to store key schema: %v", id, err)
				return
			}

			// Store value schema
			err = handler.storeTopicSchemaConfig(topic, valueSchemaID, schema.FormatProtobuf)
			if err != nil {
				t.Errorf("Goroutine %d: Failed to store value schema: %v", id, err)
				return
			}
		}(i)
	}

	// Start readers
	for i := 0; i < numGoroutines; i++ {
		go func(id int) {
			defer func() { done <- true }()

			// Try to read configuration
			_, err := handler.getTopicSchemaConfig(topic)
			// It's okay if config doesn't exist initially or if it changes during concurrent access
			if err != nil {
				// Expected in concurrent scenario
			}
		}(i)
	}

	// Wait for all goroutines to complete
	for i := 0; i < numGoroutines*2; i++ {
		<-done
	}

	// Final verification - should have some configuration
	config, err := handler.getTopicSchemaConfig(topic)
	if err != nil {
		t.Fatalf("Failed to get final topic schema config: %v", err)
	}

	if !config.HasKeySchema {
		t.Error("Expected final configuration to have key schema")
	}

	if config.KeySchemaID == 0 {
		t.Error("Expected final configuration to have non-zero key schema ID")
	}

	if config.ValueSchemaID == 0 {
		t.Error("Expected final configuration to have non-zero value schema ID")
	}
}
