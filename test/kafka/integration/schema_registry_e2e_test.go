package integration

import (
	"encoding/json"
	"fmt"
	"os"
	"testing"

	"github.com/seaweedfs/seaweedfs/test/kafka/internal/testutil"
	"github.com/seaweedfs/seaweedfs/weed/mq/kafka/schema"
	"github.com/seaweedfs/seaweedfs/weed/pb/schema_pb"
	"google.golang.org/protobuf/proto"
)

func TestSchemaRegistryE2E(t *testing.T) {
	// Skip if no schema registry URL is provided
	schemaRegistryURL := os.Getenv("SCHEMA_REGISTRY_URL")
	if schemaRegistryURL == "" {
		t.Skip("SCHEMA_REGISTRY_URL not set, skipping schema registry E2E test")
	}

	t.Logf("Running E2E test with Schema Registry: %s", schemaRegistryURL)

	// Set up gateway with schema management enabled
	gateway := testutil.NewGatewayTestServerWithSMQ(t, testutil.SMQRequired)
	defer gateway.Close()

	// Enable schema management
	err := gateway.GetHandler().EnableSchemaManagement(schema.ManagerConfig{
		RegistryURL: schemaRegistryURL,
	})
	testutil.AssertNoError(t, err, "Failed to enable schema management")

	topic := "schema-registry-e2e-topic"
	partition := int32(0)

	// Create topic
	err = gateway.GetHandler().GetSeaweedMQHandler().CreateTopic(topic, 1)
	testutil.AssertNoError(t, err, "Failed to create topic")

	// Test data
	testUser := map[string]interface{}{
		"name": "John Doe",
		"age":  30,
		"email": "john.doe@example.com",
	}

	// Step 1: Register schema in schema registry
	subject := topic + "-value"
	avroSchema := `{
		"type": "record",
		"name": "User",
		"fields": [
			{"name": "name", "type": "string"},
			{"name": "age", "type": "int"},
			{"name": "email", "type": "string"}
		]
	}`

	registryClient := schema.NewRegistryClient(schema.RegistryConfig{URL: schemaRegistryURL})
	schemaID, err := registryClient.RegisterSchema(subject, avroSchema)
	testutil.AssertNoError(t, err, "Failed to register schema")
	t.Logf("Registered schema with ID: %d", schemaID)

	// Step 2: Create Confluent-framed message
	testUserJSON, err := json.Marshal(testUser)
	testutil.AssertNoError(t, err, "Failed to marshal test user")

	confluentMessage := schema.CreateConfluentEnvelope(schema.FormatAvro, schemaID, nil, testUserJSON)
	t.Logf("Created Confluent message with schema ID %d, size: %d bytes", schemaID, len(confluentMessage))

	// Step 3: Produce schematized message
	handler := gateway.GetHandler()
	key := []byte("user-123")
	
	offset, err := handler.ProduceSchemaBasedRecord(topic, partition, key, confluentMessage)
	testutil.AssertNoError(t, err, "Failed to produce schematized record")
	t.Logf("Produced schematized message at offset: %d", offset)

	// Step 4: Verify message was stored as RecordValue
	smqRecords, err := handler.GetSeaweedMQHandler().GetStoredRecords(topic, partition, offset, 1)
	testutil.AssertNoError(t, err, "Failed to get stored records")

	if len(smqRecords) != 1 {
		t.Fatalf("Expected 1 record, got %d", len(smqRecords))
	}

	storedRecord := smqRecords[0]
	recordValue := &schema_pb.RecordValue{}
	err = proto.Unmarshal(storedRecord.GetValue(), recordValue)
	testutil.AssertNoError(t, err, "Stored record should be valid RecordValue")

	// Step 5: Verify RecordValue contains the decoded schema data
	if recordValue.Fields == nil {
		t.Fatal("RecordValue.Fields is nil")
	}

	if len(recordValue.Fields) == 0 {
		t.Fatal("RecordValue has no fields")
	}

	// Verify the schema-based fields are present
	nameField, hasName := recordValue.Fields["name"]
	if !hasName {
		t.Fatal("RecordValue missing 'name' field from schema")
	}
	if nameValue, ok := nameField.Kind.(*schema_pb.Value_StringValue); ok {
		if nameValue.StringValue != "John Doe" {
			t.Errorf("Expected name 'John Doe', got '%s'", nameValue.StringValue)
		}
	} else {
		t.Errorf("Name field is not StringValue: %T", nameField.Kind)
	}

	ageField, hasAge := recordValue.Fields["age"]
	if !hasAge {
		t.Fatal("RecordValue missing 'age' field from schema")
	}
	if ageValue, ok := ageField.Kind.(*schema_pb.Value_Int32Value); ok {
		if ageValue.Int32Value != 30 {
			t.Errorf("Expected age 30, got %d", ageValue.Int32Value)
		}
	} else {
		t.Errorf("Age field is not Int32Value: %T", ageField.Kind)
	}

	emailField, hasEmail := recordValue.Fields["email"]
	if !hasEmail {
		t.Fatal("RecordValue missing 'email' field from schema")
	}
	if emailValue, ok := emailField.Kind.(*schema_pb.Value_StringValue); ok {
		if emailValue.StringValue != "john.doe@example.com" {
			t.Errorf("Expected email 'john.doe@example.com', got '%s'", emailValue.StringValue)
		}
	} else {
		t.Errorf("Email field is not StringValue: %T", emailField.Kind)
	}

	t.Logf("✅ RecordValue correctly contains schema-based fields: name=%s, age=%d, email=%s",
		recordValue.Fields["name"].Kind.(*schema_pb.Value_StringValue).StringValue,
		recordValue.Fields["age"].Kind.(*schema_pb.Value_Int32Value).Int32Value,
		recordValue.Fields["email"].Kind.(*schema_pb.Value_StringValue).StringValue)

	// Step 6: Test fetch path - decode RecordValue back to Confluent format
	decodedValue := handler.DecodeRecordValueToKafkaMessage(topic, storedRecord.GetValue())
	if decodedValue == nil {
		t.Fatal("Decoded value is nil")
	}

	// Step 7: Verify the decoded message is a valid Confluent envelope
	envelope, isConfluent := schema.ParseConfluentEnvelope(decodedValue)
	if !isConfluent {
		t.Fatal("Decoded message is not a valid Confluent envelope")
	}

	if envelope.SchemaID != schemaID {
		t.Errorf("Expected schema ID %d, got %d", schemaID, envelope.SchemaID)
	}

	if envelope.Format != schema.FormatAvro {
		t.Errorf("Expected Avro format, got %v", envelope.Format)
	}

	t.Logf("✅ Successfully decoded RecordValue back to Confluent format with schema ID %d", envelope.SchemaID)

	// Step 8: Verify the payload can be decoded back to original data
	decodedUser := make(map[string]interface{})
	err = json.Unmarshal(envelope.Payload, &decodedUser)
	testutil.AssertNoError(t, err, "Failed to unmarshal decoded payload")

	if decodedUser["name"] != testUser["name"] {
		t.Errorf("Name mismatch: expected '%s', got '%s'", testUser["name"], decodedUser["name"])
	}

	// Age might be decoded as float64 from JSON
	if fmt.Sprintf("%.0f", decodedUser["age"]) != fmt.Sprintf("%d", testUser["age"]) {
		t.Errorf("Age mismatch: expected %v, got %v", testUser["age"], decodedUser["age"])
	}

	if decodedUser["email"] != testUser["email"] {
		t.Errorf("Email mismatch: expected '%s', got '%s'", testUser["email"], decodedUser["email"])
	}

	t.Logf("✅ Complete round-trip successful: Original → Confluent → RecordValue → Confluent → Original")
	t.Logf("✅ Schema-based message handling working correctly with real Schema Registry!")
}

func TestSchemaRegistryTopicConfiguration(t *testing.T) {
	// Test that schema configuration is stored per topic, not per message
	schemaRegistryURL := os.Getenv("SCHEMA_REGISTRY_URL")
	if schemaRegistryURL == "" {
		t.Skip("SCHEMA_REGISTRY_URL not set, skipping topic configuration test")
	}

	gateway := testutil.NewGatewayTestServerWithSMQ(t, testutil.SMQRequired)
	defer gateway.Close()

	err := gateway.GetHandler().EnableSchemaManagement(schema.ManagerConfig{
		RegistryURL: schemaRegistryURL,
	})
	testutil.AssertNoError(t, err, "Failed to enable schema management")

	// Create two topics with different schemas
	topic1 := "schema-topic-1"
	topic2 := "schema-topic-2"
	partition := int32(0)

	err = gateway.GetHandler().GetSeaweedMQHandler().CreateTopic(topic1, 1)
	testutil.AssertNoError(t, err, "Failed to create topic1")

	err = gateway.GetHandler().GetSeaweedMQHandler().CreateTopic(topic2, 1)
	testutil.AssertNoError(t, err, "Failed to create topic2")

	// Register different schemas for each topic
	registryClient := schema.NewRegistryClient(schema.RegistryConfig{URL: schemaRegistryURL})

	// Schema 1: User
	userSchema := `{
		"type": "record",
		"name": "User",
		"fields": [
			{"name": "id", "type": "int"},
			{"name": "name", "type": "string"}
		]
	}`
	schemaID1, err := registryClient.RegisterSchema(topic1+"-value", userSchema)
	testutil.AssertNoError(t, err, "Failed to register user schema")

	// Schema 2: Product
	productSchema := `{
		"type": "record",
		"name": "Product",
		"fields": [
			{"name": "sku", "type": "string"},
			{"name": "price", "type": "double"}
		]
	}`
	schemaID2, err := registryClient.RegisterSchema(topic2+"-value", productSchema)
	testutil.AssertNoError(t, err, "Failed to register product schema")

	// Produce messages to both topics
	handler := gateway.GetHandler()

	// Message 1: User
	userData := map[string]interface{}{"id": 123, "name": "Alice"}
	userJSON, _ := json.Marshal(userData)
	userMessage := schema.CreateConfluentEnvelope(schema.FormatAvro, schemaID1, nil, userJSON)

	offset1, err := handler.ProduceSchemaBasedRecord(topic1, partition, []byte("user-123"), userMessage)
	testutil.AssertNoError(t, err, "Failed to produce user message")

	// Message 2: Product
	productData := map[string]interface{}{"sku": "ABC123", "price": 29.99}
	productJSON, _ := json.Marshal(productData)
	productMessage := schema.CreateConfluentEnvelope(schema.FormatAvro, schemaID2, nil, productJSON)

	offset2, err := handler.ProduceSchemaBasedRecord(topic2, partition, []byte("product-abc"), productMessage)
	testutil.AssertNoError(t, err, "Failed to produce product message")

	// Verify both messages are stored correctly with their respective schemas
	// Topic 1 verification
	smqRecords1, err := handler.GetSeaweedMQHandler().GetStoredRecords(topic1, partition, offset1, 1)
	testutil.AssertNoError(t, err, "Failed to get stored records for topic1")

	recordValue1 := &schema_pb.RecordValue{}
	err = proto.Unmarshal(smqRecords1[0].GetValue(), recordValue1)
	testutil.AssertNoError(t, err, "Failed to unmarshal RecordValue for topic1")

	// Should have user schema fields
	if _, hasID := recordValue1.Fields["id"]; !hasID {
		t.Error("Topic1 RecordValue missing 'id' field from user schema")
	}
	if _, hasName := recordValue1.Fields["name"]; !hasName {
		t.Error("Topic1 RecordValue missing 'name' field from user schema")
	}

	// Topic 2 verification
	smqRecords2, err := handler.GetSeaweedMQHandler().GetStoredRecords(topic2, partition, offset2, 1)
	testutil.AssertNoError(t, err, "Failed to get stored records for topic2")

	recordValue2 := &schema_pb.RecordValue{}
	err = proto.Unmarshal(smqRecords2[0].GetValue(), recordValue2)
	testutil.AssertNoError(t, err, "Failed to unmarshal RecordValue for topic2")

	// Should have product schema fields
	if _, hasSKU := recordValue2.Fields["sku"]; !hasSKU {
		t.Error("Topic2 RecordValue missing 'sku' field from product schema")
	}
	if _, hasPrice := recordValue2.Fields["price"]; !hasPrice {
		t.Error("Topic2 RecordValue missing 'price' field from product schema")
	}

	t.Logf("✅ Topic-based schema configuration working correctly!")
	t.Logf("   Topic1 has user schema (id, name)")
	t.Logf("   Topic2 has product schema (sku, price)")
}
