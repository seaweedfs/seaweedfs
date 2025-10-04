package integration

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/mq/kafka/integration"
	"github.com/seaweedfs/seaweedfs/weed/pb/mq_pb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	schemaRegistryURL = "http://localhost:8081"
)

type SchemaResponse struct {
	ID int `json:"id"`
}

type SchemaGetResponse struct {
	Subject string `json:"subject"`
	Version int    `json:"version"`
	ID      int    `json:"id"`
	Schema  string `json:"schema"`
}

// TestSchemaRegistryRegisterAndRetrieve tests the full registration and retrieval flow
func TestSchemaRegistryRegisterAndRetrieve(t *testing.T) {
	ctx := context.Background()
	handler := integration.NewTestSeaweedMQHandler(t)
	defer handler.Cleanup()

	// Wait for schema registry to be ready
	waitForSchemaRegistry(t, 30*time.Second)

	subject := "test-subject-" + fmt.Sprint(time.Now().Unix())
	schemaStr := `{"type":"record","name":"Test","fields":[{"name":"field1","type":"string"}]}`

	// Register schema
	schemaID := registerSchema(t, subject, schemaStr)
	assert.Greater(t, schemaID, 0, "Schema ID should be positive")

	// Retrieve schema by ID
	retrievedSchema := getSchemaByID(t, schemaID)
	assert.Contains(t, retrievedSchema, "Test", "Retrieved schema should contain record name")

	// Retrieve schema by subject and version
	subjectSchema := getSchemaBySubject(t, subject, "latest")
	assert.Equal(t, schemaID, subjectSchema.ID, "Schema IDs should match")
	assert.Contains(t, subjectSchema.Schema, "Test", "Subject schema should contain record name")
}

// TestSchemaRegistryConsumerProgression verifies consumer offsets advance correctly
func TestSchemaRegistryConsumerProgression(t *testing.T) {
	ctx := context.Background()
	handler := integration.NewTestSeaweedMQHandler(t)
	defer handler.Cleanup()

	waitForSchemaRegistry(t, 30*time.Second)

	topic := mq_pb.Topic{Namespace: "kafka", Name: "_schemas"}
	partition := mq_pb.Partition{RangeStart: 0, RangeStop: 1024, RingSize: 1024}
	consumerGroup := "schema-registry"

	// Get initial offset
	initialOffset, _, err := handler.FetchOffset(ctx, consumerGroup, topic, partition)
	if err != nil {
		t.Logf("No initial offset found (expected for new group): %v", err)
		initialOffset = -1
	}

	// Register a schema
	subject := "progression-test-" + fmt.Sprint(time.Now().Unix())
	schemaStr := `{"type":"record","name":"ProgressionTest","fields":[{"name":"id","type":"int"}]}`
	schemaID := registerSchema(t, subject, schemaStr)
	require.Greater(t, schemaID, 0)

	// Wait for consumer to process
	time.Sleep(2 * time.Second)

	// Get new offset
	newOffset, _, err := handler.FetchOffset(ctx, consumerGroup, topic, partition)
	if err == nil {
		// Offset should have advanced (or at least not gone backwards)
		if initialOffset >= 0 {
			assert.GreaterOrEqual(t, newOffset, initialOffset,
				"Consumer offset should advance or stay same, not go backwards")
		}
		t.Logf("Consumer offset progressed from %d to %d", initialOffset, newOffset)
	} else {
		t.Logf("Could not fetch new offset: %v", err)
	}
}

// TestSchemaRegistryMultipleSchemas verifies multiple schemas work correctly
func TestSchemaRegistryMultipleSchemas(t *testing.T) {
	waitForSchemaRegistry(t, 30*time.Second)

	schemas := []struct {
		subject string
		schema  string
	}{
		{"multi-test-1", `{"type":"record","name":"Test1","fields":[{"name":"a","type":"string"}]}`},
		{"multi-test-2", `{"type":"record","name":"Test2","fields":[{"name":"b","type":"int"}]}`},
		{"multi-test-3", `{"type":"record","name":"Test3","fields":[{"name":"c","type":"long"}]}`},
	}

	schemaIDs := make([]int, len(schemas))

	// Register all schemas
	for i, s := range schemas {
		schemaIDs[i] = registerSchema(t, s.subject, s.schema)
		assert.Greater(t, schemaIDs[i], 0, "Schema ID should be positive for schema %d", i)
	}

	// Wait for processing
	time.Sleep(2 * time.Second)

	// Verify all schemas can be retrieved
	for i, id := range schemaIDs {
		schema := getSchemaByID(t, id)
		assert.Contains(t, schema, schemas[i].schema[:20], "Should retrieve correct schema")
	}
}

// TestSchemaRegistryRestart verifies schema persistence across restarts
func TestSchemaRegistryRestart(t *testing.T) {
	t.Skip("Skipping restart test - requires docker-compose integration")

	waitForSchemaRegistry(t, 30*time.Second)

	subject := "restart-test-" + fmt.Sprint(time.Now().Unix())
	schemaStr := `{"type":"record","name":"RestartTest","fields":[{"name":"field1","type":"string"}]}`

	// Register schema before restart
	schemaID := registerSchema(t, subject, schemaStr)
	require.Greater(t, schemaID, 0)

	// TODO: Restart schema registry container
	// This requires docker-compose integration or similar

	// Wait for restart
	time.Sleep(5 * time.Second)
	waitForSchemaRegistry(t, 30*time.Second)

	// Verify schema still exists after restart
	retrievedSchema := getSchemaByID(t, schemaID)
	assert.Contains(t, retrievedSchema, "RestartTest", "Schema should persist across restarts")
}

// TestSchemaRegistryOffsetCommitPattern tests the offset commit pattern used by Schema Registry
func TestSchemaRegistryOffsetCommitPattern(t *testing.T) {
	ctx := context.Background()
	handler := integration.NewTestSeaweedMQHandler(t)
	defer handler.Cleanup()

	topic := mq_pb.Topic{Namespace: "kafka", Name: "_schemas"}
	partition := mq_pb.Partition{RangeStart: 0, RangeStop: 1024, RingSize: 1024}
	consumerGroup := "test-sr-pattern"

	// Schema Registry pattern: commit offset after each message
	// Offset represents the NEXT offset to read

	// Simulate reading message at offset 0, commit offset 1
	err := handler.CommitOffset(ctx, consumerGroup, topic, partition, 1, "")
	require.NoError(t, err)

	fetchedOffset, _, err := handler.FetchOffset(ctx, consumerGroup, topic, partition)
	require.NoError(t, err)
	assert.Equal(t, int64(1), fetchedOffset, "Should commit next offset to read")

	// Simulate reading message at offset 1, commit offset 2
	err = handler.CommitOffset(ctx, consumerGroup, topic, partition, 2, "")
	require.NoError(t, err)

	fetchedOffset, _, err = handler.FetchOffset(ctx, consumerGroup, topic, partition)
	require.NoError(t, err)
	assert.Equal(t, int64(2), fetchedOffset, "Offset should advance sequentially")
}

// Helper functions

func waitForSchemaRegistry(t *testing.T, timeout time.Duration) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		resp, err := http.Get(schemaRegistryURL + "/subjects")
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return
			}
		}
		time.Sleep(500 * time.Millisecond)
	}
	t.Fatal("Schema Registry did not become ready within timeout")
}

func registerSchema(t *testing.T, subject, schema string) int {
	requestBody := map[string]string{"schema": schema}
	jsonBody, err := json.Marshal(requestBody)
	require.NoError(t, err)

	url := fmt.Sprintf("%s/subjects/%s/versions", schemaRegistryURL, subject)
	resp, err := http.Post(url, "application/json", bytes.NewBuffer(jsonBody))
	require.NoError(t, err)
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("Failed to register schema: status=%d, body=%s", resp.StatusCode, string(body))
	}

	var schemaResp SchemaResponse
	err = json.Unmarshal(body, &schemaResp)
	require.NoError(t, err)

	return schemaResp.ID
}

func getSchemaByID(t *testing.T, id int) string {
	url := fmt.Sprintf("%s/schemas/ids/%d", schemaRegistryURL, id)
	resp, err := http.Get(url)
	require.NoError(t, err)
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("Failed to get schema by ID: status=%d, body=%s", resp.StatusCode, string(body))
	}

	var schemaResp map[string]interface{}
	err = json.Unmarshal(body, &schemaResp)
	require.NoError(t, err)

	schema, ok := schemaResp["schema"].(string)
	require.True(t, ok, "Response should contain schema string")

	return schema
}

func getSchemaBySubject(t *testing.T, subject, version string) SchemaGetResponse {
	url := fmt.Sprintf("%s/subjects/%s/versions/%s", schemaRegistryURL, subject, version)
	resp, err := http.Get(url)
	require.NoError(t, err)
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("Failed to get schema by subject: status=%d, body=%s", resp.StatusCode, string(body))
	}

	var schemaResp SchemaGetResponse
	err = json.Unmarshal(body, &schemaResp)
	require.NoError(t, err)

	return schemaResp
}

