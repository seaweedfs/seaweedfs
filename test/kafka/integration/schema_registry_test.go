package integration

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/test/kafka/internal/testutil"
)

// TestSchemaRegistryEventualConsistency reproduces the issue where schemas
// are registered successfully but are not immediately queryable due to
// Schema Registry's consumer lag
func TestSchemaRegistryEventualConsistency(t *testing.T) {
	// This test requires real SMQ backend
	gateway := testutil.NewGatewayTestServerWithSMQ(t, testutil.SMQRequired)
	defer gateway.CleanupAndClose()

	addr := gateway.StartAndWait()
	t.Logf("Gateway running on %s", addr)

	// Schema Registry URL from environment or default
	schemaRegistryURL := "http://localhost:8081"

	// Wait for Schema Registry to be ready
	if !waitForSchemaRegistry(t, schemaRegistryURL, 30*time.Second) {
		t.Fatal("Schema Registry not ready")
	}

	// Define test schemas
	valueSchema := `{"type":"record","name":"TestMessage","fields":[{"name":"id","type":"string"}]}`
	keySchema := `{"type":"string"}`

	// Register multiple schemas rapidly (simulates the load test scenario)
	subjects := []string{
		"test-topic-0-value",
		"test-topic-0-key",
		"test-topic-1-value",
		"test-topic-1-key",
		"test-topic-2-value",
		"test-topic-2-key",
		"test-topic-3-value",
		"test-topic-3-key",
	}

	t.Log("Registering schemas rapidly...")
	registeredIDs := make(map[string]int)
	for _, subject := range subjects {
		schema := valueSchema
		if strings.HasSuffix(subject, "-key") {
			schema = keySchema
		}

		id, err := registerSchema(schemaRegistryURL, subject, schema)
		if err != nil {
			t.Fatalf("Failed to register schema for %s: %v", subject, err)
		}
		registeredIDs[subject] = id
		t.Logf("Registered %s with ID %d", subject, id)
	}

	t.Log("All schemas registered successfully!")

	// Now immediately try to verify them (this reproduces the bug)
	t.Log("Immediately verifying schemas (without delay)...")
	immediateFailures := 0
	for _, subject := range subjects {
		exists, id, version, err := verifySchema(schemaRegistryURL, subject)
		if err != nil || !exists {
			immediateFailures++
			t.Logf("Immediate verification failed for %s: exists=%v id=%d err=%v", subject, exists, id, err)
		} else {
			t.Logf("Immediate verification passed for %s: ID=%d Version=%d", subject, id, version)
		}
	}

	if immediateFailures > 0 {
		t.Logf("BUG REPRODUCED: %d/%d schemas not immediately queryable after registration",
			immediateFailures, len(subjects))
		t.Logf("  This is due to Schema Registry's KafkaStoreReaderThread lag")
	}

	// Now verify with retry logic (this should succeed)
	t.Log("Verifying schemas with retry logic...")
	for _, subject := range subjects {
		expectedID := registeredIDs[subject]
		if !verifySchemaWithRetry(t, schemaRegistryURL, subject, expectedID, 5*time.Second) {
			t.Errorf("Failed to verify %s even with retry", subject)
		}
	}

	t.Log("✓ All schemas verified successfully with retry logic!")
}

// registerSchema registers a schema and returns its ID
func registerSchema(registryURL, subject, schema string) (int, error) {
	// Escape the schema JSON
	escapedSchema, err := json.Marshal(schema)
	if err != nil {
		return 0, err
	}

	payload := fmt.Sprintf(`{"schema":%s,"schemaType":"AVRO"}`, escapedSchema)

	resp, err := http.Post(
		fmt.Sprintf("%s/subjects/%s/versions", registryURL, subject),
		"application/vnd.schemaregistry.v1+json",
		strings.NewReader(payload),
	)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)

	if resp.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("registration failed: %s - %s", resp.Status, string(body))
	}

	var result struct {
		ID int `json:"id"`
	}
	if err := json.Unmarshal(body, &result); err != nil {
		return 0, err
	}

	return result.ID, nil
}

// verifySchema checks if a schema exists
func verifySchema(registryURL, subject string) (exists bool, id int, version int, err error) {
	resp, err := http.Get(fmt.Sprintf("%s/subjects/%s/versions/latest", registryURL, subject))
	if err != nil {
		return false, 0, 0, err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		return false, 0, 0, nil
	}

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return false, 0, 0, fmt.Errorf("verification failed: %s - %s", resp.Status, string(body))
	}

	var result struct {
		ID      int    `json:"id"`
		Version int    `json:"version"`
		Schema  string `json:"schema"`
	}
	body, _ := io.ReadAll(resp.Body)
	if err := json.Unmarshal(body, &result); err != nil {
		return false, 0, 0, err
	}

	return true, result.ID, result.Version, nil
}

// verifySchemaWithRetry verifies a schema with retry logic
func verifySchemaWithRetry(t *testing.T, registryURL, subject string, expectedID int, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	attempt := 0

	for time.Now().Before(deadline) {
		attempt++
		exists, id, version, err := verifySchema(registryURL, subject)

		if err == nil && exists && id == expectedID {
			if attempt > 1 {
				t.Logf("✓ %s verified after %d attempts (ID=%d, Version=%d)", subject, attempt, id, version)
			}
			return true
		}

		// Wait before retry (exponential backoff)
		waitTime := time.Duration(attempt*100) * time.Millisecond
		if waitTime > 1*time.Second {
			waitTime = 1 * time.Second
		}
		time.Sleep(waitTime)
	}

	t.Logf("%s verification timed out after %d attempts", subject, attempt)
	return false
}

// waitForSchemaRegistry waits for Schema Registry to be ready
func waitForSchemaRegistry(t *testing.T, url string, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)

	for time.Now().Before(deadline) {
		resp, err := http.Get(url + "/subjects")
		if err == nil && resp.StatusCode == http.StatusOK {
			resp.Body.Close()
			return true
		}
		if resp != nil {
			resp.Body.Close()
		}
		time.Sleep(500 * time.Millisecond)
	}

	return false
}
