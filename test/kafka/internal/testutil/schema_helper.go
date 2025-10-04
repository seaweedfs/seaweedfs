package testutil

import (
	"testing"

	kschema "github.com/seaweedfs/seaweedfs/weed/mq/kafka/schema"
)

// EnsureValueSchema registers a minimal Avro value schema for the given topic if not present.
// Returns the latest schema ID if successful.
func EnsureValueSchema(t *testing.T, registryURL, topic string) (uint32, error) {
	t.Helper()
	subject := topic + "-value"
	rc := kschema.NewRegistryClient(kschema.RegistryConfig{URL: registryURL})

	// Minimal Avro record schema with string field "value"
	schemaJSON := `{"type":"record","name":"TestRecord","fields":[{"name":"value","type":"string"}]}`

	// Try to get existing
	if latest, err := rc.GetLatestSchema(subject); err == nil {
		return latest.LatestID, nil
	}

	// Register and fetch latest
	if _, err := rc.RegisterSchema(subject, schemaJSON); err != nil {
		return 0, err
	}
	latest, err := rc.GetLatestSchema(subject)
	if err != nil {
		return 0, err
	}
	return latest.LatestID, nil
}
