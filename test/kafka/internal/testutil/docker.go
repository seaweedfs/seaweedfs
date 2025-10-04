package testutil

import (
	"os"
	"testing"
)

// DockerEnvironment provides utilities for Docker-based integration tests
type DockerEnvironment struct {
	KafkaBootstrap string
	KafkaGateway   string
	SchemaRegistry string
	Available      bool
}

// NewDockerEnvironment creates a new Docker environment helper
func NewDockerEnvironment(t *testing.T) *DockerEnvironment {
	t.Helper()

	env := &DockerEnvironment{
		KafkaBootstrap: os.Getenv("KAFKA_BOOTSTRAP_SERVERS"),
		KafkaGateway:   os.Getenv("KAFKA_GATEWAY_URL"),
		SchemaRegistry: os.Getenv("SCHEMA_REGISTRY_URL"),
	}

	env.Available = env.KafkaBootstrap != ""

	if env.Available {
		t.Logf("Docker environment detected:")
		t.Logf("  Kafka Bootstrap: %s", env.KafkaBootstrap)
		t.Logf("  Kafka Gateway: %s", env.KafkaGateway)
		t.Logf("  Schema Registry: %s", env.SchemaRegistry)
	}

	return env
}

// SkipIfNotAvailable skips the test if Docker environment is not available
func (d *DockerEnvironment) SkipIfNotAvailable(t *testing.T) {
	t.Helper()
	if !d.Available {
		t.Skip("Skipping Docker integration test - set KAFKA_BOOTSTRAP_SERVERS to run")
	}
}

// RequireKafka ensures Kafka is available or skips the test
func (d *DockerEnvironment) RequireKafka(t *testing.T) {
	t.Helper()
	if d.KafkaBootstrap == "" {
		t.Skip("Kafka bootstrap servers not available")
	}
}

// RequireGateway ensures Kafka Gateway is available or skips the test
func (d *DockerEnvironment) RequireGateway(t *testing.T) {
	t.Helper()
	if d.KafkaGateway == "" {
		t.Skip("Kafka Gateway not available")
	}
}

// RequireSchemaRegistry ensures Schema Registry is available or skips the test
func (d *DockerEnvironment) RequireSchemaRegistry(t *testing.T) {
	t.Helper()
	if d.SchemaRegistry == "" {
		t.Skip("Schema Registry not available")
	}
}
