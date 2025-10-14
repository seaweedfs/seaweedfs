package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"time"
)

// Schema represents a schema registry schema
type Schema struct {
	Subject string `json:"subject"`
	Version int    `json:"version"`
	Schema  string `json:"schema"`
}

// SchemaResponse represents the response from schema registry
type SchemaResponse struct {
	ID int `json:"id"`
}

func main() {
	log.Println("Setting up Kafka integration test environment...")

	kafkaBootstrap := getEnv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
	schemaRegistryURL := getEnv("SCHEMA_REGISTRY_URL", "http://schema-registry:8081")
	kafkaGatewayURL := getEnv("KAFKA_GATEWAY_URL", "kafka-gateway:9093")

	log.Printf("Kafka Bootstrap Servers: %s", kafkaBootstrap)
	log.Printf("Schema Registry URL: %s", schemaRegistryURL)
	log.Printf("Kafka Gateway URL: %s", kafkaGatewayURL)

	// Wait for services to be ready
	waitForHTTPService("Schema Registry", schemaRegistryURL+"/subjects")
	waitForTCPService("Kafka Gateway", kafkaGatewayURL) // TCP connectivity check for Kafka protocol

	// Register test schemas
	if err := registerSchemas(schemaRegistryURL); err != nil {
		log.Fatalf("Failed to register schemas: %v", err)
	}

	log.Println("Test environment setup completed successfully!")
}

func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

func waitForHTTPService(name, url string) {
	log.Printf("Waiting for %s to be ready...", name)
	for i := 0; i < 60; i++ { // Wait up to 60 seconds
		resp, err := http.Get(url)
		if err == nil && resp.StatusCode < 400 {
			resp.Body.Close()
			log.Printf("%s is ready", name)
			return
		}
		if resp != nil {
			resp.Body.Close()
		}
		time.Sleep(1 * time.Second)
	}
	log.Fatalf("%s is not ready after 60 seconds", name)
}

func waitForTCPService(name, address string) {
	log.Printf("Waiting for %s to be ready...", name)
	for i := 0; i < 60; i++ { // Wait up to 60 seconds
		conn, err := net.DialTimeout("tcp", address, 2*time.Second)
		if err == nil {
			conn.Close()
			log.Printf("%s is ready", name)
			return
		}
		time.Sleep(1 * time.Second)
	}
	log.Fatalf("%s is not ready after 60 seconds", name)
}

func registerSchemas(registryURL string) error {
	schemas := []Schema{
		{
			Subject: "user-value",
			Schema: `{
				"type": "record",
				"name": "User",
				"fields": [
					{"name": "id", "type": "int"},
					{"name": "name", "type": "string"},
					{"name": "email", "type": ["null", "string"], "default": null}
				]
			}`,
		},
		{
			Subject: "user-event-value",
			Schema: `{
				"type": "record",
				"name": "UserEvent",
				"fields": [
					{"name": "userId", "type": "int"},
					{"name": "eventType", "type": "string"},
					{"name": "timestamp", "type": "long"},
					{"name": "data", "type": ["null", "string"], "default": null}
				]
			}`,
		},
		{
			Subject: "log-entry-value",
			Schema: `{
				"type": "record",
				"name": "LogEntry",
				"fields": [
					{"name": "level", "type": "string"},
					{"name": "message", "type": "string"},
					{"name": "timestamp", "type": "long"},
					{"name": "service", "type": "string"},
					{"name": "metadata", "type": {"type": "map", "values": "string"}}
				]
			}`,
		},
	}

	for _, schema := range schemas {
		if err := registerSchema(registryURL, schema); err != nil {
			return fmt.Errorf("failed to register schema %s: %w", schema.Subject, err)
		}
		log.Printf("Registered schema: %s", schema.Subject)
	}

	return nil
}

func registerSchema(registryURL string, schema Schema) error {
	url := fmt.Sprintf("%s/subjects/%s/versions", registryURL, schema.Subject)

	payload := map[string]interface{}{
		"schema": schema.Schema,
	}

	jsonData, err := json.Marshal(payload)
	if err != nil {
		return err
	}

	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Post(url, "application/vnd.schemaregistry.v1+json", bytes.NewBuffer(jsonData))
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("HTTP %d: %s", resp.StatusCode, string(body))
	}

	var response SchemaResponse
	if err := json.NewDecoder(resp.Body).Decode(&response); err != nil {
		return err
	}

	log.Printf("Schema %s registered with ID: %d", schema.Subject, response.ID)
	return nil
}
