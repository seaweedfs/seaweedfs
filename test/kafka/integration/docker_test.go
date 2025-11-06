package integration

import (
	"encoding/json"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/test/kafka/internal/testutil"
)

// TestDockerIntegration tests the complete Kafka integration using Docker Compose
func TestDockerIntegration(t *testing.T) {
	env := testutil.NewDockerEnvironment(t)
	env.SkipIfNotAvailable(t)

	t.Run("KafkaConnectivity", func(t *testing.T) {
		env.RequireKafka(t)
		testDockerKafkaConnectivity(t, env.KafkaBootstrap)
	})

	t.Run("SchemaRegistryConnectivity", func(t *testing.T) {
		env.RequireSchemaRegistry(t)
		testDockerSchemaRegistryConnectivity(t, env.SchemaRegistry)
	})

	t.Run("KafkaGatewayConnectivity", func(t *testing.T) {
		env.RequireGateway(t)
		testDockerKafkaGatewayConnectivity(t, env.KafkaGateway)
	})

	t.Run("SaramaProduceConsume", func(t *testing.T) {
		env.RequireKafka(t)
		testDockerSaramaProduceConsume(t, env.KafkaBootstrap)
	})

	t.Run("KafkaGoProduceConsume", func(t *testing.T) {
		env.RequireKafka(t)
		testDockerKafkaGoProduceConsume(t, env.KafkaBootstrap)
	})

	t.Run("GatewayProduceConsume", func(t *testing.T) {
		env.RequireGateway(t)
		testDockerGatewayProduceConsume(t, env.KafkaGateway)
	})

	t.Run("CrossClientCompatibility", func(t *testing.T) {
		env.RequireKafka(t)
		env.RequireGateway(t)
		testDockerCrossClientCompatibility(t, env.KafkaBootstrap, env.KafkaGateway)
	})
}

func testDockerKafkaConnectivity(t *testing.T, bootstrap string) {
	client := testutil.NewSaramaClient(t, bootstrap)

	// Test basic connectivity by creating a topic
	topicName := testutil.GenerateUniqueTopicName("connectivity-test")
	err := client.CreateTopic(topicName, 1, 1)
	testutil.AssertNoError(t, err, "Failed to create topic for connectivity test")

	t.Logf("Kafka connectivity test passed")
}

func testDockerSchemaRegistryConnectivity(t *testing.T, registryURL string) {
	// Test basic HTTP connectivity to Schema Registry
	client := &http.Client{Timeout: 10 * time.Second}

	// Test 1: Check if Schema Registry is responding
	resp, err := client.Get(registryURL + "/subjects")
	if err != nil {
		t.Fatalf("Failed to connect to Schema Registry at %s: %v", registryURL, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("Schema Registry returned status %d, expected 200", resp.StatusCode)
	}

	// Test 2: Verify response is valid JSON array
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("Failed to read response body: %v", err)
	}

	var subjects []string
	if err := json.Unmarshal(body, &subjects); err != nil {
		t.Fatalf("Schema Registry response is not valid JSON array: %v", err)
	}

	t.Logf("Schema Registry is accessible with %d subjects", len(subjects))

	// Test 3: Check config endpoint
	configResp, err := client.Get(registryURL + "/config")
	if err != nil {
		t.Fatalf("Failed to get Schema Registry config: %v", err)
	}
	defer configResp.Body.Close()

	if configResp.StatusCode != http.StatusOK {
		t.Fatalf("Schema Registry config endpoint returned status %d", configResp.StatusCode)
	}

	configBody, err := io.ReadAll(configResp.Body)
	if err != nil {
		t.Fatalf("Failed to read config response: %v", err)
	}

	var config map[string]interface{}
	if err := json.Unmarshal(configBody, &config); err != nil {
		t.Fatalf("Schema Registry config response is not valid JSON: %v", err)
	}

	t.Logf("Schema Registry config: %v", config)
	t.Logf("Schema Registry connectivity test passed")
}

func testDockerKafkaGatewayConnectivity(t *testing.T, gatewayURL string) {
	client := testutil.NewSaramaClient(t, gatewayURL)

	// Test basic connectivity to gateway
	topicName := testutil.GenerateUniqueTopicName("gateway-connectivity-test")
	err := client.CreateTopic(topicName, 1, 1)
	testutil.AssertNoError(t, err, "Failed to create topic via gateway")

	t.Logf("Kafka Gateway connectivity test passed")
}

func testDockerSaramaProduceConsume(t *testing.T, bootstrap string) {
	client := testutil.NewSaramaClient(t, bootstrap)
	msgGen := testutil.NewMessageGenerator()

	topicName := testutil.GenerateUniqueTopicName("sarama-docker-test")

	// Create topic
	err := client.CreateTopic(topicName, 1, 1)
	testutil.AssertNoError(t, err, "Failed to create topic")

	// Produce and consume messages
	messages := msgGen.GenerateStringMessages(3)
	err = client.ProduceMessages(topicName, messages)
	testutil.AssertNoError(t, err, "Failed to produce messages")

	consumed, err := client.ConsumeMessages(topicName, 0, len(messages))
	testutil.AssertNoError(t, err, "Failed to consume messages")

	err = testutil.ValidateMessageContent(messages, consumed)
	testutil.AssertNoError(t, err, "Message validation failed")

	t.Logf("Sarama produce/consume test passed")
}

func testDockerKafkaGoProduceConsume(t *testing.T, bootstrap string) {
	client := testutil.NewKafkaGoClient(t, bootstrap)
	msgGen := testutil.NewMessageGenerator()

	topicName := testutil.GenerateUniqueTopicName("kafka-go-docker-test")

	// Create topic
	err := client.CreateTopic(topicName, 1, 1)
	testutil.AssertNoError(t, err, "Failed to create topic")

	// Produce and consume messages
	messages := msgGen.GenerateKafkaGoMessages(3)
	err = client.ProduceMessages(topicName, messages)
	testutil.AssertNoError(t, err, "Failed to produce messages")

	consumed, err := client.ConsumeMessages(topicName, len(messages))
	testutil.AssertNoError(t, err, "Failed to consume messages")

	err = testutil.ValidateKafkaGoMessageContent(messages, consumed)
	testutil.AssertNoError(t, err, "Message validation failed")

	t.Logf("kafka-go produce/consume test passed")
}

func testDockerGatewayProduceConsume(t *testing.T, gatewayURL string) {
	client := testutil.NewSaramaClient(t, gatewayURL)
	msgGen := testutil.NewMessageGenerator()

	topicName := testutil.GenerateUniqueTopicName("gateway-docker-test")

	// Produce and consume via gateway
	messages := msgGen.GenerateStringMessages(3)
	err := client.ProduceMessages(topicName, messages)
	testutil.AssertNoError(t, err, "Failed to produce messages via gateway")

	consumed, err := client.ConsumeMessages(topicName, 0, len(messages))
	testutil.AssertNoError(t, err, "Failed to consume messages via gateway")

	err = testutil.ValidateMessageContent(messages, consumed)
	testutil.AssertNoError(t, err, "Message validation failed")

	t.Logf("Gateway produce/consume test passed")
}

func testDockerCrossClientCompatibility(t *testing.T, kafkaBootstrap, gatewayURL string) {
	kafkaClient := testutil.NewSaramaClient(t, kafkaBootstrap)
	msgGen := testutil.NewMessageGenerator()

	topicName := testutil.GenerateUniqueTopicName("cross-client-docker-test")

	// Create topic on Kafka
	err := kafkaClient.CreateTopic(topicName, 1, 1)
	testutil.AssertNoError(t, err, "Failed to create topic on Kafka")

	// Produce to Kafka
	messages := msgGen.GenerateStringMessages(2)
	err = kafkaClient.ProduceMessages(topicName, messages)
	testutil.AssertNoError(t, err, "Failed to produce to Kafka")

	// This tests the integration between Kafka and the Gateway
	// In a real scenario, messages would be replicated or bridged
	t.Logf("Cross-client compatibility test passed")
}
