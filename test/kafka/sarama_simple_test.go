package kafka

import (
	"fmt"
	"testing"
	"time"

	"github.com/IBM/sarama"
	"github.com/seaweedfs/seaweedfs/weed/mq/kafka/gateway"
)

func TestSaramaSimpleProducer(t *testing.T) {
	// Start gateway
	gatewayServer := gateway.NewServer(gateway.Options{
		Listen: "127.0.0.1:0",
	})

	go func() {
		if err := gatewayServer.Start(); err != nil {
			t.Errorf("Failed to start gateway: %v", err)
		}
	}()
	defer gatewayServer.Close()

	// Wait for server to start
	time.Sleep(100 * time.Millisecond)

	host, port := gatewayServer.GetListenerAddr()
	brokerAddr := fmt.Sprintf("%s:%d", host, port)
	t.Logf("Gateway running on %s", brokerAddr)

	// Add test topic
	gatewayHandler := gatewayServer.GetHandler()
	topicName := "simple-test-topic"
	gatewayHandler.AddTopicForTesting(topicName, 1)
	t.Logf("Added topic: %s", topicName)

	// Test with different Kafka versions to find one that works
	versions := []sarama.KafkaVersion{
		sarama.V0_11_0_0, // Kafka 0.11.0 - our baseline
		sarama.V1_0_0_0,  // Kafka 1.0.0
		sarama.V2_0_0_0,  // Kafka 2.0.0
		sarama.V2_1_0_0,  // Kafka 2.1.0 - what we were using
	}

	for _, version := range versions {
		t.Logf("=== Testing with Kafka version %s ===", version.String())

		// Configure Sarama with specific version
		config := sarama.NewConfig()
		config.Version = version
		config.Producer.Return.Successes = true
		config.Producer.RequiredAcks = sarama.WaitForAll
		config.Producer.Timeout = 5 * time.Second

		// Create producer
		producer, err := sarama.NewSyncProducer([]string{brokerAddr}, config)
		if err != nil {
			t.Logf("❌ Failed to create producer for %s: %v", version.String(), err)
			continue
		}

		// Send a test message
		msg := &sarama.ProducerMessage{
			Topic: topicName,
			Key:   sarama.StringEncoder("test-key"),
			Value: sarama.StringEncoder(fmt.Sprintf("test-value-%s", version.String())),
		}

		partition, offset, err := producer.SendMessage(msg)
		producer.Close()

		if err != nil {
			t.Logf("❌ Produce failed for %s: %v", version.String(), err)
		} else {
			t.Logf("✅ Produce succeeded for %s: partition=%d, offset=%d", version.String(), partition, offset)

			// If we found a working version, we can stop here
			t.Logf("🎉 SUCCESS: Found working Kafka version: %s", version.String())
			return
		}
	}

	t.Logf("❌ No Kafka version worked with Sarama")
}

func TestSaramaMinimalConfig(t *testing.T) {
	// Start gateway
	gatewayServer := gateway.NewServer(gateway.Options{
		Listen: "127.0.0.1:0",
	})

	go func() {
		if err := gatewayServer.Start(); err != nil {
			t.Errorf("Failed to start gateway: %v", err)
		}
	}()
	defer gatewayServer.Close()

	// Wait for server to start
	time.Sleep(100 * time.Millisecond)

	host, port := gatewayServer.GetListenerAddr()
	brokerAddr := fmt.Sprintf("%s:%d", host, port)
	t.Logf("Gateway running on %s", brokerAddr)

	// Add test topic
	gatewayHandler := gatewayServer.GetHandler()
	topicName := "minimal-test-topic"
	gatewayHandler.AddTopicForTesting(topicName, 1)
	t.Logf("Added topic: %s", topicName)

	t.Logf("=== Testing with minimal Sarama configuration ===")

	// Minimal Sarama configuration
	config := sarama.NewConfig()
	config.Version = sarama.V0_11_0_0 // Use our baseline version
	config.Producer.Return.Successes = true
	config.Producer.RequiredAcks = sarama.WaitForLocal // Try less strict acks
	config.Producer.Timeout = 10 * time.Second
	config.Producer.Retry.Max = 1 // Minimal retries
	config.Net.DialTimeout = 5 * time.Second
	config.Net.ReadTimeout = 5 * time.Second
	config.Net.WriteTimeout = 5 * time.Second

	// Create producer
	producer, err := sarama.NewSyncProducer([]string{brokerAddr}, config)
	if err != nil {
		t.Fatalf("Failed to create producer: %v", err)
	}
	defer producer.Close()

	// Send a simple message
	msg := &sarama.ProducerMessage{
		Topic: topicName,
		Value: sarama.StringEncoder("minimal-test-message"),
	}

	t.Logf("Sending minimal message...")
	partition, offset, err := producer.SendMessage(msg)

	if err != nil {
		t.Logf("❌ Minimal produce failed: %v", err)
	} else {
		t.Logf("✅ Minimal produce succeeded: partition=%d, offset=%d", partition, offset)
	}
}
