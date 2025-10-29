package testutil

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/IBM/sarama"
	"github.com/segmentio/kafka-go"
)

// KafkaGoClient wraps kafka-go client with test utilities
type KafkaGoClient struct {
	brokerAddr string
	t          *testing.T
}

// SaramaClient wraps Sarama client with test utilities
type SaramaClient struct {
	brokerAddr string
	config     *sarama.Config
	t          *testing.T
}

// NewKafkaGoClient creates a new kafka-go test client
func NewKafkaGoClient(t *testing.T, brokerAddr string) *KafkaGoClient {
	return &KafkaGoClient{
		brokerAddr: brokerAddr,
		t:          t,
	}
}

// NewSaramaClient creates a new Sarama test client with default config
func NewSaramaClient(t *testing.T, brokerAddr string) *SaramaClient {
	config := sarama.NewConfig()
	config.Version = sarama.V2_8_0_0
	config.Producer.Return.Successes = true
	config.Consumer.Return.Errors = true
	config.Consumer.Offsets.Initial = sarama.OffsetOldest // Start from earliest when no committed offset

	return &SaramaClient{
		brokerAddr: brokerAddr,
		config:     config,
		t:          t,
	}
}

// CreateTopic creates a topic using kafka-go
func (k *KafkaGoClient) CreateTopic(topicName string, partitions int, replicationFactor int) error {
	k.t.Helper()

	conn, err := kafka.Dial("tcp", k.brokerAddr)
	if err != nil {
		return fmt.Errorf("dial broker: %w", err)
	}
	defer conn.Close()

	topicConfig := kafka.TopicConfig{
		Topic:             topicName,
		NumPartitions:     partitions,
		ReplicationFactor: replicationFactor,
	}

	err = conn.CreateTopics(topicConfig)
	if err != nil {
		return fmt.Errorf("create topic: %w", err)
	}

	k.t.Logf("Created topic %s with %d partitions", topicName, partitions)
	return nil
}

// ProduceMessages produces messages using kafka-go
func (k *KafkaGoClient) ProduceMessages(topicName string, messages []kafka.Message) error {
	k.t.Helper()

	writer := &kafka.Writer{
		Addr:         kafka.TCP(k.brokerAddr),
		Topic:        topicName,
		Balancer:     &kafka.LeastBytes{},
		BatchTimeout: 50 * time.Millisecond,
		RequiredAcks: kafka.RequireOne,
	}
	defer writer.Close()

	// Increased timeout to handle slow CI environments, especially when consumer groups
	// are active and holding locks or requiring offset commits
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	err := writer.WriteMessages(ctx, messages...)
	if err != nil {
		return fmt.Errorf("write messages: %w", err)
	}

	k.t.Logf("Produced %d messages to topic %s", len(messages), topicName)
	return nil
}

// ConsumeMessages consumes messages using kafka-go
func (k *KafkaGoClient) ConsumeMessages(topicName string, expectedCount int) ([]kafka.Message, error) {
	k.t.Helper()

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     []string{k.brokerAddr},
		Topic:       topicName,
		Partition:   0, // Explicitly set partition 0 for simple consumption
		StartOffset: kafka.FirstOffset,
		MinBytes:    1,
		MaxBytes:    10e6,
	})
	defer reader.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	var messages []kafka.Message
	for i := 0; i < expectedCount; i++ {
		msg, err := reader.ReadMessage(ctx)
		if err != nil {
			return messages, fmt.Errorf("read message %d: %w", i, err)
		}
		messages = append(messages, msg)
	}

	k.t.Logf("Consumed %d messages from topic %s", len(messages), topicName)
	return messages, nil
}

// ConsumeWithGroup consumes messages using consumer group
func (k *KafkaGoClient) ConsumeWithGroup(topicName, groupID string, expectedCount int) ([]kafka.Message, error) {
	k.t.Helper()

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        []string{k.brokerAddr},
		Topic:          topicName,
		GroupID:        groupID,
		MinBytes:       1,
		MaxBytes:       10e6,
		CommitInterval: 500 * time.Millisecond,
	})
	defer reader.Close()

	// Log the initial offset position
	offset := reader.Offset()
	k.t.Logf("Consumer group reader created for group %s, initial offset: %d", groupID, offset)

	// Increased timeout for consumer groups - they require coordinator discovery,
	// offset fetching, and offset commits which can be slow in CI environments
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	var messages []kafka.Message
	for i := 0; i < expectedCount; i++ {
		// Fetch then explicitly commit to better control commit timing
		msg, err := reader.FetchMessage(ctx)
		if err != nil {
			return messages, fmt.Errorf("read message %d: %w", i, err)
		}
		messages = append(messages, msg)
		k.t.Logf("  Fetched message %d: offset=%d, partition=%d", i, msg.Offset, msg.Partition)

		// Commit with simple retry to handle transient connection churn
		var commitErr error
		for attempt := 0; attempt < 3; attempt++ {
			commitErr = reader.CommitMessages(ctx, msg)
			if commitErr == nil {
				k.t.Logf("  Committed offset %d (attempt %d)", msg.Offset, attempt+1)
				break
			}
			k.t.Logf("  Commit attempt %d failed for offset %d: %v", attempt+1, msg.Offset, commitErr)
			// brief backoff
			time.Sleep(time.Duration(50*(1<<attempt)) * time.Millisecond)
		}
		if commitErr != nil {
			return messages, fmt.Errorf("committing message %d: %w", i, commitErr)
		}
	}

	k.t.Logf("Consumed %d messages from topic %s with group %s", len(messages), topicName, groupID)
	return messages, nil
}

// CreateTopic creates a topic using Sarama
func (s *SaramaClient) CreateTopic(topicName string, partitions int32, replicationFactor int16) error {
	s.t.Helper()

	admin, err := sarama.NewClusterAdmin([]string{s.brokerAddr}, s.config)
	if err != nil {
		return fmt.Errorf("create admin client: %w", err)
	}
	defer admin.Close()

	topicDetail := &sarama.TopicDetail{
		NumPartitions:     partitions,
		ReplicationFactor: replicationFactor,
	}

	err = admin.CreateTopic(topicName, topicDetail, false)
	if err != nil {
		return fmt.Errorf("create topic: %w", err)
	}

	s.t.Logf("Created topic %s with %d partitions", topicName, partitions)
	return nil
}

// ProduceMessages produces messages using Sarama
func (s *SaramaClient) ProduceMessages(topicName string, messages []string) error {
	s.t.Helper()

	producer, err := sarama.NewSyncProducer([]string{s.brokerAddr}, s.config)
	if err != nil {
		return fmt.Errorf("create producer: %w", err)
	}
	defer producer.Close()

	for i, msgText := range messages {
		msg := &sarama.ProducerMessage{
			Topic: topicName,
			Key:   sarama.StringEncoder(fmt.Sprintf("Test message %d", i)),
			Value: sarama.StringEncoder(msgText),
		}

		partition, offset, err := producer.SendMessage(msg)
		if err != nil {
			return fmt.Errorf("send message %d: %w", i, err)
		}

		s.t.Logf("Produced message %d: partition=%d, offset=%d", i, partition, offset)
	}

	return nil
}

// ProduceMessageToPartition produces a single message to a specific partition using Sarama
func (s *SaramaClient) ProduceMessageToPartition(topicName string, partition int32, message string) error {
	s.t.Helper()

	producer, err := sarama.NewSyncProducer([]string{s.brokerAddr}, s.config)
	if err != nil {
		return fmt.Errorf("create producer: %w", err)
	}
	defer producer.Close()

	msg := &sarama.ProducerMessage{
		Topic:     topicName,
		Partition: partition,
		Key:       sarama.StringEncoder(fmt.Sprintf("key-p%d", partition)),
		Value:     sarama.StringEncoder(message),
	}

	actualPartition, offset, err := producer.SendMessage(msg)
	if err != nil {
		return fmt.Errorf("send message to partition %d: %w", partition, err)
	}

	s.t.Logf("Produced message to partition %d: actualPartition=%d, offset=%d", partition, actualPartition, offset)
	return nil
}

// ConsumeMessages consumes messages using Sarama
func (s *SaramaClient) ConsumeMessages(topicName string, partition int32, expectedCount int) ([]string, error) {
	s.t.Helper()

	consumer, err := sarama.NewConsumer([]string{s.brokerAddr}, s.config)
	if err != nil {
		return nil, fmt.Errorf("create consumer: %w", err)
	}
	defer consumer.Close()

	partitionConsumer, err := consumer.ConsumePartition(topicName, partition, sarama.OffsetOldest)
	if err != nil {
		return nil, fmt.Errorf("create partition consumer: %w", err)
	}
	defer partitionConsumer.Close()

	var messages []string
	timeout := time.After(30 * time.Second)

	for len(messages) < expectedCount {
		select {
		case msg := <-partitionConsumer.Messages():
			messages = append(messages, string(msg.Value))
		case err := <-partitionConsumer.Errors():
			return messages, fmt.Errorf("consumer error: %w", err)
		case <-timeout:
			return messages, fmt.Errorf("timeout waiting for messages, got %d/%d", len(messages), expectedCount)
		}
	}

	s.t.Logf("Consumed %d messages from topic %s", len(messages), topicName)
	return messages, nil
}

// GetConfig returns the Sarama configuration
func (s *SaramaClient) GetConfig() *sarama.Config {
	return s.config
}

// SetConfig sets a custom Sarama configuration
func (s *SaramaClient) SetConfig(config *sarama.Config) {
	s.config = config
}
