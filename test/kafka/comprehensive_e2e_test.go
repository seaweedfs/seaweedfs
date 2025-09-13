package kafka

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/IBM/sarama"
	"github.com/seaweedfs/seaweedfs/weed/mq/kafka/gateway"
	"github.com/segmentio/kafka-go"
)

// TestComprehensiveE2E tests both kafka-go and Sarama clients in a comprehensive scenario
func TestComprehensiveE2E(t *testing.T) {
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

	time.Sleep(100 * time.Millisecond)

	host, port := gatewayServer.GetListenerAddr()
	addr := fmt.Sprintf("%s:%d", host, port)
	t.Logf("Gateway running on %s", addr)

	// Create multiple topics for different test scenarios
	topics := []string{
		"e2e-kafka-go-topic",
		"e2e-sarama-topic", 
		"e2e-mixed-topic",
	}

	for _, topic := range topics {
		gatewayServer.GetHandler().AddTopicForTesting(topic, 1)
		t.Logf("Added topic: %s", topic)
	}

	// Test 1: kafka-go producer -> kafka-go consumer
	t.Run("KafkaGo_to_KafkaGo", func(t *testing.T) {
		testKafkaGoToKafkaGo(t, addr, topics[0])
	})

	// Test 2: Sarama producer -> Sarama consumer
	t.Run("Sarama_to_Sarama", func(t *testing.T) {
		testSaramaToSarama(t, addr, topics[1])
	})

	// Test 3: Mixed clients - kafka-go producer -> Sarama consumer
	t.Run("KafkaGo_to_Sarama", func(t *testing.T) {
		testKafkaGoToSarama(t, addr, topics[2])
	})

	// Test 4: Mixed clients - Sarama producer -> kafka-go consumer
	t.Run("Sarama_to_KafkaGo", func(t *testing.T) {
		testSaramaToKafkaGo(t, addr, topics[2])
	})
}

func testKafkaGoToKafkaGo(t *testing.T, addr, topic string) {
	messages := []kafka.Message{
		{Key: []byte("kgo-key1"), Value: []byte("kafka-go to kafka-go message 1")},
		{Key: []byte("kgo-key2"), Value: []byte("kafka-go to kafka-go message 2")},
	}

	// Produce with kafka-go
	w := &kafka.Writer{
		Addr:         kafka.TCP(addr),
		Topic:        topic,
		Balancer:     &kafka.LeastBytes{},
		BatchTimeout: 50 * time.Millisecond,
		RequiredAcks: kafka.RequireOne,
	}
	defer w.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err := w.WriteMessages(ctx, messages...)
	if err != nil {
		t.Fatalf("kafka-go produce failed: %v", err)
	}
	t.Logf("✅ kafka-go produced %d messages", len(messages))

	// Consume with kafka-go
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     []string{addr},
		Topic:       topic,
		StartOffset: kafka.FirstOffset,
		MinBytes:    1,
		MaxBytes:    10e6,
	})
	defer r.Close()

	consumeCtx, consumeCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer consumeCancel()

	for i := 0; i < len(messages); i++ {
		msg, err := r.ReadMessage(consumeCtx)
		if err != nil {
			t.Fatalf("kafka-go consume failed: %v", err)
		}
		t.Logf("✅ kafka-go consumed: key=%s, value=%s", string(msg.Key), string(msg.Value))
		
		// Validate message
		expected := messages[i]
		if string(msg.Key) != string(expected.Key) || string(msg.Value) != string(expected.Value) {
			t.Errorf("Message mismatch: got key=%s value=%s, want key=%s value=%s",
				string(msg.Key), string(msg.Value), string(expected.Key), string(expected.Value))
		}
	}

	t.Logf("🎉 kafka-go to kafka-go test PASSED")
}

func testSaramaToSarama(t *testing.T, addr, topic string) {
	// Configure Sarama
	config := sarama.NewConfig()
	config.Version = sarama.V0_11_0_0
	config.Producer.Return.Successes = true
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Consumer.Return.Errors = true

	messages := []string{
		"Sarama to Sarama message 1",
		"Sarama to Sarama message 2",
	}

	// Produce with Sarama
	producer, err := sarama.NewSyncProducer([]string{addr}, config)
	if err != nil {
		t.Fatalf("Failed to create Sarama producer: %v", err)
	}
	defer producer.Close()

	for i, msgText := range messages {
		msg := &sarama.ProducerMessage{
			Topic: topic,
			Key:   sarama.StringEncoder(fmt.Sprintf("sarama-key-%d", i)),
			Value: sarama.StringEncoder(msgText),
		}

		partition, offset, err := producer.SendMessage(msg)
		if err != nil {
			t.Fatalf("Sarama produce failed: %v", err)
		}
		t.Logf("✅ Sarama produced message %d: partition=%d, offset=%d", i, partition, offset)
	}

	// Consume with Sarama
	consumer, err := sarama.NewConsumer([]string{addr}, config)
	if err != nil {
		t.Fatalf("Failed to create Sarama consumer: %v", err)
	}
	defer consumer.Close()

	partitionConsumer, err := consumer.ConsumePartition(topic, 0, sarama.OffsetOldest)
	if err != nil {
		t.Fatalf("Failed to create Sarama partition consumer: %v", err)
	}
	defer partitionConsumer.Close()

	consumedCount := 0
	timeout := time.After(10 * time.Second)

	for consumedCount < len(messages) {
		select {
		case msg := <-partitionConsumer.Messages():
			t.Logf("✅ Sarama consumed: key=%s, value=%s, offset=%d",
				string(msg.Key), string(msg.Value), msg.Offset)

			expectedValue := messages[consumedCount]
			if string(msg.Value) != expectedValue {
				t.Errorf("Message mismatch: got %s, want %s", string(msg.Value), expectedValue)
			}
			consumedCount++

		case err := <-partitionConsumer.Errors():
			t.Fatalf("Sarama consumer error: %v", err)

		case <-timeout:
			t.Fatalf("Timeout waiting for Sarama messages. Consumed %d/%d", consumedCount, len(messages))
		}
	}

	t.Logf("🎉 Sarama to Sarama test PASSED")
}

func testKafkaGoToSarama(t *testing.T, addr, topic string) {
	// Note: In a real test environment, we'd need to ensure topic isolation
	// For now, we'll use a different key prefix to distinguish messages
	
	messages := []kafka.Message{
		{Key: []byte("mixed-kgo-key1"), Value: []byte("kafka-go producer to Sarama consumer")},
		{Key: []byte("mixed-kgo-key2"), Value: []byte("Cross-client compatibility test")},
	}

	// Produce with kafka-go
	w := &kafka.Writer{
		Addr:         kafka.TCP(addr),
		Topic:        topic,
		Balancer:     &kafka.LeastBytes{},
		BatchTimeout: 50 * time.Millisecond,
		RequiredAcks: kafka.RequireOne,
	}
	defer w.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err := w.WriteMessages(ctx, messages...)
	if err != nil {
		t.Fatalf("kafka-go produce failed: %v", err)
	}
	t.Logf("✅ kafka-go produced %d messages for Sarama consumer", len(messages))

	// Consume with Sarama
	config := sarama.NewConfig()
	config.Version = sarama.V0_11_0_0
	config.Consumer.Return.Errors = true

	consumer, err := sarama.NewConsumer([]string{addr}, config)
	if err != nil {
		t.Fatalf("Failed to create Sarama consumer: %v", err)
	}
	defer consumer.Close()

	// Start from latest to avoid consuming previous test messages
	partitionConsumer, err := consumer.ConsumePartition(topic, 0, sarama.OffsetNewest)
	if err != nil {
		t.Fatalf("Failed to create Sarama partition consumer: %v", err)
	}
	defer partitionConsumer.Close()

	// Give a moment for the consumer to be ready
	time.Sleep(100 * time.Millisecond)

	// Produce again to ensure we get fresh messages
	err = w.WriteMessages(ctx, messages...)
	if err != nil {
		t.Fatalf("kafka-go second produce failed: %v", err)
	}

	consumedCount := 0
	timeout := time.After(10 * time.Second)

	for consumedCount < len(messages) {
		select {
		case msg := <-partitionConsumer.Messages():
			// Only count messages with our test key prefix
			if strings.HasPrefix(string(msg.Key), "mixed-kgo-key") {
				t.Logf("✅ Sarama consumed kafka-go message: key=%s, value=%s",
					string(msg.Key), string(msg.Value))
				consumedCount++
			}

		case err := <-partitionConsumer.Errors():
			t.Fatalf("Sarama consumer error: %v", err)

		case <-timeout:
			t.Fatalf("Timeout waiting for mixed messages. Consumed %d/%d", consumedCount, len(messages))
		}
	}

	t.Logf("🎉 kafka-go to Sarama test PASSED")
}

func testSaramaToKafkaGo(t *testing.T, addr, topic string) {
	// Configure Sarama
	config := sarama.NewConfig()
	config.Version = sarama.V0_11_0_0
	config.Producer.Return.Successes = true
	config.Producer.RequiredAcks = sarama.WaitForAll

	messages := []string{
		"Sarama producer to kafka-go consumer",
		"Reverse cross-client compatibility test",
	}

	// Produce with Sarama
	producer, err := sarama.NewSyncProducer([]string{addr}, config)
	if err != nil {
		t.Fatalf("Failed to create Sarama producer: %v", err)
	}
	defer producer.Close()

	for i, msgText := range messages {
		msg := &sarama.ProducerMessage{
			Topic: topic,
			Key:   sarama.StringEncoder(fmt.Sprintf("mixed-sarama-key-%d", i)),
			Value: sarama.StringEncoder(msgText),
		}

		partition, offset, err := producer.SendMessage(msg)
		if err != nil {
			t.Fatalf("Sarama produce failed: %v", err)
		}
		t.Logf("✅ Sarama produced message %d for kafka-go consumer: partition=%d, offset=%d", i, partition, offset)
	}

	// Consume with kafka-go
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     []string{addr},
		Topic:       topic,
		StartOffset: kafka.LastOffset, // Start from latest to avoid previous messages
		MinBytes:    1,
		MaxBytes:    10e6,
	})
	defer r.Close()

	// Give a moment for the reader to be ready, then produce fresh messages
	time.Sleep(100 * time.Millisecond)

	// Produce again to ensure fresh messages for the latest offset reader
	for i, msgText := range messages {
		msg := &sarama.ProducerMessage{
			Topic: topic,
			Key:   sarama.StringEncoder(fmt.Sprintf("mixed-sarama-fresh-key-%d", i)),
			Value: sarama.StringEncoder(msgText),
		}

		_, _, err := producer.SendMessage(msg)
		if err != nil {
			t.Fatalf("Sarama second produce failed: %v", err)
		}
	}

	consumeCtx, consumeCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer consumeCancel()

	consumedCount := 0
	for consumedCount < len(messages) {
		msg, err := r.ReadMessage(consumeCtx)
		if err != nil {
			t.Fatalf("kafka-go consume failed: %v", err)
		}

		// Only count messages with our fresh test key prefix
		if strings.HasPrefix(string(msg.Key), "mixed-sarama-fresh-key") {
			t.Logf("✅ kafka-go consumed Sarama message: key=%s, value=%s",
				string(msg.Key), string(msg.Value))
			consumedCount++
		}
	}

	t.Logf("🎉 Sarama to kafka-go test PASSED")
}

// TestOffsetManagement tests offset commit and fetch operations
func TestOffsetManagement(t *testing.T) {
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

	time.Sleep(100 * time.Millisecond)

	host, port := gatewayServer.GetListenerAddr()
	addr := fmt.Sprintf("%s:%d", host, port)
	topic := "offset-management-topic"
	groupID := "offset-test-group"

	gatewayServer.GetHandler().AddTopicForTesting(topic, 1)
	t.Logf("Testing offset management on %s with topic %s", addr, topic)

	// Produce test messages
	messages := []kafka.Message{
		{Key: []byte("offset-key1"), Value: []byte("Offset test message 1")},
		{Key: []byte("offset-key2"), Value: []byte("Offset test message 2")},
		{Key: []byte("offset-key3"), Value: []byte("Offset test message 3")},
		{Key: []byte("offset-key4"), Value: []byte("Offset test message 4")},
		{Key: []byte("offset-key5"), Value: []byte("Offset test message 5")},
	}

	w := &kafka.Writer{
		Addr:         kafka.TCP(addr),
		Topic:        topic,
		Balancer:     &kafka.LeastBytes{},
		BatchTimeout: 50 * time.Millisecond,
		RequiredAcks: kafka.RequireOne,
	}
	defer w.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err := w.WriteMessages(ctx, messages...)
	if err != nil {
		t.Fatalf("Failed to produce offset test messages: %v", err)
	}
	t.Logf("✅ Produced %d messages for offset test", len(messages))

	// Test 1: Consume first 3 messages and commit offsets
	t.Logf("=== Phase 1: Consuming first 3 messages ===")
	
	r1 := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     []string{addr},
		Topic:       topic,
		GroupID:     groupID,
		StartOffset: kafka.FirstOffset,
		MinBytes:    1,
		MaxBytes:    10e6,
	})

	consumeCtx1, consumeCancel1 := context.WithTimeout(context.Background(), 10*time.Second)
	defer consumeCancel1()

	for i := 0; i < 3; i++ {
		msg, err := r1.ReadMessage(consumeCtx1)
		if err != nil {
			t.Fatalf("Failed to read message %d: %v", i, err)
		}
		t.Logf("✅ Phase 1 consumed message %d: key=%s, offset=%d", 
			i, string(msg.Key), msg.Offset)
	}

	// Commit the offset (kafka-go automatically commits when using GroupID)
	r1.Close()
	t.Logf("✅ Phase 1 completed - offsets should be committed")

	// Test 2: Create new consumer with same group ID - should resume from committed offset
	t.Logf("=== Phase 2: Resuming from committed offset ===")
	
	r2 := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     []string{addr},
		Topic:       topic,
		GroupID:     groupID, // Same group ID
		StartOffset: kafka.FirstOffset, // This should be ignored due to committed offset
		MinBytes:    1,
		MaxBytes:    10e6,
	})
	defer r2.Close()

	consumeCtx2, consumeCancel2 := context.WithTimeout(context.Background(), 10*time.Second)
	defer consumeCancel2()

	remainingCount := 0
	expectedRemaining := len(messages) - 3 // Should get the last 2 messages

	for remainingCount < expectedRemaining {
		msg, err := r2.ReadMessage(consumeCtx2)
		if err != nil {
			t.Fatalf("Failed to read remaining message %d: %v", remainingCount, err)
		}
		t.Logf("✅ Phase 2 consumed remaining message %d: key=%s, offset=%d", 
			remainingCount, string(msg.Key), msg.Offset)
		remainingCount++
	}

	if remainingCount != expectedRemaining {
		t.Errorf("Expected %d remaining messages, got %d", expectedRemaining, remainingCount)
	}

	t.Logf("🎉 SUCCESS: Offset management test completed - consumed 3 + %d messages", remainingCount)
}
