package main

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/IBM/sarama"
)

func main() {
	fmt.Println("Testing offset handling for multiple consumers...")

	// Create producer
	config := sarama.NewConfig()
	config.Version = sarama.V2_8_0_0
	config.Producer.Return.Successes = true
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Retry.Max = 3

	producer, err := sarama.NewSyncProducer([]string{"kafka-gateway:9093"}, config)
	if err != nil {
		log.Fatalf("Failed to create producer: %v", err)
	}
	defer producer.Close()

	// Create admin client
	adminClient, err := sarama.NewClusterAdmin([]string{"kafka-gateway:9093"}, config)
	if err != nil {
		log.Fatalf("Failed to create admin client: %v", err)
	}
	defer adminClient.Close()

	// Create a unique topic name
	topicName := fmt.Sprintf("test-offset-handling-%d", time.Now().Unix())
	fmt.Printf("Creating topic: %s\n", topicName)

	err = adminClient.CreateTopic(topicName, &sarama.TopicDetail{
		NumPartitions:     2, // Use 2 partitions to test partition assignment
		ReplicationFactor: 1,
	}, false)
	if err != nil {
		log.Fatalf("Failed to create topic: %v", err)
	}
	fmt.Println("Topic created successfully")

	// Produce 10 messages
	fmt.Println("Producing 10 messages...")
	for i := 0; i < 10; i++ {
		message := fmt.Sprintf("offset-test-message-%d", i)
		_, _, err := producer.SendMessage(&sarama.ProducerMessage{
			Topic: topicName,
			Key:   sarama.StringEncoder(fmt.Sprintf("key-%d", i)),
			Value: sarama.StringEncoder(message),
		})
		if err != nil {
			log.Fatalf("Failed to send message: %v", err)
		}
		fmt.Printf("Produced message %d: %s\n", i, message)
	}

	// Wait for messages to be written
	time.Sleep(2 * time.Second)

	// Test 1: Multiple consumers in the same consumer group
	fmt.Println("\n=== Test 1: Multiple consumers in same group ===")
	testSameConsumerGroup(topicName)

	// Wait a bit
	time.Sleep(2 * time.Second)

	// Test 2: Different consumer groups
	fmt.Println("\n=== Test 2: Different consumer groups ===")
	testDifferentConsumerGroups(topicName)

	// Wait a bit
	time.Sleep(2 * time.Second)

	// Test 3: Consumer group rebalancing
	fmt.Println("\n=== Test 3: Consumer group rebalancing ===")
	testConsumerGroupRebalancing(topicName)

	// Test 4: Offset persistence across restarts
	fmt.Println("\n=== Test 4: Offset persistence ===")
	testOffsetPersistence(topicName)
}

func testSameConsumerGroup(topicName string) {
	config := sarama.NewConfig()
	config.Version = sarama.V2_8_0_0
	config.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategyRoundRobin
	config.Consumer.Offsets.Initial = sarama.OffsetOldest

	// Create consumer group
	consumer1, err := sarama.NewConsumerGroup([]string{"kafka-gateway:9093"}, "test-group-1", config)
	if err != nil {
		log.Printf("Failed to create consumer 1: %v", err)
		return
	}
	defer consumer1.Close()

	consumer2, err := sarama.NewConsumerGroup([]string{"kafka-gateway:9093"}, "test-group-1", config)
	if err != nil {
		log.Printf("Failed to create consumer 2: %v", err)
		return
	}
	defer consumer2.Close()

	// Consumer handler
	handler1 := &TestConsumerGroupHandler{id: "consumer-1", group: "test-group-1"}
	handler2 := &TestConsumerGroupHandler{id: "consumer-2", group: "test-group-1"}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Start consumers
	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		for {
			select {
			case <-ctx.Done():
				return
			default:
				err := consumer1.Consume(ctx, []string{topicName}, handler1)
				if err != nil {
					log.Printf("Consumer 1 error: %v", err)
					return
				}
			}
		}
	}()

	go func() {
		defer wg.Done()
		for {
			select {
			case <-ctx.Done():
				return
			default:
				err := consumer2.Consume(ctx, []string{topicName}, handler2)
				if err != nil {
					log.Printf("Consumer 2 error: %v", err)
					return
				}
			}
		}
	}()

	// Wait for consumers to process messages
	time.Sleep(8 * time.Second)
	cancel()
	wg.Wait()

	fmt.Printf("Consumer 1 received %d messages\n", handler1.messageCount)
	fmt.Printf("Consumer 2 received %d messages\n", handler2.messageCount)
	fmt.Printf("Total messages processed: %d\n", handler1.messageCount+handler2.messageCount)
}

func testDifferentConsumerGroups(topicName string) {
	config := sarama.NewConfig()
	config.Version = sarama.V2_8_0_0
	config.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategyRoundRobin
	config.Consumer.Offsets.Initial = sarama.OffsetOldest

	// Create different consumer groups
	consumer1, err := sarama.NewConsumerGroup([]string{"kafka-gateway:9093"}, "test-group-2", config)
	if err != nil {
		log.Printf("Failed to create consumer 1: %v", err)
		return
	}
	defer consumer1.Close()

	consumer2, err := sarama.NewConsumerGroup([]string{"kafka-gateway:9093"}, "test-group-3", config)
	if err != nil {
		log.Printf("Failed to create consumer 2: %v", err)
		return
	}
	defer consumer2.Close()

	// Consumer handlers
	handler1 := &TestConsumerGroupHandler{id: "consumer-1", group: "test-group-2"}
	handler2 := &TestConsumerGroupHandler{id: "consumer-2", group: "test-group-3"}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Start consumers
	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		for {
			select {
			case <-ctx.Done():
				return
			default:
				err := consumer1.Consume(ctx, []string{topicName}, handler1)
				if err != nil {
					log.Printf("Consumer 1 error: %v", err)
					return
				}
			}
		}
	}()

	go func() {
		defer wg.Done()
		for {
			select {
			case <-ctx.Done():
				return
			default:
				err := consumer2.Consume(ctx, []string{topicName}, handler2)
				if err != nil {
					log.Printf("Consumer 2 error: %v", err)
					return
				}
			}
		}
	}()

	// Wait for consumers to process messages
	time.Sleep(8 * time.Second)
	cancel()
	wg.Wait()

	fmt.Printf("Group 2 consumer received %d messages\n", handler1.messageCount)
	fmt.Printf("Group 3 consumer received %d messages\n", handler2.messageCount)
	fmt.Printf("Total messages processed: %d\n", handler1.messageCount+handler2.messageCount)
}

func testConsumerGroupRebalancing(topicName string) {
	config := sarama.NewConfig()
	config.Version = sarama.V2_8_0_0
	config.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategyRoundRobin
	config.Consumer.Offsets.Initial = sarama.OffsetOldest

	// Start with one consumer
	consumer1, err := sarama.NewConsumerGroup([]string{"kafka-gateway:9093"}, "test-group-4", config)
	if err != nil {
		log.Printf("Failed to create consumer 1: %v", err)
		return
	}
	defer consumer1.Close()

	handler1 := &TestConsumerGroupHandler{id: "consumer-1", group: "test-group-4"}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	// Start first consumer
	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		defer wg.Done()
		for {
			select {
			case <-ctx.Done():
				return
			default:
				err := consumer1.Consume(ctx, []string{topicName}, handler1)
				if err != nil {
					log.Printf("Consumer 1 error: %v", err)
					return
				}
			}
		}
	}()

	// Let first consumer run for 3 seconds
	time.Sleep(3 * time.Second)
	fmt.Printf("After 3 seconds, consumer 1 received %d messages\n", handler1.messageCount)

	// Add second consumer
	consumer2, err := sarama.NewConsumerGroup([]string{"kafka-gateway:9093"}, "test-group-4", config)
	if err != nil {
		log.Printf("Failed to create consumer 2: %v", err)
		return
	}
	defer consumer2.Close()

	handler2 := &TestConsumerGroupHandler{id: "consumer-2", group: "test-group-4"}
	wg.Add(1)

	go func() {
		defer wg.Done()
		for {
			select {
			case <-ctx.Done():
				return
			default:
				err := consumer2.Consume(ctx, []string{topicName}, handler2)
				if err != nil {
					log.Printf("Consumer 2 error: %v", err)
					return
				}
			}
		}
	}()

	// Let both consumers run for 5 more seconds
	time.Sleep(5 * time.Second)
	fmt.Printf("After rebalancing, consumer 1 received %d messages\n", handler1.messageCount)
	fmt.Printf("After rebalancing, consumer 2 received %d messages\n", handler2.messageCount)

	cancel()
	wg.Wait()
}

func testOffsetPersistence(topicName string) {
	config := sarama.NewConfig()
	config.Version = sarama.V2_8_0_0
	config.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategyRoundRobin
	config.Consumer.Offsets.Initial = sarama.OffsetOldest

	// Create consumer group
	consumer, err := sarama.NewConsumerGroup([]string{"kafka-gateway:9093"}, "test-group-5", config)
	if err != nil {
		log.Printf("Failed to create consumer: %v", err)
		return
	}
	defer consumer.Close()

	handler := &TestConsumerGroupHandler{id: "consumer-1", group: "test-group-5"}

	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()

	// Start consumer
	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		defer wg.Done()
		for {
			select {
			case <-ctx.Done():
				return
			default:
				err := consumer.Consume(ctx, []string{topicName}, handler)
				if err != nil {
					log.Printf("Consumer error: %v", err)
					return
				}
			}
		}
	}()

	// Let consumer process some messages
	time.Sleep(5 * time.Second)
	cancel()
	wg.Wait()

	fmt.Printf("Consumer processed %d messages before restart\n", handler.messageCount)

	// Restart consumer (simulate Kafka Gateway restart)
	fmt.Println("Simulating consumer restart...")
	time.Sleep(2 * time.Second)

	// Create new consumer with same group
	consumer2, err := sarama.NewConsumerGroup([]string{"kafka-gateway:9093"}, "test-group-5", config)
	if err != nil {
		log.Printf("Failed to create consumer after restart: %v", err)
		return
	}
	defer consumer2.Close()

	handler2 := &TestConsumerGroupHandler{id: "consumer-2", group: "test-group-5"}

	ctx2, cancel2 := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel2()

	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-ctx2.Done():
				return
			default:
				err := consumer2.Consume(ctx2, []string{topicName}, handler2)
				if err != nil {
					log.Printf("Consumer after restart error: %v", err)
					return
				}
			}
		}
	}()

	time.Sleep(5 * time.Second)
	cancel2()
	wg.Wait()

	fmt.Printf("Consumer after restart processed %d messages\n", handler2.messageCount)
	fmt.Printf("Total messages processed: %d\n", handler.messageCount+handler2.messageCount)
}

type TestConsumerGroupHandler struct {
	id           string
	group        string
	messageCount int
}

func (h *TestConsumerGroupHandler) Setup(sarama.ConsumerGroupSession) error {
	fmt.Printf("%s: Setup for group %s\n", h.id, h.group)
	return nil
}

func (h *TestConsumerGroupHandler) Cleanup(sarama.ConsumerGroupSession) error {
	fmt.Printf("%s: Cleanup for group %s\n", h.id, h.group)
	return nil
}

func (h *TestConsumerGroupHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	fmt.Printf("%s: Consuming from partition %d, offset %d to %d\n",
		h.id, claim.Partition(), claim.InitialOffset(), claim.HighWaterMarkOffset())

	for message := range claim.Messages() {
		h.messageCount++
		fmt.Printf("%s: Received message %d: %s (offset: %d, partition: %d)\n",
			h.id, h.messageCount, string(message.Value), message.Offset, message.Partition)
		session.MarkMessage(message, "")
	}
	return nil
}


