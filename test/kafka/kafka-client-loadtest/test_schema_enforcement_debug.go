package main

import (
	"fmt"
	"time"

	"github.com/IBM/sarama"
)

func main() {
	fmt.Println("🔍 Debug Schema Enforcement")
	fmt.Println("Testing schema enforcement with detailed logging")

	// Test 1: Try to create a regular topic - should fail
	fmt.Println("\n1️⃣  Test 1: Create regular topic (should fail)")
	if err := testCreateRegularTopic(); err != nil {
		fmt.Printf("✅ Regular topic creation failed as expected: %v\n", err)
	} else {
		fmt.Println("❌ Regular topic creation should have failed")
	}

	// Test 2: Try to produce to a non-existent regular topic (auto-create)
	fmt.Println("\n2️⃣  Test 2: Auto-create regular topic via produce (should fail)")
	if err := testProduceToRegularTopic(); err != nil {
		fmt.Printf("✅ Auto-creation via produce failed as expected: %v\n", err)
	} else {
		fmt.Println("❌ Auto-creation via produce should have failed")
	}

	// Test 3: Create system topic - should succeed
	fmt.Println("\n3️⃣  Test 3: Create system topic (should succeed)")
	if err := testCreateSystemTopic(); err != nil {
		fmt.Printf("❌ System topic creation failed: %v\n", err)
	} else {
		fmt.Println("✅ System topic creation succeeded")
	}

	// Test 4: Produce to system topic - should succeed
	fmt.Println("\n4️⃣  Test 4: Produce to system topic (should succeed)")
	if err := testProduceToSystemTopic(); err != nil {
		fmt.Printf("❌ System topic produce failed: %v\n", err)
	} else {
		fmt.Println("✅ System topic produce succeeded")
	}

	fmt.Println("\n📋 Debug Summary:")
	fmt.Println("- Regular topic creation behavior")
	fmt.Println("- Auto-creation behavior")
	fmt.Println("- System topic behavior")
}

func testCreateRegularTopic() error {
	fmt.Println("   📋 Creating admin client...")
	config := sarama.NewConfig()
	config.Version = sarama.V2_8_0_0

	admin, err := sarama.NewClusterAdmin([]string{"kafka-gateway:9093"}, config)
	if err != nil {
		return fmt.Errorf("failed to create admin client: %v", err)
	}
	defer admin.Close()

	topicName := "debug-regular-topic"
	fmt.Printf("   📋 Creating topic: %s\n", topicName)
	
	topicDetail := &sarama.TopicDetail{
		NumPartitions:     1,
		ReplicationFactor: 1,
	}

	err = admin.CreateTopic(topicName, topicDetail, false)
	fmt.Printf("   📋 CreateTopic result: %v\n", err)
	return err
}

func testProduceToRegularTopic() error {
	fmt.Println("   📋 Creating producer...")
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	config.Producer.Return.Errors = true
	config.Version = sarama.V2_8_0_0

	producer, err := sarama.NewSyncProducer([]string{"kafka-gateway:9093"}, config)
	if err != nil {
		return fmt.Errorf("failed to create producer: %v", err)
	}
	defer producer.Close()

	topicName := "debug-auto-create-topic"
	fmt.Printf("   📋 Producing to topic: %s\n", topicName)

	msg := &sarama.ProducerMessage{
		Topic: topicName,
		Key:   sarama.StringEncoder("test-key"),
		Value: sarama.StringEncoder("test-value"),
	}

	partition, offset, err := producer.SendMessage(msg)
	fmt.Printf("   📋 SendMessage result: partition=%d, offset=%d, err=%v\n", partition, offset, err)
	return err
}

func testCreateSystemTopic() error {
	fmt.Println("   📋 Creating admin client...")
	config := sarama.NewConfig()
	config.Version = sarama.V2_8_0_0

	admin, err := sarama.NewClusterAdmin([]string{"kafka-gateway:9093"}, config)
	if err != nil {
		return fmt.Errorf("failed to create admin client: %v", err)
	}
	defer admin.Close()

	topicName := "__debug_system_topic"
	fmt.Printf("   📋 Creating system topic: %s\n", topicName)
	
	topicDetail := &sarama.TopicDetail{
		NumPartitions:     1,
		ReplicationFactor: 1,
	}

	err = admin.CreateTopic(topicName, topicDetail, false)
	fmt.Printf("   📋 CreateTopic result: %v\n", err)
	return err
}

func testProduceToSystemTopic() error {
	fmt.Println("   📋 Creating producer...")
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	config.Producer.Return.Errors = true
	config.Version = sarama.V2_8_0_0

	producer, err := sarama.NewSyncProducer([]string{"kafka-gateway:9093"}, config)
	if err != nil {
		return fmt.Errorf("failed to create producer: %v", err)
	}
	defer producer.Close()

	topicName := "__debug_system_topic"
	fmt.Printf("   📋 Producing to system topic: %s\n", topicName)

	msg := &sarama.ProducerMessage{
		Topic: topicName,
		Key:   sarama.StringEncoder("system-key"),
		Value: sarama.StringEncoder(`{"test": "system topic message"}`),
	}

	partition, offset, err := producer.SendMessage(msg)
	fmt.Printf("   📋 SendMessage result: partition=%d, offset=%d, err=%v\n", partition, offset, err)
	
	if err == nil {
		// Wait a moment then try to consume
		time.Sleep(2 * time.Second)
		return testConsumeFromSystemTopic(topicName)
	}
	
	return err
}

func testConsumeFromSystemTopic(topicName string) error {
	fmt.Println("   📋 Creating consumer...")
	config := sarama.NewConfig()
	config.Version = sarama.V2_8_0_0
	config.Consumer.Return.Errors = true
	config.Consumer.Offsets.Initial = sarama.OffsetOldest

	consumer, err := sarama.NewConsumer([]string{"kafka-gateway:9093"}, config)
	if err != nil {
		return fmt.Errorf("failed to create consumer: %v", err)
	}
	defer consumer.Close()

	partitionConsumer, err := consumer.ConsumePartition(topicName, 0, sarama.OffsetOldest)
	if err != nil {
		return fmt.Errorf("failed to create partition consumer: %v", err)
	}
	defer partitionConsumer.Close()

	select {
	case message := <-partitionConsumer.Messages():
		fmt.Printf("   📋 Consumed: Key=%s, Value=%s, Timestamp=%s\n", 
			string(message.Key), string(message.Value), message.Timestamp.Format("15:04:05"))
		return nil

	case err := <-partitionConsumer.Errors():
		return fmt.Errorf("consumer error: %v", err)

	case <-time.After(5 * time.Second):
		return fmt.Errorf("timeout waiting for message")
	}
}
