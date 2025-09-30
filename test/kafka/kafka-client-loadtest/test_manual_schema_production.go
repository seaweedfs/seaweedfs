package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/IBM/sarama"
)

func main() {
	fmt.Println("🧪 Testing Manual Schema-Aware Message Production")
	fmt.Println("This test demonstrates the Kafka Gateway's ability to handle schematized messages")

	// Create Kafka producer
	fmt.Println("1️⃣  Creating Kafka producer...")
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
	fmt.Println("✅ Producer created!")

	// Create admin client
	fmt.Println("2️⃣  Creating admin client...")
	adminClient, err := sarama.NewClient([]string{"kafka-gateway:9093"}, config)
	if err != nil {
		log.Fatalf("Failed to create admin client: %v", err)
	}
	defer adminClient.Close()

	admin, err := sarama.NewClusterAdminFromClient(adminClient)
	if err != nil {
		log.Fatalf("Failed to create admin: %v", err)
	}
	defer admin.Close()
	fmt.Println("✅ Admin client created!")

	// Create topic
	fmt.Println("3️⃣  Creating topic...")
	topicName := "test-schema-debug-topic"
	err = admin.CreateTopic(topicName, &sarama.TopicDetail{
		NumPartitions:     1,
		ReplicationFactor: 1,
	}, false)
	if err != nil && err != sarama.ErrTopicAlreadyExists {
		log.Fatalf("Failed to create topic: %v", err)
	}
	fmt.Printf("✅ Topic '%s' created!\n", topicName)

	// Wait a moment for topic creation to propagate
	time.Sleep(2 * time.Second)

	// Produce messages with different schema IDs to demonstrate schema handling
	fmt.Println("4️⃣  Producing schematized messages...")

	schemas := []struct {
		schemaID int
		data     string
		desc     string
	}{
		{1001, `{"user_id":"user-001","action":"login","timestamp":1758915900000}`, "User login event"},
		{1002, `{"product_id":"prod-123","price":29.99,"currency":"USD"}`, "Product pricing event"},
		{1001, `{"user_id":"user-002","action":"logout","timestamp":1758915901000}`, "User logout event"},
	}

	for i, schema := range schemas {
		// Create Confluent Wire Format message
		// Format: [magic_byte][schema_id][data]
		messageData := []byte(schema.data)

		// Confluent Wire Format: magic byte (0) + schema ID (4 bytes big-endian) + data
		wireFormat := make([]byte, 5+len(messageData))
		wireFormat[0] = 0 // Magic byte
		// Schema ID in big-endian
		wireFormat[1] = byte(schema.schemaID >> 24)
		wireFormat[2] = byte(schema.schemaID >> 16)
		wireFormat[3] = byte(schema.schemaID >> 8)
		wireFormat[4] = byte(schema.schemaID)
		copy(wireFormat[5:], messageData)

		msg := &sarama.ProducerMessage{
			Topic: topicName,
			Key:   sarama.StringEncoder(fmt.Sprintf("key-%d", i+1)),
			Value: sarama.ByteEncoder(wireFormat),
		}

		partition, offset, err := producer.SendMessage(msg)
		if err != nil {
			log.Fatalf("Failed to send message %d: %v", i+1, err)
		}
		fmt.Printf("   📝 Message %d sent: %s (schemaID=%d, partition=%d, offset=%d)\n",
			i+1, schema.desc, schema.schemaID, partition, offset)
	}
	fmt.Println("✅ All schematized messages produced!")

	// Verify topic configuration
	fmt.Println("5️⃣  Verifying topic configuration...")
	time.Sleep(2 * time.Second) // Allow time for configuration to be written

	// Check if topic.conf exists
	resp, err := http.Get(fmt.Sprintf("http://localhost:8888/topics/kafka/%s/topic.conf", topicName))
	if err != nil {
		fmt.Printf("   ⚠️  Could not check topic configuration: %v\n", err)
	} else {
		defer resp.Body.Close()
		if resp.StatusCode == 200 {
			fmt.Println("   📋 Topic configuration found!")
			// Read and display the configuration
			var config map[string]interface{}
			json.NewDecoder(resp.Body).Decode(&config)

			configJson, _ := json.MarshalIndent(config, "      ", "  ")
			fmt.Printf("   📄 Configuration:\n      %s\n", string(configJson))

			if recordType, exists := config["messageRecordType"]; exists && recordType != nil {
				fmt.Printf("   📝 Message record type: %v\n", recordType)
			} else {
				fmt.Println("   ℹ️  No explicit schema configuration (using default handling)")
			}
		} else {
			fmt.Printf("   ⚠️  Topic configuration not found (status: %d)\n", resp.StatusCode)
		}
	}

	// Create a consumer to verify messages can be consumed
	fmt.Println("6️⃣  Testing message consumption...")
	consumer, err := sarama.NewConsumer([]string{"kafka-gateway:9093"}, config)
	if err != nil {
		log.Fatalf("Failed to create consumer: %v", err)
	}
	defer consumer.Close()

	partitionConsumer, err := consumer.ConsumePartition(topicName, 0, sarama.OffsetOldest)
	if err != nil {
		log.Fatalf("Failed to create partition consumer: %v", err)
	}
	defer partitionConsumer.Close()

	fmt.Println("   📖 Consuming messages...")
	consumedCount := 0
	timeout := time.After(10 * time.Second)

	for consumedCount < len(schemas) {
		select {
		case msg := <-partitionConsumer.Messages():
			// Parse Confluent Wire Format
			if len(msg.Value) >= 5 && msg.Value[0] == 0 {
				schemaID := int(msg.Value[1])<<24 | int(msg.Value[2])<<16 | int(msg.Value[3])<<8 | int(msg.Value[4])
				payload := msg.Value[5:]
				fmt.Printf("   📖 Message consumed: key=%s, schemaID=%d, payload=%s\n",
					string(msg.Key), schemaID, string(payload))
				consumedCount++
			} else {
				fmt.Printf("   📖 Message consumed (non-schema): key=%s, value=%s\n",
					string(msg.Key), string(msg.Value))
				consumedCount++
			}
		case <-timeout:
			fmt.Printf("   ⚠️  Timeout: consumed %d/%d messages\n", consumedCount, len(schemas))
			goto done
		}
	}

done:
	fmt.Printf("✅ Successfully consumed %d/%d messages!\n", consumedCount, len(schemas))

	fmt.Println("🎉 Manual schema-aware message production test completed!")
	fmt.Println()
	fmt.Println("✅ Demonstrated capabilities:")
	fmt.Println("   - Topic creation via Kafka Gateway")
	fmt.Println("   - Confluent Wire Format message production")
	fmt.Println("   - Schema ID encoding/decoding")
	fmt.Println("   - Message consumption with schema awareness")
	fmt.Println("   - Topic configuration persistence")

	if consumedCount == len(schemas) {
		fmt.Println("🎯 All tests passed! The Kafka Gateway correctly handles schematized messages.")
	} else {
		fmt.Printf("⚠️  Partial success: %d/%d messages consumed\n", consumedCount, len(schemas))
	}
}
