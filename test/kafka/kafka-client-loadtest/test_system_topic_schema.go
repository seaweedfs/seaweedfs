package main

import (
	"fmt"
	"log"
	"time"

	"github.com/IBM/sarama"
)

func main() {
	fmt.Println("🧪 Testing System Topic Default Schema")

	// Create Kafka admin client
	config := sarama.NewConfig()
	config.Version = sarama.V2_8_0_0

	admin, err := sarama.NewClusterAdmin([]string{"kafka-gateway:9093"}, config)
	if err != nil {
		log.Fatalf("Failed to create admin client: %v", err)
	}
	defer admin.Close()

	// Test creating a system topic
	systemTopicName := "__consumer_offsets"
	topicDetail := &sarama.TopicDetail{
		NumPartitions:     4,
		ReplicationFactor: 1,
	}

	fmt.Printf("📝 Creating system topic: %s\n", systemTopicName)
	err = admin.CreateTopic(systemTopicName, topicDetail, false)
	if err != nil {
		fmt.Printf("⚠️  Topic creation warning (may already exist): %v\n", err)
	} else {
		fmt.Printf("✅ System topic created: %s\n", systemTopicName)
	}

	// Wait a moment for topic creation
	time.Sleep(2 * time.Second)

	fmt.Println("\n🎯 Expected behavior:")
	fmt.Println("   - System topic should be created with default key/value bytes schema")
	fmt.Println("   - Topic configuration should show messageRecordType with key and value BYTES fields")
	fmt.Println("   - KeyColumns should be [\"key\"]")

	fmt.Printf("\n📋 Check topic configuration at:")
	fmt.Printf("   http://localhost:8888/topics/kafka/%s/topic.conf\n", systemTopicName)

	fmt.Println("\n✅ Test completed!")
}

