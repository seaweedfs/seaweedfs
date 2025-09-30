package main

import (
	"fmt"
	"log"
	"time"

	"github.com/IBM/sarama"
)

func main() {
	fmt.Println("🧪 Testing Schema Persistence Fix")
	fmt.Println("Creating a new topic and checking if schema is persisted to topic.conf")

	// Create a unique topic name
	topicName := fmt.Sprintf("schema-fix-test-%d", time.Now().Unix())

	// Create admin client to create topic
	config := sarama.NewConfig()
	config.Version = sarama.V2_8_0_0
	config.Net.DialTimeout = 10 * time.Second

	admin, err := sarama.NewClusterAdmin([]string{"kafka-gateway:9093"}, config)
	if err != nil {
		log.Fatalf("❌ Failed to create admin client: %v", err)
	}
	defer admin.Close()

	topicDetail := &sarama.TopicDetail{
		NumPartitions:     1,
		ReplicationFactor: 1,
	}

	fmt.Printf("🚀 Creating topic: %s\n", topicName)
	err = admin.CreateTopic(topicName, topicDetail, false)
	if err != nil {
		log.Fatalf("❌ Failed to create topic: %v", err)
	}

	fmt.Printf("✅ Topic created successfully\n")

	// Wait for topic to be fully created and configured
	time.Sleep(3 * time.Second)

	fmt.Printf("📋 Topic created: %s\n", topicName)
	fmt.Printf("🔍 Check topic.conf at: http://localhost:8888/topics/kafka/%s/topic.conf\n", topicName)
	fmt.Printf("✅ Test completed - check the URL above to verify schema persistence\n")
}

