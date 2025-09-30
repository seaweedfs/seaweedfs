package main

import (
	"fmt"
	"log"
	"time"

	"github.com/IBM/sarama"
)

func main() {
	fmt.Println("🧪 Testing System Topic Schema After Restart")

	// Create Kafka admin client
	config := sarama.NewConfig()
	config.Version = sarama.V2_8_0_0

	admin, err := sarama.NewClusterAdmin([]string{"kafka-gateway:9093"}, config)
	if err != nil {
		log.Fatalf("Failed to create admin client: %v", err)
	}
	defer admin.Close()

	// Test creating a new system topic after restart
	systemTopicName := "__test_system_after_restart"
	topicDetail := &sarama.TopicDetail{
		NumPartitions:     2,
		ReplicationFactor: 1,
	}

	fmt.Printf("📝 Creating system topic after restart: %s\n", systemTopicName)
	err = admin.CreateTopic(systemTopicName, topicDetail, false)
	if err != nil {
		fmt.Printf("❌ Topic creation failed: %v\n", err)
		return
	} else {
		fmt.Printf("✅ System topic created: %s\n", systemTopicName)
	}

	// Wait a moment for topic creation
	time.Sleep(3 * time.Second)

	fmt.Printf("\n📋 Check topic configuration at:")
	fmt.Printf("   http://localhost:8888/topics/kafka/%s/topic.conf\n", systemTopicName)

	fmt.Println("\n🎯 Expected: messageRecordType with key/value BYTES fields and keyColumns: [\"key\"]")
	fmt.Println("\n✅ Test completed!")
}

