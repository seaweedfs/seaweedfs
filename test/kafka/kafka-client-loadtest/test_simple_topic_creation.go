package main

import (
	"fmt"
	"time"

	"github.com/IBM/sarama"
)

func main() {
	fmt.Println("🧪 Testing Simple Topic Creation with Schema Management")

	// Create Kafka admin client
	fmt.Println("1️⃣  Creating Kafka admin client...")

	config := sarama.NewConfig()
	config.Version = sarama.V2_6_0_0

	admin, err := sarama.NewClusterAdmin([]string{"kafka-gateway:9093"}, config)
	if err != nil {
		fmt.Printf("❌ Failed to create admin client: %v\n", err)
		return
	}
	defer admin.Close()

	fmt.Println("✅ Admin client created!")

	// Create topic with unique name
	topicName := fmt.Sprintf("test-schema-topic-%d", time.Now().Unix())
	fmt.Printf("2️⃣  Creating topic: %s\n", topicName)

	err = admin.CreateTopic(topicName, &sarama.TopicDetail{
		NumPartitions:     1,
		ReplicationFactor: 1,
	}, false)

	if err != nil {
		fmt.Printf("❌ Failed to create topic: %v\n", err)
		return
	}

	fmt.Printf("✅ Topic '%s' created!\n", topicName)

	// Wait a bit for topic to be fully created
	fmt.Println("3️⃣  Waiting for topic initialization...")
	time.Sleep(3 * time.Second)

	fmt.Printf("🎉 Topic creation test completed! Topic name: %s\n", topicName)
	fmt.Printf("📋 Check topic configuration at: http://localhost:8888/topics/kafka/%s/topic.conf\n", topicName)
}


