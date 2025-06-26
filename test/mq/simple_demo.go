package main

import (
	"fmt"
	"log"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/mq/client/pub_client"
	"github.com/seaweedfs/seaweedfs/weed/mq/topic"
)

func main() {
	log.Println("Starting SeaweedMQ logging test...")

	// Create publisher configuration
	config := &pub_client.PublisherConfiguration{
		Topic:          topic.NewTopic("test", "logging-demo"),
		PartitionCount: 3,
		Brokers:        []string{"127.0.0.1:17777"},
		PublisherName:  "logging-test-client",
	}

	log.Println("Creating topic publisher...")
	publisher, err := pub_client.NewTopicPublisher(config)
	if err != nil {
		log.Printf("Failed to create publisher: %v", err)
		return
	}
	defer publisher.Shutdown()

	log.Println("Publishing test messages...")

	// Publish some test messages
	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("key-%d", i)
		value := fmt.Sprintf("message-%d-timestamp-%d", i, time.Now().Unix())

		err := publisher.Publish([]byte(key), []byte(value))
		if err != nil {
			log.Printf("Failed to publish message %d: %v", i, err)
		}

		// Small delay to create some connection stress
		if i%10 == 0 {
			time.Sleep(10 * time.Millisecond)
		}
	}

	log.Println("Finishing publish...")
	err = publisher.FinishPublish()
	if err != nil {
		log.Printf("Failed to finish publish: %v", err)
	}

	log.Println("Test completed successfully!")
}
