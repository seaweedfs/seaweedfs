package main

import (
	"flag"
	"fmt"
	"github.com/seaweedfs/seaweedfs/weed/mq/client/pub_client"
	"log"
	"strings"
	"sync"
	"time"
)

var (
	messageCount = flag.Int("n", 1000, "message count")
	concurrency  = flag.Int("c", 4, "concurrency count")

	namespace   = flag.String("ns", "test", "namespace")
	topic       = flag.String("topic", "test", "topic")
	seedBrokers = flag.String("brokers", "localhost:17777", "seed brokers")
)

func doPublish(publisher *pub_client.TopicPublisher, id int) {
	startTime := time.Now()
	for i := 0; i < *messageCount / *concurrency; i++ {
		// Simulate publishing a message
		key := []byte(fmt.Sprintf("key-%d-%d", id, i))
		value := []byte(fmt.Sprintf("value-%d-%d", id, i))
		publisher.Publish(key, value) // Call your publisher function here
		// println("Published", string(key), string(value))
	}
	elapsed := time.Since(startTime)
	log.Printf("Publisher %d finished in %s", id, elapsed)
}

func main() {
	flag.Parse()
	config := &pub_client.PublisherConfiguration{
		CreateTopic: true,
	}
	publisher := pub_client.NewTopicPublisher(*namespace, *topic, config)
	brokers := strings.Split(*seedBrokers, ",")
	if err := publisher.Connect(brokers); err != nil {
		fmt.Println(err)
		return
	}

	startTime := time.Now()

	// Start multiple publishers
	var wg sync.WaitGroup
	for i := 0; i < *concurrency; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			doPublish(publisher, id)
		}(i)
	}

	// Wait for all publishers to finish
	wg.Wait()
	elapsed := time.Since(startTime)
	publisher.Shutdown()

	log.Printf("Published %d messages in %s (%.2f msg/s)", *messageCount, elapsed, float64(*messageCount)/elapsed.Seconds())

}
