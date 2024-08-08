package main

import (
	"flag"
	"fmt"
	"github.com/seaweedfs/seaweedfs/weed/mq/client/pub_client"
	"github.com/seaweedfs/seaweedfs/weed/mq/topic"
	"log"
	"strings"
	"sync"
	"time"
	util_http "github.com/seaweedfs/seaweedfs/weed/util/http"
)

var (
	messageCount   = flag.Int("n", 1000, "message count")
	concurrency    = flag.Int("c", 4, "concurrent publishers")
	partitionCount = flag.Int("p", 6, "partition count")

	clientName = flag.String("client", "c1", "client name")

	namespace   = flag.String("ns", "test", "namespace")
	t           = flag.String("t", "test", "t")
	seedBrokers = flag.String("brokers", "localhost:17777", "seed brokers")
)

func doPublish(publisher *pub_client.TopicPublisher, id int) {
	startTime := time.Now()
	for i := 0; i < *messageCount / *concurrency; i++ {
		// Simulate publishing a message
		key := []byte(fmt.Sprintf("key-%s-%d-%d", *clientName, id, i))
		value := []byte(fmt.Sprintf("value-%s-%d-%d", *clientName, id, i))
		if err := publisher.Publish(key, value); err != nil {
			fmt.Println(err)
			break
		}
		time.Sleep(time.Second)
		// println("Published", string(key), string(value))
	}
	if err := publisher.FinishPublish(); err != nil {
		fmt.Println(err)
	}
	elapsed := time.Since(startTime)
	log.Printf("Publisher %s-%d finished in %s", *clientName, id, elapsed)
}

func main() {
	flag.Parse()
	util_http.InitGlobalHttpClient()

	config := &pub_client.PublisherConfiguration{
		Topic:          topic.NewTopic(*namespace, *t),
		PartitionCount: int32(*partitionCount),
		Brokers:        strings.Split(*seedBrokers, ","),
		PublisherName:  *clientName,
	}
	publisher := pub_client.NewTopicPublisher(config)

	startTime := time.Now()

	var wg sync.WaitGroup
	// Start multiple publishers
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
