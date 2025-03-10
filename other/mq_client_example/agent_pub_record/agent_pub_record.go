package main

import (
	"flag"
	"fmt"
	"github.com/seaweedfs/seaweedfs/other/mq_client_example/example"
	"github.com/seaweedfs/seaweedfs/weed/mq/client/agent_client"
	"github.com/seaweedfs/seaweedfs/weed/mq/schema"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

var (
	messageCount   = flag.Int("n", 1000, "message count")
	messageDelay   = flag.Duration("d", time.Second, "delay between messages")
	concurrency    = flag.Int("c", 4, "concurrent publishers")
	partitionCount = flag.Int("p", 6, "partition count")

	clientName = flag.String("client", "c1", "client name")

	namespace = flag.String("ns", "test", "namespace")
	t         = flag.String("t", "test", "t")
	agent     = flag.String("agent", "localhost:16777", "mq agent address")

	counter int32
)

func genMyRecord(id int32) *example.MyRecord {
	return &example.MyRecord{
		Key:    []byte(fmt.Sprintf("key-%s-%d", *clientName, id)),
		Field1: []byte(fmt.Sprintf("field1-%s-%d", *clientName, id)),
		Field2: fmt.Sprintf("field2-%s-%d", *clientName, id),
		Field3: id,
		Field4: int64(id),
		Field5: float32(id),
		Field6: float64(id),
		Field7: id%2 == 0,
	}
}

func doPublish(publisher *agent_client.PublishSession, id int) {
	startTime := time.Now()
	for {
		i := atomic.AddInt32(&counter, 1)
		if i > int32(*messageCount) {
			break
		}
		// Simulate publishing a message
		myRecord := genMyRecord(int32(i))
		if err := publisher.PublishMessageRecord(myRecord.Key, myRecord.ToRecordValue()); err != nil {
			fmt.Println(err)
			break
		}
		if *messageDelay > 0 {
			time.Sleep(*messageDelay)
			fmt.Printf("sent %+v\n", string(myRecord.Key))
		}
	}
	elapsed := time.Since(startTime)
	log.Printf("Publisher %s-%d finished in %s", *clientName, id, elapsed)
}

func main() {
	flag.Parse()

	recordType := example.MyRecordType()

	session, err := agent_client.NewPublishSession(*agent, schema.NewSchema(*namespace, *t, recordType), *partitionCount, *clientName)
	if err != nil {
		log.Printf("failed to create session: %v", err)
		return
	}
	defer session.CloseSession()

	startTime := time.Now()

	var wg sync.WaitGroup
	// Start multiple publishers
	for i := 0; i < *concurrency; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			doPublish(session, id)
		}(i)
	}

	// Wait for all publishers to finish
	wg.Wait()
	elapsed := time.Since(startTime)

	log.Printf("Published %d messages in %s (%.2f msg/s)", *messageCount, elapsed, float64(*messageCount)/elapsed.Seconds())

}
