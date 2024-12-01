package main

import (
	"flag"
	"fmt"
	"github.com/seaweedfs/seaweedfs/weed/mq/client/agent_client"
	"github.com/seaweedfs/seaweedfs/weed/mq/schema"
	"github.com/seaweedfs/seaweedfs/weed/pb/schema_pb"
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

	namespace   = flag.String("ns", "test", "namespace")
	t           = flag.String("t", "test", "t")
	seedBrokers = flag.String("brokers", "localhost:17777", "seed brokers")

	counter int32
)

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

type MyRecord struct {
	Key    []byte
	Field1 []byte
	Field2 string
	Field3 int32
	Field4 int64
	Field5 float32
	Field6 float64
	Field7 bool
}

func genMyRecord(id int32) *MyRecord {
	return &MyRecord{
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

func (r *MyRecord) ToRecordValue() *schema_pb.RecordValue {
	return schema.RecordBegin().
		SetBytes("key", r.Key).
		SetBytes("field1", r.Field1).
		SetString("field2", r.Field2).
		SetInt32("field3", r.Field3).
		SetInt64("field4", r.Field4).
		SetFloat("field5", r.Field5).
		SetDouble("field6", r.Field6).
		SetBool("field7", r.Field7).
		RecordEnd()
}

func main() {
	flag.Parse()

	recordType := schema.RecordTypeBegin().
		WithField("key", schema.TypeBytes).
		WithField("field1", schema.TypeBytes).
		WithField("field2", schema.TypeString).
		WithField("field3", schema.TypeInt32).
		WithField("field4", schema.TypeInt64).
		WithField("field5", schema.TypeFloat).
		WithField("field6", schema.TypeDouble).
		WithField("field7", schema.TypeBoolean).
		RecordTypeEnd()

	session, err := agent_client.NewPublishSession("localhost:16777", schema.NewSchema(*namespace, *t, recordType), *partitionCount, *clientName)
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
