package main

import (
	"flag"
	"fmt"
	"github.com/seaweedfs/seaweedfs/other/mq_client_example/example"
	"github.com/seaweedfs/seaweedfs/weed/mq/client/agent_client"
	"github.com/seaweedfs/seaweedfs/weed/mq/topic"
	"github.com/seaweedfs/seaweedfs/weed/pb/schema_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"log"
	"time"
)

var (
	namespace         = flag.String("ns", "test", "namespace")
	t                 = flag.String("topic", "test", "topic")
	agent             = flag.String("agent", "localhost:16777", "mq agent address")
	maxPartitionCount = flag.Int("maxPartitionCount", 3, "max partition count")
	slidingWindowSize = flag.Int("slidingWindowSize", 1, "per partition concurrency")
	timeAgo           = flag.Duration("timeAgo", 0, "start time before now. \"300ms\", \"1.5h\" or \"2h45m\". Valid time units are \"ns\", \"us\" (or \"µs\"), \"ms\", \"s\", \"m\", \"h\"")

	clientId = flag.Uint("client_id", uint(util.RandomInt32()), "client id")
)

func main() {
	flag.Parse()

	// determine the start of the messages
	var startTsNs int64
	startType := schema_pb.OffsetType_RESUME_OR_EARLIEST
	if *timeAgo > 0 {
		startTsNs = time.Now().Add(-*timeAgo).UnixNano()
		startType = schema_pb.OffsetType_EXACT_TS_NS
	}

	session, err := agent_client.NewSubscribeSession(*agent, &agent_client.SubscribeOption{
		ConsumerGroup:           "test",
		ConsumerGroupInstanceId: fmt.Sprintf("client-%d", *clientId),
		Topic:                   topic.NewTopic(*namespace, *t),
		OffsetType:              startType,
		OffsetTsNs:              startTsNs,
		Filter:                  "",
		MaxSubscribedPartitions: int32(*maxPartitionCount),
		SlidingWindowSize:       int32(*slidingWindowSize),
	})
	if err != nil {
		log.Printf("new subscribe session: %v", err)
		return
	}
	defer session.CloseSession()

	counter := 0
	session.SubscribeMessageRecord(func(key []byte, recordValue *schema_pb.RecordValue) {
		counter++
		record := example.FromRecordValue(recordValue)
		fmt.Printf("%d %s %v\n", counter, string(key), record.Field2)
	}, func() {
		log.Printf("done received %d messages", counter)
	})

}
