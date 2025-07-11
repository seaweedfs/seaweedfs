package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/mq/client/agent_client"
	"github.com/seaweedfs/seaweedfs/weed/mq/topic"
	"github.com/seaweedfs/seaweedfs/weed/pb/schema_pb"
)

var (
	agentAddr               = flag.String("agent", "localhost:16777", "MQ agent address")
	topicNamespace          = flag.String("namespace", "test", "topic namespace")
	topicName               = flag.String("topic", "test-topic", "topic name")
	consumerGroup           = flag.String("group", "test-consumer-group", "consumer group name")
	consumerGroupInstanceId = flag.String("instance", "test-consumer-1", "consumer group instance id")
	maxPartitions           = flag.Int("max-partitions", 10, "maximum number of partitions to consume")
	slidingWindowSize       = flag.Int("window-size", 100, "sliding window size for concurrent processing")
	offsetType              = flag.String("offset", "latest", "offset type: earliest, latest, timestamp")
	offsetTsNs              = flag.Int64("offset-ts", 0, "offset timestamp in nanoseconds (for timestamp offset type)")
	showMessages            = flag.Bool("show-messages", true, "show consumed messages")
	logProgress             = flag.Bool("log-progress", true, "log progress every 10 messages")
	filter                  = flag.String("filter", "", "message filter")
)

func main() {
	flag.Parse()

	fmt.Printf("Starting message consumer:\n")
	fmt.Printf("  Agent: %s\n", *agentAddr)
	fmt.Printf("  Topic: %s.%s\n", *topicNamespace, *topicName)
	fmt.Printf("  Consumer Group: %s\n", *consumerGroup)
	fmt.Printf("  Consumer Instance: %s\n", *consumerGroupInstanceId)
	fmt.Printf("  Max Partitions: %d\n", *maxPartitions)
	fmt.Printf("  Sliding Window Size: %d\n", *slidingWindowSize)
	fmt.Printf("  Offset Type: %s\n", *offsetType)
	fmt.Printf("  Filter: %s\n", *filter)

	// Create topic
	topicObj := topic.NewTopic(*topicNamespace, *topicName)

	// Determine offset type
	var pbOffsetType schema_pb.OffsetType
	switch *offsetType {
	case "earliest":
		pbOffsetType = schema_pb.OffsetType_RESET_TO_EARLIEST
	case "latest":
		pbOffsetType = schema_pb.OffsetType_RESET_TO_LATEST
	case "timestamp":
		pbOffsetType = schema_pb.OffsetType_EXACT_TS_NS
	default:
		pbOffsetType = schema_pb.OffsetType_RESET_TO_LATEST
	}

	// Create subscribe option
	option := &agent_client.SubscribeOption{
		ConsumerGroup:           *consumerGroup,
		ConsumerGroupInstanceId: *consumerGroupInstanceId,
		Topic:                   topicObj,
		OffsetType:              pbOffsetType,
		OffsetTsNs:              *offsetTsNs,
		Filter:                  *filter,
		MaxSubscribedPartitions: int32(*maxPartitions),
		SlidingWindowSize:       int32(*slidingWindowSize),
	}

	// Create subscribe session
	session, err := agent_client.NewSubscribeSession(*agentAddr, option)
	if err != nil {
		log.Fatalf("Failed to create subscribe session: %v", err)
	}
	defer session.CloseSession()

	// Statistics
	var messageCount int64
	var mu sync.Mutex
	startTime := time.Now()

	// Handle graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	// Channel to signal completion
	done := make(chan error, 1)

	// Start consuming messages
	fmt.Printf("\nStarting to consume messages...\n")
	go func() {
		err := session.SubscribeMessageRecord(
			// onEachMessageFn
			func(key []byte, record *schema_pb.RecordValue) {
				mu.Lock()
				messageCount++
				currentCount := messageCount
				mu.Unlock()

				if *showMessages {
					fmt.Printf("Received message: key=%s\n", string(key))
					printRecordValue(record)
				}

				if *logProgress && currentCount%10 == 0 {
					elapsed := time.Since(startTime)
					rate := float64(currentCount) / elapsed.Seconds()
					fmt.Printf("Consumed %d messages (%.2f msg/sec)\n", currentCount, rate)
				}
			},
			// onCompletionFn
			func() {
				fmt.Printf("Subscription completed\n")
				done <- nil
			},
		)
		if err != nil {
			done <- err
		}
	}()

	// Wait for signal or completion
	select {
	case <-sigChan:
		fmt.Printf("\nReceived shutdown signal, stopping consumer...\n")
	case err := <-done:
		if err != nil {
			log.Printf("Subscription error: %v", err)
		}
	}

	// Print final statistics
	mu.Lock()
	finalCount := messageCount
	mu.Unlock()

	duration := time.Since(startTime)
	fmt.Printf("Consumed %d messages in %v\n", finalCount, duration)
	if duration.Seconds() > 0 {
		fmt.Printf("Average throughput: %.2f messages/sec\n", float64(finalCount)/duration.Seconds())
	}
}

func printRecordValue(record *schema_pb.RecordValue) {
	if record == nil || record.Fields == nil {
		fmt.Printf("  (empty record)\n")
		return
	}

	for fieldName, value := range record.Fields {
		fmt.Printf("  %s: %s\n", fieldName, formatValue(value))
	}
}

func formatValue(value *schema_pb.Value) string {
	if value == nil {
		return "(nil)"
	}

	switch kind := value.Kind.(type) {
	case *schema_pb.Value_BoolValue:
		return fmt.Sprintf("%t", kind.BoolValue)
	case *schema_pb.Value_Int32Value:
		return fmt.Sprintf("%d", kind.Int32Value)
	case *schema_pb.Value_Int64Value:
		return fmt.Sprintf("%d", kind.Int64Value)
	case *schema_pb.Value_FloatValue:
		return fmt.Sprintf("%f", kind.FloatValue)
	case *schema_pb.Value_DoubleValue:
		return fmt.Sprintf("%f", kind.DoubleValue)
	case *schema_pb.Value_BytesValue:
		if len(kind.BytesValue) > 50 {
			return fmt.Sprintf("bytes[%d] %x...", len(kind.BytesValue), kind.BytesValue[:50])
		}
		return fmt.Sprintf("bytes[%d] %x", len(kind.BytesValue), kind.BytesValue)
	case *schema_pb.Value_StringValue:
		if len(kind.StringValue) > 100 {
			return fmt.Sprintf("\"%s...\"", kind.StringValue[:100])
		}
		return fmt.Sprintf("\"%s\"", kind.StringValue)
	case *schema_pb.Value_ListValue:
		return fmt.Sprintf("list[%d items]", len(kind.ListValue.Values))
	case *schema_pb.Value_RecordValue:
		return fmt.Sprintf("record[%d fields]", len(kind.RecordValue.Fields))
	default:
		return "(unknown)"
	}
}
