package main

import (
	"flag"
	"fmt"
	"log"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/mq/client/agent_client"
	"github.com/seaweedfs/seaweedfs/weed/mq/schema"
	"github.com/seaweedfs/seaweedfs/weed/pb/schema_pb"
)

var (
	agentAddr      = flag.String("agent", "localhost:16777", "MQ agent address")
	topicNamespace = flag.String("namespace", "test", "topic namespace")
	topicName      = flag.String("topic", "test-topic", "topic name")
	partitionCount = flag.Int("partitions", 4, "number of partitions")
	messageCount   = flag.Int("messages", 100, "number of messages to produce")
	publisherName  = flag.String("publisher", "test-producer", "publisher name")
	messageSize    = flag.Int("size", 1024, "message size in bytes")
	interval       = flag.Duration("interval", 100*time.Millisecond, "interval between messages")
)

// TestMessage represents the structure of messages we'll be sending
type TestMessage struct {
	ID        int64  `json:"id"`
	Message   string `json:"message"`
	Payload   []byte `json:"payload"`
	Timestamp int64  `json:"timestamp"`
}

func main() {
	flag.Parse()

	fmt.Printf("Starting message producer:\n")
	fmt.Printf("  Agent: %s\n", *agentAddr)
	fmt.Printf("  Topic: %s.%s\n", *topicNamespace, *topicName)
	fmt.Printf("  Partitions: %d\n", *partitionCount)
	fmt.Printf("  Messages: %d\n", *messageCount)
	fmt.Printf("  Publisher: %s\n", *publisherName)
	fmt.Printf("  Message Size: %d bytes\n", *messageSize)
	fmt.Printf("  Interval: %v\n", *interval)

	// Create an instance of the message struct to generate schema from
	messageInstance := TestMessage{}

	// Automatically generate RecordType from the struct
	recordType := schema.StructToSchema(messageInstance)
	if recordType == nil {
		log.Fatalf("Failed to generate schema from struct")
	}

	fmt.Printf("\nGenerated schema with %d fields:\n", len(recordType.Fields))
	for _, field := range recordType.Fields {
		fmt.Printf("  - %s: %s\n", field.Name, getTypeString(field.Type))
	}

	topicSchema := schema.NewSchema(*topicNamespace, *topicName, recordType)

	// Create publish session
	session, err := agent_client.NewPublishSession(*agentAddr, topicSchema, *partitionCount, *publisherName)
	if err != nil {
		log.Fatalf("Failed to create publish session: %v", err)
	}
	defer session.CloseSession()

	// Create message payload
	payload := make([]byte, *messageSize)
	for i := range payload {
		payload[i] = byte(i % 256)
	}

	// Start producing messages
	fmt.Printf("\nStarting to produce messages...\n")
	startTime := time.Now()

	for i := 0; i < *messageCount; i++ {
		key := fmt.Sprintf("key-%d", i)

		// Create a message struct
		message := TestMessage{
			ID:        int64(i),
			Message:   fmt.Sprintf("This is message number %d", i),
			Payload:   payload[:min(100, len(payload))], // First 100 bytes
			Timestamp: time.Now().UnixNano(),
		}

		// Convert struct to RecordValue
		record := structToRecordValue(message)

		err := session.PublishMessageRecord([]byte(key), record)
		if err != nil {
			log.Printf("Failed to publish message %d: %v", i, err)
			continue
		}

		if (i+1)%10 == 0 {
			fmt.Printf("Published %d messages\n", i+1)
		}

		if *interval > 0 {
			time.Sleep(*interval)
		}
	}

	duration := time.Since(startTime)
	fmt.Printf("\nCompleted producing %d messages in %v\n", *messageCount, duration)
	fmt.Printf("Throughput: %.2f messages/sec\n", float64(*messageCount)/duration.Seconds())
}

// Helper function to convert struct to RecordValue
func structToRecordValue(msg TestMessage) *schema_pb.RecordValue {
	return &schema_pb.RecordValue{
		Fields: map[string]*schema_pb.Value{
			"ID": {
				Kind: &schema_pb.Value_Int64Value{
					Int64Value: msg.ID,
				},
			},
			"Message": {
				Kind: &schema_pb.Value_StringValue{
					StringValue: msg.Message,
				},
			},
			"Payload": {
				Kind: &schema_pb.Value_BytesValue{
					BytesValue: msg.Payload,
				},
			},
			"Timestamp": {
				Kind: &schema_pb.Value_Int64Value{
					Int64Value: msg.Timestamp,
				},
			},
		},
	}
}

func getTypeString(t *schema_pb.Type) string {
	switch kind := t.Kind.(type) {
	case *schema_pb.Type_ScalarType:
		switch kind.ScalarType {
		case schema_pb.ScalarType_BOOL:
			return "bool"
		case schema_pb.ScalarType_INT32:
			return "int32"
		case schema_pb.ScalarType_INT64:
			return "int64"
		case schema_pb.ScalarType_FLOAT:
			return "float"
		case schema_pb.ScalarType_DOUBLE:
			return "double"
		case schema_pb.ScalarType_BYTES:
			return "bytes"
		case schema_pb.ScalarType_STRING:
			return "string"
		}
	case *schema_pb.Type_ListType:
		return fmt.Sprintf("list<%s>", getTypeString(kind.ListType.ElementType))
	case *schema_pb.Type_RecordType:
		return "record"
	}
	return "unknown"
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
