package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/IBM/sarama"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/seaweedfs/seaweedfs/weed/pb/mq_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/schema_pb"
)

const (
	brokerAddress     = "localhost:9093"
	grpcBrokerAddress = "localhost:17777"
	topic             = "test-grpc-range-info-new"
	partition         = 0
)

func main() {
	fmt.Println("🧪 Testing GetPartitionRangeInfo gRPC API directly")

	// First, create topic and produce some messages via Kafka
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Timeout = 5 * time.Second

	client, err := sarama.NewClient([]string{brokerAddress}, config)
	if err != nil {
		log.Fatalf("Failed to create Kafka client: %v", err)
	}
	defer client.Close()

	// Create topic
	admin, err := sarama.NewClusterAdminFromClient(client)
	if err != nil {
		log.Fatalf("Failed to create Kafka cluster admin: %v", err)
	}
	defer admin.Close()

	topicDetail := &sarama.TopicDetail{
		NumPartitions:     1,
		ReplicationFactor: 1,
	}
	err = admin.CreateTopic(topic, topicDetail, false)
	if err != nil && err.Error() != "kafka server: Topic with this name already exists" {
		log.Fatalf("Failed to create topic %s: %v", topic, err)
	}
	fmt.Printf("✅ Topic ready: %s\n", topic)

	// Produce some messages
	fmt.Println("\n📝 Producing 2 messages...")
	producer, err := sarama.NewSyncProducerFromClient(client)
	if err != nil {
		log.Fatalf("Failed to create producer: %v", err)
	}
	defer producer.Close()

	baseTime := time.Now()
	for i := 0; i < 2; i++ {
		msg := &sarama.ProducerMessage{
			Topic:     topic,
			Partition: partition,
			Key:       sarama.StringEncoder(fmt.Sprintf("key-%d", i)),
			Value:     sarama.StringEncoder(fmt.Sprintf("test message %d for gRPC range info", i)),
			Timestamp: baseTime.Add(time.Duration(i) * time.Second),
		}

		p, o, err := producer.SendMessage(msg)
		if err != nil {
			log.Fatalf("Failed to send message %d: %v", i, err)
		}
		fmt.Printf("✅ Message %d sent: partition=%d, offset=%d\n", i, p, o)
	}

	// Wait for messages to be processed
	time.Sleep(3 * time.Second)

	// Now test the gRPC GetPartitionRangeInfo API directly
	fmt.Println("\n🔍 Testing GetPartitionRangeInfo gRPC API...")

	conn, err := grpc.Dial(grpcBrokerAddress, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("Failed to connect to gRPC broker: %v", err)
	}
	defer conn.Close()

	client_grpc := mq_pb.NewSeaweedMessagingClient(conn)

	// Create the request
	req := &mq_pb.GetPartitionRangeInfoRequest{
		Topic: &schema_pb.Topic{
			Namespace: "kafka",
			Name:      topic,
		},
		Partition: &schema_pb.Partition{
			RingSize:   2520,
			RangeStart: 0,
			RangeStop:  2520,
		},
	}

	resp, err := client_grpc.GetPartitionRangeInfo(context.Background(), req)
	if err != nil {
		log.Fatalf("Failed to call GetPartitionRangeInfo: %v", err)
	}

	if resp.Error != "" {
		log.Fatalf("GetPartitionRangeInfo returned error: %s", resp.Error)
	}

	fmt.Println("✅ GetPartitionRangeInfo gRPC call successful!")

	// Display offset range information
	if resp.OffsetRange != nil {
		fmt.Printf("📊 Offset Range:\n")
		fmt.Printf("   - Earliest Offset: %d\n", resp.OffsetRange.EarliestOffset)
		fmt.Printf("   - Latest Offset: %d\n", resp.OffsetRange.LatestOffset)
		fmt.Printf("   - High Water Mark: %d\n", resp.OffsetRange.HighWaterMark)
	}

	// Display timestamp range information
	if resp.TimestampRange != nil {
		fmt.Printf("📅 Timestamp Range:\n")
		fmt.Printf("   - Earliest Timestamp: %d ns\n", resp.TimestampRange.EarliestTimestampNs)
		fmt.Printf("   - Latest Timestamp: %d ns\n", resp.TimestampRange.LatestTimestampNs)
		if resp.TimestampRange.EarliestTimestampNs > 0 {
			fmt.Printf("   - Earliest Time: %v\n", time.Unix(0, resp.TimestampRange.EarliestTimestampNs))
		}
		if resp.TimestampRange.LatestTimestampNs > 0 {
			fmt.Printf("   - Latest Time: %v\n", time.Unix(0, resp.TimestampRange.LatestTimestampNs))
		}
	}

	// Display partition metadata
	fmt.Printf("📈 Partition Metadata:\n")
	fmt.Printf("   - Record Count: %d\n", resp.RecordCount)
	fmt.Printf("   - Active Subscriptions: %d\n", resp.ActiveSubscriptions)

	// Verify the results make sense
	if resp.OffsetRange != nil && resp.OffsetRange.EarliestOffset == 0 && resp.OffsetRange.LatestOffset == 2 {
		fmt.Println("✅ Enhanced GetPartitionRangeInfo test completed successfully!")
		fmt.Println("   - Offset range is correct: [0, 2]")
		fmt.Println("   - Timestamp range functionality is available")
		fmt.Println("   - Comprehensive partition metadata is provided")
	} else {
		log.Fatalf("❌ GetPartitionRangeInfo test FAILED! Expected offset range [0,2], got [%d,%d]",
			resp.OffsetRange.EarliestOffset, resp.OffsetRange.LatestOffset)
	}
}
