package integration

import (
	"context"
	"fmt"
	"math/rand"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"

	"github.com/seaweedfs/seaweedfs/weed/pb"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/mq_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/schema_pb"
)

// TestRecord represents a record with reasonable fields for integration testing
type MockTestRecord struct {
	ID        string
	UserID    int64
	Timestamp int64
	Event     string
	Data      map[string]interface{}
	Metadata  map[string]string
}

// GenerateTestRecord creates a realistic test record
func GenerateMockTestRecord(id int) MockTestRecord {
	events := []string{"user_login", "user_logout", "page_view", "purchase", "signup", "profile_update", "search"}
	metadata := map[string]string{
		"source":    "web",
		"version":   "1.0.0",
		"region":    "us-west-2",
		"client_ip": fmt.Sprintf("192.168.%d.%d", rand.Intn(255), rand.Intn(255)),
	}

	data := map[string]interface{}{
		"session_id": fmt.Sprintf("sess_%d_%d", id, time.Now().Unix()),
		"user_agent": "Mozilla/5.0 (compatible; SeaweedFS-Test/1.0)",
		"referrer":   "https://example.com/page" + strconv.Itoa(rand.Intn(100)),
		"duration":   rand.Intn(3600), // seconds
		"score":      rand.Float64() * 100,
	}

	return MockTestRecord{
		ID:        fmt.Sprintf("record_%d", id),
		UserID:    int64(rand.Intn(10000) + 1),
		Timestamp: time.Now().UnixNano(),
		Event:     events[rand.Intn(len(events))],
		Data:      data,
		Metadata:  metadata,
	}
}

// SerializeTestRecord converts TestRecord to key-value pair for Kafka
func SerializeMockTestRecord(record MockTestRecord) ([]byte, []byte) {
	key := fmt.Sprintf("user_%d:%s", record.UserID, record.ID)

	// Create a realistic JSON-like value with reasonable size (200-500 bytes)
	value := fmt.Sprintf(`{
		"id": "%s",
		"user_id": %d,
		"timestamp": %d,
		"event": "%s",
		"session_id": "%v",
		"user_agent": "%v",
		"referrer": "%v",
		"duration": %v,
		"score": %.2f,
		"source": "%s",
		"version": "%s",
		"region": "%s",
		"client_ip": "%s",
		"batch_info": "This is additional data to make the record size more realistic for testing purposes. It simulates the kind of metadata and context that would typically be included in real-world event data."
	}`,
		record.ID,
		record.UserID,
		record.Timestamp,
		record.Event,
		record.Data["session_id"],
		record.Data["user_agent"],
		record.Data["referrer"],
		record.Data["duration"],
		record.Data["score"],
		record.Metadata["source"],
		record.Metadata["version"],
		record.Metadata["region"],
		record.Metadata["client_ip"],
	)

	return []byte(key), []byte(value)
}

// DirectBrokerClient connects directly to the broker without discovery
type DirectBrokerClient struct {
	brokerAddress string
	conn          *grpc.ClientConn
	client        mq_pb.SeaweedMessagingClient

	// Publisher streams: topic-partition -> stream info
	publishersLock sync.RWMutex
	publishers     map[string]*PublisherSession

	ctx    context.Context
	cancel context.CancelFunc
}

// PublisherSession tracks a publishing stream to SeaweedMQ broker
type PublisherSession struct {
	Topic        string
	Partition    int32
	Stream       mq_pb.SeaweedMessaging_PublishMessageClient
	MessageCount int64 // Track messages sent for batch ack handling
}

func NewDirectBrokerClient(brokerAddr string) (*DirectBrokerClient, error) {
	// Use a short-lived context for dialing so we don't store a canceled
	// context in the returned client. The client's operational context
	// (used by methods) should be cancellable independently.
	dialCtx, dialCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer dialCancel()

	// Add keepalive settings; use exported server constants to keep values in sync.
	conn, err := grpc.DialContext(dialCtx, brokerAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time:                pb.GrpcKeepAliveTime,    // align with server MinTime
			Timeout:             pb.GrpcKeepAliveTimeout, // align with server timeout
			PermitWithoutStream: false,                    // reduce pings when idle
		}))
	if err != nil {
		return nil, fmt.Errorf("failed to connect to broker: %v", err)
	}

	// Create a long-lived context for the client's lifetime and store it
	// in the returned DirectBrokerClient so callers can cancel when done.
	clientCtx, clientCancel := context.WithCancel(context.Background())

	client := mq_pb.NewSeaweedMessagingClient(conn)

	return &DirectBrokerClient{
		brokerAddress: brokerAddr,
		conn:          conn,
		client:        client,
		publishers:    make(map[string]*PublisherSession),
		ctx:           ctx,
		cancel:        cancel,
	}, nil
}

func (c *DirectBrokerClient) Close() {
	c.cancel()

	// Close all publisher streams
	c.publishersLock.Lock()
	for key := range c.publishers {
		delete(c.publishers, key)
	}
	c.publishersLock.Unlock()

	c.conn.Close()
}

func (c *DirectBrokerClient) ConfigureTopic(topicName string, partitions int32) error {
	topic := &schema_pb.Topic{
		Namespace: "kafka",
		Name:      topicName,
	}

	// Create schema for MockTestRecord
	recordType := &schema_pb.RecordType{
		Fields: []*schema_pb.Field{
			{
				Name:       "id",
				FieldIndex: 0,
				Type: &schema_pb.Type{
					Kind: &schema_pb.Type_ScalarType{ScalarType: schema_pb.ScalarType_STRING},
				},
			},
			{
				Name:       "user_id",
				FieldIndex: 1,
				Type: &schema_pb.Type{
					Kind: &schema_pb.Type_ScalarType{ScalarType: schema_pb.ScalarType_INT64},
				},
			},
			{
				Name:       "timestamp",
				FieldIndex: 2,
				Type: &schema_pb.Type{
					Kind: &schema_pb.Type_ScalarType{ScalarType: schema_pb.ScalarType_INT64},
				},
			},
			{
				Name:       "event",
				FieldIndex: 3,
				Type: &schema_pb.Type{
					Kind: &schema_pb.Type_ScalarType{ScalarType: schema_pb.ScalarType_STRING},
				},
			},
			{
				Name:       "data",
				FieldIndex: 4,
				Type: &schema_pb.Type{
					Kind: &schema_pb.Type_ScalarType{ScalarType: schema_pb.ScalarType_STRING}, // JSON string
				},
			},
			{
				Name:       "metadata",
				FieldIndex: 5,
				Type: &schema_pb.Type{
					Kind: &schema_pb.Type_ScalarType{ScalarType: schema_pb.ScalarType_STRING}, // JSON string
				},
			},
		},
	}

	// Use user_id as the key column for partitioning
	keyColumns := []string{"user_id"}

	_, err := c.client.ConfigureTopic(c.ctx, &mq_pb.ConfigureTopicRequest{
		Topic:             topic,
		PartitionCount:    partitions,
		MessageRecordType: recordType,
		KeyColumns:        keyColumns,
	})
	return err
}

func (c *DirectBrokerClient) PublishRecord(topicName string, partition int32, key, value []byte) error {
	session, err := c.getOrCreatePublisher(topicName, partition)
	if err != nil {
		return err
	}

	// Send data message using broker API format
	dataMsg := &mq_pb.DataMessage{
		Key:   key,
		Value: value,
		TsNs:  time.Now().UnixNano(),
	}

	if err := session.Stream.Send(&mq_pb.PublishMessageRequest{
		Message: &mq_pb.PublishMessageRequest_Data{
			Data: dataMsg,
		},
	}); err != nil {
		return fmt.Errorf("failed to send data: %v", err)
	}

	// Don't wait for individual acks! AckInterval=100 means acks come in batches
	// The broker will handle acknowledgments asynchronously
	return nil
}

// getOrCreatePublisher gets or creates a publisher stream for a topic-partition
func (c *DirectBrokerClient) getOrCreatePublisher(topic string, partition int32) (*PublisherSession, error) {
	key := fmt.Sprintf("%s-%d", topic, partition)

	// Try to get existing publisher
	c.publishersLock.RLock()
	if session, exists := c.publishers[key]; exists {
		c.publishersLock.RUnlock()
		return session, nil
	}
	c.publishersLock.RUnlock()

	// Create new publisher stream
	c.publishersLock.Lock()
	defer c.publishersLock.Unlock()

	// Double-check after acquiring write lock
	if session, exists := c.publishers[key]; exists {
		return session, nil
	}

	// Create the stream
	stream, err := c.client.PublishMessage(c.ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to create publish stream: %v", err)
	}

	// Get the actual partition assignment from the broker
	actualPartition, err := c.getActualPartitionAssignment(topic, partition)
	if err != nil {
		return nil, fmt.Errorf("failed to get actual partition assignment: %v", err)
	}

	// Send init message using the actual partition structure that the broker allocated
	if err := stream.Send(&mq_pb.PublishMessageRequest{
		Message: &mq_pb.PublishMessageRequest_Init{
			Init: &mq_pb.PublishMessageRequest_InitMessage{
				Topic: &schema_pb.Topic{
					Namespace: "kafka",
					Name:      topic,
				},
				Partition:     actualPartition,
				AckInterval:   200, // Ack every 200 messages for better balance
				PublisherName: "direct-test",
			},
		},
	}); err != nil {
		return nil, fmt.Errorf("failed to send init message: %v", err)
	}

	session := &PublisherSession{
		Topic:        topic,
		Partition:    partition,
		Stream:       stream,
		MessageCount: 0,
	}

	c.publishers[key] = session
	return session, nil
}

// getActualPartitionAssignment looks up the actual partition assignment from the broker configuration
func (c *DirectBrokerClient) getActualPartitionAssignment(topic string, kafkaPartition int32) (*schema_pb.Partition, error) {
	// Look up the topic configuration from the broker to get the actual partition assignments
	lookupResp, err := c.client.LookupTopicBrokers(c.ctx, &mq_pb.LookupTopicBrokersRequest{
		Topic: &schema_pb.Topic{
			Namespace: "kafka",
			Name:      topic,
		},
	})
	if err != nil {
		return nil, fmt.Errorf("failed to lookup topic brokers: %v", err)
	}

	if len(lookupResp.BrokerPartitionAssignments) == 0 {
		return nil, fmt.Errorf("no partition assignments found for topic %s", topic)
	}

	totalPartitions := int32(len(lookupResp.BrokerPartitionAssignments))
	if kafkaPartition >= totalPartitions {
		return nil, fmt.Errorf("kafka partition %d out of range, topic %s has %d partitions",
			kafkaPartition, topic, totalPartitions)
	}

	// Calculate expected range for this Kafka partition
	// Ring is divided equally among partitions, with last partition getting any remainder
	const ringSize = int32(2520) // MaxPartitionCount constant
	rangeSize := ringSize / totalPartitions
	expectedRangeStart := kafkaPartition * rangeSize
	var expectedRangeStop int32

	if kafkaPartition == totalPartitions-1 {
		// Last partition gets the remainder to fill the entire ring
		expectedRangeStop = ringSize
	} else {
		expectedRangeStop = (kafkaPartition + 1) * rangeSize
	}

	// Find the broker assignment that matches this range
	for _, assignment := range lookupResp.BrokerPartitionAssignments {
		if assignment.Partition == nil {
			continue
		}

		// Check if this assignment's range matches our expected range
		if assignment.Partition.RangeStart == expectedRangeStart && assignment.Partition.RangeStop == expectedRangeStop {
			return assignment.Partition, nil
		}
	}

	return nil, fmt.Errorf("no broker assignment found for Kafka partition %d with expected range [%d, %d]",
		kafkaPartition, expectedRangeStart, expectedRangeStop)
}

// TestDirectBroker_MillionRecordsIntegration tests the broker directly without discovery
func TestDirectBroker_MillionRecordsIntegration(t *testing.T) {
	// Skip by default - this is a large integration test
	if testing.Short() {
		t.Skip("Skipping million-record integration test in short mode")
	}

	// Configuration
	const (
		totalRecords  = 1000000
		numPartitions = int32(8) // Use multiple partitions for better performance
		numProducers  = 4        // Concurrent producers
		brokerAddr    = "localhost:17777"
	)

	// Create direct broker client for topic configuration
	configClient, err := NewDirectBrokerClient(brokerAddr)
	if err != nil {
		t.Fatalf("Failed to create direct broker client: %v", err)
	}
	defer configClient.Close()

	topicName := fmt.Sprintf("million-records-direct-test-%d", time.Now().Unix())

	// Create topic
	glog.Infof("Creating topic %s with %d partitions", topicName, numPartitions)
	err = configClient.ConfigureTopic(topicName, numPartitions)
	if err != nil {
		t.Fatalf("Failed to configure topic: %v", err)
	}

	// Performance tracking
	var totalProduced int64
	var totalErrors int64
	startTime := time.Now()

	// Progress tracking
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		for {
			select {
			case <-ticker.C:
				produced := atomic.LoadInt64(&totalProduced)
				errors := atomic.LoadInt64(&totalErrors)
				elapsed := time.Since(startTime)
				rate := float64(produced) / elapsed.Seconds()
				glog.Infof("Progress: %d/%d records (%.1f%%), rate: %.0f records/sec, errors: %d",
					produced, totalRecords, float64(produced)/float64(totalRecords)*100, rate, errors)
			case <-ctx.Done():
				return
			}
		}
	}()

	// Producer function
	producer := func(producerID int, recordsPerProducer int) error {
		defer func() {
			glog.Infof("Producer %d finished", producerID)
		}()

		// Create dedicated client for this producer
		producerClient, err := NewDirectBrokerClient(brokerAddr)
		if err != nil {
			return fmt.Errorf("Producer %d failed to create client: %v", producerID, err)
		}
		defer producerClient.Close()

		// Add timeout context for each producer
		producerCtx, producerCancel := context.WithTimeout(ctx, 10*time.Minute)
		defer producerCancel()

		glog.Infof("Producer %d: About to start producing %d records with dedicated client", producerID, recordsPerProducer)

		for i := 0; i < recordsPerProducer; i++ {
			// Check if context is cancelled or timed out
			select {
			case <-producerCtx.Done():
				glog.Errorf("Producer %d timed out or cancelled after %d records", producerID, i)
				return producerCtx.Err()
			default:
			}

			// Debug progress for all producers every 50k records
			if i > 0 && i%50000 == 0 {
				glog.Infof("Producer %d: Progress %d/%d records (%.1f%%)", producerID, i, recordsPerProducer, float64(i)/float64(recordsPerProducer)*100)
			}
			// Calculate global record ID
			recordID := producerID*recordsPerProducer + i

			// Generate test record
			testRecord := GenerateMockTestRecord(recordID)
			key, value := SerializeMockTestRecord(testRecord)

			// Distribute across partitions based on user ID
			partition := int32(testRecord.UserID % int64(numPartitions))

			// Debug first few records for each producer
			if i < 3 {
				glog.Infof("Producer %d: Record %d -> UserID %d -> Partition %d", producerID, i, testRecord.UserID, partition)
			}

			// Produce the record with retry logic
			var err error
			maxRetries := 3
			for retry := 0; retry < maxRetries; retry++ {
				err = producerClient.PublishRecord(topicName, partition, key, value)
				if err == nil {
					break // Success
				}

				// If it's an EOF error, wait a bit before retrying
				if err.Error() == "failed to send data: EOF" {
					time.Sleep(time.Duration(retry+1) * 100 * time.Millisecond)
					continue
				}

				// For other errors, don't retry
				break
			}

			if err != nil {
				atomic.AddInt64(&totalErrors, 1)
				errorCount := atomic.LoadInt64(&totalErrors)
				if errorCount < 20 { // Log first 20 errors to get more insight
					glog.Errorf("Producer %d failed to produce record %d (i=%d) after %d retries: %v", producerID, recordID, i, maxRetries, err)
				}
				// Don't continue - this might be causing producers to exit early
				// Let's see what happens if we return the error instead
				if errorCount > 1000 { // If too many errors, give up
					glog.Errorf("Producer %d giving up after %d errors", producerID, errorCount)
					return fmt.Errorf("too many errors: %d", errorCount)
				}
				continue
			}

			atomic.AddInt64(&totalProduced, 1)

			// Log progress for first producer
			if producerID == 0 && (i+1)%10000 == 0 {
				glog.Infof("Producer %d: produced %d records", producerID, i+1)
			}
		}

		glog.Infof("Producer %d: Completed loop, produced %d records successfully", producerID, recordsPerProducer)
		return nil
	}

	// Start concurrent producers
	glog.Infof("Starting %d concurrent producers to produce %d records", numProducers, totalRecords)

	var wg sync.WaitGroup
	recordsPerProducer := totalRecords / numProducers

	for i := 0; i < numProducers; i++ {
		wg.Add(1)
		go func(producerID int) {
			defer wg.Done()
			glog.Infof("Producer %d starting with %d records to produce", producerID, recordsPerProducer)
			if err := producer(producerID, recordsPerProducer); err != nil {
				glog.Errorf("Producer %d failed: %v", producerID, err)
			}
		}(i)
	}

	// Wait for all producers to complete
	wg.Wait()
	cancel() // Stop progress reporting

	produceTime := time.Since(startTime)
	finalProduced := atomic.LoadInt64(&totalProduced)
	finalErrors := atomic.LoadInt64(&totalErrors)

	glog.Infof("Production completed: %d records in %v (%.0f records/sec), errors: %d",
		finalProduced, produceTime, float64(finalProduced)/produceTime.Seconds(), finalErrors)

	// Performance summary
	if finalProduced > 0 {
		glog.Infof("\n"+
			"=== PERFORMANCE SUMMARY ===\n"+
			"Records produced: %d\n"+
			"Production time: %v\n"+
			"Production rate: %.0f records/sec\n"+
			"Errors: %d (%.2f%%)\n"+
			"Partitions: %d\n"+
			"Concurrent producers: %d\n"+
			"Average record size: ~300 bytes\n"+
			"Total data: ~%.1f MB\n"+
			"Throughput: ~%.1f MB/sec\n",
			finalProduced,
			produceTime,
			float64(finalProduced)/produceTime.Seconds(),
			finalErrors,
			float64(finalErrors)/float64(totalRecords)*100,
			numPartitions,
			numProducers,
			float64(finalProduced)*300/(1024*1024),
			float64(finalProduced)*300/(1024*1024)/produceTime.Seconds(),
		)
	}

	// Test assertions
	if finalProduced < int64(totalRecords*0.95) { // Allow 5% tolerance for errors
		t.Errorf("Too few records produced: %d < %d (95%% of target)", finalProduced, int64(float64(totalRecords)*0.95))
	}

	if finalErrors > int64(totalRecords*0.05) { // Error rate should be < 5%
		t.Errorf("Too many errors: %d > %d (5%% of target)", finalErrors, int64(float64(totalRecords)*0.05))
	}

	glog.Infof("Direct broker million-record integration test completed successfully!")
}

// BenchmarkDirectBroker_ProduceThroughput benchmarks the production throughput
func BenchmarkDirectBroker_ProduceThroughput(b *testing.B) {
	if testing.Short() {
		b.Skip("Skipping benchmark in short mode")
	}

	client, err := NewDirectBrokerClient("localhost:17777")
	if err != nil {
		b.Fatalf("Failed to create client: %v", err)
	}
	defer client.Close()

	topicName := fmt.Sprintf("benchmark-topic-%d", time.Now().Unix())
	err = client.ConfigureTopic(topicName, 1)
	if err != nil {
		b.Fatalf("Failed to configure topic: %v", err)
	}

	// Pre-generate test data
	records := make([]MockTestRecord, b.N)
	for i := 0; i < b.N; i++ {
		records[i] = GenerateMockTestRecord(i)
	}

	b.ResetTimer()
	b.StartTimer()

	for i := 0; i < b.N; i++ {
		key, value := SerializeMockTestRecord(records[i])
		err := client.PublishRecord(topicName, 0, key, value)
		if err != nil {
			b.Fatalf("Failed to produce record %d: %v", i, err)
		}
	}

	b.StopTimer()
}
