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

	"github.com/seaweedfs/seaweedfs/weed/glog"
	kafkaintegration "github.com/seaweedfs/seaweedfs/weed/mq/kafka/integration"
)

// TestRecord represents a record with reasonable fields for integration testing
type TestRecord struct {
	ID        string
	UserID    int64
	Timestamp int64
	Event     string
	Data      map[string]interface{}
	Metadata  map[string]string
}

// GenerateTestRecord creates a realistic test record
func GenerateTestRecord(id int) TestRecord {
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

	return TestRecord{
		ID:        fmt.Sprintf("record_%d", id),
		UserID:    int64(rand.Intn(10000) + 1),
		Timestamp: time.Now().UnixNano(),
		Event:     events[rand.Intn(len(events))],
		Data:      data,
		Metadata:  metadata,
	}
}

// SerializeTestRecord converts TestRecord to key-value pair for Kafka
func SerializeTestRecord(record TestRecord) ([]byte, []byte) {
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

// TestSeaweedMQHandler_MillionRecordsIntegration tests the Kafka gateway with 1 million records
func TestSeaweedMQHandler_MillionRecordsIntegration(t *testing.T) {
	// Skip by default - this is a large integration test that requires real infrastructure
	// Remove this skip when running with actual SeaweedMQ infrastructure
	if testing.Short() {
		t.Skip("Skipping million-record integration test in short mode")
	}

	// Configuration
	const (
		totalRecords  = 1000000
		numPartitions = int32(8)         // Use multiple partitions for better performance
		batchSize     = 1000             // Process records in batches
		numProducers  = 4                // Concurrent producers
		masters       = "localhost:9333" // Update this to your masters
		filerGroup    = "default"
		clientHost    = "localhost"
	)

	// Create handler
	handler, err := kafkaintegration.NewSeaweedMQBrokerHandler(masters, filerGroup, clientHost)
	if err != nil {
		t.Fatalf("Failed to create SeaweedMQ handler: %v", err)
	}
	defer handler.Close()

	topicName := fmt.Sprintf("million-records-test-%d", time.Now().Unix())

	// Create topic with multiple partitions
	glog.Infof("Creating topic %s with %d partitions", topicName, numPartitions)
	err = handler.CreateTopic(topicName, numPartitions)
	if err != nil {
		t.Fatalf("Failed to create topic: %v", err)
	}
	defer func() {
		if deleteErr := handler.DeleteTopic(topicName); deleteErr != nil {
			glog.Errorf("Failed to cleanup topic: %v", deleteErr)
		}
	}()

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

		for i := 0; i < recordsPerProducer; i++ {
			// Calculate global record ID
			recordID := producerID*recordsPerProducer + i

			// Generate test record
			testRecord := GenerateTestRecord(recordID)
			key, value := SerializeTestRecord(testRecord)

			// Distribute across partitions based on user ID for good distribution
			partition := int32(testRecord.UserID % int64(numPartitions))

			// Produce the record
			offset, err := handler.ProduceRecord(topicName, partition, key, value)
			if err != nil {
				atomic.AddInt64(&totalErrors, 1)
				if atomic.LoadInt64(&totalErrors) < 10 { // Log first few errors
					glog.Errorf("Producer %d failed to produce record %d: %v", producerID, recordID, err)
				}
				continue
			}

			atomic.AddInt64(&totalProduced, 1)

			// Log progress for first producer
			if producerID == 0 && (i+1)%10000 == 0 {
				glog.Infof("Producer %d: produced %d records, latest offset: %d", producerID, i+1, offset)
			}
		}

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

	// Verify topic ledgers
	glog.Infof("Verifying offset ledgers...")
	var totalOffsets int64
	for partitionID := int32(0); partitionID < numPartitions; partitionID++ {
		ledger := handler.GetLedger(topicName, partitionID)
		if ledger == nil {
			t.Errorf("No ledger found for partition %d", partitionID)
			continue
		}

		hwm := ledger.GetHighWaterMark()
		totalOffsets += hwm
		glog.Infof("Partition %d: high water mark = %d", partitionID, hwm)
	}

	glog.Infof("Total offsets across all partitions: %d", totalOffsets)

	if totalOffsets != finalProduced {
		t.Errorf("Offset count mismatch: total offsets (%d) != produced records (%d)", totalOffsets, finalProduced)
	}

	// Test fetching records
	glog.Infof("Testing record retrieval...")
	fetchStartTime := time.Now()

	var totalFetched int64
	maxRecordsPerFetch := 100

	for partitionID := int32(0); partitionID < numPartitions; partitionID++ {
		ledger := handler.GetLedger(topicName, partitionID)
		if ledger == nil {
			continue
		}

		hwm := ledger.GetHighWaterMark()
		if hwm == 0 {
			glog.Infof("Partition %d is empty, skipping fetch test", partitionID)
			continue
		}

		// Fetch from beginning
		fetchedRecords, err := handler.GetStoredRecords(topicName, partitionID, 0, maxRecordsPerFetch)
		if err != nil {
			t.Errorf("Failed to fetch records from partition %d: %v", partitionID, err)
			continue
		}

		if len(fetchedRecords) == 0 {
			t.Errorf("No records fetched from partition %d (expected up to %d)", partitionID, maxRecordsPerFetch)
			continue
		}

		// Validate some fetched records
		for i, record := range fetchedRecords {
			if len(record.GetKey()) == 0 {
				t.Errorf("Partition %d, record %d: empty key", partitionID, i)
			}
			if len(record.GetValue()) == 0 {
				t.Errorf("Partition %d, record %d: empty value", partitionID, i)
			}
			if record.GetTimestamp() == 0 {
				t.Errorf("Partition %d, record %d: zero timestamp", partitionID, i)
			}
		}

		totalFetched += int64(len(fetchedRecords))
		glog.Infof("Partition %d: fetched %d records (HWM: %d)", partitionID, len(fetchedRecords), hwm)
	}

	fetchTime := time.Since(fetchStartTime)
	glog.Infof("Fetch test completed: retrieved %d sample records in %v", totalFetched, fetchTime)

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

	if totalFetched == 0 {
		t.Error("No records could be fetched - fetch functionality is broken")
	}

	glog.Infof("Million-record integration test completed successfully!")
}

// BenchmarkSeaweedMQHandler_ProduceThroughput benchmarks the production throughput
func BenchmarkSeaweedMQHandler_ProduceThroughput(b *testing.B) {
	if testing.Short() {
		b.Skip("Skipping benchmark in short mode")
	}

	handler, err := kafkaintegration.NewSeaweedMQBrokerHandler("localhost:9333", "default", "localhost")
	if err != nil {
		b.Fatalf("Failed to create handler: %v", err)
	}
	defer handler.Close()

	topicName := fmt.Sprintf("benchmark-topic-%d", time.Now().Unix())
	err = handler.CreateTopic(topicName, 1)
	if err != nil {
		b.Fatalf("Failed to create topic: %v", err)
	}
	defer handler.DeleteTopic(topicName)

	// Pre-generate test data to avoid measuring serialization time
	records := make([]TestRecord, b.N)
	for i := 0; i < b.N; i++ {
		records[i] = GenerateTestRecord(i)
	}

	b.ResetTimer()
	b.StartTimer()

	for i := 0; i < b.N; i++ {
		key, value := SerializeTestRecord(records[i])
		_, err := handler.ProduceRecord(topicName, 0, key, value)
		if err != nil {
			b.Fatalf("Failed to produce record %d: %v", i, err)
		}
	}

	b.StopTimer()
}

// TestSeaweedMQHandler_ConcurrentAccess tests concurrent produce/fetch operations
func TestSeaweedMQHandler_ConcurrentAccess(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping concurrent test in short mode")
	}

	handler, err := kafkaintegration.NewSeaweedMQBrokerHandler("localhost:9333", "default", "localhost")
	if err != nil {
		t.Fatalf("Failed to create handler: %v", err)
	}
	defer handler.Close()

	topicName := fmt.Sprintf("concurrent-test-%d", time.Now().Unix())
	err = handler.CreateTopic(topicName, 2)
	if err != nil {
		t.Fatalf("Failed to create topic: %v", err)
	}
	defer handler.DeleteTopic(topicName)

	const numGoroutines = 10
	const recordsPerGoroutine = 1000

	var wg sync.WaitGroup
	var totalProduced int64
	var totalErrors int64

	// Concurrent producers
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(goroutineID int) {
			defer wg.Done()

			for j := 0; j < recordsPerGoroutine; j++ {
				record := GenerateTestRecord(goroutineID*recordsPerGoroutine + j)
				key, value := SerializeTestRecord(record)

				partition := int32(goroutineID % 2) // Distribute between 2 partitions
				_, err := handler.ProduceRecord(topicName, partition, key, value)
				if err != nil {
					atomic.AddInt64(&totalErrors, 1)
					continue
				}
				atomic.AddInt64(&totalProduced, 1)
			}
		}(i)
	}

	wg.Wait()

	produced := atomic.LoadInt64(&totalProduced)
	errors := atomic.LoadInt64(&totalErrors)

	glog.Infof("Concurrent test: %d records produced, %d errors", produced, errors)

	if produced < int64(numGoroutines*recordsPerGoroutine*0.95) {
		t.Errorf("Too few records produced in concurrent test: %d", produced)
	}
}
