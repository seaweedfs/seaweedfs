package integration

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
)

// TestQuickPerformance_10K tests the fixed broker with 10K records
func TestQuickPerformance_10K(t *testing.T) {
	const (
		totalRecords  = 10000 // 10K records for quick test
		numPartitions = int32(4)
		numProducers  = 4
		brokerAddr    = "localhost:17777"
	)

	// Create direct broker client
	client, err := NewDirectBrokerClient(brokerAddr)
	if err != nil {
		t.Fatalf("Failed to create direct broker client: %v", err)
	}
	defer client.Close()

	topicName := fmt.Sprintf("quick-test-%d", time.Now().Unix())

	// Create topic
	glog.Infof("Creating topic %s with %d partitions", topicName, numPartitions)
	err = client.ConfigureTopic(topicName, numPartitions)
	if err != nil {
		t.Fatalf("Failed to configure topic: %v", err)
	}

	// Performance tracking
	var totalProduced int64
	var totalErrors int64
	startTime := time.Now()

	// Producer function
	producer := func(producerID int, recordsPerProducer int) error {
		for i := 0; i < recordsPerProducer; i++ {
			recordID := producerID*recordsPerProducer + i

			// Generate test record
			testRecord := GenerateMockTestRecord(recordID)
			key, value := SerializeMockTestRecord(testRecord)

			partition := int32(testRecord.UserID % int64(numPartitions))

			// Produce the record (now async!)
			err := client.PublishRecord(topicName, partition, key, value)
			if err != nil {
				atomic.AddInt64(&totalErrors, 1)
				if atomic.LoadInt64(&totalErrors) < 5 {
					glog.Errorf("Producer %d failed to produce record %d: %v", producerID, recordID, err)
				}
				continue
			}

			atomic.AddInt64(&totalProduced, 1)

			// Log progress
			if (i+1)%1000 == 0 {
				elapsed := time.Since(startTime)
				rate := float64(atomic.LoadInt64(&totalProduced)) / elapsed.Seconds()
				glog.Infof("Producer %d: %d records, current rate: %.0f records/sec",
					producerID, i+1, rate)
			}
		}
		return nil
	}

	// Start concurrent producers
	glog.Infof("Starting %d producers for %d records total", numProducers, totalRecords)

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

	// Wait for completion
	wg.Wait()

	produceTime := time.Since(startTime)
	finalProduced := atomic.LoadInt64(&totalProduced)
	finalErrors := atomic.LoadInt64(&totalErrors)

	// Performance results
	throughputPerSec := float64(finalProduced) / produceTime.Seconds()
	dataVolumeMB := float64(finalProduced) * 300 / (1024 * 1024) // ~300 bytes per record
	throughputMBPerSec := dataVolumeMB / produceTime.Seconds()

	glog.Infof("\n"+
		"QUICK PERFORMANCE TEST RESULTS\n"+
		"=====================================\n"+
		"Records produced: %d / %d\n"+
		"Production time: %v\n"+
		"Throughput: %.0f records/sec\n"+
		"Data volume: %.1f MB\n"+
		"Bandwidth: %.1f MB/sec\n"+
		"Errors: %d (%.2f%%)\n"+
		"Success rate: %.1f%%\n",
		finalProduced, totalRecords,
		produceTime,
		throughputPerSec,
		dataVolumeMB,
		throughputMBPerSec,
		finalErrors,
		float64(finalErrors)/float64(totalRecords)*100,
		float64(finalProduced)/float64(totalRecords)*100,
	)

	// Assertions
	if finalProduced < int64(totalRecords*0.90) { // Allow 10% tolerance
		t.Errorf("Too few records produced: %d < %d (90%% of target)", finalProduced, int64(float64(totalRecords)*0.90))
	}

	if throughputPerSec < 100 { // Should be much higher than 1 record/sec now!
		t.Errorf("Throughput too low: %.0f records/sec (expected > 100)", throughputPerSec)
	}

	if finalErrors > int64(totalRecords*0.10) { // Error rate should be < 10%
		t.Errorf("Too many errors: %d > %d (10%% of target)", finalErrors, int64(float64(totalRecords)*0.10))
	}

	glog.Infof("Performance test passed! Ready for million-record test.")
}
