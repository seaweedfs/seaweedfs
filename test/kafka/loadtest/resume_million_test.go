package integration

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
)

// TestResumeMillionRecords_Fixed - Fixed version with better concurrency handling
func TestResumeMillionRecords_Fixed(t *testing.T) {
	const (
		totalRecords  = 1000000
		numPartitions = int32(8)
		numProducers  = 4
		brokerAddr    = "localhost:17777"
		batchSize     = 100 // Process in smaller batches to avoid overwhelming
	)

	// Create direct broker client
	client, err := NewDirectBrokerClient(brokerAddr)
	if err != nil {
		t.Fatalf("Failed to create direct broker client: %v", err)
	}
	defer client.Close()

	topicName := fmt.Sprintf("resume-million-test-%d", time.Now().Unix())

	// Create topic
	glog.Infof("Creating topic %s with %d partitions for RESUMED test", topicName, numPartitions)
	err = client.ConfigureTopic(topicName, numPartitions)
	if err != nil {
		t.Fatalf("Failed to configure topic: %v", err)
	}

	// Performance tracking
	var totalProduced int64
	var totalErrors int64
	startTime := time.Now()

	// Progress tracking
	ticker := time.NewTicker(5 * time.Second) // More frequent updates
	defer ticker.Stop()

	go func() {
		for range ticker.C {
			produced := atomic.LoadInt64(&totalProduced)
			errors := atomic.LoadInt64(&totalErrors)
			elapsed := time.Since(startTime)
			rate := float64(produced) / elapsed.Seconds()
			progressPercent := float64(produced) / float64(totalRecords) * 100

			glog.Infof("PROGRESS: %d/%d records (%.1f%%), rate: %.0f records/sec, errors: %d",
				produced, totalRecords, progressPercent, rate, errors)

			if produced >= totalRecords {
				return
			}
		}
	}()

	// Fixed producer function with better error handling
	producer := func(producerID int, recordsPerProducer int) error {
		defer glog.Infof("Producer %d FINISHED", producerID)

		// Create dedicated clients per producer to avoid contention
		producerClient, err := NewDirectBrokerClient(brokerAddr)
		if err != nil {
			return fmt.Errorf("producer %d failed to create client: %v", producerID, err)
		}
		defer producerClient.Close()

		successCount := 0
		for i := 0; i < recordsPerProducer; i++ {
			recordID := producerID*recordsPerProducer + i

			// Generate test record
			testRecord := GenerateMockTestRecord(recordID)
			key, value := SerializeMockTestRecord(testRecord)

			partition := int32(testRecord.UserID % int64(numPartitions))

			// Produce with retry logic
			maxRetries := 3
			var lastErr error
			success := false

			for retry := 0; retry < maxRetries; retry++ {
				err := producerClient.PublishRecord(topicName, partition, key, value)
				if err == nil {
					success = true
					break
				}
				lastErr = err
				time.Sleep(time.Duration(retry+1) * 100 * time.Millisecond) // Exponential backoff
			}

			if success {
				atomic.AddInt64(&totalProduced, 1)
				successCount++
			} else {
				atomic.AddInt64(&totalErrors, 1)
				if atomic.LoadInt64(&totalErrors) < 10 {
					glog.Errorf("Producer %d failed record %d after retries: %v", producerID, recordID, lastErr)
				}
			}

			// Batch progress logging
			if successCount > 0 && successCount%10000 == 0 {
				glog.Infof("Producer %d: %d/%d records completed", producerID, successCount, recordsPerProducer)
			}

			// Small delay to prevent overwhelming the broker
			if i > 0 && i%batchSize == 0 {
				time.Sleep(10 * time.Millisecond)
			}
		}

		glog.Infof("Producer %d completed: %d successful, %d errors",
			producerID, successCount, recordsPerProducer-successCount)
		return nil
	}

	// Start concurrent producers
	glog.Infof("Starting FIXED %d producers for %d records total", numProducers, totalRecords)

	var wg sync.WaitGroup
	recordsPerProducer := totalRecords / numProducers

	for i := 0; i < numProducers; i++ {
		wg.Add(1)
		go func(producerID int) {
			defer wg.Done()
			if err := producer(producerID, recordsPerProducer); err != nil {
				glog.Errorf("Producer %d FAILED: %v", producerID, err)
			}
		}(i)
	}

	// Wait for completion with timeout
	done := make(chan bool)
	go func() {
		wg.Wait()
		done <- true
	}()

	select {
	case <-done:
		glog.Infof("All producers completed normally")
	case <-time.After(30 * time.Minute): // 30-minute timeout
		glog.Errorf("Test timed out after 30 minutes")
		t.Errorf("Test timed out")
		return
	}

	produceTime := time.Since(startTime)
	finalProduced := atomic.LoadInt64(&totalProduced)
	finalErrors := atomic.LoadInt64(&totalErrors)

	// Performance results
	throughputPerSec := float64(finalProduced) / produceTime.Seconds()
	dataVolumeMB := float64(finalProduced) * 300 / (1024 * 1024)
	throughputMBPerSec := dataVolumeMB / produceTime.Seconds()
	successRate := float64(finalProduced) / float64(totalRecords) * 100

	glog.Infof("\n"+
		"=== FINAL MILLION RECORD TEST RESULTS ===\n"+
		"==========================================\n"+
		"Records produced: %d / %d\n"+
		"Production time: %v\n"+
		"Average throughput: %.0f records/sec\n"+
		"Data volume: %.1f MB\n"+
		"Bandwidth: %.1f MB/sec\n"+
		"Errors: %d (%.2f%%)\n"+
		"Success rate: %.1f%%\n"+
		"Partitions used: %d\n"+
		"Concurrent producers: %d\n",
		finalProduced, totalRecords,
		produceTime,
		throughputPerSec,
		dataVolumeMB,
		throughputMBPerSec,
		finalErrors,
		float64(finalErrors)/float64(totalRecords)*100,
		successRate,
		numPartitions,
		numProducers,
	)

	// Test assertions
	if finalProduced < int64(totalRecords*0.95) { // Allow 5% tolerance
		t.Errorf("Too few records produced: %d < %d (95%% of target)", finalProduced, int64(float64(totalRecords)*0.95))
	}

	if finalErrors > int64(totalRecords*0.05) { // Error rate should be < 5%
		t.Errorf("Too many errors: %d > %d (5%% of target)", finalErrors, int64(float64(totalRecords)*0.05))
	}

	if throughputPerSec < 100 {
		t.Errorf("Throughput too low: %.0f records/sec (expected > 100)", throughputPerSec)
	}

	glog.Infof("🏆 MILLION RECORD KAFKA INTEGRATION TEST COMPLETED SUCCESSFULLY!")
}
