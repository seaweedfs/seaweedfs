package log_buffer

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
)

// TestConcurrentProducerConsumer simulates the integration test scenario:
// - One producer writing messages continuously
// - Multiple consumers reading from different offsets
// - Consumers reading sequentially (like Kafka consumers)
func TestConcurrentProducerConsumer(t *testing.T) {
	lb := NewLogBuffer("integration-test", time.Hour, nil, nil, func() {})
	lb.hasOffsets = true

	const numMessages = 1000
	const numConsumers = 2
	const messagesPerConsumer = numMessages / numConsumers

	// Start producer
	producerDone := make(chan bool)
	go func() {
		for i := 0; i < numMessages; i++ {
			entry := &filer_pb.LogEntry{
				TsNs:   time.Now().UnixNano(),
				Key:    []byte("key"),
				Data:   []byte("value"),
				Offset: int64(i),
			}
			if err := lb.AddLogEntryToBuffer(entry); err != nil {
				t.Errorf("Failed to add log entry: %v", err)
				return
			}
			time.Sleep(1 * time.Millisecond) // Simulate production rate
		}
		producerDone <- true
	}()

	// Start consumers
	consumerWg := sync.WaitGroup{}
	consumerErrors := make(chan error, numConsumers)
	consumedCounts := make([]int64, numConsumers)

	for consumerID := 0; consumerID < numConsumers; consumerID++ {
		consumerWg.Add(1)
		go func(id int, startOffset int64, endOffset int64) {
			defer consumerWg.Done()

			currentOffset := startOffset
			for currentOffset < endOffset {
				// Read 10 messages at a time (like integration test)
				messages, nextOffset, _, _, err := lb.ReadMessagesAtOffset(currentOffset, 10, 10240)
				if err != nil {
					consumerErrors <- err
					return
				}

				if len(messages) == 0 {
					// No data yet, wait a bit
					time.Sleep(5 * time.Millisecond)
					continue
				}

				// Count only messages in this consumer's assigned range
				messagesInRange := 0
				for i, msg := range messages {
					if msg.Offset >= startOffset && msg.Offset < endOffset {
						messagesInRange++
						expectedOffset := currentOffset + int64(i)
						if msg.Offset != expectedOffset {
							t.Errorf("Consumer %d: Expected offset %d, got %d", id, expectedOffset, msg.Offset)
						}
					}
				}

				atomic.AddInt64(&consumedCounts[id], int64(messagesInRange))
				currentOffset = nextOffset
			}
		}(consumerID, int64(consumerID*messagesPerConsumer), int64((consumerID+1)*messagesPerConsumer))
	}

	// Wait for producer to finish
	<-producerDone

	// Wait for consumers (with timeout)
	done := make(chan bool)
	go func() {
		consumerWg.Wait()
		done <- true
	}()

	select {
	case <-done:
		// Success
	case err := <-consumerErrors:
		t.Fatalf("Consumer error: %v", err)
	case <-time.After(10 * time.Second):
		t.Fatal("Timeout waiting for consumers to finish")
	}

	// Verify all messages were consumed
	totalConsumed := int64(0)
	for i, count := range consumedCounts {
		t.Logf("Consumer %d consumed %d messages", i, count)
		totalConsumed += count
	}

	if totalConsumed != numMessages {
		t.Errorf("Expected to consume %d messages, but consumed %d", numMessages, totalConsumed)
	}
}

// TestBackwardSeeksWhileProducing simulates consumer rebalancing where
// consumers seek backward to earlier offsets while producer is still writing
func TestBackwardSeeksWhileProducing(t *testing.T) {
	lb := NewLogBuffer("backward-seek-test", time.Hour, nil, nil, func() {})
	lb.hasOffsets = true

	const numMessages = 500
	const numSeeks = 10

	// Start producer
	producerDone := make(chan bool)
	go func() {
		for i := 0; i < numMessages; i++ {
			entry := &filer_pb.LogEntry{
				TsNs:   time.Now().UnixNano(),
				Key:    []byte("key"),
				Data:   []byte("value"),
				Offset: int64(i),
			}
			if err := lb.AddLogEntryToBuffer(entry); err != nil {
				t.Errorf("Failed to add log entry: %v", err)
				return
			}
			time.Sleep(1 * time.Millisecond)
		}
		producerDone <- true
	}()

	// Consumer that seeks backward periodically
	consumerDone := make(chan bool)
	readOffsets := make(map[int64]int) // Track how many times each offset was read

	go func() {
		currentOffset := int64(0)
		seeksRemaining := numSeeks

		for currentOffset < numMessages {
			// Read some messages
			messages, nextOffset, _, endOfPartition, err := lb.ReadMessagesAtOffset(currentOffset, 10, 10240)
			if err != nil {
				// For stateless reads, "offset out of range" means data not in memory yet
				// This is expected when reading historical data or before production starts
				time.Sleep(5 * time.Millisecond)
				continue
			}

			if len(messages) == 0 {
				// No data available yet or caught up to producer
				if !endOfPartition {
					// Data might be coming, wait
					time.Sleep(5 * time.Millisecond)
				} else {
					// At end of partition, wait for more production
					time.Sleep(5 * time.Millisecond)
				}
				continue
			}

			// Track read offsets
			for _, msg := range messages {
				readOffsets[msg.Offset]++
			}

			// Periodically seek backward (simulating rebalancing)
			if seeksRemaining > 0 && nextOffset > 50 && nextOffset%100 == 0 {
				seekOffset := nextOffset - 20
				t.Logf("Seeking backward from %d to %d", nextOffset, seekOffset)
				currentOffset = seekOffset
				seeksRemaining--
			} else {
				currentOffset = nextOffset
			}
		}

		consumerDone <- true
	}()

	// Wait for both
	<-producerDone
	<-consumerDone

	// Verify each offset was read at least once
	for i := int64(0); i < numMessages; i++ {
		if readOffsets[i] == 0 {
			t.Errorf("Offset %d was never read", i)
		}
	}

	t.Logf("Total unique offsets read: %d out of %d", len(readOffsets), numMessages)
}

// TestHighConcurrencyReads simulates multiple consumers reading from
// different offsets simultaneously (stress test)
func TestHighConcurrencyReads(t *testing.T) {
	lb := NewLogBuffer("high-concurrency-test", time.Hour, nil, nil, func() {})
	lb.hasOffsets = true

	const numMessages = 1000
	const numReaders = 10

	// Pre-populate buffer
	for i := 0; i < numMessages; i++ {
		entry := &filer_pb.LogEntry{
			TsNs:   time.Now().UnixNano(),
			Key:    []byte("key"),
			Data:   []byte("value"),
			Offset: int64(i),
		}
		if err := lb.AddLogEntryToBuffer(entry); err != nil {
			t.Fatalf("Failed to add log entry: %v", err)
		}
	}

	// Start many concurrent readers at different offsets
	wg := sync.WaitGroup{}
	errors := make(chan error, numReaders)

	for reader := 0; reader < numReaders; reader++ {
		wg.Add(1)
		go func(startOffset int64) {
			defer wg.Done()

			// Read 100 messages from this offset
			currentOffset := startOffset
			readCount := 0

			for readCount < 100 && currentOffset < numMessages {
				messages, nextOffset, _, _, err := lb.ReadMessagesAtOffset(currentOffset, 10, 10240)
				if err != nil {
					errors <- err
					return
				}

				// Verify offsets are sequential
				for i, msg := range messages {
					expected := currentOffset + int64(i)
					if msg.Offset != expected {
						t.Errorf("Reader at %d: expected offset %d, got %d", startOffset, expected, msg.Offset)
					}
				}

				readCount += len(messages)
				currentOffset = nextOffset
			}
		}(int64(reader * 10))
	}

	// Wait with timeout
	done := make(chan bool)
	go func() {
		wg.Wait()
		done <- true
	}()

	select {
	case <-done:
		// Success
	case err := <-errors:
		t.Fatalf("Reader error: %v", err)
	case <-time.After(10 * time.Second):
		t.Fatal("Timeout waiting for readers")
	}
}

// TestRepeatedReadsAtSameOffset simulates what happens when Kafka
// consumer re-fetches the same offset multiple times (due to timeouts or retries)
func TestRepeatedReadsAtSameOffset(t *testing.T) {
	lb := NewLogBuffer("repeated-reads-test", time.Hour, nil, nil, func() {})
	lb.hasOffsets = true

	const numMessages = 100

	// Pre-populate buffer
	for i := 0; i < numMessages; i++ {
		entry := &filer_pb.LogEntry{
			TsNs:   time.Now().UnixNano(),
			Key:    []byte("key"),
			Data:   []byte("value"),
			Offset: int64(i),
		}
		if err := lb.AddLogEntryToBuffer(entry); err != nil {
			t.Fatalf("Failed to add log entry: %v", err)
		}
	}

	// Read the same offset multiple times concurrently
	const numReads = 10
	const testOffset = int64(50)

	wg := sync.WaitGroup{}
	results := make([][]*filer_pb.LogEntry, numReads)

	for i := 0; i < numReads; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			messages, _, _, _, err := lb.ReadMessagesAtOffset(testOffset, 10, 10240)
			if err != nil {
				t.Errorf("Read %d error: %v", idx, err)
				return
			}
			results[idx] = messages
		}(i)
	}

	wg.Wait()

	// Verify all reads returned the same data
	firstRead := results[0]
	for i := 1; i < numReads; i++ {
		if len(results[i]) != len(firstRead) {
			t.Errorf("Read %d returned %d messages, expected %d", i, len(results[i]), len(firstRead))
		}

		for j := range results[i] {
			if results[i][j].Offset != firstRead[j].Offset {
				t.Errorf("Read %d message %d has offset %d, expected %d",
					i, j, results[i][j].Offset, firstRead[j].Offset)
			}
		}
	}
}

// TestEmptyPartitionPolling simulates consumers polling empty partitions
// waiting for data (common in Kafka)
func TestEmptyPartitionPolling(t *testing.T) {
	lb := NewLogBuffer("empty-partition-test", time.Hour, nil, nil, func() {})
	lb.hasOffsets = true
	lb.bufferStartOffset = 0
	lb.offset = 0

	// Try to read from empty partition
	messages, nextOffset, _, endOfPartition, err := lb.ReadMessagesAtOffset(0, 10, 10240)

	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}
	if len(messages) != 0 {
		t.Errorf("Expected 0 messages, got %d", len(messages))
	}
	if nextOffset != 0 {
		t.Errorf("Expected nextOffset=0, got %d", nextOffset)
	}
	if !endOfPartition {
		t.Error("Expected endOfPartition=true for future offset")
	}
}
