package offset

import (
	"fmt"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/schema_pb"
)

// BenchmarkOffsetAssignment benchmarks sequential offset assignment
func BenchmarkOffsetAssignment(b *testing.B) {
	storage := NewInMemoryOffsetStorage()

	partition := &schema_pb.Partition{
		RingSize:   1024,
		RangeStart: 0,
		RangeStop:  31,
		UnixTimeNs: time.Now().UnixNano(),
	}

	manager, err := NewPartitionOffsetManager("test-namespace", "test-topic", partition, storage)
	if err != nil {
		b.Fatalf("Failed to create partition manager: %v", err)
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			manager.AssignOffset()
		}
	})
}

// BenchmarkBatchOffsetAssignment benchmarks batch offset assignment
func BenchmarkBatchOffsetAssignment(b *testing.B) {
	storage := NewInMemoryOffsetStorage()

	partition := &schema_pb.Partition{
		RingSize:   1024,
		RangeStart: 0,
		RangeStop:  31,
		UnixTimeNs: time.Now().UnixNano(),
	}

	manager, err := NewPartitionOffsetManager("test-namespace", "test-topic", partition, storage)
	if err != nil {
		b.Fatalf("Failed to create partition manager: %v", err)
	}

	batchSizes := []int64{1, 10, 100, 1000}

	for _, batchSize := range batchSizes {
		b.Run(fmt.Sprintf("BatchSize%d", batchSize), func(b *testing.B) {
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				manager.AssignOffsets(batchSize)
			}
		})
	}
}

// BenchmarkOffsetSubscription benchmarks subscription operations
func BenchmarkOffsetSubscription(b *testing.B) {
	storage := NewInMemoryOffsetStorage()
	registry := NewPartitionOffsetRegistry(storage)
	subscriber := NewOffsetSubscriber(registry)

	partition := &schema_pb.Partition{
		RingSize:   1024,
		RangeStart: 0,
		RangeStop:  31,
		UnixTimeNs: time.Now().UnixNano(),
	}

	// Pre-assign offsets
	registry.AssignOffsets("test-namespace", "test-topic", partition, 10000)

	b.Run("CreateSubscription", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			subscriptionID := fmt.Sprintf("bench-sub-%d", i)
			_, err := subscriber.CreateSubscription(
				subscriptionID,
				"test-namespace", "test-topic",
				partition,
				schema_pb.OffsetType_RESET_TO_EARLIEST,
				0,
			)
			if err != nil {
				b.Fatalf("Failed to create subscription: %v", err)
			}
			subscriber.CloseSubscription(subscriptionID)
		}
	})

	// Create subscription for other benchmarks
	sub, err := subscriber.CreateSubscription(
		"bench-sub",
		"test-namespace", "test-topic",
		partition,
		schema_pb.OffsetType_RESET_TO_EARLIEST,
		0,
	)
	if err != nil {
		b.Fatalf("Failed to create subscription: %v", err)
	}

	b.Run("GetOffsetRange", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			sub.GetOffsetRange(100)
		}
	})

	b.Run("AdvanceOffset", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			sub.AdvanceOffset()
		}
	})

	b.Run("GetLag", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			sub.GetLag()
		}
	})

	b.Run("SeekToOffset", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			offset := int64(i % 9000) // Stay within bounds
			sub.SeekToOffset(offset)
		}
	})
}

// BenchmarkSMQOffsetIntegration benchmarks the full integration layer
func BenchmarkSMQOffsetIntegration(b *testing.B) {
	storage := NewInMemoryOffsetStorage()
	integration := NewSMQOffsetIntegration(storage)

	partition := &schema_pb.Partition{
		RingSize:   1024,
		RangeStart: 0,
		RangeStop:  31,
		UnixTimeNs: time.Now().UnixNano(),
	}

	b.Run("PublishRecord", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			key := fmt.Sprintf("key-%d", i)
			integration.PublishRecord("test-namespace", "test-topic", partition, []byte(key), &schema_pb.RecordValue{})
		}
	})

	b.Run("PublishRecordBatch", func(b *testing.B) {
		batchSizes := []int{1, 10, 100}

		for _, batchSize := range batchSizes {
			b.Run(fmt.Sprintf("BatchSize%d", batchSize), func(b *testing.B) {
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					records := make([]PublishRecordRequest, batchSize)
					for j := 0; j < batchSize; j++ {
						records[j] = PublishRecordRequest{
							Key:   []byte(fmt.Sprintf("batch-%d-key-%d", i, j)),
							Value: &schema_pb.RecordValue{},
						}
					}
					integration.PublishRecordBatch("test-namespace", "test-topic", partition, records)
				}
			})
		}
	})

	// Pre-populate for subscription benchmarks
	records := make([]PublishRecordRequest, 1000)
	for i := 0; i < 1000; i++ {
		records[i] = PublishRecordRequest{
			Key:   []byte(fmt.Sprintf("pre-key-%d", i)),
			Value: &schema_pb.RecordValue{},
		}
	}
	integration.PublishRecordBatch("test-namespace", "test-topic", partition, records)

	b.Run("CreateSubscription", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			subscriptionID := fmt.Sprintf("integration-sub-%d", i)
			_, err := integration.CreateSubscription(
				subscriptionID,
				"test-namespace", "test-topic",
				partition,
				schema_pb.OffsetType_RESET_TO_EARLIEST,
				0,
			)
			if err != nil {
				b.Fatalf("Failed to create subscription: %v", err)
			}
			integration.CloseSubscription(subscriptionID)
		}
	})

	b.Run("GetHighWaterMark", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			integration.GetHighWaterMark("test-namespace", "test-topic", partition)
		}
	})

	b.Run("GetPartitionOffsetInfo", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			integration.GetPartitionOffsetInfo("test-namespace", "test-topic", partition)
		}
	})
}

// BenchmarkConcurrentOperations benchmarks concurrent offset operations
func BenchmarkConcurrentOperations(b *testing.B) {
	storage := NewInMemoryOffsetStorage()
	integration := NewSMQOffsetIntegration(storage)

	partition := &schema_pb.Partition{
		RingSize:   1024,
		RangeStart: 0,
		RangeStop:  31,
		UnixTimeNs: time.Now().UnixNano(),
	}

	b.Run("ConcurrentPublish", func(b *testing.B) {
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			i := 0
			for pb.Next() {
				key := fmt.Sprintf("concurrent-key-%d", i)
				integration.PublishRecord("test-namespace", "test-topic", partition, []byte(key), &schema_pb.RecordValue{})
				i++
			}
		})
	})

	// Pre-populate for concurrent reads
	for i := 0; i < 1000; i++ {
		key := fmt.Sprintf("read-key-%d", i)
		integration.PublishRecord("test-namespace", "test-topic", partition, []byte(key), &schema_pb.RecordValue{})
	}

	b.Run("ConcurrentRead", func(b *testing.B) {
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				integration.GetHighWaterMark("test-namespace", "test-topic", partition)
			}
		})
	})

	b.Run("ConcurrentMixed", func(b *testing.B) {
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			i := 0
			for pb.Next() {
				if i%10 == 0 {
					// 10% writes
					key := fmt.Sprintf("mixed-key-%d", i)
					integration.PublishRecord("test-namespace", "test-topic", partition, []byte(key), &schema_pb.RecordValue{})
				} else {
					// 90% reads
					integration.GetHighWaterMark("test-namespace", "test-topic", partition)
				}
				i++
			}
		})
	})
}

// BenchmarkMemoryUsage benchmarks memory usage patterns
func BenchmarkMemoryUsage(b *testing.B) {
	b.Run("InMemoryStorage", func(b *testing.B) {
		storage := NewInMemoryOffsetStorage()
		partition := &schema_pb.Partition{
			RingSize:   1024,
			RangeStart: 0,
			RangeStop:  31,
			UnixTimeNs: time.Now().UnixNano(),
		}

		manager, err := NewPartitionOffsetManager("test-namespace", "test-topic", partition, storage)
		if err != nil {
			b.Fatalf("Failed to create partition manager: %v", err)
		}

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			manager.AssignOffset()
			// Note: Checkpointing now happens automatically in background every 2 seconds
		}

		// Clean up background goroutine
		manager.Close()
	})
}
