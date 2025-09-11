package offset

import (
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/schema_pb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestKafkaToSMQMapping(t *testing.T) {
	// Create a ledger with some test data
	ledger := NewLedger()
	mapper := NewKafkaToSMQMapper(ledger)

	// Add some test records
	baseTime := time.Now().UnixNano()
	testRecords := []struct {
		kafkaOffset int64
		timestamp   int64
		size        int32
	}{
		{0, baseTime + 1000, 100},
		{1, baseTime + 2000, 150},
		{2, baseTime + 3000, 200},
		{3, baseTime + 4000, 120},
	}

	// Populate the ledger
	for _, record := range testRecords {
		offset := ledger.AssignOffsets(1)
		require.Equal(t, record.kafkaOffset, offset)
		err := ledger.AppendRecord(record.kafkaOffset, record.timestamp, record.size)
		require.NoError(t, err)
	}

	t.Run("KafkaOffsetToSMQPartitionOffset", func(t *testing.T) {
		kafkaPartition := int32(0)
		kafkaOffset := int64(1)

		partitionOffset, err := mapper.KafkaOffsetToSMQPartitionOffset(
			kafkaOffset, "test-topic", kafkaPartition)
		require.NoError(t, err)

		// Verify the mapping
		assert.Equal(t, baseTime+2000, partitionOffset.StartTsNs)
		assert.Equal(t, int32(1024), partitionOffset.Partition.RingSize)
		assert.Equal(t, int32(0), partitionOffset.Partition.RangeStart)
		assert.Equal(t, int32(31), partitionOffset.Partition.RangeStop)

		t.Logf("Kafka offset %d → SMQ timestamp %d", kafkaOffset, partitionOffset.StartTsNs)
	})

	t.Run("SMQPartitionOffsetToKafkaOffset", func(t *testing.T) {
		// Create a partition offset
		partitionOffset := &schema_pb.PartitionOffset{
			StartTsNs: baseTime + 3000, // This should map to Kafka offset 2
		}

		kafkaOffset, err := mapper.SMQPartitionOffsetToKafkaOffset(partitionOffset)
		require.NoError(t, err)
		assert.Equal(t, int64(2), kafkaOffset)

		t.Logf("SMQ timestamp %d → Kafka offset %d", partitionOffset.StartTsNs, kafkaOffset)
	})

	t.Run("MultiplePartitionMapping", func(t *testing.T) {
		testCases := []struct {
			kafkaPartition int32
			expectedStart  int32
			expectedStop   int32
		}{
			{0, 0, 31},
			{1, 32, 63},
			{2, 64, 95},
			{15, 480, 511},
		}

		for _, tc := range testCases {
			partitionOffset, err := mapper.KafkaOffsetToSMQPartitionOffset(
				0, "test-topic", tc.kafkaPartition)
			require.NoError(t, err)

			assert.Equal(t, tc.expectedStart, partitionOffset.Partition.RangeStart)
			assert.Equal(t, tc.expectedStop, partitionOffset.Partition.RangeStop)

			// Verify reverse mapping
			extractedPartition := ExtractKafkaPartitionFromSMQPartition(partitionOffset.Partition)
			assert.Equal(t, tc.kafkaPartition, extractedPartition)

			t.Logf("Kafka partition %d → SMQ range [%d, %d]",
				tc.kafkaPartition, tc.expectedStart, tc.expectedStop)
		}
	})
}

func TestCreateSMQSubscriptionRequest(t *testing.T) {
	ledger := NewLedger()
	mapper := NewKafkaToSMQMapper(ledger)

	// Add some test data
	baseTime := time.Now().UnixNano()
	for i := int64(0); i < 5; i++ {
		offset := ledger.AssignOffsets(1)
		err := ledger.AppendRecord(offset, baseTime+i*1000, 100)
		require.NoError(t, err)
	}

	t.Run("SpecificOffset", func(t *testing.T) {
		partitionOffset, offsetType, err := mapper.CreateSMQSubscriptionRequest(
			"test-topic", 0, 2, "test-group")
		require.NoError(t, err)

		assert.Equal(t, schema_pb.OffsetType_EXACT_TS_NS, offsetType)
		assert.Equal(t, baseTime+2000, partitionOffset.StartTsNs)
		assert.Equal(t, int32(0), partitionOffset.Partition.RangeStart)
		assert.Equal(t, int32(31), partitionOffset.Partition.RangeStop)

		t.Logf("Specific offset 2 → SMQ timestamp %d", partitionOffset.StartTsNs)
	})

	t.Run("EarliestOffset", func(t *testing.T) {
		partitionOffset, offsetType, err := mapper.CreateSMQSubscriptionRequest(
			"test-topic", 0, -2, "test-group")
		require.NoError(t, err)

		assert.Equal(t, schema_pb.OffsetType_RESET_TO_EARLIEST, offsetType)
		assert.Equal(t, baseTime, partitionOffset.StartTsNs)

		t.Logf("EARLIEST → SMQ timestamp %d", partitionOffset.StartTsNs)
	})

	t.Run("LatestOffset", func(t *testing.T) {
		partitionOffset, offsetType, err := mapper.CreateSMQSubscriptionRequest(
			"test-topic", 0, -1, "test-group")
		require.NoError(t, err)

		assert.Equal(t, schema_pb.OffsetType_RESET_TO_LATEST, offsetType)
		assert.Equal(t, baseTime+4000, partitionOffset.StartTsNs)

		t.Logf("LATEST → SMQ timestamp %d", partitionOffset.StartTsNs)
	})

	t.Run("FutureOffset", func(t *testing.T) {
		// Request offset beyond high water mark
		partitionOffset, offsetType, err := mapper.CreateSMQSubscriptionRequest(
			"test-topic", 0, 10, "test-group")
		require.NoError(t, err)

		assert.Equal(t, schema_pb.OffsetType_EXACT_TS_NS, offsetType)
		// Should use current time for future offsets
		assert.True(t, partitionOffset.StartTsNs > baseTime+4000)

		t.Logf("Future offset 10 → SMQ timestamp %d (current time)", partitionOffset.StartTsNs)
	})
}

func TestMappingValidation(t *testing.T) {
	ledger := NewLedger()
	mapper := NewKafkaToSMQMapper(ledger)

	t.Run("ValidSequentialMapping", func(t *testing.T) {
		baseTime := time.Now().UnixNano()

		// Add sequential records
		for i := int64(0); i < 3; i++ {
			offset := ledger.AssignOffsets(1)
			err := ledger.AppendRecord(offset, baseTime+i*1000, 100)
			require.NoError(t, err)
		}

		err := mapper.ValidateMapping("test-topic", 0)
		assert.NoError(t, err)
	})

	t.Run("InvalidNonSequentialOffsets", func(t *testing.T) {
		ledger2 := NewLedger()
		mapper2 := NewKafkaToSMQMapper(ledger2)

		baseTime := time.Now().UnixNano()

		// Manually create non-sequential offsets (this shouldn't happen in practice)
		ledger2.entries = []OffsetEntry{
			{KafkaOffset: 0, Timestamp: baseTime, Size: 100},
			{KafkaOffset: 2, Timestamp: baseTime + 1000, Size: 100}, // Gap!
		}

		err := mapper2.ValidateMapping("test-topic", 0)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "non-sequential")
	})
}

func TestGetMappingInfo(t *testing.T) {
	ledger := NewLedger()
	mapper := NewKafkaToSMQMapper(ledger)

	baseTime := time.Now().UnixNano()
	offset := ledger.AssignOffsets(1)
	err := ledger.AppendRecord(offset, baseTime, 150)
	require.NoError(t, err)

	info, err := mapper.GetMappingInfo(0, 2)
	require.NoError(t, err)

	assert.Equal(t, int64(0), info.KafkaOffset)
	assert.Equal(t, baseTime, info.SMQTimestamp)
	assert.Equal(t, int32(2), info.KafkaPartition)
	assert.Equal(t, int32(64), info.SMQRangeStart) // 2 * 32
	assert.Equal(t, int32(95), info.SMQRangeStop)  // (2+1) * 32 - 1
	assert.Equal(t, int32(150), info.MessageSize)

	t.Logf("Mapping info: Kafka %d:%d → SMQ %d [%d-%d] (%d bytes)",
		info.KafkaPartition, info.KafkaOffset, info.SMQTimestamp,
		info.SMQRangeStart, info.SMQRangeStop, info.MessageSize)
}

func TestGetOffsetRange(t *testing.T) {
	ledger := NewLedger()
	mapper := NewKafkaToSMQMapper(ledger)

	baseTime := time.Now().UnixNano()
	timestamps := []int64{
		baseTime + 1000,
		baseTime + 2000,
		baseTime + 3000,
		baseTime + 4000,
		baseTime + 5000,
	}

	// Add records
	for i, timestamp := range timestamps {
		offset := ledger.AssignOffsets(1)
		err := ledger.AppendRecord(offset, timestamp, 100)
		require.NoError(t, err, "Failed to add record %d", i)
	}

	t.Run("FullRange", func(t *testing.T) {
		startOffset, endOffset, err := mapper.GetOffsetRange(
			baseTime+1500, baseTime+4500)
		require.NoError(t, err)

		assert.Equal(t, int64(1), startOffset) // First offset >= baseTime+1500
		assert.Equal(t, int64(3), endOffset)   // Last offset <= baseTime+4500

		t.Logf("Time range [%d, %d] → Kafka offsets [%d, %d]",
			baseTime+1500, baseTime+4500, startOffset, endOffset)
	})

	t.Run("NoMatchingRange", func(t *testing.T) {
		_, _, err := mapper.GetOffsetRange(baseTime+10000, baseTime+20000)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "no offsets found")
	})
}

func TestCreatePartitionOffsetForTimeRange(t *testing.T) {
	ledger := NewLedger()
	mapper := NewKafkaToSMQMapper(ledger)

	startTime := time.Now().UnixNano()
	kafkaPartition := int32(5)

	partitionOffset := mapper.CreatePartitionOffsetForTimeRange(kafkaPartition, startTime)

	assert.Equal(t, startTime, partitionOffset.StartTsNs)
	assert.Equal(t, int32(1024), partitionOffset.Partition.RingSize)
	assert.Equal(t, int32(160), partitionOffset.Partition.RangeStart) // 5 * 32
	assert.Equal(t, int32(191), partitionOffset.Partition.RangeStop)  // (5+1) * 32 - 1

	t.Logf("Kafka partition %d time range → SMQ PartitionOffset [%d-%d] @ %d",
		kafkaPartition, partitionOffset.Partition.RangeStart,
		partitionOffset.Partition.RangeStop, partitionOffset.StartTsNs)
}

// BenchmarkMapping tests the performance of offset mapping operations
func BenchmarkMapping(b *testing.B) {
	ledger := NewLedger()
	mapper := NewKafkaToSMQMapper(ledger)

	// Populate with test data
	baseTime := time.Now().UnixNano()
	for i := int64(0); i < 1000; i++ {
		offset := ledger.AssignOffsets(1)
		ledger.AppendRecord(offset, baseTime+i*1000, 100)
	}

	b.Run("KafkaToSMQ", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			kafkaOffset := int64(i % 1000)
			_, err := mapper.KafkaOffsetToSMQPartitionOffset(kafkaOffset, "test", 0)
			if err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("SMQToKafka", func(b *testing.B) {
		partitionOffset := &schema_pb.PartitionOffset{
			StartTsNs: baseTime + 500000, // Middle timestamp
		}
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, err := mapper.SMQPartitionOffsetToKafkaOffset(partitionOffset)
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}
