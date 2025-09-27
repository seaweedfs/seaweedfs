package kafka

import (
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/mq/pub_balancer"
)

func TestPartitionMapper_GetRangeSize(t *testing.T) {
	mapper := NewPartitionMapper()
	rangeSize := mapper.GetRangeSize()

	if rangeSize != 32 {
		t.Errorf("Expected range size 32, got %d", rangeSize)
	}

	// Verify that the range size divides evenly into available partitions
	maxPartitions := mapper.GetMaxKafkaPartitions()
	totalUsed := maxPartitions * rangeSize

	if totalUsed > int32(pub_balancer.MaxPartitionCount) {
		t.Errorf("Total used slots (%d) exceeds MaxPartitionCount (%d)", totalUsed, pub_balancer.MaxPartitionCount)
	}

	t.Logf("Range size: %d, Max Kafka partitions: %d, Ring utilization: %.2f%%",
		rangeSize, maxPartitions, float64(totalUsed)/float64(pub_balancer.MaxPartitionCount)*100)
}

func TestPartitionMapper_MapKafkaPartitionToSMQRange(t *testing.T) {
	mapper := NewPartitionMapper()

	tests := []struct {
		kafkaPartition int32
		expectedStart  int32
		expectedStop   int32
	}{
		{0, 0, 31},
		{1, 32, 63},
		{2, 64, 95},
		{10, 320, 351},
	}

	for _, tt := range tests {
		t.Run("", func(t *testing.T) {
			start, stop := mapper.MapKafkaPartitionToSMQRange(tt.kafkaPartition)

			if start != tt.expectedStart {
				t.Errorf("Kafka partition %d: expected start %d, got %d", tt.kafkaPartition, tt.expectedStart, start)
			}

			if stop != tt.expectedStop {
				t.Errorf("Kafka partition %d: expected stop %d, got %d", tt.kafkaPartition, tt.expectedStop, stop)
			}

			// Verify range size is consistent
			rangeSize := stop - start + 1
			if rangeSize != mapper.GetRangeSize() {
				t.Errorf("Inconsistent range size: expected %d, got %d", mapper.GetRangeSize(), rangeSize)
			}
		})
	}
}

func TestPartitionMapper_ExtractKafkaPartitionFromSMQRange(t *testing.T) {
	mapper := NewPartitionMapper()

	tests := []struct {
		rangeStart    int32
		expectedKafka int32
	}{
		{0, 0},
		{32, 1},
		{64, 2},
		{320, 10},
	}

	for _, tt := range tests {
		t.Run("", func(t *testing.T) {
			kafkaPartition := mapper.ExtractKafkaPartitionFromSMQRange(tt.rangeStart)

			if kafkaPartition != tt.expectedKafka {
				t.Errorf("Range start %d: expected Kafka partition %d, got %d",
					tt.rangeStart, tt.expectedKafka, kafkaPartition)
			}
		})
	}
}

func TestPartitionMapper_RoundTrip(t *testing.T) {
	mapper := NewPartitionMapper()

	// Test round-trip conversion for all valid Kafka partitions
	maxPartitions := mapper.GetMaxKafkaPartitions()

	for kafkaPartition := int32(0); kafkaPartition < maxPartitions; kafkaPartition++ {
		// Kafka -> SMQ -> Kafka
		rangeStart, rangeStop := mapper.MapKafkaPartitionToSMQRange(kafkaPartition)
		extractedKafka := mapper.ExtractKafkaPartitionFromSMQRange(rangeStart)

		if extractedKafka != kafkaPartition {
			t.Errorf("Round-trip failed for partition %d: got %d", kafkaPartition, extractedKafka)
		}

		// Verify no overlap with next partition
		if kafkaPartition < maxPartitions-1 {
			nextStart, _ := mapper.MapKafkaPartitionToSMQRange(kafkaPartition + 1)
			if rangeStop >= nextStart {
				t.Errorf("Partition %d range [%d,%d] overlaps with partition %d start %d",
					kafkaPartition, rangeStart, rangeStop, kafkaPartition+1, nextStart)
			}
		}
	}
}

func TestPartitionMapper_CreateSMQPartition(t *testing.T) {
	mapper := NewPartitionMapper()

	kafkaPartition := int32(5)
	unixTimeNs := time.Now().UnixNano()

	partition := mapper.CreateSMQPartition(kafkaPartition, unixTimeNs)

	if partition.RingSize != pub_balancer.MaxPartitionCount {
		t.Errorf("Expected ring size %d, got %d", pub_balancer.MaxPartitionCount, partition.RingSize)
	}

	expectedStart, expectedStop := mapper.MapKafkaPartitionToSMQRange(kafkaPartition)
	if partition.RangeStart != expectedStart {
		t.Errorf("Expected range start %d, got %d", expectedStart, partition.RangeStart)
	}

	if partition.RangeStop != expectedStop {
		t.Errorf("Expected range stop %d, got %d", expectedStop, partition.RangeStop)
	}

	if partition.UnixTimeNs != unixTimeNs {
		t.Errorf("Expected timestamp %d, got %d", unixTimeNs, partition.UnixTimeNs)
	}
}

func TestPartitionMapper_ValidateKafkaPartition(t *testing.T) {
	mapper := NewPartitionMapper()

	tests := []struct {
		partition int32
		valid     bool
	}{
		{-1, false},
		{0, true},
		{1, true},
		{mapper.GetMaxKafkaPartitions() - 1, true},
		{mapper.GetMaxKafkaPartitions(), false},
		{1000, false},
	}

	for _, tt := range tests {
		t.Run("", func(t *testing.T) {
			valid := mapper.ValidateKafkaPartition(tt.partition)
			if valid != tt.valid {
				t.Errorf("Partition %d: expected valid=%v, got %v", tt.partition, tt.valid, valid)
			}
		})
	}
}

func TestPartitionMapper_ConsistencyWithGlobalFunctions(t *testing.T) {
	mapper := NewPartitionMapper()

	kafkaPartition := int32(7)
	unixTimeNs := time.Now().UnixNano()

	// Test that global functions produce same results as mapper methods
	start1, stop1 := mapper.MapKafkaPartitionToSMQRange(kafkaPartition)
	start2, stop2 := MapKafkaPartitionToSMQRange(kafkaPartition)

	if start1 != start2 || stop1 != stop2 {
		t.Errorf("Global function inconsistent: mapper=(%d,%d), global=(%d,%d)",
			start1, stop1, start2, stop2)
	}

	partition1 := mapper.CreateSMQPartition(kafkaPartition, unixTimeNs)
	partition2 := CreateSMQPartition(kafkaPartition, unixTimeNs)

	if partition1.RangeStart != partition2.RangeStart || partition1.RangeStop != partition2.RangeStop {
		t.Errorf("Global CreateSMQPartition inconsistent")
	}

	extracted1 := mapper.ExtractKafkaPartitionFromSMQRange(start1)
	extracted2 := ExtractKafkaPartitionFromSMQRange(start1)

	if extracted1 != extracted2 {
		t.Errorf("Global ExtractKafkaPartitionFromSMQRange inconsistent: %d vs %d", extracted1, extracted2)
	}
}

func TestPartitionMapper_GetPartitionMappingInfo(t *testing.T) {
	mapper := NewPartitionMapper()

	info := mapper.GetPartitionMappingInfo()

	// Verify all expected keys are present
	expectedKeys := []string{"ring_size", "range_size", "max_kafka_partitions", "ring_utilization"}
	for _, key := range expectedKeys {
		if _, exists := info[key]; !exists {
			t.Errorf("Missing key in mapping info: %s", key)
		}
	}

	// Verify values are reasonable
	if info["ring_size"].(int) != pub_balancer.MaxPartitionCount {
		t.Errorf("Incorrect ring_size in info")
	}

	if info["range_size"].(int32) != mapper.GetRangeSize() {
		t.Errorf("Incorrect range_size in info")
	}

	utilization := info["ring_utilization"].(float64)
	if utilization <= 0 || utilization > 1 {
		t.Errorf("Invalid ring utilization: %f", utilization)
	}

	t.Logf("Partition mapping info: %+v", info)
}
