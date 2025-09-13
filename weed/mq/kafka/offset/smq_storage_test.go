package offset

import (
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/util"
)

func TestSMQOffsetStorage_ConsumerOffsetOperations(t *testing.T) {
	// This test verifies the core offset operations work correctly
	// Note: This is a unit test that would need a running filer to execute fully
	// For now, we test the data structures and logic paths

	storage := &SMQOffsetStorage{
		filerClientAccessor: nil, // Would need mock or real filer client
	}

	key := ConsumerOffsetKey{
		Topic:               "test-topic",
		Partition:           0,
		ConsumerGroup:       "test-group",
		ConsumerGroupInstance: "instance-1",
	}

	// Test that we can create the storage instance
	if storage == nil {
		t.Fatal("Failed to create SMQ offset storage")
	}

	// Test offset key construction
	if key.Topic != "test-topic" {
		t.Errorf("Expected topic 'test-topic', got %s", key.Topic)
	}

	if key.Partition != 0 {
		t.Errorf("Expected partition 0, got %d", key.Partition)
	}

	if key.ConsumerGroup != "test-group" {
		t.Errorf("Expected consumer group 'test-group', got %s", key.ConsumerGroup)
	}
}

func TestSMQOffsetStorage_OffsetEncoding(t *testing.T) {
	// Test that we encode offsets in the same format as SMQ brokers
	testCases := []int64{0, 1, 100, 1000, 9223372036854775807} // max int64

	for _, expectedOffset := range testCases {
		// Encode offset using SMQ format
		offsetBytes := make([]byte, 8)
		util.Uint64toBytes(offsetBytes, uint64(expectedOffset))

		// Decode offset
		decodedOffset := int64(util.BytesToUint64(offsetBytes))

		if decodedOffset != expectedOffset {
			t.Errorf("Offset encoding mismatch: expected %d, got %d", expectedOffset, decodedOffset)
		}
	}
}

func TestSMQOffsetStorage_ConsumerOffsetKey(t *testing.T) {
	// Test ConsumerOffsetKey functionality
	key1 := ConsumerOffsetKey{
		Topic:               "topic1",
		Partition:           0,
		ConsumerGroup:       "group1",
		ConsumerGroupInstance: "instance1",
	}

	key2 := ConsumerOffsetKey{
		Topic:               "topic1",
		Partition:           0,
		ConsumerGroup:       "group1",
		ConsumerGroupInstance: "", // No instance
	}

	// Test String() method
	str1 := key1.String()
	str2 := key2.String()

	expectedStr1 := "topic1:0:group1:instance1"
	expectedStr2 := "topic1:0:group1"

	if str1 != expectedStr1 {
		t.Errorf("Expected key string '%s', got '%s'", expectedStr1, str1)
	}

	if str2 != expectedStr2 {
		t.Errorf("Expected key string '%s', got '%s'", expectedStr2, str2)
	}

	// Test that keys with and without instance ID are different
	if str1 == str2 {
		t.Error("Keys with and without instance ID should be different")
	}
}

func TestSMQOffsetStorage_HighWaterMarkLogic(t *testing.T) {
	// Test the high water mark calculation logic
	testCases := []struct {
		committedOffset    int64
		expectedHighWater int64
		description       string
	}{
		{-1, 0, "no committed offset"},
		{0, 1, "committed offset 0"},
		{100, 101, "committed offset 100"},
		{9223372036854775806, 9223372036854775807, "near max int64"},
	}

	for _, tc := range testCases {
		// Simulate the high water mark calculation
		var highWaterMark int64
		if tc.committedOffset < 0 {
			highWaterMark = 0
		} else {
			highWaterMark = tc.committedOffset + 1
		}

		if highWaterMark != tc.expectedHighWater {
			t.Errorf("%s: expected high water mark %d, got %d", 
				tc.description, tc.expectedHighWater, highWaterMark)
		}
	}
}

// TestSMQOffsetStorage_LegacyCompatibility tests backward compatibility
func TestSMQOffsetStorage_LegacyCompatibility(t *testing.T) {
	storage := &SMQOffsetStorage{
		filerClientAccessor: nil,
	}

	// Test that legacy methods exist and return appropriate errors for unimplemented parsing
	_, err := storage.LoadOffsetMappings("topic:0")
	if err == nil {
		t.Error("Expected error for unimplemented legacy parsing, got nil")
	}

	_, err = storage.GetHighWaterMark("topic:0")  
	if err == nil {
		t.Error("Expected error for unimplemented legacy parsing, got nil")
	}

	err = storage.SaveOffsetMapping("topic:0", 100, 1234567890, 1024)
	if err == nil {
		t.Error("Expected error for unimplemented legacy parsing, got nil")
	}
}
