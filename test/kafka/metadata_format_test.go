package kafka

import (
	"encoding/binary"
	"fmt"
	"testing"
)

// TestMetadataFormat tests different metadata formats to find kafka-go compatibility
func TestMetadataFormat(t *testing.T) {
	// Test different subscription metadata formats that kafka-go might expect
	
	t.Log("=== Testing different subscription metadata formats ===")
	
	// Format 1: Our current format (version 0, topics, userdata)
	format1 := generateSubscriptionMetadata([]string{"test-topic"}, 0)
	t.Logf("Format 1 (current): %d bytes: %x", len(format1), format1)
	
	// Format 2: Version 1 format (might include owned partitions)
	format2 := generateSubscriptionMetadata([]string{"test-topic"}, 1)
	t.Logf("Format 2 (version 1): %d bytes: %x", len(format2), format2)
	
	// Format 3: Empty metadata (let kafka-go handle it)
	format3 := []byte{}
	t.Logf("Format 3 (empty): %d bytes: %x", len(format3), format3)
	
	// Format 4: Minimal valid metadata
	format4 := []byte{0x00, 0x01, 0x00, 0x00, 0x00, 0x01, 0x00, 0x09, 't', 'e', 's', 't', '-', 't', 'o', 'p', 'i', 'c', 0x00, 0x00, 0x00, 0x00}
	t.Logf("Format 4 (minimal): %d bytes: %x", len(format4), format4)
	
	// Test each format by creating a modified JoinGroup handler
	for i, metadata := range [][]byte{format1, format2, format3, format4} {
		t.Logf("\n--- Testing Format %d ---", i+1)
		success := testMetadataFormat(t, metadata, fmt.Sprintf("format-%d", i+1))
		if success {
			t.Logf("✅ Format %d might be compatible!", i+1)
		} else {
			t.Logf("❌ Format %d rejected by kafka-go", i+1)
		}
	}
}

func generateSubscriptionMetadata(topics []string, version int) []byte {
	metadata := make([]byte, 0, 64)
	
	// Version (2 bytes)
	metadata = append(metadata, byte(version>>8), byte(version))
	
	// Topics count (4 bytes)
	topicsCount := make([]byte, 4)
	binary.BigEndian.PutUint32(topicsCount, uint32(len(topics)))
	metadata = append(metadata, topicsCount...)
	
	// Topics (string array)
	for _, topic := range topics {
		topicLen := make([]byte, 2)
		binary.BigEndian.PutUint16(topicLen, uint16(len(topic)))
		metadata = append(metadata, topicLen...)
		metadata = append(metadata, []byte(topic)...)
	}
	
	if version >= 1 {
		// OwnedPartitions (for version 1+) - empty for now
		metadata = append(metadata, 0x00, 0x00, 0x00, 0x00) // empty owned partitions
	}
	
	// UserData (4 bytes length + data)
	metadata = append(metadata, 0x00, 0x00, 0x00, 0x00) // empty user data
	
	return metadata
}

func testMetadataFormat(t *testing.T, metadata []byte, testName string) bool {
	// This is a placeholder for testing different metadata formats
	// In a real test, we'd:
	// 1. Start a gateway with modified JoinGroup handler that uses this metadata
	// 2. Connect with kafka-go consumer
	// 3. Check if it proceeds to SyncGroup
	
	// For now, just log the format
	t.Logf("Testing %s with metadata: %x", testName, metadata)
	
	// TODO: Implement actual kafka-go integration test
	// This would require modifying the JoinGroup handler to use specific metadata
	
	return false // Placeholder
}
