package protocol

import (
	"encoding/binary"
	"testing"
	"time"
)

func TestHandler_handleProduce(t *testing.T) {
	h := NewHandler()
	correlationID := uint32(333)

	// First create a topic
	h.topics["test-topic"] = &TopicInfo{
		Name:       "test-topic",
		Partitions: 1,
		CreatedAt:  time.Now().UnixNano(),
	}

	// Build a simple Produce request with minimal record
	clientID := "test-producer"
	topicName := "test-topic"

	requestBody := make([]byte, 0, 256)

	// Client ID
	requestBody = append(requestBody, 0, byte(len(clientID)))
	requestBody = append(requestBody, []byte(clientID)...)

	// Acks (1 - wait for leader acknowledgment)
	requestBody = append(requestBody, 0, 1)

	// Timeout (5000ms)
	requestBody = append(requestBody, 0, 0, 0x13, 0x88)

	// Topics count (1)
	requestBody = append(requestBody, 0, 0, 0, 1)

	// Topic name
	requestBody = append(requestBody, 0, byte(len(topicName)))
	requestBody = append(requestBody, []byte(topicName)...)

	// Partitions count (1)
	requestBody = append(requestBody, 0, 0, 0, 1)

	// Partition 0
	requestBody = append(requestBody, 0, 0, 0, 0) // partition ID

	// Record set (simplified - just dummy data)
	recordSet := make([]byte, 32)
	// Basic record batch header structure for parsing
	binary.BigEndian.PutUint64(recordSet[0:8], 0)   // base offset
	binary.BigEndian.PutUint32(recordSet[8:12], 24) // batch length
	binary.BigEndian.PutUint32(recordSet[12:16], 0) // partition leader epoch
	recordSet[16] = 2                               // magic byte
	binary.BigEndian.PutUint32(recordSet[16:20], 1) // record count at offset 16

	recordSetSize := uint32(len(recordSet))
	requestBody = append(requestBody, byte(recordSetSize>>24), byte(recordSetSize>>16), byte(recordSetSize>>8), byte(recordSetSize))
	requestBody = append(requestBody, recordSet...)

	response, err := h.handleProduce(correlationID, 7, requestBody)
	if err != nil {
		t.Fatalf("handleProduce: %v", err)
	}

	if len(response) < 40 { // minimum expected size
		t.Fatalf("response too short: %d bytes", len(response))
	}

	// Check correlation ID
	respCorrelationID := binary.BigEndian.Uint32(response[0:4])
	if respCorrelationID != correlationID {
		t.Errorf("correlation ID: got %d, want %d", respCorrelationID, correlationID)
	}

	// Check topics count
	topicsCount := binary.BigEndian.Uint32(response[4:8])
	if topicsCount != 1 {
		t.Errorf("topics count: got %d, want 1", topicsCount)
	}

	// Parse response structure
	offset := 8
	respTopicNameSize := binary.BigEndian.Uint16(response[offset : offset+2])
	offset += 2
	if respTopicNameSize != uint16(len(topicName)) {
		t.Errorf("response topic name size: got %d, want %d", respTopicNameSize, len(topicName))
	}

	respTopicName := string(response[offset : offset+int(respTopicNameSize)])
	offset += int(respTopicNameSize)
	if respTopicName != topicName {
		t.Errorf("response topic name: got %s, want %s", respTopicName, topicName)
	}

	// Partitions count
	respPartitionsCount := binary.BigEndian.Uint32(response[offset : offset+4])
	offset += 4
	if respPartitionsCount != 1 {
		t.Errorf("response partitions count: got %d, want 1", respPartitionsCount)
	}

	// Partition response: partition_id(4) + error_code(2) + base_offset(8) + log_append_time(8) + log_start_offset(8)
	partitionID := binary.BigEndian.Uint32(response[offset : offset+4])
	offset += 4
	if partitionID != 0 {
		t.Errorf("partition ID: got %d, want 0", partitionID)
	}

	errorCode := binary.BigEndian.Uint16(response[offset : offset+2])
	offset += 2
	if errorCode != 0 {
		t.Errorf("partition error: got %d, want 0", errorCode)
	}

	baseOffset := int64(binary.BigEndian.Uint64(response[offset : offset+8]))
	offset += 8
	if baseOffset < 0 {
		t.Errorf("base offset: got %d, want >= 0", baseOffset)
	}

	// Verify record was added to ledger
	ledger := h.GetLedger(topicName, 0)
	if ledger == nil {
		t.Fatalf("ledger not found for topic-partition")
	}

	if hwm := ledger.GetHighWaterMark(); hwm <= baseOffset {
		t.Errorf("high water mark: got %d, want > %d", hwm, baseOffset)
	}
}

func TestHandler_handleProduce_UnknownTopic(t *testing.T) {
	h := NewHandler()
	correlationID := uint32(444)

	// Build Produce request for non-existent topic
	clientID := "test-producer"
	topicName := "non-existent-topic"

	requestBody := make([]byte, 0, 128)

	// Client ID
	requestBody = append(requestBody, 0, byte(len(clientID)))
	requestBody = append(requestBody, []byte(clientID)...)

	// Acks (1)
	requestBody = append(requestBody, 0, 1)

	// Timeout
	requestBody = append(requestBody, 0, 0, 0x13, 0x88)

	// Topics count (1)
	requestBody = append(requestBody, 0, 0, 0, 1)

	// Topic name
	requestBody = append(requestBody, 0, byte(len(topicName)))
	requestBody = append(requestBody, []byte(topicName)...)

	// Partitions count (1)
	requestBody = append(requestBody, 0, 0, 0, 1)

	// Partition 0 with minimal record set
	requestBody = append(requestBody, 0, 0, 0, 0) // partition ID

	recordSet := make([]byte, 32)                   // dummy record set
	binary.BigEndian.PutUint32(recordSet[16:20], 1) // record count
	recordSetSize := uint32(len(recordSet))
	requestBody = append(requestBody, byte(recordSetSize>>24), byte(recordSetSize>>16), byte(recordSetSize>>8), byte(recordSetSize))
	requestBody = append(requestBody, recordSet...)

	response, err := h.handleProduce(correlationID, 7, requestBody)
	if err != nil {
		t.Fatalf("handleProduce: %v", err)
	}

	// Parse response to check for UNKNOWN_TOPIC_OR_PARTITION error
	offset := 8 + 2 + len(topicName) + 4 + 4 // skip to error code
	errorCode := binary.BigEndian.Uint16(response[offset : offset+2])
	if errorCode != 3 { // UNKNOWN_TOPIC_OR_PARTITION
		t.Errorf("expected UNKNOWN_TOPIC_OR_PARTITION error (3), got: %d", errorCode)
	}
}

func TestHandler_handleProduce_FireAndForget(t *testing.T) {
	h := NewHandler()
	correlationID := uint32(555)

	// Create a topic
	h.topics["test-topic"] = &TopicInfo{
		Name:       "test-topic",
		Partitions: 1,
		CreatedAt:  time.Now().UnixNano(),
	}

	// Build Produce request with acks=0 (fire and forget)
	clientID := "test-producer"
	topicName := "test-topic"

	requestBody := make([]byte, 0, 128)

	// Client ID
	requestBody = append(requestBody, 0, byte(len(clientID)))
	requestBody = append(requestBody, []byte(clientID)...)

	// Acks (0 - fire and forget)
	requestBody = append(requestBody, 0, 0)

	// Timeout
	requestBody = append(requestBody, 0, 0, 0x13, 0x88)

	// Topics count (1)
	requestBody = append(requestBody, 0, 0, 0, 1)

	// Topic name
	requestBody = append(requestBody, 0, byte(len(topicName)))
	requestBody = append(requestBody, []byte(topicName)...)

	// Partitions count (1)
	requestBody = append(requestBody, 0, 0, 0, 1)

	// Partition 0 with record set
	requestBody = append(requestBody, 0, 0, 0, 0) // partition ID

	recordSet := make([]byte, 32)
	binary.BigEndian.PutUint32(recordSet[16:20], 1) // record count
	recordSetSize := uint32(len(recordSet))
	requestBody = append(requestBody, byte(recordSetSize>>24), byte(recordSetSize>>16), byte(recordSetSize>>8), byte(recordSetSize))
	requestBody = append(requestBody, recordSet...)

	response, err := h.handleProduce(correlationID, 7, requestBody)
	if err != nil {
		t.Fatalf("handleProduce: %v", err)
	}

	// For acks=0, should return empty response
	if len(response) != 0 {
		t.Errorf("fire and forget response: got %d bytes, want 0", len(response))
	}

	// But record should still be added to ledger
	ledger := h.GetLedger(topicName, 0)
	if ledger == nil {
		t.Fatalf("ledger not found for topic-partition")
	}

	if hwm := ledger.GetHighWaterMark(); hwm == 0 {
		t.Errorf("high water mark: got %d, want > 0", hwm)
	}
}

func TestHandler_parseRecordSet(t *testing.T) {
	h := NewHandler()

	// Test with valid record set
	recordSet := make([]byte, 32)
	binary.BigEndian.PutUint64(recordSet[0:8], 0)   // base offset
	binary.BigEndian.PutUint32(recordSet[8:12], 24) // batch length
	binary.BigEndian.PutUint32(recordSet[12:16], 0) // partition leader epoch
	recordSet[16] = 2                               // magic byte
	binary.BigEndian.PutUint32(recordSet[16:20], 3) // record count at correct offset

	count, size, err := h.parseRecordSet(recordSet)
	if err != nil {
		t.Fatalf("parseRecordSet: %v", err)
	}
	if count != 3 {
		t.Errorf("record count: got %d, want 3", count)
	}
	if size != int32(len(recordSet)) {
		t.Errorf("total size: got %d, want %d", size, len(recordSet))
	}

	// Test with invalid record set (too small)
	invalidRecordSet := []byte{1, 2, 3}
	_, _, err = h.parseRecordSet(invalidRecordSet)
	if err == nil {
		t.Errorf("expected error for invalid record set")
	}

	// Test with unrealistic record count (should fall back to estimation)
	badRecordSet := make([]byte, 32)
	binary.BigEndian.PutUint32(badRecordSet[16:20], 999999999) // unrealistic count

	count, size, err = h.parseRecordSet(badRecordSet)
	if err != nil {
		t.Fatalf("parseRecordSet fallback: %v", err)
	}
	if count <= 0 {
		t.Errorf("fallback count: got %d, want > 0", count)
	}

	// Test with small batch (should estimate 1 record)
	smallRecordSet := make([]byte, 18) // Just enough for header check
	count, size, err = h.parseRecordSet(smallRecordSet)
	if err != nil {
		t.Fatalf("parseRecordSet small batch: %v", err)
	}
	if count != 1 {
		t.Errorf("small batch count: got %d, want 1", count)
	}
}
