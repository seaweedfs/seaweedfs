package protocol

import (
	"context"
	"encoding/binary"
	"testing"
	"time"
)

func TestHandler_handleFetch(t *testing.T) {
	h := NewTestHandler()
	correlationID := uint32(666)

	// Create a topic and add some records
	topicName := "test-topic"
	// Mock SeaweedMQ handler for testing - in real tests, this would use a proper mock
	// For now, just comment out the topic creation as it's handled by SeaweedMQ handler

	// Add some records to the ledger
	ledger := h.GetOrCreateLedger(topicName, 0)
	baseOffset := ledger.AssignOffsets(3)
	currentTime := time.Now().UnixNano()
	ledger.AppendRecord(baseOffset+0, currentTime+0, 100)
	ledger.AppendRecord(baseOffset+1, currentTime+1000, 200)
	ledger.AppendRecord(baseOffset+2, currentTime+2000, 150)

	// Build a Fetch request
	requestBody := make([]byte, 0, 256)

	// NOTE: client_id is handled by HandleConn and stripped before reaching handler
	// Start directly with Fetch-specific fields

	// Replica ID (-1 for consumer)
	requestBody = append(requestBody, 0xFF, 0xFF, 0xFF, 0xFF)

	// Max wait time (5000ms)
	requestBody = append(requestBody, 0, 0, 0x13, 0x88)

	// Min bytes (1)
	requestBody = append(requestBody, 0, 0, 0, 1)

	// Max bytes (1MB)
	requestBody = append(requestBody, 0, 0x10, 0, 0)

	// Isolation level (0 = read uncommitted)
	requestBody = append(requestBody, 0)

	// Session ID (0)
	requestBody = append(requestBody, 0, 0, 0, 0)

	// Epoch (0)
	requestBody = append(requestBody, 0, 0, 0, 0)

	// Topics count (1)
	requestBody = append(requestBody, 0, 0, 0, 1)

	// Topic name
	requestBody = append(requestBody, 0, byte(len(topicName)))
	requestBody = append(requestBody, []byte(topicName)...)

	// Partitions count (1)
	requestBody = append(requestBody, 0, 0, 0, 1)

	// Partition 0
	requestBody = append(requestBody, 0, 0, 0, 0) // partition ID
	// NOTE: current leader epoch only in v9+, not v7
	requestBody = append(requestBody, 0, 0, 0, 0, 0, 0, 0, byte(baseOffset)) // fetch offset
	requestBody = append(requestBody, 0, 0, 0, 0, 0, 0, 0, 0)                // log start offset
	requestBody = append(requestBody, 0, 0, 0x10, 0)                         // partition max bytes (1MB)

	response, err := h.handleFetch(context.Background(), correlationID, 7, requestBody)
	if err != nil {
		t.Fatalf("handleFetch: %v", err)
	}

	if len(response) < 60 { // minimum expected size
		t.Fatalf("response too short: %d bytes", len(response))
	}

	// Check response structure
	respCorrelationID := binary.BigEndian.Uint32(response[0:4])
	if respCorrelationID != correlationID {
		t.Errorf("correlation ID: got %d, want %d", respCorrelationID, correlationID)
	}

	// Check throttle time
	throttleTime := binary.BigEndian.Uint32(response[4:8])
	if throttleTime != 0 {
		t.Errorf("throttle time: got %d, want 0", throttleTime)
	}

	// Check error code
	errorCode := binary.BigEndian.Uint16(response[8:10])
	if errorCode != 0 {
		t.Errorf("error code: got %d, want 0", errorCode)
	}

	// Parse response structure (simplified validation)
	offset := 14 // skip correlation_id + throttle_time + error_code + session_id
	topicsCount := binary.BigEndian.Uint32(response[offset : offset+4])
	if topicsCount != 1 {
		t.Errorf("topics count: got %d, want 1", topicsCount)
	}

	offset += 4
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

	// Partition ID
	partitionID := binary.BigEndian.Uint32(response[offset : offset+4])
	offset += 4
	if partitionID != 0 {
		t.Errorf("partition ID: got %d, want 0", partitionID)
	}

	// Partition error code
	partitionErrorCode := binary.BigEndian.Uint16(response[offset : offset+2])
	offset += 2
	if partitionErrorCode != 0 {
		t.Errorf("partition error code: got %d, want 0", partitionErrorCode)
	}

	// High water mark
	highWaterMark := int64(binary.BigEndian.Uint64(response[offset : offset+8]))
	offset += 8
	if highWaterMark != 3 { // baseOffset + 3 records
		t.Errorf("high water mark: got %d, want %d", highWaterMark, baseOffset+3)
	}

	// Skip last_stable_offset, log_start_offset, aborted_transactions_count
	offset += 8 + 8 + 4

	// Records size
	recordsSize := binary.BigEndian.Uint32(response[offset : offset+4])
	offset += 4
	if recordsSize == 0 {
		t.Errorf("expected some records, got size 0")
	}

	// Verify we have records data
	if len(response) < offset+int(recordsSize) {
		t.Errorf("response shorter than expected records size")
	}
}

func TestHandler_handleFetch_UnknownTopic(t *testing.T) {
	h := NewTestHandler()
	correlationID := uint32(777)

	// Build Fetch request for non-existent topic (don't create it)
	topicName := "non-existent-topic"

	requestBody := make([]byte, 0, 128)

	// NOTE: client_id is handled by HandleConn and stripped before reaching handler
	// Start directly with Fetch-specific fields

	// Standard Fetch parameters
	requestBody = append(requestBody, 0xFF, 0xFF, 0xFF, 0xFF) // replica ID
	requestBody = append(requestBody, 0, 0, 0x13, 0x88)       // max wait time
	requestBody = append(requestBody, 0, 0, 0, 1)             // min bytes
	requestBody = append(requestBody, 0, 0x10, 0, 0)          // max bytes
	requestBody = append(requestBody, 0)                      // isolation level
	requestBody = append(requestBody, 0, 0, 0, 0)             // session ID
	requestBody = append(requestBody, 0, 0, 0, 0)             // epoch

	// Topics count (1)
	requestBody = append(requestBody, 0, 0, 0, 1)

	// Topic name
	requestBody = append(requestBody, 0, byte(len(topicName)))
	requestBody = append(requestBody, []byte(topicName)...)

	// Partitions count (1)
	requestBody = append(requestBody, 0, 0, 0, 1)

	// Partition 0
	requestBody = append(requestBody, 0, 0, 0, 0)             // partition ID
	requestBody = append(requestBody, 0, 0, 0, 0)             // current leader epoch
	requestBody = append(requestBody, 0, 0, 0, 0, 0, 0, 0, 0) // fetch offset
	requestBody = append(requestBody, 0, 0, 0, 0, 0, 0, 0, 0) // log start offset
	requestBody = append(requestBody, 0, 0, 0x10, 0)          // partition max bytes

	response, err := h.handleFetch(context.Background(), correlationID, 7, requestBody)
	if err != nil {
		t.Fatalf("handleFetch: %v", err)
	}

	// Parse response to check for UNKNOWN_TOPIC_OR_PARTITION error
	offset := 14 + 4 + 2 + len(topicName) + 4 + 4 // skip to partition error code
	partitionErrorCode := binary.BigEndian.Uint16(response[offset : offset+2])
	if partitionErrorCode != 3 { // UNKNOWN_TOPIC_OR_PARTITION
		t.Errorf("expected UNKNOWN_TOPIC_OR_PARTITION error (3), got: %d", partitionErrorCode)
	}
}

func TestHandler_handleFetch_EmptyPartition(t *testing.T) {
	h := NewTestHandler()
	correlationID := uint32(888)

	// Create a topic but don't add any records
	topicName := "empty-topic"
	// Mock SeaweedMQ handler for testing - in real tests, this would use a proper mock
	// For now, just comment out the topic creation as it's handled by SeaweedMQ handler

	// Get ledger but don't add records
	ledger := h.GetOrCreateLedger(topicName, 0)
	_ = ledger // ledger exists but is empty

	// Build Fetch request
	requestBody := make([]byte, 0, 128)

	// NOTE: client_id is handled by HandleConn and stripped before reaching handler
	// Start directly with Fetch-specific fields

	// Standard parameters
	requestBody = append(requestBody, 0xFF, 0xFF, 0xFF, 0xFF) // replica ID
	requestBody = append(requestBody, 0, 0, 0x13, 0x88)       // max wait time
	requestBody = append(requestBody, 0, 0, 0, 1)             // min bytes
	requestBody = append(requestBody, 0, 0x10, 0, 0)          // max bytes
	requestBody = append(requestBody, 0)                      // isolation level
	requestBody = append(requestBody, 0, 0, 0, 0)             // session ID
	requestBody = append(requestBody, 0, 0, 0, 0)             // epoch

	// Topics count (1)
	requestBody = append(requestBody, 0, 0, 0, 1)

	// Topic name
	requestBody = append(requestBody, 0, byte(len(topicName)))
	requestBody = append(requestBody, []byte(topicName)...)

	// Partitions count (1)
	requestBody = append(requestBody, 0, 0, 0, 1)

	// Partition 0 - fetch from offset 0
	requestBody = append(requestBody, 0, 0, 0, 0) // partition ID
	// NOTE: current leader epoch only in v9+, not v7
	requestBody = append(requestBody, 0, 0, 0, 0, 0, 0, 0, 0) // fetch offset
	requestBody = append(requestBody, 0, 0, 0, 0, 0, 0, 0, 0) // log start offset
	requestBody = append(requestBody, 0, 0, 0x10, 0)          // partition max bytes

	response, err := h.handleFetch(context.Background(), correlationID, 7, requestBody)
	if err != nil {
		t.Fatalf("handleFetch: %v", err)
	}

	// Parse response - should have no error but empty records
	offset := 14 + 4 + 2 + len(topicName) + 4 + 4 // skip to partition error code
	partitionErrorCode := binary.BigEndian.Uint16(response[offset : offset+2])
	if partitionErrorCode != 0 {
		t.Errorf("partition error code: got %d, want 0", partitionErrorCode)
	}

	// High water mark should be 0
	offset += 2
	highWaterMark := int64(binary.BigEndian.Uint64(response[offset : offset+8]))
	if highWaterMark != 0 {
		t.Errorf("high water mark: got %d, want 0", highWaterMark)
	}

	// Skip to records size
	offset += 8 + 8 + 4 // skip last_stable_offset, log_start_offset, aborted_transactions_count
	recordsSize := binary.BigEndian.Uint32(response[offset : offset+4])
	if recordsSize != 0 {
		t.Errorf("records size: got %d, want 0", recordsSize)
	}
}

func TestHandler_constructRecordBatch(t *testing.T) {
	h := NewTestHandler()

	// Test with simple parameters
	records := h.constructRecordBatch(nil, 0, 3)
	if len(records) == 0 {
		t.Errorf("expected some records, got empty")
	}

	// Should have proper record batch structure
	if len(records) < 61 { // minimum record batch header size
		t.Errorf("record batch too small: %d bytes", len(records))
	}

	// Check base offset
	baseOffset := int64(binary.BigEndian.Uint64(records[0:8]))
	if baseOffset != 0 {
		t.Errorf("base offset: got %d, want 0", baseOffset)
	}

	// Check magic byte
	if records[16] != 2 {
		t.Errorf("magic byte: got %d, want 2", records[16])
	}

	// Test with no records to fetch
	emptyRecords := h.constructRecordBatch(nil, 5, 5)
	if len(emptyRecords) != 0 {
		t.Errorf("expected empty records, got %d bytes", len(emptyRecords))
	}

	// Test with large range (should be limited)
	largeRecords := h.constructRecordBatch(nil, 0, 100)
	if len(largeRecords) == 0 {
		t.Errorf("expected some records for large range")
	}

	// Should be limited to reasonable size
	if len(largeRecords) > 10000 { // arbitrary reasonable limit
		t.Errorf("record batch too large: %d bytes", len(largeRecords))
	}
}
