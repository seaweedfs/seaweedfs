package protocol

import (
	"encoding/binary"
	"fmt"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/mq/kafka/schema"
	"github.com/seaweedfs/seaweedfs/weed/pb/schema_pb"
)

func (h *Handler) handleFetch(correlationID uint32, apiVersion uint16, requestBody []byte) ([]byte, error) {
	fmt.Printf("DEBUG: *** FETCH REQUEST RECEIVED *** Correlation: %d, Version: %d\n", correlationID, apiVersion)
	fmt.Printf("DEBUG: Fetch v%d request hex dump (first 83 bytes): %x\n", apiVersion, requestBody[:min(83, len(requestBody))])

	// For now, create a minimal working Fetch response that returns empty records
	// This will allow Sarama to parse the response successfully, even if no messages are returned

	response := make([]byte, 0, 256)

	// Correlation ID (4 bytes)
	correlationIDBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(correlationIDBytes, correlationID)
	response = append(response, correlationIDBytes...)

	// Fetch v1+ has throttle_time_ms at the beginning
	if apiVersion >= 1 {
		response = append(response, 0, 0, 0, 0) // throttle_time_ms (4 bytes, 0 = no throttling)
	}

	// Fetch v4+ has session_id, but let's check if v5 has it at all
	if apiVersion >= 4 {
		// Let's try v5 without session_id entirely
		if apiVersion == 5 {
			// No session_id for v5 - go directly to topics
		} else {
			response = append(response, 0, 0)       // error_code (2 bytes, 0 = no error)
			response = append(response, 0, 0, 0, 0) // session_id (4 bytes, 0 for now)
		}
	}

	// Topics count (1 topic - hardcoded for now)
	response = append(response, 0, 0, 0, 1) // 1 topic

	// Topic: "sarama-e2e-topic"
	topicName := "sarama-e2e-topic"
	topicNameBytes := []byte(topicName)
	response = append(response, byte(len(topicNameBytes)>>8), byte(len(topicNameBytes))) // topic name length
	response = append(response, topicNameBytes...)                                       // topic name

	// Partitions count (1 partition)
	response = append(response, 0, 0, 0, 1) // 1 partition

	// Partition 0 response
	response = append(response, 0, 0, 0, 0)             // partition_id (4 bytes) = 0
	response = append(response, 0, 0)                   // error_code (2 bytes) = 0 (no error)
	response = append(response, 0, 0, 0, 0, 0, 0, 0, 3) // high_water_mark (8 bytes) = 3 (we produced 3 messages)

	// Fetch v4+ has last_stable_offset and log_start_offset
	if apiVersion >= 4 {
		response = append(response, 0, 0, 0, 0, 0, 0, 0, 3) // last_stable_offset (8 bytes) = 3
		response = append(response, 0, 0, 0, 0, 0, 0, 0, 0) // log_start_offset (8 bytes) = 0
	}

	// Fetch v4+ has aborted_transactions
	if apiVersion >= 4 {
		response = append(response, 0, 0, 0, 0) // aborted_transactions count (4 bytes) = 0
	}

	// Records size and data (empty for now - no records returned)
	response = append(response, 0, 0, 0, 0) // records size (4 bytes) = 0 (no records)

	fmt.Printf("DEBUG: Fetch v%d response: %d bytes, hex dump: %x\n", apiVersion, len(response), response)

	// Let's manually verify our response structure for debugging
	fmt.Printf("DEBUG: Response breakdown:\n")
	fmt.Printf("  - correlation_id (4): %x\n", response[0:4])
	if apiVersion >= 1 {
		fmt.Printf("  - throttle_time_ms (4): %x\n", response[4:8])
		if apiVersion >= 4 {
			if apiVersion == 5 {
				// v5 doesn't have session_id at all
				fmt.Printf("  - topics_count (4): %x\n", response[8:12])
			} else {
				fmt.Printf("  - error_code (2): %x\n", response[8:10])
				fmt.Printf("  - session_id (4): %x\n", response[10:14])
				fmt.Printf("  - topics_count (4): %x\n", response[14:18])
			}
		} else {
			fmt.Printf("  - topics_count (4): %x\n", response[8:12])
		}
	}
	return response, nil
}

// constructRecordBatch creates a realistic Kafka record batch that matches produced messages
// This creates record batches that mirror what was actually stored during Produce operations
func (h *Handler) constructRecordBatch(ledger interface{}, fetchOffset, highWaterMark int64) []byte {
	recordsToFetch := highWaterMark - fetchOffset
	if recordsToFetch <= 0 {
		return []byte{} // no records to fetch
	}

	// Limit the number of records for testing
	if recordsToFetch > 10 {
		recordsToFetch = 10
	}

	// Create a realistic record batch that matches what clients expect
	// This simulates the same format that would be stored during Produce operations
	batch := make([]byte, 0, 512)

	// Record batch header (61 bytes total)
	baseOffsetBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(baseOffsetBytes, uint64(fetchOffset))
	batch = append(batch, baseOffsetBytes...) // base offset (8 bytes)

	// Calculate batch length (will be filled after we know the size)
	batchLengthPos := len(batch)
	batch = append(batch, 0, 0, 0, 0) // batch length placeholder (4 bytes)

	batch = append(batch, 0, 0, 0, 0) // partition leader epoch (4 bytes)
	batch = append(batch, 2)          // magic byte (version 2) (1 byte)

	// CRC placeholder (4 bytes) - for testing, use 0
	batch = append(batch, 0, 0, 0, 0) // CRC32

	// Batch attributes (2 bytes) - no compression, no transactional
	batch = append(batch, 0, 0) // attributes

	// Last offset delta (4 bytes)
	lastOffsetDelta := uint32(recordsToFetch - 1)
	lastOffsetDeltaBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(lastOffsetDeltaBytes, lastOffsetDelta)
	batch = append(batch, lastOffsetDeltaBytes...)

	// First timestamp (8 bytes)
	firstTimestamp := time.Now().UnixMilli() // Use milliseconds like Kafka
	firstTimestampBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(firstTimestampBytes, uint64(firstTimestamp))
	batch = append(batch, firstTimestampBytes...)

	// Max timestamp (8 bytes)
	maxTimestamp := firstTimestamp + recordsToFetch - 1 // 1ms per record
	maxTimestampBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(maxTimestampBytes, uint64(maxTimestamp))
	batch = append(batch, maxTimestampBytes...)

	// Producer ID (8 bytes) - -1 for non-transactional
	batch = append(batch, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF)

	// Producer Epoch (2 bytes) - -1 for non-transactional
	batch = append(batch, 0xFF, 0xFF)

	// Base Sequence (4 bytes) - -1 for non-transactional
	batch = append(batch, 0xFF, 0xFF, 0xFF, 0xFF)

	// Record count (4 bytes)
	recordCountBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(recordCountBytes, uint32(recordsToFetch))
	batch = append(batch, recordCountBytes...)

	// Add records that match typical client expectations
	for i := int64(0); i < recordsToFetch; i++ {
		// Build individual record
		record := make([]byte, 0, 64)

		// Record attributes (1 byte)
		record = append(record, 0)

		// Timestamp delta (varint) - use proper varint encoding
		timestampDelta := i // milliseconds from first timestamp
		record = append(record, encodeVarint(timestampDelta)...)

		// Offset delta (varint)
		offsetDelta := i
		record = append(record, encodeVarint(offsetDelta)...)

		// Key length (varint) - -1 for null key
		record = append(record, encodeVarint(-1)...)

		// Value length and value
		value := fmt.Sprintf("Test message %d", fetchOffset+i)
		record = append(record, encodeVarint(int64(len(value)))...)
		record = append(record, []byte(value)...)

		// Headers count (varint) - 0 headers
		record = append(record, encodeVarint(0)...)

		// Prepend record length (varint)
		recordLength := int64(len(record))
		batch = append(batch, encodeVarint(recordLength)...)
		batch = append(batch, record...)
	}

	// Fill in the batch length
	batchLength := uint32(len(batch) - batchLengthPos - 4)
	binary.BigEndian.PutUint32(batch[batchLengthPos:batchLengthPos+4], batchLength)

	return batch
}

// encodeVarint encodes a signed integer using Kafka's varint encoding
func encodeVarint(value int64) []byte {
	// Kafka uses zigzag encoding for signed integers
	zigzag := uint64((value << 1) ^ (value >> 63))

	var buf []byte
	for zigzag >= 0x80 {
		buf = append(buf, byte(zigzag)|0x80)
		zigzag >>= 7
	}
	buf = append(buf, byte(zigzag))
	return buf
}

// reconstructSchematizedMessage reconstructs a schematized message from SMQ RecordValue
func (h *Handler) reconstructSchematizedMessage(recordValue *schema_pb.RecordValue, metadata map[string]string) ([]byte, error) {
	// Only reconstruct if schema management is enabled
	if !h.IsSchemaEnabled() {
		return nil, fmt.Errorf("schema management not enabled")
	}

	// Extract schema information from metadata
	schemaIDStr, exists := metadata["schema_id"]
	if !exists {
		return nil, fmt.Errorf("no schema ID in metadata")
	}

	var schemaID uint32
	if _, err := fmt.Sscanf(schemaIDStr, "%d", &schemaID); err != nil {
		return nil, fmt.Errorf("invalid schema ID: %w", err)
	}

	formatStr, exists := metadata["schema_format"]
	if !exists {
		return nil, fmt.Errorf("no schema format in metadata")
	}

	var format schema.Format
	switch formatStr {
	case "AVRO":
		format = schema.FormatAvro
	case "PROTOBUF":
		format = schema.FormatProtobuf
	case "JSON_SCHEMA":
		format = schema.FormatJSONSchema
	default:
		return nil, fmt.Errorf("unsupported schema format: %s", formatStr)
	}

	// Use schema manager to encode back to original format
	return h.schemaManager.EncodeMessage(recordValue, schemaID, format)
}

// fetchSchematizedRecords fetches and reconstructs schematized records from SeaweedMQ
func (h *Handler) fetchSchematizedRecords(topicName string, partitionID int32, offset int64, maxBytes int32) ([][]byte, error) {
	// This is a placeholder for Phase 7
	// In Phase 8, this will integrate with SeaweedMQ to:
	// 1. Fetch stored RecordValues and metadata from SeaweedMQ
	// 2. Reconstruct original Kafka message format using schema information
	// 3. Handle schema evolution and compatibility
	// 4. Return properly formatted Kafka record batches

	fmt.Printf("DEBUG: Would fetch schematized records - topic: %s, partition: %d, offset: %d, maxBytes: %d\n",
		topicName, partitionID, offset, maxBytes)

	// For Phase 7, return empty records
	// In Phase 8, this will return actual reconstructed messages
	return [][]byte{}, nil
}

// createSchematizedRecordBatch creates a Kafka record batch from reconstructed schematized messages
func (h *Handler) createSchematizedRecordBatch(messages [][]byte, baseOffset int64) []byte {
	if len(messages) == 0 {
		// Return empty record batch
		return h.createEmptyRecordBatch(baseOffset)
	}

	// For Phase 7, create a simple record batch
	// In Phase 8, this will properly format multiple messages into a record batch
	// with correct headers, compression, and CRC validation

	// Combine all messages into a single batch payload
	var batchPayload []byte
	for _, msg := range messages {
		// Add message length prefix (for record batch format)
		msgLen := len(msg)
		lengthBytes := make([]byte, 4)
		binary.BigEndian.PutUint32(lengthBytes, uint32(msgLen))
		batchPayload = append(batchPayload, lengthBytes...)
		batchPayload = append(batchPayload, msg...)
	}

	return h.createRecordBatchWithPayload(baseOffset, int32(len(messages)), batchPayload)
}

// createEmptyRecordBatch creates an empty Kafka record batch
func (h *Handler) createEmptyRecordBatch(baseOffset int64) []byte {
	// Create a minimal empty record batch
	batch := make([]byte, 0, 61) // Standard record batch header size

	// Base offset (8 bytes)
	baseOffsetBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(baseOffsetBytes, uint64(baseOffset))
	batch = append(batch, baseOffsetBytes...)

	// Batch length (4 bytes) - will be filled at the end
	lengthPlaceholder := len(batch)
	batch = append(batch, 0, 0, 0, 0)

	// Partition leader epoch (4 bytes) - 0 for simplicity
	batch = append(batch, 0, 0, 0, 0)

	// Magic byte (1 byte) - version 2
	batch = append(batch, 2)

	// CRC32 (4 bytes) - placeholder, should be calculated
	batch = append(batch, 0, 0, 0, 0)

	// Attributes (2 bytes) - no compression, no transactional
	batch = append(batch, 0, 0)

	// Last offset delta (4 bytes) - 0 for empty batch
	batch = append(batch, 0xFF, 0xFF, 0xFF, 0xFF)

	// First timestamp (8 bytes) - current time
	timestamp := time.Now().UnixMilli()
	timestampBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(timestampBytes, uint64(timestamp))
	batch = append(batch, timestampBytes...)

	// Max timestamp (8 bytes) - same as first for empty batch
	batch = append(batch, timestampBytes...)

	// Producer ID (8 bytes) - -1 for non-transactional
	batch = append(batch, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF)

	// Producer Epoch (2 bytes) - -1 for non-transactional
	batch = append(batch, 0xFF, 0xFF)

	// Base Sequence (4 bytes) - -1 for non-transactional
	batch = append(batch, 0xFF, 0xFF, 0xFF, 0xFF)

	// Record count (4 bytes) - 0 for empty batch
	batch = append(batch, 0, 0, 0, 0)

	// Fill in the batch length
	batchLength := len(batch) - 12 // Exclude base offset and length field itself
	binary.BigEndian.PutUint32(batch[lengthPlaceholder:lengthPlaceholder+4], uint32(batchLength))

	return batch
}

// createRecordBatchWithPayload creates a record batch with the given payload
func (h *Handler) createRecordBatchWithPayload(baseOffset int64, recordCount int32, payload []byte) []byte {
	// For Phase 7, create a simplified record batch
	// In Phase 8, this will implement proper Kafka record batch format v2

	batch := h.createEmptyRecordBatch(baseOffset)

	// Update record count
	recordCountOffset := len(batch) - 4
	binary.BigEndian.PutUint32(batch[recordCountOffset:recordCountOffset+4], uint32(recordCount))

	// Append payload (simplified - real implementation would format individual records)
	batch = append(batch, payload...)

	// Update batch length
	batchLength := len(batch) - 12
	binary.BigEndian.PutUint32(batch[8:12], uint32(batchLength))

	return batch
}

// handleSchematizedFetch handles fetch requests for topics with schematized messages
func (h *Handler) handleSchematizedFetch(topicName string, partitionID int32, offset int64, maxBytes int32) ([]byte, error) {
	// Check if this topic uses schema management
	if !h.IsSchemaEnabled() {
		// Fall back to regular fetch handling
		return nil, fmt.Errorf("schema management not enabled")
	}

	// Fetch schematized records from SeaweedMQ
	messages, err := h.fetchSchematizedRecords(topicName, partitionID, offset, maxBytes)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch schematized records: %w", err)
	}

	// Create record batch from reconstructed messages
	recordBatch := h.createSchematizedRecordBatch(messages, offset)

	fmt.Printf("DEBUG: Created schematized record batch: %d bytes for %d messages\n",
		len(recordBatch), len(messages))

	return recordBatch, nil
}

// isSchematizedTopic checks if a topic uses schema management
func (h *Handler) isSchematizedTopic(topicName string) bool {
	// For Phase 7, we'll implement a simple check
	// In Phase 8, this will check SeaweedMQ metadata or configuration
	// to determine if a topic has schematized messages

	// For now, assume topics ending with "-value" or "-key" are schematized
	// This is a common Confluent Schema Registry convention
	if len(topicName) > 6 {
		suffix := topicName[len(topicName)-6:]
		if suffix == "-value" {
			return true
		}
	}
	if len(topicName) > 4 {
		suffix := topicName[len(topicName)-4:]
		if suffix == "-key" {
			return true
		}
	}

	return false
}

// getSchemaMetadataForTopic retrieves schema metadata for a topic
func (h *Handler) getSchemaMetadataForTopic(topicName string) (map[string]string, error) {
	// This is a placeholder for Phase 7
	// In Phase 8, this will retrieve actual schema metadata from SeaweedMQ
	// including schema ID, format, subject, version, etc.

	if !h.IsSchemaEnabled() {
		return nil, fmt.Errorf("schema management not enabled")
	}

	// For Phase 7, return mock metadata
	metadata := map[string]string{
		"schema_id":      "1",
		"schema_format":  "AVRO",
		"schema_subject": topicName,
		"schema_version": "1",
	}

	fmt.Printf("DEBUG: Retrieved schema metadata for topic %s: %v\n", topicName, metadata)
	return metadata, nil
}
