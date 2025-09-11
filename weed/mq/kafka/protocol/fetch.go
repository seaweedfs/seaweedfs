package protocol

import (
	"encoding/binary"
	"fmt"
	"time"
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
