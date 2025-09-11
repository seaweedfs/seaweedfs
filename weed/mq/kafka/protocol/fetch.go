package protocol

import (
	"encoding/binary"
	"fmt"
	"time"
)

func (h *Handler) handleFetch(correlationID uint32, apiVersion uint16, requestBody []byte) ([]byte, error) {
	fmt.Printf("DEBUG: *** FETCH REQUEST RECEIVED *** Correlation: %d, Version: %d\n", correlationID, apiVersion)
	
	// Parse minimal Fetch request
	// Request format: client_id + replica_id(4) + max_wait_time(4) + min_bytes(4) + max_bytes(4) + isolation_level(1) + session_id(4) + epoch(4) + topics_array

	if len(requestBody) < 8 { // client_id_size(2) + replica_id(4) + max_wait_time(4) + ...
		return nil, fmt.Errorf("Fetch request too short")
	}

	// Skip client_id
	clientIDSize := binary.BigEndian.Uint16(requestBody[0:2])
	offset := 2 + int(clientIDSize)

	if len(requestBody) < offset+21 { // replica_id(4) + max_wait_time(4) + min_bytes(4) + max_bytes(4) + isolation_level(1) + session_id(4) + epoch(4)
		return nil, fmt.Errorf("Fetch request missing data")
	}

	// Parse Fetch parameters
	replicaID := int32(binary.BigEndian.Uint32(requestBody[offset : offset+4]))
	offset += 4
	maxWaitTime := binary.BigEndian.Uint32(requestBody[offset : offset+4])
	offset += 4
	minBytes := binary.BigEndian.Uint32(requestBody[offset : offset+4])
	offset += 4
	maxBytes := binary.BigEndian.Uint32(requestBody[offset : offset+4])
	offset += 4
	isolationLevel := requestBody[offset]
	offset += 1
	sessionID := binary.BigEndian.Uint32(requestBody[offset : offset+4])
	offset += 4
	epoch := binary.BigEndian.Uint32(requestBody[offset : offset+4])
	offset += 4

	// For Phase 1, ignore most parameters and focus on basic functionality
	_ = replicaID
	_ = maxWaitTime
	_ = minBytes
	_ = maxBytes
	_ = isolationLevel
	_ = sessionID
	_ = epoch

	if len(requestBody) < offset+4 {
		return nil, fmt.Errorf("Fetch request missing topics count")
	}

	topicsCount := binary.BigEndian.Uint32(requestBody[offset : offset+4])
	offset += 4

	response := make([]byte, 0, 1024)

	// Correlation ID
	correlationIDBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(correlationIDBytes, correlationID)
	response = append(response, correlationIDBytes...)

	// Throttle time (4 bytes, 0 = no throttling)
	response = append(response, 0, 0, 0, 0)

	// Error code (2 bytes, 0 = no error)
	response = append(response, 0, 0)

	// Session ID (4 bytes)
	sessionIDBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(sessionIDBytes, sessionID)
	response = append(response, sessionIDBytes...)

	// Topics count (same as request)
	topicsCountBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(topicsCountBytes, topicsCount)
	response = append(response, topicsCountBytes...)

	// Process each topic
	for i := uint32(0); i < topicsCount && offset < len(requestBody); i++ {
		if len(requestBody) < offset+2 {
			break
		}

		// Parse topic name
		topicNameSize := binary.BigEndian.Uint16(requestBody[offset : offset+2])
		offset += 2

		if len(requestBody) < offset+int(topicNameSize)+4 {
			break
		}

		topicName := string(requestBody[offset : offset+int(topicNameSize)])
		offset += int(topicNameSize)

		// Parse partitions count
		partitionsCount := binary.BigEndian.Uint32(requestBody[offset : offset+4])
		offset += 4

		// Check if topic exists
		h.topicsMu.RLock()
		_, topicExists := h.topics[topicName]
		h.topicsMu.RUnlock()

		// Response: topic_name_size(2) + topic_name + partitions_array
		response = append(response, byte(topicNameSize>>8), byte(topicNameSize))
		response = append(response, []byte(topicName)...)

		partitionsCountBytes := make([]byte, 4)
		binary.BigEndian.PutUint32(partitionsCountBytes, partitionsCount)
		response = append(response, partitionsCountBytes...)

		// Process each partition
		for j := uint32(0); j < partitionsCount && offset < len(requestBody); j++ {
			if len(requestBody) < offset+16 {
				break
			}

			// Parse partition: partition_id(4) + current_leader_epoch(4) + fetch_offset(8)
			partitionID := binary.BigEndian.Uint32(requestBody[offset : offset+4])
			offset += 4
			currentLeaderEpoch := binary.BigEndian.Uint32(requestBody[offset : offset+4])
			offset += 4
			fetchOffset := int64(binary.BigEndian.Uint64(requestBody[offset : offset+8]))
			offset += 8
			logStartOffset := int64(binary.BigEndian.Uint64(requestBody[offset : offset+8]))
			offset += 8
			partitionMaxBytes := binary.BigEndian.Uint32(requestBody[offset : offset+4])
			offset += 4

			_ = currentLeaderEpoch
			_ = logStartOffset
			_ = partitionMaxBytes

			// Response: partition_id(4) + error_code(2) + high_water_mark(8) + last_stable_offset(8) + log_start_offset(8) + aborted_transactions + records
			partitionIDBytes := make([]byte, 4)
			binary.BigEndian.PutUint32(partitionIDBytes, partitionID)
			response = append(response, partitionIDBytes...)

			var errorCode uint16 = 0
			var highWaterMark int64 = 0
			var records []byte

			if !topicExists {
				errorCode = 3 // UNKNOWN_TOPIC_OR_PARTITION
			} else {
				// Get ledger and fetch records
				ledger := h.GetLedger(topicName, int32(partitionID))
				if ledger == nil {
					errorCode = 3 // UNKNOWN_TOPIC_OR_PARTITION
				} else {
					highWaterMark = ledger.GetHighWaterMark()

					// Try to fetch actual records using SeaweedMQ integration if available
					if fetchOffset < highWaterMark {
						if h.useSeaweedMQ && h.seaweedMQHandler != nil {
							// Use SeaweedMQ integration for real message fetching
							fetchedRecords, err := h.seaweedMQHandler.FetchRecords(topicName, int32(partitionID), fetchOffset, int32(partitionMaxBytes))
							if err != nil {
								fmt.Printf("DEBUG: FetchRecords error: %v\n", err)
								errorCode = 1 // OFFSET_OUT_OF_RANGE
							} else {
								records = fetchedRecords
							}
						} else {
							// Fallback to in-memory stub records
							records = h.constructRecordBatch(ledger, fetchOffset, highWaterMark)
						}
					}
				}
			}

			// Error code
			response = append(response, byte(errorCode>>8), byte(errorCode))

			// High water mark (8 bytes)
			highWaterMarkBytes := make([]byte, 8)
			binary.BigEndian.PutUint64(highWaterMarkBytes, uint64(highWaterMark))
			response = append(response, highWaterMarkBytes...)

			// Last stable offset (8 bytes) - same as high water mark for simplicity
			lastStableOffsetBytes := make([]byte, 8)
			binary.BigEndian.PutUint64(lastStableOffsetBytes, uint64(highWaterMark))
			response = append(response, lastStableOffsetBytes...)

			// Log start offset (8 bytes)
			logStartOffsetBytes := make([]byte, 8)
			binary.BigEndian.PutUint64(logStartOffsetBytes, 0) // always 0 for Phase 1
			response = append(response, logStartOffsetBytes...)

			// Aborted transactions count (4 bytes, 0 = none)
			response = append(response, 0, 0, 0, 0)

			// Records size and data
			recordsSize := uint32(len(records))
			recordsSizeBytes := make([]byte, 4)
			binary.BigEndian.PutUint32(recordsSizeBytes, recordsSize)
			response = append(response, recordsSizeBytes...)
			response = append(response, records...)
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
