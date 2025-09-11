package protocol

import (
	"encoding/binary"
	"fmt"
	"time"
)

func (h *Handler) handleFetch(correlationID uint32, requestBody []byte) ([]byte, error) {
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

// constructRecordBatch creates a simplified Kafka record batch for testing
// TODO: CRITICAL - This function creates fake record batches with dummy data
// For real client compatibility need to:
// - Read actual message data from SeaweedMQ/storage
// - Construct proper record batch headers with correct CRC
// - Use proper varint encoding (not single-byte shortcuts)
// - Support different record batch versions
// - Handle compressed batches if messages were stored compressed
// Currently returns fake "message-N" data that no real client expects
func (h *Handler) constructRecordBatch(ledger interface{}, fetchOffset, highWaterMark int64) []byte {
	// For Phase 1, create a simple record batch with dummy messages
	// This simulates what would come from real message storage

	recordsToFetch := highWaterMark - fetchOffset
	if recordsToFetch <= 0 {
		return []byte{} // no records to fetch
	}

	// Limit the number of records for Phase 1
	if recordsToFetch > 10 {
		recordsToFetch = 10
	}

	// Create a simple record batch
	batch := make([]byte, 0, 256)

	// Record batch header
	baseOffsetBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(baseOffsetBytes, uint64(fetchOffset))
	batch = append(batch, baseOffsetBytes...) // base offset

	// Calculate batch length (will be filled after we know the size)
	batchLengthPos := len(batch)
	batch = append(batch, 0, 0, 0, 0) // batch length placeholder

	batch = append(batch, 0, 0, 0, 0) // partition leader epoch
	batch = append(batch, 2)          // magic byte (version 2)

	// CRC placeholder
	batch = append(batch, 0, 0, 0, 0) // CRC32 (simplified)

	// Batch attributes
	batch = append(batch, 0, 0) // attributes

	// Last offset delta
	lastOffsetDelta := uint32(recordsToFetch - 1)
	lastOffsetDeltaBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(lastOffsetDeltaBytes, lastOffsetDelta)
	batch = append(batch, lastOffsetDeltaBytes...)

	// First timestamp
	firstTimestamp := time.Now().UnixNano()
	firstTimestampBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(firstTimestampBytes, uint64(firstTimestamp))
	batch = append(batch, firstTimestampBytes...)

	// Max timestamp
	maxTimestamp := firstTimestamp + int64(recordsToFetch)*1000000 // 1ms apart
	maxTimestampBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(maxTimestampBytes, uint64(maxTimestamp))
	batch = append(batch, maxTimestampBytes...)

	// Producer ID, Producer Epoch, Base Sequence
	batch = append(batch, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF) // producer ID (-1)
	batch = append(batch, 0xFF, 0xFF)                                     // producer epoch (-1)
	batch = append(batch, 0xFF, 0xFF, 0xFF, 0xFF)                         // base sequence (-1)

	// Record count
	recordCountBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(recordCountBytes, uint32(recordsToFetch))
	batch = append(batch, recordCountBytes...)

	// Add simple records
	for i := int64(0); i < recordsToFetch; i++ {
		// Each record: length + attributes + timestamp_delta + offset_delta + key_length + key + value_length + value + headers_count
		record := make([]byte, 0, 32)

		// Record attributes
		record = append(record, 0)

		// Timestamp delta (varint - simplified to 1 byte)
		timestampDelta := byte(i) // simple delta
		record = append(record, timestampDelta)

		// Offset delta (varint - simplified to 1 byte)
		offsetDelta := byte(i)
		record = append(record, offsetDelta)

		// Key length (-1 = null key)
		record = append(record, 0xFF)

		// Value (simple test message)
		value := fmt.Sprintf("message-%d", fetchOffset+i)
		record = append(record, byte(len(value))) // value length
		record = append(record, []byte(value)...) // value

		// Headers count (0)
		record = append(record, 0)

		// Record length (varint - simplified)
		recordLength := byte(len(record))
		batch = append(batch, recordLength)
		batch = append(batch, record...)
	}

	// Fill in the batch length
	batchLength := uint32(len(batch) - batchLengthPos - 4)
	binary.BigEndian.PutUint32(batch[batchLengthPos:batchLengthPos+4], batchLength)

	return batch
}
