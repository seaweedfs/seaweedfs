package protocol

import (
	"encoding/binary"
	"fmt"
	"time"
)

func (h *Handler) handleProduce(correlationID uint32, requestBody []byte) ([]byte, error) {
	// Parse minimal Produce request
	// Request format: client_id + acks(2) + timeout(4) + topics_array

	if len(requestBody) < 8 { // client_id_size(2) + acks(2) + timeout(4)
		return nil, fmt.Errorf("Produce request too short")
	}

	// Skip client_id
	clientIDSize := binary.BigEndian.Uint16(requestBody[0:2])
	offset := 2 + int(clientIDSize)

	if len(requestBody) < offset+10 { // acks(2) + timeout(4) + topics_count(4)
		return nil, fmt.Errorf("Produce request missing data")
	}

	// Parse acks and timeout
	acks := int16(binary.BigEndian.Uint16(requestBody[offset : offset+2]))
	offset += 2
	timeout := binary.BigEndian.Uint32(requestBody[offset : offset+4])
	offset += 4
	_ = timeout // unused for now

	topicsCount := binary.BigEndian.Uint32(requestBody[offset : offset+4])
	offset += 4

	response := make([]byte, 0, 1024)

	// Correlation ID
	correlationIDBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(correlationIDBytes, correlationID)
	response = append(response, correlationIDBytes...)

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

		// Check if topic exists, auto-create if it doesn't (simulates auto.create.topics.enable=true)
		h.topicsMu.Lock()
		_, topicExists := h.topics[topicName]
		if !topicExists {
			fmt.Printf("DEBUG: Auto-creating topic during Produce: %s\n", topicName)
			h.topics[topicName] = &TopicInfo{
				Name:       topicName,
				Partitions: 1, // Default to 1 partition
				CreatedAt:  time.Now().UnixNano(),
			}
			// Initialize ledger for partition 0
			h.GetOrCreateLedger(topicName, 0)
			topicExists = true
		}
		h.topicsMu.Unlock()

		// Response: topic_name_size(2) + topic_name + partitions_array
		response = append(response, byte(topicNameSize>>8), byte(topicNameSize))
		response = append(response, []byte(topicName)...)

		partitionsCountBytes := make([]byte, 4)
		binary.BigEndian.PutUint32(partitionsCountBytes, partitionsCount)
		response = append(response, partitionsCountBytes...)

		// Process each partition
		for j := uint32(0); j < partitionsCount && offset < len(requestBody); j++ {
			if len(requestBody) < offset+8 {
				break
			}

			// Parse partition: partition_id(4) + record_set_size(4) + record_set
			partitionID := binary.BigEndian.Uint32(requestBody[offset : offset+4])
			offset += 4

			recordSetSize := binary.BigEndian.Uint32(requestBody[offset : offset+4])
			offset += 4

			if len(requestBody) < offset+int(recordSetSize) {
				break
			}

			recordSetData := requestBody[offset : offset+int(recordSetSize)]
			offset += int(recordSetSize)

			// Response: partition_id(4) + error_code(2) + base_offset(8) + log_append_time(8) + log_start_offset(8)
			partitionIDBytes := make([]byte, 4)
			binary.BigEndian.PutUint32(partitionIDBytes, partitionID)
			response = append(response, partitionIDBytes...)

			var errorCode uint16 = 0
			var baseOffset int64 = 0
			currentTime := time.Now().UnixNano()

			if !topicExists {
				errorCode = 3 // UNKNOWN_TOPIC_OR_PARTITION
			} else {
				// Process the record set
				recordCount, totalSize, parseErr := h.parseRecordSet(recordSetData)
				if parseErr != nil {
					errorCode = 42 // INVALID_RECORD
				} else if recordCount > 0 {
					if h.useSeaweedMQ {
						// Use SeaweedMQ integration for production
						offset, err := h.produceToSeaweedMQ(topicName, int32(partitionID), recordSetData)
						if err != nil {
							errorCode = 1 // UNKNOWN_SERVER_ERROR
						} else {
							baseOffset = offset
						}
					} else {
						// Use legacy in-memory mode for tests
						ledger := h.GetOrCreateLedger(topicName, int32(partitionID))
						baseOffset = ledger.AssignOffsets(int64(recordCount))

						// Append each record to the ledger
						avgSize := totalSize / recordCount
						for k := int64(0); k < int64(recordCount); k++ {
							ledger.AppendRecord(baseOffset+k, currentTime+k*1000, avgSize)
						}
					}
				}
			}

			// Error code
			response = append(response, byte(errorCode>>8), byte(errorCode))

			// Base offset (8 bytes)
			baseOffsetBytes := make([]byte, 8)
			binary.BigEndian.PutUint64(baseOffsetBytes, uint64(baseOffset))
			response = append(response, baseOffsetBytes...)

			// Log append time (8 bytes) - timestamp when appended
			logAppendTimeBytes := make([]byte, 8)
			binary.BigEndian.PutUint64(logAppendTimeBytes, uint64(currentTime))
			response = append(response, logAppendTimeBytes...)

			// Log start offset (8 bytes) - same as base for now
			logStartOffsetBytes := make([]byte, 8)
			binary.BigEndian.PutUint64(logStartOffsetBytes, uint64(baseOffset))
			response = append(response, logStartOffsetBytes...)
		}
	}

	// Add throttle time at the end (4 bytes)
	response = append(response, 0, 0, 0, 0)

	// If acks=0, return empty response (fire and forget)
	if acks == 0 {
		return []byte{}, nil
	}

	return response, nil
}

// parseRecordSet parses a Kafka record set and returns the number of records and total size
// TODO: CRITICAL - This is a simplified parser that needs complete rewrite for protocol compatibility
// Missing:
// - Proper record batch format parsing (v0, v1, v2)
// - Compression support (gzip, snappy, lz4, zstd) 
// - CRC32 validation
// - Transaction markers and control records
// - Individual record extraction (key, value, headers, timestamps)
func (h *Handler) parseRecordSet(recordSetData []byte) (recordCount int32, totalSize int32, err error) {
	if len(recordSetData) < 12 { // minimum record set size
		return 0, 0, fmt.Errorf("record set too small")
	}

	// For Phase 1, we'll do a very basic parse to count records
	// In a full implementation, this would parse the record batch format properly

	// Record batch header: base_offset(8) + length(4) + partition_leader_epoch(4) + magic(1) + ...
	if len(recordSetData) < 17 {
		return 0, 0, fmt.Errorf("invalid record batch header")
	}

	// Skip to record count (at offset 16 in record batch)
	if len(recordSetData) < 20 {
		// Assume single record for very small batches
		return 1, int32(len(recordSetData)), nil
	}

	// Try to read record count from the batch header
	recordCount = int32(binary.BigEndian.Uint32(recordSetData[16:20]))

	// Validate record count is reasonable
	if recordCount <= 0 || recordCount > 1000000 { // sanity check
		// Fallback to estimating based on size
		estimatedCount := int32(len(recordSetData)) / 32 // rough estimate
		if estimatedCount <= 0 {
			estimatedCount = 1
		}
		return estimatedCount, int32(len(recordSetData)), nil
	}

	return recordCount, int32(len(recordSetData)), nil
}

// produceToSeaweedMQ publishes a single record to SeaweedMQ (simplified for Phase 2)
func (h *Handler) produceToSeaweedMQ(topic string, partition int32, recordSetData []byte) (int64, error) {
	// For Phase 2, we'll extract a simple key-value from the record set
	// In a full implementation, this would parse the entire batch properly

	// Extract first record from record set (simplified)
	key, value := h.extractFirstRecord(recordSetData)

	// Publish to SeaweedMQ
	return h.seaweedMQHandler.ProduceRecord(topic, partition, key, value)
}

// extractFirstRecord extracts the first record from a Kafka record set (simplified)
// TODO: CRITICAL - This function returns placeholder data instead of parsing real records
// For real client compatibility, need to:
// - Parse record batch header properly 
// - Extract actual key/value from first record in batch
// - Handle compressed record batches
// - Support all record formats (v0, v1, v2)
func (h *Handler) extractFirstRecord(recordSetData []byte) ([]byte, []byte) {
	// For Phase 2, create a simple placeholder record
	// This represents what would be extracted from the actual Kafka record batch

	key := []byte("kafka-key")
	value := fmt.Sprintf("kafka-message-data-%d", time.Now().UnixNano())

	// In a real implementation, this would:
	// 1. Parse the record batch header
	// 2. Extract individual records with proper key/value/timestamp
	// 3. Handle compression, transaction markers, etc.

	return key, []byte(value)
}
