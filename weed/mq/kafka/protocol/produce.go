package protocol

import (
	"encoding/binary"
	"fmt"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/mq/kafka/schema"
)

func (h *Handler) handleProduce(correlationID uint32, apiVersion uint16, requestBody []byte) ([]byte, error) {
	// Version-specific handling
	switch apiVersion {
	case 0, 1:
		return h.handleProduceV0V1(correlationID, apiVersion, requestBody)
	case 2, 3, 4, 5, 6, 7:
		return h.handleProduceV2Plus(correlationID, apiVersion, requestBody)
	default:
		return nil, fmt.Errorf("produce version %d not implemented yet", apiVersion)
	}
}

func (h *Handler) handleProduceV0V1(correlationID uint32, apiVersion uint16, requestBody []byte) ([]byte, error) {
	// DEBUG: Show version being handled
	fmt.Printf("DEBUG: Handling Produce v%d request\n", apiVersion)

	// DEBUG: Hex dump first 50 bytes to understand actual request format
	dumpLen := len(requestBody)
	if dumpLen > 50 {
		dumpLen = 50
	}
	fmt.Printf("DEBUG: Produce request hex dump (first %d bytes): %x\n", dumpLen, requestBody[:dumpLen])

	// Parse Produce v0/v1 request
	// Request format: client_id + acks(2) + timeout(4) + topics_array

	if len(requestBody) < 8 { // client_id_size(2) + acks(2) + timeout(4)
		return nil, fmt.Errorf("Produce request too short")
	}

	// Skip client_id
	clientIDSize := binary.BigEndian.Uint16(requestBody[0:2])
	fmt.Printf("DEBUG: Client ID size: %d\n", clientIDSize)

	if len(requestBody) < 2+int(clientIDSize) {
		return nil, fmt.Errorf("Produce request client_id too short")
	}

	clientID := string(requestBody[2 : 2+int(clientIDSize)])
	offset := 2 + int(clientIDSize)
	fmt.Printf("DEBUG: Client ID: '%s', offset after client_id: %d\n", clientID, offset)

	if len(requestBody) < offset+10 { // acks(2) + timeout(4) + topics_count(4)
		return nil, fmt.Errorf("Produce request missing data")
	}

	// Parse acks and timeout
	acks := int16(binary.BigEndian.Uint16(requestBody[offset : offset+2]))
	offset += 2
	fmt.Printf("DEBUG: Acks: %d, offset after acks: %d\n", acks, offset)

	timeout := binary.BigEndian.Uint32(requestBody[offset : offset+4])
	offset += 4
	fmt.Printf("DEBUG: Timeout: %d, offset after timeout: %d\n", timeout, offset)
	_ = timeout // unused for now

	topicsCount := binary.BigEndian.Uint32(requestBody[offset : offset+4])
	offset += 4
	fmt.Printf("DEBUG: Topics count: %d, offset after topics_count: %d\n", topicsCount, offset)

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

		fmt.Printf("DEBUG: Produce request for topic '%s' (%d partitions)\n", topicName, partitionsCount)

		// Check if topic exists, auto-create if it doesn't (simulates auto.create.topics.enable=true)
		h.topicsMu.Lock()
		_, topicExists := h.topics[topicName]

		// Debug: show all existing topics
		existingTopics := make([]string, 0, len(h.topics))
		for tName := range h.topics {
			existingTopics = append(existingTopics, tName)
		}
		fmt.Printf("DEBUG: Topic exists check: '%s' -> %v (existing topics: %v)\n", topicName, topicExists, existingTopics)
		if !topicExists {
			fmt.Printf("DEBUG: Auto-creating topic during Produce: %s\n", topicName)
			h.topics[topicName] = &TopicInfo{
				Name:       topicName,
				Partitions: 1, // Default to 1 partition
				CreatedAt:  time.Now().UnixNano(),
			}
			// Initialize ledger for partition 0
			h.GetOrCreateLedger(topicName, 0)
			topicExists = true // CRITICAL FIX: Update the flag after creating the topic
			fmt.Printf("DEBUG: Topic '%s' auto-created successfully, topicExists = %v\n", topicName, topicExists)
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

			fmt.Printf("DEBUG: Processing partition %d for topic '%s' (topicExists=%v)\n", partitionID, topicName, topicExists)

			if !topicExists {
				fmt.Printf("DEBUG: ERROR - Topic '%s' not found, returning UNKNOWN_TOPIC_OR_PARTITION\n", topicName)
				errorCode = 3 // UNKNOWN_TOPIC_OR_PARTITION
			} else {
				fmt.Printf("DEBUG: SUCCESS - Topic '%s' found, processing record set (size=%d)\n", topicName, recordSetSize)
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

	// Even for acks=0, kafka-go expects a minimal response structure
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

// handleProduceV2Plus handles Produce API v2-v7 (Kafka 0.11+)
func (h *Handler) handleProduceV2Plus(correlationID uint32, apiVersion uint16, requestBody []byte) ([]byte, error) {
	fmt.Printf("DEBUG: Handling Produce v%d request\n", apiVersion)

	// DEBUG: Hex dump first 100 bytes to understand actual request format
	dumpLen := len(requestBody)
	if dumpLen > 100 {
		dumpLen = 100
	}
	fmt.Printf("DEBUG: Produce v%d request hex dump (first %d bytes): %x\n", apiVersion, dumpLen, requestBody[:dumpLen])
	fmt.Printf("DEBUG: Produce v%d request total length: %d bytes\n", apiVersion, len(requestBody))

	// For now, use simplified parsing similar to v0/v1 but handle v2+ response format
	// In v2+, the main differences are:
	// - Request: transactional_id field (nullable string) at the beginning
	// - Response: throttle_time_ms field at the end (v1+)

	// Parse Produce v7 request format based on actual Sarama request
	// Format: client_id(STRING) + transactional_id(NULLABLE_STRING) + acks(INT16) + timeout_ms(INT32) + topics(ARRAY)

	offset := 0

	// Parse client_id (STRING: 2 bytes length + data)
	if len(requestBody) < 2 {
		return nil, fmt.Errorf("Produce v%d request too short for client_id", apiVersion)
	}
	clientIDLen := binary.BigEndian.Uint16(requestBody[offset : offset+2])
	offset += 2

	if len(requestBody) < offset+int(clientIDLen) {
		return nil, fmt.Errorf("Produce v%d request client_id too short", apiVersion)
	}
	clientID := string(requestBody[offset : offset+int(clientIDLen)])
	offset += int(clientIDLen)
	fmt.Printf("DEBUG: Produce v%d - client_id: %s\n", apiVersion, clientID)

	// Parse transactional_id (NULLABLE_STRING: 2 bytes length + data, -1 = null)
	if len(requestBody) < offset+2 {
		return nil, fmt.Errorf("Produce v%d request too short for transactional_id", apiVersion)
	}
	transactionalIDLen := int16(binary.BigEndian.Uint16(requestBody[offset : offset+2]))
	offset += 2

	var transactionalID string
	if transactionalIDLen == -1 {
		transactionalID = "null"
	} else if transactionalIDLen >= 0 {
		if len(requestBody) < offset+int(transactionalIDLen) {
			return nil, fmt.Errorf("Produce v%d request transactional_id too short", apiVersion)
		}
		transactionalID = string(requestBody[offset : offset+int(transactionalIDLen)])
		offset += int(transactionalIDLen)
	}
	fmt.Printf("DEBUG: Produce v%d - transactional_id: %s\n", apiVersion, transactionalID)

	// Parse acks (INT16) and timeout_ms (INT32)
	if len(requestBody) < offset+6 {
		return nil, fmt.Errorf("Produce v%d request missing acks/timeout", apiVersion)
	}

	acks := int16(binary.BigEndian.Uint16(requestBody[offset : offset+2]))
	offset += 2
	timeout := binary.BigEndian.Uint32(requestBody[offset : offset+4])
	offset += 4

	fmt.Printf("DEBUG: Produce v%d - acks: %d, timeout: %d\n", apiVersion, acks, timeout)

	// Parse topics array
	if len(requestBody) < offset+4 {
		return nil, fmt.Errorf("Produce v%d request missing topics count", apiVersion)
	}

	topicsCount := binary.BigEndian.Uint32(requestBody[offset : offset+4])
	offset += 4

	fmt.Printf("DEBUG: Produce v%d - topics count: %d\n", apiVersion, topicsCount)

	// Build response
	response := make([]byte, 0, 256)

	// Correlation ID (always first)
	correlationIDBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(correlationIDBytes, correlationID)
	response = append(response, correlationIDBytes...)

	// NOTE: For v1+, Sarama expects throttle_time_ms at the END of the response body.
	// We will append topics array first, and add throttle_time_ms just before returning.

	// Topics array length
	topicsCountBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(topicsCountBytes, topicsCount)
	response = append(response, topicsCountBytes...)

	// Process each topic with correct parsing and response format
	for i := uint32(0); i < topicsCount && offset < len(requestBody); i++ {
		// Parse topic name
		if len(requestBody) < offset+2 {
			break
		}

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

		fmt.Printf("DEBUG: Produce v%d - topic: %s, partitions: %d\n", apiVersion, topicName, partitionsCount)

		// Response: topic name (STRING: 2 bytes length + data)
		response = append(response, byte(topicNameSize>>8), byte(topicNameSize))
		response = append(response, []byte(topicName)...)

		// Response: partitions count (4 bytes)
		partitionsCountBytes := make([]byte, 4)
		binary.BigEndian.PutUint32(partitionsCountBytes, partitionsCount)
		response = append(response, partitionsCountBytes...)

		// Process each partition with correct parsing
		for j := uint32(0); j < partitionsCount && offset < len(requestBody); j++ {
			// Parse partition request: partition_id(4) + record_set_size(4) + record_set_data
			if len(requestBody) < offset+8 {
				break
			}

			partitionID := binary.BigEndian.Uint32(requestBody[offset : offset+4])
			offset += 4
			recordSetSize := binary.BigEndian.Uint32(requestBody[offset : offset+4])
			offset += 4

			// Skip the record set data for now (we'll implement proper parsing later)
			if len(requestBody) < offset+int(recordSetSize) {
				break
			}
			offset += int(recordSetSize)

			fmt.Printf("DEBUG: Produce v%d - partition: %d, record_set_size: %d\n", apiVersion, partitionID, recordSetSize)

			// Build correct Produce v2+ response for this partition
			// Format: partition_id(4) + error_code(2) + base_offset(8) + [log_append_time(8) if v>=2] + [log_start_offset(8) if v>=5]

			// partition_id (4 bytes)
			partitionIDBytes := make([]byte, 4)
			binary.BigEndian.PutUint32(partitionIDBytes, partitionID)
			response = append(response, partitionIDBytes...)

			// error_code (2 bytes) - 0 = success
			response = append(response, 0, 0)

			// base_offset (8 bytes) - offset of first message (stubbed)
			currentTime := time.Now().UnixNano()
			baseOffset := currentTime / 1000000 // Use timestamp as offset for now
			baseOffsetBytes := make([]byte, 8)
			binary.BigEndian.PutUint64(baseOffsetBytes, uint64(baseOffset))
			response = append(response, baseOffsetBytes...)

			// log_append_time (8 bytes) - v2+ field (actual timestamp, not -1)
			if apiVersion >= 2 {
				logAppendTimeBytes := make([]byte, 8)
				binary.BigEndian.PutUint64(logAppendTimeBytes, uint64(currentTime))
				response = append(response, logAppendTimeBytes...)
			}

			// log_start_offset (8 bytes) - v5+ field
			if apiVersion >= 5 {
				logStartOffsetBytes := make([]byte, 8)
				binary.BigEndian.PutUint64(logStartOffsetBytes, uint64(baseOffset))
				response = append(response, logStartOffsetBytes...)
			}
		}
	}

	// Append throttle_time_ms at the END for v1+
	if apiVersion >= 1 {
		response = append(response, 0, 0, 0, 0)
	}

	fmt.Printf("DEBUG: Produce v%d response: %d bytes\n", apiVersion, len(response))
	return response, nil
}

// processSchematizedMessage processes a message that may contain schema information
func (h *Handler) processSchematizedMessage(topicName string, partitionID int32, messageBytes []byte) error {
	// Only process if schema management is enabled
	if !h.IsSchemaEnabled() {
		return nil // Skip schema processing
	}

	// Check if message is schematized
	if !h.schemaManager.IsSchematized(messageBytes) {
		fmt.Printf("DEBUG: Message is not schematized, skipping schema processing\n")
		return nil // Not schematized, continue with normal processing
	}

	fmt.Printf("DEBUG: Processing schematized message for topic %s, partition %d\n", topicName, partitionID)

	// Decode the message
	decodedMsg, err := h.schemaManager.DecodeMessage(messageBytes)
	if err != nil {
		fmt.Printf("ERROR: Failed to decode schematized message: %v\n", err)
		// In permissive mode, we could continue with raw bytes
		// In strict mode, we should reject the message
		return fmt.Errorf("schema decoding failed: %w", err)
	}

	fmt.Printf("DEBUG: Successfully decoded message with schema ID %d, format %s, subject %s\n",
		decodedMsg.SchemaID, decodedMsg.SchemaFormat, decodedMsg.Subject)

	// If SeaweedMQ integration is enabled, store the decoded message
	if h.useSeaweedMQ && h.seaweedMQHandler != nil {
		return h.storeDecodedMessage(topicName, partitionID, decodedMsg)
	}

	// For in-memory mode, we could store metadata about the schema
	// For now, just log the successful decoding
	fmt.Printf("DEBUG: Schema decoding successful - would store RecordValue with %d fields\n",
		len(decodedMsg.RecordValue.Fields))

	return nil
}

// storeDecodedMessage stores a decoded message using SeaweedMQ integration
func (h *Handler) storeDecodedMessage(topicName string, partitionID int32, decodedMsg *schema.DecodedMessage) error {
	// TODO: Integrate with SeaweedMQ to store the RecordValue and RecordType
	// This would involve:
	// 1. Converting RecordValue to the format expected by SeaweedMQ
	// 2. Storing schema metadata alongside the message
	// 3. Maintaining schema evolution history
	// 4. Handling schema compatibility checks

	fmt.Printf("DEBUG: Would store decoded message to SeaweedMQ - topic: %s, partition: %d, schema: %d\n",
		topicName, partitionID, decodedMsg.SchemaID)

	// For Phase 4, we'll simulate successful storage
	// In Phase 8, we'll implement the full SeaweedMQ integration
	return nil
}

// extractMessagesFromRecordSet extracts individual messages from a Kafka record set
// This is a simplified implementation for Phase 4 - full implementation in Phase 8
func (h *Handler) extractMessagesFromRecordSet(recordSetData []byte) ([][]byte, error) {
	// For now, treat the entire record set as a single message
	// In a full implementation, this would:
	// 1. Parse the record batch header
	// 2. Handle compression (gzip, snappy, lz4, zstd)
	// 3. Extract individual records with their keys, values, headers
	// 4. Validate CRC32 checksums
	// 5. Handle different record batch versions (v0, v1, v2)

	if len(recordSetData) < 20 {
		return nil, fmt.Errorf("record set too small for extraction")
	}

	// Simplified: assume single message starting after record batch header
	// Real implementation would parse the record batch format properly
	messages := [][]byte{recordSetData}

	return messages, nil
}

// validateSchemaCompatibility checks if a message is compatible with existing schema
func (h *Handler) validateSchemaCompatibility(topicName string, messageBytes []byte) error {
	if !h.IsSchemaEnabled() {
		return nil // No validation if schema management is disabled
	}

	// Extract schema information
	schemaID, format, err := h.schemaManager.GetSchemaInfo(messageBytes)
	if err != nil {
		return nil // Not schematized, no validation needed
	}

	fmt.Printf("DEBUG: Validating schema compatibility - ID: %d, Format: %s, Topic: %s\n",
		schemaID, format, topicName)

	// TODO: Implement topic-specific schema validation
	// This would involve:
	// 1. Checking if the topic has a registered schema
	// 2. Validating schema evolution rules
	// 3. Ensuring backward/forward compatibility
	// 4. Handling schema versioning policies

	return nil
}
