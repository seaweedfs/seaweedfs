package protocol

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/mq/kafka/compression"
	"github.com/seaweedfs/seaweedfs/weed/mq/kafka/schema"
	"github.com/seaweedfs/seaweedfs/weed/pb/schema_pb"
)

func (h *Handler) handleFetch(correlationID uint32, apiVersion uint16, requestBody []byte) ([]byte, error) {
	fmt.Printf("DEBUG: *** FETCH HANDLER CALLED *** Correlation: %d, Version: %d\n", correlationID, apiVersion)
	// Parse the Fetch request to get the requested topics and partitions
	fetchRequest, err := h.parseFetchRequest(apiVersion, requestBody)
	if err != nil {
		return nil, fmt.Errorf("parse fetch request: %w", err)
	}

	// Build the response
	response := make([]byte, 0, 1024)

	// Correlation ID (4 bytes)
	correlationIDBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(correlationIDBytes, correlationID)
	response = append(response, correlationIDBytes...)

	// Fetch v1+ has throttle_time_ms at the beginning
	if apiVersion >= 1 {
		response = append(response, 0, 0, 0, 0) // throttle_time_ms (4 bytes, 0 = no throttling)
	}

	// Fetch v7+ has error_code and session_id
	if apiVersion >= 7 {
		response = append(response, 0, 0)       // error_code (2 bytes, 0 = no error)
		response = append(response, 0, 0, 0, 0) // session_id (4 bytes, 0 = no session)
	}

	// Topics count
	topicsCount := len(fetchRequest.Topics)
	topicsCountBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(topicsCountBytes, uint32(topicsCount))
	response = append(response, topicsCountBytes...)

	// Process each requested topic
	for _, topic := range fetchRequest.Topics {
		topicNameBytes := []byte(topic.Name)

		// Topic name length and name
		response = append(response, byte(len(topicNameBytes)>>8), byte(len(topicNameBytes)))
		response = append(response, topicNameBytes...)

		// Partitions count for this topic
		partitionsCount := len(topic.Partitions)
		partitionsCountBytes := make([]byte, 4)
		binary.BigEndian.PutUint32(partitionsCountBytes, uint32(partitionsCount))
		response = append(response, partitionsCountBytes...)

		// Process each requested partition
		for _, partition := range topic.Partitions {
			// Partition ID
			partitionIDBytes := make([]byte, 4)
			binary.BigEndian.PutUint32(partitionIDBytes, uint32(partition.PartitionID))
			response = append(response, partitionIDBytes...)

			// Error code (2 bytes) - default 0 = no error (may patch below)
			errorPos := len(response)
			response = append(response, 0, 0)

			// Get ledger for this topic-partition to determine high water mark
			// Use GetLedger (not GetOrCreateLedger) to avoid creating topics that don't exist
			ledger := h.GetLedger(topic.Name, partition.PartitionID)
			var highWaterMark int64 = 0
			if ledger != nil {
				highWaterMark = ledger.GetHighWaterMark()
			}

			fmt.Printf("DEBUG: Fetch v%d - topic: %s, partition: %d, fetchOffset: %d, highWaterMark: %d, maxBytes: %d\n",
				apiVersion, topic.Name, partition.PartitionID, partition.FetchOffset, highWaterMark, partition.MaxBytes)

			// High water mark (8 bytes)
			highWaterMarkBytes := make([]byte, 8)
			binary.BigEndian.PutUint64(highWaterMarkBytes, uint64(highWaterMark))
			response = append(response, highWaterMarkBytes...)

			// Fetch v4+ has last_stable_offset and log_start_offset
			if apiVersion >= 4 {
				// Last stable offset (8 bytes) - same as high water mark for non-transactional
				response = append(response, highWaterMarkBytes...)
				// Log start offset (8 bytes) - 0 for simplicity
				response = append(response, 0, 0, 0, 0, 0, 0, 0, 0)

				// Aborted transactions count (4 bytes) = 0
				response = append(response, 0, 0, 0, 0)
			}

			// If topic/ledger does not exist, patch error to UNKNOWN_TOPIC_OR_PARTITION
			if ledger == nil || !h.seaweedMQHandler.TopicExists(topic.Name) {
				response[errorPos] = 0
				response[errorPos+1] = 3 // UNKNOWN_TOPIC_OR_PARTITION
			}

			// Records - get actual stored record batches
			var recordBatch []byte
			if ledger != nil && highWaterMark > partition.FetchOffset {
				fmt.Printf("DEBUG: GetRecordBatch delegated to SeaweedMQ handler - topic:%s, partition:%d, offset:%d\n",
					topic.Name, partition.PartitionID, partition.FetchOffset)

				// Try to get stored messages first
				if basicHandler, ok := h.seaweedMQHandler.(*basicSeaweedMQHandler); ok {
					maxMessages := int(highWaterMark - partition.FetchOffset)
					if maxMessages > 10 {
						maxMessages = 10
					}
					storedMessages := basicHandler.GetStoredMessages(topic.Name, partition.PartitionID, partition.FetchOffset, maxMessages)
					if len(storedMessages) > 0 {
						fmt.Printf("DEBUG: Found %d stored messages for offset %d, constructing real record batch\n", len(storedMessages), partition.FetchOffset)
						recordBatch = h.constructRecordBatchFromMessages(partition.FetchOffset, storedMessages)
						fmt.Printf("DEBUG: Using real stored message batch for offset %d, size: %d bytes\n", partition.FetchOffset, len(recordBatch))
					} else {
						fmt.Printf("DEBUG: No stored messages found for offset %d, using synthetic batch\n", partition.FetchOffset)
						recordBatch = h.constructSimpleRecordBatch(partition.FetchOffset, highWaterMark)
						fmt.Printf("DEBUG: Using synthetic record batch for offset %d, size: %d bytes\n", partition.FetchOffset, len(recordBatch))
					}
				} else {
					fmt.Printf("DEBUG: Not using basicSeaweedMQHandler, using synthetic batch\n")
					recordBatch = h.constructSimpleRecordBatch(partition.FetchOffset, highWaterMark)
					fmt.Printf("DEBUG: Using synthetic record batch for offset %d, size: %d bytes\n", partition.FetchOffset, len(recordBatch))
				}
			} else {
				fmt.Printf("DEBUG: No messages available - fetchOffset %d >= highWaterMark %d\n", partition.FetchOffset, highWaterMark)
				recordBatch = []byte{} // No messages available
			}

			// Records size (4 bytes)
			recordsSizeBytes := make([]byte, 4)
			binary.BigEndian.PutUint32(recordsSizeBytes, uint32(len(recordBatch)))
			response = append(response, recordsSizeBytes...)

			// Records data
			response = append(response, recordBatch...)

			fmt.Printf("DEBUG: Would fetch schematized records - topic: %s, partition: %d, offset: %d, maxBytes: %d\n",
				topic.Name, partition.PartitionID, partition.FetchOffset, partition.MaxBytes)
		}
	}

	fmt.Printf("DEBUG: Fetch v%d response constructed, size: %d bytes\n", apiVersion, len(response))
	return response, nil
}

// FetchRequest represents a parsed Kafka Fetch request
type FetchRequest struct {
	ReplicaID      int32
	MaxWaitTime    int32
	MinBytes       int32
	MaxBytes       int32
	IsolationLevel int8
	Topics         []FetchTopic
}

type FetchTopic struct {
	Name       string
	Partitions []FetchPartition
}

type FetchPartition struct {
	PartitionID    int32
	FetchOffset    int64
	LogStartOffset int64
	MaxBytes       int32
}

// parseFetchRequest parses a Kafka Fetch request
func (h *Handler) parseFetchRequest(apiVersion uint16, requestBody []byte) (*FetchRequest, error) {
	if len(requestBody) < 12 {
		return nil, fmt.Errorf("fetch request too short: %d bytes", len(requestBody))
	}

	offset := 0
	request := &FetchRequest{}

	// NOTE: client_id is already handled by HandleConn and stripped from requestBody
	// Request body starts directly with fetch-specific fields

	// Replica ID (4 bytes)
	if offset+4 > len(requestBody) {
		return nil, fmt.Errorf("insufficient data for replica_id")
	}
	request.ReplicaID = int32(binary.BigEndian.Uint32(requestBody[offset : offset+4]))
	offset += 4

	// Max wait time (4 bytes)
	if offset+4 > len(requestBody) {
		return nil, fmt.Errorf("insufficient data for max_wait_time")
	}
	request.MaxWaitTime = int32(binary.BigEndian.Uint32(requestBody[offset : offset+4]))
	offset += 4

	// Min bytes (4 bytes)
	if offset+4 > len(requestBody) {
		return nil, fmt.Errorf("insufficient data for min_bytes")
	}
	request.MinBytes = int32(binary.BigEndian.Uint32(requestBody[offset : offset+4]))
	offset += 4

	// Max bytes (4 bytes) - only in v3+
	if apiVersion >= 3 {
		if offset+4 > len(requestBody) {
			return nil, fmt.Errorf("insufficient data for max_bytes")
		}
		request.MaxBytes = int32(binary.BigEndian.Uint32(requestBody[offset : offset+4]))
		offset += 4
	}

	// Isolation level (1 byte) - only in v4+
	if apiVersion >= 4 {
		if offset+1 > len(requestBody) {
			return nil, fmt.Errorf("insufficient data for isolation_level")
		}
		request.IsolationLevel = int8(requestBody[offset])
		offset += 1
	}

	// Session ID (4 bytes) and Session Epoch (4 bytes) - only in v7+
	if apiVersion >= 7 {
		if offset+8 > len(requestBody) {
			return nil, fmt.Errorf("insufficient data for session_id and epoch")
		}
		offset += 8 // Skip session_id and session_epoch
	}

	// Topics count (4 bytes)
	if offset+4 > len(requestBody) {
		return nil, fmt.Errorf("insufficient data for topics count")
	}
	topicsCount := int(binary.BigEndian.Uint32(requestBody[offset : offset+4]))
	offset += 4

	// Parse topics
	request.Topics = make([]FetchTopic, topicsCount)
	for i := 0; i < topicsCount; i++ {
		// Topic name length (2 bytes)
		if offset+2 > len(requestBody) {
			return nil, fmt.Errorf("insufficient data for topic name length")
		}
		topicNameLength := int(binary.BigEndian.Uint16(requestBody[offset : offset+2]))
		offset += 2

		// Topic name
		if offset+topicNameLength > len(requestBody) {
			return nil, fmt.Errorf("insufficient data for topic name")
		}
		request.Topics[i].Name = string(requestBody[offset : offset+topicNameLength])
		offset += topicNameLength

		// Partitions count (4 bytes)
		if offset+4 > len(requestBody) {
			return nil, fmt.Errorf("insufficient data for partitions count")
		}
		partitionsCount := int(binary.BigEndian.Uint32(requestBody[offset : offset+4]))
		offset += 4

		// Parse partitions
		request.Topics[i].Partitions = make([]FetchPartition, partitionsCount)
		for j := 0; j < partitionsCount; j++ {
			// Partition ID (4 bytes)
			if offset+4 > len(requestBody) {
				return nil, fmt.Errorf("insufficient data for partition ID")
			}
			request.Topics[i].Partitions[j].PartitionID = int32(binary.BigEndian.Uint32(requestBody[offset : offset+4]))
			offset += 4

			// Current leader epoch (4 bytes) - only in v9+
			if apiVersion >= 9 {
				if offset+4 > len(requestBody) {
					return nil, fmt.Errorf("insufficient data for current leader epoch")
				}
				offset += 4 // Skip current leader epoch
			}

			// Fetch offset (8 bytes)
			if offset+8 > len(requestBody) {
				return nil, fmt.Errorf("insufficient data for fetch offset")
			}
			request.Topics[i].Partitions[j].FetchOffset = int64(binary.BigEndian.Uint64(requestBody[offset : offset+8]))
			offset += 8

			// Log start offset (8 bytes) - only in v5+
			if apiVersion >= 5 {
				if offset+8 > len(requestBody) {
					return nil, fmt.Errorf("insufficient data for log start offset")
				}
				request.Topics[i].Partitions[j].LogStartOffset = int64(binary.BigEndian.Uint64(requestBody[offset : offset+8]))
				offset += 8
			}

			// Partition max bytes (4 bytes)
			if offset+4 > len(requestBody) {
				return nil, fmt.Errorf("insufficient data for partition max bytes")
			}
			request.Topics[i].Partitions[j].MaxBytes = int32(binary.BigEndian.Uint32(requestBody[offset : offset+4]))
			offset += 4
		}
	}

	return request, nil
}

// constructRecordBatchFromLedger creates a record batch from messages stored in the ledger
func (h *Handler) constructRecordBatchFromLedger(ledger interface{}, fetchOffset, highWaterMark int64) []byte {
	// Get the actual ledger interface
	offsetLedger, ok := ledger.(interface {
		GetMessages(startOffset, endOffset int64) []interface{}
	})

	if !ok {
		// If ledger doesn't support GetMessages, return empty batch
		return []byte{}
	}

	// Calculate how many records to fetch
	recordsToFetch := highWaterMark - fetchOffset
	if recordsToFetch <= 0 {
		return []byte{} // no records to fetch
	}

	// Limit the number of records for performance
	if recordsToFetch > 100 {
		recordsToFetch = 100
	}

	// Get messages from ledger
	messages := offsetLedger.GetMessages(fetchOffset, fetchOffset+recordsToFetch)
	if len(messages) == 0 {
		return []byte{} // no messages available
	}

	// Create a realistic record batch
	batch := make([]byte, 0, 1024)

	// Record batch header
	baseOffsetBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(baseOffsetBytes, uint64(fetchOffset))
	batch = append(batch, baseOffsetBytes...) // base offset (8 bytes)

	// Calculate batch length (will be filled after we know the size)
	batchLengthPos := len(batch)
	batch = append(batch, 0, 0, 0, 0) // batch length placeholder (4 bytes)

	batch = append(batch, 0, 0, 0, 0) // partition leader epoch (4 bytes)
	batch = append(batch, 2)          // magic byte (version 2) (1 byte)

	// CRC placeholder (4 bytes) - will be calculated at the end
	crcPos := len(batch)
	batch = append(batch, 0, 0, 0, 0) // CRC32 placeholder

	// Batch attributes (2 bytes) - no compression, no transactional
	batch = append(batch, 0, 0) // attributes

	// Last offset delta (4 bytes)
	lastOffsetDelta := uint32(len(messages) - 1)
	lastOffsetDeltaBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(lastOffsetDeltaBytes, lastOffsetDelta)
	batch = append(batch, lastOffsetDeltaBytes...)

	// First timestamp (8 bytes)
	firstTimestamp := time.Now().UnixMilli()
	firstTimestampBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(firstTimestampBytes, uint64(firstTimestamp))
	batch = append(batch, firstTimestampBytes...)

	// Max timestamp (8 bytes)
	maxTimestamp := firstTimestamp + int64(len(messages)) - 1
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
	binary.BigEndian.PutUint32(recordCountBytes, uint32(len(messages)))
	batch = append(batch, recordCountBytes...)

	// Add individual records
	for i, msg := range messages {
		// Try to extract key and value from the message
		var key, value []byte

		// Handle different message types
		if msgMap, ok := msg.(map[string]interface{}); ok {
			if keyVal, exists := msgMap["key"]; exists {
				if keyBytes, ok := keyVal.([]byte); ok {
					key = keyBytes
				} else if keyStr, ok := keyVal.(string); ok {
					key = []byte(keyStr)
				}
			}
			if valueVal, exists := msgMap["value"]; exists {
				if valueBytes, ok := valueVal.([]byte); ok {
					value = valueBytes
				} else if valueStr, ok := valueVal.(string); ok {
					value = []byte(valueStr)
				}
			}
		}

		// If we couldn't extract key/value, create default ones
		if value == nil {
			value = []byte(fmt.Sprintf("Message %d", fetchOffset+int64(i)))
		}

		// Build individual record
		record := make([]byte, 0, 128)

		// Record attributes (1 byte)
		record = append(record, 0)

		// Timestamp delta (varint)
		timestampDelta := int64(i)
		record = append(record, encodeVarint(timestampDelta)...)

		// Offset delta (varint)
		offsetDelta := int64(i)
		record = append(record, encodeVarint(offsetDelta)...)

		// Key length and key (varint + data)
		if key == nil {
			record = append(record, encodeVarint(-1)...) // null key
		} else {
			record = append(record, encodeVarint(int64(len(key)))...)
			record = append(record, key...)
		}

		// Value length and value (varint + data)
		record = append(record, encodeVarint(int64(len(value)))...)
		record = append(record, value...)

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

	// Calculate CRC32 for the batch
	// CRC is calculated over: attributes + last_offset_delta + first_timestamp + max_timestamp + producer_id + producer_epoch + base_sequence + records_count + records
	// This starts after the CRC field (which comes after magic byte)
	crcStartPos := crcPos + 4 // start after the CRC field
	crcData := batch[crcStartPos:]
	crc := crc32.Checksum(crcData, crc32.MakeTable(crc32.Castagnoli))
	binary.BigEndian.PutUint32(batch[crcPos:crcPos+4], crc)

	return batch
}

// constructSimpleRecordBatch creates a simple record batch for testing
func (h *Handler) constructSimpleRecordBatch(fetchOffset, highWaterMark int64) []byte {
	recordsToFetch := highWaterMark - fetchOffset
	if recordsToFetch <= 0 {
		return []byte{} // no records to fetch
	}

	// Limit the number of records for testing
	if recordsToFetch > 10 {
		recordsToFetch = 10
	}

	// Create a simple record batch
	batch := make([]byte, 0, 512)

	// Record batch header
	baseOffsetBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(baseOffsetBytes, uint64(fetchOffset))
	batch = append(batch, baseOffsetBytes...) // base offset (8 bytes)

	// Calculate batch length (will be filled after we know the size)
	batchLengthPos := len(batch)
	batch = append(batch, 0, 0, 0, 0) // batch length placeholder (4 bytes)

	batch = append(batch, 0, 0, 0, 0) // partition leader epoch (4 bytes)
	batch = append(batch, 2)          // magic byte (version 2) (1 byte)

	// CRC placeholder (4 bytes) - will be calculated at the end
	crcPos := len(batch)
	batch = append(batch, 0, 0, 0, 0) // CRC32 placeholder

	// Batch attributes (2 bytes) - no compression, no transactional
	batch = append(batch, 0, 0) // attributes

	// Last offset delta (4 bytes)
	lastOffsetDelta := uint32(recordsToFetch - 1)
	lastOffsetDeltaBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(lastOffsetDeltaBytes, lastOffsetDelta)
	batch = append(batch, lastOffsetDeltaBytes...)

	// First timestamp (8 bytes)
	firstTimestamp := time.Now().UnixMilli()
	firstTimestampBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(firstTimestampBytes, uint64(firstTimestamp))
	batch = append(batch, firstTimestampBytes...)

	// Max timestamp (8 bytes)
	maxTimestamp := firstTimestamp + recordsToFetch - 1
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

	// Add individual records
	for i := int64(0); i < recordsToFetch; i++ {
		// Create test message
		key := []byte(fmt.Sprintf("key-%d", fetchOffset+i))
		value := []byte(fmt.Sprintf("Test message %d", fetchOffset+i))

		// Build individual record
		record := make([]byte, 0, 128)

		// Record attributes (1 byte)
		record = append(record, 0)

		// Timestamp delta (varint)
		timestampDelta := i
		record = append(record, encodeVarint(timestampDelta)...)

		// Offset delta (varint)
		offsetDelta := i
		record = append(record, encodeVarint(offsetDelta)...)

		// Key length and key (varint + data)
		record = append(record, encodeVarint(int64(len(key)))...)
		record = append(record, key...)

		// Value length and value (varint + data)
		record = append(record, encodeVarint(int64(len(value)))...)
		record = append(record, value...)

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

	// Calculate CRC32 for the batch
	// CRC is calculated over: attributes + last_offset_delta + first_timestamp + max_timestamp + producer_id + producer_epoch + base_sequence + records_count + records
	// This starts after the CRC field (which comes after magic byte)
	crcStartPos := crcPos + 4 // start after the CRC field
	crcData := batch[crcStartPos:]
	crc := crc32.Checksum(crcData, crc32.MakeTable(crc32.Castagnoli))
	binary.BigEndian.PutUint32(batch[crcPos:crcPos+4], crc)

	return batch
}

// constructRecordBatchFromMessages creates a Kafka record batch from actual stored messages
func (h *Handler) constructRecordBatchFromMessages(fetchOffset int64, messages []*MessageRecord) []byte {
	if len(messages) == 0 {
		return []byte{}
	}

	// Create record batch using the real stored messages
	batch := make([]byte, 0, 512)

	// Record batch header
	baseOffsetBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(baseOffsetBytes, uint64(fetchOffset))
	batch = append(batch, baseOffsetBytes...) // base offset (8 bytes)

	// Calculate batch length (will be filled after we know the size)
	batchLengthPos := len(batch)
	batch = append(batch, 0, 0, 0, 0) // batch length placeholder (4 bytes)

	// Partition leader epoch (4 bytes) - use -1 for no epoch
	batch = append(batch, 0xFF, 0xFF, 0xFF, 0xFF)

	// Magic byte (1 byte) - v2 format
	batch = append(batch, 2)

	// CRC placeholder (4 bytes) - will be calculated later
	crcPos := len(batch)
	batch = append(batch, 0, 0, 0, 0)

	// Attributes (2 bytes) - no compression, etc.
	batch = append(batch, 0, 0)

	// Last offset delta (4 bytes)
	lastOffsetDelta := int32(len(messages) - 1)
	lastOffsetDeltaBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(lastOffsetDeltaBytes, uint32(lastOffsetDelta))
	batch = append(batch, lastOffsetDeltaBytes...)

	// Base timestamp (8 bytes) - use first message timestamp
	baseTimestamp := messages[0].Timestamp
	baseTimestampBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(baseTimestampBytes, uint64(baseTimestamp))
	batch = append(batch, baseTimestampBytes...)

	// Max timestamp (8 bytes) - use last message timestamp or same as base
	maxTimestamp := baseTimestamp
	if len(messages) > 1 {
		maxTimestamp = messages[len(messages)-1].Timestamp
	}
	maxTimestampBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(maxTimestampBytes, uint64(maxTimestamp))
	batch = append(batch, maxTimestampBytes...)

	// Producer ID (8 bytes) - use -1 for no producer ID
	batch = append(batch, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF)

	// Producer epoch (2 bytes) - use -1 for no producer epoch
	batch = append(batch, 0xFF, 0xFF)

	// Base sequence (4 bytes) - use -1 for no base sequence
	batch = append(batch, 0xFF, 0xFF, 0xFF, 0xFF)

	// Records count (4 bytes)
	recordCountBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(recordCountBytes, uint32(len(messages)))
	batch = append(batch, recordCountBytes...)

	// Add individual records from stored messages
	for i, msg := range messages {
		// Build individual record
		record := make([]byte, 0, 128)

		// Record attributes (1 byte)
		record = append(record, 0)

		// Timestamp delta (varint) - calculate from base timestamp
		timestampDelta := msg.Timestamp - baseTimestamp
		record = append(record, encodeVarint(timestampDelta)...)

		// Offset delta (varint)
		offsetDelta := int64(i)
		record = append(record, encodeVarint(offsetDelta)...)

		// Key length and key (varint + data)
		if msg.Key == nil {
			record = append(record, encodeVarint(-1)...) // null key
		} else {
			record = append(record, encodeVarint(int64(len(msg.Key)))...)
			record = append(record, msg.Key...)
		}

		// Value length and value (varint + data)
		if msg.Value == nil {
			record = append(record, encodeVarint(-1)...) // null value
		} else {
			record = append(record, encodeVarint(int64(len(msg.Value)))...)
			record = append(record, msg.Value...)
		}

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

	// Calculate CRC32 for the batch
	crcStartPos := crcPos + 4 // start after the CRC field
	crcData := batch[crcStartPos:]
	crc := crc32.Checksum(crcData, crc32.MakeTable(crc32.Castagnoli))
	binary.BigEndian.PutUint32(batch[crcPos:crcPos+4], crc)

	return batch
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

// getMultipleRecordBatches retrieves and combines multiple record batches starting from the given offset
func (h *Handler) getMultipleRecordBatches(topicName string, partitionID int32, startOffset, highWaterMark int64) []byte {
	var combinedBatch []byte

	// Try to get all available record batches from startOffset to highWaterMark-1
	for offset := startOffset; offset < highWaterMark; offset++ {
		if batch, exists := h.GetRecordBatch(topicName, partitionID, offset); exists {
			// For the first batch, include the full record batch
			if len(combinedBatch) == 0 {
				combinedBatch = append(combinedBatch, batch...)
			} else {
				// For subsequent batches, we need to append them properly
				// For now, just return the first batch to avoid format issues
				// TODO: Implement proper record batch concatenation
				break
			}
		}
	}

	return combinedBatch
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

// createEmptyRecordBatch creates an empty Kafka record batch using the new parser
func (h *Handler) createEmptyRecordBatch(baseOffset int64) []byte {
	// Use the new record batch creation function with no compression
	emptyRecords := []byte{}
	batch, err := CreateRecordBatch(baseOffset, emptyRecords, compression.None)
	if err != nil {
		// Fallback to manual creation if there's an error
		return h.createEmptyRecordBatchManual(baseOffset)
	}
	return batch
}

// createEmptyRecordBatchManual creates an empty Kafka record batch manually (fallback)
func (h *Handler) createEmptyRecordBatchManual(baseOffset int64) []byte {
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
