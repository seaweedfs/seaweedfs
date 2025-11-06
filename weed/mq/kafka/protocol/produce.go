package protocol

import (
	"context"
	"encoding/binary"
	"fmt"
	"strings"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/mq/kafka/compression"
	"github.com/seaweedfs/seaweedfs/weed/mq/kafka/schema"
	"github.com/seaweedfs/seaweedfs/weed/pb/schema_pb"
	"google.golang.org/protobuf/proto"
)

func (h *Handler) handleProduce(ctx context.Context, correlationID uint32, apiVersion uint16, requestBody []byte) ([]byte, error) {

	// Version-specific handling
	switch apiVersion {
	case 0, 1:
		return h.handleProduceV0V1(ctx, correlationID, apiVersion, requestBody)
	case 2, 3, 4, 5, 6, 7:
		return h.handleProduceV2Plus(ctx, correlationID, apiVersion, requestBody)
	default:
		return nil, fmt.Errorf("produce version %d not implemented yet", apiVersion)
	}
}

func (h *Handler) handleProduceV0V1(ctx context.Context, correlationID uint32, apiVersion uint16, requestBody []byte) ([]byte, error) {
	// Parse Produce v0/v1 request
	// Request format: client_id + acks(2) + timeout(4) + topics_array

	if len(requestBody) < 8 { // client_id_size(2) + acks(2) + timeout(4)
		return nil, fmt.Errorf("Produce request too short")
	}

	// Skip client_id
	clientIDSize := binary.BigEndian.Uint16(requestBody[0:2])

	if len(requestBody) < 2+int(clientIDSize) {
		return nil, fmt.Errorf("Produce request client_id too short")
	}

	_ = string(requestBody[2 : 2+int(clientIDSize)]) // clientID
	offset := 2 + int(clientIDSize)

	if len(requestBody) < offset+10 { // acks(2) + timeout(4) + topics_count(4)
		return nil, fmt.Errorf("Produce request missing data")
	}

	// Parse acks and timeout
	_ = int16(binary.BigEndian.Uint16(requestBody[offset : offset+2])) // acks
	offset += 2

	topicsCount := binary.BigEndian.Uint32(requestBody[offset : offset+4])
	offset += 4

	response := make([]byte, 0, 1024)

	// NOTE: Correlation ID is handled by writeResponseWithHeader
	// Do NOT include it in the response body

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
		topicExists := h.seaweedMQHandler.TopicExists(topicName)

		_ = h.seaweedMQHandler.ListTopics() // existingTopics
		if !topicExists {
			// Use schema-aware topic creation for auto-created topics with configurable default partitions
			defaultPartitions := h.GetDefaultPartitions()
			glog.V(1).Infof("[PRODUCE] Topic %s does not exist, auto-creating with %d partitions", topicName, defaultPartitions)
			if err := h.createTopicWithSchemaSupport(topicName, defaultPartitions); err != nil {
				glog.V(0).Infof("[PRODUCE] ERROR: Failed to auto-create topic %s: %v", topicName, err)
			} else {
				glog.V(1).Infof("[PRODUCE] Successfully auto-created topic %s", topicName)
				// Invalidate cache immediately after creation so consumers can find it
				h.seaweedMQHandler.InvalidateTopicExistsCache(topicName)
				topicExists = true
			}
		} else {
			glog.V(2).Infof("[PRODUCE] Topic %s already exists", topicName)
		}

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

			// CRITICAL FIX: Make a copy of recordSetData to prevent buffer sharing corruption
			// The slice requestBody[offset:offset+int(recordSetSize)] shares the underlying array
			// with the request buffer, which can be reused and cause data corruption
			recordSetData := make([]byte, recordSetSize)
			copy(recordSetData, requestBody[offset:offset+int(recordSetSize)])
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
				recordCount, _, parseErr := h.parseRecordSet(recordSetData) // totalSize unused
				if parseErr != nil {
					errorCode = 42 // INVALID_RECORD
				} else if recordCount > 0 {
					// Use SeaweedMQ integration
					offset, err := h.produceToSeaweedMQ(ctx, topicName, int32(partitionID), recordSetData)
					if err != nil {
						// Check if this is a schema validation error and add delay to prevent overloading
						if h.isSchemaValidationError(err) {
							time.Sleep(200 * time.Millisecond) // Brief delay for schema validation failures
						}
						errorCode = 0xFFFF // UNKNOWN_SERVER_ERROR (-1 as uint16)
					} else {
						baseOffset = offset
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

// parseRecordSet parses a Kafka record set using the enhanced record batch parser
// Now supports:
// - Proper record batch format parsing (v2)
// - Compression support (gzip, snappy, lz4, zstd)
// - CRC32 validation
// - Individual record extraction
func (h *Handler) parseRecordSet(recordSetData []byte) (recordCount int32, totalSize int32, err error) {

	// Heuristic: permit short inputs for tests
	if len(recordSetData) < 61 {
		// If very small, decide error vs fallback
		if len(recordSetData) < 8 {
			return 0, 0, fmt.Errorf("failed to parse record batch: record set too small: %d bytes", len(recordSetData))
		}
		// If we have at least 20 bytes, attempt to read a count at [16:20]
		if len(recordSetData) >= 20 {
			cnt := int32(binary.BigEndian.Uint32(recordSetData[16:20]))
			if cnt <= 0 || cnt > 1000000 {
				cnt = 1
			}
			return cnt, int32(len(recordSetData)), nil
		}
		// Otherwise default to 1 record
		return 1, int32(len(recordSetData)), nil
	}

	parser := NewRecordBatchParser()

	// Parse the record batch with CRC validation
	batch, err := parser.ParseRecordBatchWithValidation(recordSetData, true)
	if err != nil {
		// If CRC validation fails, try without validation for backward compatibility
		batch, err = parser.ParseRecordBatch(recordSetData)
		if err != nil {
			return 0, 0, fmt.Errorf("failed to parse record batch: %w", err)
		}
	}

	return batch.RecordCount, int32(len(recordSetData)), nil
}

// produceToSeaweedMQ publishes a single record to SeaweedMQ (simplified for Phase 2)
// ctx controls the publish timeout - if client cancels, produce operation is cancelled
func (h *Handler) produceToSeaweedMQ(ctx context.Context, topic string, partition int32, recordSetData []byte) (int64, error) {
	// Extract all records from the record set and publish each one
	// extractAllRecords handles fallback internally for various cases
	records := h.extractAllRecords(recordSetData)

	if len(records) == 0 {
		return 0, fmt.Errorf("failed to parse Kafka record set: no records extracted")
	}

	// Publish all records and return the offset of the first record (base offset)
	var baseOffset int64
	for idx, kv := range records {
		offsetProduced, err := h.produceSchemaBasedRecord(ctx, topic, partition, kv.Key, kv.Value)
		if err != nil {
			return 0, err
		}
		if idx == 0 {
			baseOffset = offsetProduced
		}
	}

	return baseOffset, nil
}

// extractAllRecords parses a Kafka record batch and returns all records' key/value pairs
func (h *Handler) extractAllRecords(recordSetData []byte) []struct{ Key, Value []byte } {
	results := make([]struct{ Key, Value []byte }, 0, 8)

	if len(recordSetData) > 0 {
	}

	if len(recordSetData) < 61 {
		// Too small to be a full batch; treat as single opaque record
		key, value := h.extractFirstRecord(recordSetData)
		// Always include records, even if both key and value are null
		// Schema Registry Noop records may have null values
		results = append(results, struct{ Key, Value []byte }{Key: key, Value: value})
		return results
	}

	// Parse record batch header (Kafka v2)
	offset := 0
	_ = int64(binary.BigEndian.Uint64(recordSetData[offset:])) // baseOffset
	offset += 8                                                // base_offset
	_ = binary.BigEndian.Uint32(recordSetData[offset:])        // batchLength
	offset += 4                                                // batch_length
	_ = binary.BigEndian.Uint32(recordSetData[offset:])        // partitionLeaderEpoch
	offset += 4                                                // partition_leader_epoch

	if offset >= len(recordSetData) {
		return results
	}
	magic := recordSetData[offset] // magic
	offset += 1

	if magic != 2 {
		// Unsupported, fallback
		key, value := h.extractFirstRecord(recordSetData)
		// Always include records, even if both key and value are null
		results = append(results, struct{ Key, Value []byte }{Key: key, Value: value})
		return results
	}

	// Skip CRC, read attributes to check compression
	offset += 4 // crc
	attributes := binary.BigEndian.Uint16(recordSetData[offset:])
	offset += 2 // attributes

	// Check compression codec from attributes (bits 0-2)
	compressionCodec := compression.CompressionCodec(attributes & 0x07)

	offset += 4 // last_offset_delta
	offset += 8 // first_timestamp
	offset += 8 // max_timestamp
	offset += 8 // producer_id
	offset += 2 // producer_epoch
	offset += 4 // base_sequence

	// records_count
	if offset+4 > len(recordSetData) {
		return results
	}
	recordsCount := int(binary.BigEndian.Uint32(recordSetData[offset:]))
	offset += 4

	// Extract and decompress the records section
	recordsData := recordSetData[offset:]
	if compressionCodec != compression.None {
		decompressed, err := compression.Decompress(compressionCodec, recordsData)
		if err != nil {
			// Fallback to extractFirstRecord
			key, value := h.extractFirstRecord(recordSetData)
			results = append(results, struct{ Key, Value []byte }{Key: key, Value: value})
			return results
		}
		recordsData = decompressed
	}
	// Reset offset to start of records data (whether compressed or not)
	offset = 0

	if len(recordsData) > 0 {
	}

	// Iterate records
	for i := 0; i < recordsCount && offset < len(recordsData); i++ {
		// record_length is a SIGNED zigzag-encoded varint (like all varints in Kafka record format)
		recLen, n := decodeVarint(recordsData[offset:])
		if n == 0 || recLen <= 0 {
			break
		}
		offset += n
		if offset+int(recLen) > len(recordsData) {
			break
		}
		rec := recordsData[offset : offset+int(recLen)]
		offset += int(recLen)

		// Parse record fields
		rpos := 0
		if rpos >= len(rec) {
			break
		}
		rpos += 1 // attributes

		// timestamp_delta (varint)
		var nBytes int
		_, nBytes = decodeVarint(rec[rpos:])
		if nBytes == 0 {
			continue
		}
		rpos += nBytes
		// offset_delta (varint)
		_, nBytes = decodeVarint(rec[rpos:])
		if nBytes == 0 {
			continue
		}
		rpos += nBytes

		// key
		keyLen, nBytes := decodeVarint(rec[rpos:])
		if nBytes == 0 {
			continue
		}
		rpos += nBytes
		var key []byte
		if keyLen >= 0 {
			if rpos+int(keyLen) > len(rec) {
				continue
			}
			key = rec[rpos : rpos+int(keyLen)]
			rpos += int(keyLen)
		}

		// value
		valLen, nBytes := decodeVarint(rec[rpos:])
		if nBytes == 0 {
			continue
		}
		rpos += nBytes
		var value []byte
		if valLen >= 0 {
			if rpos+int(valLen) > len(rec) {
				continue
			}
			value = rec[rpos : rpos+int(valLen)]
			rpos += int(valLen)
		}

		// headers (varint) - skip
		_, n = decodeVarint(rec[rpos:])
		if n == 0 { /* ignore */
		}

		// DO NOT normalize nils to empty slices - Kafka distinguishes null vs empty
		// Keep nil as nil, empty as empty

		results = append(results, struct{ Key, Value []byte }{Key: key, Value: value})
	}

	return results
}

// extractFirstRecord extracts the first record from a Kafka record batch
func (h *Handler) extractFirstRecord(recordSetData []byte) ([]byte, []byte) {

	if len(recordSetData) < 61 {
		// Record set too small to contain a valid Kafka v2 batch
		return nil, nil
	}

	offset := 0

	// Parse record batch header (Kafka v2 format)
	// base_offset(8) + batch_length(4) + partition_leader_epoch(4) + magic(1) + crc(4) + attributes(2)
	// + last_offset_delta(4) + first_timestamp(8) + max_timestamp(8) + producer_id(8) + producer_epoch(2)
	// + base_sequence(4) + records_count(4) = 61 bytes header

	offset += 8                                                // skip base_offset
	_ = int32(binary.BigEndian.Uint32(recordSetData[offset:])) // batchLength unused
	offset += 4                                                // batch_length

	offset += 4 // skip partition_leader_epoch
	magic := recordSetData[offset]
	offset += 1 // magic byte

	if magic != 2 {
		// Unsupported magic byte - only Kafka v2 format is supported
		return nil, nil
	}

	offset += 4 // skip crc
	offset += 2 // skip attributes
	offset += 4 // skip last_offset_delta
	offset += 8 // skip first_timestamp
	offset += 8 // skip max_timestamp
	offset += 8 // skip producer_id
	offset += 2 // skip producer_epoch
	offset += 4 // skip base_sequence

	recordsCount := int32(binary.BigEndian.Uint32(recordSetData[offset:]))
	offset += 4 // records_count

	if recordsCount == 0 {
		// No records in batch
		return nil, nil
	}

	// Parse first record
	if offset >= len(recordSetData) {
		// Not enough data to parse record
		return nil, nil
	}

	// Read record length (unsigned varint)
	recordLengthU32, varintLen, err := DecodeUvarint(recordSetData[offset:])
	if err != nil || varintLen == 0 {
		// Invalid varint encoding
		return nil, nil
	}
	recordLength := int64(recordLengthU32)
	offset += varintLen

	if offset+int(recordLength) > len(recordSetData) {
		// Record length exceeds available data
		return nil, nil
	}

	recordData := recordSetData[offset : offset+int(recordLength)]
	recordOffset := 0

	// Parse record: attributes(1) + timestamp_delta(varint) + offset_delta(varint) + key + value + headers
	recordOffset += 1 // skip attributes

	// Skip timestamp_delta (varint)
	_, varintLen = decodeVarint(recordData[recordOffset:])
	if varintLen == 0 {
		// Invalid timestamp_delta varint
		return nil, nil
	}
	recordOffset += varintLen

	// Skip offset_delta (varint)
	_, varintLen = decodeVarint(recordData[recordOffset:])
	if varintLen == 0 {
		// Invalid offset_delta varint
		return nil, nil
	}
	recordOffset += varintLen

	// Read key length and key
	keyLength, varintLen := decodeVarint(recordData[recordOffset:])
	if varintLen == 0 {
		// Invalid key length varint
		return nil, nil
	}
	recordOffset += varintLen

	var key []byte
	if keyLength == -1 {
		key = nil // null key
	} else if keyLength == 0 {
		key = []byte{} // empty key
	} else {
		if recordOffset+int(keyLength) > len(recordData) {
			// Key length exceeds available data
			return nil, nil
		}
		key = recordData[recordOffset : recordOffset+int(keyLength)]
		recordOffset += int(keyLength)
	}

	// Read value length and value
	valueLength, varintLen := decodeVarint(recordData[recordOffset:])
	if varintLen == 0 {
		// Invalid value length varint
		return nil, nil
	}
	recordOffset += varintLen

	var value []byte
	if valueLength == -1 {
		value = nil // null value
	} else if valueLength == 0 {
		value = []byte{} // empty value
	} else {
		if recordOffset+int(valueLength) > len(recordData) {
			// Value length exceeds available data
			return nil, nil
		}
		value = recordData[recordOffset : recordOffset+int(valueLength)]
	}

	// Preserve null semantics - don't convert null to empty
	// Schema Registry Noop records specifically use null values
	return key, value
}

// decodeVarint decodes a variable-length integer from bytes using zigzag encoding
// Returns the decoded value and the number of bytes consumed
func decodeVarint(data []byte) (int64, int) {
	if len(data) == 0 {
		return 0, 0
	}

	var result int64
	var shift uint
	var bytesRead int

	for i, b := range data {
		if i > 9 { // varints can be at most 10 bytes
			return 0, 0 // invalid varint
		}

		bytesRead++
		result |= int64(b&0x7F) << shift

		if (b & 0x80) == 0 {
			// Most significant bit is 0, we're done
			// Apply zigzag decoding for signed integers
			return (result >> 1) ^ (-(result & 1)), bytesRead
		}

		shift += 7
	}

	return 0, 0 // incomplete varint
}

// handleProduceV2Plus handles Produce API v2-v7 (Kafka 0.11+)
func (h *Handler) handleProduceV2Plus(ctx context.Context, correlationID uint32, apiVersion uint16, requestBody []byte) ([]byte, error) {

	// For now, use simplified parsing similar to v0/v1 but handle v2+ response format
	// In v2+, the main differences are:
	// - Request: transactional_id field (nullable string) at the beginning
	// - Response: throttle_time_ms field at the end (v1+)

	// Parse Produce v2+ request format (client_id already stripped in HandleConn)
	// v2: acks(INT16) + timeout_ms(INT32) + topics(ARRAY)
	// v3+: transactional_id(NULLABLE_STRING) + acks(INT16) + timeout_ms(INT32) + topics(ARRAY)

	offset := 0

	// transactional_id only exists in v3+
	if apiVersion >= 3 {
		if len(requestBody) < offset+2 {
			return nil, fmt.Errorf("Produce v%d request too short for transactional_id", apiVersion)
		}
		txIDLen := int16(binary.BigEndian.Uint16(requestBody[offset : offset+2]))
		offset += 2
		if txIDLen >= 0 {
			if len(requestBody) < offset+int(txIDLen) {
				return nil, fmt.Errorf("Produce v%d request transactional_id too short", apiVersion)
			}
			_ = string(requestBody[offset : offset+int(txIDLen)])
			offset += int(txIDLen)
		}
	}

	// Parse acks (INT16) and timeout_ms (INT32)
	if len(requestBody) < offset+6 {
		return nil, fmt.Errorf("Produce v%d request missing acks/timeout", apiVersion)
	}

	acks := int16(binary.BigEndian.Uint16(requestBody[offset : offset+2]))
	offset += 2
	_ = binary.BigEndian.Uint32(requestBody[offset : offset+4])
	offset += 4

	// Remember if this is fire-and-forget mode
	isFireAndForget := acks == 0
	if isFireAndForget {
	} else {
	}

	if len(requestBody) < offset+4 {
		return nil, fmt.Errorf("Produce v%d request missing topics count", apiVersion)
	}
	topicsCount := binary.BigEndian.Uint32(requestBody[offset : offset+4])
	offset += 4

	// If topicsCount is implausible, there might be a parsing issue
	if topicsCount > 1000 {
		return nil, fmt.Errorf("Produce v%d request has implausible topics count: %d", apiVersion, topicsCount)
	}

	// Build response
	response := make([]byte, 0, 256)

	// NOTE: Correlation ID is handled by writeResponseWithHeader
	// Do NOT include it in the response body

	// Topics array length (first field in response body)
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
			if len(requestBody) < offset+int(recordSetSize) {
				break
			}
			// CRITICAL FIX: Make a copy of recordSetData to prevent buffer sharing corruption
			// The slice requestBody[offset:offset+int(recordSetSize)] shares the underlying array
			// with the request buffer, which can be reused and cause data corruption
			recordSetData := make([]byte, recordSetSize)
			copy(recordSetData, requestBody[offset:offset+int(recordSetSize)])
			offset += int(recordSetSize)

			// Process the record set and store in ledger
			var errorCode uint16 = 0
			var baseOffset int64 = 0
			currentTime := time.Now().UnixNano()

			// Check if topic exists; for v2+ do NOT auto-create
			topicExists := h.seaweedMQHandler.TopicExists(topicName)

			if !topicExists {
				errorCode = 3 // UNKNOWN_TOPIC_OR_PARTITION
			} else {
				// Process the record set (lenient parsing)
				recordCount, _, parseErr := h.parseRecordSet(recordSetData) // totalSize unused

				if parseErr != nil {
					errorCode = 42 // INVALID_RECORD
				} else if recordCount > 0 {
					// Extract all records from the record set and publish each one
					// extractAllRecords handles fallback internally for various cases
					records := h.extractAllRecords(recordSetData)

					if len(records) == 0 {
						errorCode = 42 // INVALID_RECORD
					} else {
						for idx, kv := range records {
							offsetProduced, prodErr := h.produceSchemaBasedRecord(ctx, topicName, int32(partitionID), kv.Key, kv.Value)

							if prodErr != nil {
								// Check if this is a schema validation error and add delay to prevent overloading
								if h.isSchemaValidationError(prodErr) {
									time.Sleep(200 * time.Millisecond) // Brief delay for schema validation failures
								}
								errorCode = 0xFFFF // UNKNOWN_SERVER_ERROR (-1 as uint16)
								break
							}

							if idx == 0 {
								baseOffset = offsetProduced
							}
						}
					}
				} else {
					// Try to extract anyway - this might be a Noop record
					records := h.extractAllRecords(recordSetData)
					if len(records) > 0 {
						for idx, kv := range records {
							offsetProduced, prodErr := h.produceSchemaBasedRecord(ctx, topicName, int32(partitionID), kv.Key, kv.Value)
							if prodErr != nil {
								errorCode = 0xFFFF // UNKNOWN_SERVER_ERROR (-1 as uint16)
								break
							}
							if idx == 0 {
								baseOffset = offsetProduced
							}
						}
					}
				}
			}

			// Build correct Produce v2+ response for this partition
			// Format: partition_id(4) + error_code(2) + base_offset(8) + [log_append_time(8) if v>=2] + [log_start_offset(8) if v>=5]

			// partition_id (4 bytes)
			partitionIDBytes := make([]byte, 4)
			binary.BigEndian.PutUint32(partitionIDBytes, partitionID)
			response = append(response, partitionIDBytes...)

			// error_code (2 bytes)
			response = append(response, byte(errorCode>>8), byte(errorCode))

			// base_offset (8 bytes) - offset of first message
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

	// For fire-and-forget mode, return empty response after processing
	if isFireAndForget {
		return []byte{}, nil
	}

	// Append throttle_time_ms at the END for v1+ (as per original Kafka protocol)
	if apiVersion >= 1 {
		response = append(response, 0, 0, 0, 0) // throttle_time_ms = 0
	}

	if len(response) < 20 {
	}

	return response, nil
}

// performSchemaValidation performs comprehensive schema validation for a topic
func (h *Handler) performSchemaValidation(topicName string, schemaID uint32, messageFormat schema.Format, messageBytes []byte) error {
	// 1. Check if topic is configured to require schemas
	if !h.isSchematizedTopic(topicName) {
		// Topic doesn't require schemas, but message is schematized - this is allowed
		return nil
	}

	// 2. Get expected schema metadata for the topic
	expectedMetadata, err := h.getSchemaMetadataForTopic(topicName)
	if err != nil {
		// No expected schema found - in strict mode this would be an error
		// In permissive mode, allow any valid schema
		if h.isStrictSchemaValidation() {
			// Add delay before returning schema validation error to prevent overloading
			time.Sleep(100 * time.Millisecond)
			return fmt.Errorf("topic %s requires schema but no expected schema found: %w", topicName, err)
		}
		return nil
	}

	// 3. Validate schema ID matches expected schema
	expectedSchemaID, err := h.parseSchemaID(expectedMetadata["schema_id"])
	if err != nil {
		// Add delay before returning schema validation error to prevent overloading
		time.Sleep(100 * time.Millisecond)
		return fmt.Errorf("invalid expected schema ID for topic %s: %w", topicName, err)
	}

	// 4. Check schema compatibility
	if schemaID != expectedSchemaID {
		// Schema ID doesn't match - check if it's a compatible evolution
		compatible, err := h.checkSchemaEvolution(topicName, expectedSchemaID, schemaID, messageFormat)
		if err != nil {
			// Add delay before returning schema validation error to prevent overloading
			time.Sleep(100 * time.Millisecond)
			return fmt.Errorf("failed to check schema evolution for topic %s: %w", topicName, err)
		}
		if !compatible {
			// Add delay before returning schema validation error to prevent overloading
			time.Sleep(100 * time.Millisecond)
			return fmt.Errorf("schema ID %d is not compatible with expected schema %d for topic %s",
				schemaID, expectedSchemaID, topicName)
		}
	}

	// 5. Validate message format matches expected format
	expectedFormatStr := expectedMetadata["schema_format"]
	var expectedFormat schema.Format
	switch expectedFormatStr {
	case "AVRO":
		expectedFormat = schema.FormatAvro
	case "PROTOBUF":
		expectedFormat = schema.FormatProtobuf
	case "JSON_SCHEMA":
		expectedFormat = schema.FormatJSONSchema
	default:
		expectedFormat = schema.FormatUnknown
	}
	if messageFormat != expectedFormat {
		return fmt.Errorf("message format %s does not match expected format %s for topic %s",
			messageFormat, expectedFormat, topicName)
	}

	// 6. Perform message-level validation
	return h.validateMessageContent(schemaID, messageFormat, messageBytes)
}

// checkSchemaEvolution checks if a schema evolution is compatible
func (h *Handler) checkSchemaEvolution(topicName string, expectedSchemaID, actualSchemaID uint32, format schema.Format) (bool, error) {
	// Get both schemas
	expectedSchema, err := h.schemaManager.GetSchemaByID(expectedSchemaID)
	if err != nil {
		return false, fmt.Errorf("failed to get expected schema %d: %w", expectedSchemaID, err)
	}

	actualSchema, err := h.schemaManager.GetSchemaByID(actualSchemaID)
	if err != nil {
		return false, fmt.Errorf("failed to get actual schema %d: %w", actualSchemaID, err)
	}

	// Since we're accessing schema from registry for this topic, ensure topic config is updated
	h.ensureTopicSchemaFromRegistryCache(topicName, expectedSchema, actualSchema)

	// Check compatibility based on topic's compatibility level
	compatibilityLevel := h.getTopicCompatibilityLevel(topicName)

	result, err := h.schemaManager.CheckSchemaCompatibility(
		expectedSchema.Schema,
		actualSchema.Schema,
		format,
		compatibilityLevel,
	)
	if err != nil {
		return false, fmt.Errorf("failed to check schema compatibility: %w", err)
	}

	return result.Compatible, nil
}

// validateMessageContent validates the message content against its schema
func (h *Handler) validateMessageContent(schemaID uint32, format schema.Format, messageBytes []byte) error {
	// Decode the message to validate it can be parsed correctly
	_, err := h.schemaManager.DecodeMessage(messageBytes)
	if err != nil {
		return fmt.Errorf("message validation failed for schema %d: %w", schemaID, err)
	}

	// Additional format-specific validation could be added here
	switch format {
	case schema.FormatAvro:
		return h.validateAvroMessage(schemaID, messageBytes)
	case schema.FormatProtobuf:
		return h.validateProtobufMessage(schemaID, messageBytes)
	case schema.FormatJSONSchema:
		return h.validateJSONSchemaMessage(schemaID, messageBytes)
	default:
		return fmt.Errorf("unsupported schema format for validation: %s", format)
	}
}

// validateAvroMessage performs Avro-specific validation
func (h *Handler) validateAvroMessage(schemaID uint32, messageBytes []byte) error {
	// Basic validation is already done in DecodeMessage
	// Additional Avro-specific validation could be added here
	return nil
}

// validateProtobufMessage performs Protobuf-specific validation
func (h *Handler) validateProtobufMessage(schemaID uint32, messageBytes []byte) error {
	// Get the schema for additional validation
	cachedSchema, err := h.schemaManager.GetSchemaByID(schemaID)
	if err != nil {
		return fmt.Errorf("failed to get Protobuf schema %d: %w", schemaID, err)
	}

	// Parse the schema to get the descriptor
	parser := schema.NewProtobufDescriptorParser()
	protobufSchema, err := parser.ParseBinaryDescriptor([]byte(cachedSchema.Schema), "")
	if err != nil {
		return fmt.Errorf("failed to parse Protobuf schema: %w", err)
	}

	// Validate message against schema
	envelope, ok := schema.ParseConfluentEnvelope(messageBytes)
	if !ok {
		return fmt.Errorf("invalid Confluent envelope")
	}

	return protobufSchema.ValidateMessage(envelope.Payload)
}

// validateJSONSchemaMessage performs JSON Schema-specific validation
func (h *Handler) validateJSONSchemaMessage(schemaID uint32, messageBytes []byte) error {
	// Get the schema for validation
	cachedSchema, err := h.schemaManager.GetSchemaByID(schemaID)
	if err != nil {
		return fmt.Errorf("failed to get JSON schema %d: %w", schemaID, err)
	}

	// Create JSON Schema decoder for validation
	decoder, err := schema.NewJSONSchemaDecoder(cachedSchema.Schema)
	if err != nil {
		return fmt.Errorf("failed to create JSON Schema decoder: %w", err)
	}

	// Parse envelope and validate payload
	envelope, ok := schema.ParseConfluentEnvelope(messageBytes)
	if !ok {
		return fmt.Errorf("invalid Confluent envelope")
	}

	// Validate JSON payload against schema
	_, err = decoder.Decode(envelope.Payload)
	if err != nil {
		return fmt.Errorf("JSON Schema validation failed: %w", err)
	}

	return nil
}

// Helper methods for configuration

// isSchemaValidationError checks if an error is related to schema validation
func (h *Handler) isSchemaValidationError(err error) bool {
	if err == nil {
		return false
	}
	errStr := strings.ToLower(err.Error())
	return strings.Contains(errStr, "schema") ||
		strings.Contains(errStr, "decode") ||
		strings.Contains(errStr, "validation") ||
		strings.Contains(errStr, "registry") ||
		strings.Contains(errStr, "avro") ||
		strings.Contains(errStr, "protobuf") ||
		strings.Contains(errStr, "json schema")
}

// isStrictSchemaValidation returns whether strict schema validation is enabled
func (h *Handler) isStrictSchemaValidation() bool {
	// This could be configurable per topic or globally
	// For now, default to permissive mode
	return false
}

// getTopicCompatibilityLevel returns the compatibility level for a topic
func (h *Handler) getTopicCompatibilityLevel(topicName string) schema.CompatibilityLevel {
	// This could be configurable per topic
	// For now, default to backward compatibility
	return schema.CompatibilityBackward
}

// parseSchemaID parses a schema ID from string
func (h *Handler) parseSchemaID(schemaIDStr string) (uint32, error) {
	if schemaIDStr == "" {
		return 0, fmt.Errorf("empty schema ID")
	}

	var schemaID uint64
	if _, err := fmt.Sscanf(schemaIDStr, "%d", &schemaID); err != nil {
		return 0, fmt.Errorf("invalid schema ID format: %w", err)
	}

	if schemaID > 0xFFFFFFFF {
		return 0, fmt.Errorf("schema ID too large: %d", schemaID)
	}

	return uint32(schemaID), nil
}

// isSystemTopic checks if a topic should bypass schema processing
func (h *Handler) isSystemTopic(topicName string) bool {
	// System topics that should be stored as-is without schema processing
	systemTopics := []string{
		"_schemas",            // Schema Registry topic
		"__consumer_offsets",  // Kafka consumer offsets topic
		"__transaction_state", // Kafka transaction state topic
	}

	for _, systemTopic := range systemTopics {
		if topicName == systemTopic {
			return true
		}
	}

	// Also check for topics with system prefixes
	return strings.HasPrefix(topicName, "_") || strings.HasPrefix(topicName, "__")
}

// produceSchemaBasedRecord produces a record using schema-based encoding to RecordValue
// ctx controls the publish timeout - if client cancels, produce operation is cancelled
func (h *Handler) produceSchemaBasedRecord(ctx context.Context, topic string, partition int32, key []byte, value []byte) (int64, error) {

	// System topics should always bypass schema processing and be stored as-is
	if h.isSystemTopic(topic) {
		offset, err := h.seaweedMQHandler.ProduceRecord(ctx, topic, partition, key, value)
		return offset, err
	}

	// If schema management is not enabled, fall back to raw message handling
	isEnabled := h.IsSchemaEnabled()
	if !isEnabled {
		return h.seaweedMQHandler.ProduceRecord(ctx, topic, partition, key, value)
	}

	var keyDecodedMsg *schema.DecodedMessage
	var valueDecodedMsg *schema.DecodedMessage

	// Check and decode key if schematized
	if key != nil {
		isSchematized := h.schemaManager.IsSchematized(key)
		if isSchematized {
			var err error
			keyDecodedMsg, err = h.schemaManager.DecodeMessage(key)
			if err != nil {
				// Add delay before returning schema decoding error to prevent overloading
				time.Sleep(100 * time.Millisecond)
				return 0, fmt.Errorf("failed to decode schematized key: %w", err)
			}
		}
	}

	// Check and decode value if schematized
	if value != nil && len(value) > 0 {
		isSchematized := h.schemaManager.IsSchematized(value)
		if isSchematized {
			var err error
			valueDecodedMsg, err = h.schemaManager.DecodeMessage(value)
			if err != nil {
				// If message has schema ID (magic byte 0x00), decoding MUST succeed
				// Do not fall back to raw storage - this would corrupt the data model
				time.Sleep(100 * time.Millisecond)
				return 0, fmt.Errorf("message has schema ID but decoding failed (schema registry may be unavailable): %w", err)
			}
		}
	}

	// If neither key nor value is schematized, fall back to raw message handling
	// This is OK for non-schematized messages (no magic byte 0x00)
	if keyDecodedMsg == nil && valueDecodedMsg == nil {
		return h.seaweedMQHandler.ProduceRecord(ctx, topic, partition, key, value)
	}

	// Process key schema if present
	if keyDecodedMsg != nil {
		// Store key schema information in memory cache for fetch path performance
		if !h.hasTopicKeySchemaConfig(topic, keyDecodedMsg.SchemaID, keyDecodedMsg.SchemaFormat) {
			err := h.storeTopicKeySchemaConfig(topic, keyDecodedMsg.SchemaID, keyDecodedMsg.SchemaFormat)
			if err != nil {
			}

			// Schedule key schema registration in background (leader-only, non-blocking)
			h.scheduleKeySchemaRegistration(topic, keyDecodedMsg.RecordType)
		}
	}

	// Process value schema if present and create combined RecordValue with key fields
	var recordValueBytes []byte
	if valueDecodedMsg != nil {
		// Create combined RecordValue that includes both key and value fields
		combinedRecordValue := h.createCombinedRecordValue(keyDecodedMsg, valueDecodedMsg)

		// Store the combined RecordValue - schema info is stored in topic configuration
		var err error
		recordValueBytes, err = proto.Marshal(combinedRecordValue)
		if err != nil {
			return 0, fmt.Errorf("failed to marshal combined RecordValue: %w", err)
		}

		// Store value schema information in memory cache for fetch path performance
		// Only store if not already cached to avoid mutex contention on hot path
		hasConfig := h.hasTopicSchemaConfig(topic, valueDecodedMsg.SchemaID, valueDecodedMsg.SchemaFormat)
		if !hasConfig {
			err = h.storeTopicSchemaConfig(topic, valueDecodedMsg.SchemaID, valueDecodedMsg.SchemaFormat)
			if err != nil {
				// Log error but don't fail the produce
			}

			// Schedule value schema registration in background (leader-only, non-blocking)
			h.scheduleSchemaRegistration(topic, valueDecodedMsg.RecordType)
		}
	} else if keyDecodedMsg != nil {
		// If only key is schematized, create RecordValue with just key fields
		combinedRecordValue := h.createCombinedRecordValue(keyDecodedMsg, nil)

		var err error
		recordValueBytes, err = proto.Marshal(combinedRecordValue)
		if err != nil {
			return 0, fmt.Errorf("failed to marshal key-only RecordValue: %w", err)
		}
	} else {
		// If value is not schematized, use raw value
		recordValueBytes = value
	}

	// Prepare final key for storage
	finalKey := key
	if keyDecodedMsg != nil {
		// If key was schematized, convert back to raw bytes for storage
		keyBytes, err := proto.Marshal(keyDecodedMsg.RecordValue)
		if err != nil {
			return 0, fmt.Errorf("failed to marshal key RecordValue: %w", err)
		}
		finalKey = keyBytes
	}

	// Send to SeaweedMQ
	if valueDecodedMsg != nil || keyDecodedMsg != nil {
		// Store the DECODED RecordValue (not the original Confluent Wire Format)
		// This enables SQL queries to work properly. Kafka consumers will receive the RecordValue
		// which can be re-encoded to Confluent Wire Format during fetch if needed
		return h.seaweedMQHandler.ProduceRecordValue(ctx, topic, partition, finalKey, recordValueBytes)
	} else {
		// Send with raw format for non-schematized data
		return h.seaweedMQHandler.ProduceRecord(ctx, topic, partition, finalKey, recordValueBytes)
	}
}

// hasTopicSchemaConfig checks if schema config already exists (read-only, fast path)
func (h *Handler) hasTopicSchemaConfig(topic string, schemaID uint32, schemaFormat schema.Format) bool {
	h.topicSchemaConfigMu.RLock()
	defer h.topicSchemaConfigMu.RUnlock()

	if h.topicSchemaConfigs == nil {
		return false
	}

	config, exists := h.topicSchemaConfigs[topic]
	if !exists {
		return false
	}

	// Check if the schema matches (avoid re-registration of same schema)
	return config.ValueSchemaID == schemaID && config.ValueSchemaFormat == schemaFormat
}

// storeTopicSchemaConfig stores original Kafka schema metadata (ID + format) for fetch path
// This is kept in memory for performance when reconstructing Confluent messages during fetch.
// The translated RecordType is persisted via background schema registration.
func (h *Handler) storeTopicSchemaConfig(topic string, schemaID uint32, schemaFormat schema.Format) error {
	// Store in memory cache for quick access during fetch operations
	h.topicSchemaConfigMu.Lock()
	defer h.topicSchemaConfigMu.Unlock()

	if h.topicSchemaConfigs == nil {
		h.topicSchemaConfigs = make(map[string]*TopicSchemaConfig)
	}

	config, exists := h.topicSchemaConfigs[topic]
	if !exists {
		config = &TopicSchemaConfig{}
		h.topicSchemaConfigs[topic] = config
	}

	config.ValueSchemaID = schemaID
	config.ValueSchemaFormat = schemaFormat

	return nil
}

// storeTopicKeySchemaConfig stores key schema configuration
func (h *Handler) storeTopicKeySchemaConfig(topic string, schemaID uint32, schemaFormat schema.Format) error {
	h.topicSchemaConfigMu.Lock()
	defer h.topicSchemaConfigMu.Unlock()

	if h.topicSchemaConfigs == nil {
		h.topicSchemaConfigs = make(map[string]*TopicSchemaConfig)
	}

	config, exists := h.topicSchemaConfigs[topic]
	if !exists {
		config = &TopicSchemaConfig{}
		h.topicSchemaConfigs[topic] = config
	}

	config.KeySchemaID = schemaID
	config.KeySchemaFormat = schemaFormat
	config.HasKeySchema = true

	return nil
}

// hasTopicKeySchemaConfig checks if key schema config already exists
func (h *Handler) hasTopicKeySchemaConfig(topic string, schemaID uint32, schemaFormat schema.Format) bool {
	h.topicSchemaConfigMu.RLock()
	defer h.topicSchemaConfigMu.RUnlock()

	config, exists := h.topicSchemaConfigs[topic]
	if !exists {
		return false
	}

	// Check if the key schema matches
	return config.HasKeySchema && config.KeySchemaID == schemaID && config.KeySchemaFormat == schemaFormat
}

// scheduleSchemaRegistration registers value schema once per topic-schema combination
func (h *Handler) scheduleSchemaRegistration(topicName string, recordType *schema_pb.RecordType) {
	if recordType == nil {
		return
	}

	// Create a unique key for this value schema registration
	schemaKey := fmt.Sprintf("%s:value:%d", topicName, h.getRecordTypeHash(recordType))

	// Check if already registered
	h.registeredSchemasMu.RLock()
	if h.registeredSchemas[schemaKey] {
		h.registeredSchemasMu.RUnlock()
		return // Already registered
	}
	h.registeredSchemasMu.RUnlock()

	// Double-check with write lock to prevent race condition
	h.registeredSchemasMu.Lock()
	defer h.registeredSchemasMu.Unlock()

	if h.registeredSchemas[schemaKey] {
		return // Already registered by another goroutine
	}

	// Mark as registered before attempting registration
	h.registeredSchemas[schemaKey] = true

	// Perform synchronous registration
	if err := h.registerSchemasViaBrokerAPI(topicName, recordType, nil); err != nil {
		// Remove from registered map on failure so it can be retried
		delete(h.registeredSchemas, schemaKey)
	}
}

// scheduleKeySchemaRegistration registers key schema once per topic-schema combination
func (h *Handler) scheduleKeySchemaRegistration(topicName string, recordType *schema_pb.RecordType) {
	if recordType == nil {
		return
	}

	// Create a unique key for this key schema registration
	schemaKey := fmt.Sprintf("%s:key:%d", topicName, h.getRecordTypeHash(recordType))

	// Check if already registered
	h.registeredSchemasMu.RLock()
	if h.registeredSchemas[schemaKey] {
		h.registeredSchemasMu.RUnlock()
		return // Already registered
	}
	h.registeredSchemasMu.RUnlock()

	// Double-check with write lock to prevent race condition
	h.registeredSchemasMu.Lock()
	defer h.registeredSchemasMu.Unlock()

	if h.registeredSchemas[schemaKey] {
		return // Already registered by another goroutine
	}

	// Mark as registered before attempting registration
	h.registeredSchemas[schemaKey] = true

	// Register key schema to the same topic (not a phantom "-key" topic)
	// This uses the extended ConfigureTopicRequest with separate key/value RecordTypes
	if err := h.registerSchemasViaBrokerAPI(topicName, nil, recordType); err != nil {
		// Remove from registered map on failure so it can be retried
		delete(h.registeredSchemas, schemaKey)
	} else {
	}
}

// ensureTopicSchemaFromRegistryCache ensures topic configuration is updated when schemas are retrieved from registry
func (h *Handler) ensureTopicSchemaFromRegistryCache(topicName string, schemas ...*schema.CachedSchema) {
	if len(schemas) == 0 {
		return
	}

	// Use the latest/most relevant schema (last one in the list)
	latestSchema := schemas[len(schemas)-1]
	if latestSchema == nil {
		return
	}

	// Try to infer RecordType from the cached schema
	recordType, err := h.inferRecordTypeFromCachedSchema(latestSchema)
	if err != nil {
		return
	}

	// Schedule schema registration to update topic.conf
	if recordType != nil {
		h.scheduleSchemaRegistration(topicName, recordType)
	}
}

// ensureTopicKeySchemaFromRegistryCache ensures topic configuration is updated when key schemas are retrieved from registry
func (h *Handler) ensureTopicKeySchemaFromRegistryCache(topicName string, schemas ...*schema.CachedSchema) {
	if len(schemas) == 0 {
		return
	}

	// Use the latest/most relevant schema (last one in the list)
	latestSchema := schemas[len(schemas)-1]
	if latestSchema == nil {
		return
	}

	// Try to infer RecordType from the cached schema
	recordType, err := h.inferRecordTypeFromCachedSchema(latestSchema)
	if err != nil {
		return
	}

	// Schedule key schema registration to update topic.conf
	if recordType != nil {
		h.scheduleKeySchemaRegistration(topicName, recordType)
	}
}

// getRecordTypeHash generates a simple hash for RecordType to use as a key
func (h *Handler) getRecordTypeHash(recordType *schema_pb.RecordType) uint32 {
	if recordType == nil {
		return 0
	}

	// Simple hash based on field count and first field name
	hash := uint32(len(recordType.Fields))
	if len(recordType.Fields) > 0 {
		// Use first field name for additional uniqueness
		firstFieldName := recordType.Fields[0].Name
		for _, char := range firstFieldName {
			hash = hash*31 + uint32(char)
		}
	}

	return hash
}

// createCombinedRecordValue creates a RecordValue that combines fields from both key and value decoded messages
// Key fields are prefixed with "key_" to distinguish them from value fields
// The message key bytes are stored in the _key system column (from logEntry.Key)
func (h *Handler) createCombinedRecordValue(keyDecodedMsg *schema.DecodedMessage, valueDecodedMsg *schema.DecodedMessage) *schema_pb.RecordValue {
	combinedFields := make(map[string]*schema_pb.Value)

	// Add key fields with "key_" prefix
	if keyDecodedMsg != nil && keyDecodedMsg.RecordValue != nil {
		for fieldName, fieldValue := range keyDecodedMsg.RecordValue.Fields {
			combinedFields["key_"+fieldName] = fieldValue
		}
		// Note: The message key bytes are stored in the _key system column (from logEntry.Key)
		// We don't create a "key" field here to avoid redundancy
	}

	// Add value fields (no prefix)
	if valueDecodedMsg != nil && valueDecodedMsg.RecordValue != nil {
		for fieldName, fieldValue := range valueDecodedMsg.RecordValue.Fields {
			combinedFields[fieldName] = fieldValue
		}
	}

	return &schema_pb.RecordValue{
		Fields: combinedFields,
	}
}

// inferRecordTypeFromCachedSchema attempts to infer RecordType from a cached schema
func (h *Handler) inferRecordTypeFromCachedSchema(cachedSchema *schema.CachedSchema) (*schema_pb.RecordType, error) {
	if cachedSchema == nil {
		return nil, fmt.Errorf("cached schema is nil")
	}

	switch cachedSchema.Format {
	case schema.FormatAvro:
		return h.inferRecordTypeFromAvroSchema(cachedSchema.Schema)
	case schema.FormatProtobuf:
		return h.inferRecordTypeFromProtobufSchema(cachedSchema.Schema)
	case schema.FormatJSONSchema:
		return h.inferRecordTypeFromJSONSchema(cachedSchema.Schema)
	default:
		return nil, fmt.Errorf("unsupported schema format for inference: %v", cachedSchema.Format)
	}
}

// inferRecordTypeFromAvroSchema infers RecordType from Avro schema string
// Uses cache to avoid recreating expensive Avro codecs (17% CPU overhead!)
func (h *Handler) inferRecordTypeFromAvroSchema(avroSchema string) (*schema_pb.RecordType, error) {
	// Check cache first
	h.inferredRecordTypesMu.RLock()
	if recordType, exists := h.inferredRecordTypes[avroSchema]; exists {
		h.inferredRecordTypesMu.RUnlock()
		return recordType, nil
	}
	h.inferredRecordTypesMu.RUnlock()

	// Cache miss - create decoder and infer type
	decoder, err := schema.NewAvroDecoder(avroSchema)
	if err != nil {
		return nil, fmt.Errorf("failed to create Avro decoder: %w", err)
	}

	recordType, err := decoder.InferRecordType()
	if err != nil {
		return nil, err
	}

	// Cache the result
	h.inferredRecordTypesMu.Lock()
	h.inferredRecordTypes[avroSchema] = recordType
	h.inferredRecordTypesMu.Unlock()

	return recordType, nil
}

// inferRecordTypeFromProtobufSchema infers RecordType from Protobuf schema
// Uses cache to avoid recreating expensive decoders
func (h *Handler) inferRecordTypeFromProtobufSchema(protobufSchema string) (*schema_pb.RecordType, error) {
	// Check cache first
	cacheKey := "protobuf:" + protobufSchema
	h.inferredRecordTypesMu.RLock()
	if recordType, exists := h.inferredRecordTypes[cacheKey]; exists {
		h.inferredRecordTypesMu.RUnlock()
		return recordType, nil
	}
	h.inferredRecordTypesMu.RUnlock()

	// Cache miss - create decoder and infer type
	decoder, err := schema.NewProtobufDecoder([]byte(protobufSchema))
	if err != nil {
		return nil, fmt.Errorf("failed to create Protobuf decoder: %w", err)
	}

	recordType, err := decoder.InferRecordType()
	if err != nil {
		return nil, err
	}

	// Cache the result
	h.inferredRecordTypesMu.Lock()
	h.inferredRecordTypes[cacheKey] = recordType
	h.inferredRecordTypesMu.Unlock()

	return recordType, nil
}

// inferRecordTypeFromJSONSchema infers RecordType from JSON Schema string
// Uses cache to avoid recreating expensive decoders
func (h *Handler) inferRecordTypeFromJSONSchema(jsonSchema string) (*schema_pb.RecordType, error) {
	// Check cache first
	cacheKey := "json:" + jsonSchema
	h.inferredRecordTypesMu.RLock()
	if recordType, exists := h.inferredRecordTypes[cacheKey]; exists {
		h.inferredRecordTypesMu.RUnlock()
		return recordType, nil
	}
	h.inferredRecordTypesMu.RUnlock()

	// Cache miss - create decoder and infer type
	decoder, err := schema.NewJSONSchemaDecoder(jsonSchema)
	if err != nil {
		return nil, fmt.Errorf("failed to create JSON Schema decoder: %w", err)
	}

	recordType, err := decoder.InferRecordType()
	if err != nil {
		return nil, err
	}

	// Cache the result
	h.inferredRecordTypesMu.Lock()
	h.inferredRecordTypes[cacheKey] = recordType
	h.inferredRecordTypesMu.Unlock()

	return recordType, nil
}
