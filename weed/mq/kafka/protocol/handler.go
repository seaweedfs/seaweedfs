package protocol

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"sync"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/mq/kafka/offset"
)

// TopicInfo holds basic information about a topic
type TopicInfo struct {
	Name       string
	Partitions int32
	CreatedAt  int64
}

// TopicPartitionKey uniquely identifies a topic partition
type TopicPartitionKey struct {
	Topic     string
	Partition int32
}

// Handler processes Kafka protocol requests from clients
type Handler struct {
	topicsMu sync.RWMutex
	topics   map[string]*TopicInfo // topic name -> topic info

	ledgersMu sync.RWMutex
	ledgers   map[TopicPartitionKey]*offset.Ledger // topic-partition -> offset ledger
}

func NewHandler() *Handler {
	return &Handler{
		topics:  make(map[string]*TopicInfo),
		ledgers: make(map[TopicPartitionKey]*offset.Ledger),
	}
}

// GetOrCreateLedger returns the offset ledger for a topic-partition, creating it if needed
func (h *Handler) GetOrCreateLedger(topic string, partition int32) *offset.Ledger {
	key := TopicPartitionKey{Topic: topic, Partition: partition}

	// First try to get existing ledger with read lock
	h.ledgersMu.RLock()
	ledger, exists := h.ledgers[key]
	h.ledgersMu.RUnlock()

	if exists {
		return ledger
	}

	// Create new ledger with write lock
	h.ledgersMu.Lock()
	defer h.ledgersMu.Unlock()

	// Double-check after acquiring write lock
	if ledger, exists := h.ledgers[key]; exists {
		return ledger
	}

	// Create and store new ledger
	ledger = offset.NewLedger()
	h.ledgers[key] = ledger
	return ledger
}

// GetLedger returns the offset ledger for a topic-partition, or nil if not found
func (h *Handler) GetLedger(topic string, partition int32) *offset.Ledger {
	key := TopicPartitionKey{Topic: topic, Partition: partition}

	h.ledgersMu.RLock()
	defer h.ledgersMu.RUnlock()

	return h.ledgers[key]
}

// HandleConn processes a single client connection
func (h *Handler) HandleConn(conn net.Conn) error {
	defer conn.Close()

	r := bufio.NewReader(conn)
	w := bufio.NewWriter(conn)
	defer w.Flush()

	for {
		// Read message size (4 bytes)
		var sizeBytes [4]byte
		if _, err := io.ReadFull(r, sizeBytes[:]); err != nil {
			if err == io.EOF {
				return nil // clean disconnect
			}
			return fmt.Errorf("read size: %w", err)
		}

		size := binary.BigEndian.Uint32(sizeBytes[:])
		if size == 0 || size > 1024*1024 { // 1MB limit
			return fmt.Errorf("invalid message size: %d", size)
		}

		// Read the message
		messageBuf := make([]byte, size)
		if _, err := io.ReadFull(r, messageBuf); err != nil {
			return fmt.Errorf("read message: %w", err)
		}

		// Parse at least the basic header to get API key and correlation ID
		if len(messageBuf) < 8 {
			return fmt.Errorf("message too short")
		}

		apiKey := binary.BigEndian.Uint16(messageBuf[0:2])
		apiVersion := binary.BigEndian.Uint16(messageBuf[2:4])
		correlationID := binary.BigEndian.Uint32(messageBuf[4:8])

		// Handle the request based on API key
		var response []byte
		var err error

		switch apiKey {
		case 18: // ApiVersions
			response, err = h.handleApiVersions(correlationID)
		case 3: // Metadata
			response, err = h.handleMetadata(correlationID, messageBuf[8:]) // skip header
		case 2: // ListOffsets
			response, err = h.handleListOffsets(correlationID, messageBuf[8:]) // skip header
		case 19: // CreateTopics
			response, err = h.handleCreateTopics(correlationID, messageBuf[8:]) // skip header
		case 20: // DeleteTopics
			response, err = h.handleDeleteTopics(correlationID, messageBuf[8:]) // skip header
		case 0: // Produce
			response, err = h.handleProduce(correlationID, messageBuf[8:]) // skip header
		case 1: // Fetch
			response, err = h.handleFetch(correlationID, messageBuf[8:]) // skip header
		default:
			err = fmt.Errorf("unsupported API key: %d (version %d)", apiKey, apiVersion)
		}

		if err != nil {
			return fmt.Errorf("handle request: %w", err)
		}

		// Write response size and data
		responseSizeBytes := make([]byte, 4)
		binary.BigEndian.PutUint32(responseSizeBytes, uint32(len(response)))

		if _, err := w.Write(responseSizeBytes); err != nil {
			return fmt.Errorf("write response size: %w", err)
		}
		if _, err := w.Write(response); err != nil {
			return fmt.Errorf("write response: %w", err)
		}

		if err := w.Flush(); err != nil {
			return fmt.Errorf("flush response: %w", err)
		}
	}
}

func (h *Handler) handleApiVersions(correlationID uint32) ([]byte, error) {
	// Build ApiVersions response manually
	// Response format: correlation_id(4) + error_code(2) + num_api_keys(4) + api_keys + throttle_time(4)

	response := make([]byte, 0, 64)

	// Correlation ID
	correlationIDBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(correlationIDBytes, correlationID)
	response = append(response, correlationIDBytes...)

	// Error code (0 = no error)
	response = append(response, 0, 0)

	// Number of API keys (compact array format in newer versions, but using basic format for simplicity)
	response = append(response, 0, 0, 0, 7) // 7 API keys

	// API Key 18 (ApiVersions): api_key(2) + min_version(2) + max_version(2)
	response = append(response, 0, 18) // API key 18
	response = append(response, 0, 0)  // min version 0
	response = append(response, 0, 3)  // max version 3

	// API Key 3 (Metadata): api_key(2) + min_version(2) + max_version(2)
	response = append(response, 0, 3) // API key 3
	response = append(response, 0, 0) // min version 0
	response = append(response, 0, 7) // max version 7

	// API Key 2 (ListOffsets): api_key(2) + min_version(2) + max_version(2)
	response = append(response, 0, 2) // API key 2
	response = append(response, 0, 0) // min version 0
	response = append(response, 0, 5) // max version 5

	// API Key 19 (CreateTopics): api_key(2) + min_version(2) + max_version(2)
	response = append(response, 0, 19) // API key 19
	response = append(response, 0, 0)  // min version 0
	response = append(response, 0, 4)  // max version 4

	// API Key 20 (DeleteTopics): api_key(2) + min_version(2) + max_version(2)
	response = append(response, 0, 20) // API key 20
	response = append(response, 0, 0)  // min version 0
	response = append(response, 0, 4)  // max version 4

	// API Key 0 (Produce): api_key(2) + min_version(2) + max_version(2)
	response = append(response, 0, 0) // API key 0
	response = append(response, 0, 0) // min version 0
	response = append(response, 0, 7) // max version 7

	// API Key 1 (Fetch): api_key(2) + min_version(2) + max_version(2)
	response = append(response, 0, 1) // API key 1
	response = append(response, 0, 0) // min version 0
	response = append(response, 0, 11) // max version 11

	// Throttle time (4 bytes, 0 = no throttling)
	response = append(response, 0, 0, 0, 0)

	return response, nil
}

func (h *Handler) handleMetadata(correlationID uint32, requestBody []byte) ([]byte, error) {
	// For now, ignore the request body content (topics filter, etc.)
	// Build minimal Metadata response
	// Response format: correlation_id(4) + throttle_time(4) + brokers + cluster_id + controller_id + topics

	response := make([]byte, 0, 256)

	// Correlation ID
	correlationIDBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(correlationIDBytes, correlationID)
	response = append(response, correlationIDBytes...)

	// Throttle time (4 bytes, 0 = no throttling)
	response = append(response, 0, 0, 0, 0)

	// Brokers array length (4 bytes) - 1 broker (this gateway)
	response = append(response, 0, 0, 0, 1)

	// Broker 0: node_id(4) + host + port(4) + rack
	response = append(response, 0, 0, 0, 0) // node_id = 0

	// Host string: length(2) + "localhost"
	host := "localhost"
	response = append(response, 0, byte(len(host)))
	response = append(response, []byte(host)...)

	// Port (4 bytes) - 9092 (standard Kafka port)
	response = append(response, 0, 0, 0x23, 0x84) // 9092 in big-endian

	// Rack - nullable string, using null (-1 length)
	response = append(response, 0xFF, 0xFF) // null rack

	// Cluster ID - nullable string, using null
	response = append(response, 0xFF, 0xFF) // null cluster_id

	// Controller ID (4 bytes) - -1 (no controller)
	response = append(response, 0xFF, 0xFF, 0xFF, 0xFF)

	// Topics array length (4 bytes) - 0 topics for now
	response = append(response, 0, 0, 0, 0)

	return response, nil
}

func (h *Handler) handleListOffsets(correlationID uint32, requestBody []byte) ([]byte, error) {
	// Parse minimal request to understand what's being asked
	// For this stub, we'll just return stub responses for any requested topic/partition
	// Request format after client_id: topics_array

	if len(requestBody) < 6 { // at minimum need client_id_size(2) + topics_count(4)
		return nil, fmt.Errorf("ListOffsets request too short")
	}

	// Skip client_id: client_id_size(2) + client_id_data
	clientIDSize := binary.BigEndian.Uint16(requestBody[0:2])
	offset := 2 + int(clientIDSize)

	if len(requestBody) < offset+4 {
		return nil, fmt.Errorf("ListOffsets request missing topics count")
	}

	topicsCount := binary.BigEndian.Uint32(requestBody[offset : offset+4])
	offset += 4

	response := make([]byte, 0, 256)

	// Correlation ID
	correlationIDBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(correlationIDBytes, correlationID)
	response = append(response, correlationIDBytes...)

	// Throttle time (4 bytes, 0 = no throttling)
	response = append(response, 0, 0, 0, 0)

	// Topics count (same as request)
	topicsCountBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(topicsCountBytes, topicsCount)
	response = append(response, topicsCountBytes...)

	// Process each requested topic
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

		topicName := requestBody[offset : offset+int(topicNameSize)]
		offset += int(topicNameSize)

		// Parse partitions count for this topic
		partitionsCount := binary.BigEndian.Uint32(requestBody[offset : offset+4])
		offset += 4

		// Response: topic_name_size(2) + topic_name + partitions_array
		response = append(response, byte(topicNameSize>>8), byte(topicNameSize))
		response = append(response, topicName...)

		partitionsCountBytes := make([]byte, 4)
		binary.BigEndian.PutUint32(partitionsCountBytes, partitionsCount)
		response = append(response, partitionsCountBytes...)

		// Process each partition
		for j := uint32(0); j < partitionsCount && offset+12 <= len(requestBody); j++ {
			// Parse partition request: partition_id(4) + timestamp(8)
			partitionID := binary.BigEndian.Uint32(requestBody[offset : offset+4])
			timestamp := int64(binary.BigEndian.Uint64(requestBody[offset+4 : offset+12]))
			offset += 12

			// Response: partition_id(4) + error_code(2) + timestamp(8) + offset(8)
			partitionIDBytes := make([]byte, 4)
			binary.BigEndian.PutUint32(partitionIDBytes, partitionID)
			response = append(response, partitionIDBytes...)

			// Error code (0 = no error)
			response = append(response, 0, 0)

			// Get the ledger for this topic-partition
			ledger := h.GetOrCreateLedger(string(topicName), int32(partitionID))

			var responseTimestamp int64
			var responseOffset int64

			switch timestamp {
			case -2: // earliest offset
				responseOffset = ledger.GetEarliestOffset()
				if responseOffset == ledger.GetHighWaterMark() {
					// No messages yet, return current time
					responseTimestamp = time.Now().UnixNano()
				} else {
					// Get timestamp of earliest message
					if ts, _, err := ledger.GetRecord(responseOffset); err == nil {
						responseTimestamp = ts
					} else {
						responseTimestamp = time.Now().UnixNano()
					}
				}
			case -1: // latest offset
				responseOffset = ledger.GetLatestOffset()
				if responseOffset == 0 && ledger.GetHighWaterMark() == 0 {
					// No messages yet
					responseTimestamp = time.Now().UnixNano()
					responseOffset = 0
				} else {
					// Get timestamp of latest message
					if ts, _, err := ledger.GetRecord(responseOffset); err == nil {
						responseTimestamp = ts
					} else {
						responseTimestamp = time.Now().UnixNano()
					}
				}
			default: // specific timestamp - find offset by timestamp
				responseOffset = ledger.FindOffsetByTimestamp(timestamp)
				responseTimestamp = timestamp
			}

			timestampBytes := make([]byte, 8)
			binary.BigEndian.PutUint64(timestampBytes, uint64(responseTimestamp))
			response = append(response, timestampBytes...)

			offsetBytes := make([]byte, 8)
			binary.BigEndian.PutUint64(offsetBytes, uint64(responseOffset))
			response = append(response, offsetBytes...)
		}
	}

	return response, nil
}

func (h *Handler) handleCreateTopics(correlationID uint32, requestBody []byte) ([]byte, error) {
	// Parse minimal CreateTopics request
	// Request format: client_id + timeout(4) + topics_array

	if len(requestBody) < 6 { // client_id_size(2) + timeout(4)
		return nil, fmt.Errorf("CreateTopics request too short")
	}

	// Skip client_id
	clientIDSize := binary.BigEndian.Uint16(requestBody[0:2])
	offset := 2 + int(clientIDSize)

	if len(requestBody) < offset+8 { // timeout(4) + topics_count(4)
		return nil, fmt.Errorf("CreateTopics request missing data")
	}

	// Skip timeout
	offset += 4

	topicsCount := binary.BigEndian.Uint32(requestBody[offset : offset+4])
	offset += 4

	response := make([]byte, 0, 256)

	// Correlation ID
	correlationIDBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(correlationIDBytes, correlationID)
	response = append(response, correlationIDBytes...)

	// Throttle time (4 bytes, 0 = no throttling)
	response = append(response, 0, 0, 0, 0)

	// Topics count (same as request)
	topicsCountBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(topicsCountBytes, topicsCount)
	response = append(response, topicsCountBytes...)

	// Process each topic
	h.topicsMu.Lock()
	defer h.topicsMu.Unlock()

	for i := uint32(0); i < topicsCount && offset < len(requestBody); i++ {
		if len(requestBody) < offset+2 {
			break
		}

		// Parse topic name
		topicNameSize := binary.BigEndian.Uint16(requestBody[offset : offset+2])
		offset += 2

		if len(requestBody) < offset+int(topicNameSize)+12 { // name + num_partitions(4) + replication_factor(2) + configs_count(4) + timeout(4) - simplified
			break
		}

		topicName := string(requestBody[offset : offset+int(topicNameSize)])
		offset += int(topicNameSize)

		// Parse num_partitions and replication_factor (skip others for simplicity)
		numPartitions := binary.BigEndian.Uint32(requestBody[offset : offset+4])
		offset += 4
		replicationFactor := binary.BigEndian.Uint16(requestBody[offset : offset+2])
		offset += 2

		// Skip configs and remaining fields for simplicity
		// In a real implementation, we'd parse these properly
		if len(requestBody) >= offset+4 {
			configsCount := binary.BigEndian.Uint32(requestBody[offset : offset+4])
			offset += 4
			// Skip configs (simplified)
			for j := uint32(0); j < configsCount && offset+6 <= len(requestBody); j++ {
				if len(requestBody) >= offset+2 {
					configNameSize := binary.BigEndian.Uint16(requestBody[offset : offset+2])
					offset += 2 + int(configNameSize)
					if len(requestBody) >= offset+2 {
						configValueSize := binary.BigEndian.Uint16(requestBody[offset : offset+2])
						offset += 2 + int(configValueSize)
					}
				}
			}
		}

		// Skip timeout field if present
		if len(requestBody) >= offset+4 {
			offset += 4
		}

		// Response: topic_name + error_code(2) + error_message
		response = append(response, byte(topicNameSize>>8), byte(topicNameSize))
		response = append(response, []byte(topicName)...)

		// Check if topic already exists
		var errorCode uint16 = 0
		var errorMessage string = ""

		if _, exists := h.topics[topicName]; exists {
			errorCode = 36 // TOPIC_ALREADY_EXISTS
			errorMessage = "Topic already exists"
		} else if numPartitions <= 0 {
			errorCode = 37 // INVALID_PARTITIONS
			errorMessage = "Invalid number of partitions"
		} else if replicationFactor <= 0 {
			errorCode = 38 // INVALID_REPLICATION_FACTOR
			errorMessage = "Invalid replication factor"
		} else {
			// Create the topic
			h.topics[topicName] = &TopicInfo{
				Name:       topicName,
				Partitions: int32(numPartitions),
				CreatedAt:  time.Now().UnixNano(),
			}

			// Initialize ledgers for all partitions
			for partitionID := int32(0); partitionID < int32(numPartitions); partitionID++ {
				h.GetOrCreateLedger(topicName, partitionID)
			}
		}

		// Error code
		response = append(response, byte(errorCode>>8), byte(errorCode))

		// Error message (nullable string)
		if errorMessage == "" {
			response = append(response, 0xFF, 0xFF) // null string
		} else {
			errorMsgLen := uint16(len(errorMessage))
			response = append(response, byte(errorMsgLen>>8), byte(errorMsgLen))
			response = append(response, []byte(errorMessage)...)
		}
	}

	return response, nil
}

func (h *Handler) handleDeleteTopics(correlationID uint32, requestBody []byte) ([]byte, error) {
	// Parse minimal DeleteTopics request
	// Request format: client_id + timeout(4) + topics_array

	if len(requestBody) < 6 { // client_id_size(2) + timeout(4)
		return nil, fmt.Errorf("DeleteTopics request too short")
	}

	// Skip client_id
	clientIDSize := binary.BigEndian.Uint16(requestBody[0:2])
	offset := 2 + int(clientIDSize)

	if len(requestBody) < offset+8 { // timeout(4) + topics_count(4)
		return nil, fmt.Errorf("DeleteTopics request missing data")
	}

	// Skip timeout
	offset += 4

	topicsCount := binary.BigEndian.Uint32(requestBody[offset : offset+4])
	offset += 4

	response := make([]byte, 0, 256)

	// Correlation ID
	correlationIDBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(correlationIDBytes, correlationID)
	response = append(response, correlationIDBytes...)

	// Throttle time (4 bytes, 0 = no throttling)
	response = append(response, 0, 0, 0, 0)

	// Topics count (same as request)
	topicsCountBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(topicsCountBytes, topicsCount)
	response = append(response, topicsCountBytes...)

	// Process each topic
	h.topicsMu.Lock()
	defer h.topicsMu.Unlock()

	for i := uint32(0); i < topicsCount && offset < len(requestBody); i++ {
		if len(requestBody) < offset+2 {
			break
		}

		// Parse topic name
		topicNameSize := binary.BigEndian.Uint16(requestBody[offset : offset+2])
		offset += 2

		if len(requestBody) < offset+int(topicNameSize) {
			break
		}

		topicName := string(requestBody[offset : offset+int(topicNameSize)])
		offset += int(topicNameSize)

		// Response: topic_name + error_code(2) + error_message
		response = append(response, byte(topicNameSize>>8), byte(topicNameSize))
		response = append(response, []byte(topicName)...)

		// Check if topic exists and delete it
		var errorCode uint16 = 0
		var errorMessage string = ""

		topicInfo, exists := h.topics[topicName]
		if !exists {
			errorCode = 3 // UNKNOWN_TOPIC_OR_PARTITION
			errorMessage = "Unknown topic"
		} else {
			// Delete the topic
			delete(h.topics, topicName)

			// Clean up associated ledgers
			h.ledgersMu.Lock()
			for partitionID := int32(0); partitionID < topicInfo.Partitions; partitionID++ {
				key := TopicPartitionKey{Topic: topicName, Partition: partitionID}
				delete(h.ledgers, key)
			}
			h.ledgersMu.Unlock()
		}

		// Error code
		response = append(response, byte(errorCode>>8), byte(errorCode))

		// Error message (nullable string)
		if errorMessage == "" {
			response = append(response, 0xFF, 0xFF) // null string
		} else {
			errorMsgLen := uint16(len(errorMessage))
			response = append(response, byte(errorMsgLen>>8), byte(errorMsgLen))
			response = append(response, []byte(errorMessage)...)
		}
	}

	return response, nil
}
