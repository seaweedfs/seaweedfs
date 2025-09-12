package protocol

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/mq/kafka/consumer"
	"github.com/seaweedfs/seaweedfs/weed/mq/kafka/integration"
	"github.com/seaweedfs/seaweedfs/weed/mq/kafka/offset"
	"github.com/seaweedfs/seaweedfs/weed/mq/kafka/schema"
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
	// Legacy in-memory mode (for backward compatibility and tests)
	topicsMu sync.RWMutex
	topics   map[string]*TopicInfo // topic name -> topic info

	ledgersMu sync.RWMutex
	ledgers   map[TopicPartitionKey]*offset.Ledger // topic-partition -> offset ledger

	// Record batch storage for in-memory mode (for testing)
	recordBatchMu sync.RWMutex
	recordBatches map[string][]byte // "topic:partition:offset" -> record batch data

	// SeaweedMQ integration (optional, for production use)
	seaweedMQHandler *integration.SeaweedMQHandler
	useSeaweedMQ     bool

	// Consumer group coordination
	groupCoordinator *consumer.GroupCoordinator

	// Schema management (optional, for schematized topics)
	schemaManager *schema.Manager
	useSchema     bool
	brokerClient  *schema.BrokerClient

	// Dynamic broker address for Metadata responses
	brokerHost string
	brokerPort int
}

// NewHandler creates a new handler in legacy in-memory mode
func NewHandler() *Handler {
	return &Handler{
		topics:           make(map[string]*TopicInfo),
		ledgers:          make(map[TopicPartitionKey]*offset.Ledger),
		recordBatches:    make(map[string][]byte),
		useSeaweedMQ:     false,
		groupCoordinator: consumer.NewGroupCoordinator(),
		brokerHost:       "localhost", // default fallback
		brokerPort:       9092,        // default fallback
	}
}

// NewSeaweedMQHandler creates a new handler with SeaweedMQ integration
func NewSeaweedMQHandler(agentAddress string) (*Handler, error) {
	smqHandler, err := integration.NewSeaweedMQHandler(agentAddress)
	if err != nil {
		return nil, err
	}

	return &Handler{
		topics:           make(map[string]*TopicInfo),                // Keep for compatibility
		ledgers:          make(map[TopicPartitionKey]*offset.Ledger), // Keep for compatibility
		seaweedMQHandler: smqHandler,
		useSeaweedMQ:     true,
		groupCoordinator: consumer.NewGroupCoordinator(),
	}, nil
}

// Close shuts down the handler and all connections
func (h *Handler) Close() error {
	// Close group coordinator
	if h.groupCoordinator != nil {
		h.groupCoordinator.Close()
	}

	// Close broker client if present
	if h.brokerClient != nil {
		if err := h.brokerClient.Close(); err != nil {
			fmt.Printf("Warning: failed to close broker client: %v\n", err)
		}
	}

	// Close SeaweedMQ handler if present
	if h.useSeaweedMQ && h.seaweedMQHandler != nil {
		return h.seaweedMQHandler.Close()
	}
	return nil
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

// StoreRecordBatch stores a record batch for later retrieval during Fetch operations
func (h *Handler) StoreRecordBatch(topicName string, partition int32, baseOffset int64, recordBatch []byte) {
	key := fmt.Sprintf("%s:%d:%d", topicName, partition, baseOffset)
	h.recordBatchMu.Lock()
	defer h.recordBatchMu.Unlock()
	h.recordBatches[key] = recordBatch
}

// GetRecordBatch retrieves a stored record batch that contains the requested offset
func (h *Handler) GetRecordBatch(topicName string, partition int32, offset int64) ([]byte, bool) {
	h.recordBatchMu.RLock()
	defer h.recordBatchMu.RUnlock()
	
	fmt.Printf("DEBUG: GetRecordBatch - looking for topic=%s, partition=%d, offset=%d\n", topicName, partition, offset)
	fmt.Printf("DEBUG: Available record batches: %d\n", len(h.recordBatches))
	
	// Look for a record batch that contains this offset
	// Record batches are stored by their base offset, but may contain multiple records
	topicPartitionPrefix := fmt.Sprintf("%s:%d:", topicName, partition)
	
	for key, batch := range h.recordBatches {
		fmt.Printf("DEBUG: Checking key: %s\n", key)
		if !strings.HasPrefix(key, topicPartitionPrefix) {
			continue
		}
		
		// Extract the base offset from the key
		parts := strings.Split(key, ":")
		if len(parts) != 3 {
			continue
		}
		
		baseOffset, err := strconv.ParseInt(parts[2], 10, 64)
		if err != nil {
			continue
		}
		
		// Check if this batch could contain the requested offset
		// We need to parse the batch to determine how many records it contains
		recordCount := h.getRecordCountFromBatch(batch)
		fmt.Printf("DEBUG: Batch key=%s, baseOffset=%d, recordCount=%d, requested offset=%d\n", key, baseOffset, recordCount, offset)
		
		if recordCount > 0 && offset >= baseOffset && offset < baseOffset+int64(recordCount) {
			fmt.Printf("DEBUG: Found matching batch for offset %d in batch with baseOffset %d\n", offset, baseOffset)
			return batch, true
		}
	}
	
	fmt.Printf("DEBUG: No matching batch found for offset %d\n", offset)
	return nil, false
}

// getRecordCountFromBatch extracts the record count from a Kafka record batch
func (h *Handler) getRecordCountFromBatch(batch []byte) int32 {
	// Kafka record batch format:
	// base_offset (8) + batch_length (4) + partition_leader_epoch (4) + magic (1) + crc (4) + 
	// attributes (2) + last_offset_delta (4) + first_timestamp (8) + max_timestamp (8) +
	// producer_id (8) + producer_epoch (2) + base_sequence (4) + records_count (4) + records...
	
	// The record count is at offset 57 (8+4+4+1+4+2+4+8+8+8+2+4 = 57)
	if len(batch) < 61 { // 57 + 4 bytes for record count
		return 0
	}
	
	recordCount := binary.BigEndian.Uint32(batch[57:61])
	return int32(recordCount)
}

// SetBrokerAddress updates the broker address used in Metadata responses
func (h *Handler) SetBrokerAddress(host string, port int) {
	h.brokerHost = host
	h.brokerPort = port
}

// HandleConn processes a single client connection
func (h *Handler) HandleConn(conn net.Conn) error {
	connectionID := fmt.Sprintf("%s->%s", conn.RemoteAddr(), conn.LocalAddr())
	defer func() {
		fmt.Printf("DEBUG: [%s] Connection closing\n", connectionID)
		conn.Close()
	}()

	r := bufio.NewReader(conn)
	w := bufio.NewWriter(conn)
	defer w.Flush()

	for {
		// Read message size (4 bytes)
		var sizeBytes [4]byte
		if _, err := io.ReadFull(r, sizeBytes[:]); err != nil {
			if err == io.EOF {
				fmt.Printf("DEBUG: Client closed connection (clean EOF)\n")
				return nil // clean disconnect
			}
			fmt.Printf("DEBUG: Error reading message size: %v\n", err)
			return fmt.Errorf("read size: %w", err)
		}

		size := binary.BigEndian.Uint32(sizeBytes[:])
		if size == 0 || size > 1024*1024 { // 1MB limit
			// TODO: Consider making message size limit configurable
			// 1MB might be too restrictive for some use cases
			// Kafka default max.message.bytes is often higher
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
		
		fmt.Printf("DEBUG: API Request - Key: %d, Version: %d, Correlation: %d\n", apiKey, apiVersion, correlationID)

		apiName := getAPIName(apiKey)

		// Validate API version against what we support
		if err := h.validateAPIVersion(apiKey, apiVersion); err != nil {
			// Return proper Kafka error response for unsupported version
			response, writeErr := h.buildUnsupportedVersionResponse(correlationID, apiKey, apiVersion)
			if writeErr != nil {
				return fmt.Errorf("build error response: %w", writeErr)
			}
			// Send error response and continue to next request
			responseSizeBytes := make([]byte, 4)
			binary.BigEndian.PutUint32(responseSizeBytes, uint32(len(response)))
			w.Write(responseSizeBytes)
			w.Write(response)
			w.Flush()
			continue
		}

		// Handle the request based on API key and version
		var response []byte
		var err error

		switch apiKey {
		case 18: // ApiVersions
			response, err = h.handleApiVersions(correlationID)
		case 3: // Metadata
			response, err = h.handleMetadata(correlationID, apiVersion, messageBuf[8:])
		case 2: // ListOffsets
			fmt.Printf("DEBUG: *** LISTOFFSETS REQUEST RECEIVED *** Correlation: %d, Version: %d\n", correlationID, apiVersion)
			response, err = h.handleListOffsets(correlationID, apiVersion, messageBuf[8:]) // skip header
		case 19: // CreateTopics
			response, err = h.handleCreateTopics(correlationID, apiVersion, messageBuf[8:]) // skip header
		case 20: // DeleteTopics
			response, err = h.handleDeleteTopics(correlationID, messageBuf[8:]) // skip header
		case 0: // Produce
			response, err = h.handleProduce(correlationID, apiVersion, messageBuf[8:])
		case 1: // Fetch
			fmt.Printf("DEBUG: *** FETCH HANDLER CALLED *** Correlation: %d, Version: %d\n", correlationID, apiVersion)
			response, err = h.handleFetch(correlationID, apiVersion, messageBuf[8:]) // skip header
			if err != nil {
				fmt.Printf("DEBUG: Fetch error: %v\n", err)
			} else {
				fmt.Printf("DEBUG: Fetch response hex dump (%d bytes): %x\n", len(response), response)
			}
		case 11: // JoinGroup
			fmt.Printf("DEBUG: *** JOINGROUP REQUEST RECEIVED *** Correlation: %d, Version: %d\n", correlationID, apiVersion)
			response, err = h.handleJoinGroup(correlationID, apiVersion, messageBuf[8:]) // skip header
			if err != nil {
				fmt.Printf("DEBUG: JoinGroup error: %v\n", err)
			} else {
				fmt.Printf("DEBUG: JoinGroup response hex dump (%d bytes): %x\n", len(response), response)
			}
		case 14: // SyncGroup
			fmt.Printf("DEBUG: *** 🎉 SYNCGROUP API CALLED! Version: %d, Correlation: %d ***\n", apiVersion, correlationID)
			response, err = h.handleSyncGroup(correlationID, apiVersion, messageBuf[8:]) // skip header
			if err != nil {
				fmt.Printf("DEBUG: SyncGroup error: %v\n", err)
			} else {
				fmt.Printf("DEBUG: SyncGroup response hex dump (%d bytes): %x\n", len(response), response)
			}
		case 8: // OffsetCommit
			response, err = h.handleOffsetCommit(correlationID, messageBuf[8:]) // skip header
		case 9: // OffsetFetch
			response, err = h.handleOffsetFetch(correlationID, messageBuf[8:]) // skip header
		case 10: // FindCoordinator
			fmt.Printf("DEBUG: *** FINDCOORDINATOR REQUEST RECEIVED *** Correlation: %d\n", correlationID)
			response, err = h.handleFindCoordinator(correlationID, messageBuf[8:]) // skip header
			if err != nil {
				fmt.Printf("DEBUG: FindCoordinator error: %v\n", err)
			}
		case 12: // Heartbeat
			response, err = h.handleHeartbeat(correlationID, messageBuf[8:]) // skip header
		case 13: // LeaveGroup
			response, err = h.handleLeaveGroup(correlationID, messageBuf[8:]) // skip header
		default:
			fmt.Printf("DEBUG: *** UNSUPPORTED API KEY *** %d (%s) v%d - Correlation: %d\n", apiKey, apiName, apiVersion, correlationID)
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

		// Minimal flush logging
		// fmt.Printf("DEBUG: API %d flushed\n", apiKey)
	}
}

func (h *Handler) handleApiVersions(correlationID uint32) ([]byte, error) {
	// Build ApiVersions response manually
	// Response format (v0): correlation_id(4) + error_code(2) + num_api_keys(4) + api_keys

	response := make([]byte, 0, 64)

	// Correlation ID
	correlationIDBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(correlationIDBytes, correlationID)
	response = append(response, correlationIDBytes...)

	// Error code (0 = no error)
	response = append(response, 0, 0)

	// Number of API keys (compact array format in newer versions, but using basic format for simplicity)
	response = append(response, 0, 0, 0, 14) // 14 API keys

	// API Key 18 (ApiVersions): api_key(2) + min_version(2) + max_version(2)
	response = append(response, 0, 18) // API key 18
	response = append(response, 0, 0)  // min version 0
	response = append(response, 0, 3)  // max version 3

	// API Key 3 (Metadata): api_key(2) + min_version(2) + max_version(2)
	// TEMPORARY FIX: Limit to v4 since v6 has format issues with kafka-go
	// Sarama works with v4, kafka-go should also work with v4
	response = append(response, 0, 3) // API key 3
	response = append(response, 0, 0) // min version 0
	response = append(response, 0, 4) // max version 4 (was 6)

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
	// Support v7 for Sarama compatibility (Kafka 2.1.0)
	response = append(response, 0, 0) // API key 0
	response = append(response, 0, 0) // min version 0
	response = append(response, 0, 7) // max version 7

	// API Key 1 (Fetch): api_key(2) + min_version(2) + max_version(2)
	response = append(response, 0, 1)  // API key 1
	response = append(response, 0, 0)  // min version 0
	response = append(response, 0, 11) // max version 11

	// API Key 11 (JoinGroup): api_key(2) + min_version(2) + max_version(2)
	response = append(response, 0, 11) // API key 11
	response = append(response, 0, 0)  // min version 0
	response = append(response, 0, 7)  // max version 7

	// API Key 14 (SyncGroup): api_key(2) + min_version(2) + max_version(2)
	response = append(response, 0, 14) // API key 14
	response = append(response, 0, 0)  // min version 0
	response = append(response, 0, 5)  // max version 5

	// API Key 8 (OffsetCommit): api_key(2) + min_version(2) + max_version(2)
	response = append(response, 0, 8) // API key 8
	response = append(response, 0, 0) // min version 0
	response = append(response, 0, 8) // max version 8

	// API Key 9 (OffsetFetch): api_key(2) + min_version(2) + max_version(2)
	response = append(response, 0, 9) // API key 9
	response = append(response, 0, 0) // min version 0
	response = append(response, 0, 8) // max version 8

	// API Key 10 (FindCoordinator): api_key(2) + min_version(2) + max_version(2)
	response = append(response, 0, 10) // API key 10
	response = append(response, 0, 0)  // min version 0
	response = append(response, 0, 4)  // max version 4

	// API Key 12 (Heartbeat): api_key(2) + min_version(2) + max_version(2)
	response = append(response, 0, 12) // API key 12
	response = append(response, 0, 0)  // min version 0
	response = append(response, 0, 4)  // max version 4

	// API Key 13 (LeaveGroup): api_key(2) + min_version(2) + max_version(2)
	response = append(response, 0, 13) // API key 13
	response = append(response, 0, 0)  // min version 0
	response = append(response, 0, 4)  // max version 4

	return response, nil
}

// handleMetadataV0 implements the Metadata API response in version 0 format.
// v0 response layout:
// correlation_id(4) + brokers(ARRAY) + topics(ARRAY)
// broker: node_id(4) + host(STRING) + port(4)
// topic: error_code(2) + name(STRING) + partitions(ARRAY)
// partition: error_code(2) + partition_id(4) + leader(4) + replicas(ARRAY<int32>) + isr(ARRAY<int32>)
func (h *Handler) HandleMetadataV0(correlationID uint32, requestBody []byte) ([]byte, error) {
	response := make([]byte, 0, 256)

	// Correlation ID
	correlationIDBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(correlationIDBytes, correlationID)
	response = append(response, correlationIDBytes...)

	// Brokers array length (4 bytes) - 1 broker (this gateway)
	response = append(response, 0, 0, 0, 1)

	// Broker 0: node_id(4) + host(STRING) + port(4)
	response = append(response, 0, 0, 0, 1) // node_id = 1 (consistent with partitions)

	// Use dynamic broker address set by the server
	host := h.brokerHost
	port := h.brokerPort
	fmt.Printf("DEBUG: Advertising broker (v0) at %s:%d\n", host, port)

	// Host (STRING: 2 bytes length + bytes)
	hostLen := uint16(len(host))
	response = append(response, byte(hostLen>>8), byte(hostLen))
	response = append(response, []byte(host)...)

	// Port (4 bytes)
	portBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(portBytes, uint32(port))
	response = append(response, portBytes...)

	// Parse requested topics (empty means all)
	requestedTopics := h.parseMetadataTopics(requestBody)
	fmt.Printf("DEBUG: 🔍 METADATA v0 REQUEST - Requested: %v (empty=all)\n", requestedTopics)

	// Determine topics to return
	h.topicsMu.RLock()
	var topicsToReturn []string
	if len(requestedTopics) == 0 {
		topicsToReturn = make([]string, 0, len(h.topics))
		for name := range h.topics {
			topicsToReturn = append(topicsToReturn, name)
		}
	} else {
		for _, name := range requestedTopics {
			if _, exists := h.topics[name]; exists {
				topicsToReturn = append(topicsToReturn, name)
			}
		}
	}
	h.topicsMu.RUnlock()

	// Topics array length (4 bytes)
	topicsCountBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(topicsCountBytes, uint32(len(topicsToReturn)))
	response = append(response, topicsCountBytes...)

	// Topic entries
	for _, topicName := range topicsToReturn {
		// error_code(2) = 0
		response = append(response, 0, 0)

		// name (STRING)
		nameBytes := []byte(topicName)
		nameLen := uint16(len(nameBytes))
		response = append(response, byte(nameLen>>8), byte(nameLen))
		response = append(response, nameBytes...)

		// partitions array length (4 bytes) - 1 partition
		response = append(response, 0, 0, 0, 1)

		// partition: error_code(2) + partition_id(4) + leader(4)
		response = append(response, 0, 0)       // error_code
		response = append(response, 0, 0, 0, 0) // partition_id = 0
		response = append(response, 0, 0, 0, 0) // leader = 0 (this broker)

		// replicas: array length(4) + one broker id (0)
		response = append(response, 0, 0, 0, 1)
		response = append(response, 0, 0, 0, 0)

		// isr: array length(4) + one broker id (0)
		response = append(response, 0, 0, 0, 1)
		response = append(response, 0, 0, 0, 0)
	}

	fmt.Printf("DEBUG: Metadata v0 response for %d topics: %v\n", len(topicsToReturn), topicsToReturn)
	fmt.Printf("DEBUG: *** METADATA v0 RESPONSE DETAILS ***\n")
	fmt.Printf("DEBUG: Response size: %d bytes\n", len(response))
	fmt.Printf("DEBUG: Broker: %s:%d\n", h.brokerHost, h.brokerPort)
	fmt.Printf("DEBUG: Topics: %v\n", topicsToReturn)
	for i, topic := range topicsToReturn {
		fmt.Printf("DEBUG: Topic[%d]: %s (1 partition)\n", i, topic)
	}
	fmt.Printf("DEBUG: *** END METADATA v0 RESPONSE ***\n")
	return response, nil
}

func (h *Handler) HandleMetadataV1(correlationID uint32, requestBody []byte) ([]byte, error) {
	// Simplified Metadata v1 implementation - based on working v0 + v1 additions
	// v1 adds: ControllerID (after brokers), Rack (for brokers), IsInternal (for topics)

	// Parse requested topics (empty means all)
	requestedTopics := h.parseMetadataTopics(requestBody)
	fmt.Printf("DEBUG: 🔍 METADATA v1 REQUEST - Requested: %v (empty=all)\n", requestedTopics)

	// Determine topics to return
	h.topicsMu.RLock()
	var topicsToReturn []string
	if len(requestedTopics) == 0 {
		topicsToReturn = make([]string, 0, len(h.topics))
		for name := range h.topics {
			topicsToReturn = append(topicsToReturn, name)
		}
	} else {
		for _, name := range requestedTopics {
			if _, exists := h.topics[name]; exists {
				topicsToReturn = append(topicsToReturn, name)
			}
		}
	}
	h.topicsMu.RUnlock()

	// Build response using same approach as v0 but with v1 additions
	response := make([]byte, 0, 256)

	// Correlation ID (4 bytes)
	correlationIDBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(correlationIDBytes, correlationID)
	response = append(response, correlationIDBytes...)

	// Brokers array length (4 bytes) - 1 broker (this gateway)
	response = append(response, 0, 0, 0, 1)

	// Broker 0: node_id(4) + host(STRING) + port(4) + rack(STRING)
	response = append(response, 0, 0, 0, 1) // node_id = 1

	// Use dynamic broker address set by the server
	host := h.brokerHost
	port := h.brokerPort
	fmt.Printf("DEBUG: Advertising broker (v1) at %s:%d\n", host, port)

	// Host (STRING: 2 bytes length + bytes)
	hostLen := uint16(len(host))
	response = append(response, byte(hostLen>>8), byte(hostLen))
	response = append(response, []byte(host)...)

	// Port (4 bytes)
	portBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(portBytes, uint32(port))
	response = append(response, portBytes...)

	// Rack (STRING: 2 bytes length + bytes) - v1 addition, non-nullable empty string
	response = append(response, 0, 0) // empty string

	// ControllerID (4 bytes) - v1 addition
	response = append(response, 0, 0, 0, 1) // controller_id = 1

	// Topics array length (4 bytes)
	topicsCountBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(topicsCountBytes, uint32(len(topicsToReturn)))
	response = append(response, topicsCountBytes...)

	// Topics
	for _, topicName := range topicsToReturn {
		// error_code (2 bytes)
		response = append(response, 0, 0)

		// topic name (STRING: 2 bytes length + bytes)
		topicLen := uint16(len(topicName))
		response = append(response, byte(topicLen>>8), byte(topicLen))
		response = append(response, []byte(topicName)...)

		// is_internal (1 byte) - v1 addition
		response = append(response, 0) // false

		// partitions array length (4 bytes) - 1 partition
		response = append(response, 0, 0, 0, 1)

		// partition 0: error_code(2) + partition_id(4) + leader_id(4) + replicas(ARRAY) + isr(ARRAY)
		response = append(response, 0, 0)       // error_code
		response = append(response, 0, 0, 0, 0) // partition_id = 0
		response = append(response, 0, 0, 0, 1) // leader_id = 1

		// replicas: array length(4) + one broker id (1)
		response = append(response, 0, 0, 0, 1)
		response = append(response, 0, 0, 0, 1)

		// isr: array length(4) + one broker id (1)
		response = append(response, 0, 0, 0, 1)
		response = append(response, 0, 0, 0, 1)
	}

	fmt.Printf("DEBUG: Metadata v1 response for %d topics: %v\n", len(topicsToReturn), topicsToReturn)
	fmt.Printf("DEBUG: Metadata v1 response size: %d bytes\n", len(response))
	return response, nil
}

// HandleMetadataV2 implements Metadata API v2 with ClusterID field
func (h *Handler) HandleMetadataV2(correlationID uint32, requestBody []byte) ([]byte, error) {
	// Metadata v2 adds ClusterID field (nullable string)
	// v2 response layout: correlation_id(4) + brokers(ARRAY) + cluster_id(NULLABLE_STRING) + controller_id(4) + topics(ARRAY)

	// Parse requested topics (empty means all)
	requestedTopics := h.parseMetadataTopics(requestBody)
	fmt.Printf("DEBUG: 🔍 METADATA v2 REQUEST - Requested: %v (empty=all)\n", requestedTopics)

	// Determine topics to return
	h.topicsMu.RLock()
	var topicsToReturn []string
	if len(requestedTopics) == 0 {
		topicsToReturn = make([]string, 0, len(h.topics))
		for name := range h.topics {
			topicsToReturn = append(topicsToReturn, name)
		}
	} else {
		for _, name := range requestedTopics {
			if _, exists := h.topics[name]; exists {
				topicsToReturn = append(topicsToReturn, name)
			}
		}
	}
	h.topicsMu.RUnlock()

	var buf bytes.Buffer

	// Correlation ID (4 bytes)
	binary.Write(&buf, binary.BigEndian, correlationID)

	// Brokers array (4 bytes length + brokers)
	binary.Write(&buf, binary.BigEndian, int32(1)) // 1 broker

	// Broker 0
	binary.Write(&buf, binary.BigEndian, int32(1)) // NodeID

	// Host (STRING: 2 bytes length + data)
	host := h.brokerHost
	binary.Write(&buf, binary.BigEndian, int16(len(host)))
	buf.WriteString(host)

	// Port (4 bytes)
	binary.Write(&buf, binary.BigEndian, int32(h.brokerPort))

	// Rack (STRING: 2 bytes length + data) - v1+ addition, non-nullable
	binary.Write(&buf, binary.BigEndian, int16(0)) // Empty string

	// ClusterID (NULLABLE_STRING: 2 bytes length + data) - v2 addition
	// Use -1 length to indicate null
	binary.Write(&buf, binary.BigEndian, int16(-1)) // Null cluster ID

	// ControllerID (4 bytes) - v1+ addition
	binary.Write(&buf, binary.BigEndian, int32(1))

	// Topics array (4 bytes length + topics)
	binary.Write(&buf, binary.BigEndian, int32(len(topicsToReturn)))

	for _, topicName := range topicsToReturn {
		// ErrorCode (2 bytes)
		binary.Write(&buf, binary.BigEndian, int16(0))

		// Name (STRING: 2 bytes length + data)
		binary.Write(&buf, binary.BigEndian, int16(len(topicName)))
		buf.WriteString(topicName)

		// IsInternal (1 byte) - v1+ addition
		buf.WriteByte(0) // false

		// Partitions array (4 bytes length + partitions)
		binary.Write(&buf, binary.BigEndian, int32(1)) // 1 partition

		// Partition 0
		binary.Write(&buf, binary.BigEndian, int16(0)) // ErrorCode
		binary.Write(&buf, binary.BigEndian, int32(0)) // PartitionIndex
		binary.Write(&buf, binary.BigEndian, int32(1)) // LeaderID

		// ReplicaNodes array (4 bytes length + nodes)
		binary.Write(&buf, binary.BigEndian, int32(1)) // 1 replica
		binary.Write(&buf, binary.BigEndian, int32(1)) // NodeID 1

		// IsrNodes array (4 bytes length + nodes)
		binary.Write(&buf, binary.BigEndian, int32(1)) // 1 ISR node
		binary.Write(&buf, binary.BigEndian, int32(1)) // NodeID 1
	}

	response := buf.Bytes()
	fmt.Printf("DEBUG: Advertising broker (v2) at %s:%d\n", h.brokerHost, h.brokerPort)
	fmt.Printf("DEBUG: Metadata v2 response for %d topics: %v\n", len(topicsToReturn), topicsToReturn)

	return response, nil
}

// HandleMetadataV3V4 implements Metadata API v3/v4 with ThrottleTimeMs field
func (h *Handler) HandleMetadataV3V4(correlationID uint32, requestBody []byte) ([]byte, error) {
	// Metadata v3/v4 adds ThrottleTimeMs field at the beginning
	// v3/v4 response layout: correlation_id(4) + throttle_time_ms(4) + brokers(ARRAY) + cluster_id(NULLABLE_STRING) + controller_id(4) + topics(ARRAY)

	// Parse requested topics (empty means all)
	requestedTopics := h.parseMetadataTopics(requestBody)

	// Determine topics to return
	h.topicsMu.RLock()
	var topicsToReturn []string
	if len(requestedTopics) == 0 {
		topicsToReturn = make([]string, 0, len(h.topics))
		for name := range h.topics {
			topicsToReturn = append(topicsToReturn, name)
		}
	} else {
		for _, name := range requestedTopics {
			if _, exists := h.topics[name]; exists {
				topicsToReturn = append(topicsToReturn, name)
			}
		}
	}
	h.topicsMu.RUnlock()

	var buf bytes.Buffer

	// Correlation ID (4 bytes)
	binary.Write(&buf, binary.BigEndian, correlationID)

	// ThrottleTimeMs (4 bytes) - v3+ addition
	binary.Write(&buf, binary.BigEndian, int32(0)) // No throttling

	// Brokers array (4 bytes length + brokers)
	binary.Write(&buf, binary.BigEndian, int32(1)) // 1 broker

	// Broker 0
	binary.Write(&buf, binary.BigEndian, int32(1)) // NodeID

	// Host (STRING: 2 bytes length + data)
	host := h.brokerHost
	binary.Write(&buf, binary.BigEndian, int16(len(host)))
	buf.WriteString(host)

	// Port (4 bytes)
	binary.Write(&buf, binary.BigEndian, int32(h.brokerPort))

	// Rack (STRING: 2 bytes length + data) - v1+ addition, non-nullable
	binary.Write(&buf, binary.BigEndian, int16(0)) // Empty string

	// ClusterID (NULLABLE_STRING: 2 bytes length + data) - v2+ addition
	// Use -1 length to indicate null
	binary.Write(&buf, binary.BigEndian, int16(-1)) // Null cluster ID

	// ControllerID (4 bytes) - v1+ addition
	binary.Write(&buf, binary.BigEndian, int32(1))

	// Topics array (4 bytes length + topics)
	binary.Write(&buf, binary.BigEndian, int32(len(topicsToReturn)))

	for _, topicName := range topicsToReturn {
		// ErrorCode (2 bytes)
		binary.Write(&buf, binary.BigEndian, int16(0))

		// Name (STRING: 2 bytes length + data)
		binary.Write(&buf, binary.BigEndian, int16(len(topicName)))
		buf.WriteString(topicName)

		// IsInternal (1 byte) - v1+ addition
		buf.WriteByte(0) // false

		// Partitions array (4 bytes length + partitions)
		binary.Write(&buf, binary.BigEndian, int32(1)) // 1 partition

		// Partition 0
		binary.Write(&buf, binary.BigEndian, int16(0)) // ErrorCode
		binary.Write(&buf, binary.BigEndian, int32(0)) // PartitionIndex
		binary.Write(&buf, binary.BigEndian, int32(1)) // LeaderID

		// ReplicaNodes array (4 bytes length + nodes)
		binary.Write(&buf, binary.BigEndian, int32(1)) // 1 replica
		binary.Write(&buf, binary.BigEndian, int32(1)) // NodeID 1

		// IsrNodes array (4 bytes length + nodes)
		binary.Write(&buf, binary.BigEndian, int32(1)) // 1 ISR node
		binary.Write(&buf, binary.BigEndian, int32(1)) // NodeID 1
	}

	response := buf.Bytes()

	return response, nil
}

// HandleMetadataV5V6 implements Metadata API v5/v6 with OfflineReplicas field
func (h *Handler) HandleMetadataV5V6(correlationID uint32, requestBody []byte) ([]byte, error) {
	// Metadata v5/v6 adds OfflineReplicas field to partitions
	// v5/v6 response layout: correlation_id(4) + throttle_time_ms(4) + brokers(ARRAY) + cluster_id(NULLABLE_STRING) + controller_id(4) + topics(ARRAY)
	// Each partition now includes: error_code(2) + partition_index(4) + leader_id(4) + replica_nodes(ARRAY) + isr_nodes(ARRAY) + offline_replicas(ARRAY)

	// Parse requested topics (empty means all)
	requestedTopics := h.parseMetadataTopics(requestBody)
	fmt.Printf("DEBUG: 🔍 METADATA v5/v6 REQUEST - Requested: %v (empty=all)\n", requestedTopics)

	// Determine topics to return
	h.topicsMu.RLock()
	var topicsToReturn []string
	if len(requestedTopics) == 0 {
		topicsToReturn = make([]string, 0, len(h.topics))
		for name := range h.topics {
			topicsToReturn = append(topicsToReturn, name)
		}
	} else {
		for _, name := range requestedTopics {
			if _, exists := h.topics[name]; exists {
				topicsToReturn = append(topicsToReturn, name)
			}
		}
	}
	h.topicsMu.RUnlock()

	var buf bytes.Buffer

	// Correlation ID (4 bytes)
	binary.Write(&buf, binary.BigEndian, correlationID)

	// ThrottleTimeMs (4 bytes) - v3+ addition
	binary.Write(&buf, binary.BigEndian, int32(0)) // No throttling

	// Brokers array (4 bytes length + brokers)
	binary.Write(&buf, binary.BigEndian, int32(1)) // 1 broker

	// Broker 0
	binary.Write(&buf, binary.BigEndian, int32(1)) // NodeID

	// Host (STRING: 2 bytes length + data)
	host := h.brokerHost
	binary.Write(&buf, binary.BigEndian, int16(len(host)))
	buf.WriteString(host)

	// Port (4 bytes)
	binary.Write(&buf, binary.BigEndian, int32(h.brokerPort))

	// Rack (STRING: 2 bytes length + data) - v1+ addition, non-nullable
	binary.Write(&buf, binary.BigEndian, int16(0)) // Empty string

	// ClusterID (NULLABLE_STRING: 2 bytes length + data) - v2+ addition
	// Use -1 length to indicate null
	binary.Write(&buf, binary.BigEndian, int16(-1)) // Null cluster ID

	// ControllerID (4 bytes) - v1+ addition
	binary.Write(&buf, binary.BigEndian, int32(1))

	// Topics array (4 bytes length + topics)
	binary.Write(&buf, binary.BigEndian, int32(len(topicsToReturn)))

	for _, topicName := range topicsToReturn {
		// ErrorCode (2 bytes)
		binary.Write(&buf, binary.BigEndian, int16(0))

		// Name (STRING: 2 bytes length + data)
		binary.Write(&buf, binary.BigEndian, int16(len(topicName)))
		buf.WriteString(topicName)

		// IsInternal (1 byte) - v1+ addition
		buf.WriteByte(0) // false

		// Partitions array (4 bytes length + partitions)
		binary.Write(&buf, binary.BigEndian, int32(1)) // 1 partition

		// Partition 0
		binary.Write(&buf, binary.BigEndian, int16(0)) // ErrorCode
		binary.Write(&buf, binary.BigEndian, int32(0)) // PartitionIndex
		binary.Write(&buf, binary.BigEndian, int32(1)) // LeaderID

		// ReplicaNodes array (4 bytes length + nodes)
		binary.Write(&buf, binary.BigEndian, int32(1)) // 1 replica
		binary.Write(&buf, binary.BigEndian, int32(1)) // NodeID 1

		// IsrNodes array (4 bytes length + nodes)
		binary.Write(&buf, binary.BigEndian, int32(1)) // 1 ISR node
		binary.Write(&buf, binary.BigEndian, int32(1)) // NodeID 1

		// OfflineReplicas array (4 bytes length + nodes) - v5+ addition
		binary.Write(&buf, binary.BigEndian, int32(0)) // No offline replicas
	}

	response := buf.Bytes()
	fmt.Printf("DEBUG: Advertising broker (v5/v6) at %s:%d\n", h.brokerHost, h.brokerPort)
	fmt.Printf("DEBUG: Metadata v5/v6 response for %d topics: %v\n", len(topicsToReturn), topicsToReturn)

	return response, nil
}

// HandleMetadataV7 implements Metadata API v7 with LeaderEpoch field
func (h *Handler) HandleMetadataV7(correlationID uint32, requestBody []byte) ([]byte, error) {
	// Metadata v7 adds LeaderEpoch field to partitions
	// v7 response layout: correlation_id(4) + throttle_time_ms(4) + brokers(ARRAY) + cluster_id(NULLABLE_STRING) + controller_id(4) + topics(ARRAY)
	// Each partition now includes: error_code(2) + partition_index(4) + leader_id(4) + leader_epoch(4) + replica_nodes(ARRAY) + isr_nodes(ARRAY) + offline_replicas(ARRAY)

	// Parse requested topics (empty means all)
	requestedTopics := h.parseMetadataTopics(requestBody)
	fmt.Printf("DEBUG: 🔍 METADATA v7 REQUEST - Requested: %v (empty=all)\n", requestedTopics)

	// Determine topics to return
	h.topicsMu.RLock()
	var topicsToReturn []string
	if len(requestedTopics) == 0 {
		topicsToReturn = make([]string, 0, len(h.topics))
		for name := range h.topics {
			topicsToReturn = append(topicsToReturn, name)
		}
	} else {
		for _, name := range requestedTopics {
			if _, exists := h.topics[name]; exists {
				topicsToReturn = append(topicsToReturn, name)
			}
		}
	}
	h.topicsMu.RUnlock()

	var buf bytes.Buffer

	// Correlation ID (4 bytes)
	binary.Write(&buf, binary.BigEndian, correlationID)

	// ThrottleTimeMs (4 bytes) - v3+ addition
	binary.Write(&buf, binary.BigEndian, int32(0)) // No throttling

	// Brokers array (4 bytes length + brokers)
	binary.Write(&buf, binary.BigEndian, int32(1)) // 1 broker

	// Broker 0
	binary.Write(&buf, binary.BigEndian, int32(1)) // NodeID

	// Host (STRING: 2 bytes length + data)
	host := h.brokerHost
	binary.Write(&buf, binary.BigEndian, int16(len(host)))
	buf.WriteString(host)

	// Port (4 bytes)
	binary.Write(&buf, binary.BigEndian, int32(h.brokerPort))

	// Rack (STRING: 2 bytes length + data) - v1+ addition, non-nullable
	binary.Write(&buf, binary.BigEndian, int16(0)) // Empty string

	// ClusterID (NULLABLE_STRING: 2 bytes length + data) - v2+ addition
	// Use -1 length to indicate null
	binary.Write(&buf, binary.BigEndian, int16(-1)) // Null cluster ID

	// ControllerID (4 bytes) - v1+ addition
	binary.Write(&buf, binary.BigEndian, int32(1))

	// Topics array (4 bytes length + topics)
	binary.Write(&buf, binary.BigEndian, int32(len(topicsToReturn)))

	for _, topicName := range topicsToReturn {
		// ErrorCode (2 bytes)
		binary.Write(&buf, binary.BigEndian, int16(0))

		// Name (STRING: 2 bytes length + data)
		binary.Write(&buf, binary.BigEndian, int16(len(topicName)))
		buf.WriteString(topicName)

		// IsInternal (1 byte) - v1+ addition
		buf.WriteByte(0) // false

		// Partitions array (4 bytes length + partitions)
		binary.Write(&buf, binary.BigEndian, int32(1)) // 1 partition

		// Partition 0
		binary.Write(&buf, binary.BigEndian, int16(0)) // ErrorCode
		binary.Write(&buf, binary.BigEndian, int32(0)) // PartitionIndex
		binary.Write(&buf, binary.BigEndian, int32(1)) // LeaderID

		// LeaderEpoch (4 bytes) - v7+ addition
		binary.Write(&buf, binary.BigEndian, int32(0)) // Leader epoch 0

		// ReplicaNodes array (4 bytes length + nodes)
		binary.Write(&buf, binary.BigEndian, int32(1)) // 1 replica
		binary.Write(&buf, binary.BigEndian, int32(1)) // NodeID 1

		// IsrNodes array (4 bytes length + nodes)
		binary.Write(&buf, binary.BigEndian, int32(1)) // 1 ISR node
		binary.Write(&buf, binary.BigEndian, int32(1)) // NodeID 1

		// OfflineReplicas array (4 bytes length + nodes) - v5+ addition
		binary.Write(&buf, binary.BigEndian, int32(0)) // No offline replicas
	}

	response := buf.Bytes()
	fmt.Printf("DEBUG: Advertising broker (v7) at %s:%d\n", h.brokerHost, h.brokerPort)
	fmt.Printf("DEBUG: Metadata v7 response for %d topics: %v\n", len(topicsToReturn), topicsToReturn)

	return response, nil
}

func (h *Handler) parseMetadataTopics(requestBody []byte) []string {
	// Support both v0/v1 parsing: v1 payload starts directly with topics array length (int32),
	// while older assumptions may have included a client_id string first.
	if len(requestBody) < 4 {
		return []string{}
	}

	// Try path A: interpret first 4 bytes as topics_count
	offset := 0
	topicsCount := binary.BigEndian.Uint32(requestBody[offset : offset+4])
	if topicsCount == 0xFFFFFFFF { // -1 means all topics
		return []string{}
	}
	if topicsCount <= 1000000 { // sane bound
		offset += 4
		topics := make([]string, 0, topicsCount)
		for i := uint32(0); i < topicsCount && offset+2 <= len(requestBody); i++ {
			nameLen := int(binary.BigEndian.Uint16(requestBody[offset : offset+2]))
			offset += 2
			if offset+nameLen > len(requestBody) {
				break
			}
			topics = append(topics, string(requestBody[offset:offset+nameLen]))
			offset += nameLen
		}
		return topics
	}

	// Path B: assume leading client_id string then topics_count
	if len(requestBody) < 6 {
		return []string{}
	}
	clientIDLen := int(binary.BigEndian.Uint16(requestBody[0:2]))
	offset = 2 + clientIDLen
	if len(requestBody) < offset+4 {
		return []string{}
	}
	topicsCount = binary.BigEndian.Uint32(requestBody[offset : offset+4])
	offset += 4
	if topicsCount == 0xFFFFFFFF {
		return []string{}
	}
	topics := make([]string, 0, topicsCount)
	for i := uint32(0); i < topicsCount && offset+2 <= len(requestBody); i++ {
		nameLen := int(binary.BigEndian.Uint16(requestBody[offset : offset+2]))
		offset += 2
		if offset+nameLen > len(requestBody) {
			break
		}
		topics = append(topics, string(requestBody[offset:offset+nameLen]))
		offset += nameLen
	}
	return topics
}

func (h *Handler) handleListOffsets(correlationID uint32, apiVersion uint16, requestBody []byte) ([]byte, error) {
	fmt.Printf("DEBUG: ListOffsets v%d request hex dump (first 100 bytes): %x\n", apiVersion, requestBody[:min(100, len(requestBody))])

	// Parse minimal request to understand what's being asked
	// For this stub, we'll just return stub responses for any requested topic/partition
	// Request format after client_id: topics_array

	if len(requestBody) < 6 { // at minimum need client_id_size(2) + topics_count(4)
		return nil, fmt.Errorf("ListOffsets request too short")
	}

	// Skip client_id: client_id_size(2) + client_id_data
	clientIDSize := binary.BigEndian.Uint16(requestBody[0:2])
	offset := 2 + int(clientIDSize)

	// ListOffsets v2+ has additional fields: replica_id(4) + isolation_level(1)
	if apiVersion >= 2 {
		if len(requestBody) < offset+5 {
			return nil, fmt.Errorf("ListOffsets v%d request missing replica_id/isolation_level", apiVersion)
		}
		replicaID := int32(binary.BigEndian.Uint32(requestBody[offset : offset+4]))
		isolationLevel := requestBody[offset+4]
		offset += 5
		fmt.Printf("DEBUG: ListOffsets v%d - replica_id: %d, isolation_level: %d\n", apiVersion, replicaID, isolationLevel)
	}

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

	// Throttle time (4 bytes, 0 = no throttling) - v2+ only
	if apiVersion >= 2 {
		response = append(response, 0, 0, 0, 0)
	}

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

	fmt.Printf("DEBUG: ListOffsets v%d response: %d bytes\n", apiVersion, len(response))
	return response, nil
}

func (h *Handler) handleCreateTopics(correlationID uint32, apiVersion uint16, requestBody []byte) ([]byte, error) {
	fmt.Printf("DEBUG: *** CREATETOPICS REQUEST RECEIVED *** Correlation: %d, Version: %d\n", correlationID, apiVersion)

	if len(requestBody) < 2 {
		return nil, fmt.Errorf("CreateTopics request too short")
	}

	// Parse based on API version
	switch apiVersion {
	case 0, 1:
		return h.handleCreateTopicsV0V1(correlationID, requestBody)
	case 2, 3, 4, 5:
		return h.handleCreateTopicsV2Plus(correlationID, apiVersion, requestBody)
	default:
		return nil, fmt.Errorf("unsupported CreateTopics API version: %d", apiVersion)
	}
}

func (h *Handler) handleCreateTopicsV2Plus(correlationID uint32, apiVersion uint16, requestBody []byte) ([]byte, error) {
	// CreateTopics v2+ format:
	// topics_array + timeout_ms(4) + validate_only(1) + [tagged_fields]

	offset := 0

	// Parse topics array (compact array format in v2+)
	if len(requestBody) < offset+1 {
		return nil, fmt.Errorf("CreateTopics v2+ request missing topics array")
	}

	// Read topics count (compact array: length + 1)
	topicsCountRaw := requestBody[offset]
	offset += 1

	var topicsCount uint32
	if topicsCountRaw == 0 {
		topicsCount = 0
	} else {
		topicsCount = uint32(topicsCountRaw) - 1
	}

	fmt.Printf("DEBUG: CreateTopics v%d - Topics count: %d, remaining bytes: %d\n", apiVersion, topicsCount, len(requestBody)-offset)

	// DEBUG: Hex dump to understand request format
	dumpLen := len(requestBody)
	if dumpLen > 50 {
		dumpLen = 50
	}
	fmt.Printf("DEBUG: CreateTopics v%d request hex dump (first %d bytes): %x\n", apiVersion, dumpLen, requestBody[:dumpLen])

	// Build response
	response := make([]byte, 0, 256)

	// Correlation ID
	correlationIDBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(correlationIDBytes, correlationID)
	response = append(response, correlationIDBytes...)

	// Throttle time (4 bytes, 0 = no throttling)
	response = append(response, 0, 0, 0, 0)

	// Topics array (compact format in v2+: count + 1)
	if topicsCount == 0 {
		response = append(response, 0) // Empty array
	} else {
		response = append(response, byte(topicsCount+1)) // Compact array format
	}

	// Process each topic
	h.topicsMu.Lock()
	defer h.topicsMu.Unlock()

	for i := uint32(0); i < topicsCount && offset < len(requestBody); i++ {
		// Parse topic name (compact string in v2+)
		if len(requestBody) < offset+1 {
			break
		}

		topicNameLengthRaw := requestBody[offset]
		offset += 1

		var topicNameLength int
		if topicNameLengthRaw == 0 {
			topicNameLength = 0
		} else {
			topicNameLength = int(topicNameLengthRaw) - 1
		}

		if len(requestBody) < offset+topicNameLength {
			break
		}

		topicName := string(requestBody[offset : offset+topicNameLength])
		offset += topicNameLength

		// Parse num_partitions (4 bytes)
		if len(requestBody) < offset+4 {
			break
		}
		numPartitions := binary.BigEndian.Uint32(requestBody[offset : offset+4])
		offset += 4

		// Parse replication_factor (2 bytes)
		if len(requestBody) < offset+2 {
			break
		}
		replicationFactor := binary.BigEndian.Uint16(requestBody[offset : offset+2])
		offset += 2

		// Parse configs (compact array in v2+)
		if len(requestBody) >= offset+1 {
			configsCountRaw := requestBody[offset]
			offset += 1

			var configsCount uint32
			if configsCountRaw == 0 {
				configsCount = 0
			} else {
				configsCount = uint32(configsCountRaw) - 1
			}

			// Skip configs for now (simplified)
			for j := uint32(0); j < configsCount && offset < len(requestBody); j++ {
				// Skip config name (compact string)
				if len(requestBody) >= offset+1 {
					configNameLengthRaw := requestBody[offset]
					offset += 1
					if configNameLengthRaw > 0 {
						configNameLength := int(configNameLengthRaw) - 1
						offset += configNameLength
					}
				}
				// Skip config value (compact string)
				if len(requestBody) >= offset+1 {
					configValueLengthRaw := requestBody[offset]
					offset += 1
					if configValueLengthRaw > 0 {
						configValueLength := int(configValueLengthRaw) - 1
						offset += configValueLength
					}
				}
			}
		}

		// Skip tagged fields (empty for now)
		if len(requestBody) >= offset+1 {
			taggedFieldsCount := requestBody[offset]
			offset += 1
			// Skip tagged fields (simplified - should be 0 for basic requests)
			for j := 0; j < int(taggedFieldsCount); j++ {
				// Skip tagged field parsing for now
				break
			}
		}

		fmt.Printf("DEBUG: Parsed topic: %s, partitions: %d, replication: %d\n", topicName, numPartitions, replicationFactor)

		// Response: topic_name (compact string) + error_code(2) + error_message (compact string)
		if len(topicName) == 0 {
			response = append(response, 0) // Empty string
		} else {
			response = append(response, byte(len(topicName)+1)) // Compact string format
		}
		response = append(response, []byte(topicName)...)

		// Check if topic already exists
		var errorCode uint16 = 0
		var errorMessage string = ""

		if h.useSeaweedMQ {
			// Use SeaweedMQ integration
			if h.seaweedMQHandler.TopicExists(topicName) {
				errorCode = 36 // TOPIC_ALREADY_EXISTS
				errorMessage = "Topic already exists"
			} else if numPartitions <= 0 {
				errorCode = 37 // INVALID_PARTITIONS
				errorMessage = "Invalid number of partitions"
			} else if replicationFactor <= 0 {
				errorCode = 38 // INVALID_REPLICATION_FACTOR
				errorMessage = "Invalid replication factor"
			} else {
				// Create the topic in SeaweedMQ
				if err := h.seaweedMQHandler.CreateTopic(topicName, int32(numPartitions)); err != nil {
					errorCode = 1 // UNKNOWN_SERVER_ERROR
					errorMessage = err.Error()
				}
			}
		} else {
			// Use legacy in-memory mode
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
		}

		// Error code
		response = append(response, byte(errorCode>>8), byte(errorCode))

		// Error message (compact nullable string in v2+)
		if errorMessage == "" {
			response = append(response, 0) // null string in compact format
		} else {
			response = append(response, byte(len(errorMessage)+1)) // Compact string format
			response = append(response, []byte(errorMessage)...)
		}

		// Tagged fields (empty)
		response = append(response, 0)
	}

	// Parse timeout_ms and validate_only at the end (after all topics)
	if len(requestBody) >= offset+4 {
		timeoutMs := binary.BigEndian.Uint32(requestBody[offset : offset+4])
		offset += 4
		fmt.Printf("DEBUG: CreateTopics timeout_ms: %d\n", timeoutMs)
	}

	if len(requestBody) >= offset+1 {
		validateOnly := requestBody[offset] != 0
		offset += 1
		fmt.Printf("DEBUG: CreateTopics validate_only: %v\n", validateOnly)
	}

	// Tagged fields at the end
	response = append(response, 0)

	return response, nil
}

// handleCreateTopicsV0V1 handles CreateTopics API versions 0 and 1
func (h *Handler) handleCreateTopicsV0V1(correlationID uint32, requestBody []byte) ([]byte, error) {
	// TODO: Implement v0/v1 parsing if needed
	// For now, return unsupported version error
	response := make([]byte, 0, 32)

	// Correlation ID
	correlationIDBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(correlationIDBytes, correlationID)
	response = append(response, correlationIDBytes...)

	// Throttle time
	response = append(response, 0, 0, 0, 0)

	// Empty topics array
	response = append(response, 0, 0, 0, 0)

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

		if h.useSeaweedMQ {
			// Use SeaweedMQ integration
			if !h.seaweedMQHandler.TopicExists(topicName) {
				errorCode = 3 // UNKNOWN_TOPIC_OR_PARTITION
				errorMessage = "Unknown topic"
			} else {
				// Delete the topic from SeaweedMQ
				if err := h.seaweedMQHandler.DeleteTopic(topicName); err != nil {
					errorCode = 1 // UNKNOWN_SERVER_ERROR
					errorMessage = err.Error()
				}
			}
		} else {
			// Use legacy in-memory mode
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

// validateAPIVersion checks if we support the requested API version
func (h *Handler) validateAPIVersion(apiKey, apiVersion uint16) error {
	supportedVersions := map[uint16][2]uint16{
		18: {0, 3},  // ApiVersions: v0-v3
		3:  {0, 7},  // Metadata: v0-v7
		0:  {0, 7},  // Produce: v0-v7
		1:  {0, 11}, // Fetch: v0-v11
		2:  {0, 5},  // ListOffsets: v0-v5
		19: {0, 4},  // CreateTopics: v0-v4
		20: {0, 4},  // DeleteTopics: v0-v4
		10: {0, 4},  // FindCoordinator: v0-v4
		11: {0, 7},  // JoinGroup: v0-v7
		14: {0, 5},  // SyncGroup: v0-v5
		8:  {0, 8},  // OffsetCommit: v0-v8
		9:  {0, 8},  // OffsetFetch: v0-v8
		12: {0, 4},  // Heartbeat: v0-v4
		13: {0, 4},  // LeaveGroup: v0-v4
	}

	if versionRange, exists := supportedVersions[apiKey]; exists {
		minVer, maxVer := versionRange[0], versionRange[1]
		if apiVersion < minVer || apiVersion > maxVer {
			return fmt.Errorf("unsupported API version %d for API key %d (supported: %d-%d)",
				apiVersion, apiKey, minVer, maxVer)
		}
		return nil
	}

	return fmt.Errorf("unsupported API key: %d", apiKey)
}

// buildUnsupportedVersionResponse creates a proper Kafka error response
func (h *Handler) buildUnsupportedVersionResponse(correlationID uint32, apiKey, apiVersion uint16) ([]byte, error) {
	response := make([]byte, 0, 16)

	// Correlation ID
	correlationIDBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(correlationIDBytes, correlationID)
	response = append(response, correlationIDBytes...)

	// Error code: UNSUPPORTED_VERSION (35)
	response = append(response, 0, 35)

	// Error message
	errorMsg := fmt.Sprintf("Unsupported version %d for API key %d", apiVersion, apiKey)
	errorMsgLen := uint16(len(errorMsg))
	response = append(response, byte(errorMsgLen>>8), byte(errorMsgLen))
	response = append(response, []byte(errorMsg)...)

	return response, nil
}

// handleMetadata routes to the appropriate version-specific handler
func (h *Handler) handleMetadata(correlationID uint32, apiVersion uint16, requestBody []byte) ([]byte, error) {
	switch apiVersion {
	case 0:
		return h.HandleMetadataV0(correlationID, requestBody)
	case 1:
		return h.HandleMetadataV1(correlationID, requestBody)
	case 2:
		return h.HandleMetadataV2(correlationID, requestBody)
	case 3, 4:
		return h.HandleMetadataV3V4(correlationID, requestBody)
	case 5, 6:
		return h.HandleMetadataV5V6(correlationID, requestBody)
	case 7:
		return h.HandleMetadataV7(correlationID, requestBody)
	default:
		return nil, fmt.Errorf("metadata version %d not implemented yet", apiVersion)
	}
}

// getAPIName returns a human-readable name for Kafka API keys (for debugging)
func getAPIName(apiKey uint16) string {
	switch apiKey {
	case 0:
		return "Produce"
	case 1:
		return "Fetch"
	case 2:
		return "ListOffsets"
	case 3:
		return "Metadata"
	case 8:
		return "OffsetCommit"
	case 9:
		return "OffsetFetch"
	case 10:
		return "FindCoordinator"
	case 11:
		return "JoinGroup"
	case 12:
		return "Heartbeat"
	case 13:
		return "LeaveGroup"
	case 14:
		return "SyncGroup"
	case 18:
		return "ApiVersions"
	case 19:
		return "CreateTopics"
	case 20:
		return "DeleteTopics"
	default:
		return "Unknown"
	}
}

// AddTopicForTesting adds a topic directly to the handler (for testing only)
func (h *Handler) AddTopicForTesting(topicName string, partitions int32) {
	h.topicsMu.Lock()
	defer h.topicsMu.Unlock()

	if _, exists := h.topics[topicName]; !exists {
		h.topics[topicName] = &TopicInfo{
			Name:       topicName,
			Partitions: partitions,
			CreatedAt:  time.Now().UnixNano(),
		}

		// Initialize ledgers for all partitions
		for partitionID := int32(0); partitionID < partitions; partitionID++ {
			h.GetOrCreateLedger(topicName, partitionID)
		}

	}
}

// EnableSchemaManagement enables schema management with the given configuration
func (h *Handler) EnableSchemaManagement(config schema.ManagerConfig) error {
	manager, err := schema.NewManagerWithHealthCheck(config)
	if err != nil {
		return fmt.Errorf("failed to create schema manager: %w", err)
	}

	h.schemaManager = manager
	h.useSchema = true

	fmt.Printf("Schema management enabled with registry: %s\n", config.RegistryURL)
	return nil
}

// EnableBrokerIntegration enables mq.broker integration for schematized messages
func (h *Handler) EnableBrokerIntegration(brokers []string) error {
	if !h.IsSchemaEnabled() {
		return fmt.Errorf("schema management must be enabled before broker integration")
	}

	brokerClient := schema.NewBrokerClient(schema.BrokerClientConfig{
		Brokers:       brokers,
		SchemaManager: h.schemaManager,
	})

	h.brokerClient = brokerClient
	fmt.Printf("Broker integration enabled with brokers: %v\n", brokers)
	return nil
}

// DisableSchemaManagement disables schema management and broker integration
func (h *Handler) DisableSchemaManagement() {
	if h.brokerClient != nil {
		h.brokerClient.Close()
		h.brokerClient = nil
		fmt.Println("Broker integration disabled")
	}
	h.schemaManager = nil
	h.useSchema = false
	fmt.Println("Schema management disabled")
}

// IsSchemaEnabled returns whether schema management is enabled
func (h *Handler) IsSchemaEnabled() bool {
	return h.useSchema && h.schemaManager != nil
}

// IsBrokerIntegrationEnabled returns true if broker integration is enabled
func (h *Handler) IsBrokerIntegrationEnabled() bool {
	return h.IsSchemaEnabled() && h.brokerClient != nil
}
