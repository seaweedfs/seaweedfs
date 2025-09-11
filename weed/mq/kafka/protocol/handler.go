package protocol

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"sync"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/mq/kafka/consumer"
	"github.com/seaweedfs/seaweedfs/weed/mq/kafka/integration"
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
	// Legacy in-memory mode (for backward compatibility and tests)
	topicsMu sync.RWMutex
	topics   map[string]*TopicInfo // topic name -> topic info

	ledgersMu sync.RWMutex
	ledgers   map[TopicPartitionKey]*offset.Ledger // topic-partition -> offset ledger

	// SeaweedMQ integration (optional, for production use)
	seaweedMQHandler *integration.SeaweedMQHandler
	useSeaweedMQ     bool

	// Consumer group coordination
	groupCoordinator *consumer.GroupCoordinator

	// Dynamic broker address for Metadata responses
	brokerHost string
	brokerPort int
}

// NewHandler creates a new handler in legacy in-memory mode
func NewHandler() *Handler {
	return &Handler{
		topics:           make(map[string]*TopicInfo),
		ledgers:          make(map[TopicPartitionKey]*offset.Ledger),
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

	fmt.Printf("DEBUG: [%s] New connection established\n", connectionID)

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

		// DEBUG: Log all incoming requests (minimal for performance)
		apiName := getAPIName(apiKey)
		requestStart := time.Now()
		fmt.Printf("DEBUG: API %d (%s) v%d - Correlation: %d, Size: %d\n",
			apiKey, apiName, apiVersion, correlationID, size)

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
			response, err = h.handleListOffsets(correlationID, messageBuf[8:]) // skip header
		case 19: // CreateTopics
			response, err = h.handleCreateTopics(correlationID, messageBuf[8:]) // skip header
		case 20: // DeleteTopics
			response, err = h.handleDeleteTopics(correlationID, messageBuf[8:]) // skip header
		case 0: // Produce
			fmt.Printf("DEBUG: *** PRODUCE REQUEST RECEIVED *** Correlation: %d\n", correlationID)
			response, err = h.handleProduce(correlationID, apiVersion, messageBuf[8:])
		case 1: // Fetch
			response, err = h.handleFetch(correlationID, messageBuf[8:]) // skip header
		case 11: // JoinGroup
			fmt.Printf("DEBUG: *** JOINGROUP REQUEST RECEIVED *** Correlation: %d, Version: %d\n", correlationID, apiVersion)
			response, err = h.handleJoinGroup(correlationID, apiVersion, messageBuf[8:]) // skip header
			if err != nil {
				fmt.Printf("DEBUG: JoinGroup error: %v\n", err)
			} else {
				fmt.Printf("DEBUG: JoinGroup response hex dump (%d bytes): %x\n", len(response), response)
			}
		case 14: // SyncGroup
			fmt.Printf("DEBUG: *** SYNCGROUP REQUEST RECEIVED *** Correlation: %d, Version: %d\n", correlationID, apiVersion)
			fmt.Printf("DEBUG: *** THIS IS CRITICAL - SYNCGROUP WAS CALLED! ***\n")
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

		// DEBUG: Log response details (minimal for performance)
		processingDuration := time.Since(requestStart)
		fmt.Printf("DEBUG: API %d (%s) response: %d bytes, %v\n",
			apiKey, apiName, len(response), processingDuration)

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
	// Response format: correlation_id(4) + error_code(2) + num_api_keys(4) + api_keys + throttle_time(4)

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
	// TEMPORARY: Force v0 only until kafka-go compatibility issue is resolved
	response = append(response, 0, 3) // API key 3
	response = append(response, 0, 0) // min version 0
	response = append(response, 0, 0) // max version 0 (force v0 for kafka-go compatibility)

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
	// Advertise v1 to get simpler request format from kafka-go
	response = append(response, 0, 0) // API key 0
	response = append(response, 0, 0) // min version 0
	response = append(response, 0, 1) // max version 1 (simplified parsing)

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

	// Throttle time (4 bytes, 0 = no throttling)
	response = append(response, 0, 0, 0, 0)

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
	response = append(response, 0, 0, 0, 0) // node_id = 0

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
	return response, nil
}

func (h *Handler) HandleMetadataV1(correlationID uint32, requestBody []byte) ([]byte, error) {
	// TEMPORARY: Use v0 format as base and add only the essential v1 differences
	// This is to debug the kafka-go parsing issue

	response := make([]byte, 0, 256)

	// Correlation ID
	correlationIDBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(correlationIDBytes, correlationID)
	response = append(response, correlationIDBytes...)

	// Brokers array length (4 bytes) - 1 broker (this gateway)
	response = append(response, 0, 0, 0, 1)

	// Broker 0: node_id(4) + host(STRING) + port(4) + rack(STRING) [v1 adds rack]
	response = append(response, 0, 0, 0, 0) // node_id = 0

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

	// Rack (STRING) - v1 addition: empty string (NOT nullable)
	response = append(response, 0x00, 0x00)

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

	// Topics array length (4 bytes)
	topicsCountBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(topicsCountBytes, uint32(len(topicsToReturn)))
	response = append(response, topicsCountBytes...)

	// Topic entries - using v0 format first, then add v1 differences
	for _, topicName := range topicsToReturn {
		// error_code(2) = 0
		response = append(response, 0, 0)

		// name (STRING)
		nameBytes := []byte(topicName)
		nameLen := uint16(len(nameBytes))
		response = append(response, byte(nameLen>>8), byte(nameLen))
		response = append(response, nameBytes...)

		// is_internal(1) = false - v1 addition: this is the key difference!
		response = append(response, 0)

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

	fmt.Printf("DEBUG: Metadata v1 response for %d topics: %v\n", len(topicsToReturn), topicsToReturn)
	fmt.Printf("DEBUG: Metadata v1 response hex dump (%d bytes): %x\n", len(response), response)

	// CRITICAL DEBUG: Let's also compare with v0 format
	v0Response, _ := h.HandleMetadataV0(correlationID, requestBody)
	fmt.Printf("DEBUG: Metadata v0 response hex dump (%d bytes): %x\n", len(v0Response), v0Response)
	fmt.Printf("DEBUG: v1 vs v0 length difference: %d bytes\n", len(response)-len(v0Response))

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
	// TODO: CRITICAL - This function only supports CreateTopics v0 format
	// kafka-go uses v2 which has a different request structure!
	// The wrong topics count (1274981) shows we're parsing from wrong offset
	// Need to implement proper v2 request parsing or negotiate API version

	// Parse minimal CreateTopics request
	// Request format: client_id + timeout(4) + topics_array

	if len(requestBody) < 6 { // client_id_size(2) + timeout(4)
		return nil, fmt.Errorf("CreateTopics request too short")
	}

	// Skip client_id
	clientIDSize := binary.BigEndian.Uint16(requestBody[0:2])
	offset := 2 + int(clientIDSize)

	fmt.Printf("DEBUG: Client ID size: %d, client ID: %s\n", clientIDSize, string(requestBody[2:2+clientIDSize]))

	// CreateTopics v2 has different format than v0
	// v2 format: client_id + topics_array + timeout(4) + validate_only(1)
	// (no separate timeout field before topics like in v0)

	if len(requestBody) < offset+4 {
		return nil, fmt.Errorf("CreateTopics request missing topics array")
	}

	// Read topics count directly (no timeout field before it in v2)
	topicsCount := binary.BigEndian.Uint32(requestBody[offset : offset+4])
	offset += 4

	// DEBUG: Hex dump first 50 bytes to understand v2 format
	dumpLen := len(requestBody)
	if dumpLen > 50 {
		dumpLen = 50
	}
	fmt.Printf("DEBUG: CreateTopics v2 request hex dump (first %d bytes): %x\n", dumpLen, requestBody[:dumpLen])
	fmt.Printf("DEBUG: CreateTopics v2 - Topics count: %d, remaining bytes: %d\n", topicsCount, len(requestBody)-offset)

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
		18: {0, 3}, // ApiVersions: v0-v3
		3:  {0, 1}, // Metadata: v0-v1
		0:  {0, 1}, // Produce: v0-v1
		1:  {0, 1}, // Fetch: v0-v1
		2:  {0, 5}, // ListOffsets: v0-v5
		19: {0, 4}, // CreateTopics: v0-v4
		20: {0, 4}, // DeleteTopics: v0-v4
		10: {0, 4}, // FindCoordinator: v0-v4
		11: {0, 7}, // JoinGroup: v0-v7
		14: {0, 5}, // SyncGroup: v0-v5
		8:  {0, 8}, // OffsetCommit: v0-v8
		9:  {0, 8}, // OffsetFetch: v0-v8
		12: {0, 4}, // Heartbeat: v0-v4
		13: {0, 4}, // LeaveGroup: v0-v4
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
	case 2, 3, 4, 5, 6:
		// For now, use v1 format for higher versions (kafka-go compatibility)
		// TODO: Implement proper v2-v6 formats with additional fields
		return h.HandleMetadataV1(correlationID, requestBody)
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

		fmt.Printf("DEBUG: Added topic for testing: %s with %d partitions\n", topicName, partitions)
	}
}
