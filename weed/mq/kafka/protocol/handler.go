package protocol

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"strings"
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

// SeaweedMQHandlerInterface defines the interface for SeaweedMQ integration
type SeaweedMQHandlerInterface interface {
	TopicExists(topic string) bool
	ListTopics() []string
	CreateTopic(topic string, partitions int32) error
	DeleteTopic(topic string) error
	GetOrCreateLedger(topic string, partition int32) *offset.Ledger
	GetLedger(topic string, partition int32) *offset.Ledger
	ProduceRecord(topicName string, partitionID int32, key, value []byte) (int64, error)
	// GetStoredRecords retrieves records from SMQ storage (optional - for advanced implementations)
	GetStoredRecords(topic string, partition int32, fromOffset int64, maxRecords int) ([]offset.SMQRecord, error)
	Close() error
}

// Handler processes Kafka protocol requests from clients using SeaweedMQ
type Handler struct {
	// SeaweedMQ integration
	seaweedMQHandler SeaweedMQHandlerInterface

	// SMQ offset storage for consumer group offsets
	smqOffsetStorage *offset.SMQOffsetStorage

	// Consumer group coordination
	groupCoordinator *consumer.GroupCoordinator

	// Schema management (optional, for schematized topics)
	schemaManager *schema.Manager
	useSchema     bool
	brokerClient  *schema.BrokerClient

	// Dynamic broker address for Metadata responses
	brokerHost string
	brokerPort int

	// Connection context for tracking client information
	connContext *ConnectionContext
}

// NewHandler creates a basic Kafka handler with in-memory storage
// WARNING: This is for testing ONLY - never use in production!
// For production use with persistent storage, use NewSeaweedMQBrokerHandler instead
func NewHandler() *Handler {
	// Production safety check - prevent accidental production use
	// Comment out for testing: os.Getenv can be used for runtime checks
	panic("NewHandler() with in-memory storage should NEVER be used in production! Use NewSeaweedMQBrokerHandler() with SeaweedMQ masters for production, or NewTestHandler() for tests.")
}

// NewTestHandler and NewSimpleTestHandler moved to handler_test.go (test-only file)

// All test-related types and implementations moved to handler_test.go (test-only file)

// NewSeaweedMQHandler creates a new handler with SeaweedMQ integration
func NewSeaweedMQHandler(agentAddress string) (*Handler, error) {
	smqHandler, err := integration.NewSeaweedMQHandler(agentAddress)
	if err != nil {
		return nil, err
	}

	return &Handler{
		seaweedMQHandler: smqHandler,
		groupCoordinator: consumer.NewGroupCoordinator(),
		brokerHost:       "localhost",
		brokerPort:       9092,
	}, nil
}

// NewSeaweedMQBrokerHandler creates a new handler with SeaweedMQ broker integration
func NewSeaweedMQBrokerHandler(masters string, filerGroup string) (*Handler, error) {
	// Set up SeaweedMQ integration
	smqHandler, err := integration.NewSeaweedMQBrokerHandler(masters, filerGroup)
	if err != nil {
		return nil, err
	}

	// Create SMQ offset storage using the first master as filer address
	masterAddresses := strings.Split(masters, ",")
	filerAddress := masterAddresses[0] // Use first master as filer

	smqOffsetStorage, err := offset.NewSMQOffsetStorage(filerAddress)
	if err != nil {
		return nil, fmt.Errorf("failed to create SMQ offset storage: %w", err)
	}

	return &Handler{
		seaweedMQHandler: smqHandler,
		smqOffsetStorage: smqOffsetStorage,
		groupCoordinator: consumer.NewGroupCoordinator(),
		brokerHost:       "localhost", // default fallback
		brokerPort:       9092,        // default fallback
	}, nil
}

// AddTopicForTesting creates a topic for testing purposes
// This delegates to the underlying SeaweedMQ handler
func (h *Handler) AddTopicForTesting(topicName string, partitions int32) {
	if h.seaweedMQHandler != nil {
		h.seaweedMQHandler.CreateTopic(topicName, partitions)
	}
}

// Delegate methods to SeaweedMQ handler

// GetOrCreateLedger delegates to SeaweedMQ handler
func (h *Handler) GetOrCreateLedger(topic string, partition int32) *offset.Ledger {
	return h.seaweedMQHandler.GetOrCreateLedger(topic, partition)
}

// GetLedger delegates to SeaweedMQ handler
func (h *Handler) GetLedger(topic string, partition int32) *offset.Ledger {
	return h.seaweedMQHandler.GetLedger(topic, partition)
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
	if h.seaweedMQHandler != nil {
		return h.seaweedMQHandler.Close()
	}
	return nil
}

// StoreRecordBatch stores a record batch for later retrieval during Fetch operations
func (h *Handler) StoreRecordBatch(topicName string, partition int32, baseOffset int64, recordBatch []byte) {
	// Record batch storage is now handled by the SeaweedMQ handler
	fmt.Printf("DEBUG: StoreRecordBatch delegated to SeaweedMQ handler - topic:%s, partition:%d, offset:%d\n",
		topicName, partition, baseOffset)
}

// GetRecordBatch retrieves a stored record batch that contains the requested offset
func (h *Handler) GetRecordBatch(topicName string, partition int32, offset int64) ([]byte, bool) {
	// Record batch retrieval is now handled by the SeaweedMQ handler
	fmt.Printf("DEBUG: GetRecordBatch delegated to SeaweedMQ handler - topic:%s, partition:%d, offset:%d\n",
		topicName, partition, offset)
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
func (h *Handler) HandleConn(ctx context.Context, conn net.Conn) error {
	connectionID := fmt.Sprintf("%s->%s", conn.RemoteAddr(), conn.LocalAddr())

	// Set connection context for this connection
	h.connContext = &ConnectionContext{
		RemoteAddr:   conn.RemoteAddr(),
		LocalAddr:    conn.LocalAddr(),
		ConnectionID: connectionID,
	}

	defer func() {
		fmt.Printf("DEBUG: [%s] Connection closing\n", connectionID)
		h.connContext = nil // Clear connection context
		conn.Close()
	}()

	r := bufio.NewReader(conn)
	w := bufio.NewWriter(conn)
	defer w.Flush()

	// Use default timeout config
	timeoutConfig := DefaultTimeoutConfig()

	for {
		// Check if context is cancelled
		select {
		case <-ctx.Done():
			fmt.Printf("DEBUG: [%s] Context cancelled, closing connection\n", connectionID)
			return ctx.Err()
		default:
		}

		// Set a read deadline for the connection based on context or default timeout
		var readDeadline time.Time
		if deadline, ok := ctx.Deadline(); ok {
			readDeadline = deadline
		} else {
			// Use configurable read timeout instead of hardcoded 5 seconds
			readDeadline = time.Now().Add(timeoutConfig.ReadTimeout)
		}

		if err := conn.SetReadDeadline(readDeadline); err != nil {
			fmt.Printf("DEBUG: [%s] Failed to set read deadline: %v\n", connectionID, err)
			return fmt.Errorf("set read deadline: %w", err)
		}

		// Read message size (4 bytes)
		var sizeBytes [4]byte
		if _, err := io.ReadFull(r, sizeBytes[:]); err != nil {
			if err == io.EOF {
				fmt.Printf("DEBUG: Client closed connection (clean EOF)\n")
				return nil // clean disconnect
			}

			// Use centralized error classification
			errorCode := ClassifyNetworkError(err)
			switch errorCode {
			case ErrorCodeRequestTimedOut:
				// Check if error is due to context cancellation
				select {
				case <-ctx.Done():
					fmt.Printf("DEBUG: [%s] Read timeout due to context cancellation\n", connectionID)
					return ctx.Err()
				default:
					fmt.Printf("DEBUG: [%s] Read timeout: %v\n", connectionID, err)
					return fmt.Errorf("read timeout: %w", err)
				}
			case ErrorCodeNetworkException:
				fmt.Printf("DEBUG: [%s] Network error reading message size: %v\n", connectionID, err)
				return fmt.Errorf("network error: %w", err)
			default:
				fmt.Printf("DEBUG: [%s] Error reading message size: %v (code: %d)\n", connectionID, err, errorCode)
				return fmt.Errorf("read size: %w", err)
			}
		}

		size := binary.BigEndian.Uint32(sizeBytes[:])
		if size == 0 || size > 1024*1024 { // 1MB limit
			// Use standardized error for message size limit
			fmt.Printf("DEBUG: [%s] Invalid message size: %d (limit: 1MB)\n", connectionID, size)
			// Send error response for message too large
			errorResponse := BuildErrorResponse(0, ErrorCodeMessageTooLarge) // correlation ID 0 since we can't parse it yet
			if writeErr := h.writeResponseWithTimeout(w, errorResponse, timeoutConfig.WriteTimeout); writeErr != nil {
				fmt.Printf("DEBUG: [%s] Failed to send message too large response: %v\n", connectionID, writeErr)
			}
			return fmt.Errorf("message size %d exceeds limit", size)
		}

		// Set read deadline for message body
		if err := conn.SetReadDeadline(time.Now().Add(timeoutConfig.ReadTimeout)); err != nil {
			fmt.Printf("DEBUG: [%s] Failed to set message read deadline: %v\n", connectionID, err)
		}

		// Read the message
		messageBuf := make([]byte, size)
		if _, err := io.ReadFull(r, messageBuf); err != nil {
			errorCode := HandleTimeoutError(err, "read")
			fmt.Printf("DEBUG: [%s] Error reading message body: %v (code: %d)\n", connectionID, err, errorCode)
			return fmt.Errorf("read message: %w", err)
		}

		// Parse at least the basic header to get API key and correlation ID
		if len(messageBuf) < 8 {
			return fmt.Errorf("message too short")
		}

		apiKey := binary.BigEndian.Uint16(messageBuf[0:2])
		apiVersion := binary.BigEndian.Uint16(messageBuf[2:4])
		correlationID := binary.BigEndian.Uint32(messageBuf[4:8])

		apiName := getAPIName(apiKey)

		// Validate API version against what we support
		if err := h.validateAPIVersion(apiKey, apiVersion); err != nil {
			// Return proper Kafka error response for unsupported version
			response, writeErr := h.buildUnsupportedVersionResponse(correlationID, apiKey, apiVersion)
			if writeErr != nil {
				return fmt.Errorf("build error response: %w", writeErr)
			}
			// Send error response and continue to next request
			if writeErr := h.writeResponseWithTimeout(w, response, timeoutConfig.WriteTimeout); writeErr != nil {
				fmt.Printf("DEBUG: [%s] Failed to send unsupported version response: %v\n", connectionID, writeErr)
				return fmt.Errorf("send error response: %w", writeErr)
			}
			continue
		}

		// Parse header using flexible version utilities for validation and client ID extraction
		header, requestBody, parseErr := ParseRequestHeader(messageBuf)
		if parseErr != nil {
			// Fall back to basic header parsing if flexible version parsing fails
			fmt.Printf("DEBUG: Flexible header parsing failed, using basic parsing: %v\n", parseErr)

			// Basic header parsing fallback (original logic)
			bodyOffset := 8
			if len(messageBuf) < bodyOffset+2 {
				return fmt.Errorf("invalid header: missing client_id length")
			}
			clientIDLen := int16(binary.BigEndian.Uint16(messageBuf[bodyOffset : bodyOffset+2]))
			bodyOffset += 2
			if clientIDLen >= 0 {
				if len(messageBuf) < bodyOffset+int(clientIDLen) {
					return fmt.Errorf("invalid header: client_id truncated")
				}
				bodyOffset += int(clientIDLen)
			}
			requestBody = messageBuf[bodyOffset:]
		} else {
			// Validate parsed header matches what we already extracted
			if header.APIKey != apiKey || header.APIVersion != apiVersion || header.CorrelationID != correlationID {
				fmt.Printf("DEBUG: Header parsing mismatch - using basic parsing as fallback\n")
				// Fall back to basic parsing rather than failing
				bodyOffset := 8
				if len(messageBuf) < bodyOffset+2 {
					return fmt.Errorf("invalid header: missing client_id length")
				}
				clientIDLen := int16(binary.BigEndian.Uint16(messageBuf[bodyOffset : bodyOffset+2]))
				bodyOffset += 2
				if clientIDLen >= 0 {
					if len(messageBuf) < bodyOffset+int(clientIDLen) {
						return fmt.Errorf("invalid header: client_id truncated")
					}
					bodyOffset += int(clientIDLen)
				}
				requestBody = messageBuf[bodyOffset:]
			} else if header.ClientID != nil {
				// Log client ID if available and parsing was successful
				fmt.Printf("DEBUG: Client ID: %s\n", *header.ClientID)
			}
		}

		// Handle the request based on API key and version
		var response []byte
		var err error

		switch apiKey {
		case 18: // ApiVersions
			response, err = h.handleApiVersions(correlationID, apiVersion)
		case 3: // Metadata
			response, err = h.handleMetadata(correlationID, apiVersion, requestBody)
		case 2: // ListOffsets
			fmt.Printf("DEBUG: *** LISTOFFSETS REQUEST RECEIVED *** Correlation: %d, Version: %d\n", correlationID, apiVersion)
			response, err = h.handleListOffsets(correlationID, apiVersion, requestBody)
		case 19: // CreateTopics
			response, err = h.handleCreateTopics(correlationID, apiVersion, requestBody)
		case 20: // DeleteTopics
			response, err = h.handleDeleteTopics(correlationID, requestBody)
		case 0: // Produce
			response, err = h.handleProduce(correlationID, apiVersion, requestBody)
		case 1: // Fetch
			fmt.Printf("DEBUG: *** FETCH HANDLER CALLED *** Correlation: %d, Version: %d\n", correlationID, apiVersion)
			response, err = h.handleFetch(correlationID, apiVersion, requestBody)
			if err != nil {
				fmt.Printf("DEBUG: Fetch error: %v\n", err)
			} else {
				fmt.Printf("DEBUG: Fetch response hex dump (%d bytes): %x\n", len(response), response)
			}
		case 11: // JoinGroup
			fmt.Printf("DEBUG: *** JOINGROUP REQUEST RECEIVED *** Correlation: %d, Version: %d\n", correlationID, apiVersion)
			response, err = h.handleJoinGroup(correlationID, apiVersion, requestBody)
			if err != nil {
				fmt.Printf("DEBUG: JoinGroup error: %v\n", err)
			} else {
				fmt.Printf("DEBUG: JoinGroup response hex dump (%d bytes): %x\n", len(response), response)
			}
		case 14: // SyncGroup
			fmt.Printf("DEBUG: *** 🎉 SYNCGROUP API CALLED! Version: %d, Correlation: %d ***\n", apiVersion, correlationID)
			response, err = h.handleSyncGroup(correlationID, apiVersion, requestBody)
			if err != nil {
				fmt.Printf("DEBUG: SyncGroup error: %v\n", err)
			} else {
				fmt.Printf("DEBUG: SyncGroup response hex dump (%d bytes): %x\n", len(response), response)
			}
		case 8: // OffsetCommit
			response, err = h.handleOffsetCommit(correlationID, requestBody)
		case 9: // OffsetFetch
			fmt.Printf("DEBUG: *** OFFSETFETCH REQUEST RECEIVED *** Correlation: %d, Version: %d\n", correlationID, apiVersion)
			response, err = h.handleOffsetFetch(correlationID, apiVersion, requestBody)
			if err != nil {
				fmt.Printf("DEBUG: OffsetFetch error: %v\n", err)
			} else {
				fmt.Printf("DEBUG: OffsetFetch response hex dump (%d bytes): %x\n", len(response), response)
			}
		case 10: // FindCoordinator
			fmt.Printf("DEBUG: *** FINDCOORDINATOR REQUEST RECEIVED *** Correlation: %d, Version: %d\n", correlationID, apiVersion)
			response, err = h.handleFindCoordinator(correlationID, requestBody)
			if err != nil {
				fmt.Printf("DEBUG: FindCoordinator error: %v\n", err)
			}
		case 12: // Heartbeat
			response, err = h.handleHeartbeat(correlationID, requestBody)
		case 13: // LeaveGroup
			response, err = h.handleLeaveGroup(correlationID, requestBody)
		default:
			fmt.Printf("DEBUG: *** UNSUPPORTED API KEY *** %d (%s) v%d - Correlation: %d\n", apiKey, apiName, apiVersion, correlationID)
			err = fmt.Errorf("unsupported API key: %d (version %d)", apiKey, apiVersion)
		}

		if err != nil {
			return fmt.Errorf("handle request: %w", err)
		}

		// Send response with timeout handling
		if err := h.writeResponseWithTimeout(w, response, timeoutConfig.WriteTimeout); err != nil {
			errorCode := HandleTimeoutError(err, "write")
			fmt.Printf("DEBUG: [%s] Error sending response: %v (code: %d)\n", connectionID, err, errorCode)
			return fmt.Errorf("send response: %w", err)
		}

		// Minimal flush logging
		// fmt.Printf("DEBUG: API %d flushed\n", apiKey)
	}
}

func (h *Handler) handleApiVersions(correlationID uint32, apiVersion uint16) ([]byte, error) {
	// Build ApiVersions response supporting flexible versions (v3+)
	isFlexible := IsFlexibleVersion(18, apiVersion)

	response := make([]byte, 0, 128)

	// Correlation ID
	correlationIDBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(correlationIDBytes, correlationID)
	response = append(response, correlationIDBytes...)

	// Error code (0 = no error)
	response = append(response, 0, 0)

	// Number of API keys - use compact or regular array format based on version
	apiKeysCount := uint32(14)
	if isFlexible {
		// Compact array format for flexible versions
		response = append(response, CompactArrayLength(apiKeysCount)...)
	} else {
		// Regular array format for older versions
		response = append(response, 0, 0, 0, 14) // 14 API keys
	}

	// API Key 18 (ApiVersions): api_key(2) + min_version(2) + max_version(2)
	response = append(response, 0, 18) // API key 18
	response = append(response, 0, 0)  // min version 0
	response = append(response, 0, 3)  // max version 3

	// API Key 3 (Metadata): api_key(2) + min_version(2) + max_version(2)
	// TEMPORARY FIX: Limit to v4 since v6 has format issues with kafka-go
	// Sarama works with v4, kafka-go should also work with v4
	response = append(response, 0, 3) // API key 3
	response = append(response, 0, 0) // min version 0
	response = append(response, 0, 7) // max version 7

	// API Key 2 (ListOffsets): limit to v2 (implemented and tested)
	response = append(response, 0, 2) // API key 2
	response = append(response, 0, 0) // min version 0
	response = append(response, 0, 2) // max version 2

	// API Key 19 (CreateTopics): api_key(2) + min_version(2) + max_version(2)
	response = append(response, 0, 19) // API key 19
	response = append(response, 0, 0)  // min version 0
	response = append(response, 0, 5)  // max version 5

	// API Key 20 (DeleteTopics): api_key(2) + min_version(2) + max_version(2)
	response = append(response, 0, 20) // API key 20
	response = append(response, 0, 0)  // min version 0
	response = append(response, 0, 4)  // max version 4

	// API Key 0 (Produce): api_key(2) + min_version(2) + max_version(2)
	// Support v7 for Sarama compatibility (Kafka 2.1.0)
	response = append(response, 0, 0) // API key 0
	response = append(response, 0, 0) // min version 0
	response = append(response, 0, 7) // max version 7

	// API Key 1 (Fetch): limit to v7 (current handler semantics)
	response = append(response, 0, 1) // API key 1
	response = append(response, 0, 0) // min version 0
	response = append(response, 0, 7) // max version 7

	// API Key 11 (JoinGroup): api_key(2) + min_version(2) + max_version(2)
	response = append(response, 0, 11) // API key 11
	response = append(response, 0, 0)  // min version 0
	response = append(response, 0, 7)  // max version 7

	// API Key 14 (SyncGroup): api_key(2) + min_version(2) + max_version(2)
	response = append(response, 0, 14) // API key 14
	response = append(response, 0, 0)  // min version 0
	response = append(response, 0, 5)  // max version 5

	// API Key 8 (OffsetCommit): limit to v2 for current implementation
	response = append(response, 0, 8) // API key 8
	response = append(response, 0, 0) // min version 0
	response = append(response, 0, 2) // max version 2

	// API Key 9 (OffsetFetch): supports up to v5 (with leader epoch and throttle time)
	response = append(response, 0, 9) // API key 9
	response = append(response, 0, 0) // min version 0
	response = append(response, 0, 5) // max version 5

	// API Key 10 (FindCoordinator): limit to v2 (implemented)
	response = append(response, 0, 10) // API key 10
	response = append(response, 0, 0)  // min version 0
	response = append(response, 0, 2)  // max version 2

	// API Key 12 (Heartbeat): api_key(2) + min_version(2) + max_version(2)
	response = append(response, 0, 12) // API key 12
	response = append(response, 0, 0)  // min version 0
	response = append(response, 0, 4)  // max version 4

	// API Key 13 (LeaveGroup): api_key(2) + min_version(2) + max_version(2)
	response = append(response, 0, 13) // API key 13
	response = append(response, 0, 0)  // min version 0
	response = append(response, 0, 4)  // max version 4

	// Add tagged fields for flexible versions
	if isFlexible {
		// Empty tagged fields for now
		response = append(response, 0)
	}

	fmt.Printf("DEBUG: ApiVersions v%d response: %d bytes\n", apiVersion, len(response))
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

	// Determine topics to return using SeaweedMQ handler
	var topicsToReturn []string
	if len(requestedTopics) == 0 {
		topicsToReturn = h.seaweedMQHandler.ListTopics()
	} else {
		for _, name := range requestedTopics {
			if h.seaweedMQHandler.TopicExists(name) {
				topicsToReturn = append(topicsToReturn, name)
			}
		}
	}

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
		response = append(response, 0, 0, 0, 1) // leader = 1 (this broker)

		// replicas: array length(4) + one broker id (1)
		response = append(response, 0, 0, 0, 1)
		response = append(response, 0, 0, 0, 1)

		// isr: array length(4) + one broker id (1)
		response = append(response, 0, 0, 0, 1)
		response = append(response, 0, 0, 0, 1)
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

	// Determine topics to return using SeaweedMQ handler
	var topicsToReturn []string
	if len(requestedTopics) == 0 {
		topicsToReturn = h.seaweedMQHandler.ListTopics()
	} else {
		for _, name := range requestedTopics {
			if h.seaweedMQHandler.TopicExists(name) {
				topicsToReturn = append(topicsToReturn, name)
			}
		}
	}

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

	// Determine topics to return using SeaweedMQ handler
	var topicsToReturn []string
	if len(requestedTopics) == 0 {
		topicsToReturn = h.seaweedMQHandler.ListTopics()
	} else {
		for _, name := range requestedTopics {
			if h.seaweedMQHandler.TopicExists(name) {
				topicsToReturn = append(topicsToReturn, name)
			}
		}
	}

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

	// Determine topics to return using SeaweedMQ handler
	var topicsToReturn []string
	if len(requestedTopics) == 0 {
		topicsToReturn = h.seaweedMQHandler.ListTopics()
	} else {
		for _, name := range requestedTopics {
			if h.seaweedMQHandler.TopicExists(name) {
				topicsToReturn = append(topicsToReturn, name)
			}
		}
	}

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

	// Determine topics to return using SeaweedMQ handler
	var topicsToReturn []string
	if len(requestedTopics) == 0 {
		topicsToReturn = h.seaweedMQHandler.ListTopics()
	} else {
		for _, name := range requestedTopics {
			if h.seaweedMQHandler.TopicExists(name) {
				topicsToReturn = append(topicsToReturn, name)
			}
		}
	}

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

	// Determine topics to return using SeaweedMQ handler
	var topicsToReturn []string
	if len(requestedTopics) == 0 {
		topicsToReturn = h.seaweedMQHandler.ListTopics()
	} else {
		for _, name := range requestedTopics {
			if h.seaweedMQHandler.TopicExists(name) {
				topicsToReturn = append(topicsToReturn, name)
			}
		}
	}

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

	// Parse minimal request to understand what's being asked (header already stripped)
	offset := 0

	// v1+ has replica_id(4)
	if apiVersion >= 1 {
		if len(requestBody) < offset+4 {
			return nil, fmt.Errorf("ListOffsets v%d request missing replica_id", apiVersion)
		}
		replicaID := int32(binary.BigEndian.Uint32(requestBody[offset : offset+4]))
		offset += 4
		fmt.Printf("DEBUG: ListOffsets v%d - replica_id: %d\n", apiVersion, replicaID)
	}

	// v2+ adds isolation_level(1)
	if apiVersion >= 2 {
		if len(requestBody) < offset+1 {
			return nil, fmt.Errorf("ListOffsets v%d request missing isolation_level", apiVersion)
		}
		isolationLevel := requestBody[offset]
		offset += 1
		fmt.Printf("DEBUG: ListOffsets v%d - isolation_level: %d\n", apiVersion, isolationLevel)
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

	// Process each topic (using SeaweedMQ handler)

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
	fmt.Printf("DEBUG: CreateTopics v0/v1 - parsing request of %d bytes\n", len(requestBody))

	if len(requestBody) < 4 {
		return nil, fmt.Errorf("CreateTopics v0/v1 request too short")
	}

	offset := 0

	// Parse topics array (regular array format: count + topics)
	topicsCount := binary.BigEndian.Uint32(requestBody[offset : offset+4])
	offset += 4

	fmt.Printf("DEBUG: CreateTopics v0/v1 - Topics count: %d\n", topicsCount)

	// Build response
	response := make([]byte, 0, 256)

	// Correlation ID
	correlationIDBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(correlationIDBytes, correlationID)
	response = append(response, correlationIDBytes...)

	// Topics array count (4 bytes in v0/v1)
	topicsCountBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(topicsCountBytes, topicsCount)
	response = append(response, topicsCountBytes...)

	// Process each topic
	for i := uint32(0); i < topicsCount && offset < len(requestBody); i++ {
		// Parse topic name (regular string: length + bytes)
		if len(requestBody) < offset+2 {
			break
		}
		topicNameLength := binary.BigEndian.Uint16(requestBody[offset : offset+2])
		offset += 2

		if len(requestBody) < offset+int(topicNameLength) {
			break
		}
		topicName := string(requestBody[offset : offset+int(topicNameLength)])
		offset += int(topicNameLength)

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

		// Parse assignments array (4 bytes count, then assignments)
		if len(requestBody) < offset+4 {
			break
		}
		assignmentsCount := binary.BigEndian.Uint32(requestBody[offset : offset+4])
		offset += 4

		// Skip assignments for now (simplified)
		for j := uint32(0); j < assignmentsCount && offset < len(requestBody); j++ {
			// Skip partition_id (4 bytes)
			if len(requestBody) >= offset+4 {
				offset += 4
			}
			// Skip replicas array (4 bytes count + replica_ids)
			if len(requestBody) >= offset+4 {
				replicasCount := binary.BigEndian.Uint32(requestBody[offset : offset+4])
				offset += 4
				offset += int(replicasCount) * 4 // Skip replica IDs
			}
		}

		// Parse configs array (4 bytes count, then configs)
		if len(requestBody) >= offset+4 {
			configsCount := binary.BigEndian.Uint32(requestBody[offset : offset+4])
			offset += 4

			// Skip configs (simplified)
			for j := uint32(0); j < configsCount && offset < len(requestBody); j++ {
				// Skip config name (string: 2 bytes length + bytes)
				if len(requestBody) >= offset+2 {
					configNameLength := binary.BigEndian.Uint16(requestBody[offset : offset+2])
					offset += 2 + int(configNameLength)
				}
				// Skip config value (string: 2 bytes length + bytes)
				if len(requestBody) >= offset+2 {
					configValueLength := binary.BigEndian.Uint16(requestBody[offset : offset+2])
					offset += 2 + int(configValueLength)
				}
			}
		}

		fmt.Printf("DEBUG: CreateTopics v0/v1 - Parsed topic: %s, partitions: %d, replication: %d\n",
			topicName, numPartitions, replicationFactor)

		// Build response for this topic
		// Topic name (string: length + bytes)
		topicNameLengthBytes := make([]byte, 2)
		binary.BigEndian.PutUint16(topicNameLengthBytes, uint16(len(topicName)))
		response = append(response, topicNameLengthBytes...)
		response = append(response, []byte(topicName)...)

		// Determine error code and message
		var errorCode uint16 = 0

		// Use SeaweedMQ integration
		if h.seaweedMQHandler.TopicExists(topicName) {
			errorCode = 36 // TOPIC_ALREADY_EXISTS
		} else if numPartitions <= 0 {
			errorCode = 37 // INVALID_PARTITIONS
		} else if replicationFactor <= 0 {
			errorCode = 38 // INVALID_REPLICATION_FACTOR
		} else {
			// Create the topic in SeaweedMQ
			if err := h.seaweedMQHandler.CreateTopic(topicName, int32(numPartitions)); err != nil {
				errorCode = 1 // UNKNOWN_SERVER_ERROR
			}
		}

		// Error code (2 bytes)
		errorCodeBytes := make([]byte, 2)
		binary.BigEndian.PutUint16(errorCodeBytes, errorCode)
		response = append(response, errorCodeBytes...)
	}

	// Parse timeout_ms (4 bytes) - at the end of request
	if len(requestBody) >= offset+4 {
		timeoutMs := binary.BigEndian.Uint32(requestBody[offset : offset+4])
		fmt.Printf("DEBUG: CreateTopics v0/v1 - timeout_ms: %d\n", timeoutMs)
		offset += 4
	}

	// Parse validate_only (1 byte) - only in v1
	if len(requestBody) >= offset+1 {
		validateOnly := requestBody[offset] != 0
		fmt.Printf("DEBUG: CreateTopics v0/v1 - validate_only: %v\n", validateOnly)
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

	// Process each topic (using SeaweedMQ handler)

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
		3:  {0, 7}, // Metadata: v0-v7
		0:  {0, 7}, // Produce: v0-v7
		1:  {0, 7}, // Fetch: v0-v7
		2:  {0, 2}, // ListOffsets: v0-v2
		19: {0, 5}, // CreateTopics: v0-v5 (updated to match implementation)
		20: {0, 4}, // DeleteTopics: v0-v4
		10: {0, 2}, // FindCoordinator: v0-v2
		11: {0, 7}, // JoinGroup: v0-v7
		14: {0, 5}, // SyncGroup: v0-v5
		8:  {0, 2}, // OffsetCommit: v0-v2
		9:  {0, 5}, // OffsetFetch: v0-v5 (updated to match implementation)
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
	errorMsg := fmt.Sprintf("Unsupported version %d for API key %d", apiVersion, apiKey)
	return BuildErrorResponseWithMessage(correlationID, ErrorCodeUnsupportedVersion, errorMsg), nil
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

// writeResponseWithTimeout writes a Kafka response with timeout handling
func (h *Handler) writeResponseWithTimeout(w *bufio.Writer, response []byte, timeout time.Duration) error {
	// Note: bufio.Writer doesn't support direct timeout setting
	// Timeout handling should be done at the connection level before calling this function

	// Write response size (4 bytes)
	responseSizeBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(responseSizeBytes, uint32(len(response)))

	if _, err := w.Write(responseSizeBytes); err != nil {
		return fmt.Errorf("write response size: %w", err)
	}

	// Write response data
	if _, err := w.Write(response); err != nil {
		return fmt.Errorf("write response data: %w", err)
	}

	// Flush the buffer
	if err := w.Flush(); err != nil {
		return fmt.Errorf("flush response: %w", err)
	}

	return nil
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

// commitOffsetToSMQ commits offset using SMQ storage
func (h *Handler) commitOffsetToSMQ(key offset.ConsumerOffsetKey, offsetValue int64, metadata string) error {
	if h.smqOffsetStorage == nil {
		return fmt.Errorf("SMQ offset storage not initialized")
	}

	// Save to SMQ storage - use current timestamp and size 0 as placeholders
	// since SMQ storage primarily tracks the committed offset
	return h.smqOffsetStorage.SaveConsumerOffset(key, offsetValue, time.Now().UnixNano(), 0)
}

// fetchOffsetFromSMQ fetches offset using SMQ storage
func (h *Handler) fetchOffsetFromSMQ(key offset.ConsumerOffsetKey) (int64, string, error) {
	if h.smqOffsetStorage == nil {
		return -1, "", fmt.Errorf("SMQ offset storage not initialized")
	}

	entries, err := h.smqOffsetStorage.LoadConsumerOffsets(key)
	if err != nil {
		return -1, "", err
	}

	if len(entries) == 0 {
		return -1, "", nil // No committed offset
	}

	// Return the committed offset (metadata is not stored in SMQ format)
	return entries[0].KafkaOffset, "", nil
}
