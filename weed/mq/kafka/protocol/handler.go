package protocol

import (
	"bufio"
	"bytes"
	"context"
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
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/mq_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/schema_pb"
	"github.com/seaweedfs/seaweedfs/weed/security"
	"github.com/seaweedfs/seaweedfs/weed/util"
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

const (
	// DefaultKafkaNamespace is the default namespace for Kafka topics in SeaweedMQ
	DefaultKafkaNamespace = "kafka"
)

// SeaweedMQHandlerInterface defines the interface for SeaweedMQ integration
type SeaweedMQHandlerInterface interface {
	TopicExists(topic string) bool
	ListTopics() []string
	CreateTopic(topic string, partitions int32) error
	DeleteTopic(topic string) error
	GetTopicInfo(topic string) (*integration.KafkaTopicInfo, bool)
	GetOrCreateLedger(topic string, partition int32) *offset.Ledger
	GetLedger(topic string, partition int32) *offset.Ledger
	ProduceRecord(topicName string, partitionID int32, key, value []byte) (int64, error)
	ProduceRecordValue(topicName string, partitionID int32, key []byte, recordValueBytes []byte) (int64, error)
	// GetStoredRecords retrieves records from SMQ storage (optional - for advanced implementations)
	GetStoredRecords(topic string, partition int32, fromOffset int64, maxRecords int) ([]offset.SMQRecord, error)
	// WithFilerClient executes a function with a filer client for accessing SeaweedMQ metadata
	WithFilerClient(streamingMode bool, fn func(client filer_pb.SeaweedFilerClient) error) error
	// GetBrokerAddresses returns the discovered SMQ broker addresses for Metadata responses
	GetBrokerAddresses() []string
	Close() error
}

// TopicSchemaConfig holds schema configuration for a topic
type TopicSchemaConfig struct {
	SchemaID     uint32
	SchemaFormat schema.Format
}

// Handler processes Kafka protocol requests from clients using SeaweedMQ
type Handler struct {
	// SeaweedMQ integration
	seaweedMQHandler SeaweedMQHandlerInterface

	// SMQ offset storage for consumer group offsets
	smqOffsetStorage offset.LedgerStorage

	// Consumer group coordination
	groupCoordinator *consumer.GroupCoordinator

	// Coordinator registry for distributed coordinator assignment
	coordinatorRegistry CoordinatorRegistryInterface

	// Schema management (optional, for schematized topics)
	schemaManager *schema.Manager
	useSchema     bool
	brokerClient  *schema.BrokerClient

	// Topic schema configuration cache
	topicSchemaConfigs  map[string]*TopicSchemaConfig
	topicSchemaConfigMu sync.RWMutex

	filerClient filer_pb.SeaweedFilerClient

	// SMQ broker addresses discovered from masters for Metadata responses
	smqBrokerAddresses []string

	// Gateway address for coordinator registry
	gatewayAddress string

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

// NewSeaweedMQBrokerHandler creates a new handler with SeaweedMQ broker integration
func NewSeaweedMQBrokerHandler(masters string, filerGroup string, clientHost string) (*Handler, error) {
	// Set up SeaweedMQ integration
	smqHandler, err := integration.NewSeaweedMQBrokerHandler(masters, filerGroup, clientHost)
	if err != nil {
		return nil, err
	}

	// Use the shared filer client accessor from SeaweedMQHandler
	sharedFilerAccessor := smqHandler.GetFilerClientAccessor()
	if sharedFilerAccessor == nil {
		return nil, fmt.Errorf("no shared filer client accessor available from SMQ handler")
	}

	// Create SMQ offset storage using the shared filer client accessor
	smqOffsetStorage := offset.NewSMQOffsetStorage(sharedFilerAccessor)

	return &Handler{
		seaweedMQHandler:   smqHandler,
		smqOffsetStorage:   smqOffsetStorage,
		groupCoordinator:   consumer.NewGroupCoordinator(),
		smqBrokerAddresses: nil, // Will be set by SetSMQBrokerAddresses() when server starts
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
			Warning("Failed to close broker client: %v", err)
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
	Debug("StoreRecordBatch delegated to SeaweedMQ handler - partition:%d, offset:%d",
		partition, baseOffset)
}

// GetRecordBatch retrieves a stored record batch that contains the requested offset
func (h *Handler) GetRecordBatch(topicName string, partition int32, offset int64) ([]byte, bool) {
	// Record batch retrieval is now handled by the SeaweedMQ handler
	Debug("GetRecordBatch delegated to SeaweedMQ handler - partition:%d, offset:%d",
		partition, offset)
	return nil, false
}

// SetSMQBrokerAddresses updates the SMQ broker addresses used in Metadata responses
func (h *Handler) SetSMQBrokerAddresses(brokerAddresses []string) {
	h.smqBrokerAddresses = brokerAddresses
}

// GetSMQBrokerAddresses returns the SMQ broker addresses
func (h *Handler) GetSMQBrokerAddresses() []string {
	// First try to get from the SeaweedMQ handler (preferred)
	if h.seaweedMQHandler != nil {
		if brokerAddresses := h.seaweedMQHandler.GetBrokerAddresses(); len(brokerAddresses) > 0 {
			return brokerAddresses
		}
	}

	// Fallback to manually set addresses
	if len(h.smqBrokerAddresses) > 0 {
		return h.smqBrokerAddresses
	}

	// Final fallback for testing
	return []string{"localhost:17777"}
}

// GetGatewayAddress returns the current gateway address as a string (for coordinator registry)
func (h *Handler) GetGatewayAddress() string {
	if h.gatewayAddress != "" {
		return h.gatewayAddress
	}
	// Fallback for testing
	return "localhost:9092"
}

// SetGatewayAddress sets the gateway address for coordinator registry
func (h *Handler) SetGatewayAddress(address string) {
	h.gatewayAddress = address
}

// SetCoordinatorRegistry sets the coordinator registry for this handler
func (h *Handler) SetCoordinatorRegistry(registry CoordinatorRegistryInterface) {
	h.coordinatorRegistry = registry
}

// GetCoordinatorRegistry returns the coordinator registry
func (h *Handler) GetCoordinatorRegistry() CoordinatorRegistryInterface {
	return h.coordinatorRegistry
}

// parseBrokerAddress parses a broker address string (host:port) into host and port
func (h *Handler) parseBrokerAddress(address string) (host string, port int, err error) {
	// Split by the last colon to handle IPv6 addresses
	lastColon := strings.LastIndex(address, ":")
	if lastColon == -1 {
		return "", 0, fmt.Errorf("invalid broker address format: %s", address)
	}

	host = address[:lastColon]
	portStr := address[lastColon+1:]

	port, err = strconv.Atoi(portStr)
	if err != nil {
		return "", 0, fmt.Errorf("invalid port in broker address %s: %v", address, err)
	}

	return host, port, nil
}

// grpcAddressToHttpAddress converts a gRPC address back to HTTP address
// e.g., "127.0.0.1:18888" -> "127.0.0.1:8888" (subtracts 10000 from port)
func grpcAddressToHttpAddress(grpcAddress string) string {
	lastColon := strings.LastIndex(grpcAddress, ":")
	if lastColon == -1 {
		return grpcAddress // Return as-is if no port found
	}

	host := grpcAddress[:lastColon]
	portStr := grpcAddress[lastColon+1:]

	port, err := strconv.Atoi(portStr)
	if err != nil {
		return grpcAddress // Return as-is if port parsing fails
	}

	httpPort := port - 10000
	if httpPort <= 0 {
		return grpcAddress // Return as-is if result would be invalid
	}

	return net.JoinHostPort(host, strconv.Itoa(httpPort))
}

// HandleConn processes a single client connection
func (h *Handler) HandleConn(ctx context.Context, conn net.Conn) error {
	connectionID := fmt.Sprintf("%s->%s", conn.RemoteAddr(), conn.LocalAddr())

	// Record connection metrics
	RecordConnectionMetrics()

	// Set connection context for this connection
	h.connContext = &ConnectionContext{
		RemoteAddr:   conn.RemoteAddr(),
		LocalAddr:    conn.LocalAddr(),
		ConnectionID: connectionID,
	}

	defer func() {
		Debug("[%s] Connection closing", connectionID)
		RecordDisconnectionMetrics()
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
			Debug("[%s] Context cancelled, closing connection", connectionID)
			return ctx.Err()
		default:
		}

		// Set a read deadline for the connection based on context or default timeout
		var readDeadline time.Time
		var timeoutDuration time.Duration

		if deadline, ok := ctx.Deadline(); ok {
			readDeadline = deadline
			timeoutDuration = time.Until(deadline)
			Debug("[%s] Using context deadline: %v", connectionID, timeoutDuration)
		} else {
			// Use configurable read timeout instead of hardcoded 5 seconds
			timeoutDuration = timeoutConfig.ReadTimeout
			readDeadline = time.Now().Add(timeoutDuration)
			Debug("[%s] Using config timeout: %v", connectionID, timeoutDuration)
		}

		if err := conn.SetReadDeadline(readDeadline); err != nil {
			Debug("[%s] Failed to set read deadline: %v", connectionID, err)
			return fmt.Errorf("set read deadline: %w", err)
		}

		// Check context before reading
		select {
		case <-ctx.Done():
			Debug("[%s] Context cancelled before reading message header", connectionID)
			// Give a small delay to ensure proper cleanup
			time.Sleep(100 * time.Millisecond)
			return ctx.Err()
		default:
			// If context is close to being cancelled, set a very short timeout
			if deadline, ok := ctx.Deadline(); ok {
				timeUntilDeadline := time.Until(deadline)
				if timeUntilDeadline < 2*time.Second && timeUntilDeadline > 0 {
					shortDeadline := time.Now().Add(500 * time.Millisecond)
					if err := conn.SetReadDeadline(shortDeadline); err == nil {
						Debug("[%s] Context deadline approaching, using 500ms timeout", connectionID)
					}
				}
			}
		}

		// Read message size (4 bytes)
		Debug("[%s] About to read message size header", connectionID)
		var sizeBytes [4]byte
		if _, err := io.ReadFull(r, sizeBytes[:]); err != nil {
			if err == io.EOF {
				Debug("[%s] Client closed connection (clean EOF)", connectionID)
				return nil
			}
			if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
				// Idle timeout while waiting for next request; keep connection open
				Debug("[%s] Read timeout waiting for request, continuing", connectionID)
				continue
			}
			Debug("[%s] Read error: %v", connectionID, err)
			return fmt.Errorf("read message size: %w", err)
		}

		// Successfully read the message size
		size := binary.BigEndian.Uint32(sizeBytes[:])
		Debug("[%s] Read message size header: %d bytes", connectionID, size)
		if size == 0 || size > 1024*1024 { // 1MB limit
			// Use standardized error for message size limit
			Debug("[%s] Invalid message size: %d (limit: 1MB)", connectionID, size)
			// Send error response for message too large
			errorResponse := BuildErrorResponse(0, ErrorCodeMessageTooLarge) // correlation ID 0 since we can't parse it yet
			if writeErr := h.writeResponseWithTimeout(w, errorResponse, timeoutConfig.WriteTimeout); writeErr != nil {
				Debug("[%s] Failed to send message too large response: %v", connectionID, writeErr)
			}
			return fmt.Errorf("message size %d exceeds limit", size)
		}

		// Set read deadline for message body
		if err := conn.SetReadDeadline(time.Now().Add(timeoutConfig.ReadTimeout)); err != nil {
			Debug("[%s] Failed to set message read deadline: %v", connectionID, err)
		}

		// Read the message
		messageBuf := make([]byte, size)
		if _, err := io.ReadFull(r, messageBuf); err != nil {
			errorCode := HandleTimeoutError(err, "read")
			Debug("[%s] Error reading message body: %v (code: %d)", connectionID, err, errorCode)
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
				Debug("[%s] Failed to send unsupported version response: %v", connectionID, writeErr)
				return fmt.Errorf("send error response: %w", writeErr)
			}
			continue
		}

		// Parse header using flexible version utilities for validation and client ID extraction
		header, requestBody, parseErr := ParseRequestHeader(messageBuf)
		if parseErr != nil {
			// Fall back to basic header parsing if flexible version parsing fails
			Debug("Flexible header parsing failed, using basic parsing: %v", parseErr)

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
				Debug("Header parsing mismatch - using basic parsing as fallback")
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
				Debug("Client ID: %s", *header.ClientID)
			}
		}

		// Handle the request based on API key and version
		var response []byte
		var err error

		// Record request start time for latency tracking
		requestStart := time.Now()

		Debug("API REQUEST - Key: %d (%s), Version: %d, Correlation: %d", apiKey, getAPIName(apiKey), apiVersion, correlationID)
		switch apiKey {
		case 18: // ApiVersions
			Debug("-> ApiVersions v%d", apiVersion)
			response, err = h.handleApiVersions(correlationID, apiVersion)
		case 3: // Metadata
			Debug("-> Metadata v%d", apiVersion)
			response, err = h.handleMetadata(correlationID, apiVersion, requestBody)
		case 2: // ListOffsets
			Debug("*** LISTOFFSETS REQUEST RECEIVED *** Correlation: %d, Version: %d", correlationID, apiVersion)
			response, err = h.handleListOffsets(correlationID, apiVersion, requestBody)
		case 19: // CreateTopics
			response, err = h.handleCreateTopics(correlationID, apiVersion, requestBody)
		case 20: // DeleteTopics
			response, err = h.handleDeleteTopics(correlationID, requestBody)
		case 0: // Produce
			Debug("-> Produce v%d", apiVersion)
			response, err = h.handleProduce(correlationID, apiVersion, requestBody)
		case 1: // Fetch
			Debug("-> Fetch v%d", apiVersion)
			response, err = h.handleFetch(ctx, correlationID, apiVersion, requestBody)
		case 11: // JoinGroup
			Debug("-> JoinGroup v%d", apiVersion)
			response, err = h.handleJoinGroup(correlationID, apiVersion, requestBody)
		case 14: // SyncGroup
			Debug("-> SyncGroup v%d", apiVersion)
			response, err = h.handleSyncGroup(correlationID, apiVersion, requestBody)
		case 8: // OffsetCommit
			Debug("-> OffsetCommit")
			response, err = h.handleOffsetCommit(correlationID, requestBody)
		case 9: // OffsetFetch
			Debug("-> OffsetFetch v%d", apiVersion)
			response, err = h.handleOffsetFetch(correlationID, apiVersion, requestBody)
		case 10: // FindCoordinator
			Debug("-> FindCoordinator v%d", apiVersion)
			response, err = h.handleFindCoordinator(correlationID, apiVersion, requestBody)
		case 12: // Heartbeat
			Debug("-> Heartbeat v%d", apiVersion)
			response, err = h.handleHeartbeat(correlationID, requestBody)
		case 13: // LeaveGroup
			response, err = h.handleLeaveGroup(correlationID, apiVersion, requestBody)
		case 15: // DescribeGroups
			Debug("DescribeGroups request received, correlation: %d, version: %d", correlationID, apiVersion)
			response, err = h.handleDescribeGroups(correlationID, apiVersion, requestBody)
		case 16: // ListGroups
			Debug("ListGroups request received, correlation: %d, version: %d", correlationID, apiVersion)
			response, err = h.handleListGroups(correlationID, apiVersion, requestBody)
		case 32: // DescribeConfigs
			Debug("DescribeConfigs request received, correlation: %d, version: %d", correlationID, apiVersion)
			response, err = h.handleDescribeConfigs(correlationID, apiVersion, requestBody)
		default:
			Warning("Unsupported API key: %d (%s) v%d - Correlation: %d", apiKey, apiName, apiVersion, correlationID)
			err = fmt.Errorf("unsupported API key: %d (version %d)", apiKey, apiVersion)
		}

		// Record metrics based on success/error
		requestLatency := time.Since(requestStart)
		if err != nil {
			RecordErrorMetrics(apiKey, requestLatency)
			return fmt.Errorf("handle request: %w", err)
		}

		// Send response with timeout handling
		Debug("[%s] Sending %s response: %d bytes", connectionID, getAPIName(apiKey), len(response))
		if err := h.writeResponseWithTimeout(w, response, timeoutConfig.WriteTimeout); err != nil {
			errorCode := HandleTimeoutError(err, "write")
			Error("[%s] Error sending response: %v (code: %d)", connectionID, err, errorCode)
			RecordErrorMetrics(apiKey, requestLatency)
			return fmt.Errorf("send response: %w", err)
		}

		// Record successful request metrics
		RecordRequestMetrics(apiKey, requestLatency)

		// Minimal flush logging
		// Debug("API %d flushed", apiKey)
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
	apiKeysCount := uint32(16)
	if isFlexible {
		// Compact array format for flexible versions
		response = append(response, CompactArrayLength(apiKeysCount)...)
	} else {
		// Regular array format for older versions
		response = append(response, 0, 0, 0, 16) // 16 API keys
	}

	// API Key 18 (ApiVersions): api_key(2) + min_version(2) + max_version(2)
	response = append(response, 0, 18) // API key 18
	response = append(response, 0, 0)  // min version 0
	response = append(response, 0, 3)  // max version 3
	if isFlexible {
		// per-element tagged fields (empty)
		response = append(response, 0)
	}

	// API Key 3 (Metadata): api_key(2) + min_version(2) + max_version(2)
	response = append(response, 0, 3) // API key 3
	response = append(response, 0, 0) // min version 0
	response = append(response, 0, 7) // max version 7
	if isFlexible {
		response = append(response, 0)
	}

	// API Key 2 (ListOffsets): limit to v2 (implemented and tested)
	response = append(response, 0, 2) // API key 2
	response = append(response, 0, 0) // min version 0
	response = append(response, 0, 2) // max version 2
	if isFlexible {
		response = append(response, 0)
	}

	// API Key 19 (CreateTopics): api_key(2) + min_version(2) + max_version(2)
	response = append(response, 0, 19) // API key 19
	response = append(response, 0, 0)  // min version 0
	response = append(response, 0, 5)  // max version 5
	if isFlexible {
		response = append(response, 0)
	}

	// API Key 20 (DeleteTopics): api_key(2) + min_version(2) + max_version(2)
	response = append(response, 0, 20) // API key 20
	response = append(response, 0, 0)  // min version 0
	response = append(response, 0, 4)  // max version 4
	if isFlexible {
		response = append(response, 0)
	}

	// API Key 0 (Produce): api_key(2) + min_version(2) + max_version(2)
	// Support v7 for Sarama compatibility (Kafka 2.1.0)
	response = append(response, 0, 0) // API key 0
	response = append(response, 0, 0) // min version 0
	response = append(response, 0, 7) // max version 7
	if isFlexible {
		response = append(response, 0)
	}

	// API Key 1 (Fetch): limit to v7 (current handler semantics)
	response = append(response, 0, 1) // API key 1
	response = append(response, 0, 0) // min version 0
	response = append(response, 0, 7) // max version 7
	if isFlexible {
		response = append(response, 0)
	}

	// API Key 11 (JoinGroup): support v0-v9 (latest Kafka protocol version)
	response = append(response, 0, 11) // API key 11
	response = append(response, 0, 0)  // min version 0
	response = append(response, 0, 9)  // max version 9
	if isFlexible {
		response = append(response, 0)
	}

	// API Key 14 (SyncGroup): support v0-v5 (latest Kafka protocol version)
	response = append(response, 0, 14) // API key 14
	response = append(response, 0, 0)  // min version 0
	response = append(response, 0, 5)  // max version 5
	if isFlexible {
		response = append(response, 0)
	}

	// API Key 8 (OffsetCommit): limit to v2 for current implementation
	response = append(response, 0, 8) // API key 8
	response = append(response, 0, 0) // min version 0
	response = append(response, 0, 2) // max version 2
	if isFlexible {
		response = append(response, 0)
	}

	// API Key 9 (OffsetFetch): supports up to v5 (with leader epoch and throttle time)
	response = append(response, 0, 9) // API key 9
	response = append(response, 0, 0) // min version 0
	response = append(response, 0, 5) // max version 5
	if isFlexible {
		response = append(response, 0)
	}

	// API Key 10 (FindCoordinator): limit to v2 (implemented)
	response = append(response, 0, 10) // API key 10
	response = append(response, 0, 0)  // min version 0
	response = append(response, 0, 2)  // max version 2
	if isFlexible {
		response = append(response, 0)
	}

	// API Key 12 (Heartbeat): api_key(2) + min_version(2) + max_version(2)
	response = append(response, 0, 12) // API key 12
	response = append(response, 0, 0)  // min version 0
	response = append(response, 0, 4)  // max version 4
	if isFlexible {
		response = append(response, 0)
	}

	// API Key 13 (LeaveGroup): api_key(2) + min_version(2) + max_version(2)
	response = append(response, 0, 13) // API key 13
	response = append(response, 0, 0)  // min version 0
	response = append(response, 0, 4)  // max version 4
	if isFlexible {
		response = append(response, 0)
	}

	// API Key 15 (DescribeGroups): api_key(2) + min_version(2) + max_version(2)
	response = append(response, 0, 15) // API key 15
	response = append(response, 0, 0)  // min version 0
	response = append(response, 0, 5)  // max version 5
	if isFlexible {
		response = append(response, 0)
	}

	// API Key 16 (ListGroups): api_key(2) + min_version(2) + max_version(2)
	response = append(response, 0, 16) // API key 16
	response = append(response, 0, 0)  // min version 0
	response = append(response, 0, 4)  // max version 4
	if isFlexible {
		response = append(response, 0)
	}

	// ApiVersions v1+ include throttle_time_ms
	if apiVersion >= 1 {
		response = append(response, 0, 0, 0, 0) // throttle_time_ms = 0
	}

	// Add tagged fields for flexible versions
	if isFlexible {
		// Empty tagged fields for now (response-level)
		response = append(response, 0)
	}

	Debug("ApiVersions v%d response: %d bytes", apiVersion, len(response))
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

	// Use gateway address for Kafka protocol compatibility
	gatewayAddr := h.GetGatewayAddress()
	host, port, err := h.parseGatewayAddress(gatewayAddr)
	if err != nil {
		Debug("Failed to parse gateway address %s: %v", gatewayAddr, err)
		// Fallback to default
		host, port = "localhost", 9092
	}
	Debug("Advertising Kafka gateway (v0) at %s:%d", host, port)

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
	Debug("🔍 METADATA v0 REQUEST - Requested: %v (empty=all)", requestedTopics)

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

		// Get actual partition count from topic info
		topicInfo, exists := h.seaweedMQHandler.GetTopicInfo(topicName)
		partitionCount := int32(1) // Default to 1 partition
		if exists && topicInfo != nil {
			partitionCount = topicInfo.Partitions
		}

		// partitions array length (4 bytes)
		partitionsBytes := make([]byte, 4)
		binary.BigEndian.PutUint32(partitionsBytes, uint32(partitionCount))
		response = append(response, partitionsBytes...)

		// Create partition entries for each partition
		for partitionID := int32(0); partitionID < partitionCount; partitionID++ {
			// partition: error_code(2) + partition_id(4) + leader(4)
			response = append(response, 0, 0) // error_code

			// partition_id (4 bytes)
			partitionIDBytes := make([]byte, 4)
			binary.BigEndian.PutUint32(partitionIDBytes, uint32(partitionID))
			response = append(response, partitionIDBytes...)

			response = append(response, 0, 0, 0, 1) // leader = 1 (this broker)

			// replicas: array length(4) + one broker id (1)
			response = append(response, 0, 0, 0, 1)
			response = append(response, 0, 0, 0, 1)

			// isr: array length(4) + one broker id (1)
			response = append(response, 0, 0, 0, 1)
			response = append(response, 0, 0, 0, 1)
		}
	}

	Debug("Metadata v0 response for %d topics: %v", len(topicsToReturn), topicsToReturn)
	Debug("*** METADATA v0 RESPONSE DETAILS ***")
	Debug("Response size: %d bytes", len(response))
	Debug("Kafka Gateway: %s", h.GetGatewayAddress())
	Debug("Topics: %v", topicsToReturn)
	for i, topic := range topicsToReturn {
		Debug("Topic[%d]: %s (1 partition)", i, topic)
	}
	Debug("*** END METADATA v0 RESPONSE ***")
	return response, nil
}

func (h *Handler) HandleMetadataV1(correlationID uint32, requestBody []byte) ([]byte, error) {
	// Simplified Metadata v1 implementation - based on working v0 + v1 additions
	// v1 adds: ControllerID (after brokers), Rack (for brokers), IsInternal (for topics)

	// Parse requested topics (empty means all)
	requestedTopics := h.parseMetadataTopics(requestBody)
	Debug("🔍 METADATA v1 REQUEST - Requested: %v (empty=all)", requestedTopics)

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

	// Use gateway address for Kafka protocol compatibility
	gatewayAddr := h.GetGatewayAddress()
	host, port, err := h.parseGatewayAddress(gatewayAddr)
	if err != nil {
		Debug("Failed to parse gateway address %s: %v", gatewayAddr, err)
		// Fallback to default
		host, port = "localhost", 9092
	}
	Debug("Advertising Kafka gateway (v1) at %s:%d", host, port)

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

		// Get actual partition count from topic info
		topicInfo, exists := h.seaweedMQHandler.GetTopicInfo(topicName)
		partitionCount := int32(1) // Default to 1 partition
		if exists && topicInfo != nil {
			partitionCount = topicInfo.Partitions
		}

		// partitions array length (4 bytes)
		partitionsBytes := make([]byte, 4)
		binary.BigEndian.PutUint32(partitionsBytes, uint32(partitionCount))
		response = append(response, partitionsBytes...)

		// Create partition entries for each partition
		for partitionID := int32(0); partitionID < partitionCount; partitionID++ {
			// partition: error_code(2) + partition_id(4) + leader_id(4) + replicas(ARRAY) + isr(ARRAY)
			response = append(response, 0, 0) // error_code

			// partition_id (4 bytes)
			partitionIDBytes := make([]byte, 4)
			binary.BigEndian.PutUint32(partitionIDBytes, uint32(partitionID))
			response = append(response, partitionIDBytes...)

			response = append(response, 0, 0, 0, 1) // leader_id = 1

			// replicas: array length(4) + one broker id (1)
			response = append(response, 0, 0, 0, 1)
			response = append(response, 0, 0, 0, 1)

			// isr: array length(4) + one broker id (1)
			response = append(response, 0, 0, 0, 1)
			response = append(response, 0, 0, 0, 1)
		}
	}

	Debug("Metadata v1 response for %d topics: %v", len(topicsToReturn), topicsToReturn)
	Debug("Metadata v1 response size: %d bytes", len(response))
	return response, nil
}

// HandleMetadataV2 implements Metadata API v2 with ClusterID field
func (h *Handler) HandleMetadataV2(correlationID uint32, requestBody []byte) ([]byte, error) {
	// Metadata v2 adds ClusterID field (nullable string)
	// v2 response layout: correlation_id(4) + brokers(ARRAY) + cluster_id(NULLABLE_STRING) + controller_id(4) + topics(ARRAY)

	// Parse requested topics (empty means all)
	requestedTopics := h.parseMetadataTopics(requestBody)
	Debug("🔍 METADATA v2 REQUEST - Requested: %v (empty=all)", requestedTopics)

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

	// Brokers array (4 bytes length + brokers) - 1 broker (this gateway)
	binary.Write(&buf, binary.BigEndian, int32(1))

	// Use gateway address for Kafka protocol compatibility
	gatewayAddr := h.GetGatewayAddress()
	host, port, err := h.parseGatewayAddress(gatewayAddr)
	if err != nil {
		Debug("Failed to parse gateway address %s: %v", gatewayAddr, err)
		// Fallback to default
		host, port = "localhost", 9092
	}
	Debug("Advertising Kafka gateway (v2) at %s:%d", host, port)

	nodeID := int32(1) // Single gateway node

	// Broker: node_id(4) + host(STRING) + port(4) + rack(STRING) + cluster_id(NULLABLE_STRING)
	binary.Write(&buf, binary.BigEndian, nodeID)

	// Host (STRING: 2 bytes length + data)
	binary.Write(&buf, binary.BigEndian, int16(len(host)))
	buf.WriteString(host)

	// Port (4 bytes)
	binary.Write(&buf, binary.BigEndian, int32(port))

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

		// Get actual partition count from topic info
		topicInfo, exists := h.seaweedMQHandler.GetTopicInfo(topicName)
		partitionCount := int32(1) // Default to 1 partition
		if exists && topicInfo != nil {
			partitionCount = topicInfo.Partitions
		}

		// Partitions array (4 bytes length + partitions)
		binary.Write(&buf, binary.BigEndian, partitionCount)

		// Create partition entries for each partition
		for partitionID := int32(0); partitionID < partitionCount; partitionID++ {
			binary.Write(&buf, binary.BigEndian, int16(0))    // ErrorCode
			binary.Write(&buf, binary.BigEndian, partitionID) // PartitionIndex
			binary.Write(&buf, binary.BigEndian, int32(1))    // LeaderID

			// ReplicaNodes array (4 bytes length + nodes)
			binary.Write(&buf, binary.BigEndian, int32(1)) // 1 replica
			binary.Write(&buf, binary.BigEndian, int32(1)) // NodeID 1

			// IsrNodes array (4 bytes length + nodes)
			binary.Write(&buf, binary.BigEndian, int32(1)) // 1 ISR node
			binary.Write(&buf, binary.BigEndian, int32(1)) // NodeID 1
		}
	}

	response := buf.Bytes()
	Debug("Advertising Kafka gateway: %s", h.GetGatewayAddress())
	Debug("Metadata v2 response for %d topics: %v", len(topicsToReturn), topicsToReturn)

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

	// Brokers array (4 bytes length + brokers) - 1 broker (this gateway)
	binary.Write(&buf, binary.BigEndian, int32(1))

	// Use gateway address for Kafka protocol compatibility
	gatewayAddr := h.GetGatewayAddress()
	host, port, err := h.parseGatewayAddress(gatewayAddr)
	if err != nil {
		Debug("Failed to parse gateway address %s: %v", gatewayAddr, err)
		// Fallback to default
		host, port = "localhost", 9092
	}
	Debug("Advertising Kafka gateway (v3/v4) at %s:%d", host, port)

	nodeID := int32(1) // Single gateway node

	// Broker: node_id(4) + host(STRING) + port(4) + rack(STRING) + cluster_id(NULLABLE_STRING)
	binary.Write(&buf, binary.BigEndian, nodeID)

	// Host (STRING: 2 bytes length + data)
	binary.Write(&buf, binary.BigEndian, int16(len(host)))
	buf.WriteString(host)

	// Port (4 bytes)
	binary.Write(&buf, binary.BigEndian, int32(port))

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

		// Get actual partition count from topic info
		topicInfo, exists := h.seaweedMQHandler.GetTopicInfo(topicName)
		partitionCount := int32(1) // Default to 1 partition
		if exists && topicInfo != nil {
			partitionCount = topicInfo.Partitions
		}

		// Partitions array (4 bytes length + partitions)
		binary.Write(&buf, binary.BigEndian, partitionCount)

		// Create partition entries for each partition
		for partitionID := int32(0); partitionID < partitionCount; partitionID++ {
			binary.Write(&buf, binary.BigEndian, int16(0))    // ErrorCode
			binary.Write(&buf, binary.BigEndian, partitionID) // PartitionIndex
			binary.Write(&buf, binary.BigEndian, int32(1))    // LeaderID

			// ReplicaNodes array (4 bytes length + nodes)
			binary.Write(&buf, binary.BigEndian, int32(1)) // 1 replica
			binary.Write(&buf, binary.BigEndian, int32(1)) // NodeID 1

			// IsrNodes array (4 bytes length + nodes)
			binary.Write(&buf, binary.BigEndian, int32(1)) // 1 ISR node
			binary.Write(&buf, binary.BigEndian, int32(1)) // NodeID 1
		}
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
	Debug("🔍 METADATA v5/v6 REQUEST - Requested: %v (empty=all)", requestedTopics)

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

	// Brokers array (4 bytes length + brokers) - 1 broker (this gateway)
	binary.Write(&buf, binary.BigEndian, int32(1))

	// Use gateway address for Kafka protocol compatibility
	gatewayAddr := h.GetGatewayAddress()
	host, port, err := h.parseGatewayAddress(gatewayAddr)
	if err != nil {
		Debug("Failed to parse gateway address %s: %v", gatewayAddr, err)
		// Fallback to default
		host, port = "localhost", 9092
	}
	Debug("Advertising Kafka gateway (v5/v6) at %s:%d", host, port)

	nodeID := int32(1) // Single gateway node

	// Broker: node_id(4) + host(STRING) + port(4) + rack(STRING) + cluster_id(NULLABLE_STRING)
	binary.Write(&buf, binary.BigEndian, nodeID)

	// Host (STRING: 2 bytes length + data)
	binary.Write(&buf, binary.BigEndian, int16(len(host)))
	buf.WriteString(host)

	// Port (4 bytes)
	binary.Write(&buf, binary.BigEndian, int32(port))

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

		// Get actual partition count from topic info
		topicInfo, exists := h.seaweedMQHandler.GetTopicInfo(topicName)
		partitionCount := int32(1) // Default to 1 partition
		if exists && topicInfo != nil {
			partitionCount = topicInfo.Partitions
		}

		// Partitions array (4 bytes length + partitions)
		binary.Write(&buf, binary.BigEndian, partitionCount)

		// Create partition entries for each partition
		for partitionID := int32(0); partitionID < partitionCount; partitionID++ {
			binary.Write(&buf, binary.BigEndian, int16(0))    // ErrorCode
			binary.Write(&buf, binary.BigEndian, partitionID) // PartitionIndex
			binary.Write(&buf, binary.BigEndian, int32(1))    // LeaderID

			// ReplicaNodes array (4 bytes length + nodes)
			binary.Write(&buf, binary.BigEndian, int32(1)) // 1 replica
			binary.Write(&buf, binary.BigEndian, int32(1)) // NodeID 1

			// IsrNodes array (4 bytes length + nodes)
			binary.Write(&buf, binary.BigEndian, int32(1)) // 1 ISR node
			binary.Write(&buf, binary.BigEndian, int32(1)) // NodeID 1

			// OfflineReplicas array (4 bytes length + nodes) - v5+ addition
			binary.Write(&buf, binary.BigEndian, int32(0)) // No offline replicas
		}
	}

	response := buf.Bytes()
	Debug("Advertising Kafka gateway: %s", h.GetGatewayAddress())
	Debug("Metadata v5/v6 response for %d topics: %v", len(topicsToReturn), topicsToReturn)

	return response, nil
}

// HandleMetadataV7 implements Metadata API v7 with LeaderEpoch field
func (h *Handler) HandleMetadataV7(correlationID uint32, requestBody []byte) ([]byte, error) {
	// Metadata v7 adds LeaderEpoch field to partitions
	// v7 response layout: correlation_id(4) + throttle_time_ms(4) + brokers(ARRAY) + cluster_id(NULLABLE_STRING) + controller_id(4) + topics(ARRAY)
	// Each partition now includes: error_code(2) + partition_index(4) + leader_id(4) + leader_epoch(4) + replica_nodes(ARRAY) + isr_nodes(ARRAY) + offline_replicas(ARRAY)

	// Parse requested topics (empty means all)
	requestedTopics := h.parseMetadataTopics(requestBody)
	Debug("🔍 METADATA v7 REQUEST - Requested: %v (empty=all)", requestedTopics)

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

	// Brokers array (4 bytes length + brokers) - 1 broker (this gateway)
	binary.Write(&buf, binary.BigEndian, int32(1))

	// Use gateway address for Kafka protocol compatibility
	gatewayAddr := h.GetGatewayAddress()
	host, port, err := h.parseGatewayAddress(gatewayAddr)
	if err != nil {
		Debug("Failed to parse gateway address %s: %v", gatewayAddr, err)
		// Fallback to default
		host, port = "localhost", 9092
	}
	Debug("Advertising Kafka gateway (v7) at %s:%d", host, port)

	nodeID := int32(1) // Single gateway node

	// Broker: node_id(4) + host(STRING) + port(4) + rack(STRING) + cluster_id(NULLABLE_STRING)
	binary.Write(&buf, binary.BigEndian, nodeID)

	// Host (STRING: 2 bytes length + data)
	binary.Write(&buf, binary.BigEndian, int16(len(host)))
	buf.WriteString(host)

	// Port (4 bytes)
	binary.Write(&buf, binary.BigEndian, int32(port))

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

		// Get actual partition count from topic info
		topicInfo, exists := h.seaweedMQHandler.GetTopicInfo(topicName)
		partitionCount := int32(1) // Default to 1 partition
		if exists && topicInfo != nil {
			partitionCount = topicInfo.Partitions
		}

		// Partitions array (4 bytes length + partitions)
		binary.Write(&buf, binary.BigEndian, partitionCount)

		// Create partition entries for each partition
		for partitionID := int32(0); partitionID < partitionCount; partitionID++ {
			binary.Write(&buf, binary.BigEndian, int16(0))    // ErrorCode
			binary.Write(&buf, binary.BigEndian, partitionID) // PartitionIndex
			binary.Write(&buf, binary.BigEndian, int32(1))    // LeaderID

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
	}

	response := buf.Bytes()
	Debug("Advertising Kafka gateway: %s", h.GetGatewayAddress())
	Debug("Metadata v7 response for %d topics: %v", len(topicsToReturn), topicsToReturn)

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
	Debug("ListOffsets v%d request hex dump (first 100 bytes): %x", apiVersion, requestBody[:min(100, len(requestBody))])

	// Parse minimal request to understand what's being asked (header already stripped)
	offset := 0

	// v1+ has replica_id(4)
	if apiVersion >= 1 {
		if len(requestBody) < offset+4 {
			return nil, fmt.Errorf("ListOffsets v%d request missing replica_id", apiVersion)
		}
		replicaID := int32(binary.BigEndian.Uint32(requestBody[offset : offset+4]))
		offset += 4
		Debug("ListOffsets v%d - replica_id: %d", apiVersion, replicaID)
	}

	// v2+ adds isolation_level(1)
	if apiVersion >= 2 {
		if len(requestBody) < offset+1 {
			return nil, fmt.Errorf("ListOffsets v%d request missing isolation_level", apiVersion)
		}
		isolationLevel := requestBody[offset]
		offset += 1
		Debug("ListOffsets v%d - isolation_level: %d", apiVersion, isolationLevel)
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
	Debug("*** CREATETOPICS REQUEST RECEIVED *** Correlation: %d, Version: %d", correlationID, apiVersion)
	Debug("CreateTopics - Request body size: %d bytes", len(requestBody))

	if len(requestBody) < 2 {
		return nil, fmt.Errorf("CreateTopics request too short")
	}

	// Parse based on API version
	switch apiVersion {
	case 0, 1:
		Debug("CreateTopics - Routing to v0/v1 handler")
		response, err := h.handleCreateTopicsV0V1(correlationID, requestBody)
		Debug("CreateTopics - v0/v1 handler returned, response size: %d bytes, err: %v", len(response), err)
		return response, err
	case 2, 3, 4:
		// kafka-go sends v2-4 in regular format, not compact
		Debug("CreateTopics - Routing to v2-4 handler")
		response, err := h.handleCreateTopicsV2To4(correlationID, requestBody)
		Debug("CreateTopics - v2-4 handler returned, response size: %d bytes, err: %v", len(response), err)
		return response, err
	case 5:
		// v5+ uses flexible format with compact arrays
		Debug("CreateTopics - Routing to v5+ handler")
		response, err := h.handleCreateTopicsV2Plus(correlationID, apiVersion, requestBody)
		Debug("CreateTopics - v5+ handler returned, response size: %d bytes, err: %v", len(response), err)
		return response, err
	default:
		return nil, fmt.Errorf("unsupported CreateTopics API version: %d", apiVersion)
	}
}

// handleCreateTopicsV2To4 handles CreateTopics API versions 2-4 (auto-detect regular vs compact format)
func (h *Handler) handleCreateTopicsV2To4(correlationID uint32, requestBody []byte) ([]byte, error) {
	// Auto-detect format: kafka-go sends regular format, tests send compact format
	if len(requestBody) < 1 {
		return nil, fmt.Errorf("CreateTopics v2-4 request too short")
	}

	// Detect format by checking first byte
	// Compact format: first byte is compact array length (usually 0x02 for 1 topic)
	// Regular format: first 4 bytes are regular array count (usually 0x00000001 for 1 topic)
	isCompactFormat := false
	if len(requestBody) >= 4 {
		// Check if this looks like a regular 4-byte array count
		regularCount := binary.BigEndian.Uint32(requestBody[0:4])
		// If the "regular count" is very large (> 1000), it's probably compact format
		// Also check if first byte is small (typical compact array length)
		if regularCount > 1000 || (requestBody[0] <= 10 && requestBody[0] > 0) {
			isCompactFormat = true
		}
	} else if requestBody[0] <= 10 && requestBody[0] > 0 {
		isCompactFormat = true
	}

	if isCompactFormat {
		Debug("CreateTopics v2-4 - Detected compact format")
		// Delegate to the compact format handler
		response, err := h.handleCreateTopicsV2Plus(correlationID, 2, requestBody)
		Debug("CreateTopics v2-4 - Compact format handler returned, response size: %d bytes, err: %v", len(response), err)
		return response, err
	}

	Debug("CreateTopics v2-4 - Detected regular format")
	// Handle regular format
	offset := 0
	if len(requestBody) < offset+4 {
		return nil, fmt.Errorf("CreateTopics v2-4 request too short for topics array")
	}

	topicsCount := binary.BigEndian.Uint32(requestBody[offset : offset+4])
	offset += 4
	Debug("CreateTopics v2-4 - Topics count: %d, remaining bytes: %d", topicsCount, len(requestBody)-offset)

	// Parse topics
	topics := make([]struct {
		name        string
		partitions  uint32
		replication uint16
	}, 0, topicsCount)
	for i := uint32(0); i < topicsCount; i++ {
		if len(requestBody) < offset+2 {
			return nil, fmt.Errorf("CreateTopics v2-4: truncated topic name length")
		}
		nameLen := binary.BigEndian.Uint16(requestBody[offset : offset+2])
		offset += 2
		if len(requestBody) < offset+int(nameLen) {
			return nil, fmt.Errorf("CreateTopics v2-4: truncated topic name")
		}
		topicName := string(requestBody[offset : offset+int(nameLen)])
		offset += int(nameLen)

		if len(requestBody) < offset+4 {
			return nil, fmt.Errorf("CreateTopics v2-4: truncated num_partitions")
		}
		numPartitions := binary.BigEndian.Uint32(requestBody[offset : offset+4])
		offset += 4

		if len(requestBody) < offset+2 {
			return nil, fmt.Errorf("CreateTopics v2-4: truncated replication_factor")
		}
		replication := binary.BigEndian.Uint16(requestBody[offset : offset+2])
		offset += 2

		// Assignments array (array of partition assignments) - skip contents
		if len(requestBody) < offset+4 {
			return nil, fmt.Errorf("CreateTopics v2-4: truncated assignments count")
		}
		assignments := binary.BigEndian.Uint32(requestBody[offset : offset+4])
		offset += 4
		for j := uint32(0); j < assignments; j++ {
			// partition_id (int32) + replicas (array int32)
			if len(requestBody) < offset+4 {
				return nil, fmt.Errorf("CreateTopics v2-4: truncated assignment partition id")
			}
			offset += 4
			if len(requestBody) < offset+4 {
				return nil, fmt.Errorf("CreateTopics v2-4: truncated replicas count")
			}
			replicasCount := binary.BigEndian.Uint32(requestBody[offset : offset+4])
			offset += 4
			// skip replica ids
			offset += int(replicasCount) * 4
		}

		// Configs array (array of (name,value) strings) - skip contents
		if len(requestBody) < offset+4 {
			return nil, fmt.Errorf("CreateTopics v2-4: truncated configs count")
		}
		configs := binary.BigEndian.Uint32(requestBody[offset : offset+4])
		offset += 4
		for j := uint32(0); j < configs; j++ {
			// name (string)
			if len(requestBody) < offset+2 {
				return nil, fmt.Errorf("CreateTopics v2-4: truncated config name length")
			}
			nameLen := binary.BigEndian.Uint16(requestBody[offset : offset+2])
			offset += 2 + int(nameLen)
			// value (nullable string)
			if len(requestBody) < offset+2 {
				return nil, fmt.Errorf("CreateTopics v2-4: truncated config value length")
			}
			valueLen := int16(binary.BigEndian.Uint16(requestBody[offset : offset+2]))
			offset += 2
			if valueLen >= 0 {
				offset += int(valueLen)
			}
		}

		topics = append(topics, struct {
			name        string
			partitions  uint32
			replication uint16
		}{topicName, numPartitions, replication})
	}

	// timeout_ms
	if len(requestBody) >= offset+4 {
		_ = binary.BigEndian.Uint32(requestBody[offset : offset+4])
		offset += 4
	}
	// validate_only (boolean)
	if len(requestBody) >= offset+1 {
		_ = requestBody[offset]
		offset += 1
	}

	// Build response
	response := make([]byte, 0, 128)
	// Correlation ID
	cid := make([]byte, 4)
	binary.BigEndian.PutUint32(cid, correlationID)
	response = append(response, cid...)
	// throttle_time_ms (4 bytes)
	response = append(response, 0, 0, 0, 0)
	// topics array count (int32)
	countBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(countBytes, uint32(len(topics)))
	response = append(response, countBytes...)
	// per-topic responses
	for _, t := range topics {
		// topic name (string)
		nameLen := make([]byte, 2)
		binary.BigEndian.PutUint16(nameLen, uint16(len(t.name)))
		response = append(response, nameLen...)
		response = append(response, []byte(t.name)...)
		// error_code (int16)
		var errCode uint16 = 0
		if h.seaweedMQHandler.TopicExists(t.name) {
			errCode = 36 // TOPIC_ALREADY_EXISTS
		} else if t.partitions == 0 {
			errCode = 37 // INVALID_PARTITIONS
		} else if t.replication == 0 {
			errCode = 38 // INVALID_REPLICATION_FACTOR
		} else {
			if err := h.seaweedMQHandler.CreateTopic(t.name, int32(t.partitions)); err != nil {
				errCode = 1 // UNKNOWN_SERVER_ERROR
			}
		}
		eb := make([]byte, 2)
		binary.BigEndian.PutUint16(eb, errCode)
		response = append(response, eb...)
		// error_message (nullable string) -> null
		response = append(response, 0xFF, 0xFF)
	}

	Debug("CreateTopics v2-4 - Regular format handler completed, response size: %d bytes", len(response))
	return response, nil
}

func (h *Handler) handleCreateTopicsV0V1(correlationID uint32, requestBody []byte) ([]byte, error) {
	Debug("CreateTopics v0/v1 - parsing request of %d bytes", len(requestBody))

	if len(requestBody) < 4 {
		return nil, fmt.Errorf("CreateTopics v0/v1 request too short")
	}

	offset := 0

	// Parse topics array (regular array format: count + topics)
	topicsCount := binary.BigEndian.Uint32(requestBody[offset : offset+4])
	offset += 4

	Debug("CreateTopics v0/v1 - Topics count: %d", topicsCount)

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

		Debug("CreateTopics v0/v1 - Parsed topic: %s, partitions: %d, replication: %d",
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
		Debug("CreateTopics v0/v1 - timeout_ms: %d", timeoutMs)
		offset += 4
	}

	// Parse validate_only (1 byte) - only in v1
	if len(requestBody) >= offset+1 {
		validateOnly := requestBody[offset] != 0
		Debug("CreateTopics v0/v1 - validate_only: %v", validateOnly)
	}

	return response, nil
}

// handleCreateTopicsV2Plus handles CreateTopics API versions 2+ (flexible versions with compact arrays/strings)
// For simplicity and consistency with existing response builder, this parses the flexible request,
// converts it into the non-flexible v2-v4 body format, and reuses handleCreateTopicsV2To4 to build the response.
func (h *Handler) handleCreateTopicsV2Plus(correlationID uint32, apiVersion uint16, requestBody []byte) ([]byte, error) {
	Debug("CreateTopics V2+ (flexible) - parsing request of %d bytes (version %d)", len(requestBody), apiVersion)

	offset := 0

	// Topics (compact array)
	topicsCount, consumed, err := DecodeCompactArrayLength(requestBody[offset:])
	if err != nil {
		return nil, fmt.Errorf("CreateTopics v%d: decode topics compact array: %w", apiVersion, err)
	}
	offset += consumed

	type topicSpec struct {
		name        string
		partitions  uint32
		replication uint16
	}
	topics := make([]topicSpec, 0, topicsCount)

	for i := uint32(0); i < topicsCount; i++ {
		// Topic name (compact string)
		name, consumed, err := DecodeFlexibleString(requestBody[offset:])
		if err != nil {
			return nil, fmt.Errorf("CreateTopics v%d: decode topic[%d] name: %w", apiVersion, i, err)
		}
		offset += consumed

		if len(requestBody) < offset+6 {
			return nil, fmt.Errorf("CreateTopics v%d: truncated partitions/replication for topic[%d]", apiVersion, i)
		}

		partitions := binary.BigEndian.Uint32(requestBody[offset : offset+4])
		offset += 4
		replication := binary.BigEndian.Uint16(requestBody[offset : offset+2])
		offset += 2

		// Configs (compact array) - skip entries
		cfgCount, consumed, err := DecodeCompactArrayLength(requestBody[offset:])
		if err != nil {
			return nil, fmt.Errorf("CreateTopics v%d: decode topic[%d] configs array: %w", apiVersion, i, err)
		}
		offset += consumed

		for j := uint32(0); j < cfgCount; j++ {
			// name (compact string)
			_, consumed, err := DecodeFlexibleString(requestBody[offset:])
			if err != nil {
				return nil, fmt.Errorf("CreateTopics v%d: decode topic[%d] config[%d] name: %w", apiVersion, i, j, err)
			}
			offset += consumed

			// value (nullable compact string)
			_, consumed, err = DecodeFlexibleString(requestBody[offset:])
			if err != nil {
				return nil, fmt.Errorf("CreateTopics v%d: decode topic[%d] config[%d] value: %w", apiVersion, i, j, err)
			}
			offset += consumed

			// tagged fields for each config
			_, consumed, err = DecodeTaggedFields(requestBody[offset:])
			if err != nil {
				return nil, fmt.Errorf("CreateTopics v%d: decode topic[%d] config[%d] tagged fields: %w", apiVersion, i, j, err)
			}
			offset += consumed
		}

		// Tagged fields for topic
		_, consumed, err = DecodeTaggedFields(requestBody[offset:])
		if err != nil {
			return nil, fmt.Errorf("CreateTopics v%d: decode topic[%d] tagged fields: %w", apiVersion, i, err)
		}
		offset += consumed

		topics = append(topics, topicSpec{name: name, partitions: partitions, replication: replication})
	}

	// timeout_ms (int32)
	if len(requestBody) < offset+4 {
		return nil, fmt.Errorf("CreateTopics v%d: missing timeout_ms", apiVersion)
	}
	timeoutMs := binary.BigEndian.Uint32(requestBody[offset : offset+4])
	offset += 4

	// validate_only (boolean)
	if len(requestBody) < offset+1 {
		return nil, fmt.Errorf("CreateTopics v%d: missing validate_only flag", apiVersion)
	}
	validateOnly := requestBody[offset] != 0
	offset += 1

	// Tagged fields (top-level)
	if _, consumed, err = DecodeTaggedFields(requestBody[offset:]); err != nil {
		return nil, fmt.Errorf("CreateTopics v%d: decode top-level tagged fields: %w", apiVersion, err)
	}
	// offset += consumed // Not needed further

	// Reconstruct a non-flexible v2-like request body and reuse existing handler
	// Format: topics(ARRAY) + timeout_ms(INT32) + validate_only(BOOLEAN)
	var legacyBody []byte

	// topics count (int32)
	legacyBody = append(legacyBody, 0, 0, 0, byte(len(topics)))
	if len(topics) > 0 {
		legacyBody[len(legacyBody)-1] = byte(len(topics))
	}

	for _, t := range topics {
		// topic name (STRING)
		nameLen := uint16(len(t.name))
		legacyBody = append(legacyBody, byte(nameLen>>8), byte(nameLen))
		legacyBody = append(legacyBody, []byte(t.name)...)

		// num_partitions (INT32)
		legacyBody = append(legacyBody, byte(t.partitions>>24), byte(t.partitions>>16), byte(t.partitions>>8), byte(t.partitions))

		// replication_factor (INT16)
		legacyBody = append(legacyBody, byte(t.replication>>8), byte(t.replication))

		// assignments array (INT32 count = 0)
		legacyBody = append(legacyBody, 0, 0, 0, 0)

		// configs array (INT32 count = 0)
		legacyBody = append(legacyBody, 0, 0, 0, 0)
	}

	// timeout_ms
	legacyBody = append(legacyBody, byte(timeoutMs>>24), byte(timeoutMs>>16), byte(timeoutMs>>8), byte(timeoutMs))

	// validate_only
	if validateOnly {
		legacyBody = append(legacyBody, 1)
	} else {
		legacyBody = append(legacyBody, 0)
	}

	// Build response directly instead of delegating to avoid circular dependency
	response := make([]byte, 0, 128)

	// Correlation ID
	cid := make([]byte, 4)
	binary.BigEndian.PutUint32(cid, correlationID)
	response = append(response, cid...)

	// throttle_time_ms (4 bytes)
	response = append(response, 0, 0, 0, 0)

	// topics array count (int32)
	countBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(countBytes, uint32(len(topics)))
	response = append(response, countBytes...)

	// For each topic
	for _, t := range topics {
		// topic name (string)
		response = append(response, 0, byte(len(t.name)))
		response = append(response, []byte(t.name)...)
		// error_code (int16)
		var errCode uint16 = 0
		if h.seaweedMQHandler.TopicExists(t.name) {
			errCode = 36 // TOPIC_ALREADY_EXISTS
		} else if t.partitions == 0 {
			errCode = 37 // INVALID_PARTITIONS
		} else if t.replication == 0 {
			errCode = 38 // INVALID_REPLICATION_FACTOR
		} else {
			if err := h.seaweedMQHandler.CreateTopic(t.name, int32(t.partitions)); err != nil {
				errCode = 1 // UNKNOWN_SERVER_ERROR
			}
		}
		eb := make([]byte, 2)
		binary.BigEndian.PutUint16(eb, errCode)
		response = append(response, eb...)
		// error_message (nullable string) -> null
		response = append(response, 0xFF, 0xFF)
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
		15: {0, 5}, // DescribeGroups: v0-v5
		16: {0, 4}, // ListGroups: v0-v4
		32: {0, 4}, // DescribeConfigs: v0-v4
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
	errorMsg := fmt.Sprintf("Unsupported version %d for API key", apiVersion)
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
	case 15:
		return "DescribeGroups"
	case 16:
		return "ListGroups"
	case 18:
		return "ApiVersions"
	case 19:
		return "CreateTopics"
	case 20:
		return "DeleteTopics"
	case 32:
		return "DescribeConfigs"
	default:
		return "Unknown"
	}
}

// handleDescribeConfigs handles DescribeConfigs API requests (API key 32)
func (h *Handler) handleDescribeConfigs(correlationID uint32, apiVersion uint16, requestBody []byte) ([]byte, error) {
	Debug("DescribeConfigs v%d - parsing request body (%d bytes)", apiVersion, len(requestBody))

	// Parse request to extract resources
	resources, err := h.parseDescribeConfigsRequest(requestBody)
	if err != nil {
		Error("DescribeConfigs parsing error: %v", err)
		return nil, fmt.Errorf("failed to parse DescribeConfigs request: %w", err)
	}

	Debug("DescribeConfigs parsed %d resources", len(resources))

	// Build response
	response := make([]byte, 0, 2048)

	// Correlation ID
	correlationIDBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(correlationIDBytes, correlationID)
	response = append(response, correlationIDBytes...)

	// Throttle time (0ms)
	throttleBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(throttleBytes, 0)
	response = append(response, throttleBytes...)

	// Resources array length
	resourcesBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(resourcesBytes, uint32(len(resources)))
	response = append(response, resourcesBytes...)

	// For each resource, return appropriate configs
	for _, resource := range resources {
		resourceResponse := h.buildDescribeConfigsResourceResponse(resource, apiVersion)
		response = append(response, resourceResponse...)
	}

	Debug("DescribeConfigs v%d response constructed, size: %d bytes", apiVersion, len(response))
	return response, nil
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

	offset := entries[0].KafkaOffset
	// Return the committed offset (metadata is not stored in SMQ format)
	return offset, "", nil
}

// DescribeConfigsResource represents a resource in a DescribeConfigs request
type DescribeConfigsResource struct {
	ResourceType int8 // 2 = Topic, 4 = Broker
	ResourceName string
	ConfigNames  []string // Empty means return all configs
}

// parseDescribeConfigsRequest parses a DescribeConfigs request body
func (h *Handler) parseDescribeConfigsRequest(requestBody []byte) ([]DescribeConfigsResource, error) {
	if len(requestBody) < 4 {
		return nil, fmt.Errorf("request too short")
	}

	offset := 0
	resourcesLength := int32(binary.BigEndian.Uint32(requestBody[offset : offset+4]))
	offset += 4

	// Validate resources length to prevent panic
	if resourcesLength < 0 || resourcesLength > 100 { // Reasonable limit
		return nil, fmt.Errorf("invalid resources length: %d", resourcesLength)
	}

	resources := make([]DescribeConfigsResource, 0, resourcesLength)

	for i := int32(0); i < resourcesLength; i++ {
		if offset+1 > len(requestBody) {
			return nil, fmt.Errorf("insufficient data for resource type")
		}

		// Resource type (1 byte)
		resourceType := int8(requestBody[offset])
		offset++

		// Resource name (string)
		if offset+2 > len(requestBody) {
			return nil, fmt.Errorf("insufficient data for resource name length")
		}
		nameLength := int(binary.BigEndian.Uint16(requestBody[offset : offset+2]))
		offset += 2

		// Validate name length to prevent panic
		if nameLength < 0 || nameLength > 1000 { // Reasonable limit
			return nil, fmt.Errorf("invalid resource name length: %d", nameLength)
		}

		if offset+nameLength > len(requestBody) {
			return nil, fmt.Errorf("insufficient data for resource name")
		}
		resourceName := string(requestBody[offset : offset+nameLength])
		offset += nameLength

		// Config names array (optional filter)
		if offset+4 > len(requestBody) {
			return nil, fmt.Errorf("insufficient data for config names length")
		}
		configNamesLength := int32(binary.BigEndian.Uint32(requestBody[offset : offset+4]))
		offset += 4

		// Validate config names length to prevent panic
		// Note: -1 means null/empty array in Kafka protocol
		if configNamesLength < -1 || configNamesLength > 1000 { // Reasonable limit
			return nil, fmt.Errorf("invalid config names length: %d", configNamesLength)
		}

		// Handle null array case
		if configNamesLength == -1 {
			configNamesLength = 0
		}

		configNames := make([]string, 0, configNamesLength)
		for j := int32(0); j < configNamesLength; j++ {
			if offset+2 > len(requestBody) {
				return nil, fmt.Errorf("insufficient data for config name length")
			}
			configNameLength := int(binary.BigEndian.Uint16(requestBody[offset : offset+2]))
			offset += 2

			// Validate config name length to prevent panic
			if configNameLength < 0 || configNameLength > 500 { // Reasonable limit
				return nil, fmt.Errorf("invalid config name length: %d", configNameLength)
			}

			if offset+configNameLength > len(requestBody) {
				return nil, fmt.Errorf("insufficient data for config name")
			}
			configName := string(requestBody[offset : offset+configNameLength])
			offset += configNameLength

			configNames = append(configNames, configName)
		}

		resources = append(resources, DescribeConfigsResource{
			ResourceType: resourceType,
			ResourceName: resourceName,
			ConfigNames:  configNames,
		})
	}

	return resources, nil
}

// buildDescribeConfigsResourceResponse builds the response for a single resource
func (h *Handler) buildDescribeConfigsResourceResponse(resource DescribeConfigsResource, apiVersion uint16) []byte {
	response := make([]byte, 0, 512)

	// Error code (0 = no error)
	errorCodeBytes := make([]byte, 2)
	binary.BigEndian.PutUint16(errorCodeBytes, 0)
	response = append(response, errorCodeBytes...)

	// Error message (null string = -1 length)
	errorMsgBytes := make([]byte, 2)
	binary.BigEndian.PutUint16(errorMsgBytes, 0xFFFF) // -1 as uint16
	response = append(response, errorMsgBytes...)

	// Resource type
	response = append(response, byte(resource.ResourceType))

	// Resource name
	nameBytes := make([]byte, 2+len(resource.ResourceName))
	binary.BigEndian.PutUint16(nameBytes[0:2], uint16(len(resource.ResourceName)))
	copy(nameBytes[2:], []byte(resource.ResourceName))
	response = append(response, nameBytes...)

	// Get configs for this resource
	configs := h.getConfigsForResource(resource)

	// Config entries array length
	configCountBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(configCountBytes, uint32(len(configs)))
	response = append(response, configCountBytes...)

	// Add each config entry
	for _, config := range configs {
		configBytes := h.buildConfigEntry(config, apiVersion)
		response = append(response, configBytes...)
	}

	return response
}

// ConfigEntry represents a single configuration entry
type ConfigEntry struct {
	Name      string
	Value     string
	ReadOnly  bool
	IsDefault bool
	Sensitive bool
}

// getConfigsForResource returns appropriate configs for a resource
func (h *Handler) getConfigsForResource(resource DescribeConfigsResource) []ConfigEntry {
	switch resource.ResourceType {
	case 2: // Topic
		return h.getTopicConfigs(resource.ResourceName, resource.ConfigNames)
	case 4: // Broker
		return h.getBrokerConfigs(resource.ConfigNames)
	default:
		return []ConfigEntry{}
	}
}

// getTopicConfigs returns topic-level configurations
func (h *Handler) getTopicConfigs(topicName string, requestedConfigs []string) []ConfigEntry {
	// Default topic configs that admin clients commonly request
	allConfigs := map[string]ConfigEntry{
		"cleanup.policy": {
			Name:      "cleanup.policy",
			Value:     "delete",
			ReadOnly:  false,
			IsDefault: true,
			Sensitive: false,
		},
		"retention.ms": {
			Name:      "retention.ms",
			Value:     "604800000", // 7 days in milliseconds
			ReadOnly:  false,
			IsDefault: true,
			Sensitive: false,
		},
		"retention.bytes": {
			Name:      "retention.bytes",
			Value:     "-1", // Unlimited
			ReadOnly:  false,
			IsDefault: true,
			Sensitive: false,
		},
		"segment.ms": {
			Name:      "segment.ms",
			Value:     "86400000", // 1 day in milliseconds
			ReadOnly:  false,
			IsDefault: true,
			Sensitive: false,
		},
		"max.message.bytes": {
			Name:      "max.message.bytes",
			Value:     "1048588", // ~1MB
			ReadOnly:  false,
			IsDefault: true,
			Sensitive: false,
		},
		"min.insync.replicas": {
			Name:      "min.insync.replicas",
			Value:     "1",
			ReadOnly:  false,
			IsDefault: true,
			Sensitive: false,
		},
	}

	// If specific configs requested, filter to those
	if len(requestedConfigs) > 0 {
		filteredConfigs := make([]ConfigEntry, 0, len(requestedConfigs))
		for _, configName := range requestedConfigs {
			if config, exists := allConfigs[configName]; exists {
				filteredConfigs = append(filteredConfigs, config)
			}
		}
		return filteredConfigs
	}

	// Return all configs
	configs := make([]ConfigEntry, 0, len(allConfigs))
	for _, config := range allConfigs {
		configs = append(configs, config)
	}
	return configs
}

// getBrokerConfigs returns broker-level configurations
func (h *Handler) getBrokerConfigs(requestedConfigs []string) []ConfigEntry {
	// Default broker configs that admin clients commonly request
	allConfigs := map[string]ConfigEntry{
		"log.retention.hours": {
			Name:      "log.retention.hours",
			Value:     "168", // 7 days
			ReadOnly:  false,
			IsDefault: true,
			Sensitive: false,
		},
		"log.segment.bytes": {
			Name:      "log.segment.bytes",
			Value:     "1073741824", // 1GB
			ReadOnly:  false,
			IsDefault: true,
			Sensitive: false,
		},
		"num.network.threads": {
			Name:      "num.network.threads",
			Value:     "3",
			ReadOnly:  true,
			IsDefault: true,
			Sensitive: false,
		},
		"num.io.threads": {
			Name:      "num.io.threads",
			Value:     "8",
			ReadOnly:  true,
			IsDefault: true,
			Sensitive: false,
		},
	}

	// If specific configs requested, filter to those
	if len(requestedConfigs) > 0 {
		filteredConfigs := make([]ConfigEntry, 0, len(requestedConfigs))
		for _, configName := range requestedConfigs {
			if config, exists := allConfigs[configName]; exists {
				filteredConfigs = append(filteredConfigs, config)
			}
		}
		return filteredConfigs
	}

	// Return all configs
	configs := make([]ConfigEntry, 0, len(allConfigs))
	for _, config := range allConfigs {
		configs = append(configs, config)
	}
	return configs
}

// buildConfigEntry builds the wire format for a single config entry
func (h *Handler) buildConfigEntry(config ConfigEntry, apiVersion uint16) []byte {
	entry := make([]byte, 0, 256)

	// Config name
	nameBytes := make([]byte, 2+len(config.Name))
	binary.BigEndian.PutUint16(nameBytes[0:2], uint16(len(config.Name)))
	copy(nameBytes[2:], []byte(config.Name))
	entry = append(entry, nameBytes...)

	// Config value
	valueBytes := make([]byte, 2+len(config.Value))
	binary.BigEndian.PutUint16(valueBytes[0:2], uint16(len(config.Value)))
	copy(valueBytes[2:], []byte(config.Value))
	entry = append(entry, valueBytes...)

	// Read only flag
	if config.ReadOnly {
		entry = append(entry, 1)
	} else {
		entry = append(entry, 0)
	}

	// Is default flag (only for version 0)
	if apiVersion == 0 {
		if config.IsDefault {
			entry = append(entry, 1)
		} else {
			entry = append(entry, 0)
		}
	}

	// Config source (for versions 1-3)
	if apiVersion >= 1 && apiVersion <= 3 {
		// ConfigSource: 1 = DYNAMIC_TOPIC_CONFIG, 2 = DYNAMIC_BROKER_CONFIG, 4 = STATIC_BROKER_CONFIG, 5 = DEFAULT_CONFIG
		configSource := int8(5) // DEFAULT_CONFIG for all our configs since they're defaults
		entry = append(entry, byte(configSource))
	}

	// Sensitive flag
	if config.Sensitive {
		entry = append(entry, 1)
	} else {
		entry = append(entry, 0)
	}

	// Config synonyms (for versions 1-3)
	if apiVersion >= 1 && apiVersion <= 3 {
		// Empty synonyms array (4 bytes for array length = 0)
		synonymsLength := make([]byte, 4)
		binary.BigEndian.PutUint32(synonymsLength, 0)
		entry = append(entry, synonymsLength...)
	}

	// Config type (for version 3 only)
	if apiVersion == 3 {
		configType := int8(1) // STRING type for all our configs
		entry = append(entry, byte(configType))
	}

	// Config documentation (for version 3 only)
	if apiVersion == 3 {
		// Null documentation (length = -1)
		docLength := make([]byte, 2)
		binary.BigEndian.PutUint16(docLength, 0xFFFF) // -1 as uint16
		entry = append(entry, docLength...)
	}

	return entry
}

// ProduceSchemaBasedRecord exposes the schema-based record production for testing
func (h *Handler) ProduceSchemaBasedRecord(topic string, partition int32, key []byte, value []byte) (int64, error) {
	return h.produceSchemaBasedRecord(topic, partition, key, value)
}

// DecodeRecordValueToKafkaMessage exposes the RecordValue decoding for testing
func (h *Handler) DecodeRecordValueToKafkaMessage(topicName string, recordValueBytes []byte) []byte {
	return h.decodeRecordValueToKafkaMessage(topicName, recordValueBytes)
}

// GetSeaweedMQHandler exposes the SeaweedMQ handler for testing
func (h *Handler) GetSeaweedMQHandler() SeaweedMQHandlerInterface {
	return h.seaweedMQHandler
}

// registerSchemaViaBrokerAPI registers the translated schema via the broker's ConfigureTopic API
// Only the gateway leader performs the registration to avoid concurrent updates.
func (h *Handler) registerSchemaViaBrokerAPI(topicName string, recordType *schema_pb.RecordType) error {
	if recordType == nil {
		return nil
	}

	// Gate by coordinator registry leadership
	if reg := h.GetCoordinatorRegistry(); reg != nil && !reg.IsLeader() {
		return nil
	}

	// Require SeaweedMQ integration to access broker
	if h.seaweedMQHandler == nil {
		return fmt.Errorf("no SeaweedMQ handler available for broker access")
	}

	// Get broker addresses
	brokerAddresses := h.seaweedMQHandler.GetBrokerAddresses()
	if len(brokerAddresses) == 0 {
		return fmt.Errorf("no broker addresses available")
	}

	// Use the first available broker
	brokerAddress := brokerAddresses[0]

	// Load security configuration
	util.LoadSecurityConfiguration()
	grpcDialOption := security.LoadClientTLS(util.GetViper(), "grpc.mq")

	// Get current topic configuration to preserve partition count
	seaweedTopic := &schema_pb.Topic{
		Namespace: DefaultKafkaNamespace,
		Name:      topicName,
	}

	return pb.WithBrokerGrpcClient(false, brokerAddress, grpcDialOption, func(client mq_pb.SeaweedMessagingClient) error {
		// First get current configuration
		getResp, err := client.GetTopicConfiguration(context.Background(), &mq_pb.GetTopicConfigurationRequest{
			Topic: seaweedTopic,
		})
		if err != nil {
			// If topic doesn't exist, create it with default partition count
			_, err := client.ConfigureTopic(context.Background(), &mq_pb.ConfigureTopicRequest{
				Topic:          seaweedTopic,
				PartitionCount: 1, // Default partition count
				RecordType:     recordType,
			})
			return err
		}

		// Update existing topic with new schema
		_, err = client.ConfigureTopic(context.Background(), &mq_pb.ConfigureTopicRequest{
			Topic:          seaweedTopic,
			PartitionCount: getResp.PartitionCount,
			RecordType:     recordType,
			Retention:      getResp.Retention,
		})
		return err
	})
}
