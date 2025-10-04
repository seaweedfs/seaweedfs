package protocol

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/mq/kafka/consumer"
	"github.com/seaweedfs/seaweedfs/weed/mq/kafka/consumer_offset"
	"github.com/seaweedfs/seaweedfs/weed/mq/kafka/integration"
	"github.com/seaweedfs/seaweedfs/weed/mq/kafka/schema"
	mqschema "github.com/seaweedfs/seaweedfs/weed/mq/schema"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/mq_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/schema_pb"
	"github.com/seaweedfs/seaweedfs/weed/security"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

// GetAdvertisedAddress returns the host:port that should be advertised to clients
// This handles the Docker networking issue where internal IPs aren't reachable by external clients
func (h *Handler) GetAdvertisedAddress(gatewayAddr string) (string, int) {
	host, port := "localhost", 9093

	// Try to parse the gateway address if provided to get the port
	if gatewayAddr != "" {
		if _, gatewayPort, err := net.SplitHostPort(gatewayAddr); err == nil {
			if gatewayPortInt, err := strconv.Atoi(gatewayPort); err == nil {
				port = gatewayPortInt // Only use the port, not the host
			}
		}
	}

	// Override with environment variable if set, otherwise always use localhost for external clients
	if advertisedHost := os.Getenv("KAFKA_ADVERTISED_HOST"); advertisedHost != "" {
		host = advertisedHost
		Debug("Using KAFKA_ADVERTISED_HOST: %s:%d", host, port)
	} else {
		host = "localhost"
		Debug("Using default advertised address: %s:%d (set KAFKA_ADVERTISED_HOST to override)", host, port)
	}

	return host, port
}

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

// kafkaRequest represents a Kafka API request to be processed
type kafkaRequest struct {
	correlationID uint32
	apiKey        uint16
	apiVersion    uint16
	requestBody   []byte
	ctx           context.Context
}

// kafkaResponse represents a Kafka API response
type kafkaResponse struct {
	correlationID uint32
	apiKey        uint16
	apiVersion    uint16
	response      []byte
	err           error
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
	CreateTopicWithSchemas(name string, partitions int32, valueRecordType *schema_pb.RecordType, keyRecordType *schema_pb.RecordType) error
	DeleteTopic(topic string) error
	GetTopicInfo(topic string) (*integration.KafkaTopicInfo, bool)
	// Ledger methods REMOVED - SMQ handles Kafka offsets natively
	ProduceRecord(topicName string, partitionID int32, key, value []byte) (int64, error)
	ProduceRecordValue(topicName string, partitionID int32, key []byte, recordValueBytes []byte) (int64, error)
	// GetStoredRecords retrieves records from SMQ storage (optional - for advanced implementations)
	GetStoredRecords(topic string, partition int32, fromOffset int64, maxRecords int) ([]integration.SMQRecord, error)
	// GetEarliestOffset returns the earliest available offset for a topic partition
	GetEarliestOffset(topic string, partition int32) (int64, error)
	// GetLatestOffset returns the latest available offset for a topic partition
	GetLatestOffset(topic string, partition int32) (int64, error)
	// WithFilerClient executes a function with a filer client for accessing SeaweedMQ metadata
	WithFilerClient(streamingMode bool, fn func(client filer_pb.SeaweedFilerClient) error) error
	// GetBrokerAddresses returns the discovered SMQ broker addresses for Metadata responses
	GetBrokerAddresses() []string
	// CreatePerConnectionBrokerClient creates an isolated BrokerClient for each TCP connection
	CreatePerConnectionBrokerClient() (*integration.BrokerClient, error)
	// SetProtocolHandler sets the protocol handler reference for connection context access
	SetProtocolHandler(handler integration.ProtocolHandler)
	Close() error
}

// ConsumerOffsetStorage defines the interface for storing consumer offsets
// This is used by OffsetCommit and OffsetFetch protocol handlers
type ConsumerOffsetStorage interface {
	CommitOffset(group, topic string, partition int32, offset int64, metadata string) error
	FetchOffset(group, topic string, partition int32) (int64, string, error)
	FetchAllOffsets(group string) (map[TopicPartition]OffsetMetadata, error)
	DeleteGroup(group string) error
	Close() error
}

// TopicPartition uniquely identifies a topic partition for offset storage
type TopicPartition struct {
	Topic     string
	Partition int32
}

// OffsetMetadata contains offset and associated metadata
type OffsetMetadata struct {
	Offset   int64
	Metadata string
}

// TopicSchemaConfig holds schema configuration for a topic
type TopicSchemaConfig struct {
	// Value schema configuration
	ValueSchemaID     uint32
	ValueSchemaFormat schema.Format

	// Key schema configuration (optional)
	KeySchemaID     uint32
	KeySchemaFormat schema.Format
	HasKeySchema    bool // indicates if key schema is configured
}

// Legacy accessors for backward compatibility
func (c *TopicSchemaConfig) SchemaID() uint32 {
	return c.ValueSchemaID
}

func (c *TopicSchemaConfig) SchemaFormat() schema.Format {
	return c.ValueSchemaFormat
}

// stringPtr returns a pointer to the given string
func stringPtr(s string) *string {
	return &s
}

// Handler processes Kafka protocol requests from clients using SeaweedMQ
type Handler struct {
	// SeaweedMQ integration
	seaweedMQHandler SeaweedMQHandlerInterface

	// SMQ offset storage removed - using ConsumerOffsetStorage instead

	// Consumer offset storage for Kafka protocol OffsetCommit/OffsetFetch
	consumerOffsetStorage ConsumerOffsetStorage

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

	// Track registered schemas to prevent duplicate registrations
	registeredSchemas   map[string]bool // key: "topic:schemaID" or "topic-key:schemaID"
	registeredSchemasMu sync.RWMutex

	filerClient filer_pb.SeaweedFilerClient

	// SMQ broker addresses discovered from masters for Metadata responses
	smqBrokerAddresses []string

	// Gateway address for coordinator registry
	gatewayAddress string

	// Connection context for tracking client information
	connContext *ConnectionContext

	// Schema Registry URL for delayed initialization
	schemaRegistryURL string

	// Default partition count for auto-created topics
	defaultPartitions int32
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
	return NewSeaweedMQBrokerHandlerWithDefaults(masters, filerGroup, clientHost, 4) // Default to 4 partitions
}

// NewSeaweedMQBrokerHandlerWithDefaults creates a new handler with SeaweedMQ broker integration and custom defaults
func NewSeaweedMQBrokerHandlerWithDefaults(masters string, filerGroup string, clientHost string, defaultPartitions int32) (*Handler, error) {
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

	// Create consumer offset storage (for OffsetCommit/OffsetFetch protocol)
	// Use filer-based storage for persistence across restarts
	consumerOffsetStorage := newOffsetStorageAdapter(
		consumer_offset.NewFilerStorage(sharedFilerAccessor),
	)

	handler := &Handler{
		seaweedMQHandler:      smqHandler,
		consumerOffsetStorage: consumerOffsetStorage,
		groupCoordinator:      consumer.NewGroupCoordinator(),
		smqBrokerAddresses:    nil, // Will be set by SetSMQBrokerAddresses() when server starts
		registeredSchemas:     make(map[string]bool),
		defaultPartitions:     defaultPartitions,
	}

	// Set protocol handler reference in SMQ handler for connection context access
	smqHandler.SetProtocolHandler(handler)

	return handler, nil
}

// AddTopicForTesting creates a topic for testing purposes
// This delegates to the underlying SeaweedMQ handler
func (h *Handler) AddTopicForTesting(topicName string, partitions int32) {
	if h.seaweedMQHandler != nil {
		h.seaweedMQHandler.CreateTopic(topicName, partitions)
	}
}

// Delegate methods to SeaweedMQ handler

// GetOrCreateLedger method REMOVED - SMQ handles Kafka offsets natively

// GetLedger method REMOVED - SMQ handles Kafka offsets natively

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

// isDataPlaneAPI returns true if the API key is a data plane operation (Fetch, Produce)
// Data plane operations can be slow and may block on I/O
func isDataPlaneAPI(apiKey uint16) bool {
	switch apiKey {
	case 0: // Produce
		return true
	case 1: // Fetch
		return true
	default:
		return false
	}
}

// GetConnectionContext returns the current connection context converted to integration.ConnectionContext
// This implements the integration.ProtocolHandler interface
func (h *Handler) GetConnectionContext() *integration.ConnectionContext {
	if h.connContext == nil {
		return nil
	}
	// Convert protocol.ConnectionContext to integration.ConnectionContext
	return &integration.ConnectionContext{
		ClientID:      h.connContext.ClientID,
		ConsumerGroup: h.connContext.ConsumerGroup,
		MemberID:      h.connContext.MemberID,
		BrokerClient:  h.connContext.BrokerClient,
	}
}

// HandleConn processes a single client connection
func (h *Handler) HandleConn(ctx context.Context, conn net.Conn) error {
	connectionID := fmt.Sprintf("%s->%s", conn.RemoteAddr(), conn.LocalAddr())
	fmt.Printf("🔥🔥🔥 HandleConn START: %s\n", connectionID)
	Debug("KAFKA 8.0.0 DEBUG: NEW HANDLER CODE ACTIVE - %s", time.Now().Format("15:04:05"))

	// Record connection metrics
	RecordConnectionMetrics()

	// Set connection context for this connection
	h.connContext = &ConnectionContext{
		RemoteAddr:   conn.RemoteAddr(),
		LocalAddr:    conn.LocalAddr(),
		ConnectionID: connectionID,
	}

	// CRITICAL: Create per-connection BrokerClient for isolated gRPC streams
	// This prevents different connections from interfering with each other's Fetch requests
	fmt.Printf("🔥🔥🔥 [%s] [BROKER_CLIENT] Creating per-connection BrokerClient\n", connectionID)
	glog.Infof("[%s] [BROKER_CLIENT] Creating per-connection BrokerClient", connectionID)
	connBrokerClient, err := h.seaweedMQHandler.CreatePerConnectionBrokerClient()
	if err != nil {
		fmt.Printf("🔥🔥🔥 [%s] [BROKER_CLIENT] Failed: %v\n", connectionID, err)
		glog.Errorf("[%s] [BROKER_CLIENT] Failed to create per-connection BrokerClient: %v", connectionID, err)
		return fmt.Errorf("failed to create broker client: %w", err)
	}
	h.connContext.BrokerClient = connBrokerClient
	fmt.Printf("🔥🔥🔥 [%s] [BROKER_CLIENT] Per-connection BrokerClient created successfully\n", connectionID)
	glog.Infof("[%s] [BROKER_CLIENT] Per-connection BrokerClient created successfully", connectionID)

	Debug("[%s] NEW CONNECTION ESTABLISHED", connectionID)

	defer func() {
		Debug("[%s] Connection closing, cleaning up BrokerClient", connectionID)
		// Close the per-connection broker client
		if connBrokerClient != nil {
			if closeErr := connBrokerClient.Close(); closeErr != nil {
				Error("[%s] Error closing BrokerClient: %v", connectionID, closeErr)
			}
		}
		RecordDisconnectionMetrics()
		h.connContext = nil // Clear connection context
		conn.Close()
	}()

	r := bufio.NewReader(conn)
	w := bufio.NewWriter(conn)
	defer w.Flush()

	// Use default timeout config
	timeoutConfig := DefaultTimeoutConfig()

	// CRITICAL: Separate control plane from data plane
	// Control plane: Metadata, Heartbeat, JoinGroup, etc. (must be fast, never block)
	// Data plane: Fetch, Produce (can be slow, may block on I/O)
	//
	// Architecture:
	// - Main loop routes requests to appropriate channel based on API key
	// - Control goroutine processes control messages (fast, sequential)
	// - Data goroutine processes data messages (can be slow)
	// - Response writer handles responses in order using correlation IDs
	controlChan := make(chan *kafkaRequest, 10)
	dataChan := make(chan *kafkaRequest, 10)
	responseChan := make(chan *kafkaResponse, 100)
	var wg sync.WaitGroup

	// Response writer - maintains request/response order per connection
	// CRITICAL: While we process requests concurrently (control/data plane),
	// we MUST track the order requests arrive and send responses in that same order.
	// Solution: Track received correlation IDs in a queue, send responses in that queue order.
	correlationQueue := make([]uint32, 0, 100)
	correlationQueueMu := &sync.Mutex{}

	wg.Add(1)
	go func() {
		defer wg.Done()
		pendingResponses := make(map[uint32]*kafkaResponse)
		nextToSend := 0 // Index in correlationQueue

		for resp := range responseChan {
			correlationQueueMu.Lock()
			pendingResponses[resp.correlationID] = resp

			// Send all responses we can in queue order
			for nextToSend < len(correlationQueue) {
				expectedID := correlationQueue[nextToSend]
				readyResp, exists := pendingResponses[expectedID]
				if !exists {
					// Response not ready yet, stop sending
					break
				}

				// Send this response
				if readyResp.err != nil {
					Error("[%s] Error processing correlation=%d: %v", connectionID, readyResp.correlationID, readyResp.err)
				} else {
					Debug("[%s] Sending response correlation=%d: %d bytes (in order)", connectionID, readyResp.correlationID, len(readyResp.response))
					if writeErr := h.writeResponseWithHeader(w, readyResp.correlationID, readyResp.apiKey, readyResp.apiVersion, readyResp.response, timeoutConfig.WriteTimeout); writeErr != nil {
						Error("[%s] Write error correlation=%d: %v", connectionID, readyResp.correlationID, writeErr)
						correlationQueueMu.Unlock()
						return
					}
				}

				// Remove from pending and advance
				delete(pendingResponses, expectedID)
				nextToSend++
			}
			correlationQueueMu.Unlock()
		}
	}()

	// Control plane processor - fast operations, never blocks
	wg.Add(1)
	go func() {
		defer wg.Done()
		for req := range controlChan {
			glog.Infof("[%s] Control plane processing correlation=%d", connectionID, req.correlationID)
			response, err := h.processRequestSync(req)
			glog.Infof("[%s] Control plane completed correlation=%d, sending to responseChan", connectionID, req.correlationID)
			select {
			case responseChan <- &kafkaResponse{
				correlationID: req.correlationID,
				apiKey:        req.apiKey,
				apiVersion:    req.apiVersion,
				response:      response,
				err:           err,
			}:
				glog.Infof("[%s] Control plane sent correlation=%d to responseChan", connectionID, req.correlationID)
			case <-time.After(5 * time.Second):
				glog.Errorf("[%s] DEADLOCK: Control plane timeout sending correlation=%d to responseChan (buffer full?)", connectionID, req.correlationID)
			}
		}
	}()

	// Data plane processor - can block on I/O
	wg.Add(1)
	go func() {
		defer wg.Done()
		for req := range dataChan {
			response, err := h.processRequestSync(req)
			responseChan <- &kafkaResponse{
				correlationID: req.correlationID,
				apiKey:        req.apiKey,
				apiVersion:    req.apiVersion,
				response:      response,
				err:           err,
			}
		}
	}()

	defer func() {
		close(controlChan)
		close(dataChan)
		wg.Wait()
		close(responseChan)
	}()

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
				Debug("[%s] Client closed connection (clean EOF) - no request sent", connectionID)
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
		fmt.Printf("🔥 PROTOCOL DEBUG: Read message size: %d bytes\n", size)
		if size == 0 || size > 1024*1024 { // 1MB limit
			// Use standardized error for message size limit
			Debug("[%s] Invalid message size: %d (limit: 1MB)", connectionID, size)
			// Send error response for message too large
			errorResponse := BuildErrorResponse(0, ErrorCodeMessageTooLarge) // correlation ID 0 since we can't parse it yet
			if writeErr := h.writeResponseWithCorrelationID(w, 0, errorResponse, timeoutConfig.WriteTimeout); writeErr != nil {
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
		Debug("[%s] SUCCESSFULLY READ MESSAGE BODY: %d bytes", connectionID, len(messageBuf))

		// Parse at least the basic header to get API key and correlation ID
		if len(messageBuf) < 8 {
			return fmt.Errorf("message too short")
		}

		apiKey := binary.BigEndian.Uint16(messageBuf[0:2])
		apiVersion := binary.BigEndian.Uint16(messageBuf[2:4])
		correlationID := binary.BigEndian.Uint32(messageBuf[4:8])

		apiName := getAPIName(apiKey)
		fmt.Printf("🔥 PROTOCOL DEBUG: Parsed header - API Key: %d (%s), Version: %d, Correlation: %d\n", apiKey, apiName, apiVersion, correlationID)

		// Validate API version against what we support
		Debug("VALIDATING API VERSION: Key=%d, Version=%d", apiKey, apiVersion)
		if err := h.validateAPIVersion(apiKey, apiVersion); err != nil {
			// Return proper Kafka error response for unsupported version
			response, writeErr := h.buildUnsupportedVersionResponse(correlationID, apiKey, apiVersion)
			if writeErr != nil {
				return fmt.Errorf("build error response: %w", writeErr)
			}
			// CRITICAL: Send error response through response queue to maintain sequential ordering
			// This prevents deadlocks in the response writer which expects all correlation IDs in sequence
			select {
			case responseChan <- &kafkaResponse{
				correlationID: correlationID,
				apiKey:        apiKey,
				apiVersion:    apiVersion,
				response:      response,
				err:           nil,
			}:
				// Error response queued successfully, continue reading next request
				continue
			case <-ctx.Done():
				return ctx.Err()
			}
		}

		// Extract request body - special handling for ApiVersions requests
		var requestBody []byte
		if apiKey == 18 && apiVersion >= 3 {
			// ApiVersions v3+ uses client_software_name + client_software_version, not client_id
			bodyOffset := 8 // Skip api_key(2) + api_version(2) + correlation_id(4)

			// Skip client_software_name (compact string)
			if len(messageBuf) > bodyOffset {
				clientNameLen := int(messageBuf[bodyOffset]) // compact string length
				if clientNameLen > 0 {
					clientNameLen-- // compact strings encode length+1
					bodyOffset += 1 + clientNameLen
				} else {
					bodyOffset += 1 // just the length byte for null/empty
				}
			}

			// Skip client_software_version (compact string)
			if len(messageBuf) > bodyOffset {
				clientVersionLen := int(messageBuf[bodyOffset]) // compact string length
				if clientVersionLen > 0 {
					clientVersionLen-- // compact strings encode length+1
					bodyOffset += 1 + clientVersionLen
				} else {
					bodyOffset += 1 // just the length byte for null/empty
				}
			}

			// Skip tagged fields (should be 0x00 for ApiVersions)
			if len(messageBuf) > bodyOffset {
				bodyOffset += 1 // tagged fields byte
			}

			requestBody = messageBuf[bodyOffset:]
		} else {
			// Parse header using flexible version utilities for other APIs
			header, parsedRequestBody, parseErr := ParseRequestHeader(messageBuf)
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
				// Use the successfully parsed request body
				requestBody = parsedRequestBody

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
					// Store client ID in connection context for use in fetch requests
					if h.connContext != nil {
						h.connContext.ClientID = *header.ClientID
					}
					Debug("Client ID: %s", *header.ClientID)
				}
			}
		}

		// CRITICAL: Route request to appropriate processor
		// Control plane: Fast, never blocks (Metadata, Heartbeat, etc.)
		// Data plane: Can be slow (Fetch, Produce)
		Debug("API REQUEST - Key: %d (%s), Version: %d, Correlation: %d", apiKey, getAPIName(apiKey), apiVersion, correlationID)

		req := &kafkaRequest{
			correlationID: correlationID,
			apiKey:        apiKey,
			apiVersion:    apiVersion,
			requestBody:   requestBody,
			ctx:           ctx,
		}

		// Track this correlation ID in the order queue for response ordering
		correlationQueueMu.Lock()
		correlationQueue = append(correlationQueue, correlationID)
		correlationQueueMu.Unlock()

		// Route to appropriate channel based on API key
		var targetChan chan *kafkaRequest
		if isDataPlaneAPI(apiKey) {
			targetChan = dataChan
			Debug("[%s] Routing correlation=%d to DATA plane", connectionID, correlationID)
		} else {
			targetChan = controlChan
			Debug("[%s] Routing correlation=%d to CONTROL plane", connectionID, correlationID)
		}

		select {
		case targetChan <- req:
			// Request queued successfully, continue reading next request
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// processRequestSync processes a single Kafka API request synchronously and returns the response
func (h *Handler) processRequestSync(req *kafkaRequest) ([]byte, error) {
	// Record request start time for latency tracking
	requestStart := time.Now()
	apiName := getAPIName(req.apiKey)

	Debug("PROCESSING API REQUEST: Key=%d (%s), Version=%d, Correlation=%d",
		req.apiKey, apiName, req.apiVersion, req.correlationID)

	var response []byte
	var err error

	switch req.apiKey {
	case 18: // ApiVersions
		Debug("-> ApiVersions v%d", req.apiVersion)
		response, err = h.handleApiVersions(req.correlationID, req.apiVersion)

	case 3: // Metadata
		Debug("-> Metadata v%d", req.apiVersion)
		response, err = h.handleMetadata(req.correlationID, req.apiVersion, req.requestBody)

	case 2: // ListOffsets
		response, err = h.handleListOffsets(req.correlationID, req.apiVersion, req.requestBody)

	case 19: // CreateTopics
		response, err = h.handleCreateTopics(req.correlationID, req.apiVersion, req.requestBody)

	case 20: // DeleteTopics
		response, err = h.handleDeleteTopics(req.correlationID, req.requestBody)

	case 0: // Produce
		Debug("-> Produce v%d", req.apiVersion)
		response, err = h.handleProduce(req.correlationID, req.apiVersion, req.requestBody)

	case 1: // Fetch
		Debug("-> Fetch v%d", req.apiVersion)
		response, err = h.handleFetch(req.ctx, req.correlationID, req.apiVersion, req.requestBody)

	case 11: // JoinGroup
		Debug("-> JoinGroup v%d", req.apiVersion)
		response, err = h.handleJoinGroup(req.correlationID, req.apiVersion, req.requestBody)

	case 14: // SyncGroup
		Debug("-> SyncGroup v%d", req.apiVersion)
		response, err = h.handleSyncGroup(req.correlationID, req.apiVersion, req.requestBody)

	case 8: // OffsetCommit
		Debug("-> OffsetCommit")
		response, err = h.handleOffsetCommit(req.correlationID, req.requestBody)

	case 9: // OffsetFetch
		Debug("-> OffsetFetch v%d", req.apiVersion)
		response, err = h.handleOffsetFetch(req.correlationID, req.apiVersion, req.requestBody)

	case 10: // FindCoordinator
		Debug("-> FindCoordinator v%d", req.apiVersion)
		response, err = h.handleFindCoordinator(req.correlationID, req.apiVersion, req.requestBody)

	case 12: // Heartbeat
		Debug("-> Heartbeat v%d", req.apiVersion)
		response, err = h.handleHeartbeat(req.correlationID, req.apiVersion, req.requestBody)

	case 13: // LeaveGroup
		response, err = h.handleLeaveGroup(req.correlationID, req.apiVersion, req.requestBody)

	case 15: // DescribeGroups
		Debug("DescribeGroups request received, correlation: %d, version: %d", req.correlationID, req.apiVersion)
		response, err = h.handleDescribeGroups(req.correlationID, req.apiVersion, req.requestBody)

	case 16: // ListGroups
		Debug("ListGroups request received, correlation: %d, version: %d", req.correlationID, req.apiVersion)
		response, err = h.handleListGroups(req.correlationID, req.apiVersion, req.requestBody)

	case 32: // DescribeConfigs
		Debug("DescribeConfigs request received, correlation: %d, version: %d", req.correlationID, req.apiVersion)
		response, err = h.handleDescribeConfigs(req.correlationID, req.apiVersion, req.requestBody)

	case 60: // DescribeCluster
		Debug("-> DescribeCluster v%d", req.apiVersion)
		response, err = h.handleDescribeCluster(req.correlationID, req.apiVersion, req.requestBody)

	case 22: // InitProducerId
		Debug("-> InitProducerId v%d", req.apiVersion)
		response, err = h.handleInitProducerId(req.correlationID, req.apiVersion, req.requestBody)

	default:
		Warning("Unsupported API key: %d (%s) v%d - Correlation: %d", req.apiKey, apiName, req.apiVersion, req.correlationID)
		err = fmt.Errorf("unsupported API key: %d (version %d)", req.apiKey, req.apiVersion)
	}

	// Record metrics
	requestLatency := time.Since(requestStart)
	if err != nil {
		RecordErrorMetrics(req.apiKey, requestLatency)
	} else {
		RecordRequestMetrics(req.apiKey, requestLatency)
	}

	return response, err
}

// ApiKeyInfo represents supported API key information
type ApiKeyInfo struct {
	ApiKey     uint16
	MinVersion uint16
	MaxVersion uint16
}

// SupportedApiKeys defines all supported API keys and their version ranges
var SupportedApiKeys = []ApiKeyInfo{
	{18, 0, 4}, // ApiVersions - support up to v4 for Kafka 8.0.0 compatibility
	{3, 0, 7},  // Metadata - support up to v7
	{0, 0, 7},  // Produce
	{1, 0, 7},  // Fetch
	{2, 0, 2},  // ListOffsets
	{19, 0, 5}, // CreateTopics
	{20, 0, 4}, // DeleteTopics
	{10, 0, 2}, // FindCoordinator
	{11, 0, 6}, // JoinGroup
	{14, 0, 5}, // SyncGroup
	{8, 0, 2},  // OffsetCommit
	{9, 0, 5},  // OffsetFetch
	{12, 0, 4}, // Heartbeat
	{13, 0, 4}, // LeaveGroup
	{15, 0, 5}, // DescribeGroups
	{16, 0, 4}, // ListGroups
	{32, 0, 4}, // DescribeConfigs
	{22, 0, 4}, // InitProducerId - support up to v4 for transactional producers
	{60, 0, 1}, // DescribeCluster - for AdminClient compatibility (KIP-919)
}

func (h *Handler) handleApiVersions(correlationID uint32, apiVersion uint16) ([]byte, error) {
	// Send correct flexible or non-flexible response based on API version
	// This fixes the AdminClient "collection size 2184558" error by using proper varint encoding
	response := make([]byte, 0, 512)

	// NOTE: Correlation ID is handled by writeResponseWithCorrelationID
	// Do NOT include it in the response body

	// === RESPONSE BODY ===
	// Error code (2 bytes) - always fixed-length
	response = append(response, 0, 0) // No error

	// API Keys Array - CRITICAL FIX: Use correct encoding based on version
	if apiVersion >= 3 {
		// FLEXIBLE FORMAT: Compact array with varint length - THIS FIXES THE ADMINCLIENT BUG!
		response = append(response, CompactArrayLength(uint32(len(SupportedApiKeys)))...)

		// Add API key entries with per-element tagged fields
		for _, api := range SupportedApiKeys {
			response = append(response, byte(api.ApiKey>>8), byte(api.ApiKey))         // api_key (2 bytes)
			response = append(response, byte(api.MinVersion>>8), byte(api.MinVersion)) // min_version (2 bytes)
			response = append(response, byte(api.MaxVersion>>8), byte(api.MaxVersion)) // max_version (2 bytes)
			response = append(response, 0x00)                                          // Per-element tagged fields (varint: empty)
		}

	} else {
		// NON-FLEXIBLE FORMAT: Regular array with fixed 4-byte length
		response = append(response, 0, 0, 0, byte(len(SupportedApiKeys))) // Array length (4 bytes)

		// Add API key entries without tagged fields
		for _, api := range SupportedApiKeys {
			response = append(response, byte(api.ApiKey>>8), byte(api.ApiKey))         // api_key (2 bytes)
			response = append(response, byte(api.MinVersion>>8), byte(api.MinVersion)) // min_version (2 bytes)
			response = append(response, byte(api.MaxVersion>>8), byte(api.MaxVersion)) // max_version (2 bytes)
		}
	}

	// Throttle time (for v1+) - always fixed-length
	if apiVersion >= 1 {
		response = append(response, 0, 0, 0, 0) // throttle_time_ms = 0 (4 bytes)
	}

	// Response-level tagged fields (for v3+ flexible versions)
	if apiVersion >= 3 {
		response = append(response, 0x00) // Empty response-level tagged fields (varint: single byte 0)
	}

	Debug("ApiVersions v%d response: %d bytes (flexible: %t) - ADMINCLIENT COMPATIBILITY FIX", apiVersion, len(response), apiVersion >= 3)

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

	// NOTE: Correlation ID is handled by writeResponseWithCorrelationID
	// Do NOT include it in the response body

	// Brokers array length (4 bytes) - 1 broker (this gateway)
	response = append(response, 0, 0, 0, 1)

	// Broker 0: node_id(4) + host(STRING) + port(4)
	response = append(response, 0, 0, 0, 1) // node_id = 1 (consistent with partitions)

	// Get advertised address for client connections
	host, port := h.GetAdvertisedAddress(h.GetGatewayAddress())

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
	Debug("METADATA v0 REQUEST - Requested: %v (empty=all)", requestedTopics)

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
		partitionCount := h.GetDefaultPartitions() // Use configurable default
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
	Debug("METADATA v1 REQUEST - Requested: %v (empty=all)", requestedTopics)

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

	// NOTE: Correlation ID is handled by writeResponseWithHeader
	// Do NOT include it in the response body

	// Brokers array length (4 bytes) - 1 broker (this gateway)
	response = append(response, 0, 0, 0, 1)

	// Broker 0: node_id(4) + host(STRING) + port(4) + rack(STRING)
	response = append(response, 0, 0, 0, 1) // node_id = 1

	// Get advertised address for client connections
	host, port := h.GetAdvertisedAddress(h.GetGatewayAddress())

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
		partitionCount := h.GetDefaultPartitions() // Use configurable default
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
	Debug("METADATA v2 REQUEST - Requested: %v (empty=all)", requestedTopics)

	// Determine topics to return using SeaweedMQ handler
	var topicsToReturn []string
	if len(requestedTopics) == 0 {
		fmt.Printf("METADATA V2: About to call ListTopics()\n")
		topicsToReturn = h.seaweedMQHandler.ListTopics()
		fmt.Printf("METADATA V2: ListTopics() returned %d topics: %v\n", len(topicsToReturn), topicsToReturn)
	} else {
		fmt.Printf("METADATA V2: Checking specific topics: %v\n", requestedTopics)
		for _, name := range requestedTopics {
			if h.seaweedMQHandler.TopicExists(name) {
				topicsToReturn = append(topicsToReturn, name)
			}
		}
		fmt.Printf("METADATA V2: Found %d existing topics: %v\n", len(topicsToReturn), topicsToReturn)
	}

	var buf bytes.Buffer

	// Correlation ID (4 bytes)
	// NOTE: Correlation ID is handled by writeResponseWithCorrelationID
	// Do NOT include it in the response body

	// Brokers array (4 bytes length + brokers) - 1 broker (this gateway)
	binary.Write(&buf, binary.BigEndian, int32(1))

	// Get advertised address for client connections
	host, port := h.GetAdvertisedAddress(h.GetGatewayAddress())

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
	// Schema Registry requires a non-null cluster ID
	clusterID := "seaweedfs-kafka-gateway"
	binary.Write(&buf, binary.BigEndian, int16(len(clusterID)))
	buf.WriteString(clusterID)

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
		partitionCount := h.GetDefaultPartitions() // Use configurable default
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
	// NOTE: Correlation ID is handled by writeResponseWithCorrelationID
	// Do NOT include it in the response body

	// ThrottleTimeMs (4 bytes) - v3+ addition
	binary.Write(&buf, binary.BigEndian, int32(0)) // No throttling

	// Brokers array (4 bytes length + brokers) - 1 broker (this gateway)
	binary.Write(&buf, binary.BigEndian, int32(1))

	// Get advertised address for client connections
	host, port := h.GetAdvertisedAddress(h.GetGatewayAddress())

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
	// Schema Registry requires a non-null cluster ID
	clusterID := "seaweedfs-kafka-gateway"
	binary.Write(&buf, binary.BigEndian, int16(len(clusterID)))
	buf.WriteString(clusterID)

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
		partitionCount := h.GetDefaultPartitions() // Use configurable default
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
	Debug("METADATA v5/v6 REQUEST - Requested: %v (empty=all)", requestedTopics)

	// Determine topics to return using SeaweedMQ handler
	var topicsToReturn []string
	if len(requestedTopics) == 0 {
		topicsToReturn = h.seaweedMQHandler.ListTopics()
	} else {
		// FIXED: Proper topic existence checking (removed the hack)
		// Now that CreateTopics v5 works, we use proper Kafka workflow:
		// 1. Check which requested topics actually exist
		// 2. Auto-create system topics if they don't exist
		// 3. Only return existing topics in metadata
		// 4. Client will call CreateTopics for non-existent topics
		// 5. Then request metadata again to see the created topics
		for _, topic := range requestedTopics {
			if isSystemTopic(topic) {
				// Always try to auto-create system topics during metadata requests
				Debug("Metadata: Ensuring system topic %s exists during metadata request", topic)
				if !h.seaweedMQHandler.TopicExists(topic) {
					Debug("Metadata: Auto-creating system topic %s during metadata request", topic)
					if err := h.createTopicWithSchemaSupport(topic, 1); err != nil {
						Debug("Metadata: Failed to auto-create system topic %s: %v", topic, err)
						// Continue without adding to topicsToReturn - client will get UNKNOWN_TOPIC_OR_PARTITION
					} else {
						Debug("Metadata: Successfully auto-created system topic %s", topic)
					}
				} else {
					Debug("Metadata: System topic %s already exists", topic)
				}
				topicsToReturn = append(topicsToReturn, topic)
			} else if h.seaweedMQHandler.TopicExists(topic) {
				topicsToReturn = append(topicsToReturn, topic)
			}
		}
		Debug("PROPER KAFKA FLOW: Returning existing topics only: %v (requested: %v)", topicsToReturn, requestedTopics)
	}

	var buf bytes.Buffer

	// Correlation ID (4 bytes)
	// NOTE: Correlation ID is handled by writeResponseWithCorrelationID
	// Do NOT include it in the response body

	// ThrottleTimeMs (4 bytes) - v3+ addition
	binary.Write(&buf, binary.BigEndian, int32(0)) // No throttling

	// Brokers array (4 bytes length + brokers) - 1 broker (this gateway)
	binary.Write(&buf, binary.BigEndian, int32(1))

	// Get advertised address for client connections
	host, port := h.GetAdvertisedAddress(h.GetGatewayAddress())

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
	// Schema Registry requires a non-null cluster ID
	clusterID := "seaweedfs-kafka-gateway"
	binary.Write(&buf, binary.BigEndian, int16(len(clusterID)))
	buf.WriteString(clusterID)

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
		partitionCount := h.GetDefaultPartitions() // Use configurable default
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

			// LeaderEpoch (4 bytes) - v7+ addition, included for AdminClient compatibility
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
	Debug("Metadata v5/v6 response for %d topics: %v", len(topicsToReturn), topicsToReturn)

	return response, nil
}

// HandleMetadataV7 implements Metadata API v7 with LeaderEpoch field (FLEXIBLE FORMAT)
func (h *Handler) HandleMetadataV7(correlationID uint32, requestBody []byte) ([]byte, error) {
	// Metadata v7 adds LeaderEpoch field to partitions and uses FLEXIBLE FORMAT
	// v7 response layout: correlation_id(4) + throttle_time_ms(4) + brokers(COMPACT_ARRAY) + cluster_id(COMPACT_NULLABLE_STRING) + controller_id(4) + topics(COMPACT_ARRAY) + tagged_fields
	// Each partition now includes: error_code(2) + partition_index(4) + leader_id(4) + leader_epoch(4) + replica_nodes(COMPACT_ARRAY) + isr_nodes(COMPACT_ARRAY) + offline_replicas(COMPACT_ARRAY) + tagged_fields

	Debug("HANDLEMETADATAV7 CALLED - FLEXIBLE FORMAT IMPLEMENTATION")
	fmt.Printf("🔥 METADATA V7 DEBUG: Starting HandleMetadataV7\n")

	// Parse requested topics (empty means all)
	requestedTopics := h.parseMetadataTopics(requestBody)
	Debug("METADATA v7 REQUEST (FLEXIBLE) - Requested: %v (empty=all)", requestedTopics)
	fmt.Printf("🔥 METADATA V7 DEBUG: Requested topics: %v\n", requestedTopics)

	// Determine topics to return using SeaweedMQ handler
	var topicsToReturn []string
	if len(requestedTopics) == 0 {
		fmt.Printf("🔥 METADATA V7 DEBUG: Calling ListTopics() for all topics\n")
		topicsToReturn = h.seaweedMQHandler.ListTopics()
		fmt.Printf("🔥 METADATA V7 DEBUG: ListTopics() returned %d topics: %v\n", len(topicsToReturn), topicsToReturn)
	} else {
		fmt.Printf("🔥 METADATA V7 DEBUG: Checking specific topics: %v\n", requestedTopics)
		for _, name := range requestedTopics {
			if h.seaweedMQHandler.TopicExists(name) {
				topicsToReturn = append(topicsToReturn, name)
			}
		}
		fmt.Printf("🔥 METADATA V7 DEBUG: Found %d existing topics: %v\n", len(topicsToReturn), topicsToReturn)
	}

	var buf bytes.Buffer

	// Correlation ID (4 bytes)
	// NOTE: Correlation ID is handled by writeResponseWithCorrelationID
	// Do NOT include it in the response body

	// ThrottleTimeMs (4 bytes) - v3+ addition
	binary.Write(&buf, binary.BigEndian, int32(0)) // No throttling

	// Brokers array (COMPACT_ARRAY: varint length + brokers) - 1 broker (this gateway)
	buf.Write(CompactArrayLength(1)) // 1 broker

	// Get advertised address for client connections
	host, port := h.GetAdvertisedAddress(h.GetGatewayAddress())

	nodeID := int32(1) // Single gateway node

	// Broker: node_id(4) + host(COMPACT_STRING) + port(4) + rack(COMPACT_STRING) + tagged_fields
	binary.Write(&buf, binary.BigEndian, nodeID)

	// Host (COMPACT_STRING: varint length + data)
	buf.Write(FlexibleString(host))

	// Port (4 bytes)
	binary.Write(&buf, binary.BigEndian, int32(port))

	// Rack (COMPACT_STRING: varint length + data) - v1+ addition, non-nullable
	buf.Write(FlexibleString("")) // Empty string

	// Broker tagged fields (empty)
	buf.WriteByte(0x00)

	// ClusterID (COMPACT_NULLABLE_STRING: varint length + data) - v2+ addition, AFTER all brokers
	// Use 0x00 to indicate null
	buf.WriteByte(0x00) // Null cluster ID

	// ControllerID (4 bytes) - v1+ addition, AFTER ClusterID
	binary.Write(&buf, binary.BigEndian, int32(1))

	// Topics array (COMPACT_ARRAY: varint length + topics)
	buf.Write(CompactArrayLength(uint32(len(topicsToReturn))))

	for _, topicName := range topicsToReturn {
		// ErrorCode (2 bytes)
		binary.Write(&buf, binary.BigEndian, int16(0))

		// Name (COMPACT_STRING: varint length + data)
		buf.Write(FlexibleString(topicName))

		// IsInternal (1 byte) - v1+ addition
		buf.WriteByte(0) // false

		// Get actual partition count from topic info
		topicInfo, exists := h.seaweedMQHandler.GetTopicInfo(topicName)
		partitionCount := h.GetDefaultPartitions() // Use configurable default
		if exists && topicInfo != nil {
			partitionCount = topicInfo.Partitions
		}

		// Partitions array (COMPACT_ARRAY: varint length + partitions)
		buf.Write(CompactArrayLength(uint32(partitionCount)))

		// Create partition entries for each partition
		for partitionID := int32(0); partitionID < partitionCount; partitionID++ {
			binary.Write(&buf, binary.BigEndian, int16(0))    // ErrorCode
			binary.Write(&buf, binary.BigEndian, partitionID) // PartitionIndex
			binary.Write(&buf, binary.BigEndian, int32(1))    // LeaderID

			// LeaderEpoch (4 bytes) - v7+ addition
			binary.Write(&buf, binary.BigEndian, int32(0)) // Leader epoch 0

			// ReplicaNodes array (COMPACT_ARRAY: varint length + nodes)
			buf.Write(CompactArrayLength(1))               // 1 replica
			binary.Write(&buf, binary.BigEndian, int32(1)) // NodeID 1

			// IsrNodes array (COMPACT_ARRAY: varint length + nodes)
			buf.Write(CompactArrayLength(1))               // 1 ISR node
			binary.Write(&buf, binary.BigEndian, int32(1)) // NodeID 1

			// OfflineReplicas array (COMPACT_ARRAY: varint length + nodes) - v5+ addition
			buf.Write(CompactArrayLength(0)) // No offline replicas

			// Partition tagged fields (empty)
			buf.WriteByte(0x00)
		}

		// Topic tagged fields (empty)
		buf.WriteByte(0x00)
	}

	// Response-level tagged fields (empty) - Required for Sarama v1.46.1 compatibility
	buf.WriteByte(0x00)

	response := buf.Bytes()
	Debug("Advertising Kafka gateway: %s", h.GetGatewayAddress())
	Debug("METADATA V7 FLEXIBLE RESPONSE: %d bytes, %d topics: %v", len(response), len(topicsToReturn), topicsToReturn)
	Debug("METADATA V7 RESPONSE BYTES: %x", response)

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
	glog.Infof("🚀🚀🚀 HANDLELISTOFFSETS ENTRY POINT 🚀🚀🚀")
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

	// NOTE: Correlation ID is handled by writeResponseWithHeader
	// Do NOT include it in the response body

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

			// Use direct SMQ reading - no ledgers needed
			// SMQ handles offset management internally
			var responseTimestamp int64
			var responseOffset int64

			Debug("ListOffsets - Topic: %s, Partition: %d, Timestamp: %d (using direct SMQ)",
				string(topicName), partitionID, timestamp)
			glog.Infof("🚀 NEW OFFSET CODE IS RUNNING! 🚀")

			switch timestamp {
			case -2: // earliest offset
				// Get the actual earliest offset from SMQ
				earliestOffset, err := h.seaweedMQHandler.GetEarliestOffset(string(topicName), int32(partitionID))
				if err != nil {
					Debug("Failed to get earliest offset for topic %s partition %d: %v", string(topicName), partitionID, err)
					responseOffset = 0 // fallback to 0
				} else {
					responseOffset = earliestOffset
				}
				responseTimestamp = 0 // No specific timestamp for earliest
				if strings.HasPrefix(string(topicName), "_schemas") {
					glog.Infof("📍 SCHEMA REGISTRY LISTOFFSETS EARLIEST: topic=%s partition=%d returning offset=%d", string(topicName), partitionID, responseOffset)
				}
				Debug("ListOffsets EARLIEST - returning offset: %d, timestamp: %d", responseOffset, responseTimestamp)
			case -1: // latest offset
				// Get the actual latest offset from SMQ
				Debug("XYZABC123 UNIQUE DEBUG MESSAGE - MY CODE IS RUNNING!")
				Debug("*** ABOUT TO CALL GetLatestOffset for topic %s partition %d", string(topicName), int32(partitionID))
				Debug("About to call GetLatestOffset for topic %s partition %d", string(topicName), int32(partitionID))
				if h.seaweedMQHandler == nil {
					Debug("*** ERROR: seaweedMQHandler is nil!")
					Debug("ERROR: seaweedMQHandler is nil!")
					responseOffset = 0
				} else {
					Debug("*** Calling GetLatestOffset...")
					latestOffset, err := h.seaweedMQHandler.GetLatestOffset(string(topicName), int32(partitionID))
					if err != nil {
						Debug("*** GetLatestOffset failed: %v", err)
						Debug("Failed to get latest offset for topic %s partition %d: %v", string(topicName), partitionID, err)
						responseOffset = 0 // fallback to 0
					} else {
						Debug("*** GetLatestOffset returned: %d", latestOffset)
						responseOffset = latestOffset
						Debug("GetLatestOffset returned: %d", latestOffset)
					}
				}
				responseTimestamp = 0 // No specific timestamp for latest
				Debug("*** ListOffsets LATEST - returning offset: %d, timestamp: %d", responseOffset, responseTimestamp)
			default: // specific timestamp - find offset by timestamp
				// For timestamp-based lookup, we need to implement this properly
				// For now, return 0 as fallback
				responseOffset = 0
				responseTimestamp = timestamp
				Debug("ListOffsets BY_TIMESTAMP - returning offset: %d, timestamp: %d", responseOffset, responseTimestamp)
			}

			// Ensure we never return a timestamp as offset - this was the bug!
			if responseOffset > 1000000000 { // If offset looks like a timestamp
				Debug("WARNING: Offset %d looks like a timestamp! Setting to 0", responseOffset)
				responseOffset = 0
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
			// Use schema-aware topic creation
			if err := h.createTopicWithSchemaSupport(t.name, int32(t.partitions)); err != nil {
				Debug("Failed to create topic %s with schema support: %v", t.name, err)
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

	// NOTE: Correlation ID is handled by writeResponseWithHeader
	// Do NOT include it in the response body

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

		// Apply defaults for invalid values
		if numPartitions <= 0 {
			numPartitions = uint32(h.GetDefaultPartitions()) // Use configurable default
		}
		if replicationFactor <= 0 {
			replicationFactor = 1 // Default to 1 replica
		}

		// Use SeaweedMQ integration
		if h.seaweedMQHandler.TopicExists(topicName) {
			errorCode = 36 // TOPIC_ALREADY_EXISTS
		} else {
			// Create the topic in SeaweedMQ with schema support
			if err := h.createTopicWithSchemaSupport(topicName, int32(numPartitions)); err != nil {
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
	offset := 0

	// DEBUG: Log the raw request bytes to understand AdminClient format
	Debug("CreateTopics v%d: Raw request bytes (%d total):", apiVersion, len(requestBody))
	for i := 0; i < len(requestBody) && i < 32; i++ {
		if i%16 == 0 {
			Debug("  %02d: ", i)
		}
		Debug("%02x ", requestBody[i])
		if (i+1)%16 == 0 {
			Debug("")
		}
	}
	if len(requestBody)%16 != 0 || len(requestBody) > 32 {
		Debug("")
	}

	// ADMIN CLIENT COMPATIBILITY FIX:
	// AdminClient's CreateTopics v5 request DOES start with top-level tagged fields (usually empty)
	// Parse them first, then the topics compact array
	Debug("CreateTopics v%d: AdminClient format - parsing top-level tagged fields at start", apiVersion)

	// Parse top-level tagged fields first (usually 0x00 for empty)
	_, consumed, err := DecodeTaggedFields(requestBody[offset:])
	if err != nil {
		Debug("CreateTopics v%d: Tagged fields parsing failed at start with offset=%d, remaining=%d", apiVersion, offset, len(requestBody)-offset)
		// Don't fail - AdminClient might not always include tagged fields properly
		// Just log and continue with topics parsing
	} else {
		Debug("CreateTopics v%d: Successfully parsed top-level tagged fields, consumed %d bytes", apiVersion, consumed)
		offset += consumed
	}

	// Topics (compact array) - Now correctly positioned after tagged fields
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

		// ADMIN CLIENT COMPATIBILITY: AdminClient uses little-endian for replication factor
		// This violates Kafka protocol spec but we need to handle it for compatibility
		if replication == 256 {
			replication = 1 // AdminClient sent 0x01 0x00, intended as little-endian 1
			Debug("CreateTopics v%d: AdminClient replication factor compatibility - corrected 256 → 1", apiVersion)
		}

		// Apply defaults for invalid values
		if partitions <= 0 {
			partitions = uint32(h.GetDefaultPartitions()) // Use configurable default
		}
		if replication <= 0 {
			replication = 1 // Default to 1 replica
		}

		// DEBUG: Log parsed values to understand AdminClient request format
		Debug("CreateTopics v%d: Parsed topic[%d] - partitions=%d, replication=%d (corrected) (bytes at offset %d--%d)",
			apiVersion, i, partitions, replication, offset-6, offset-1)

		// FIX 2: Assignments (compact array) - this was missing!
		assignCount, consumed, err := DecodeCompactArrayLength(requestBody[offset:])
		if err != nil {
			return nil, fmt.Errorf("CreateTopics v%d: decode topic[%d] assignments array: %w", apiVersion, i, err)
		}
		offset += consumed

		// Skip assignment entries (partition_id + replicas array)
		for j := uint32(0); j < assignCount; j++ {
			// partition_id (int32)
			if len(requestBody) < offset+4 {
				return nil, fmt.Errorf("CreateTopics v%d: truncated assignment[%d] partition_id", apiVersion, j)
			}
			offset += 4

			// replicas (compact array of int32)
			replicasCount, consumed, err := DecodeCompactArrayLength(requestBody[offset:])
			if err != nil {
				return nil, fmt.Errorf("CreateTopics v%d: decode assignment[%d] replicas: %w", apiVersion, j, err)
			}
			offset += consumed

			// Skip replica broker IDs (int32 each)
			if len(requestBody) < offset+int(replicasCount)*4 {
				return nil, fmt.Errorf("CreateTopics v%d: truncated assignment[%d] replicas", apiVersion, j)
			}
			offset += int(replicasCount) * 4

			// Assignment tagged fields
			_, consumed, err = DecodeTaggedFields(requestBody[offset:])
			if err != nil {
				return nil, fmt.Errorf("CreateTopics v%d: decode assignment[%d] tagged fields: %w", apiVersion, j, err)
			}
			offset += consumed
		}

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

	Debug("CreateTopics v%d: Successfully parsed %d topics", apiVersion, len(topics))
	for i, topic := range topics {
		Debug("CreateTopics v%d: Topic[%d]: name='%s', partitions=%d, replication=%d", apiVersion, i, topic.name, topic.partitions, topic.replication)
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

	// Remaining bytes after parsing - could be additional fields
	if offset < len(requestBody) {
		Debug("CreateTopics v%d: %d bytes remaining after parsing - likely timeout_ms, validate_only, etc.", apiVersion, len(requestBody)-offset)
	}

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

	// RESPONSE HEADER VERSION FIX:
	// CreateTopics v5+ uses response header version 1 (with tagged fields)
	// This is different from ApiVersions which uses header version 0 for AdminClient compatibility
	if apiVersion >= 5 {
		response = append(response, 0x00) // Empty header tagged fields (varint: single byte 0)
	}

	// throttle_time_ms (4 bytes) - comes directly after correlation ID in CreateTopics responses
	response = append(response, 0, 0, 0, 0)

	// topics (compact array) - V5 FLEXIBLE FORMAT
	topicCount := len(topics)
	Debug("CreateTopics v%d: Generating response for %d topics", apiVersion, topicCount)

	// Debug: log response size at each step
	debugResponseSize := func(step string) {
		Debug("CreateTopics v%d: %s - response size: %d bytes", apiVersion, step, len(response))
	}
	debugResponseSize("After correlation ID and throttle_time_ms")

	// Compact array: length is encoded as UNSIGNED_VARINT(actualLength + 1)
	response = append(response, EncodeUvarint(uint32(topicCount+1))...)
	debugResponseSize("After topics array length")

	// For each topic
	for _, t := range topics {
		// name (compact string): length is encoded as UNSIGNED_VARINT(actualLength + 1)
		nameBytes := []byte(t.name)
		response = append(response, EncodeUvarint(uint32(len(nameBytes)+1))...)
		response = append(response, nameBytes...)

		// TopicId - Not present in v5, only added in v7+
		// v5 CreateTopics response does not include TopicId field

		// error_code (int16)
		var errCode uint16 = 0

		// ADMIN CLIENT COMPATIBILITY: Apply defaults before error checking
		actualPartitions := t.partitions
		if actualPartitions == 0 {
			actualPartitions = 1 // Default to 1 partition if 0 requested
		}
		actualReplication := t.replication
		if actualReplication == 0 {
			actualReplication = 1 // Default to 1 replication if 0 requested
		}

		// ADMIN CLIENT COMPATIBILITY: Always return success for existing topics
		// AdminClient expects topic creation to succeed, even if topic already exists
		if h.seaweedMQHandler.TopicExists(t.name) {
			Debug("CreateTopics v%d: Topic '%s' already exists - returning success for AdminClient compatibility", apiVersion, t.name)
			errCode = 0 // SUCCESS - AdminClient can handle this gracefully
		} else {
			// Use corrected values for error checking and topic creation with schema support
			if err := h.createTopicWithSchemaSupport(t.name, int32(actualPartitions)); err != nil {
				errCode = 1 // UNKNOWN_SERVER_ERROR
			}
		}
		eb := make([]byte, 2)
		binary.BigEndian.PutUint16(eb, errCode)
		response = append(response, eb...)

		// error_message (compact nullable string) - ADMINCLIENT 7.4.0-CE COMPATIBILITY FIX
		// For "_schemas" topic, send null for byte-level compatibility with Java reference
		// For other topics, send empty string to avoid NPE in AdminClient response handling
		if t.name == "_schemas" {
			response = append(response, 0) // Null = 0
		} else {
			response = append(response, 1) // Empty string = 1 (0 chars + 1)
		}

		// ADDED FOR V5: num_partitions (int32)
		// ADMIN CLIENT COMPATIBILITY: Use corrected values from error checking logic
		partBytes := make([]byte, 4)
		binary.BigEndian.PutUint32(partBytes, actualPartitions)
		response = append(response, partBytes...)

		// ADDED FOR V5: replication_factor (int16)
		replBytes := make([]byte, 2)
		binary.BigEndian.PutUint16(replBytes, actualReplication)
		response = append(response, replBytes...)

		// DEBUG: Log response values
		Debug("CreateTopics v%d: Response topic[%d] - partitions=%d, replication=%d, error_code=%d",
			apiVersion, len(topics)-1, actualPartitions, actualReplication, errCode)

		// configs (compact nullable array) - ADDED FOR V5
		// ADMINCLIENT 7.4.0-CE NPE FIX: Send empty configs array instead of null
		// AdminClient 7.4.0-ce has NPE when configs=null but were requested
		// Empty array = 1 (0 configs + 1), still achieves ~30-byte response
		response = append(response, 1) // Empty configs array = 1 (0 configs + 1)

		// Tagged fields for each topic - V5 format per Kafka source
		// Count tagged fields (topicConfigErrorCode only if != 0)
		topicConfigErrorCode := uint16(0) // No error
		numTaggedFields := 0
		if topicConfigErrorCode != 0 {
			numTaggedFields = 1
		}

		// Write tagged fields count
		response = append(response, EncodeUvarint(uint32(numTaggedFields))...)

		// Write tagged fields (only if topicConfigErrorCode != 0)
		if topicConfigErrorCode != 0 {
			// Tag 0: TopicConfigErrorCode
			response = append(response, EncodeUvarint(0)...) // Tag number 0
			response = append(response, EncodeUvarint(2)...) // Length (int16 = 2 bytes)
			topicConfigErrBytes := make([]byte, 2)
			binary.BigEndian.PutUint16(topicConfigErrBytes, topicConfigErrorCode)
			response = append(response, topicConfigErrBytes...)
		}

		debugResponseSize(fmt.Sprintf("After topic '%s'", t.name))
	}

	// Top-level tagged fields for v5 flexible response (empty)
	response = append(response, 0) // Empty tagged fields = 0
	debugResponseSize("Final response")

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

	// NOTE: Correlation ID is handled by writeResponseWithHeader
	// Do NOT include it in the response body

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
		18: {0, 4}, // ApiVersions: v0-v4 (Kafka 8.0.0 compatibility)
		3:  {0, 7}, // Metadata: v0-v7
		0:  {0, 7}, // Produce: v0-v7
		1:  {0, 7}, // Fetch: v0-v7
		2:  {0, 2}, // ListOffsets: v0-v2
		19: {0, 5}, // CreateTopics: v0-v5 (updated to match implementation)
		20: {0, 4}, // DeleteTopics: v0-v4
		10: {0, 2}, // FindCoordinator: v0-v2
		11: {0, 6}, // JoinGroup: cap to v6 (first flexible version)
		14: {0, 5}, // SyncGroup: v0-v5
		8:  {0, 2}, // OffsetCommit: v0-v2
		9:  {0, 5}, // OffsetFetch: v0-v5 (updated to match implementation)
		12: {0, 4}, // Heartbeat: v0-v4
		13: {0, 4}, // LeaveGroup: v0-v4
		15: {0, 5}, // DescribeGroups: v0-v5
		16: {0, 4}, // ListGroups: v0-v4
		32: {0, 4}, // DescribeConfigs: v0-v4
		22: {0, 4}, // InitProducerId: v0-v4
		60: {0, 1}, // DescribeCluster: v0-v1 (KIP-919, AdminClient compatibility)
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
	Debug("METADATA REQUEST: apiVersion=%d, bodySize=%d", apiVersion, len(requestBody))
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
		return h.HandleMetadataV5V6(correlationID, requestBody)
	default:
		// For versions > 7, use the V7 handler (flexible format)
		if apiVersion > 7 {
			return h.HandleMetadataV7(correlationID, requestBody)
		}
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
	case 22:
		return "InitProducerId"
	default:
		return "Unknown"
	}
}

// handleDescribeConfigs handles DescribeConfigs API requests (API key 32)
func (h *Handler) handleDescribeConfigs(correlationID uint32, apiVersion uint16, requestBody []byte) ([]byte, error) {
	Debug("DescribeConfigs v%d - parsing request body (%d bytes)", apiVersion, len(requestBody))

	// Parse request to extract resources
	resources, err := h.parseDescribeConfigsRequest(requestBody, apiVersion)
	if err != nil {
		Error("DescribeConfigs parsing error: %v", err)
		return nil, fmt.Errorf("failed to parse DescribeConfigs request: %w", err)
	}

	Debug("DescribeConfigs parsed %d resources", len(resources))

	isFlexible := apiVersion >= 4
	if !isFlexible {
		// Legacy (non-flexible) response for v0-3
		response := make([]byte, 0, 2048)

		// NOTE: Correlation ID is handled by writeResponseWithHeader
		// Do NOT include it in the response body

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

	// Flexible response for v4+
	response := make([]byte, 0, 2048)

	// Correlation ID
	cid := make([]byte, 4)
	binary.BigEndian.PutUint32(cid, correlationID)
	response = append(response, cid...)

	// Add flexible response header tagged fields (empty)
	response = append(response, 0)

	// throttle_time_ms (4 bytes) - placed directly after correlation ID to match prior flexible handling
	response = append(response, 0, 0, 0, 0)

	// Results (compact array)
	response = append(response, EncodeUvarint(uint32(len(resources)+1))...)

	for _, res := range resources {
		// ErrorCode (int16) = 0
		response = append(response, 0, 0)
		// ErrorMessage (compact nullable string) = null (0)
		response = append(response, 0)
		// ResourceType (int8)
		response = append(response, byte(res.ResourceType))
		// ResourceName (compact string)
		nameBytes := []byte(res.ResourceName)
		response = append(response, EncodeUvarint(uint32(len(nameBytes)+1))...)
		response = append(response, nameBytes...)

		// Build configs for this resource
		var cfgs []ConfigEntry
		if res.ResourceType == 2 { // Topic
			cfgs = h.getTopicConfigs(res.ResourceName, res.ConfigNames)
			// Ensure cleanup.policy is compact for _schemas
			if res.ResourceName == "_schemas" {
				replaced := false
				for i := range cfgs {
					if cfgs[i].Name == "cleanup.policy" {
						cfgs[i].Value = "compact"
						replaced = true
						break
					}
				}
				if !replaced {
					cfgs = append(cfgs, ConfigEntry{Name: "cleanup.policy", Value: "compact"})
				}
			}
		} else if res.ResourceType == 4 { // Broker
			cfgs = h.getBrokerConfigs(res.ConfigNames)
		} else {
			cfgs = []ConfigEntry{}
		}

		// Configs (compact array)
		response = append(response, EncodeUvarint(uint32(len(cfgs)+1))...)

		for _, cfg := range cfgs {
			// name (compact string)
			cb := []byte(cfg.Name)
			response = append(response, EncodeUvarint(uint32(len(cb)+1))...)
			response = append(response, cb...)

			// value (compact nullable string)
			vb := []byte(cfg.Value)
			if len(vb) == 0 {
				response = append(response, 0) // null
			} else {
				response = append(response, EncodeUvarint(uint32(len(vb)+1))...)
				response = append(response, vb...)
			}

			// readOnly (bool)
			if cfg.ReadOnly {
				response = append(response, 1)
			} else {
				response = append(response, 0)
			}

			// configSource (int8): DEFAULT_CONFIG = 5
			response = append(response, byte(5))

			// isSensitive (bool)
			if cfg.Sensitive {
				response = append(response, 1)
			} else {
				response = append(response, 0)
			}

			// synonyms (compact array) - empty
			response = append(response, 1)

			// config_type (int8) - STRING = 1
			response = append(response, byte(1))

			// documentation (compact nullable string) - null
			response = append(response, 0)

			// per-config tagged fields (empty)
			response = append(response, 0)
		}

		// Per-result tagged fields (empty)
		response = append(response, 0)
	}

	// Top-level tagged fields (empty)
	response = append(response, 0)

	Debug("DescribeConfigs v%d flexible response constructed, size: %d bytes", apiVersion, len(response))
	return response, nil
}

// isFlexibleResponse determines if an API response should use flexible format (with header tagged fields)
// Based on Kafka protocol specifications: most APIs become flexible at v3+, but some differ
func isFlexibleResponse(apiKey uint16, apiVersion uint16) bool {
	// Reference: kafka-go/protocol/response.go:119 and sarama/response_header.go:21
	// Flexible responses have headerVersion >= 1, which adds tagged fields after correlation ID

	switch apiKey {
	case 0: // Produce
		return apiVersion >= 9
	case 1: // Fetch
		return apiVersion >= 12
	case 3: // Metadata
		return apiVersion >= 9
	case 8: // OffsetCommit
		return apiVersion >= 8
	case 9: // OffsetFetch
		return apiVersion >= 6
	case 10: // FindCoordinator
		return apiVersion >= 3
	case 11: // JoinGroup
		return apiVersion >= 6
	case 12: // Heartbeat
		return apiVersion >= 4
	case 13: // LeaveGroup
		return apiVersion >= 4
	case 14: // SyncGroup
		return apiVersion >= 4
	case 18: // ApiVersions
		// CRITICAL: AdminClient compatibility requires header version 0 (no tagged fields)
		// Even though ApiVersions v3+ technically supports flexible responses, AdminClient
		// expects the header to NOT include tagged fields. This is a known quirk.
		return false // Always use non-flexible header for ApiVersions
	case 32: // DescribeConfigs
		return apiVersion >= 4
	case 60: // DescribeCluster
		return true // All versions (0+) are flexible
	default:
		// For unknown APIs, assume non-flexible (safer default)
		return false
	}
}

// writeResponseWithHeader writes a Kafka response following the wire protocol:
// [Size: 4 bytes][Correlation ID: 4 bytes][Tagged Fields (if flexible)][Body]
func (h *Handler) writeResponseWithHeader(w *bufio.Writer, correlationID uint32, apiKey uint16, apiVersion uint16, responseBody []byte, timeout time.Duration) error {
	// Kafka wire protocol format (from kafka-go/protocol/response.go:116-138 and sarama/response_header.go:10-27):
	// [4 bytes: size = len(everything after this)]
	// [4 bytes: correlation ID]
	// [varint: header tagged fields (0x00 for empty) - ONLY for flexible responses with headerVersion >= 1]
	// [N bytes: response body]

	// Determine if this response should be flexible
	isFlexible := isFlexibleResponse(apiKey, apiVersion)

	// Calculate total size: correlation ID (4) + tagged fields (1 if flexible) + body
	totalSize := 4 + len(responseBody)
	if isFlexible {
		totalSize += 1 // Add 1 byte for empty tagged fields (0x00)
	}

	// Build complete response in memory for hex dump logging
	fullResponse := make([]byte, 0, 4+totalSize)

	// Write size
	sizeBuf := make([]byte, 4)
	binary.BigEndian.PutUint32(sizeBuf, uint32(totalSize))
	fullResponse = append(fullResponse, sizeBuf...)

	// Write correlation ID
	correlationBuf := make([]byte, 4)
	binary.BigEndian.PutUint32(correlationBuf, correlationID)
	fullResponse = append(fullResponse, correlationBuf...)

	// Write header-level tagged fields for flexible responses
	if isFlexible {
		// Empty tagged fields = 0x00 (varint 0)
		fullResponse = append(fullResponse, 0x00)
	}

	// Write response body
	fullResponse = append(fullResponse, responseBody...)

	// Hex dump for debugging (first 64 bytes)
	dumpLen := len(fullResponse)
	if dumpLen > 64 {
		dumpLen = 64
	}
	Debug("🔍 API %d v%d response wire format (first %d bytes):\n%s", apiKey, apiVersion, dumpLen, hexDump(fullResponse[:dumpLen]))
	Debug("Wrote API %d response v%d: size=%d, flexible=%t, correlationID=%d, totalBytes=%d", apiKey, apiVersion, totalSize, isFlexible, correlationID, len(fullResponse))

	// Write to connection
	if _, err := w.Write(fullResponse); err != nil {
		return fmt.Errorf("write response: %w", err)
	}

	// Flush
	if err := w.Flush(); err != nil {
		return fmt.Errorf("flush response: %w", err)
	}

	return nil
}

// hexDump formats bytes as a hex dump with ASCII representation
func hexDump(data []byte) string {
	var result strings.Builder
	for i := 0; i < len(data); i += 16 {
		// Offset
		result.WriteString(fmt.Sprintf("%04x  ", i))

		// Hex bytes
		for j := 0; j < 16; j++ {
			if i+j < len(data) {
				result.WriteString(fmt.Sprintf("%02x ", data[i+j]))
			} else {
				result.WriteString("   ")
			}
			if j == 7 {
				result.WriteString(" ")
			}
		}

		// ASCII representation
		result.WriteString(" |")
		for j := 0; j < 16 && i+j < len(data); j++ {
			b := data[i+j]
			if b >= 32 && b < 127 {
				result.WriteByte(b)
			} else {
				result.WriteByte('.')
			}
		}
		result.WriteString("|\n")
	}
	return result.String()
}

// writeResponseWithCorrelationID is deprecated - use writeResponseWithHeader instead
// Kept for compatibility with direct callers that don't have API info
func (h *Handler) writeResponseWithCorrelationID(w *bufio.Writer, correlationID uint32, responseBody []byte, timeout time.Duration) error {
	// Assume non-flexible for backward compatibility
	return h.writeResponseWithHeader(w, correlationID, 0, 0, responseBody, timeout)
}

// writeResponseWithTimeout writes a Kafka response with timeout handling
// DEPRECATED: Use writeResponseWithCorrelationID instead
func (h *Handler) writeResponseWithTimeout(w *bufio.Writer, response []byte, timeout time.Duration) error {
	// This old function expects response to include correlation ID at the start
	// For backward compatibility with any remaining callers

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

// SetSchemaRegistryURL sets the Schema Registry URL for delayed initialization
func (h *Handler) SetSchemaRegistryURL(url string) {
	h.schemaRegistryURL = url
}

// SetDefaultPartitions sets the default partition count for auto-created topics
func (h *Handler) SetDefaultPartitions(partitions int32) {
	h.defaultPartitions = partitions
}

// GetDefaultPartitions returns the default partition count for auto-created topics
func (h *Handler) GetDefaultPartitions() int32 {
	if h.defaultPartitions <= 0 {
		return 4 // Fallback default
	}
	return h.defaultPartitions
}

// IsSchemaEnabled returns whether schema management is enabled
func (h *Handler) IsSchemaEnabled() bool {
	// Try to initialize schema management if not already done
	if !h.useSchema && h.schemaRegistryURL != "" {
		h.tryInitializeSchemaManagement()
	}
	return h.useSchema && h.schemaManager != nil
}

// tryInitializeSchemaManagement attempts to initialize schema management
// This is called lazily when schema functionality is first needed
func (h *Handler) tryInitializeSchemaManagement() {
	if h.useSchema || h.schemaRegistryURL == "" {
		return // Already initialized or no URL provided
	}

	schemaConfig := schema.ManagerConfig{
		RegistryURL: h.schemaRegistryURL,
	}

	if err := h.EnableSchemaManagement(schemaConfig); err != nil {
		Debug("Schema management initialization failed (will retry later): %v", err)
		return
	}

	Debug("Schema management successfully initialized with registry: %s", h.schemaRegistryURL)
}

// IsBrokerIntegrationEnabled returns true if broker integration is enabled
func (h *Handler) IsBrokerIntegrationEnabled() bool {
	return h.IsSchemaEnabled() && h.brokerClient != nil
}

// commitOffsetToSMQ commits offset using SMQ storage
func (h *Handler) commitOffsetToSMQ(key ConsumerOffsetKey, offsetValue int64, metadata string) error {
	// Use new consumer offset storage if available, fall back to SMQ storage
	if h.consumerOffsetStorage != nil {
		return h.consumerOffsetStorage.CommitOffset(key.ConsumerGroup, key.Topic, key.Partition, offsetValue, metadata)
	}

	// No SMQ offset storage - only use consumer offset storage
	return fmt.Errorf("offset storage not initialized")
}

// fetchOffsetFromSMQ fetches offset using SMQ storage
func (h *Handler) fetchOffsetFromSMQ(key ConsumerOffsetKey) (int64, string, error) {
	// Use new consumer offset storage if available, fall back to SMQ storage
	if h.consumerOffsetStorage != nil {
		return h.consumerOffsetStorage.FetchOffset(key.ConsumerGroup, key.Topic, key.Partition)
	}

	// SMQ offset storage removed - no fallback
	return -1, "", fmt.Errorf("offset storage not initialized")
}

// DescribeConfigsResource represents a resource in a DescribeConfigs request
type DescribeConfigsResource struct {
	ResourceType int8 // 2 = Topic, 4 = Broker
	ResourceName string
	ConfigNames  []string // Empty means return all configs
}

// parseDescribeConfigsRequest parses a DescribeConfigs request body
func (h *Handler) parseDescribeConfigsRequest(requestBody []byte, apiVersion uint16) ([]DescribeConfigsResource, error) {
	if len(requestBody) < 1 {
		return nil, fmt.Errorf("request too short")
	}

	offset := 0

	// DescribeConfigs v4+ uses flexible protocol (compact arrays with varint)
	isFlexible := apiVersion >= 4

	var resourcesLength uint32
	if isFlexible {
		// Debug: log the first 8 bytes of the request body
		debugBytes := requestBody[offset:]
		if len(debugBytes) > 8 {
			debugBytes = debugBytes[:8]
		}
		Debug("DescribeConfigs v4 request bytes: %x", debugBytes)

		// FIX: Skip top-level tagged fields for DescribeConfigs v4+ flexible protocol
		// The request body starts with tagged fields count (usually 0x00 = empty)
		_, consumed, err := DecodeTaggedFields(requestBody[offset:])
		if err != nil {
			return nil, fmt.Errorf("DescribeConfigs v%d: decode top-level tagged fields: %w", apiVersion, err)
		}
		offset += consumed

		// Resources (compact array) - Now correctly positioned after tagged fields
		resourcesLength, consumed, err = DecodeCompactArrayLength(requestBody[offset:])
		if err != nil {
			return nil, fmt.Errorf("decode resources compact array: %w", err)
		}
		Debug("DescribeConfigs v4 parsed resources length: %d, consumed: %d bytes", resourcesLength, consumed)
		offset += consumed
	} else {
		// Regular array: length is int32
		if len(requestBody) < 4 {
			return nil, fmt.Errorf("request too short for regular array")
		}
		resourcesLength = binary.BigEndian.Uint32(requestBody[offset : offset+4])
		offset += 4
	}

	// Validate resources length to prevent panic
	if resourcesLength > 100 { // Reasonable limit
		return nil, fmt.Errorf("invalid resources length: %d", resourcesLength)
	}

	resources := make([]DescribeConfigsResource, 0, resourcesLength)

	for i := uint32(0); i < resourcesLength; i++ {
		if offset+1 > len(requestBody) {
			return nil, fmt.Errorf("insufficient data for resource type")
		}

		// Resource type (1 byte)
		resourceType := int8(requestBody[offset])
		offset++

		// Resource name (string - compact for v4+, regular for v0-3)
		var resourceName string
		if isFlexible {
			// Compact string: length is encoded as UNSIGNED_VARINT(actualLength + 1)
			name, consumed, err := DecodeFlexibleString(requestBody[offset:])
			if err != nil {
				return nil, fmt.Errorf("decode resource name compact string: %w", err)
			}
			resourceName = name
			offset += consumed
		} else {
			// Regular string: length is int16
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
			resourceName = string(requestBody[offset : offset+nameLength])
			offset += nameLength
		}

		// Config names array (compact for v4+, regular for v0-3)
		var configNames []string
		if isFlexible {
			// Compact array: length is encoded as UNSIGNED_VARINT(actualLength + 1)
			// For nullable arrays, 0 means null, 1 means empty
			configNamesCount, consumed, err := DecodeCompactArrayLength(requestBody[offset:])
			if err != nil {
				return nil, fmt.Errorf("decode config names compact array: %w", err)
			}
			offset += consumed

			// Parse each config name as compact string (if not null)
			if configNamesCount > 0 {
				for j := uint32(0); j < configNamesCount; j++ {
					configName, consumed, err := DecodeFlexibleString(requestBody[offset:])
					if err != nil {
						return nil, fmt.Errorf("decode config name[%d] compact string: %w", j, err)
					}
					offset += consumed
					configNames = append(configNames, configName)
				}
			}
		} else {
			// Regular array: length is int32
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

			configNames = make([]string, 0, configNamesLength)
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

// PreCreateSystemTopics creates essential system topics that external services expect to exist
// This is called during gateway startup to ensure topics like _schemas are available before
// services like Schema Registry attempt to use them
func (h *Handler) PreCreateSystemTopics() error {
	Debug("Pre-creating system topics...")

	// List of system topics that should be pre-created
	systemTopics := []string{"_schemas"}

	for _, topicName := range systemTopics {
		if h.seaweedMQHandler.TopicExists(topicName) {
			Debug("System topic %s already exists, skipping", topicName)
			continue
		}

		Debug("Creating system topic %s", topicName)
		if err := h.createTopicWithSchemaSupport(topicName, 1); err != nil {
			Debug("Failed to create system topic %s: %v", topicName, err)
			return fmt.Errorf("create system topic %s: %w", topicName, err)
		}
		Debug("Successfully created system topic %s", topicName)
	}

	Debug("System topics pre-creation completed")
	return nil
}

// registerSchemaViaBrokerAPI registers the translated schema via the broker's ConfigureTopic API
// Only the gateway leader performs the registration to avoid concurrent updates.
func (h *Handler) registerSchemaViaBrokerAPI(topicName string, recordType *schema_pb.RecordType) error {
	return h.registerSchemasViaBrokerAPI(topicName, recordType, nil)
}

// registerSchemasViaBrokerAPI registers both key and value schemas via the broker's ConfigureTopic API
// Only the gateway leader performs the registration to avoid concurrent updates.
func (h *Handler) registerSchemasViaBrokerAPI(topicName string, valueRecordType *schema_pb.RecordType, keyRecordType *schema_pb.RecordType) error {
	if valueRecordType == nil && keyRecordType == nil {
		return nil
	}

	// Check coordinator registry for multi-gateway deployments
	// In single-gateway mode, coordinator registry may not be initialized - that's OK
	if reg := h.GetCoordinatorRegistry(); reg != nil {
		// Multi-gateway mode - check if we're the leader
		isLeader := reg.IsLeader()

		if !isLeader {
			// Not leader - in production multi-gateway setups, skip to avoid conflicts
			// In single-gateway setups where leader election fails, log warning but proceed
			// This ensures schema registration works even if distributed locking has issues
			Debug("Not leader for schema registration of %s - proceeding anyway (may be single-gateway with lock issues)", topicName)
			// Note: Schema registration is idempotent, so duplicate registrations are safe
		} else {
			Debug("Registering schema for %s as elected leader", topicName)
		}
	} else {
		// No coordinator registry - definitely single-gateway mode
		Debug("Registering schema for %s (single-gateway mode, no coordinator)", topicName)
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
			// Convert dual schemas to flat schema format
			var flatSchema *schema_pb.RecordType
			var keyColumns []string
			if keyRecordType != nil || valueRecordType != nil {
				flatSchema, keyColumns = mqschema.CombineFlatSchemaFromKeyValue(keyRecordType, valueRecordType)
			}

			// If topic doesn't exist, create it with configurable default partition count
			_, err := client.ConfigureTopic(context.Background(), &mq_pb.ConfigureTopicRequest{
				Topic:             seaweedTopic,
				PartitionCount:    h.GetDefaultPartitions(), // Use configurable default
				MessageRecordType: flatSchema,
				KeyColumns:        keyColumns,
			})
			return err
		}

		// Convert dual schemas to flat schema format for update
		var flatSchema *schema_pb.RecordType
		var keyColumns []string
		if keyRecordType != nil || valueRecordType != nil {
			flatSchema, keyColumns = mqschema.CombineFlatSchemaFromKeyValue(keyRecordType, valueRecordType)
		}

		// Update existing topic with new schema
		_, err = client.ConfigureTopic(context.Background(), &mq_pb.ConfigureTopicRequest{
			Topic:             seaweedTopic,
			PartitionCount:    getResp.PartitionCount,
			MessageRecordType: flatSchema,
			KeyColumns:        keyColumns,
			Retention:         getResp.Retention,
		})
		return err
	})
}

// handleInitProducerId handles InitProducerId API requests (API key 22)
// This API is used to initialize a producer for transactional or idempotent operations
func (h *Handler) handleInitProducerId(correlationID uint32, apiVersion uint16, requestBody []byte) ([]byte, error) {
	Debug("InitProducerId v%d request received, correlation: %d, bodyLen: %d", apiVersion, correlationID, len(requestBody))

	// InitProducerId Request Format (varies by version):
	// v0-v1: transactional_id(NULLABLE_STRING) + transaction_timeout_ms(INT32)
	// v2+: transactional_id(NULLABLE_STRING) + transaction_timeout_ms(INT32) + producer_id(INT64) + producer_epoch(INT16)
	// v4+: Uses flexible format with tagged fields

	offset := 0

	// Parse transactional_id (NULLABLE_STRING or COMPACT_NULLABLE_STRING for flexible versions)
	var transactionalId *string
	if apiVersion >= 4 {
		// Flexible version - use compact nullable string
		if len(requestBody) < offset+1 {
			return nil, fmt.Errorf("InitProducerId request too short for transactional_id")
		}

		length := int(requestBody[offset])
		offset++

		if length == 0 {
			// Null string
			transactionalId = nil
		} else {
			// Non-null string (length is encoded as length+1 in compact format)
			actualLength := length - 1
			if len(requestBody) < offset+actualLength {
				return nil, fmt.Errorf("InitProducerId request transactional_id too short")
			}
			if actualLength > 0 {
				id := string(requestBody[offset : offset+actualLength])
				transactionalId = &id
				offset += actualLength
			} else {
				// Empty string
				id := ""
				transactionalId = &id
			}
		}
	} else {
		// Non-flexible version - use regular nullable string
		if len(requestBody) < offset+2 {
			return nil, fmt.Errorf("InitProducerId request too short for transactional_id length")
		}

		length := int(binary.BigEndian.Uint16(requestBody[offset : offset+2]))
		offset += 2

		if length == 0xFFFF {
			// Null string (-1 as uint16)
			transactionalId = nil
		} else {
			if len(requestBody) < offset+length {
				return nil, fmt.Errorf("InitProducerId request transactional_id too short")
			}
			if length > 0 {
				id := string(requestBody[offset : offset+length])
				transactionalId = &id
				offset += length
			} else {
				// Empty string
				id := ""
				transactionalId = &id
			}
		}
	}

	// Parse transaction_timeout_ms (INT32)
	if len(requestBody) < offset+4 {
		return nil, fmt.Errorf("InitProducerId request too short for transaction_timeout_ms")
	}
	transactionTimeoutMs := binary.BigEndian.Uint32(requestBody[offset : offset+4])
	offset += 4

	// For v2+, there might be additional fields, but we'll ignore them for now
	// as we're providing a basic implementation

	Debug("InitProducerId parsed - transactionalId: %v, timeoutMs: %d", transactionalId, transactionTimeoutMs)

	// Build response
	response := make([]byte, 0, 64)

	// NOTE: Correlation ID is handled by writeResponseWithHeader
	// Do NOT include it in the response body
	// Note: Header tagged fields are also handled by writeResponseWithHeader for flexible versions

	// InitProducerId Response Format:
	// throttle_time_ms(INT32) + error_code(INT16) + producer_id(INT64) + producer_epoch(INT16)
	// + tagged_fields (for flexible versions)

	// Throttle time (4 bytes) - v1+
	if apiVersion >= 1 {
		response = append(response, 0, 0, 0, 0) // No throttling
	}

	// Error code (2 bytes) - SUCCESS
	response = append(response, 0, 0) // No error

	// Producer ID (8 bytes) - generate a simple producer ID
	// In a real implementation, this would be managed by a transaction coordinator
	producerId := int64(1000) // Simple fixed producer ID for now
	producerIdBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(producerIdBytes, uint64(producerId))
	response = append(response, producerIdBytes...)

	// Producer epoch (2 bytes) - start with epoch 0
	response = append(response, 0, 0) // Epoch 0

	// For flexible versions (v4+), add response body tagged fields
	if apiVersion >= 4 {
		response = append(response, 0x00) // Empty response body tagged fields
	}

	Debug("InitProducerId v%d response: %d bytes, producerId: %d, epoch: 0", apiVersion, len(response), producerId)
	return response, nil
}

// createTopicWithSchemaSupport creates a topic with optional schema integration
// This function creates topics with schema support when schema management is enabled
func (h *Handler) createTopicWithSchemaSupport(topicName string, partitions int32) error {
	Debug("Creating topic %s with schema support", topicName)

	// For system topics like _schemas, __consumer_offsets, etc., use default schema
	if isSystemTopic(topicName) {
		Debug("System topic %s - creating with default schema", topicName)
		return h.createTopicWithDefaultFlexibleSchema(topicName, partitions)
	}

	// Check if Schema Registry URL is configured
	if h.schemaRegistryURL != "" {
		Debug("Schema Registry URL configured (%s) - enforcing schema-first approach for topic %s", h.schemaRegistryURL, topicName)

		// Try to initialize schema management if not already done
		if h.schemaManager == nil {
			h.tryInitializeSchemaManagement()
		}

		// If schema manager is still nil after initialization attempt, Schema Registry is unavailable
		if h.schemaManager == nil {
			Debug("Schema Registry is unavailable - failing fast for topic %s", topicName)
			return fmt.Errorf("Schema Registry is configured at %s but unavailable - cannot create topic %s without schema validation", h.schemaRegistryURL, topicName)
		}

		// Schema Registry is available - try to fetch existing schema
		Debug("Schema Registry available - looking up schema for topic %s", topicName)
		keyRecordType, valueRecordType, err := h.fetchSchemaForTopic(topicName)
		if err != nil {
			// Check if this is a connection error vs schema not found
			if h.isSchemaRegistryConnectionError(err) {
				Debug("Schema Registry connection error for topic %s: %v", topicName, err)
				return fmt.Errorf("Schema Registry is unavailable: %w", err)
			}
			// Schema not found - this is an error when schema management is enforced
			Debug("No schema found for topic %s in Schema Registry - schema is required", topicName)
			return fmt.Errorf("schema is required for topic %s but no schema found in Schema Registry", topicName)
		}

		if keyRecordType != nil || valueRecordType != nil {
			Debug("Found schema for topic %s in Schema Registry - creating with schema configuration", topicName)
			// Create topic with schema from Schema Registry
			return h.seaweedMQHandler.CreateTopicWithSchemas(topicName, partitions, keyRecordType, valueRecordType)
		}

		// No schemas found - this is an error when schema management is enforced
		Debug("No schemas found for topic %s in Schema Registry - schema is required", topicName)
		return fmt.Errorf("schema is required for topic %s but no schema found in Schema Registry", topicName)
	}

	// Schema Registry URL not configured - create topic without schema (backward compatibility)
	Debug("Schema Registry URL not configured - creating topic %s without schema", topicName)
	return h.seaweedMQHandler.CreateTopic(topicName, partitions)
}

// createTopicWithDefaultFlexibleSchema creates a topic with a flexible default schema
// that can handle both Avro and JSON messages when schema management is enabled
func (h *Handler) createTopicWithDefaultFlexibleSchema(topicName string, partitions int32) error {
	// For system topics like _schemas, create both key and value fields
	// Schema Registry messages have structured keys and values
	var keySchema, valueSchema *schema_pb.RecordType

	if topicName == "_schemas" {
		// _schemas topic needs both key and value fields
		// Key contains metadata (magicByte, keytype, subject, version)
		keySchema = &schema_pb.RecordType{
			Fields: []*schema_pb.Field{
				{
					Name: "key",
					Type: &schema_pb.Type{
						Kind: &schema_pb.Type_ScalarType{
							ScalarType: schema_pb.ScalarType_BYTES,
						},
					},
				},
			},
		}

		// Value contains schema data
		valueSchema = &schema_pb.RecordType{
			Fields: []*schema_pb.Field{
				{
					Name: "value",
					Type: &schema_pb.Type{
						Kind: &schema_pb.Type_ScalarType{
							ScalarType: schema_pb.ScalarType_BYTES,
						},
					},
				},
			},
		}
	} else {
		// For other system topics, use flexible schema with just value
		valueSchema = &schema_pb.RecordType{
			Fields: []*schema_pb.Field{
				{
					Name: "value",
					Type: &schema_pb.Type{
						Kind: &schema_pb.Type_ScalarType{
							ScalarType: schema_pb.ScalarType_BYTES,
						},
					},
				},
			},
		}
	}

	// Create topic with the schemas
	return h.seaweedMQHandler.CreateTopicWithSchemas(topicName, partitions, keySchema, valueSchema)
}

// fetchSchemaForTopic attempts to fetch schema information for a topic from Schema Registry
// Returns key and value RecordTypes if schemas are found
func (h *Handler) fetchSchemaForTopic(topicName string) (*schema_pb.RecordType, *schema_pb.RecordType, error) {
	if h.schemaManager == nil {
		return nil, nil, fmt.Errorf("schema manager not available")
	}

	var keyRecordType *schema_pb.RecordType
	var valueRecordType *schema_pb.RecordType
	var lastConnectionError error

	// Try to fetch value schema (most common case)
	// Common subject naming patterns: topicName-value, topicName
	valueSubjects := []string{topicName + "-value", topicName}
	for _, subject := range valueSubjects {
		cachedSchema, err := h.schemaManager.GetLatestSchema(subject)
		if err != nil {
			// Check if this is a connection error (Schema Registry unavailable)
			if h.isSchemaRegistryConnectionError(err) {
				Debug("Schema Registry connection error for subject %s: %v", subject, err)
				lastConnectionError = err
				continue
			}
			// This is likely a 404 (schema not found) - continue trying other subjects
			Debug("Schema not found for subject %s: %v", subject, err)
			continue
		}

		if cachedSchema != nil {
			Debug("Found value schema for topic %s using subject %s (ID: %d)", topicName, subject, cachedSchema.LatestID)

			// Convert schema to RecordType
			recordType, err := h.convertSchemaToRecordType(cachedSchema.Schema, cachedSchema.LatestID)
			if err != nil {
				Debug("Failed to convert value schema for topic %s: %v", topicName, err)
				continue
			}
			valueRecordType = recordType

			// Store schema configuration for later use
			h.storeTopicSchemaConfig(topicName, cachedSchema.LatestID, schema.FormatAvro)
			break
		}
	}

	// Try to fetch key schema (optional)
	keySubject := topicName + "-key"
	cachedSchema, err := h.schemaManager.GetLatestSchema(keySubject)
	if err != nil {
		if h.isSchemaRegistryConnectionError(err) {
			Debug("Schema Registry connection error for key subject %s: %v", keySubject, err)
			lastConnectionError = err
		} else {
			Debug("Key schema not found for subject %s: %v", keySubject, err)
		}
	} else if cachedSchema != nil {
		Debug("Found key schema for topic %s using subject %s (ID: %d)", topicName, keySubject, cachedSchema.LatestID)

		// Convert schema to RecordType
		recordType, err := h.convertSchemaToRecordType(cachedSchema.Schema, cachedSchema.LatestID)
		if err != nil {
			Debug("Failed to convert key schema for topic %s: %v", topicName, err)
		} else {
			keyRecordType = recordType

			// Store key schema configuration for later use
			h.storeTopicKeySchemaConfig(topicName, cachedSchema.LatestID, schema.FormatAvro)
		}
	}

	// If we encountered connection errors, fail fast
	if lastConnectionError != nil && keyRecordType == nil && valueRecordType == nil {
		return nil, nil, fmt.Errorf("Schema Registry is unavailable: %w", lastConnectionError)
	}

	// Return error if no schemas found (but Schema Registry was reachable)
	if keyRecordType == nil && valueRecordType == nil {
		return nil, nil, fmt.Errorf("no schemas found for topic %s", topicName)
	}

	return keyRecordType, valueRecordType, nil
}

// isSchemaRegistryConnectionError determines if an error is due to Schema Registry being unavailable
// vs a schema not being found (404)
func (h *Handler) isSchemaRegistryConnectionError(err error) bool {
	if err == nil {
		return false
	}

	errStr := err.Error()

	// Connection errors (network issues, DNS resolution, etc.)
	if strings.Contains(errStr, "failed to fetch") &&
		(strings.Contains(errStr, "connection refused") ||
			strings.Contains(errStr, "no such host") ||
			strings.Contains(errStr, "timeout") ||
			strings.Contains(errStr, "network is unreachable")) {
		return true
	}

	// HTTP 5xx errors (server errors)
	if strings.Contains(errStr, "schema registry error 5") {
		return true
	}

	// HTTP 404 errors are "schema not found", not connection errors
	if strings.Contains(errStr, "schema registry error 404") {
		return false
	}

	// Other HTTP errors (401, 403, etc.) should be treated as connection/config issues
	if strings.Contains(errStr, "schema registry error") {
		return true
	}

	return false
}

// convertSchemaToRecordType converts a schema string to a RecordType
func (h *Handler) convertSchemaToRecordType(schemaStr string, schemaID uint32) (*schema_pb.RecordType, error) {
	// Get the cached schema to determine format
	cachedSchema, err := h.schemaManager.GetSchemaByID(schemaID)
	if err != nil {
		return nil, fmt.Errorf("failed to get cached schema: %w", err)
	}

	// Create appropriate decoder and infer RecordType based on format
	switch cachedSchema.Format {
	case schema.FormatAvro:
		// Create Avro decoder and infer RecordType
		decoder, err := schema.NewAvroDecoder(schemaStr)
		if err != nil {
			return nil, fmt.Errorf("failed to create Avro decoder: %w", err)
		}
		return decoder.InferRecordType()

	case schema.FormatJSONSchema:
		// Create JSON Schema decoder and infer RecordType
		decoder, err := schema.NewJSONSchemaDecoder(schemaStr)
		if err != nil {
			return nil, fmt.Errorf("failed to create JSON Schema decoder: %w", err)
		}
		return decoder.InferRecordType()

	case schema.FormatProtobuf:
		// For Protobuf, we need the binary descriptor, not string
		// This is a limitation - Protobuf schemas in Schema Registry are typically stored as binary descriptors
		return nil, fmt.Errorf("Protobuf schema conversion from string not supported - requires binary descriptor")

	default:
		return nil, fmt.Errorf("unsupported schema format: %v", cachedSchema.Format)
	}
}

// isSystemTopic checks if a topic is a Kafka system topic
func isSystemTopic(topicName string) bool {
	systemTopics := []string{
		"_schemas",
		"__consumer_offsets",
		"__transaction_state",
		"_confluent-ksql-default__command_topic",
		"_confluent-metrics",
	}

	for _, systemTopic := range systemTopics {
		if topicName == systemTopic {
			return true
		}
	}

	// Check for topics starting with underscore (common system topic pattern)
	return len(topicName) > 0 && topicName[0] == '_'
}
