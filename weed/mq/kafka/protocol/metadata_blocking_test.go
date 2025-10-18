package protocol

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/mq/kafka/integration"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/schema_pb"
)

// TestMetadataRequestBlocking documents the original bug where Metadata requests hang
// when the backend (broker/filer) ListTopics call blocks indefinitely.
// This test is kept for documentation purposes and to verify the mock handler behavior.
//
// NOTE: The actual fix is in the broker's ListTopics implementation (weed/mq/broker/broker_grpc_lookup.go)
// which adds a 2-second timeout for filer operations. This test uses a mock handler that
// bypasses that fix, so it still demonstrates the original blocking behavior.
func TestMetadataRequestBlocking(t *testing.T) {
	t.Skip("This test documents the original bug. The fix is in the broker's ListTopics with filer timeout. Run TestMetadataRequestWithFastMock to verify fast path works.")

	t.Log("Testing Metadata handler with blocking backend...")

	// Create a handler with a mock backend that blocks on ListTopics
	handler := &Handler{
		seaweedMQHandler: &BlockingMockHandler{
			blockDuration: 10 * time.Second, // Simulate slow backend
		},
	}

	// Call handleMetadata in a goroutine so we can timeout
	responseChan := make(chan []byte, 1)
	errorChan := make(chan error, 1)

	go func() {
		// Build a simple Metadata v1 request body (empty topics array = all topics)
		requestBody := []byte{0, 0, 0, 0} // Empty topics array
		response, err := handler.handleMetadata(1, 1, requestBody)
		if err != nil {
			errorChan <- err
		} else {
			responseChan <- response
		}
	}()

	// Wait for response with timeout
	select {
	case response := <-responseChan:
		t.Logf("Metadata response received (%d bytes) - backend responded", len(response))
		t.Error("UNEXPECTED: Response received before timeout - backend should have blocked")
	case err := <-errorChan:
		t.Logf("Metadata returned error: %v", err)
		t.Error("UNEXPECTED: Error received - expected blocking, not error")
	case <-time.After(3 * time.Second):
		t.Logf("✓ BUG REPRODUCED: Metadata request blocked for 3+ seconds")
		t.Logf("  Root cause: seaweedMQHandler.ListTopics() blocks indefinitely when broker/filer is slow")
		t.Logf("  Impact: Entire control plane processor goroutine is frozen")
		t.Logf("  Fix implemented: Broker's ListTopics now has 2-second timeout for filer operations")
		// This is expected behavior with blocking mock - demonstrates the original issue
	}
}

// TestMetadataRequestWithFastMock verifies that Metadata requests complete quickly
// when the backend responds promptly (the common case)
func TestMetadataRequestWithFastMock(t *testing.T) {
	t.Log("Testing Metadata handler with fast-responding backend...")

	// Create a handler with a fast mock (simulates in-memory topics only)
	handler := &Handler{
		seaweedMQHandler: &FastMockHandler{
			topics: []string{"test-topic-1", "test-topic-2"},
		},
	}

	// Call handleMetadata and measure time
	start := time.Now()
	requestBody := []byte{0, 0, 0, 0} // Empty topics array = list all
	response, err := handler.handleMetadata(1, 1, requestBody)
	duration := time.Since(start)

	if err != nil {
		t.Errorf("Metadata returned error: %v", err)
	} else if response == nil {
		t.Error("Metadata returned nil response")
	} else {
		t.Logf("✓ Metadata completed in %v (%d bytes)", duration, len(response))
		if duration > 500*time.Millisecond {
			t.Errorf("Metadata took too long: %v (should be < 500ms for fast backend)", duration)
		}
	}
}

// TestMetadataRequestWithTimeoutFix tests that Metadata requests with timeout-aware backend
// complete within reasonable time even when underlying storage is slow
func TestMetadataRequestWithTimeoutFix(t *testing.T) {
	t.Log("Testing Metadata handler with timeout-aware backend...")

	// Create a handler with a timeout-aware mock
	// This simulates the broker's ListTopics with 2-second filer timeout
	handler := &Handler{
		seaweedMQHandler: &TimeoutAwareMockHandler{
			timeout:       2 * time.Second,
			blockDuration: 10 * time.Second, // Backend is slow but timeout kicks in
		},
	}

	// Call handleMetadata and measure time
	start := time.Now()
	requestBody := []byte{0, 0, 0, 0} // Empty topics array
	response, err := handler.handleMetadata(1, 1, requestBody)
	duration := time.Since(start)

	t.Logf("Metadata completed in %v", duration)

	if err != nil {
		t.Logf("✓ Metadata returned error after timeout: %v", err)
		// This is acceptable - error response is better than hanging
	} else if response != nil {
		t.Logf("✓ Metadata returned response (%d bytes) without blocking", len(response))
		// Backend timed out but still returned in-memory topics
		if duration > 3*time.Second {
			t.Errorf("Metadata took too long: %v (should timeout at ~2s)", duration)
		}
	} else {
		t.Error("Metadata returned nil response and nil error - unexpected")
	}
}

// FastMockHandler simulates a fast backend (in-memory topics only)
type FastMockHandler struct {
	topics []string
}

func (h *FastMockHandler) ListTopics() []string {
	// Fast response - simulates in-memory topics
	return h.topics
}

func (h *FastMockHandler) TopicExists(name string) bool {
	for _, topic := range h.topics {
		if topic == name {
			return true
		}
	}
	return false
}

func (h *FastMockHandler) CreateTopic(name string, partitions int32) error {
	return fmt.Errorf("not implemented")
}

func (h *FastMockHandler) CreateTopicWithSchemas(name string, partitions int32, keyRecordType *schema_pb.RecordType, valueRecordType *schema_pb.RecordType) error {
	return fmt.Errorf("not implemented")
}

func (h *FastMockHandler) DeleteTopic(name string) error {
	return fmt.Errorf("not implemented")
}

func (h *FastMockHandler) GetTopicInfo(name string) (*integration.KafkaTopicInfo, bool) {
	return nil, false
}

func (h *FastMockHandler) ProduceRecord(ctx context.Context, topicName string, partitionID int32, key, value []byte) (int64, error) {
	return 0, fmt.Errorf("not implemented")
}

func (h *FastMockHandler) ProduceRecordValue(ctx context.Context, topicName string, partitionID int32, key []byte, recordValueBytes []byte) (int64, error) {
	return 0, fmt.Errorf("not implemented")
}

func (h *FastMockHandler) GetStoredRecords(ctx context.Context, topic string, partition int32, fromOffset int64, maxRecords int) ([]integration.SMQRecord, error) {
	return nil, fmt.Errorf("not implemented")
}

func (h *FastMockHandler) GetEarliestOffset(topic string, partition int32) (int64, error) {
	return 0, fmt.Errorf("not implemented")
}

func (h *FastMockHandler) GetLatestOffset(topic string, partition int32) (int64, error) {
	return 0, fmt.Errorf("not implemented")
}

func (h *FastMockHandler) WithFilerClient(streamingMode bool, fn func(client filer_pb.SeaweedFilerClient) error) error {
	return fmt.Errorf("not implemented")
}

func (h *FastMockHandler) GetBrokerAddresses() []string {
	return []string{"localhost:17777"}
}

func (h *FastMockHandler) CreatePerConnectionBrokerClient() (*integration.BrokerClient, error) {
	return nil, fmt.Errorf("not implemented")
}

func (h *FastMockHandler) SetProtocolHandler(handler integration.ProtocolHandler) {
	// No-op
}

func (h *FastMockHandler) InvalidateTopicExistsCache(topic string) {
	// No-op for mock
}

func (h *FastMockHandler) Close() error {
	return nil
}

// BlockingMockHandler simulates a backend that blocks indefinitely on ListTopics
type BlockingMockHandler struct {
	blockDuration time.Duration
}

func (h *BlockingMockHandler) ListTopics() []string {
	// Simulate backend blocking (e.g., waiting for unresponsive broker/filer)
	time.Sleep(h.blockDuration)
	return []string{}
}

func (h *BlockingMockHandler) TopicExists(name string) bool {
	return false
}

func (h *BlockingMockHandler) CreateTopic(name string, partitions int32) error {
	return fmt.Errorf("not implemented")
}

func (h *BlockingMockHandler) CreateTopicWithSchemas(name string, partitions int32, keyRecordType *schema_pb.RecordType, valueRecordType *schema_pb.RecordType) error {
	return fmt.Errorf("not implemented")
}

func (h *BlockingMockHandler) DeleteTopic(name string) error {
	return fmt.Errorf("not implemented")
}

func (h *BlockingMockHandler) GetTopicInfo(name string) (*integration.KafkaTopicInfo, bool) {
	return nil, false
}

func (h *BlockingMockHandler) ProduceRecord(ctx context.Context, topicName string, partitionID int32, key, value []byte) (int64, error) {
	return 0, fmt.Errorf("not implemented")
}

func (h *BlockingMockHandler) ProduceRecordValue(ctx context.Context, topicName string, partitionID int32, key []byte, recordValueBytes []byte) (int64, error) {
	return 0, fmt.Errorf("not implemented")
}

func (h *BlockingMockHandler) GetStoredRecords(ctx context.Context, topic string, partition int32, fromOffset int64, maxRecords int) ([]integration.SMQRecord, error) {
	return nil, fmt.Errorf("not implemented")
}

func (h *BlockingMockHandler) GetEarliestOffset(topic string, partition int32) (int64, error) {
	return 0, fmt.Errorf("not implemented")
}

func (h *BlockingMockHandler) GetLatestOffset(topic string, partition int32) (int64, error) {
	return 0, fmt.Errorf("not implemented")
}

func (h *BlockingMockHandler) WithFilerClient(streamingMode bool, fn func(client filer_pb.SeaweedFilerClient) error) error {
	return fmt.Errorf("not implemented")
}

func (h *BlockingMockHandler) GetBrokerAddresses() []string {
	return []string{"localhost:17777"}
}

func (h *BlockingMockHandler) CreatePerConnectionBrokerClient() (*integration.BrokerClient, error) {
	return nil, fmt.Errorf("not implemented")
}

func (h *BlockingMockHandler) SetProtocolHandler(handler integration.ProtocolHandler) {
	// No-op
}

func (h *BlockingMockHandler) InvalidateTopicExistsCache(topic string) {
	// No-op for mock
}

func (h *BlockingMockHandler) Close() error {
	return nil
}

// TimeoutAwareMockHandler demonstrates expected behavior with timeout
type TimeoutAwareMockHandler struct {
	timeout       time.Duration
	blockDuration time.Duration
}

func (h *TimeoutAwareMockHandler) ListTopics() []string {
	// Simulate timeout-aware backend
	ctx, cancel := context.WithTimeout(context.Background(), h.timeout)
	defer cancel()

	done := make(chan bool)
	go func() {
		time.Sleep(h.blockDuration)
		done <- true
	}()

	select {
	case <-done:
		return []string{}
	case <-ctx.Done():
		// Timeout - return empty list rather than blocking forever
		return []string{}
	}
}

func (h *TimeoutAwareMockHandler) TopicExists(name string) bool {
	return false
}

func (h *TimeoutAwareMockHandler) CreateTopic(name string, partitions int32) error {
	return fmt.Errorf("not implemented")
}

func (h *TimeoutAwareMockHandler) CreateTopicWithSchemas(name string, partitions int32, keyRecordType *schema_pb.RecordType, valueRecordType *schema_pb.RecordType) error {
	return fmt.Errorf("not implemented")
}

func (h *TimeoutAwareMockHandler) DeleteTopic(name string) error {
	return fmt.Errorf("not implemented")
}

func (h *TimeoutAwareMockHandler) GetTopicInfo(name string) (*integration.KafkaTopicInfo, bool) {
	return nil, false
}

func (h *TimeoutAwareMockHandler) ProduceRecord(ctx context.Context, topicName string, partitionID int32, key, value []byte) (int64, error) {
	return 0, fmt.Errorf("not implemented")
}

func (h *TimeoutAwareMockHandler) ProduceRecordValue(ctx context.Context, topicName string, partitionID int32, key []byte, recordValueBytes []byte) (int64, error) {
	return 0, fmt.Errorf("not implemented")
}

func (h *TimeoutAwareMockHandler) GetStoredRecords(ctx context.Context, topic string, partition int32, fromOffset int64, maxRecords int) ([]integration.SMQRecord, error) {
	return nil, fmt.Errorf("not implemented")
}

func (h *TimeoutAwareMockHandler) GetEarliestOffset(topic string, partition int32) (int64, error) {
	return 0, fmt.Errorf("not implemented")
}

func (h *TimeoutAwareMockHandler) GetLatestOffset(topic string, partition int32) (int64, error) {
	return 0, fmt.Errorf("not implemented")
}

func (h *TimeoutAwareMockHandler) WithFilerClient(streamingMode bool, fn func(client filer_pb.SeaweedFilerClient) error) error {
	return fmt.Errorf("not implemented")
}

func (h *TimeoutAwareMockHandler) GetBrokerAddresses() []string {
	return []string{"localhost:17777"}
}

func (h *TimeoutAwareMockHandler) CreatePerConnectionBrokerClient() (*integration.BrokerClient, error) {
	return nil, fmt.Errorf("not implemented")
}

func (h *TimeoutAwareMockHandler) SetProtocolHandler(handler integration.ProtocolHandler) {
	// No-op
}

func (h *TimeoutAwareMockHandler) InvalidateTopicExistsCache(topic string) {
	// No-op for mock
}

func (h *TimeoutAwareMockHandler) Close() error {
	return nil
}
