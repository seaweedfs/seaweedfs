package offset

import (
	"fmt"
	"sync"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/mq_agent_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/schema_pb"
)

// SMQOffsetIntegration provides integration between offset management and SMQ broker
type SMQOffsetIntegration struct {
	mu               sync.RWMutex
	registry         *PartitionOffsetRegistry
	offsetAssigner   *OffsetAssigner
	offsetSubscriber *OffsetSubscriber
	offsetSeeker     *OffsetSeeker
}

// NewSMQOffsetIntegration creates a new SMQ offset integration
func NewSMQOffsetIntegration(storage OffsetStorage) *SMQOffsetIntegration {
	registry := NewPartitionOffsetRegistry(storage)
	assigner := &OffsetAssigner{registry: registry}

	return &SMQOffsetIntegration{
		registry:         registry,
		offsetAssigner:   assigner,
		offsetSubscriber: NewOffsetSubscriber(registry),
		offsetSeeker:     NewOffsetSeeker(registry),
	}
}

// Close stops all background checkpoint goroutines and performs final checkpoints
func (integration *SMQOffsetIntegration) Close() error {
	return integration.registry.Close()
}

// PublishRecord publishes a record and assigns it an offset
func (integration *SMQOffsetIntegration) PublishRecord(
	namespace, topicName string,
	partition *schema_pb.Partition,
	key []byte,
	value *schema_pb.RecordValue,
) (*mq_agent_pb.PublishRecordResponse, error) {

	// Assign offset for this record
	result := integration.offsetAssigner.AssignSingleOffset(namespace, topicName, partition)
	if result.Error != nil {
		return &mq_agent_pb.PublishRecordResponse{
			Error: fmt.Sprintf("Failed to assign offset: %v", result.Error),
		}, nil
	}

	assignment := result.Assignment

	// Note: Removed in-memory mapping storage to prevent memory leaks
	// Record-to-offset mappings are now handled by persistent storage layer

	// Return response with offset information
	return &mq_agent_pb.PublishRecordResponse{
		AckSequence: assignment.Offset, // Use offset as ack sequence for now
		BaseOffset:  assignment.Offset,
		LastOffset:  assignment.Offset,
		Error:       "",
	}, nil
}

// PublishRecordBatch publishes a batch of records and assigns them offsets
func (integration *SMQOffsetIntegration) PublishRecordBatch(
	namespace, topicName string,
	partition *schema_pb.Partition,
	records []PublishRecordRequest,
) (*mq_agent_pb.PublishRecordResponse, error) {

	if len(records) == 0 {
		return &mq_agent_pb.PublishRecordResponse{
			Error: "Empty record batch",
		}, nil
	}

	// Assign batch of offsets
	result := integration.offsetAssigner.AssignBatchOffsets(namespace, topicName, partition, int64(len(records)))
	if result.Error != nil {
		return &mq_agent_pb.PublishRecordResponse{
			Error: fmt.Sprintf("Failed to assign batch offsets: %v", result.Error),
		}, nil
	}

	batch := result.Batch

	// Note: Removed in-memory mapping storage to prevent memory leaks
	// Batch record-to-offset mappings are now handled by persistent storage layer

	return &mq_agent_pb.PublishRecordResponse{
		AckSequence: batch.LastOffset, // Use last offset as ack sequence
		BaseOffset:  batch.BaseOffset,
		LastOffset:  batch.LastOffset,
		Error:       "",
	}, nil
}

// CreateSubscription creates an offset-based subscription
func (integration *SMQOffsetIntegration) CreateSubscription(
	subscriptionID string,
	namespace, topicName string,
	partition *schema_pb.Partition,
	offsetType schema_pb.OffsetType,
	startOffset int64,
) (*OffsetSubscription, error) {

	return integration.offsetSubscriber.CreateSubscription(
		subscriptionID,
		namespace, topicName,
		partition,
		offsetType,
		startOffset,
	)
}

// SubscribeRecords subscribes to records starting from a specific offset
func (integration *SMQOffsetIntegration) SubscribeRecords(
	subscription *OffsetSubscription,
	maxRecords int64,
) ([]*mq_agent_pb.SubscribeRecordResponse, error) {

	if !subscription.IsActive {
		return nil, fmt.Errorf("subscription is not active")
	}

	// Get the range of offsets to read
	offsetRange, err := subscription.GetOffsetRange(maxRecords)
	if err != nil {
		return nil, fmt.Errorf("failed to get offset range: %w", err)
	}

	if offsetRange.Count == 0 {
		// No records available
		return []*mq_agent_pb.SubscribeRecordResponse{}, nil
	}

	// TODO: This is where we would integrate with SMQ's actual storage layer
	// For now, return mock responses with offset information
	responses := make([]*mq_agent_pb.SubscribeRecordResponse, offsetRange.Count)

	for i := int64(0); i < offsetRange.Count; i++ {
		offset := offsetRange.StartOffset + i

		responses[i] = &mq_agent_pb.SubscribeRecordResponse{
			Key:           []byte(fmt.Sprintf("key-%d", offset)),
			Value:         &schema_pb.RecordValue{}, // Mock value
			TsNs:          offset * 1000000,         // Mock timestamp based on offset
			Offset:        offset,
			IsEndOfStream: false,
			IsEndOfTopic:  false,
			Error:         "",
		}
	}

	// Advance the subscription
	subscription.AdvanceOffsetBy(offsetRange.Count)

	return responses, nil
}

// GetHighWaterMark returns the high water mark for a partition
func (integration *SMQOffsetIntegration) GetHighWaterMark(namespace, topicName string, partition *schema_pb.Partition) (int64, error) {
	return integration.offsetAssigner.GetHighWaterMark(namespace, topicName, partition)
}

// SeekSubscription seeks a subscription to a specific offset
func (integration *SMQOffsetIntegration) SeekSubscription(
	subscriptionID string,
	offset int64,
) error {

	subscription, err := integration.offsetSubscriber.GetSubscription(subscriptionID)
	if err != nil {
		return fmt.Errorf("subscription not found: %w", err)
	}

	return subscription.SeekToOffset(offset)
}

// GetSubscriptionLag returns the lag for a subscription
func (integration *SMQOffsetIntegration) GetSubscriptionLag(subscriptionID string) (int64, error) {
	subscription, err := integration.offsetSubscriber.GetSubscription(subscriptionID)
	if err != nil {
		return 0, fmt.Errorf("subscription not found: %w", err)
	}

	return subscription.GetLag()
}

// CloseSubscription closes a subscription
func (integration *SMQOffsetIntegration) CloseSubscription(subscriptionID string) error {
	return integration.offsetSubscriber.CloseSubscription(subscriptionID)
}

// ValidateOffsetRange validates an offset range for a partition
func (integration *SMQOffsetIntegration) ValidateOffsetRange(
	namespace, topicName string,
	partition *schema_pb.Partition,
	startOffset, endOffset int64,
) error {

	return integration.offsetSeeker.ValidateOffsetRange(namespace, topicName, partition, startOffset, endOffset)
}

// GetAvailableOffsetRange returns the available offset range for a partition
func (integration *SMQOffsetIntegration) GetAvailableOffsetRange(namespace, topicName string, partition *schema_pb.Partition) (*OffsetRange, error) {
	return integration.offsetSeeker.GetAvailableOffsetRange(namespace, topicName, partition)
}

// PublishRecordRequest represents a record to be published
type PublishRecordRequest struct {
	Key   []byte
	Value *schema_pb.RecordValue
}

// OffsetMetrics provides metrics about offset usage
type OffsetMetrics struct {
	PartitionCount      int64
	TotalOffsets        int64
	ActiveSubscriptions int64
	AverageLatency      float64
}

// GetOffsetMetrics returns metrics about offset usage
func (integration *SMQOffsetIntegration) GetOffsetMetrics() *OffsetMetrics {
	integration.mu.RLock()
	defer integration.mu.RUnlock()

	// Count active subscriptions
	activeSubscriptions := int64(0)
	for _, subscription := range integration.offsetSubscriber.subscriptions {
		if subscription.IsActive {
			activeSubscriptions++
		}
	}

	// Calculate total offsets from all partition managers instead of in-memory map
	var totalOffsets int64
	for _, manager := range integration.offsetAssigner.registry.managers {
		totalOffsets += manager.GetHighWaterMark()
	}

	return &OffsetMetrics{
		PartitionCount:      int64(len(integration.offsetAssigner.registry.managers)),
		TotalOffsets:        totalOffsets, // Now calculated from storage, not memory maps
		ActiveSubscriptions: activeSubscriptions,
		AverageLatency:      0.0, // TODO: Implement latency tracking
	}
}

// OffsetInfo provides detailed information about an offset
type OffsetInfo struct {
	Offset    int64
	Timestamp int64
	Partition *schema_pb.Partition
	Exists    bool
}

// GetOffsetInfo returns detailed information about a specific offset
func (integration *SMQOffsetIntegration) GetOffsetInfo(
	namespace, topicName string,
	partition *schema_pb.Partition,
	offset int64,
) (*OffsetInfo, error) {

	hwm, err := integration.GetHighWaterMark(namespace, topicName, partition)
	if err != nil {
		return nil, fmt.Errorf("failed to get high water mark: %w", err)
	}

	exists := offset >= 0 && offset < hwm

	// TODO: Get actual timestamp from storage
	timestamp := int64(0)
	// Note: Timestamp lookup from in-memory map removed to prevent memory leaks
	// For now, use a placeholder timestamp. In production, this should come from
	// persistent storage if timestamp tracking is needed.
	if exists {
		timestamp = time.Now().UnixNano() // Placeholder - should come from storage
	}

	return &OffsetInfo{
		Offset:    offset,
		Timestamp: timestamp,
		Partition: partition,
		Exists:    exists,
	}, nil
}

// PartitionOffsetInfo provides offset information for a partition
type PartitionOffsetInfo struct {
	Partition           *schema_pb.Partition
	EarliestOffset      int64
	LatestOffset        int64
	HighWaterMark       int64
	RecordCount         int64
	ActiveSubscriptions int64
}

// GetPartitionOffsetInfo returns comprehensive offset information for a partition
func (integration *SMQOffsetIntegration) GetPartitionOffsetInfo(namespace, topicName string, partition *schema_pb.Partition) (*PartitionOffsetInfo, error) {
	hwm, err := integration.GetHighWaterMark(namespace, topicName, partition)
	if err != nil {
		return nil, fmt.Errorf("failed to get high water mark: %w", err)
	}

	earliestOffset := int64(0)
	latestOffset := hwm - 1
	if hwm == 0 {
		latestOffset = -1 // No records
	}

	// Count active subscriptions for this partition
	activeSubscriptions := int64(0)
	integration.mu.RLock()
	for _, subscription := range integration.offsetSubscriber.subscriptions {
		if subscription.IsActive && partitionKey(subscription.Partition) == partitionKey(partition) {
			activeSubscriptions++
		}
	}
	integration.mu.RUnlock()

	return &PartitionOffsetInfo{
		Partition:           partition,
		EarliestOffset:      earliestOffset,
		LatestOffset:        latestOffset,
		HighWaterMark:       hwm,
		RecordCount:         hwm,
		ActiveSubscriptions: activeSubscriptions,
	}, nil
}

// GetSubscription retrieves an existing subscription
func (integration *SMQOffsetIntegration) GetSubscription(subscriptionID string) (*OffsetSubscription, error) {
	return integration.offsetSubscriber.GetSubscription(subscriptionID)
}

// ListActiveSubscriptions returns all active subscriptions
func (integration *SMQOffsetIntegration) ListActiveSubscriptions() ([]*OffsetSubscription, error) {
	integration.mu.RLock()
	defer integration.mu.RUnlock()

	result := make([]*OffsetSubscription, 0)
	for _, subscription := range integration.offsetSubscriber.subscriptions {
		if subscription.IsActive {
			result = append(result, subscription)
		}
	}

	return result, nil
}

// AssignSingleOffset assigns a single offset for a partition
func (integration *SMQOffsetIntegration) AssignSingleOffset(namespace, topicName string, partition *schema_pb.Partition) *AssignmentResult {
	return integration.offsetAssigner.AssignSingleOffset(namespace, topicName, partition)
}

// AssignBatchOffsets assigns a batch of offsets for a partition
func (integration *SMQOffsetIntegration) AssignBatchOffsets(namespace, topicName string, partition *schema_pb.Partition, count int64) *AssignmentResult {
	return integration.offsetAssigner.AssignBatchOffsets(namespace, topicName, partition, count)
}

// Reset resets the integration layer state (for testing)
func (integration *SMQOffsetIntegration) Reset() {
	integration.mu.Lock()
	defer integration.mu.Unlock()

	// Note: No in-memory maps to clear (removed to prevent memory leaks)

	// Close all subscriptions
	for _, subscription := range integration.offsetSubscriber.subscriptions {
		subscription.IsActive = false
	}
	integration.offsetSubscriber.subscriptions = make(map[string]*OffsetSubscription)

	// Reset the registries by creating new ones with the same storage
	// This ensures that partition managers start fresh
	registry := NewPartitionOffsetRegistry(integration.offsetAssigner.registry.storage)
	integration.offsetAssigner.registry = registry
	integration.offsetSubscriber.offsetRegistry = registry
	integration.offsetSeeker.offsetRegistry = registry
}
