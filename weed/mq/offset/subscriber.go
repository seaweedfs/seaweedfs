package offset

import (
	"fmt"
	"sync"

	"github.com/seaweedfs/seaweedfs/weed/pb/schema_pb"
)

// OffsetSubscriber handles offset-based subscription logic
type OffsetSubscriber struct {
	mu             sync.RWMutex
	offsetRegistry *PartitionOffsetRegistry
	subscriptions  map[string]*OffsetSubscription
}

// OffsetSubscription represents an active offset-based subscription
type OffsetSubscription struct {
	ID             string
	Namespace      string
	TopicName      string
	Partition      *schema_pb.Partition
	StartOffset    int64
	CurrentOffset  int64
	OffsetType     schema_pb.OffsetType
	IsActive       bool
	offsetRegistry *PartitionOffsetRegistry
}

// NewOffsetSubscriber creates a new offset-based subscriber
func NewOffsetSubscriber(offsetRegistry *PartitionOffsetRegistry) *OffsetSubscriber {
	return &OffsetSubscriber{
		offsetRegistry: offsetRegistry,
		subscriptions:  make(map[string]*OffsetSubscription),
	}
}

// CreateSubscription creates a new offset-based subscription
func (s *OffsetSubscriber) CreateSubscription(
	subscriptionID string,
	namespace, topicName string,
	partition *schema_pb.Partition,
	offsetType schema_pb.OffsetType,
	startOffset int64,
) (*OffsetSubscription, error) {

	s.mu.Lock()
	defer s.mu.Unlock()

	// Check if subscription already exists
	if _, exists := s.subscriptions[subscriptionID]; exists {
		return nil, fmt.Errorf("subscription %s already exists", subscriptionID)
	}

	// Resolve the actual start offset based on type
	actualStartOffset, err := s.resolveStartOffset(namespace, topicName, partition, offsetType, startOffset)
	if err != nil {
		return nil, fmt.Errorf("failed to resolve start offset: %w", err)
	}

	subscription := &OffsetSubscription{
		ID:             subscriptionID,
		Namespace:      namespace,
		TopicName:      topicName,
		Partition:      partition,
		StartOffset:    actualStartOffset,
		CurrentOffset:  actualStartOffset,
		OffsetType:     offsetType,
		IsActive:       true,
		offsetRegistry: s.offsetRegistry,
	}

	s.subscriptions[subscriptionID] = subscription
	return subscription, nil
}

// GetSubscription retrieves an existing subscription
func (s *OffsetSubscriber) GetSubscription(subscriptionID string) (*OffsetSubscription, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	subscription, exists := s.subscriptions[subscriptionID]
	if !exists {
		return nil, fmt.Errorf("subscription %s not found", subscriptionID)
	}

	return subscription, nil
}

// CloseSubscription closes and removes a subscription
func (s *OffsetSubscriber) CloseSubscription(subscriptionID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	subscription, exists := s.subscriptions[subscriptionID]
	if !exists {
		return fmt.Errorf("subscription %s not found", subscriptionID)
	}

	subscription.IsActive = false
	delete(s.subscriptions, subscriptionID)
	return nil
}

// resolveStartOffset resolves the actual start offset based on OffsetType
func (s *OffsetSubscriber) resolveStartOffset(
	namespace, topicName string,
	partition *schema_pb.Partition,
	offsetType schema_pb.OffsetType,
	requestedOffset int64,
) (int64, error) {

	switch offsetType {
	case schema_pb.OffsetType_EXACT_OFFSET:
		// Validate that the requested offset exists
		return s.validateAndGetOffset(namespace, topicName, partition, requestedOffset)

	case schema_pb.OffsetType_RESET_TO_OFFSET:
		// Use the requested offset, even if it doesn't exist yet
		return requestedOffset, nil

	case schema_pb.OffsetType_RESET_TO_EARLIEST:
		// Start from offset 0
		return 0, nil

	case schema_pb.OffsetType_RESET_TO_LATEST:
		// Start from the current high water mark
		hwm, err := s.offsetRegistry.GetHighWaterMark(namespace, topicName, partition)
		if err != nil {
			return 0, err
		}
		return hwm, nil

	case schema_pb.OffsetType_RESUME_OR_EARLIEST:
		// Try to resume from a saved position, fallback to earliest
		// For now, just use earliest (consumer group position tracking will be added later)
		return 0, nil

	case schema_pb.OffsetType_RESUME_OR_LATEST:
		// Try to resume from a saved position, fallback to latest
		// For now, just use latest
		hwm, err := s.offsetRegistry.GetHighWaterMark(namespace, topicName, partition)
		if err != nil {
			return 0, err
		}
		return hwm, nil

	default:
		return 0, fmt.Errorf("unsupported offset type: %v", offsetType)
	}
}

// validateAndGetOffset validates that an offset exists and returns it
func (s *OffsetSubscriber) validateAndGetOffset(namespace, topicName string, partition *schema_pb.Partition, offset int64) (int64, error) {
	if offset < 0 {
		return 0, fmt.Errorf("offset cannot be negative: %d", offset)
	}

	// Get the current high water mark
	hwm, err := s.offsetRegistry.GetHighWaterMark(namespace, topicName, partition)
	if err != nil {
		return 0, fmt.Errorf("failed to get high water mark: %w", err)
	}

	// Check if offset is within valid range
	if offset >= hwm {
		return 0, fmt.Errorf("offset %d is beyond high water mark %d", offset, hwm)
	}

	return offset, nil
}

// SeekToOffset seeks a subscription to a specific offset
func (sub *OffsetSubscription) SeekToOffset(offset int64) error {
	if !sub.IsActive {
		return fmt.Errorf("subscription is not active")
	}

	// Validate the offset
	if offset < 0 {
		return fmt.Errorf("offset cannot be negative: %d", offset)
	}

	hwm, err := sub.offsetRegistry.GetHighWaterMark(sub.Namespace, sub.TopicName, sub.Partition)
	if err != nil {
		return fmt.Errorf("failed to get high water mark: %w", err)
	}

	if offset > hwm {
		return fmt.Errorf("offset %d is beyond high water mark %d", offset, hwm)
	}

	sub.CurrentOffset = offset
	return nil
}

// GetNextOffset returns the next offset to read
func (sub *OffsetSubscription) GetNextOffset() int64 {
	return sub.CurrentOffset
}

// AdvanceOffset advances the subscription to the next offset
func (sub *OffsetSubscription) AdvanceOffset() {
	sub.CurrentOffset++
}

// GetLag returns the lag between current position and high water mark
func (sub *OffsetSubscription) GetLag() (int64, error) {
	if !sub.IsActive {
		return 0, fmt.Errorf("subscription is not active")
	}

	hwm, err := sub.offsetRegistry.GetHighWaterMark(sub.Namespace, sub.TopicName, sub.Partition)
	if err != nil {
		return 0, fmt.Errorf("failed to get high water mark: %w", err)
	}

	lag := hwm - sub.CurrentOffset
	if lag < 0 {
		lag = 0
	}

	return lag, nil
}

// IsAtEnd checks if the subscription has reached the end of available data
func (sub *OffsetSubscription) IsAtEnd() (bool, error) {
	if !sub.IsActive {
		return true, fmt.Errorf("subscription is not active")
	}

	hwm, err := sub.offsetRegistry.GetHighWaterMark(sub.Namespace, sub.TopicName, sub.Partition)
	if err != nil {
		return false, fmt.Errorf("failed to get high water mark: %w", err)
	}

	return sub.CurrentOffset >= hwm, nil
}

// OffsetRange represents a range of offsets
type OffsetRange struct {
	StartOffset int64
	EndOffset   int64
	Count       int64
}

// GetOffsetRange returns a range of offsets for batch reading
func (sub *OffsetSubscription) GetOffsetRange(maxCount int64) (*OffsetRange, error) {
	if !sub.IsActive {
		return nil, fmt.Errorf("subscription is not active")
	}

	hwm, err := sub.offsetRegistry.GetHighWaterMark(sub.Namespace, sub.TopicName, sub.Partition)
	if err != nil {
		return nil, fmt.Errorf("failed to get high water mark: %w", err)
	}

	startOffset := sub.CurrentOffset
	endOffset := startOffset + maxCount - 1

	// Don't go beyond high water mark
	if endOffset >= hwm {
		endOffset = hwm - 1
	}

	// If start is already at or beyond HWM, return empty range
	if startOffset >= hwm {
		return &OffsetRange{
			StartOffset: startOffset,
			EndOffset:   startOffset - 1, // Empty range
			Count:       0,
		}, nil
	}

	count := endOffset - startOffset + 1
	return &OffsetRange{
		StartOffset: startOffset,
		EndOffset:   endOffset,
		Count:       count,
	}, nil
}

// AdvanceOffsetBy advances the subscription by a specific number of offsets
func (sub *OffsetSubscription) AdvanceOffsetBy(count int64) {
	sub.CurrentOffset += count
}

// OffsetSeeker provides utilities for offset-based seeking
type OffsetSeeker struct {
	offsetRegistry *PartitionOffsetRegistry
}

// NewOffsetSeeker creates a new offset seeker
func NewOffsetSeeker(offsetRegistry *PartitionOffsetRegistry) *OffsetSeeker {
	return &OffsetSeeker{
		offsetRegistry: offsetRegistry,
	}
}

// SeekToTimestamp finds the offset closest to a given timestamp
// This bridges offset-based and timestamp-based seeking
func (seeker *OffsetSeeker) SeekToTimestamp(partition *schema_pb.Partition, timestamp int64) (int64, error) {
	// TODO: This requires integration with the storage layer to map timestamps to offsets
	// For now, return an error indicating this feature needs implementation
	return 0, fmt.Errorf("timestamp-to-offset mapping not implemented yet")
}

// ValidateOffsetRange validates that an offset range is valid
func (seeker *OffsetSeeker) ValidateOffsetRange(namespace, topicName string, partition *schema_pb.Partition, startOffset, endOffset int64) error {
	if startOffset < 0 {
		return fmt.Errorf("start offset cannot be negative: %d", startOffset)
	}

	if endOffset < startOffset {
		return fmt.Errorf("end offset %d cannot be less than start offset %d", endOffset, startOffset)
	}

	hwm, err := seeker.offsetRegistry.GetHighWaterMark(namespace, topicName, partition)
	if err != nil {
		return fmt.Errorf("failed to get high water mark: %w", err)
	}

	if startOffset >= hwm {
		return fmt.Errorf("start offset %d is beyond high water mark %d", startOffset, hwm)
	}

	if endOffset >= hwm {
		return fmt.Errorf("end offset %d is beyond high water mark %d", endOffset, hwm)
	}

	return nil
}

// GetAvailableOffsetRange returns the range of available offsets for a partition
func (seeker *OffsetSeeker) GetAvailableOffsetRange(namespace, topicName string, partition *schema_pb.Partition) (*OffsetRange, error) {
	hwm, err := seeker.offsetRegistry.GetHighWaterMark(namespace, topicName, partition)
	if err != nil {
		return nil, fmt.Errorf("failed to get high water mark: %w", err)
	}

	if hwm == 0 {
		// No data available
		return &OffsetRange{
			StartOffset: 0,
			EndOffset:   -1,
			Count:       0,
		}, nil
	}

	return &OffsetRange{
		StartOffset: 0,
		EndOffset:   hwm - 1,
		Count:       hwm,
	}, nil
}
