package offset

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/filer_client"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"google.golang.org/grpc"
)

// PersistentLedger extends Ledger with persistence capabilities
type PersistentLedger struct {
	*Ledger
	topicPartition string
	storage        LedgerStorage
}

// ConsumerOffsetKey represents the full key for consumer offset storage
type ConsumerOffsetKey struct {
	Topic                 string
	Partition             int32
	ConsumerGroup         string
	ConsumerGroupInstance string // Optional - can be empty
}

// String returns the string representation for use as map key
func (k ConsumerOffsetKey) String() string {
	if k.ConsumerGroupInstance != "" {
		return fmt.Sprintf("%s:%d:%s:%s", k.Topic, k.Partition, k.ConsumerGroup, k.ConsumerGroupInstance)
	}
	return fmt.Sprintf("%s:%d:%s", k.Topic, k.Partition, k.ConsumerGroup)
}

// LedgerStorage interface for persisting consumer group offset mappings
type LedgerStorage interface {
	// SaveConsumerOffset persists a consumer's committed Kafka offset -> SMQ timestamp mapping
	SaveConsumerOffset(key ConsumerOffsetKey, kafkaOffset, smqTimestamp int64, size int32) error

	// LoadConsumerOffsets restores all offset mappings for a consumer group's topic-partition
	LoadConsumerOffsets(key ConsumerOffsetKey) ([]OffsetEntry, error)

	// GetConsumerHighWaterMark returns the highest committed Kafka offset for a consumer
	GetConsumerHighWaterMark(key ConsumerOffsetKey) (int64, error)

	// Legacy methods for backward compatibility (deprecated)
	SaveOffsetMapping(topicPartition string, kafkaOffset, smqTimestamp int64, size int32) error
	LoadOffsetMappings(topicPartition string) ([]OffsetEntry, error)
	GetHighWaterMark(topicPartition string) (int64, error)
}

// NewPersistentLedger creates a ledger that persists to storage
func NewPersistentLedger(topicPartition string, storage LedgerStorage) (*PersistentLedger, error) {
	// Try to restore from storage (legacy method for backward compatibility)
	entries, err := storage.LoadOffsetMappings(topicPartition)
	if err != nil {
		return nil, fmt.Errorf("failed to load offset mappings: %w", err)
	}

	// Determine next offset
	var nextOffset int64 = 0
	if len(entries) > 0 {
		// Sort entries by offset to find the highest
		sort.Slice(entries, func(i, j int) bool {
			return entries[i].KafkaOffset < entries[j].KafkaOffset
		})
		nextOffset = entries[len(entries)-1].KafkaOffset + 1
	}

	// Create base ledger with restored state
	ledger := &Ledger{
		entries:    entries,
		nextOffset: nextOffset,
	}

	// Update earliest/latest timestamps
	if len(entries) > 0 {
		ledger.earliestTime = entries[0].Timestamp
		ledger.latestTime = entries[len(entries)-1].Timestamp
	}

	return &PersistentLedger{
		Ledger:         ledger,
		topicPartition: topicPartition,
		storage:        storage,
	}, nil
}

// AppendRecord persists the offset mapping in addition to in-memory storage
func (pl *PersistentLedger) AppendRecord(kafkaOffset, timestamp int64, size int32) error {
	// First persist to storage (legacy method for backward compatibility)
	if err := pl.storage.SaveOffsetMapping(pl.topicPartition, kafkaOffset, timestamp, size); err != nil {
		return fmt.Errorf("failed to persist offset mapping: %w", err)
	}

	// Then update in-memory ledger
	return pl.Ledger.AppendRecord(kafkaOffset, timestamp, size)
}

// GetEntries returns the offset entries from the underlying ledger
func (pl *PersistentLedger) GetEntries() []OffsetEntry {
	return pl.Ledger.GetEntries()
}

// SMQIntegratedStorage implements LedgerStorage using SMQ's in-memory replication pattern
// This approach avoids the scalability issue by using checkpoints instead of reading full history
type SMQIntegratedStorage struct {
	filerAddress        string
	filerClientAccessor *filer_client.FilerClientAccessor

	// In-memory replicated state (SMQ pattern)
	ledgers sync.Map // map[ConsumerOffsetKey.String()]*ReplicatedOffsetLedger

	// Configuration
	checkpointInterval time.Duration
	maxMemoryMappings  int
	ctx                context.Context
	cancel             context.CancelFunc
}

// ReplicatedOffsetLedger represents in-memory consumer offset state with checkpoint persistence
type ReplicatedOffsetLedger struct {
	consumerKey ConsumerOffsetKey

	// In-memory mappings (recent entries only)
	mappings      sync.Map // map[int64]*OffsetEntry
	currentOffset int64
	maxOffset     int64

	// Checkpoint state
	lastCheckpoint     int64
	lastCheckpointTime time.Time
	lastPersistTime    time.Time

	// State management
	mu               sync.RWMutex
	needsPersistence bool
}

// NewSMQIntegratedStorage creates SMQ-integrated offset storage
// This uses SMQ's proven in-memory replication + checkpoint persistence pattern
func NewSMQIntegratedStorage(brokers []string) (*SMQIntegratedStorage, error) {
	if len(brokers) == 0 {
		return nil, fmt.Errorf("no brokers provided")
	}

	ctx, cancel := context.WithCancel(context.Background())

	// Use first broker as filer address (brokers typically run co-located with filer)
	// In SMQ architecture, brokers connect to local filer instances
	filerAddress := brokers[0]

	// Create filer client accessor (like SMQ does)
	filerClientAccessor := &filer_client.FilerClientAccessor{
		GetFiler: func() pb.ServerAddress {
			return pb.ServerAddress(filerAddress)
		},
		GetGrpcDialOption: func() grpc.DialOption {
			return grpc.WithInsecure()
		},
	}

	storage := &SMQIntegratedStorage{
		filerAddress:        filerAddress,
		filerClientAccessor: filerClientAccessor,
		checkpointInterval:  30 * time.Second, // SMQ-style periodic checkpoints
		maxMemoryMappings:   10000,            // Keep recent mappings in memory
		ctx:                 ctx,
		cancel:              cancel,
	}

	// Start background checkpoint persistence (SMQ pattern)
	go storage.backgroundCheckpointPersistence()

	return storage, nil
}

// SaveConsumerOffset stores consumer offset mapping in memory (SMQ pattern) and triggers checkpoint if needed
func (s *SMQIntegratedStorage) SaveConsumerOffset(key ConsumerOffsetKey, kafkaOffset, smqTimestamp int64, size int32) error {
	// Get or create replicated ledger for this consumer
	ledger := s.getOrCreateLedger(key)

	// Update in-memory state (like SMQ subscriber offsets)
	entry := &OffsetEntry{
		KafkaOffset: kafkaOffset,
		Timestamp:   smqTimestamp,
		Size:        size,
	}

	ledger.mu.Lock()
	ledger.mappings.Store(kafkaOffset, entry)
	ledger.currentOffset = kafkaOffset
	if kafkaOffset > ledger.maxOffset {
		ledger.maxOffset = kafkaOffset
	}
	ledger.needsPersistence = true
	ledger.mu.Unlock()

	// Trigger checkpoint if threshold reached (SMQ pattern)
	if s.shouldCheckpoint(ledger) {
		return s.persistCheckpoint(ledger)
	}

	return nil
}

// LoadConsumerOffsets loads checkpoint + in-memory state (SMQ pattern) - O(1) instead of O(n)!
func (s *SMQIntegratedStorage) LoadConsumerOffsets(key ConsumerOffsetKey) ([]OffsetEntry, error) {
	ledger := s.getOrCreateLedger(key)

	// Load from checkpoint if not already loaded (SMQ pattern)
	if err := s.loadCheckpointIfNeeded(ledger); err != nil {
		return nil, fmt.Errorf("failed to load checkpoint: %w", err)
	}

	// Return current in-memory state (fast!)
	return s.getCurrentMappings(ledger), nil
}

// GetConsumerHighWaterMark returns consumer's next offset from in-memory state (fast!)
func (s *SMQIntegratedStorage) GetConsumerHighWaterMark(key ConsumerOffsetKey) (int64, error) {
	ledger := s.getOrCreateLedger(key)

	// Load checkpoint if needed
	if err := s.loadCheckpointIfNeeded(ledger); err != nil {
		return 0, fmt.Errorf("failed to load checkpoint: %w", err)
	}

	ledger.mu.RLock()
	maxOffset := ledger.maxOffset
	ledger.mu.RUnlock()

	if maxOffset < 0 {
		return 0, nil
	}
	return maxOffset + 1, nil
}

// Close persists all pending checkpoints and shuts down (SMQ pattern)
func (s *SMQIntegratedStorage) Close() error {
	s.cancel()

	// Persist all ledgers before shutdown (like SMQ on disconnect)
	s.ledgers.Range(func(key, value interface{}) bool {
		ledger := value.(*ReplicatedOffsetLedger)
		if ledger.needsPersistence {
			s.persistCheckpoint(ledger)
		}
		return true
	})

	return nil
}

// SMQ-style helper methods for in-memory replication + checkpoint persistence

// getOrCreateLedger gets or creates in-memory consumer ledger (SMQ pattern)
func (s *SMQIntegratedStorage) getOrCreateLedger(key ConsumerOffsetKey) *ReplicatedOffsetLedger {
	keyStr := key.String()
	if existing, ok := s.ledgers.Load(keyStr); ok {
		return existing.(*ReplicatedOffsetLedger)
	}

	// Create new consumer ledger
	ledger := &ReplicatedOffsetLedger{
		consumerKey:      key,
		currentOffset:    -1,
		maxOffset:        -1,
		lastCheckpoint:   -1,
		needsPersistence: false,
	}

	// Try to store, return existing if already created by another goroutine
	if actual, loaded := s.ledgers.LoadOrStore(keyStr, ledger); loaded {
		return actual.(*ReplicatedOffsetLedger)
	}

	return ledger
}

// loadCheckpointIfNeeded loads checkpoint from filer if not already loaded (SMQ pattern)
func (s *SMQIntegratedStorage) loadCheckpointIfNeeded(ledger *ReplicatedOffsetLedger) error {
	ledger.mu.Lock()
	defer ledger.mu.Unlock()

	// Already loaded?
	if ledger.lastCheckpoint >= 0 {
		return nil
	}

	// Load checkpoint from filer
	checkpointDir := s.getCheckpointDir()
	checkpointFile := ledger.consumerKey.String() + ".json"

	err := s.filerClientAccessor.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		data, err := filer.ReadInsideFiler(client, checkpointDir, checkpointFile)
		if err != nil {
			return err // Will be handled below
		}

		var checkpoint CheckpointData
		if err := json.Unmarshal(data, &checkpoint); err != nil {
			return fmt.Errorf("failed to unmarshal checkpoint: %w", err)
		}

		// Restore state from checkpoint
		ledger.lastCheckpoint = checkpoint.MaxOffset
		ledger.maxOffset = checkpoint.MaxOffset
		ledger.currentOffset = checkpoint.MaxOffset
		ledger.lastCheckpointTime = time.Unix(0, checkpoint.TimestampNs)

		// Load recent mappings (last N entries for fast access)
		for _, entry := range checkpoint.RecentMappings {
			ledger.mappings.Store(entry.KafkaOffset, &entry)
		}

		return nil
	})

	if err != nil && err != filer_pb.ErrNotFound {
		return fmt.Errorf("failed to load checkpoint: %w", err)
	}

	// Mark as loaded even if no checkpoint found
	if ledger.lastCheckpoint < 0 {
		ledger.lastCheckpoint = 0
	}

	return nil
}

// getCurrentMappings returns current in-memory mappings (SMQ pattern)
func (s *SMQIntegratedStorage) getCurrentMappings(ledger *ReplicatedOffsetLedger) []OffsetEntry {
	var entries []OffsetEntry

	ledger.mappings.Range(func(key, value interface{}) bool {
		entry := value.(*OffsetEntry)
		entries = append(entries, *entry)
		return true
	})

	// Sort by Kafka offset
	sort.Slice(entries, func(i, j int) bool {
		return entries[i].KafkaOffset < entries[j].KafkaOffset
	})

	return entries
}

// shouldCheckpoint determines if checkpoint persistence is needed (SMQ pattern)
func (s *SMQIntegratedStorage) shouldCheckpoint(ledger *ReplicatedOffsetLedger) bool {
	ledger.mu.RLock()
	defer ledger.mu.RUnlock()

	// Persist if:
	// 1. Enough time has passed
	// 2. Too many in-memory entries
	// 3. Significant offset advancement

	timeSinceLastCheckpoint := time.Since(ledger.lastCheckpointTime)

	mappingCount := 0
	ledger.mappings.Range(func(key, value interface{}) bool {
		mappingCount++
		return mappingCount < s.maxMemoryMappings // Stop counting if too many
	})

	offsetDelta := ledger.currentOffset - ledger.lastCheckpoint

	return timeSinceLastCheckpoint > s.checkpointInterval ||
		mappingCount >= s.maxMemoryMappings ||
		offsetDelta >= 1000 // Significant advancement
}

// persistCheckpoint saves checkpoint to filer (SMQ pattern)
func (s *SMQIntegratedStorage) persistCheckpoint(ledger *ReplicatedOffsetLedger) error {
	ledger.mu.Lock()
	defer ledger.mu.Unlock()

	// Collect recent mappings for checkpoint
	var recentMappings []OffsetEntry
	ledger.mappings.Range(func(key, value interface{}) bool {
		entry := value.(*OffsetEntry)
		recentMappings = append(recentMappings, *entry)
		return len(recentMappings) < 1000 // Keep last 1000 entries in checkpoint
	})

	// Sort by offset (keep most recent)
	sort.Slice(recentMappings, func(i, j int) bool {
		return recentMappings[i].KafkaOffset > recentMappings[j].KafkaOffset
	})
	if len(recentMappings) > 1000 {
		recentMappings = recentMappings[:1000]
	}

	// Create checkpoint
	checkpoint := CheckpointData{
		ConsumerKey:    ledger.consumerKey,
		MaxOffset:      ledger.maxOffset,
		TimestampNs:    time.Now().UnixNano(),
		RecentMappings: recentMappings,
		TopicPartition: fmt.Sprintf("%s:%d", ledger.consumerKey.Topic, ledger.consumerKey.Partition), // Legacy compatibility
	}

	// Marshal checkpoint
	data, err := json.Marshal(checkpoint)
	if err != nil {
		return fmt.Errorf("failed to marshal checkpoint: %w", err)
	}

	// Write to filer
	checkpointDir := s.getCheckpointDir()
	checkpointFile := ledger.consumerKey.String() + ".json"
	err = s.filerClientAccessor.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		return filer.SaveInsideFiler(client, checkpointDir, checkpointFile, data)
	})

	if err != nil {
		return fmt.Errorf("failed to save checkpoint: %w", err)
	}

	// Update checkpoint state
	ledger.lastCheckpoint = ledger.maxOffset
	ledger.lastCheckpointTime = time.Now()
	ledger.lastPersistTime = time.Now()
	ledger.needsPersistence = false

	return nil
}

// backgroundCheckpointPersistence runs periodic checkpoint saves (SMQ pattern)
func (s *SMQIntegratedStorage) backgroundCheckpointPersistence() {
	ticker := time.NewTicker(s.checkpointInterval)
	defer ticker.Stop()

	for {
		select {
		case <-s.ctx.Done():
			return
		case <-ticker.C:
			// Persist all ledgers that need it
			s.ledgers.Range(func(key, value interface{}) bool {
				ledger := value.(*ReplicatedOffsetLedger)
				if ledger.needsPersistence && s.shouldCheckpoint(ledger) {
					if err := s.persistCheckpoint(ledger); err != nil {
						// Log error but continue
						fmt.Printf("Failed to persist checkpoint for %s: %v\n", ledger.consumerKey.String(), err)
					}
				}
				return true
			})
		}
	}
}

// getCheckpointDir returns filer directory for checkpoints
func (s *SMQIntegratedStorage) getCheckpointDir() string {
	return "/kafka-offsets/checkpoints"
}

// CheckpointData represents persisted consumer checkpoint state
type CheckpointData struct {
	ConsumerKey    ConsumerOffsetKey `json:"consumer_key"`
	MaxOffset      int64             `json:"max_offset"`
	TimestampNs    int64             `json:"timestamp_ns"`
	RecentMappings []OffsetEntry     `json:"recent_mappings"`

	// Legacy field for backward compatibility
	TopicPartition string `json:"topic_partition,omitempty"`
}

// Legacy methods for backward compatibility (will be deprecated)

// SaveOffsetMapping - legacy method that maps to topic-partition only (no consumer group info)
func (s *SMQIntegratedStorage) SaveOffsetMapping(topicPartition string, kafkaOffset, smqTimestamp int64, size int32) error {
	// Parse topic:partition format
	parts := strings.Split(topicPartition, ":")
	if len(parts) != 2 {
		return fmt.Errorf("invalid topic-partition format: %s", topicPartition)
	}

	partition, err := strconv.ParseInt(parts[1], 10, 32)
	if err != nil {
		return fmt.Errorf("invalid partition number in %s: %w", topicPartition, err)
	}

	// Use legacy consumer key (no consumer group)
	legacyKey := ConsumerOffsetKey{
		Topic:                 parts[0],
		Partition:             int32(partition),
		ConsumerGroup:         "_legacy_",
		ConsumerGroupInstance: "",
	}

	return s.SaveConsumerOffset(legacyKey, kafkaOffset, smqTimestamp, size)
}

// LoadOffsetMappings - legacy method that loads from topic-partition only
func (s *SMQIntegratedStorage) LoadOffsetMappings(topicPartition string) ([]OffsetEntry, error) {
	// Parse topic:partition format
	parts := strings.Split(topicPartition, ":")
	if len(parts) != 2 {
		return nil, fmt.Errorf("invalid topic-partition format: %s", topicPartition)
	}

	partition, err := strconv.ParseInt(parts[1], 10, 32)
	if err != nil {
		return nil, fmt.Errorf("invalid partition number in %s: %w", topicPartition, err)
	}

	// Use legacy consumer key (no consumer group)
	legacyKey := ConsumerOffsetKey{
		Topic:                 parts[0],
		Partition:             int32(partition),
		ConsumerGroup:         "_legacy_",
		ConsumerGroupInstance: "",
	}

	return s.LoadConsumerOffsets(legacyKey)
}

// GetHighWaterMark - legacy method that gets high water mark for topic-partition only
func (s *SMQIntegratedStorage) GetHighWaterMark(topicPartition string) (int64, error) {
	// Parse topic:partition format
	parts := strings.Split(topicPartition, ":")
	if len(parts) != 2 {
		return 0, fmt.Errorf("invalid topic-partition format: %s", topicPartition)
	}

	partition, err := strconv.ParseInt(parts[1], 10, 32)
	if err != nil {
		return 0, fmt.Errorf("invalid partition number in %s: %w", topicPartition, err)
	}

	// Use legacy consumer key (no consumer group)
	legacyKey := ConsumerOffsetKey{
		Topic:                 parts[0],
		Partition:             int32(partition),
		ConsumerGroup:         "_legacy_",
		ConsumerGroupInstance: "",
	}

	return s.GetConsumerHighWaterMark(legacyKey)
}
