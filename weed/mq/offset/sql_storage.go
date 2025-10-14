package offset

import (
	"database/sql"
	"fmt"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/schema_pb"
)

// OffsetEntry represents a mapping between Kafka offset and SMQ timestamp
type OffsetEntry struct {
	KafkaOffset  int64
	SMQTimestamp int64
	MessageSize  int32
}

// SQLOffsetStorage implements OffsetStorage using SQL database with _index column
type SQLOffsetStorage struct {
	db *sql.DB
}

// NewSQLOffsetStorage creates a new SQL-based offset storage
func NewSQLOffsetStorage(db *sql.DB) (*SQLOffsetStorage, error) {
	storage := &SQLOffsetStorage{db: db}

	// Initialize database schema
	if err := storage.initializeSchema(); err != nil {
		return nil, fmt.Errorf("failed to initialize schema: %w", err)
	}

	return storage, nil
}

// initializeSchema creates the necessary tables for offset storage
func (s *SQLOffsetStorage) initializeSchema() error {
	// TODO: Create offset storage tables with _index as hidden column
	// ASSUMPTION: Using SQLite-compatible syntax, may need adaptation for other databases

	queries := []string{
		// Partition offset checkpoints table
		// TODO: Add _index as computed column when supported by database
		// ASSUMPTION: Using regular columns for now, _index concept preserved for future enhancement
		`CREATE TABLE IF NOT EXISTS partition_offset_checkpoints (
			partition_key TEXT PRIMARY KEY,
			ring_size INTEGER NOT NULL,
			range_start INTEGER NOT NULL,
			range_stop INTEGER NOT NULL,
			unix_time_ns INTEGER NOT NULL,
			checkpoint_offset INTEGER NOT NULL,
			updated_at INTEGER NOT NULL
		)`,

		// Offset mappings table for detailed tracking
		// TODO: Add _index as computed column when supported by database
		`CREATE TABLE IF NOT EXISTS offset_mappings (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			partition_key TEXT NOT NULL,
			kafka_offset INTEGER NOT NULL,
			smq_timestamp INTEGER NOT NULL,
			message_size INTEGER NOT NULL,
			created_at INTEGER NOT NULL,
			UNIQUE(partition_key, kafka_offset)
		)`,

		// Indexes for performance
		`CREATE INDEX IF NOT EXISTS idx_partition_offset_checkpoints_partition 
		 ON partition_offset_checkpoints(partition_key)`,

		`CREATE INDEX IF NOT EXISTS idx_offset_mappings_partition_offset 
		 ON offset_mappings(partition_key, kafka_offset)`,

		`CREATE INDEX IF NOT EXISTS idx_offset_mappings_timestamp 
		 ON offset_mappings(partition_key, smq_timestamp)`,
	}

	for _, query := range queries {
		if _, err := s.db.Exec(query); err != nil {
			return fmt.Errorf("failed to execute schema query: %w", err)
		}
	}

	return nil
}

// SaveCheckpoint saves the checkpoint for a partition
func (s *SQLOffsetStorage) SaveCheckpoint(namespace, topicName string, partition *schema_pb.Partition, offset int64) error {
	// Use TopicPartitionKey to ensure each topic has isolated checkpoint storage
	partitionKey := TopicPartitionKey(namespace, topicName, partition)
	now := time.Now().UnixNano()

	// TODO: Use UPSERT for better performance
	// ASSUMPTION: SQLite REPLACE syntax, may need adaptation for other databases
	query := `
		REPLACE INTO partition_offset_checkpoints 
		(partition_key, ring_size, range_start, range_stop, unix_time_ns, checkpoint_offset, updated_at)
		VALUES (?, ?, ?, ?, ?, ?, ?)
	`

	_, err := s.db.Exec(query,
		partitionKey,
		partition.RingSize,
		partition.RangeStart,
		partition.RangeStop,
		partition.UnixTimeNs,
		offset,
		now,
	)

	if err != nil {
		return fmt.Errorf("failed to save checkpoint: %w", err)
	}

	return nil
}

// LoadCheckpoint loads the checkpoint for a partition
func (s *SQLOffsetStorage) LoadCheckpoint(namespace, topicName string, partition *schema_pb.Partition) (int64, error) {
	// Use TopicPartitionKey to match SaveCheckpoint
	partitionKey := TopicPartitionKey(namespace, topicName, partition)

	query := `
		SELECT checkpoint_offset 
		FROM partition_offset_checkpoints 
		WHERE partition_key = ?
	`

	var checkpointOffset int64
	err := s.db.QueryRow(query, partitionKey).Scan(&checkpointOffset)

	if err == sql.ErrNoRows {
		return -1, fmt.Errorf("no checkpoint found")
	}

	if err != nil {
		return -1, fmt.Errorf("failed to load checkpoint: %w", err)
	}

	return checkpointOffset, nil
}

// GetHighestOffset finds the highest offset in storage for a partition
func (s *SQLOffsetStorage) GetHighestOffset(namespace, topicName string, partition *schema_pb.Partition) (int64, error) {
	// Use TopicPartitionKey to match SaveCheckpoint
	partitionKey := TopicPartitionKey(namespace, topicName, partition)

	// TODO: Use _index column for efficient querying
	// ASSUMPTION: kafka_offset represents the sequential offset we're tracking
	query := `
		SELECT MAX(kafka_offset) 
		FROM offset_mappings 
		WHERE partition_key = ?
	`

	var highestOffset sql.NullInt64
	err := s.db.QueryRow(query, partitionKey).Scan(&highestOffset)

	if err != nil {
		return -1, fmt.Errorf("failed to get highest offset: %w", err)
	}

	if !highestOffset.Valid {
		return -1, fmt.Errorf("no records found")
	}

	return highestOffset.Int64, nil
}

// SaveOffsetMapping stores an offset mapping (extends OffsetStorage interface)
func (s *SQLOffsetStorage) SaveOffsetMapping(partitionKey string, kafkaOffset, smqTimestamp int64, size int32) error {
	now := time.Now().UnixNano()

	// TODO: Handle duplicate key conflicts gracefully
	// ASSUMPTION: Using INSERT OR REPLACE for conflict resolution
	query := `
		INSERT OR REPLACE INTO offset_mappings 
		(partition_key, kafka_offset, smq_timestamp, message_size, created_at)
		VALUES (?, ?, ?, ?, ?)
	`

	_, err := s.db.Exec(query, partitionKey, kafkaOffset, smqTimestamp, size, now)
	if err != nil {
		return fmt.Errorf("failed to save offset mapping: %w", err)
	}

	return nil
}

// LoadOffsetMappings retrieves all offset mappings for a partition
func (s *SQLOffsetStorage) LoadOffsetMappings(partitionKey string) ([]OffsetEntry, error) {
	// TODO: Add pagination for large result sets
	// ASSUMPTION: Loading all mappings for now, should be paginated in production
	query := `
		SELECT kafka_offset, smq_timestamp, message_size
		FROM offset_mappings 
		WHERE partition_key = ?
		ORDER BY kafka_offset ASC
	`

	rows, err := s.db.Query(query, partitionKey)
	if err != nil {
		return nil, fmt.Errorf("failed to query offset mappings: %w", err)
	}
	defer rows.Close()

	var entries []OffsetEntry
	for rows.Next() {
		var entry OffsetEntry
		err := rows.Scan(&entry.KafkaOffset, &entry.SMQTimestamp, &entry.MessageSize)
		if err != nil {
			return nil, fmt.Errorf("failed to scan offset entry: %w", err)
		}
		entries = append(entries, entry)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating offset mappings: %w", err)
	}

	return entries, nil
}

// GetOffsetMappingsByRange retrieves offset mappings within a specific range
func (s *SQLOffsetStorage) GetOffsetMappingsByRange(partitionKey string, startOffset, endOffset int64) ([]OffsetEntry, error) {
	// TODO: Use _index column for efficient range queries
	query := `
		SELECT kafka_offset, smq_timestamp, message_size
		FROM offset_mappings 
		WHERE partition_key = ? AND kafka_offset >= ? AND kafka_offset <= ?
		ORDER BY kafka_offset ASC
	`

	rows, err := s.db.Query(query, partitionKey, startOffset, endOffset)
	if err != nil {
		return nil, fmt.Errorf("failed to query offset range: %w", err)
	}
	defer rows.Close()

	var entries []OffsetEntry
	for rows.Next() {
		var entry OffsetEntry
		err := rows.Scan(&entry.KafkaOffset, &entry.SMQTimestamp, &entry.MessageSize)
		if err != nil {
			return nil, fmt.Errorf("failed to scan offset entry: %w", err)
		}
		entries = append(entries, entry)
	}

	return entries, nil
}

// GetPartitionStats returns statistics about a partition's offset usage
func (s *SQLOffsetStorage) GetPartitionStats(partitionKey string) (*PartitionStats, error) {
	query := `
		SELECT 
			COUNT(*) as record_count,
			MIN(kafka_offset) as earliest_offset,
			MAX(kafka_offset) as latest_offset,
			SUM(message_size) as total_size,
			MIN(created_at) as first_record_time,
			MAX(created_at) as last_record_time
		FROM offset_mappings 
		WHERE partition_key = ?
	`

	var stats PartitionStats
	var earliestOffset, latestOffset sql.NullInt64
	var totalSize sql.NullInt64
	var firstRecordTime, lastRecordTime sql.NullInt64

	err := s.db.QueryRow(query, partitionKey).Scan(
		&stats.RecordCount,
		&earliestOffset,
		&latestOffset,
		&totalSize,
		&firstRecordTime,
		&lastRecordTime,
	)

	if err != nil {
		return nil, fmt.Errorf("failed to get partition stats: %w", err)
	}

	stats.PartitionKey = partitionKey

	if earliestOffset.Valid {
		stats.EarliestOffset = earliestOffset.Int64
	} else {
		stats.EarliestOffset = -1
	}

	if latestOffset.Valid {
		stats.LatestOffset = latestOffset.Int64
		stats.HighWaterMark = latestOffset.Int64 + 1
	} else {
		stats.LatestOffset = -1
		stats.HighWaterMark = 0
	}

	if firstRecordTime.Valid {
		stats.FirstRecordTime = firstRecordTime.Int64
	}

	if lastRecordTime.Valid {
		stats.LastRecordTime = lastRecordTime.Int64
	}

	if totalSize.Valid {
		stats.TotalSize = totalSize.Int64
	}

	return &stats, nil
}

// CleanupOldMappings removes offset mappings older than the specified time
func (s *SQLOffsetStorage) CleanupOldMappings(olderThanNs int64) error {
	// TODO: Add configurable cleanup policies
	// ASSUMPTION: Simple time-based cleanup, could be enhanced with retention policies
	query := `
		DELETE FROM offset_mappings 
		WHERE created_at < ?
	`

	result, err := s.db.Exec(query, olderThanNs)
	if err != nil {
		return fmt.Errorf("failed to cleanup old mappings: %w", err)
	}

	rowsAffected, _ := result.RowsAffected()
	if rowsAffected > 0 {
		// Log cleanup activity
		fmt.Printf("Cleaned up %d old offset mappings\n", rowsAffected)
	}

	return nil
}

// Close closes the database connection
func (s *SQLOffsetStorage) Close() error {
	if s.db != nil {
		return s.db.Close()
	}
	return nil
}

// PartitionStats provides statistics about a partition's offset usage
type PartitionStats struct {
	PartitionKey    string
	RecordCount     int64
	EarliestOffset  int64
	LatestOffset    int64
	HighWaterMark   int64
	TotalSize       int64
	FirstRecordTime int64
	LastRecordTime  int64
}

// GetAllPartitions returns a list of all partitions with offset data
func (s *SQLOffsetStorage) GetAllPartitions() ([]string, error) {
	query := `
		SELECT DISTINCT partition_key 
		FROM offset_mappings 
		ORDER BY partition_key
	`

	rows, err := s.db.Query(query)
	if err != nil {
		return nil, fmt.Errorf("failed to get all partitions: %w", err)
	}
	defer rows.Close()

	var partitions []string
	for rows.Next() {
		var partitionKey string
		if err := rows.Scan(&partitionKey); err != nil {
			return nil, fmt.Errorf("failed to scan partition key: %w", err)
		}
		partitions = append(partitions, partitionKey)
	}

	return partitions, nil
}

// Vacuum performs database maintenance operations
func (s *SQLOffsetStorage) Vacuum() error {
	// TODO: Add database-specific optimization commands
	// ASSUMPTION: SQLite VACUUM command, may need adaptation for other databases
	_, err := s.db.Exec("VACUUM")
	if err != nil {
		return fmt.Errorf("failed to vacuum database: %w", err)
	}

	return nil
}
