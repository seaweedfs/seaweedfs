package offset

import (
	"database/sql"
	"os"
	"testing"
	"time"

	_ "github.com/mattn/go-sqlite3" // SQLite driver
	"github.com/seaweedfs/seaweedfs/weed/pb/schema_pb"
)

func createTestDB(t *testing.T) *sql.DB {
	// Create temporary database file
	tmpFile, err := os.CreateTemp("", "offset_test_*.db")
	if err != nil {
		t.Fatalf("Failed to create temp database file: %v", err)
	}
	tmpFile.Close()

	// Clean up the file when test completes
	t.Cleanup(func() {
		os.Remove(tmpFile.Name())
	})

	db, err := sql.Open("sqlite3", tmpFile.Name())
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}

	t.Cleanup(func() {
		db.Close()
	})

	return db
}

func createTestPartitionForSQL() *schema_pb.Partition {
	return &schema_pb.Partition{
		RingSize:   1024,
		RangeStart: 0,
		RangeStop:  31,
		UnixTimeNs: time.Now().UnixNano(),
	}
}

func TestSQLOffsetStorage_InitializeSchema(t *testing.T) {
	db := createTestDB(t)

	storage, err := NewSQLOffsetStorage(db)
	if err != nil {
		t.Fatalf("Failed to create SQL storage: %v", err)
	}
	defer storage.Close()

	// Verify tables were created
	tables := []string{
		"partition_offset_checkpoints",
		"offset_mappings",
	}

	for _, table := range tables {
		var count int
		err := db.QueryRow("SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name=?", table).Scan(&count)
		if err != nil {
			t.Fatalf("Failed to check table %s: %v", table, err)
		}

		if count != 1 {
			t.Errorf("Table %s was not created", table)
		}
	}
}

func TestSQLOffsetStorage_SaveLoadCheckpoint(t *testing.T) {
	db := createTestDB(t)
	storage, err := NewSQLOffsetStorage(db)
	if err != nil {
		t.Fatalf("Failed to create SQL storage: %v", err)
	}
	defer storage.Close()

	partition := createTestPartitionForSQL()

	// Test saving checkpoint
	err = storage.SaveCheckpoint("test-namespace", "test-topic", partition, 100)
	if err != nil {
		t.Fatalf("Failed to save checkpoint: %v", err)
	}

	// Test loading checkpoint
	checkpoint, err := storage.LoadCheckpoint("test-namespace", "test-topic", partition)
	if err != nil {
		t.Fatalf("Failed to load checkpoint: %v", err)
	}

	if checkpoint != 100 {
		t.Errorf("Expected checkpoint 100, got %d", checkpoint)
	}

	// Test updating checkpoint
	err = storage.SaveCheckpoint("test-namespace", "test-topic", partition, 200)
	if err != nil {
		t.Fatalf("Failed to update checkpoint: %v", err)
	}

	checkpoint, err = storage.LoadCheckpoint("test-namespace", "test-topic", partition)
	if err != nil {
		t.Fatalf("Failed to load updated checkpoint: %v", err)
	}

	if checkpoint != 200 {
		t.Errorf("Expected updated checkpoint 200, got %d", checkpoint)
	}
}

func TestSQLOffsetStorage_LoadCheckpointNotFound(t *testing.T) {
	db := createTestDB(t)
	storage, err := NewSQLOffsetStorage(db)
	if err != nil {
		t.Fatalf("Failed to create SQL storage: %v", err)
	}
	defer storage.Close()

	partition := createTestPartitionForSQL()

	// Test loading non-existent checkpoint
	_, err = storage.LoadCheckpoint("test-namespace", "test-topic", partition)
	if err == nil {
		t.Error("Expected error for non-existent checkpoint")
	}
}

func TestSQLOffsetStorage_SaveLoadOffsetMappings(t *testing.T) {
	db := createTestDB(t)
	storage, err := NewSQLOffsetStorage(db)
	if err != nil {
		t.Fatalf("Failed to create SQL storage: %v", err)
	}
	defer storage.Close()

	partition := createTestPartitionForSQL()
	partitionKey := partitionKey(partition)

	// Save multiple offset mappings
	mappings := []struct {
		offset    int64
		timestamp int64
		size      int32
	}{
		{0, 1000, 100},
		{1, 2000, 150},
		{2, 3000, 200},
	}

	for _, mapping := range mappings {
		err := storage.SaveOffsetMapping(partitionKey, mapping.offset, mapping.timestamp, mapping.size)
		if err != nil {
			t.Fatalf("Failed to save offset mapping: %v", err)
		}
	}

	// Load offset mappings
	entries, err := storage.LoadOffsetMappings(partitionKey)
	if err != nil {
		t.Fatalf("Failed to load offset mappings: %v", err)
	}

	if len(entries) != len(mappings) {
		t.Errorf("Expected %d entries, got %d", len(mappings), len(entries))
	}

	// Verify entries are sorted by offset
	for i, entry := range entries {
		expected := mappings[i]
		if entry.KafkaOffset != expected.offset {
			t.Errorf("Entry %d: expected offset %d, got %d", i, expected.offset, entry.KafkaOffset)
		}
		if entry.SMQTimestamp != expected.timestamp {
			t.Errorf("Entry %d: expected timestamp %d, got %d", i, expected.timestamp, entry.SMQTimestamp)
		}
		if entry.MessageSize != expected.size {
			t.Errorf("Entry %d: expected size %d, got %d", i, expected.size, entry.MessageSize)
		}
	}
}

func TestSQLOffsetStorage_GetHighestOffset(t *testing.T) {
	db := createTestDB(t)
	storage, err := NewSQLOffsetStorage(db)
	if err != nil {
		t.Fatalf("Failed to create SQL storage: %v", err)
	}
	defer storage.Close()

	partition := createTestPartitionForSQL()
	partitionKey := TopicPartitionKey("test-namespace", "test-topic", partition)

	// Test empty partition
	_, err = storage.GetHighestOffset("test-namespace", "test-topic", partition)
	if err == nil {
		t.Error("Expected error for empty partition")
	}

	// Add some offset mappings
	offsets := []int64{5, 1, 3, 2, 4}
	for _, offset := range offsets {
		err := storage.SaveOffsetMapping(partitionKey, offset, offset*1000, 100)
		if err != nil {
			t.Fatalf("Failed to save offset mapping: %v", err)
		}
	}

	// Get highest offset
	highest, err := storage.GetHighestOffset("test-namespace", "test-topic", partition)
	if err != nil {
		t.Fatalf("Failed to get highest offset: %v", err)
	}

	if highest != 5 {
		t.Errorf("Expected highest offset 5, got %d", highest)
	}
}

func TestSQLOffsetStorage_GetOffsetMappingsByRange(t *testing.T) {
	db := createTestDB(t)
	storage, err := NewSQLOffsetStorage(db)
	if err != nil {
		t.Fatalf("Failed to create SQL storage: %v", err)
	}
	defer storage.Close()

	partition := createTestPartitionForSQL()
	partitionKey := partitionKey(partition)

	// Add offset mappings
	for i := int64(0); i < 10; i++ {
		err := storage.SaveOffsetMapping(partitionKey, i, i*1000, 100)
		if err != nil {
			t.Fatalf("Failed to save offset mapping: %v", err)
		}
	}

	// Get range of offsets
	entries, err := storage.GetOffsetMappingsByRange(partitionKey, 3, 7)
	if err != nil {
		t.Fatalf("Failed to get offset range: %v", err)
	}

	expectedCount := 5 // offsets 3, 4, 5, 6, 7
	if len(entries) != expectedCount {
		t.Errorf("Expected %d entries, got %d", expectedCount, len(entries))
	}

	// Verify range
	for i, entry := range entries {
		expectedOffset := int64(3 + i)
		if entry.KafkaOffset != expectedOffset {
			t.Errorf("Entry %d: expected offset %d, got %d", i, expectedOffset, entry.KafkaOffset)
		}
	}
}

func TestSQLOffsetStorage_GetPartitionStats(t *testing.T) {
	db := createTestDB(t)
	storage, err := NewSQLOffsetStorage(db)
	if err != nil {
		t.Fatalf("Failed to create SQL storage: %v", err)
	}
	defer storage.Close()

	partition := createTestPartitionForSQL()
	partitionKey := partitionKey(partition)

	// Test empty partition stats
	stats, err := storage.GetPartitionStats(partitionKey)
	if err != nil {
		t.Fatalf("Failed to get empty partition stats: %v", err)
	}

	if stats.RecordCount != 0 {
		t.Errorf("Expected record count 0, got %d", stats.RecordCount)
	}

	if stats.EarliestOffset != -1 {
		t.Errorf("Expected earliest offset -1, got %d", stats.EarliestOffset)
	}

	// Add some data
	sizes := []int32{100, 150, 200}
	for i, size := range sizes {
		err := storage.SaveOffsetMapping(partitionKey, int64(i), int64(i*1000), size)
		if err != nil {
			t.Fatalf("Failed to save offset mapping: %v", err)
		}
	}

	// Get stats with data
	stats, err = storage.GetPartitionStats(partitionKey)
	if err != nil {
		t.Fatalf("Failed to get partition stats: %v", err)
	}

	if stats.RecordCount != 3 {
		t.Errorf("Expected record count 3, got %d", stats.RecordCount)
	}

	if stats.EarliestOffset != 0 {
		t.Errorf("Expected earliest offset 0, got %d", stats.EarliestOffset)
	}

	if stats.LatestOffset != 2 {
		t.Errorf("Expected latest offset 2, got %d", stats.LatestOffset)
	}

	if stats.HighWaterMark != 3 {
		t.Errorf("Expected high water mark 3, got %d", stats.HighWaterMark)
	}

	expectedTotalSize := int64(100 + 150 + 200)
	if stats.TotalSize != expectedTotalSize {
		t.Errorf("Expected total size %d, got %d", expectedTotalSize, stats.TotalSize)
	}
}

func TestSQLOffsetStorage_GetAllPartitions(t *testing.T) {
	db := createTestDB(t)
	storage, err := NewSQLOffsetStorage(db)
	if err != nil {
		t.Fatalf("Failed to create SQL storage: %v", err)
	}
	defer storage.Close()

	// Test empty database
	partitions, err := storage.GetAllPartitions()
	if err != nil {
		t.Fatalf("Failed to get all partitions: %v", err)
	}

	if len(partitions) != 0 {
		t.Errorf("Expected 0 partitions, got %d", len(partitions))
	}

	// Add data for multiple partitions
	partition1 := createTestPartitionForSQL()
	partition2 := &schema_pb.Partition{
		RingSize:   1024,
		RangeStart: 32,
		RangeStop:  63,
		UnixTimeNs: time.Now().UnixNano(),
	}

	partitionKey1 := partitionKey(partition1)
	partitionKey2 := partitionKey(partition2)

	storage.SaveOffsetMapping(partitionKey1, 0, 1000, 100)
	storage.SaveOffsetMapping(partitionKey2, 0, 2000, 150)

	// Get all partitions
	partitions, err = storage.GetAllPartitions()
	if err != nil {
		t.Fatalf("Failed to get all partitions: %v", err)
	}

	if len(partitions) != 2 {
		t.Errorf("Expected 2 partitions, got %d", len(partitions))
	}

	// Verify partition keys are present
	partitionMap := make(map[string]bool)
	for _, p := range partitions {
		partitionMap[p] = true
	}

	if !partitionMap[partitionKey1] {
		t.Errorf("Partition key %s not found", partitionKey1)
	}

	if !partitionMap[partitionKey2] {
		t.Errorf("Partition key %s not found", partitionKey2)
	}
}

func TestSQLOffsetStorage_CleanupOldMappings(t *testing.T) {
	db := createTestDB(t)
	storage, err := NewSQLOffsetStorage(db)
	if err != nil {
		t.Fatalf("Failed to create SQL storage: %v", err)
	}
	defer storage.Close()

	partition := createTestPartitionForSQL()
	partitionKey := partitionKey(partition)

	// Add mappings with different timestamps
	now := time.Now().UnixNano()

	// Add old mapping by directly inserting with old timestamp
	oldTime := now - (24 * time.Hour).Nanoseconds() // 24 hours ago
	_, err = db.Exec(`
		INSERT INTO offset_mappings 
		(partition_key, kafka_offset, smq_timestamp, message_size, created_at)
		VALUES (?, ?, ?, ?, ?)
	`, partitionKey, 0, oldTime, 100, oldTime)
	if err != nil {
		t.Fatalf("Failed to insert old mapping: %v", err)
	}

	// Add recent mapping
	storage.SaveOffsetMapping(partitionKey, 1, now, 150)

	// Verify both mappings exist
	entries, err := storage.LoadOffsetMappings(partitionKey)
	if err != nil {
		t.Fatalf("Failed to load mappings: %v", err)
	}

	if len(entries) != 2 {
		t.Errorf("Expected 2 mappings before cleanup, got %d", len(entries))
	}

	// Cleanup old mappings (older than 12 hours)
	cutoffTime := now - (12 * time.Hour).Nanoseconds()
	err = storage.CleanupOldMappings(cutoffTime)
	if err != nil {
		t.Fatalf("Failed to cleanup old mappings: %v", err)
	}

	// Verify only recent mapping remains
	entries, err = storage.LoadOffsetMappings(partitionKey)
	if err != nil {
		t.Fatalf("Failed to load mappings after cleanup: %v", err)
	}

	if len(entries) != 1 {
		t.Errorf("Expected 1 mapping after cleanup, got %d", len(entries))
	}

	if entries[0].KafkaOffset != 1 {
		t.Errorf("Expected remaining mapping offset 1, got %d", entries[0].KafkaOffset)
	}
}

func TestSQLOffsetStorage_Vacuum(t *testing.T) {
	db := createTestDB(t)
	storage, err := NewSQLOffsetStorage(db)
	if err != nil {
		t.Fatalf("Failed to create SQL storage: %v", err)
	}
	defer storage.Close()

	// Vacuum should not fail on empty database
	err = storage.Vacuum()
	if err != nil {
		t.Fatalf("Failed to vacuum database: %v", err)
	}

	// Add some data and vacuum again
	partition := createTestPartitionForSQL()
	partitionKey := partitionKey(partition)
	storage.SaveOffsetMapping(partitionKey, 0, 1000, 100)

	err = storage.Vacuum()
	if err != nil {
		t.Fatalf("Failed to vacuum database with data: %v", err)
	}
}

func TestSQLOffsetStorage_ConcurrentAccess(t *testing.T) {
	db := createTestDB(t)
	storage, err := NewSQLOffsetStorage(db)
	if err != nil {
		t.Fatalf("Failed to create SQL storage: %v", err)
	}
	defer storage.Close()

	partition := createTestPartitionForSQL()
	partitionKey := partitionKey(partition)

	// Test concurrent writes
	const numGoroutines = 10
	const offsetsPerGoroutine = 10

	done := make(chan bool, numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		go func(goroutineID int) {
			defer func() { done <- true }()

			for j := 0; j < offsetsPerGoroutine; j++ {
				offset := int64(goroutineID*offsetsPerGoroutine + j)
				err := storage.SaveOffsetMapping(partitionKey, offset, offset*1000, 100)
				if err != nil {
					t.Errorf("Failed to save offset mapping %d: %v", offset, err)
					return
				}
			}
		}(i)
	}

	// Wait for all goroutines to complete
	for i := 0; i < numGoroutines; i++ {
		<-done
	}

	// Verify all mappings were saved
	entries, err := storage.LoadOffsetMappings(partitionKey)
	if err != nil {
		t.Fatalf("Failed to load mappings: %v", err)
	}

	expectedCount := numGoroutines * offsetsPerGoroutine
	if len(entries) != expectedCount {
		t.Errorf("Expected %d mappings, got %d", expectedCount, len(entries))
	}
}
