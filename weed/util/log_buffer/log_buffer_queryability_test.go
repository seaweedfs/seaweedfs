package log_buffer

import (
	"bytes"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"google.golang.org/protobuf/proto"
)

// TestBufferQueryability tests that data written to the buffer can be immediately queried
func TestBufferQueryability(t *testing.T) {
	// Create a log buffer with a long flush interval to prevent premature flushing
	logBuffer := NewLogBuffer("test-buffer", 10*time.Minute,
		func(logBuffer *LogBuffer, startTime, stopTime time.Time, buf []byte, minOffset, maxOffset int64) {
			// Mock flush function - do nothing to keep data in memory
		},
		func(startPosition MessagePosition, stopTsNs int64, eachLogEntryFn EachLogEntryFuncType) (MessagePosition, bool, error) {
			// Mock read from disk function
			return startPosition, false, nil
		},
		func() {
			// Mock notify function
		})

	// Test data similar to schema registry messages
	testKey := []byte(`{"keytype":"SCHEMA","subject":"test-topic-value","version":1,"magic":1}`)
	testValue := []byte(`{"subject":"test-topic-value","version":1,"id":1,"schemaType":"AVRO","schema":"\"string\"","deleted":false}`)

	// Create a LogEntry with offset (simulating the schema registry scenario)
	logEntry := &filer_pb.LogEntry{
		TsNs:             time.Now().UnixNano(),
		PartitionKeyHash: 12345,
		Data:             testValue,
		Key:              testKey,
		Offset:           1,
	}

	// Add the entry to the buffer
	logBuffer.AddLogEntryToBuffer(logEntry)

	// Verify the buffer has data
	if logBuffer.pos == 0 {
		t.Fatal("Buffer should have data after adding entry")
	}

	// Test immediate queryability - read from buffer starting from beginning
	startPosition := NewMessagePosition(0, 0) // Start from beginning
	bufferCopy, batchIndex, err := logBuffer.ReadFromBuffer(startPosition)

	if err != nil {
		t.Fatalf("ReadFromBuffer failed: %v", err)
	}

	if bufferCopy == nil {
		t.Fatal("ReadFromBuffer returned nil buffer - data should be queryable immediately")
	}

	if batchIndex != 1 {
		t.Errorf("Expected batchIndex=1, got %d", batchIndex)
	}

	// Verify we can read the data back
	buf := bufferCopy.Bytes()
	if len(buf) == 0 {
		t.Fatal("Buffer copy is empty")
	}

	// Parse the first entry from the buffer
	if len(buf) < 4 {
		t.Fatal("Buffer too small to contain entry size")
	}

	size := util.BytesToUint32(buf[0:4])
	if len(buf) < 4+int(size) {
		t.Fatalf("Buffer too small to contain entry data: need %d, have %d", 4+int(size), len(buf))
	}

	entryData := buf[4 : 4+int(size)]

	// Unmarshal and verify the entry
	retrievedEntry := &filer_pb.LogEntry{}
	if err := proto.Unmarshal(entryData, retrievedEntry); err != nil {
		t.Fatalf("Failed to unmarshal retrieved entry: %v", err)
	}

	// Verify the data matches
	if !bytes.Equal(retrievedEntry.Key, testKey) {
		t.Errorf("Key mismatch: expected %s, got %s", string(testKey), string(retrievedEntry.Key))
	}

	if !bytes.Equal(retrievedEntry.Data, testValue) {
		t.Errorf("Value mismatch: expected %s, got %s", string(testValue), string(retrievedEntry.Data))
	}

	if retrievedEntry.Offset != 1 {
		t.Errorf("Offset mismatch: expected 1, got %d", retrievedEntry.Offset)
	}

	t.Logf("Buffer queryability test passed - data is immediately readable")
}

// TestMultipleEntriesQueryability tests querying multiple entries from buffer
func TestMultipleEntriesQueryability(t *testing.T) {
	logBuffer := NewLogBuffer("test-multi-buffer", 10*time.Minute,
		func(logBuffer *LogBuffer, startTime, stopTime time.Time, buf []byte, minOffset, maxOffset int64) {
			// Mock flush function
		},
		func(startPosition MessagePosition, stopTsNs int64, eachLogEntryFn EachLogEntryFuncType) (MessagePosition, bool, error) {
			return startPosition, false, nil
		},
		func() {})

	// Add multiple entries
	for i := 1; i <= 3; i++ {
		logEntry := &filer_pb.LogEntry{
			TsNs:             time.Now().UnixNano() + int64(i*1000), // Ensure different timestamps
			PartitionKeyHash: int32(i),
			Data:             []byte("test-data-" + string(rune('0'+i))),
			Key:              []byte("test-key-" + string(rune('0'+i))),
			Offset:           int64(i),
		}
		logBuffer.AddLogEntryToBuffer(logEntry)
	}

	// Read all entries
	startPosition := NewMessagePosition(0, 0)
	bufferCopy, batchIndex, err := logBuffer.ReadFromBuffer(startPosition)

	if err != nil {
		t.Fatalf("ReadFromBuffer failed: %v", err)
	}

	if bufferCopy == nil {
		t.Fatal("ReadFromBuffer returned nil buffer")
	}

	if batchIndex != 3 {
		t.Errorf("Expected batchIndex=3, got %d", batchIndex)
	}

	// Count entries in buffer
	buf := bufferCopy.Bytes()
	entryCount := 0
	pos := 0

	for pos+4 < len(buf) {
		size := util.BytesToUint32(buf[pos : pos+4])
		if pos+4+int(size) > len(buf) {
			break
		}

		entryData := buf[pos+4 : pos+4+int(size)]
		entry := &filer_pb.LogEntry{}
		if err := proto.Unmarshal(entryData, entry); err != nil {
			t.Fatalf("Failed to unmarshal entry %d: %v", entryCount+1, err)
		}

		entryCount++
		pos += 4 + int(size)

		t.Logf("Entry %d: Key=%s, Data=%s, Offset=%d", entryCount, string(entry.Key), string(entry.Data), entry.Offset)
	}

	if entryCount != 3 {
		t.Errorf("Expected 3 entries, found %d", entryCount)
	}

	t.Logf("Multiple entries queryability test passed - found %d entries", entryCount)
}

// TestSchemaRegistryScenario tests the specific scenario that was failing
func TestSchemaRegistryScenario(t *testing.T) {
	logBuffer := NewLogBuffer("_schemas", 10*time.Minute,
		func(logBuffer *LogBuffer, startTime, stopTime time.Time, buf []byte, minOffset, maxOffset int64) {
			// Mock flush function - simulate what happens in real scenario
			t.Logf("FLUSH: startTime=%v, stopTime=%v, bufSize=%d, minOffset=%d, maxOffset=%d",
				startTime, stopTime, len(buf), minOffset, maxOffset)
		},
		func(startPosition MessagePosition, stopTsNs int64, eachLogEntryFn EachLogEntryFuncType) (MessagePosition, bool, error) {
			return startPosition, false, nil
		},
		func() {})

	// Simulate schema registry message
	schemaKey := []byte(`{"keytype":"SCHEMA","subject":"test-schema-value","version":1,"magic":1}`)
	schemaValue := []byte(`{"subject":"test-schema-value","version":1,"id":12,"schemaType":"AVRO","schema":"\"string\"","deleted":false}`)

	logEntry := &filer_pb.LogEntry{
		TsNs:             time.Now().UnixNano(),
		PartitionKeyHash: 12345,
		Data:             schemaValue,
		Key:              schemaKey,
		Offset:           0, // First message
	}

	// Add to buffer
	logBuffer.AddLogEntryToBuffer(logEntry)

	// Simulate the SQL query scenario - read from offset 0
	startPosition := NewMessagePosition(0, 0)
	bufferCopy, _, err := logBuffer.ReadFromBuffer(startPosition)

	if err != nil {
		t.Fatalf("Schema registry scenario failed: %v", err)
	}

	if bufferCopy == nil {
		t.Fatal("Schema registry scenario: ReadFromBuffer returned nil - this is the bug!")
	}

	// Verify schema data is readable
	buf := bufferCopy.Bytes()
	if len(buf) < 4 {
		t.Fatal("Buffer too small")
	}

	size := util.BytesToUint32(buf[0:4])
	entryData := buf[4 : 4+int(size)]

	retrievedEntry := &filer_pb.LogEntry{}
	if err := proto.Unmarshal(entryData, retrievedEntry); err != nil {
		t.Fatalf("Failed to unmarshal schema entry: %v", err)
	}

	// Verify schema value is preserved
	if !bytes.Equal(retrievedEntry.Data, schemaValue) {
		t.Errorf("Schema value lost! Expected: %s, Got: %s", string(schemaValue), string(retrievedEntry.Data))
	}

	if len(retrievedEntry.Data) != len(schemaValue) {
		t.Errorf("Schema value length mismatch! Expected: %d, Got: %d", len(schemaValue), len(retrievedEntry.Data))
	}

	t.Logf("Schema registry scenario test passed - schema value preserved: %d bytes", len(retrievedEntry.Data))
}

// TestTimeBasedFirstReadBeforeEarliest ensures starting slightly before earliest memory
// does not force a disk resume and returns in-memory data (regression test)
func TestTimeBasedFirstReadBeforeEarliest(t *testing.T) {
	flushed := false
	logBuffer := NewLogBuffer("local", 10*time.Minute,
		func(logBuffer *LogBuffer, startTime, stopTime time.Time, buf []byte, minOffset, maxOffset int64) {
			// keep in memory; we just want earliest time populated
			_ = buf
		},
		func(startPosition MessagePosition, stopTsNs int64, eachLogEntryFn EachLogEntryFuncType) (MessagePosition, bool, error) {
			// disk should not be consulted in this regression path
			return startPosition, false, nil
		},
		func() {})

	// Seed one entry so earliestTime is set
	baseTs := time.Now().Add(-time.Second)
	entry := &filer_pb.LogEntry{TsNs: baseTs.UnixNano(), Data: []byte("x"), Key: []byte("k"), Offset: 0}
	logBuffer.AddLogEntryToBuffer(entry)
	_ = flushed

	// Start read 1ns before earliest memory, with offset sentinel (-2)
	startPos := NewMessagePosition(baseTs.Add(-time.Nanosecond).UnixNano(), -2)
	buf, _, err := logBuffer.ReadFromBuffer(startPos)
	if err != nil {
		t.Fatalf("ReadFromBuffer returned err: %v", err)
	}
	if buf == nil {
		t.Fatalf("Expected in-memory data, got nil buffer")
	}
}

// TestEarliestTimeExactRead ensures starting exactly at earliest time returns first entry (no skip)
func TestEarliestTimeExactRead(t *testing.T) {
	logBuffer := NewLogBuffer("local", 10*time.Minute,
		func(logBuffer *LogBuffer, startTime, stopTime time.Time, buf []byte, minOffset, maxOffset int64) {},
		func(startPosition MessagePosition, stopTsNs int64, eachLogEntryFn EachLogEntryFuncType) (MessagePosition, bool, error) {
			return startPosition, false, nil
		},
		func() {})

	ts := time.Now()
	entry := &filer_pb.LogEntry{TsNs: ts.UnixNano(), Data: []byte("a"), Key: []byte("k"), Offset: 0}
	logBuffer.AddLogEntryToBuffer(entry)

	startPos := NewMessagePosition(ts.UnixNano(), -2)
	buf, _, err := logBuffer.ReadFromBuffer(startPos)
	if err != nil {
		t.Fatalf("ReadFromBuffer err: %v", err)
	}
	if buf == nil || buf.Len() == 0 {
		t.Fatalf("Expected data at earliest time, got nil/empty")
	}
}
