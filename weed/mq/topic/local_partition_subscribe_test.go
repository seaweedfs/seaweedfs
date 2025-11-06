package topic

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util/log_buffer"
)

// MockLogBuffer provides a controllable log buffer for testing
type MockLogBuffer struct {
	// In-memory data
	memoryEntries     []*filer_pb.LogEntry
	memoryStartTime   time.Time
	memoryStopTime    time.Time
	memoryStartOffset int64
	memoryStopOffset  int64

	// Disk data
	diskEntries     []*filer_pb.LogEntry
	diskStartTime   time.Time
	diskStopTime    time.Time
	diskStartOffset int64
	diskStopOffset  int64

	// Behavior control
	diskReadDelay   time.Duration
	memoryReadDelay time.Duration
	diskReadError   error
	memoryReadError error
}

// MockReadFromDiskFn simulates reading from disk
func (m *MockLogBuffer) MockReadFromDiskFn(startPosition log_buffer.MessagePosition, stopTsNs int64, eachLogEntryFn log_buffer.EachLogEntryFuncType) (log_buffer.MessagePosition, bool, error) {
	if m.diskReadDelay > 0 {
		time.Sleep(m.diskReadDelay)
	}

	if m.diskReadError != nil {
		return startPosition, false, m.diskReadError
	}

	isOffsetBased := startPosition.IsOffsetBased
	lastPosition := startPosition
	isDone := false

	for _, entry := range m.diskEntries {
		// Filter based on mode
		if isOffsetBased {
			if entry.Offset < startPosition.Offset {
				continue
			}
		} else {
			entryTime := time.Unix(0, entry.TsNs)
			if entryTime.Before(startPosition.Time) {
				continue
			}
		}

		// Apply stopTsNs filter
		if stopTsNs > 0 && entry.TsNs > stopTsNs {
			isDone = true
			break
		}

		// Call handler
		done, err := eachLogEntryFn(entry)
		if err != nil {
			return lastPosition, false, err
		}
		if done {
			isDone = true
			break
		}

		// Update position
		if isOffsetBased {
			lastPosition = log_buffer.NewMessagePosition(entry.TsNs, entry.Offset+1)
		} else {
			lastPosition = log_buffer.NewMessagePosition(entry.TsNs, entry.Offset)
		}
	}

	return lastPosition, isDone, nil
}

// MockLoopProcessLogDataWithOffset simulates reading from memory with offset
func (m *MockLogBuffer) MockLoopProcessLogDataWithOffset(readerName string, startPosition log_buffer.MessagePosition, stopTsNs int64, waitForDataFn func() bool, eachLogDataFn log_buffer.EachLogEntryWithOffsetFuncType) (log_buffer.MessagePosition, bool, error) {
	if m.memoryReadDelay > 0 {
		time.Sleep(m.memoryReadDelay)
	}

	if m.memoryReadError != nil {
		return startPosition, false, m.memoryReadError
	}

	lastPosition := startPosition
	isDone := false

	// Check if requested offset is in memory
	if startPosition.Offset < m.memoryStartOffset {
		// Data is on disk
		return startPosition, false, log_buffer.ResumeFromDiskError
	}

	for _, entry := range m.memoryEntries {
		// Filter by offset
		if entry.Offset < startPosition.Offset {
			continue
		}

		// Apply stopTsNs filter
		if stopTsNs > 0 && entry.TsNs > stopTsNs {
			isDone = true
			break
		}

		// Call handler
		done, err := eachLogDataFn(entry, entry.Offset)
		if err != nil {
			return lastPosition, false, err
		}
		if done {
			isDone = true
			break
		}

		// Update position
		lastPosition = log_buffer.NewMessagePosition(entry.TsNs, entry.Offset+1)
	}

	return lastPosition, isDone, nil
}

// Helper to create test entries
func createTestEntry(offset int64, timestamp time.Time, key, value string) *filer_pb.LogEntry {
	return &filer_pb.LogEntry{
		TsNs:   timestamp.UnixNano(),
		Offset: offset,
		Key:    []byte(key),
		Data:   []byte(value),
	}
}

// TestOffsetBasedSubscribe_AllDataInMemory tests reading when all data is in memory
func TestOffsetBasedSubscribe_AllDataInMemory(t *testing.T) {
	baseTime := time.Now()

	mock := &MockLogBuffer{
		memoryEntries: []*filer_pb.LogEntry{
			createTestEntry(0, baseTime, "key0", "value0"),
			createTestEntry(1, baseTime.Add(1*time.Second), "key1", "value1"),
			createTestEntry(2, baseTime.Add(2*time.Second), "key2", "value2"),
			createTestEntry(3, baseTime.Add(3*time.Second), "key3", "value3"),
		},
		memoryStartOffset: 0,
		memoryStopOffset:  3,
		diskEntries:       []*filer_pb.LogEntry{}, // No disk data
	}

	// Test reading from offset 0
	t.Run("ReadFromOffset0", func(t *testing.T) {
		var receivedOffsets []int64
		startPos := log_buffer.NewMessagePositionFromOffset(0)

		eachLogFn := func(entry *filer_pb.LogEntry) (bool, error) {
			receivedOffsets = append(receivedOffsets, entry.Offset)
			return false, nil
		}

		// Simulate the Subscribe logic
		// 1. Try disk read first
		pos, done, err := mock.MockReadFromDiskFn(startPos, 0, eachLogFn)
		if err != nil {
			t.Fatalf("Disk read failed: %v", err)
		}
		if done {
			t.Fatal("Should not be done after disk read")
		}

		// 2. Read from memory
		eachLogWithOffsetFn := func(entry *filer_pb.LogEntry, offset int64) (bool, error) {
			return eachLogFn(entry)
		}

		_, _, err = mock.MockLoopProcessLogDataWithOffset("test", pos, 0, func() bool { return true }, eachLogWithOffsetFn)
		if err != nil && err != log_buffer.ResumeFromDiskError {
			t.Fatalf("Memory read failed: %v", err)
		}

		// Verify we got all offsets in order
		expected := []int64{0, 1, 2, 3}
		if len(receivedOffsets) != len(expected) {
			t.Errorf("Expected %d offsets, got %d", len(expected), len(receivedOffsets))
		}
		for i, offset := range receivedOffsets {
			if offset != expected[i] {
				t.Errorf("Offset[%d]: expected %d, got %d", i, expected[i], offset)
			}
		}
	})

	// Test reading from offset 2
	t.Run("ReadFromOffset2", func(t *testing.T) {
		var receivedOffsets []int64
		startPos := log_buffer.NewMessagePositionFromOffset(2)

		eachLogFn := func(entry *filer_pb.LogEntry) (bool, error) {
			receivedOffsets = append(receivedOffsets, entry.Offset)
			return false, nil
		}

		eachLogWithOffsetFn := func(entry *filer_pb.LogEntry, offset int64) (bool, error) {
			return eachLogFn(entry)
		}

		// Should skip disk and go straight to memory
		pos, _, err := mock.MockReadFromDiskFn(startPos, 0, eachLogFn)
		if err != nil {
			t.Fatalf("Disk read failed: %v", err)
		}

		_, _, err = mock.MockLoopProcessLogDataWithOffset("test", pos, 0, func() bool { return true }, eachLogWithOffsetFn)
		if err != nil && err != log_buffer.ResumeFromDiskError {
			t.Fatalf("Memory read failed: %v", err)
		}

		// Verify we got offsets 2, 3
		expected := []int64{2, 3}
		if len(receivedOffsets) != len(expected) {
			t.Errorf("Expected %d offsets, got %d", len(expected), len(receivedOffsets))
		}
		for i, offset := range receivedOffsets {
			if offset != expected[i] {
				t.Errorf("Offset[%d]: expected %d, got %d", i, expected[i], offset)
			}
		}
	})
}

// TestOffsetBasedSubscribe_DataOnDisk tests reading when data is on disk
func TestOffsetBasedSubscribe_DataOnDisk(t *testing.T) {
	baseTime := time.Now()

	mock := &MockLogBuffer{
		// Offsets 0-9 on disk
		diskEntries: []*filer_pb.LogEntry{
			createTestEntry(0, baseTime, "key0", "value0"),
			createTestEntry(1, baseTime.Add(1*time.Second), "key1", "value1"),
			createTestEntry(2, baseTime.Add(2*time.Second), "key2", "value2"),
			createTestEntry(3, baseTime.Add(3*time.Second), "key3", "value3"),
			createTestEntry(4, baseTime.Add(4*time.Second), "key4", "value4"),
			createTestEntry(5, baseTime.Add(5*time.Second), "key5", "value5"),
			createTestEntry(6, baseTime.Add(6*time.Second), "key6", "value6"),
			createTestEntry(7, baseTime.Add(7*time.Second), "key7", "value7"),
			createTestEntry(8, baseTime.Add(8*time.Second), "key8", "value8"),
			createTestEntry(9, baseTime.Add(9*time.Second), "key9", "value9"),
		},
		diskStartOffset: 0,
		diskStopOffset:  9,
		// Offsets 10-12 in memory
		memoryEntries: []*filer_pb.LogEntry{
			createTestEntry(10, baseTime.Add(10*time.Second), "key10", "value10"),
			createTestEntry(11, baseTime.Add(11*time.Second), "key11", "value11"),
			createTestEntry(12, baseTime.Add(12*time.Second), "key12", "value12"),
		},
		memoryStartOffset: 10,
		memoryStopOffset:  12,
	}

	// Test reading from offset 0 (on disk)
	t.Run("ReadFromOffset0_OnDisk", func(t *testing.T) {
		var receivedOffsets []int64
		startPos := log_buffer.NewMessagePositionFromOffset(0)

		eachLogFn := func(entry *filer_pb.LogEntry) (bool, error) {
			receivedOffsets = append(receivedOffsets, entry.Offset)
			return false, nil
		}

		eachLogWithOffsetFn := func(entry *filer_pb.LogEntry, offset int64) (bool, error) {
			return eachLogFn(entry)
		}

		// 1. Read from disk (should get 0-9)
		pos, done, err := mock.MockReadFromDiskFn(startPos, 0, eachLogFn)
		if err != nil {
			t.Fatalf("Disk read failed: %v", err)
		}
		if done {
			t.Fatal("Should not be done after disk read")
		}

		// 2. Read from memory (should get 10-12)
		_, _, err = mock.MockLoopProcessLogDataWithOffset("test", pos, 0, func() bool { return true }, eachLogWithOffsetFn)
		if err != nil && err != log_buffer.ResumeFromDiskError {
			t.Fatalf("Memory read failed: %v", err)
		}

		// Verify we got all offsets 0-12 in order
		expected := []int64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12}
		if len(receivedOffsets) != len(expected) {
			t.Errorf("Expected %d offsets, got %d: %v", len(expected), len(receivedOffsets), receivedOffsets)
		}
		for i, offset := range receivedOffsets {
			if i < len(expected) && offset != expected[i] {
				t.Errorf("Offset[%d]: expected %d, got %d", i, expected[i], offset)
			}
		}
	})

	// Test reading from offset 5 (on disk, middle)
	t.Run("ReadFromOffset5_OnDisk", func(t *testing.T) {
		var receivedOffsets []int64
		startPos := log_buffer.NewMessagePositionFromOffset(5)

		eachLogFn := func(entry *filer_pb.LogEntry) (bool, error) {
			receivedOffsets = append(receivedOffsets, entry.Offset)
			return false, nil
		}

		eachLogWithOffsetFn := func(entry *filer_pb.LogEntry, offset int64) (bool, error) {
			return eachLogFn(entry)
		}

		// 1. Read from disk (should get 5-9)
		pos, _, err := mock.MockReadFromDiskFn(startPos, 0, eachLogFn)
		if err != nil {
			t.Fatalf("Disk read failed: %v", err)
		}

		// 2. Read from memory (should get 10-12)
		_, _, err = mock.MockLoopProcessLogDataWithOffset("test", pos, 0, func() bool { return true }, eachLogWithOffsetFn)
		if err != nil && err != log_buffer.ResumeFromDiskError {
			t.Fatalf("Memory read failed: %v", err)
		}

		// Verify we got offsets 5-12
		expected := []int64{5, 6, 7, 8, 9, 10, 11, 12}
		if len(receivedOffsets) != len(expected) {
			t.Errorf("Expected %d offsets, got %d: %v", len(expected), len(receivedOffsets), receivedOffsets)
		}
		for i, offset := range receivedOffsets {
			if i < len(expected) && offset != expected[i] {
				t.Errorf("Offset[%d]: expected %d, got %d", i, expected[i], offset)
			}
		}
	})

	// Test reading from offset 11 (in memory)
	t.Run("ReadFromOffset11_InMemory", func(t *testing.T) {
		var receivedOffsets []int64
		startPos := log_buffer.NewMessagePositionFromOffset(11)

		eachLogFn := func(entry *filer_pb.LogEntry) (bool, error) {
			receivedOffsets = append(receivedOffsets, entry.Offset)
			return false, nil
		}

		eachLogWithOffsetFn := func(entry *filer_pb.LogEntry, offset int64) (bool, error) {
			return eachLogFn(entry)
		}

		// 1. Try disk read (should get nothing)
		pos, _, err := mock.MockReadFromDiskFn(startPos, 0, eachLogFn)
		if err != nil {
			t.Fatalf("Disk read failed: %v", err)
		}

		// 2. Read from memory (should get 11-12)
		_, _, err = mock.MockLoopProcessLogDataWithOffset("test", pos, 0, func() bool { return true }, eachLogWithOffsetFn)
		if err != nil && err != log_buffer.ResumeFromDiskError {
			t.Fatalf("Memory read failed: %v", err)
		}

		// Verify we got offsets 11-12
		expected := []int64{11, 12}
		if len(receivedOffsets) != len(expected) {
			t.Errorf("Expected %d offsets, got %d: %v", len(expected), len(receivedOffsets), receivedOffsets)
		}
		for i, offset := range receivedOffsets {
			if i < len(expected) && offset != expected[i] {
				t.Errorf("Offset[%d]: expected %d, got %d", i, expected[i], offset)
			}
		}
	})
}

// TestTimestampBasedSubscribe tests timestamp-based reading
func TestTimestampBasedSubscribe(t *testing.T) {
	baseTime := time.Now()

	mock := &MockLogBuffer{
		diskEntries: []*filer_pb.LogEntry{
			createTestEntry(0, baseTime, "key0", "value0"),
			createTestEntry(1, baseTime.Add(10*time.Second), "key1", "value1"),
			createTestEntry(2, baseTime.Add(20*time.Second), "key2", "value2"),
		},
		memoryEntries: []*filer_pb.LogEntry{
			createTestEntry(3, baseTime.Add(30*time.Second), "key3", "value3"),
			createTestEntry(4, baseTime.Add(40*time.Second), "key4", "value4"),
		},
	}

	// Test reading from beginning
	t.Run("ReadFromBeginning", func(t *testing.T) {
		var receivedOffsets []int64
		startPos := log_buffer.NewMessagePosition(baseTime.UnixNano(), -1) // Timestamp-based

		eachLogFn := func(entry *filer_pb.LogEntry) (bool, error) {
			receivedOffsets = append(receivedOffsets, entry.Offset)
			return false, nil
		}

		// Read from disk
		_, _, err := mock.MockReadFromDiskFn(startPos, 0, eachLogFn)
		if err != nil {
			t.Fatalf("Disk read failed: %v", err)
		}

		// In real scenario, would then read from memory using LoopProcessLogData
		// For this test, just verify disk gave us 0-2
		expected := []int64{0, 1, 2}
		if len(receivedOffsets) != len(expected) {
			t.Errorf("Expected %d offsets, got %d", len(expected), len(receivedOffsets))
		}
	})

	// Test reading from middle timestamp
	t.Run("ReadFromMiddleTimestamp", func(t *testing.T) {
		var receivedOffsets []int64
		startPos := log_buffer.NewMessagePosition(baseTime.Add(15*time.Second).UnixNano(), -1)

		eachLogFn := func(entry *filer_pb.LogEntry) (bool, error) {
			receivedOffsets = append(receivedOffsets, entry.Offset)
			return false, nil
		}

		// Read from disk
		_, _, err := mock.MockReadFromDiskFn(startPos, 0, eachLogFn)
		if err != nil {
			t.Fatalf("Disk read failed: %v", err)
		}

		// Should get offset 2 only (timestamp at 20s >= 15s, offset 1 at 10s is excluded)
		expected := []int64{2}
		if len(receivedOffsets) != len(expected) {
			t.Errorf("Expected %d offsets, got %d: %v", len(expected), len(receivedOffsets), receivedOffsets)
		}
	})
}

// TestConcurrentSubscribers tests multiple concurrent subscribers
func TestConcurrentSubscribers(t *testing.T) {
	baseTime := time.Now()

	mock := &MockLogBuffer{
		diskEntries: []*filer_pb.LogEntry{
			createTestEntry(0, baseTime, "key0", "value0"),
			createTestEntry(1, baseTime.Add(1*time.Second), "key1", "value1"),
			createTestEntry(2, baseTime.Add(2*time.Second), "key2", "value2"),
		},
		memoryEntries: []*filer_pb.LogEntry{
			createTestEntry(3, baseTime.Add(3*time.Second), "key3", "value3"),
			createTestEntry(4, baseTime.Add(4*time.Second), "key4", "value4"),
		},
		memoryStartOffset: 3,
		memoryStopOffset:  4,
	}

	var wg sync.WaitGroup
	results := make(map[string][]int64)
	var mu sync.Mutex

	// Spawn 3 concurrent subscribers
	for i := 0; i < 3; i++ {
		wg.Add(1)
		subscriberName := fmt.Sprintf("subscriber-%d", i)

		go func(name string) {
			defer wg.Done()

			var receivedOffsets []int64
			startPos := log_buffer.NewMessagePositionFromOffset(0)

			eachLogFn := func(entry *filer_pb.LogEntry) (bool, error) {
				receivedOffsets = append(receivedOffsets, entry.Offset)
				return false, nil
			}

			eachLogWithOffsetFn := func(entry *filer_pb.LogEntry, offset int64) (bool, error) {
				return eachLogFn(entry)
			}

			// Read from disk
			pos, _, _ := mock.MockReadFromDiskFn(startPos, 0, eachLogFn)

			// Read from memory
			mock.MockLoopProcessLogDataWithOffset(name, pos, 0, func() bool { return true }, eachLogWithOffsetFn)

			mu.Lock()
			results[name] = receivedOffsets
			mu.Unlock()
		}(subscriberName)
	}

	wg.Wait()

	// Verify all subscribers got the same data
	expected := []int64{0, 1, 2, 3, 4}
	for name, offsets := range results {
		if len(offsets) != len(expected) {
			t.Errorf("%s: Expected %d offsets, got %d", name, len(expected), len(offsets))
			continue
		}
		for i, offset := range offsets {
			if offset != expected[i] {
				t.Errorf("%s: Offset[%d]: expected %d, got %d", name, i, expected[i], offset)
			}
		}
	}
}

// TestResumeFromDiskError tests handling of ResumeFromDiskError
func TestResumeFromDiskError(t *testing.T) {
	baseTime := time.Now()

	mock := &MockLogBuffer{
		diskEntries: []*filer_pb.LogEntry{
			createTestEntry(0, baseTime, "key0", "value0"),
			createTestEntry(1, baseTime.Add(1*time.Second), "key1", "value1"),
		},
		memoryEntries: []*filer_pb.LogEntry{
			createTestEntry(10, baseTime.Add(10*time.Second), "key10", "value10"),
		},
		memoryStartOffset: 10,
		memoryStopOffset:  10,
	}

	// Try to read offset 5, which is between disk (0-1) and memory (10)
	// This should trigger ResumeFromDiskError from memory read
	startPos := log_buffer.NewMessagePositionFromOffset(5)

	eachLogFn := func(entry *filer_pb.LogEntry) (bool, error) {
		return false, nil
	}

	eachLogWithOffsetFn := func(entry *filer_pb.LogEntry, offset int64) (bool, error) {
		return eachLogFn(entry)
	}

	// Disk read should return no data (offset 5 > disk end)
	_, _, err := mock.MockReadFromDiskFn(startPos, 0, eachLogFn)
	if err != nil {
		t.Fatalf("Unexpected disk read error: %v", err)
	}

	// Memory read should return ResumeFromDiskError (offset 5 < memory start)
	_, _, err = mock.MockLoopProcessLogDataWithOffset("test", startPos, 0, func() bool { return true }, eachLogWithOffsetFn)
	if err != log_buffer.ResumeFromDiskError {
		t.Errorf("Expected ResumeFromDiskError, got: %v", err)
	}
}
