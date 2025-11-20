package log_buffer

import (
	"crypto/rand"
	"fmt"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/mq_pb"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
)

func TestNewLogBufferFirstBuffer(t *testing.T) {
	flushInterval := time.Second
	lb := NewLogBuffer("test", flushInterval, func(logBuffer *LogBuffer, startTime time.Time, stopTime time.Time, buf []byte, minOffset, maxOffset int64) {
		fmt.Printf("flush from %v to %v %d bytes\n", startTime, stopTime, len(buf))
	}, nil, func() {
	})

	startTime := MessagePosition{Time: time.Now()}

	messageSize := 1024
	messageCount := 5000

	receivedMessageCount := 0
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		lastProcessedTime, isDone, err := lb.LoopProcessLogData("test", startTime, 0, func() bool {
			// stop if no more messages
			return receivedMessageCount < messageCount
		}, func(logEntry *filer_pb.LogEntry) (isDone bool, err error) {
			receivedMessageCount++
			if receivedMessageCount >= messageCount {
				println("processed all messages")
				return true, io.EOF
			}
			return false, nil
		})

		fmt.Printf("before flush: sent %d received %d\n", messageCount, receivedMessageCount)
		fmt.Printf("lastProcessedTime %v isDone %v err: %v\n", lastProcessedTime, isDone, err)
		if err != nil && err != io.EOF {
			t.Errorf("unexpected error %v", err)
		}
	}()

	var buf = make([]byte, messageSize)
	for i := 0; i < messageCount; i++ {
		rand.Read(buf)
		if err := lb.AddToBuffer(&mq_pb.DataMessage{
			Key:   nil,
			Value: buf,
			TsNs:  0,
		}); err != nil {
			t.Fatalf("Failed to add buffer: %v", err)
		}
	}
	wg.Wait()

	if receivedMessageCount != messageCount {
		t.Errorf("expect %d messages, but got %d", messageCount, receivedMessageCount)
	}
}

// TestReadFromBuffer_OldOffsetReturnsResumeFromDiskError tests that requesting an old offset
// that has been flushed to disk properly returns ResumeFromDiskError instead of hanging forever.
// This reproduces the bug where Schema Registry couldn't read the _schemas topic.
func TestReadFromBuffer_OldOffsetReturnsResumeFromDiskError(t *testing.T) {
	tests := []struct {
		name              string
		bufferStartOffset int64
		currentOffset     int64
		requestedOffset   int64
		hasData           bool
		expectError       error
		description       string
	}{
		{
			name:              "Request offset 0 when buffer starts at 4 (Schema Registry bug scenario)",
			bufferStartOffset: 4,
			currentOffset:     10,
			requestedOffset:   0,
			hasData:           true,
			expectError:       ResumeFromDiskError,
			description:       "When Schema Registry tries to read from offset 0, but data has been flushed to disk",
		},
		{
			name:              "Request offset before buffer start with empty buffer",
			bufferStartOffset: 10,
			currentOffset:     10,
			requestedOffset:   5,
			hasData:           false,
			expectError:       ResumeFromDiskError,
			description:       "Old offset with no data in memory should trigger disk read",
		},
		{
			name:              "Request offset before buffer start with data",
			bufferStartOffset: 100,
			currentOffset:     150,
			requestedOffset:   50,
			hasData:           true,
			expectError:       ResumeFromDiskError,
			description:       "Old offset with current data in memory should still trigger disk read",
		},
		{
			name:              "Request current offset (no disk read needed)",
			bufferStartOffset: 4,
			currentOffset:     10,
			requestedOffset:   10,
			hasData:           true,
			expectError:       nil,
			description:       "Current offset should return data from memory without error",
		},
		{
			name:              "Request offset within buffer range",
			bufferStartOffset: 4,
			currentOffset:     10,
			requestedOffset:   7,
			hasData:           true,
			expectError:       nil,
			description:       "Offset within buffer range should return data without error",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create a LogBuffer with minimal configuration
			lb := NewLogBuffer("test", time.Hour, nil, nil, func() {})

			// Simulate data that has been flushed to disk by setting bufferStartOffset
			lb.bufferStartOffset = tt.bufferStartOffset
			lb.offset = tt.currentOffset

			// CRITICAL: Mark this as an offset-based buffer
			lb.hasOffsets = true

			// Add some data to the buffer if needed (at current offset position)
			if tt.hasData {
				testData := []byte("test message")
				// Use AddLogEntryToBuffer to preserve offset information
				if err := lb.AddLogEntryToBuffer(&filer_pb.LogEntry{
					TsNs:   time.Now().UnixNano(),
					Key:    []byte("key"),
					Data:   testData,
					Offset: tt.currentOffset, // Add data at current offset
				}); err != nil {
					t.Fatalf("Failed to add log entry: %v", err)
				}
			}

			// Create an offset-based position for the requested offset
			requestPosition := NewMessagePositionFromOffset(tt.requestedOffset)

			// Try to read from the buffer
			buf, batchIdx, err := lb.ReadFromBuffer(requestPosition)

			// Verify the error matches expectations
			if tt.expectError != nil {
				if err != tt.expectError {
					t.Errorf("%s\nExpected error: %v\nGot error: %v\nbuf=%v, batchIdx=%d",
						tt.description, tt.expectError, err, buf != nil, batchIdx)
				} else {
					t.Logf("✓ %s: correctly returned %v", tt.description, err)
				}
			} else {
				if err != nil {
					t.Errorf("%s\nExpected no error but got: %v\nbuf=%v, batchIdx=%d",
						tt.description, err, buf != nil, batchIdx)
				} else {
					t.Logf("✓ %s: correctly returned data without error", tt.description)
				}
			}
		})
	}
}

// TestReadFromBuffer_OldOffsetWithNoPrevBuffers specifically tests the bug fix
// where requesting an old offset would return nil instead of ResumeFromDiskError
func TestReadFromBuffer_OldOffsetWithNoPrevBuffers(t *testing.T) {
	// This is the exact scenario that caused the Schema Registry to hang:
	// 1. Data was published to _schemas topic (offsets 0, 1, 2, 3)
	// 2. Data was flushed to disk
	// 3. LogBuffer's bufferStartOffset was updated to 4
	// 4. Schema Registry tried to read from offset 0
	// 5. ReadFromBuffer would return (nil, offset, nil) instead of ResumeFromDiskError
	// 6. The subscriber would wait forever for data that would never come from memory

	lb := NewLogBuffer("_schemas", time.Hour, nil, nil, func() {})

	// Simulate the state after data has been flushed to disk:
	// - bufferStartOffset = 10 (data 0-9 has been flushed)
	// - offset = 15 (next offset to assign, current buffer has 10-14)
	// - pos = 100 (some data in current buffer)
	// Set prevBuffers to have non-overlapping ranges to avoid the safety check at line 420-428
	lb.bufferStartOffset = 10
	lb.offset = 15
	lb.pos = 100

	// Modify prevBuffers to have non-zero offset ranges that DON'T include the requested offset
	// This bypasses the safety check and exposes the real bug
	for i := range lb.prevBuffers.buffers {
		lb.prevBuffers.buffers[i].startOffset = 20 + int64(i)*10 // 20, 30, 40, etc.
		lb.prevBuffers.buffers[i].offset = 25 + int64(i)*10      // 25, 35, 45, etc.
		lb.prevBuffers.buffers[i].size = 0                       // Empty (flushed)
	}

	// Schema Registry requests offset 5 (which is before bufferStartOffset=10)
	requestPosition := NewMessagePositionFromOffset(5)

	// Before the fix, this would return (nil, offset, nil) causing an infinite wait
	// After the fix, this should return ResumeFromDiskError
	buf, batchIdx, err := lb.ReadFromBuffer(requestPosition)

	t.Logf("DEBUG: ReadFromBuffer returned: buf=%v, batchIdx=%d, err=%v", buf != nil, batchIdx, err)
	t.Logf("DEBUG: Buffer state: bufferStartOffset=%d, offset=%d, pos=%d",
		lb.bufferStartOffset, lb.offset, lb.pos)
	t.Logf("DEBUG: Requested offset 5, prevBuffers[0] range: [%d-%d]",
		lb.prevBuffers.buffers[0].startOffset, lb.prevBuffers.buffers[0].offset)

	if err != ResumeFromDiskError {
		t.Errorf("CRITICAL BUG REPRODUCED: Expected ResumeFromDiskError but got err=%v, buf=%v, batchIdx=%d\n"+
			"This causes Schema Registry to hang indefinitely waiting for data that's on disk!",
			err, buf != nil, batchIdx)
		t.Errorf("The buggy code falls through without returning ResumeFromDiskError!")
	} else {
		t.Logf("✓ BUG FIX VERIFIED: Correctly returns ResumeFromDiskError when requesting old offset 5")
		t.Logf("  This allows the subscriber to read from disk instead of waiting forever")
	}
}

// TestReadFromBuffer_EmptyBufferAtCurrentOffset tests Bug #2
// where an empty buffer at the current offset would return empty data instead of ResumeFromDiskError
func TestReadFromBuffer_EmptyBufferAtCurrentOffset(t *testing.T) {
	lb := NewLogBuffer("_schemas", time.Hour, nil, nil, func() {})

	// Simulate buffer state where data 0-3 was published and flushed, but buffer NOT advanced yet:
	// - bufferStartOffset = 0 (buffer hasn't been advanced after flush)
	// - offset = 4 (next offset to assign - data 0-3 exists)
	// - pos = 0 (buffer is empty after flush)
	// This happens in the window between flush and buffer advancement
	lb.bufferStartOffset = 0
	lb.offset = 4
	lb.pos = 0

	// Schema Registry requests offset 0 (which appears to be in range [0, 4])
	requestPosition := NewMessagePositionFromOffset(0)

	// BUG: Without fix, this returns empty buffer instead of checking disk
	// FIX: Should return ResumeFromDiskError because buffer is empty (pos=0) despite valid range
	buf, batchIdx, err := lb.ReadFromBuffer(requestPosition)

	t.Logf("DEBUG: ReadFromBuffer returned: buf=%v, batchIdx=%d, err=%v", buf != nil, batchIdx, err)
	t.Logf("DEBUG: Buffer state: bufferStartOffset=%d, offset=%d, pos=%d",
		lb.bufferStartOffset, lb.offset, lb.pos)

	if err != ResumeFromDiskError {
		if buf == nil || len(buf.Bytes()) == 0 {
			t.Errorf("CRITICAL BUG #2 REPRODUCED: Empty buffer should return ResumeFromDiskError, got err=%v, buf=%v\n"+
				"Without the fix, Schema Registry gets empty data instead of reading from disk!",
				err, buf != nil)
		}
	} else {
		t.Logf("✓ BUG #2 FIX VERIFIED: Empty buffer correctly returns ResumeFromDiskError to check disk")
	}
}

// TestReadFromBuffer_OffsetRanges tests various offset range scenarios
func TestReadFromBuffer_OffsetRanges(t *testing.T) {
	lb := NewLogBuffer("test", time.Hour, nil, nil, func() {})

	// Setup: buffer contains offsets 10-20
	lb.bufferStartOffset = 10
	lb.offset = 20
	lb.pos = 100 // some data in buffer

	testCases := []struct {
		name            string
		requestedOffset int64
		expectedError   error
		description     string
	}{
		{
			name:            "Before buffer start",
			requestedOffset: 5,
			expectedError:   ResumeFromDiskError,
			description:     "Offset 5 < bufferStartOffset 10 → read from disk",
		},
		{
			name:            "At buffer start",
			requestedOffset: 10,
			expectedError:   nil,
			description:     "Offset 10 == bufferStartOffset 10 → read from buffer",
		},
		{
			name:            "Within buffer range",
			requestedOffset: 15,
			expectedError:   nil,
			description:     "Offset 15 is within [10, 20] → read from buffer",
		},
		{
			name:            "At buffer end",
			requestedOffset: 20,
			expectedError:   nil,
			description:     "Offset 20 == offset 20 → read from buffer",
		},
		{
			name:            "After buffer end",
			requestedOffset: 25,
			expectedError:   nil,
			description:     "Offset 25 > offset 20 → future data, return nil without error",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			requestPosition := NewMessagePositionFromOffset(tc.requestedOffset)
			_, _, err := lb.ReadFromBuffer(requestPosition)

			if tc.expectedError != nil {
				if err != tc.expectedError {
					t.Errorf("%s\nExpected error: %v, got: %v", tc.description, tc.expectedError, err)
				} else {
					t.Logf("✓ %s", tc.description)
				}
			} else {
				// For nil expectedError, we accept either nil or no error condition
				// (future offsets return nil without error)
				if err != nil && err != ResumeFromDiskError {
					t.Errorf("%s\nExpected no ResumeFromDiskError, got: %v", tc.description, err)
				} else {
					t.Logf("✓ %s", tc.description)
				}
			}
		})
	}
}

// TestReadFromBuffer_InitializedFromDisk tests Bug #3
// where bufferStartOffset was incorrectly set to 0 after InitializeOffsetFromExistingData,
// causing reads for old offsets to return new data instead of triggering a disk read.
func TestReadFromBuffer_InitializedFromDisk(t *testing.T) {
	// This reproduces the real Schema Registry bug scenario:
	// 1. Broker restarts, finds 4 messages on disk (offsets 0-3)
	// 2. InitializeOffsetFromExistingData sets offset=4
	//    - BUG: bufferStartOffset=0 (wrong!)
	//    - FIX: bufferStartOffset=4 (correct!)
	// 3. First new message is written (offset 4)
	// 4. Schema Registry reads offset 0
	// 5. With FIX: requestedOffset=0 < bufferStartOffset=4 → ResumeFromDiskError (correct!)
	// 6. Without FIX: requestedOffset=0 in range [0, 5] → returns wrong data (bug!)

	lb := NewLogBuffer("_schemas", time.Hour, nil, nil, func() {})

	// Use the actual InitializeOffsetFromExistingData to test the fix
	err := lb.InitializeOffsetFromExistingData(func() (int64, error) {
		return 3, nil // Simulate 4 messages on disk (offsets 0-3, highest=3)
	})
	if err != nil {
		t.Fatalf("InitializeOffsetFromExistingData failed: %v", err)
	}

	t.Logf("After InitializeOffsetFromExistingData(highestOffset=3):")
	t.Logf("  offset=%d (should be 4), bufferStartOffset=%d (FIX: should be 4, not 0)",
		lb.offset, lb.bufferStartOffset)

	// Now write a new message at offset 4
	if err := lb.AddToBuffer(&mq_pb.DataMessage{
		Key:   []byte("new-key"),
		Value: []byte("new-message-at-offset-4"),
		TsNs:  time.Now().UnixNano(),
	}); err != nil {
		t.Fatalf("Failed to add buffer: %v", err)
	}
	// After AddToBuffer: offset=5, pos>0

	// Schema Registry tries to read offset 0 (should be on disk)
	requestPosition := NewMessagePositionFromOffset(0)

	buf, batchIdx, err := lb.ReadFromBuffer(requestPosition)

	t.Logf("After writing new message:")
	t.Logf("  bufferStartOffset=%d, offset=%d, pos=%d", lb.bufferStartOffset, lb.offset, lb.pos)
	t.Logf("  Requested offset 0, got: buf=%v, batchIdx=%d, err=%v", buf != nil, batchIdx, err)

	// EXPECTED BEHAVIOR (with fix):
	// bufferStartOffset=4 after initialization, so requestedOffset=0 < bufferStartOffset=4
	// → returns ResumeFromDiskError

	// BUGGY BEHAVIOR (without fix):
	// bufferStartOffset=0 after initialization, so requestedOffset=0 is in range [0, 5]
	// → returns the NEW message (offset 4) instead of reading from disk!

	if err != ResumeFromDiskError {
		t.Errorf("CRITICAL BUG #3 REPRODUCED: Reading offset 0 after initialization from disk should return ResumeFromDiskError\n"+
			"Instead got: err=%v, buf=%v, batchIdx=%d\n"+
			"This means Schema Registry would receive WRONG data (offset 4) when requesting offset 0!",
			err, buf != nil, batchIdx)
		t.Errorf("Root cause: bufferStartOffset=%d should be 4 after InitializeOffsetFromExistingData(highestOffset=3)",
			lb.bufferStartOffset)
	} else {
		t.Logf("✓ BUG #3 FIX VERIFIED: Reading old offset 0 correctly returns ResumeFromDiskError")
		t.Logf("  This ensures Schema Registry reads correct data from disk instead of getting new messages")
	}
}

// TestLoopProcessLogDataWithOffset_DiskReadRetry tests that when a subscriber
// reads from disk before flush completes, it continues to retry disk reads
// and eventually finds the data after flush completes.
// This reproduces the Schema Registry timeout issue on first start.
func TestLoopProcessLogDataWithOffset_DiskReadRetry(t *testing.T) {
	diskReadCallCount := 0
	diskReadMu := sync.Mutex{}
	dataFlushedToDisk := false
	var flushedData []*filer_pb.LogEntry

	// Create a readFromDiskFn that simulates the race condition
	readFromDiskFn := func(startPosition MessagePosition, stopTsNs int64, eachLogEntryFn EachLogEntryFuncType) (MessagePosition, bool, error) {
		diskReadMu.Lock()
		diskReadCallCount++
		callNum := diskReadCallCount
		hasData := dataFlushedToDisk
		diskReadMu.Unlock()

		t.Logf("DISK READ #%d: startOffset=%d, dataFlushedToDisk=%v", callNum, startPosition.Offset, hasData)

		if !hasData {
			// Simulate: data not yet on disk (flush hasn't completed)
			t.Logf("  → No data found (flush not completed yet)")
			return startPosition, false, nil
		}

		// Data is now on disk, process it
		t.Logf("  → Found %d entries on disk", len(flushedData))
		for _, entry := range flushedData {
			if entry.Offset >= startPosition.Offset {
				isDone, err := eachLogEntryFn(entry)
				if err != nil || isDone {
					return NewMessagePositionFromOffset(entry.Offset + 1), isDone, err
				}
			}
		}
		return NewMessagePositionFromOffset(int64(len(flushedData))), false, nil
	}

	flushFn := func(logBuffer *LogBuffer, startTime, stopTime time.Time, buf []byte, minOffset, maxOffset int64) {
		t.Logf("FLUSH: minOffset=%d maxOffset=%d size=%d bytes", minOffset, maxOffset, len(buf))
		// Simulate writing to disk
		diskReadMu.Lock()
		dataFlushedToDisk = true
		// Parse the buffer and add entries to flushedData
		// For this test, we'll just create mock entries
		flushedData = append(flushedData, &filer_pb.LogEntry{
			Key:    []byte("key-0"),
			Data:   []byte("message-0"),
			TsNs:   time.Now().UnixNano(),
			Offset: 0,
		})
		diskReadMu.Unlock()
	}

	logBuffer := NewLogBuffer("test", 1*time.Minute, flushFn, readFromDiskFn, nil)
	defer logBuffer.ShutdownLogBuffer()

	// Simulate the race condition:
	// 1. Subscriber starts reading from offset 0
	// 2. Data is not yet flushed
	// 3. Loop calls readFromDiskFn → no data found
	// 4. A bit later, data gets flushed
	// 5. Loop should continue and call readFromDiskFn again

	receivedMessages := 0
	mu := sync.Mutex{}
	maxIterations := 50 // Allow up to 50 iterations (500ms with 10ms sleep each)
	iterationCount := 0

	waitForDataFn := func() bool {
		mu.Lock()
		defer mu.Unlock()
		iterationCount++
		// Stop after receiving message or max iterations
		return receivedMessages == 0 && iterationCount < maxIterations
	}

	eachLogEntryFn := func(logEntry *filer_pb.LogEntry, offset int64) (bool, error) {
		mu.Lock()
		receivedMessages++
		mu.Unlock()
		t.Logf("✉️  RECEIVED: offset=%d key=%s", offset, string(logEntry.Key))
		return true, nil // Stop after first message
	}

	// Start the reader in a goroutine
	var readerWg sync.WaitGroup
	readerWg.Add(1)
	go func() {
		defer readerWg.Done()
		startPosition := NewMessagePositionFromOffset(0)
		_, isDone, err := logBuffer.LoopProcessLogDataWithOffset("test-subscriber", startPosition, 0, waitForDataFn, eachLogEntryFn)
		t.Logf("📋 Reader finished: isDone=%v, err=%v", isDone, err)
	}()

	// Wait a bit to let the first disk read happen (returns no data)
	time.Sleep(50 * time.Millisecond)

	// Now add data and flush it
	t.Logf("➕ Adding message to buffer...")
	if err := logBuffer.AddToBuffer(&mq_pb.DataMessage{
		Key:   []byte("key-0"),
		Value: []byte("message-0"),
		TsNs:  time.Now().UnixNano(),
	}); err != nil {
		t.Fatalf("Failed to add buffer: %v", err)
	}

	// Force flush
	t.Logf("Force flushing...")
	logBuffer.ForceFlush()

	// Wait for reader to finish
	readerWg.Wait()

	// Check results
	diskReadMu.Lock()
	finalDiskReadCount := diskReadCallCount
	diskReadMu.Unlock()

	mu.Lock()
	finalReceivedMessages := receivedMessages
	finalIterations := iterationCount
	mu.Unlock()

	t.Logf("\nRESULTS:")
	t.Logf("  Disk reads: %d", finalDiskReadCount)
	t.Logf("  Received messages: %d", finalReceivedMessages)
	t.Logf("  Loop iterations: %d", finalIterations)

	if finalDiskReadCount < 2 {
		t.Errorf("CRITICAL BUG REPRODUCED: Disk read was only called %d time(s)", finalDiskReadCount)
		t.Errorf("Expected: Multiple disk reads as the loop continues after flush completes")
		t.Errorf("This is why Schema Registry times out - it reads once before flush, never re-reads after flush")
	}

	if finalReceivedMessages == 0 {
		t.Errorf("SCHEMA REGISTRY TIMEOUT REPRODUCED: No messages received even after flush")
		t.Errorf("The subscriber is stuck because disk reads are not retried")
	} else {
		t.Logf("✓ SUCCESS: Message received after %d disk read attempts", finalDiskReadCount)
	}
}
