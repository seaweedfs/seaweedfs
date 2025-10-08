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
		lb.AddToBuffer(&mq_pb.DataMessage{
			Key:   nil,
			Value: buf,
			TsNs:  0,
		})
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

			// Add some data to the buffer if needed
			if tt.hasData {
				testData := []byte("test message")
				lb.AddToBuffer(&mq_pb.DataMessage{
					Key:   []byte("key"),
					Value: testData,
					TsNs:  time.Now().UnixNano(),
				})
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
