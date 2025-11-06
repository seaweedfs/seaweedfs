package log_buffer

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/mq_pb"
)

// TestLoopProcessLogDataWithOffset_ClientDisconnect tests that the loop exits
// when the client disconnects (waitForDataFn returns false)
func TestLoopProcessLogDataWithOffset_ClientDisconnect(t *testing.T) {
	flushFn := func(logBuffer *LogBuffer, startTime, stopTime time.Time, buf []byte, minOffset, maxOffset int64) {}
	logBuffer := NewLogBuffer("test", 1*time.Minute, flushFn, nil, nil)
	defer logBuffer.ShutdownLogBuffer()

	// Simulate client disconnect after 100ms
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	waitForDataFn := func() bool {
		select {
		case <-ctx.Done():
			return false // Client disconnected
		default:
			return true
		}
	}

	eachLogEntryFn := func(logEntry *filer_pb.LogEntry, offset int64) (bool, error) {
		return true, nil
	}

	startPosition := NewMessagePositionFromOffset(0)
	startTime := time.Now()

	// This should exit within 200ms (100ms timeout + some buffer)
	_, isDone, _ := logBuffer.LoopProcessLogDataWithOffset("test-client", startPosition, 0, waitForDataFn, eachLogEntryFn)

	elapsed := time.Since(startTime)

	if !isDone {
		t.Errorf("Expected isDone=true when client disconnects, got false")
	}

	if elapsed > 500*time.Millisecond {
		t.Errorf("Loop took too long to exit: %v (expected < 500ms)", elapsed)
	}

	t.Logf("Loop exited cleanly in %v after client disconnect", elapsed)
}

// TestLoopProcessLogDataWithOffset_EmptyBuffer tests that the loop doesn't
// busy-wait when the buffer is empty
func TestLoopProcessLogDataWithOffset_EmptyBuffer(t *testing.T) {
	flushFn := func(logBuffer *LogBuffer, startTime, stopTime time.Time, buf []byte, minOffset, maxOffset int64) {}
	logBuffer := NewLogBuffer("test", 1*time.Minute, flushFn, nil, nil)
	defer logBuffer.ShutdownLogBuffer()

	callCount := 0
	maxCalls := 10
	mu := sync.Mutex{}

	waitForDataFn := func() bool {
		mu.Lock()
		defer mu.Unlock()
		callCount++
		// Disconnect after maxCalls to prevent infinite loop
		return callCount < maxCalls
	}

	eachLogEntryFn := func(logEntry *filer_pb.LogEntry, offset int64) (bool, error) {
		return true, nil
	}

	startPosition := NewMessagePositionFromOffset(0)
	startTime := time.Now()

	_, isDone, _ := logBuffer.LoopProcessLogDataWithOffset("test-client", startPosition, 0, waitForDataFn, eachLogEntryFn)

	elapsed := time.Since(startTime)

	if !isDone {
		t.Errorf("Expected isDone=true when waitForDataFn returns false, got false")
	}

	// With 10ms sleep per iteration, 10 iterations should take ~100ms minimum
	minExpectedTime := time.Duration(maxCalls-1) * 10 * time.Millisecond
	if elapsed < minExpectedTime {
		t.Errorf("Loop exited too quickly (%v), expected at least %v (suggests busy-waiting)", elapsed, minExpectedTime)
	}

	// But shouldn't take more than 2x expected (allows for some overhead)
	maxExpectedTime := time.Duration(maxCalls) * 30 * time.Millisecond
	if elapsed > maxExpectedTime {
		t.Errorf("Loop took too long: %v (expected < %v)", elapsed, maxExpectedTime)
	}

	mu.Lock()
	finalCallCount := callCount
	mu.Unlock()

	if finalCallCount != maxCalls {
		t.Errorf("Expected exactly %d calls to waitForDataFn, got %d", maxCalls, finalCallCount)
	}

	t.Logf("Loop exited cleanly in %v after %d iterations (no busy-waiting detected)", elapsed, finalCallCount)
}

// TestLoopProcessLogDataWithOffset_NoDataResumeFromDisk tests that the loop
// properly handles ResumeFromDiskError without busy-waiting
func TestLoopProcessLogDataWithOffset_NoDataResumeFromDisk(t *testing.T) {
	readFromDiskFn := func(startPosition MessagePosition, stopTsNs int64, eachLogEntryFn EachLogEntryFuncType) (lastReadPosition MessagePosition, isDone bool, err error) {
		// No data on disk
		return startPosition, false, nil
	}
	flushFn := func(logBuffer *LogBuffer, startTime, stopTime time.Time, buf []byte, minOffset, maxOffset int64) {}
	logBuffer := NewLogBuffer("test", 1*time.Minute, flushFn, readFromDiskFn, nil)
	defer logBuffer.ShutdownLogBuffer()

	callCount := 0
	maxCalls := 5
	mu := sync.Mutex{}

	waitForDataFn := func() bool {
		mu.Lock()
		defer mu.Unlock()
		callCount++
		// Disconnect after maxCalls
		return callCount < maxCalls
	}

	eachLogEntryFn := func(logEntry *filer_pb.LogEntry, offset int64) (bool, error) {
		return true, nil
	}

	startPosition := NewMessagePositionFromOffset(0)
	startTime := time.Now()

	_, isDone, _ := logBuffer.LoopProcessLogDataWithOffset("test-client", startPosition, 0, waitForDataFn, eachLogEntryFn)

	elapsed := time.Since(startTime)

	if !isDone {
		t.Errorf("Expected isDone=true when waitForDataFn returns false, got false")
	}

	// Should take at least (maxCalls-1) * 10ms due to sleep in ResumeFromDiskError path
	minExpectedTime := time.Duration(maxCalls-1) * 10 * time.Millisecond
	if elapsed < minExpectedTime {
		t.Errorf("Loop exited too quickly (%v), expected at least %v (suggests missing sleep)", elapsed, minExpectedTime)
	}

	t.Logf("Loop exited cleanly in %v after %d iterations (proper sleep detected)", elapsed, callCount)
}

// TestLoopProcessLogDataWithOffset_WithData tests normal operation with data
func TestLoopProcessLogDataWithOffset_WithData(t *testing.T) {
	flushFn := func(logBuffer *LogBuffer, startTime, stopTime time.Time, buf []byte, minOffset, maxOffset int64) {}
	logBuffer := NewLogBuffer("test", 1*time.Minute, flushFn, nil, nil)
	defer logBuffer.ShutdownLogBuffer()

	// Add some test data to the buffer
	testMessages := []*mq_pb.DataMessage{
		{Key: []byte("key1"), Value: []byte("message1"), TsNs: 1},
		{Key: []byte("key2"), Value: []byte("message2"), TsNs: 2},
		{Key: []byte("key3"), Value: []byte("message3"), TsNs: 3},
	}

	for _, msg := range testMessages {
		logBuffer.AddToBuffer(msg)
	}

	receivedCount := 0
	mu := sync.Mutex{}

	// Disconnect after receiving at least 1 message to test that data processing works
	waitForDataFn := func() bool {
		mu.Lock()
		defer mu.Unlock()
		return receivedCount == 0 // Disconnect after first message
	}

	eachLogEntryFn := func(logEntry *filer_pb.LogEntry, offset int64) (bool, error) {
		mu.Lock()
		receivedCount++
		mu.Unlock()
		return true, nil // Continue processing
	}

	startPosition := NewMessagePositionFromOffset(0)
	startTime := time.Now()

	_, isDone, _ := logBuffer.LoopProcessLogDataWithOffset("test-client", startPosition, 0, waitForDataFn, eachLogEntryFn)

	elapsed := time.Since(startTime)

	if !isDone {
		t.Errorf("Expected isDone=true after client disconnect, got false")
	}

	mu.Lock()
	finalCount := receivedCount
	mu.Unlock()

	if finalCount < 1 {
		t.Errorf("Expected to receive at least 1 message, got %d", finalCount)
	}

	// Should complete quickly since data is available
	if elapsed > 1*time.Second {
		t.Errorf("Processing took too long: %v (expected < 1s)", elapsed)
	}

	t.Logf("Successfully processed %d message(s) in %v", finalCount, elapsed)
}

// TestLoopProcessLogDataWithOffset_ConcurrentDisconnect tests that the loop
// handles concurrent client disconnects without panicking
func TestLoopProcessLogDataWithOffset_ConcurrentDisconnect(t *testing.T) {
	flushFn := func(logBuffer *LogBuffer, startTime, stopTime time.Time, buf []byte, minOffset, maxOffset int64) {}
	logBuffer := NewLogBuffer("test", 1*time.Minute, flushFn, nil, nil)
	defer logBuffer.ShutdownLogBuffer()

	numClients := 10
	var wg sync.WaitGroup

	for i := 0; i < numClients; i++ {
		wg.Add(1)
		go func(clientID int) {
			defer wg.Done()

			ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
			defer cancel()

			waitForDataFn := func() bool {
				select {
				case <-ctx.Done():
					return false
				default:
					return true
				}
			}

			eachLogEntryFn := func(logEntry *filer_pb.LogEntry, offset int64) (bool, error) {
				return true, nil
			}

			startPosition := NewMessagePositionFromOffset(0)
			_, _, _ = logBuffer.LoopProcessLogDataWithOffset("test-client", startPosition, 0, waitForDataFn, eachLogEntryFn)
		}(i)
	}

	// Wait for all clients to finish with a timeout
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		t.Logf("All %d concurrent clients exited cleanly", numClients)
	case <-time.After(5 * time.Second):
		t.Errorf("Timeout waiting for concurrent clients to exit (possible deadlock or stuck loop)")
	}
}

// TestLoopProcessLogDataWithOffset_StopTime tests that the loop respects stopTsNs
func TestLoopProcessLogDataWithOffset_StopTime(t *testing.T) {
	flushFn := func(logBuffer *LogBuffer, startTime, stopTime time.Time, buf []byte, minOffset, maxOffset int64) {}
	logBuffer := NewLogBuffer("test", 1*time.Minute, flushFn, nil, nil)
	defer logBuffer.ShutdownLogBuffer()

	callCount := 0
	waitForDataFn := func() bool {
		callCount++
		// Prevent infinite loop in case of test failure
		return callCount < 10
	}

	eachLogEntryFn := func(logEntry *filer_pb.LogEntry, offset int64) (bool, error) {
		t.Errorf("Should not process any entries when stopTsNs is in the past")
		return false, nil
	}

	startPosition := NewMessagePositionFromOffset(0)
	stopTsNs := time.Now().Add(-1 * time.Hour).UnixNano() // Stop time in the past

	startTime := time.Now()
	_, isDone, _ := logBuffer.LoopProcessLogDataWithOffset("test-client", startPosition, stopTsNs, waitForDataFn, eachLogEntryFn)
	elapsed := time.Since(startTime)

	if !isDone {
		t.Errorf("Expected isDone=true when stopTsNs is in the past, got false")
	}

	if elapsed > 1*time.Second {
		t.Errorf("Loop should exit quickly when stopTsNs is in the past, took %v", elapsed)
	}

	t.Logf("Loop correctly exited for past stopTsNs in %v (waitForDataFn called %d times)", elapsed, callCount)
}

// BenchmarkLoopProcessLogDataWithOffset_EmptyBuffer benchmarks the performance
// of the loop with an empty buffer to ensure no busy-waiting
func BenchmarkLoopProcessLogDataWithOffset_EmptyBuffer(b *testing.B) {
	flushFn := func(logBuffer *LogBuffer, startTime, stopTime time.Time, buf []byte, minOffset, maxOffset int64) {}
	logBuffer := NewLogBuffer("test", 1*time.Minute, flushFn, nil, nil)
	defer logBuffer.ShutdownLogBuffer()

	for i := 0; i < b.N; i++ {
		callCount := 0
		waitForDataFn := func() bool {
			callCount++
			return callCount < 3 // Exit after 3 calls
		}

		eachLogEntryFn := func(logEntry *filer_pb.LogEntry, offset int64) (bool, error) {
			return true, nil
		}

		startPosition := NewMessagePositionFromOffset(0)
		logBuffer.LoopProcessLogDataWithOffset("test-client", startPosition, 0, waitForDataFn, eachLogEntryFn)
	}
}
