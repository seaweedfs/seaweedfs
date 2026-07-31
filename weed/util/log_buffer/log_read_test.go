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
	maxCalls := 4
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

	minExpectedTime := time.Duration(maxCalls-1) * notificationHealthCheckInterval
	if elapsed < minExpectedTime {
		t.Errorf("Loop exited too quickly (%v), expected at least %v (suggests busy-waiting)", elapsed, minExpectedTime)
	}

	maxExpectedTime := time.Duration(maxCalls+1) * notificationHealthCheckInterval
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
	maxCalls := 3
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

	minExpectedTime := time.Duration(maxCalls-1) * notificationHealthCheckInterval
	if elapsed < minExpectedTime {
		t.Errorf("Loop exited too quickly (%v), expected at least %v (suggests missing wait)", elapsed, minExpectedTime)
	}

	t.Logf("Loop exited cleanly in %v after %d iterations (proper sleep detected)", elapsed, callCount)
}

// TestLoopFlush_NotifiesSubscribersAfterFlush is a regression test for the
// issue #9007 fix: loopFlush must call notifySubscribers() after processing a
// flush so that readers parked on notifyChan wake up when a flush lands. The
// classic bug scenario is a reader that got ResumeFromDiskError, did a disk
// read that raced the flush and found nothing, and is now blocked on
// notifyChan waiting for the data that just hit disk.
//
// We drain the AddToBuffer notification first, then ForceFlush, and assert a
// new notification is delivered on notifyChan well before the fallback
// timeout. If the loopFlush notification is removed, this test fails by
// hitting the fallback.
func TestLoopFlush_NotifiesSubscribersAfterFlush(t *testing.T) {
	flushFn := func(logBuffer *LogBuffer, startTime, stopTime time.Time, buf []byte, minOffset, maxOffset int64) {
	}
	logBuffer := NewLogBuffer("test", 1*time.Minute, flushFn, nil, nil)
	defer logBuffer.ShutdownLogBuffer()

	notifyChan := logBuffer.RegisterSubscriber("flush-notify-test")
	defer logBuffer.UnregisterSubscriber("flush-notify-test")

	if err := logBuffer.AddToBuffer(&mq_pb.DataMessage{
		Key:   []byte("k"),
		Value: []byte("v"),
		TsNs:  time.Now().UnixNano(),
	}); err != nil {
		t.Fatalf("AddToBuffer: %v", err)
	}

	// Consume the AddToBuffer notification so the channel starts empty.
	select {
	case <-notifyChan:
	case <-time.After(100 * time.Millisecond):
		t.Fatal("expected a notification from AddToBuffer")
	}

	// ForceFlush waits for loopFlush to process the flush. After it returns,
	// loopFlush must have called notifySubscribers() again.
	start := time.Now()
	logBuffer.ForceFlush()

	select {
	case <-notifyChan:
		elapsed := time.Since(start)
		// The fallback timeout is notificationHealthCheckInterval; the flush
		// notification should arrive well before that.
		if elapsed >= notificationHealthCheckInterval {
			t.Errorf("flush notification too slow: %v (>= fallback %v)", elapsed, notificationHealthCheckInterval)
		}
		t.Logf("flush notification delivered in %v", elapsed)
	case <-time.After(notificationHealthCheckInterval):
		t.Fatalf("loopFlush did not notify subscribers within %v", notificationHealthCheckInterval)
	}
}

// TestLoopProcessLogDataWithOffset_WakesOnDataArrival drives a real
// LoopProcessLogDataWithOffset reader from an empty buffer (readFromDiskFn
// returns nothing, forcing the reader to park on notifyChan after the
// ResumeFromDiskError branch), then adds data from another goroutine and
// asserts the reader completes well before the fallback timeout would fire.
// This protects the end-to-end wake-up path; the loopFlush-specific
// notification is covered by TestLoopFlush_NotifiesSubscribersAfterFlush.
func TestLoopProcessLogDataWithOffset_WakesOnDataArrival(t *testing.T) {
	readFromDiskFn := func(startPosition MessagePosition, stopTsNs int64, eachLogEntryFn EachLogEntryFuncType) (MessagePosition, bool, error) {
		// No data on disk; return unchanged so the reader parks on notifyChan.
		return startPosition, false, nil
	}
	flushFn := func(logBuffer *LogBuffer, startTime, stopTime time.Time, buf []byte, minOffset, maxOffset int64) {
	}
	logBuffer := NewLogBuffer("test", 1*time.Minute, flushFn, readFromDiskFn, nil)
	defer logBuffer.ShutdownLogBuffer()

	received := make(chan struct{})
	eachLogEntryFn := func(logEntry *filer_pb.LogEntry, offset int64) (bool, error) {
		close(received)
		return true, nil // isDone
	}

	waitForDataFn := func() bool { return true }

	startPosition := NewMessagePositionFromOffset(0)

	readerDone := make(chan struct{})
	go func() {
		_, _, _ = logBuffer.LoopProcessLogDataWithOffset(
			"wake-test", startPosition, 0, waitForDataFn, eachLogEntryFn)
		close(readerDone)
	}()

	// Give the reader time to reach awaitNotificationOrTimeout. Both wake
	// paths under test (notifyChan via AddToBuffer and shutdownCh via
	// ShutdownLogBuffer) are race-free even if the reader hasn't parked yet
	// — the notification stays buffered / shutdownCh stays closed — but a
	// generous head start makes it likelier we exercise the actual park-then-
	// wake path rather than the already-pending fast path. 50ms is well below
	// notificationHealthCheckInterval (250ms) and tolerates slow CI.
	time.Sleep(50 * time.Millisecond)

	start := time.Now()
	if err := logBuffer.AddToBuffer(&mq_pb.DataMessage{
		Key:   []byte("k"),
		Value: []byte("v"),
		TsNs:  time.Now().UnixNano(),
	}); err != nil {
		t.Fatalf("AddToBuffer: %v", err)
	}

	select {
	case <-received:
	case <-time.After(notificationHealthCheckInterval):
		t.Fatalf("reader did not process the entry within %v (fallback timeout)", notificationHealthCheckInterval)
	}
	<-readerDone
	elapsed := time.Since(start)

	if elapsed >= notificationHealthCheckInterval {
		t.Errorf("reader wake too slow: %v (>= fallback %v)", elapsed, notificationHealthCheckInterval)
	}
	t.Logf("reader processed the entry in %v after AddToBuffer", elapsed)
}

// TestLoopProcessLogDataWithOffset_WakesOnShutdown verifies that a reader
// parked inside awaitNotificationOrTimeout via the ResumeFromDiskError branch
// exits promptly when ShutdownLogBuffer is called, without waiting for the
// 250ms health-check fallback. Regression guard for the IsStopping() shutdown
// path: if awaitNotificationOrTimeout returns true via shutdownCh and the
// caller does not check IsStopping(), the reader either spins against the
// closed shutdownCh or returns ResumeFromDiskError instead of exiting.
func TestLoopProcessLogDataWithOffset_WakesOnShutdown(t *testing.T) {
	readFromDiskFn := func(startPosition MessagePosition, stopTsNs int64, eachLogEntryFn EachLogEntryFuncType) (MessagePosition, bool, error) {
		// No data on disk; return unchanged so the reader parks on notifyChan.
		return startPosition, false, nil
	}
	flushFn := func(logBuffer *LogBuffer, startTime, stopTime time.Time, buf []byte, minOffset, maxOffset int64) {
	}
	logBuffer := NewLogBuffer("test", 1*time.Minute, flushFn, readFromDiskFn, nil)
	// Note: not deferring ShutdownLogBuffer; we trigger it explicitly below.

	eachLogEntryFn := func(logEntry *filer_pb.LogEntry, offset int64) (bool, error) {
		return false, nil
	}
	waitForDataFn := func() bool { return true }
	startPosition := NewMessagePositionFromOffset(0)

	type result struct {
		isDone bool
		err    error
	}
	resultCh := make(chan result, 1)
	go func() {
		_, isDone, err := logBuffer.LoopProcessLogDataWithOffset(
			"shutdown-test", startPosition, 0, waitForDataFn, eachLogEntryFn)
		resultCh <- result{isDone: isDone, err: err}
	}()

	// Give the reader time to reach awaitNotificationOrTimeout. Both wake
	// paths under test (notifyChan via AddToBuffer and shutdownCh via
	// ShutdownLogBuffer) are race-free even if the reader hasn't parked yet
	// — the notification stays buffered / shutdownCh stays closed — but a
	// generous head start makes it likelier we exercise the actual park-then-
	// wake path rather than the already-pending fast path. 50ms is well below
	// notificationHealthCheckInterval (250ms) and tolerates slow CI.
	time.Sleep(50 * time.Millisecond)

	start := time.Now()
	logBuffer.ShutdownLogBuffer()

	select {
	case r := <-resultCh:
		elapsed := time.Since(start)
		if elapsed >= notificationHealthCheckInterval {
			t.Errorf("reader did not wake on shutdown: %v (>= fallback %v)", elapsed, notificationHealthCheckInterval)
		}
		if !r.isDone {
			t.Errorf("expected isDone=true on shutdown, got false")
		}
		if r.err != nil {
			t.Errorf("expected err=nil on shutdown, got %v", r.err)
		}
		t.Logf("reader exited in %v after ShutdownLogBuffer", elapsed)
	case <-time.After(2 * notificationHealthCheckInterval):
		t.Fatalf("reader did not exit within %v after ShutdownLogBuffer", 2*notificationHealthCheckInterval)
	}
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
		if err := logBuffer.AddToBuffer(msg); err != nil {
			t.Fatalf("Failed to add message to buffer: %v", err)
		}
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

func TestLoopProcessLogData_SlowConsumerFallsBehind(t *testing.T) {
	flushFn := func(logBuffer *LogBuffer, startTime, stopTime time.Time, buf []byte, minOffset, maxOffset int64) {}
	logBuffer := NewLogBuffer("test", 1*time.Minute, flushFn, nil, nil)
	defer logBuffer.ShutdownLogBuffer()

	baseTime := time.Now()
	for i := 0; i < 1000; i++ {
		ts := baseTime.Add(time.Duration(i) * time.Millisecond)
		if err := logBuffer.AddDataToBuffer([]byte("key"), []byte("value"), ts.UnixNano()); err != nil {
			t.Fatalf("AddDataToBuffer(%d): %v", i, err)
		}
	}

	oldPosition := NewMessagePosition(baseTime.Add(-10*time.Second).UnixNano(), 1)

	waitForDataFn := func() bool {
		t.Errorf("waitForDataFn should not be called for a slow consumer that has fallen behind")
		return false
	}

	eachLogEntryFn := func(logEntry *filer_pb.LogEntry) (bool, error) {
		return false, nil
	}

	done := make(chan struct{})
	var err error
	go func() {
		_, _, err = logBuffer.LoopProcessLogData("slow-consumer", oldPosition, 0, waitForDataFn, eachLogEntryFn)
		close(done)
	}()

	select {
	case <-done:
		if err != ResumeFromDiskError {
			t.Fatalf("expected ResumeFromDiskError, got %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("LoopProcessLogData blocked instead of returning ResumeFromDiskError")
	}
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
