package log_buffer

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/mq_pb"
	"google.golang.org/protobuf/proto"
)

// TestFlushOffsetGap_ReproduceDataLoss reproduces the critical bug where messages
// are lost in the gap between flushed disk data and in-memory buffer.
//
// OBSERVED BEHAVIOR FROM LOGS:
//   Request offset: 1764
//   Disk contains: 1000-1763 (764 messages)
//   Memory buffer starts at: 1800
//   Gap: 1764-1799 (36 messages) ← MISSING!
//
// This test verifies:
// 1. All messages sent to buffer are accounted for
// 2. No gaps exist between disk and memory offsets
// 3. Flushed data and in-memory data have continuous offset ranges
func TestFlushOffsetGap_ReproduceDataLoss(t *testing.T) {
	var flushedMessages []*filer_pb.LogEntry
	var flushMu sync.Mutex
	
	flushFn := func(logBuffer *LogBuffer, startTime, stopTime time.Time, buf []byte, minOffset, maxOffset int64) {
		t.Logf("FLUSH: minOffset=%d maxOffset=%d size=%d bytes", minOffset, maxOffset, len(buf))
		
		// Parse and store flushed messages
		flushMu.Lock()
		defer flushMu.Unlock()
		
		// Parse buffer to extract messages
		parsedCount := 0
		for pos := 0; pos+4 < len(buf); {
			if pos+4 > len(buf) {
				break
			}
			
			size := uint32(buf[pos])<<24 | uint32(buf[pos+1])<<16 | uint32(buf[pos+2])<<8 | uint32(buf[pos+3])
			if pos+4+int(size) > len(buf) {
				break
			}
			
			entryData := buf[pos+4 : pos+4+int(size)]
			logEntry := &filer_pb.LogEntry{}
			if err := proto.Unmarshal(entryData, logEntry); err == nil {
				flushedMessages = append(flushedMessages, logEntry)
				parsedCount++
			}
			
			pos += 4 + int(size)
		}
		
		t.Logf("  Parsed %d messages from flush buffer", parsedCount)
	}
	
	logBuffer := NewLogBuffer("test", 100*time.Millisecond, flushFn, nil, nil)
	defer logBuffer.ShutdownLogBuffer()
	
	// Send 100 messages
	messageCount := 100
	t.Logf("Sending %d messages...", messageCount)
	
	for i := 0; i < messageCount; i++ {
		logBuffer.AddToBuffer(&mq_pb.DataMessage{
			Key:   []byte(fmt.Sprintf("key-%d", i)),
			Value: []byte(fmt.Sprintf("message-%d", i)),
			TsNs:  time.Now().UnixNano(),
		})
	}
	
	// Force flush multiple times to simulate real workload
	t.Logf("Forcing flush...")
	logBuffer.ForceFlush()
	
	// Add more messages after flush
	for i := messageCount; i < messageCount+50; i++ {
		logBuffer.AddToBuffer(&mq_pb.DataMessage{
			Key:   []byte(fmt.Sprintf("key-%d", i)),
			Value: []byte(fmt.Sprintf("message-%d", i)),
			TsNs:  time.Now().UnixNano(),
		})
	}
	
	// Force another flush
	logBuffer.ForceFlush()
	time.Sleep(200 * time.Millisecond) // Wait for flush to complete
	
	// Now check the buffer state
	logBuffer.RLock()
	bufferStartOffset := logBuffer.bufferStartOffset
	currentOffset := logBuffer.offset
	pos := logBuffer.pos
	logBuffer.RUnlock()
	
	flushMu.Lock()
	flushedCount := len(flushedMessages)
	var maxFlushedOffset int64 = -1
	var minFlushedOffset int64 = -1
	if flushedCount > 0 {
		minFlushedOffset = flushedMessages[0].Offset
		maxFlushedOffset = flushedMessages[flushedCount-1].Offset
	}
	flushMu.Unlock()
	
	t.Logf("\nBUFFER STATE AFTER FLUSH:")
	t.Logf("  bufferStartOffset: %d", bufferStartOffset)
	t.Logf("  currentOffset (HWM): %d", currentOffset)
	t.Logf("  pos (bytes in buffer): %d", pos)
	t.Logf("  Messages sent: %d (offsets 0-%d)", messageCount+50, messageCount+49)
	t.Logf("  Messages flushed to disk: %d (offsets %d-%d)", flushedCount, minFlushedOffset, maxFlushedOffset)
	
	// CRITICAL CHECK: Is there a gap between flushed data and memory buffer?
	if flushedCount > 0 && maxFlushedOffset >= 0 {
		gap := bufferStartOffset - (maxFlushedOffset + 1)
		
		t.Logf("\nOFFSET CONTINUITY CHECK:")
		t.Logf("  Last flushed offset: %d", maxFlushedOffset)
		t.Logf("  Buffer starts at: %d", bufferStartOffset)
		t.Logf("  Gap: %d offsets", gap)
		
		if gap > 0 {
			t.Errorf("❌ CRITICAL BUG REPRODUCED: OFFSET GAP DETECTED!")
			t.Errorf("   Disk has offsets %d-%d", minFlushedOffset, maxFlushedOffset)
			t.Errorf("   Memory buffer starts at: %d", bufferStartOffset)
			t.Errorf("   MISSING OFFSETS: %d-%d (%d messages)", maxFlushedOffset+1, bufferStartOffset-1, gap)
			t.Errorf("   These messages are LOST - neither on disk nor in memory!")
		} else if gap < 0 {
			t.Errorf("❌ OFFSET OVERLAP: Memory buffer starts BEFORE last flushed offset!")
			t.Errorf("   This indicates data corruption or race condition")
		} else {
			t.Logf("✅ PASS: No gap detected - offsets are continuous")
		}
		
		// Check if we can read all expected offsets
		t.Logf("\nREADABILITY CHECK:")
		for testOffset := int64(0); testOffset < currentOffset; testOffset += 10 {
			// Try to read from buffer
			requestPosition := NewMessagePositionFromOffset(testOffset)
			buf, _, err := logBuffer.ReadFromBuffer(requestPosition)
			
			isReadable := (buf != nil && len(buf.Bytes()) > 0) || err == ResumeFromDiskError
			status := "✅"
			if !isReadable && err == nil {
				status = "❌ NOT READABLE"
			}
			
			t.Logf("  Offset %d: %s (buf=%v, err=%v)", testOffset, status, buf != nil, err)
			
			// If offset is in the gap, it should fail to read
			if flushedCount > 0 && testOffset > maxFlushedOffset && testOffset < bufferStartOffset {
				if isReadable {
					t.Errorf("   Unexpected: Offset %d in gap range should NOT be readable!", testOffset)
				} else {
					t.Logf("   Expected: Offset %d in gap is not readable (data lost)", testOffset)
				}
			}
		}
	}
	
	// Check that all sent messages are accounted for
	expectedMessageCount := messageCount + 50
	messagesInMemory := int(currentOffset - bufferStartOffset)
	totalAccountedFor := flushedCount + messagesInMemory
	
	t.Logf("\nMESSAGE ACCOUNTING:")
	t.Logf("  Expected: %d messages", expectedMessageCount)
	t.Logf("  Flushed to disk: %d", flushedCount)
	t.Logf("  In memory buffer: %d (offset range %d-%d)", messagesInMemory, bufferStartOffset, currentOffset-1)
	t.Logf("  Total accounted for: %d", totalAccountedFor)
	t.Logf("  Missing: %d messages", expectedMessageCount-totalAccountedFor)
	
	if totalAccountedFor < expectedMessageCount {
		t.Errorf("❌ DATA LOSS CONFIRMED: %d messages are missing!", expectedMessageCount-totalAccountedFor)
	} else {
		t.Logf("✅ All messages accounted for")
	}
}

// TestFlushOffsetGap_CheckPrevBuffers tests if messages might be stuck in prevBuffers
// instead of being properly flushed to disk.
func TestFlushOffsetGap_CheckPrevBuffers(t *testing.T) {
	var flushCount int
	var flushMu sync.Mutex
	
	flushFn := func(logBuffer *LogBuffer, startTime, stopTime time.Time, buf []byte, minOffset, maxOffset int64) {
		flushMu.Lock()
		flushCount++
		count := flushCount
		flushMu.Unlock()
		
		t.Logf("FLUSH #%d: minOffset=%d maxOffset=%d size=%d bytes", count, minOffset, maxOffset, len(buf))
	}
	
	logBuffer := NewLogBuffer("test", 100*time.Millisecond, flushFn, nil, nil)
	defer logBuffer.ShutdownLogBuffer()
	
	// Send messages in batches with flushes in between
	for batch := 0; batch < 5; batch++ {
		t.Logf("\nBatch %d:", batch)
		
		// Send 20 messages
		for i := 0; i < 20; i++ {
			offset := int64(batch*20 + i)
			logBuffer.AddToBuffer(&mq_pb.DataMessage{
				Key:   []byte(fmt.Sprintf("key-%d", offset)),
				Value: []byte(fmt.Sprintf("message-%d", offset)),
				TsNs:  time.Now().UnixNano(),
			})
		}
		
		// Check state before flush
		logBuffer.RLock()
		beforeFlushOffset := logBuffer.offset
		beforeFlushStart := logBuffer.bufferStartOffset
		logBuffer.RUnlock()
		
		// Force flush
		logBuffer.ForceFlush()
		time.Sleep(50 * time.Millisecond)
		
		// Check state after flush
		logBuffer.RLock()
		afterFlushOffset := logBuffer.offset
		afterFlushStart := logBuffer.bufferStartOffset
		prevBufferCount := len(logBuffer.prevBuffers.buffers)
		
		// Check prevBuffers state
		t.Logf("  Before flush: offset=%d, bufferStartOffset=%d", beforeFlushOffset, beforeFlushStart)
		t.Logf("  After flush: offset=%d, bufferStartOffset=%d, prevBuffers=%d",
			afterFlushOffset, afterFlushStart, prevBufferCount)
		
		// Check each prevBuffer
		for i, prevBuf := range logBuffer.prevBuffers.buffers {
			if prevBuf.size > 0 {
				t.Logf("    prevBuffer[%d]: offsets %d-%d, size=%d bytes (NOT FLUSHED!)",
					i, prevBuf.startOffset, prevBuf.offset, prevBuf.size)
			}
		}
		logBuffer.RUnlock()
		
		// CRITICAL: Check if bufferStartOffset advanced correctly
		expectedNewStart := beforeFlushOffset
		if afterFlushStart != expectedNewStart {
			t.Errorf("  ❌ bufferStartOffset mismatch!")
			t.Errorf("     Expected: %d (= offset before flush)", expectedNewStart)
			t.Errorf("     Actual: %d", afterFlushStart)
			t.Errorf("     Gap: %d offsets", expectedNewStart-afterFlushStart)
		}
	}
}

// TestFlushOffsetGap_ConcurrentWriteAndFlush tests for race conditions
// between writing new messages and flushing old ones.
func TestFlushOffsetGap_ConcurrentWriteAndFlush(t *testing.T) {
	var allFlushedOffsets []int64
	var flushMu sync.Mutex
	
	flushFn := func(logBuffer *LogBuffer, startTime, stopTime time.Time, buf []byte, minOffset, maxOffset int64) {
		t.Logf("FLUSH: offsets %d-%d (%d bytes)", minOffset, maxOffset, len(buf))
		
		flushMu.Lock()
		// Record the offset range that was flushed
		for offset := minOffset; offset <= maxOffset; offset++ {
			allFlushedOffsets = append(allFlushedOffsets, offset)
		}
		flushMu.Unlock()
	}
	
	logBuffer := NewLogBuffer("test", 50*time.Millisecond, flushFn, nil, nil)
	defer logBuffer.ShutdownLogBuffer()
	
	// Concurrently write messages and force flushes
	var wg sync.WaitGroup
	
	// Writer goroutine
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 200; i++ {
			logBuffer.AddToBuffer(&mq_pb.DataMessage{
				Key:   []byte(fmt.Sprintf("key-%d", i)),
				Value: []byte(fmt.Sprintf("message-%d", i)),
				TsNs:  time.Now().UnixNano(),
			})
			if i%50 == 0 {
				time.Sleep(10 * time.Millisecond)
			}
		}
	}()
	
	// Flusher goroutine
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 5; i++ {
			time.Sleep(30 * time.Millisecond)
			logBuffer.ForceFlush()
		}
	}()
	
	wg.Wait()
	time.Sleep(200 * time.Millisecond) // Wait for final flush
	
	// Check final state
	logBuffer.RLock()
	finalOffset := logBuffer.offset
	finalBufferStart := logBuffer.bufferStartOffset
	logBuffer.RUnlock()
	
	flushMu.Lock()
	flushedCount := len(allFlushedOffsets)
	flushMu.Unlock()
	
	expectedCount := int(finalOffset)
	inMemory := int(finalOffset - finalBufferStart)
	totalAccountedFor := flushedCount + inMemory
	
	t.Logf("\nFINAL STATE:")
	t.Logf("  Total messages sent: %d (offsets 0-%d)", expectedCount, expectedCount-1)
	t.Logf("  Flushed to disk: %d", flushedCount)
	t.Logf("  In memory: %d (offsets %d-%d)", inMemory, finalBufferStart, finalOffset-1)
	t.Logf("  Total accounted: %d", totalAccountedFor)
	t.Logf("  Missing: %d", expectedCount-totalAccountedFor)
	
	if totalAccountedFor < expectedCount {
		t.Errorf("❌ DATA LOSS in concurrent scenario: %d messages missing!", expectedCount-totalAccountedFor)
	}
}

// TestFlushOffsetGap_AddToBufferDoesNotIncrementOffset reproduces THE ACTUAL BUG
// The broker uses AddToBuffer() which calls AddDataToBuffer()
// AddDataToBuffer() does NOT increment logBuffer.offset!
// When flush happens, bufferStartOffset = stale offset → GAP!
func TestFlushOffsetGap_AddToBufferDoesNotIncrementOffset(t *testing.T) {
	flushedRanges := []struct{ min, max int64 }{}
	var flushMu sync.Mutex
	
	flushFn := func(logBuffer *LogBuffer, startTime, stopTime time.Time, buf []byte, minOffset, maxOffset int64) {
		flushMu.Lock()
		flushedRanges = append(flushedRanges, struct{ min, max int64 }{minOffset, maxOffset})
		flushMu.Unlock()
		t.Logf("FLUSH: offsets %d-%d (%d bytes)", minOffset, maxOffset, len(buf))
	}
	
	logBuffer := NewLogBuffer("test", time.Hour, flushFn, nil, nil)
	defer logBuffer.ShutdownLogBuffer()
	
	// Manually initialize offset to simulate broker state after restart
	// (e.g., broker finds 1000 messages on disk, sets offset=1000)
	logBuffer.Lock()
	logBuffer.offset = 1000
	logBuffer.bufferStartOffset = 1000
	logBuffer.Unlock()
	
	t.Logf("Initial state: offset=%d, bufferStartOffset=%d", 1000, 1000)
	
	// Now add messages using AddToBuffer (like the broker does)
	// This is the REAL PRODUCTION PATH
	for i := 0; i < 100; i++ {
		message := &mq_pb.DataMessage{
			Key:   []byte(fmt.Sprintf("key-%d", i)),
			Value: []byte(fmt.Sprintf("message-%d", i)),
			TsNs:  time.Now().UnixNano(),
		}
		logBuffer.AddToBuffer(message) // ← BUG: Does NOT increment logBuffer.offset!
	}
	
	// Check state before flush
	logBuffer.RLock()
	beforeFlushOffset := logBuffer.offset
	beforeFlushStart := logBuffer.bufferStartOffset
	logBuffer.RUnlock()
	
	t.Logf("Before flush: offset=%d, bufferStartOffset=%d", beforeFlushOffset, beforeFlushStart)
	
	// Force flush
	logBuffer.ForceFlush()
	time.Sleep(100 * time.Millisecond)
	
	// Check state after flush
	logBuffer.RLock()
	afterFlushOffset := logBuffer.offset
	afterFlushStart := logBuffer.bufferStartOffset
	logBuffer.RUnlock()
	
	flushMu.Lock()
	if len(flushedRanges) > 0 {
		flushedMin := flushedRanges[0].min
		flushedMax := flushedRanges[0].max
		flushMu.Unlock()
		
		t.Logf("After flush: offset=%d, bufferStartOffset=%d", afterFlushOffset, afterFlushStart)
		t.Logf("Flushed range: %d-%d (but these are minOffset/maxOffset, which are 0!)", flushedMin, flushedMax)
		
		// Expected behavior: offset should increment from 1000 to 1100
		expectedOffsetAfterAdd := int64(1100)
		expectedBufferStartAfterFlush := int64(1100)
		
		// Check if offset was incremented
		if beforeFlushOffset != expectedOffsetAfterAdd {
			t.Errorf("")
			t.Errorf("❌❌❌ BUG: offset not incremented correctly ❌❌❌")
			t.Errorf("  Expected offset after adding 100 messages: %d", expectedOffsetAfterAdd)
			t.Errorf("  Actual offset: %d", beforeFlushOffset)
			t.Errorf("  This means AddDataToBuffer() is not incrementing offset!")
		} else {
			t.Logf("✅ offset correctly incremented to %d", beforeFlushOffset)
		}
		
		// Check if bufferStartOffset was set correctly after flush
		if afterFlushStart != expectedBufferStartAfterFlush {
			t.Errorf("")
			t.Errorf("❌❌❌ BUG: bufferStartOffset not set correctly after flush ❌❌❌")
			t.Errorf("  Expected bufferStartOffset after flush: %d", expectedBufferStartAfterFlush)
			t.Errorf("  Actual bufferStartOffset: %d", afterFlushStart)
			t.Errorf("  Gap: %d offsets will be LOST!", expectedBufferStartAfterFlush-afterFlushStart)
		} else {
			t.Logf("✅ bufferStartOffset correctly set to %d after flush", afterFlushStart)
		}
		
		// Overall verdict
		if beforeFlushOffset == expectedOffsetAfterAdd && afterFlushStart == expectedBufferStartAfterFlush {
			t.Logf("")
			t.Logf("🎉🎉🎉 FIX VERIFIED: Buffer flush offset gap bug is FIXED! 🎉🎉🎉")
		}
	} else {
		flushMu.Unlock()
		t.Error("No flush occurred!")
	}
}

// TestFlushOffsetGap_WithExplicitOffsets reproduces the bug using explicit offset assignment
// This matches how the MQ broker uses LogBuffer in production.
func TestFlushOffsetGap_WithExplicitOffsets(t *testing.T) {
	flushedRanges := []struct{ min, max int64 }{}
	var flushMu sync.Mutex
	
	flushFn := func(logBuffer *LogBuffer, startTime, stopTime time.Time, buf []byte, minOffset, maxOffset int64) {
		flushMu.Lock()
		flushedRanges = append(flushedRanges, struct{ min, max int64 }{minOffset, maxOffset})
		flushMu.Unlock()
		t.Logf("FLUSH: offsets %d-%d (%d bytes)", minOffset, maxOffset, len(buf))
	}
	
	logBuffer := NewLogBuffer("test", time.Hour, flushFn, nil, nil)
	defer logBuffer.ShutdownLogBuffer()
	
	// Simulate MQ broker behavior: add messages with explicit offsets
	for i := int64(0); i < 30; i++ {
		logEntry := &filer_pb.LogEntry{
			Key:    []byte(fmt.Sprintf("key-%d", i)),
			Data:   []byte(fmt.Sprintf("message-%d", i)),
			TsNs:   time.Now().UnixNano(),
			Offset: i, // EXPLICIT OFFSET
		}
		logBuffer.AddLogEntryToBuffer(logEntry)
	}
	
	// Check state before flush
	logBuffer.RLock()
	beforeFlushOffset := logBuffer.offset
	beforeFlushStart := logBuffer.bufferStartOffset
	logBuffer.RUnlock()
	
	t.Logf("Before flush: offset=%d, bufferStartOffset=%d", beforeFlushOffset, beforeFlushStart)
	
	// Force flush
	logBuffer.ForceFlush()
	time.Sleep(100 * time.Millisecond)
	
	// Check state after flush
	logBuffer.RLock()
	afterFlushOffset := logBuffer.offset
	afterFlushStart := logBuffer.bufferStartOffset
	logBuffer.RUnlock()
	
	flushMu.Lock()
	if len(flushedRanges) > 0 {
		flushedMin := flushedRanges[0].min
		flushedMax := flushedRanges[0].max
		flushMu.Unlock()
		
		t.Logf("After flush: offset=%d, bufferStartOffset=%d", afterFlushOffset, afterFlushStart)
		t.Logf("Flushed range: %d-%d", flushedMin, flushedMax)
		
		// CRITICAL BUG CHECK: bufferStartOffset should be flushedMax + 1
		expectedBufferStart := flushedMax + 1
		if afterFlushStart != expectedBufferStart {
			t.Errorf("❌ CRITICAL BUG REPRODUCED!")
			t.Errorf("   Flushed messages: offsets %d-%d", flushedMin, flushedMax)
			t.Errorf("   Expected bufferStartOffset: %d (= maxOffset + 1)", expectedBufferStart)
			t.Errorf("   Actual bufferStartOffset: %d", afterFlushStart)
			t.Errorf("   This creates a GAP: offsets %d-%d will be LOST!", expectedBufferStart, afterFlushStart-1)
			t.Errorf("")
			t.Errorf("ROOT CAUSE: logBuffer.offset is not updated when AddLogEntryToBuffer is called")
			t.Errorf("   AddLogEntryToBuffer only updates minOffset/maxOffset")
			t.Errorf("   But copyToFlush sets bufferStartOffset = logBuffer.offset")
			t.Errorf("   Since logBuffer.offset is stale, a gap is created!")
		} else {
			t.Logf("✅ bufferStartOffset correctly set to %d", afterFlushStart)
		}
	} else {
		flushMu.Unlock()
		t.Error("No flush occurred!")
	}
}

// TestFlushOffsetGap_ForceFlushAdvancesBuffer tests if ForceFlush
// properly advances bufferStartOffset after flushing.
func TestFlushOffsetGap_ForceFlushAdvancesBuffer(t *testing.T) {
	flushedRanges := []struct{ min, max int64 }{}
	var flushMu sync.Mutex
	
	flushFn := func(logBuffer *LogBuffer, startTime, stopTime time.Time, buf []byte, minOffset, maxOffset int64) {
		flushMu.Lock()
		flushedRanges = append(flushedRanges, struct{ min, max int64 }{minOffset, maxOffset})
		flushMu.Unlock()
		t.Logf("FLUSH: offsets %d-%d", minOffset, maxOffset)
	}
	
	logBuffer := NewLogBuffer("test", time.Hour, flushFn, nil, nil) // Long interval, manual flush only
	defer logBuffer.ShutdownLogBuffer()
	
	// Send messages, flush, check state - repeat
	for round := 0; round < 3; round++ {
		t.Logf("\n=== ROUND %d ===", round)
		
		// Check state before adding messages
		logBuffer.RLock()
		beforeOffset := logBuffer.offset
		beforeStart := logBuffer.bufferStartOffset
		logBuffer.RUnlock()
		
		t.Logf("Before adding: offset=%d, bufferStartOffset=%d", beforeOffset, beforeStart)
		
		// Add 10 messages
		for i := 0; i < 10; i++ {
			logBuffer.AddToBuffer(&mq_pb.DataMessage{
				Key:   []byte(fmt.Sprintf("round-%d-msg-%d", round, i)),
				Value: []byte(fmt.Sprintf("data-%d-%d", round, i)),
				TsNs:  time.Now().UnixNano(),
			})
		}
		
		// Check state after adding
		logBuffer.RLock()
		afterAddOffset := logBuffer.offset
		afterAddStart := logBuffer.bufferStartOffset
		logBuffer.RUnlock()
		
		t.Logf("After adding: offset=%d, bufferStartOffset=%d", afterAddOffset, afterAddStart)
		
		// Force flush
		t.Logf("Forcing flush...")
		logBuffer.ForceFlush()
		time.Sleep(100 * time.Millisecond)
		
		// Check state after flush
		logBuffer.RLock()
		afterFlushOffset := logBuffer.offset
		afterFlushStart := logBuffer.bufferStartOffset
		logBuffer.RUnlock()
		
		t.Logf("After flush: offset=%d, bufferStartOffset=%d", afterFlushOffset, afterFlushStart)
		
		// CRITICAL CHECK: bufferStartOffset should advance to where offset was before flush
		if afterFlushStart != afterAddOffset {
			t.Errorf("❌ FLUSH BUG: bufferStartOffset did NOT advance correctly!")
			t.Errorf("   Expected bufferStartOffset=%d (= offset after add)", afterAddOffset)
			t.Errorf("   Actual bufferStartOffset=%d", afterFlushStart)
			t.Errorf("   Gap: %d offsets WILL BE LOST", afterAddOffset-afterFlushStart)
		} else {
			t.Logf("✅ bufferStartOffset correctly advanced to %d", afterFlushStart)
		}
	}
	
	// Final verification: check all offset ranges are continuous
	flushMu.Lock()
	t.Logf("\n=== FLUSHED RANGES ===")
	for i, r := range flushedRanges {
		t.Logf("Flush #%d: offsets %d-%d", i, r.min, r.max)
		
		// Check continuity with previous flush
		if i > 0 {
			prevMax := flushedRanges[i-1].max
			currentMin := r.min
			gap := currentMin - (prevMax + 1)
			
			if gap > 0 {
				t.Errorf("❌ GAP between flush #%d and #%d: %d offsets missing!", i-1, i, gap)
			} else if gap < 0 {
				t.Errorf("❌ OVERLAP between flush #%d and #%d: %d offsets duplicated!", i-1, i, -gap)
			} else {
				t.Logf("  ✅ Continuous with previous flush")
			}
		}
	}
	flushMu.Unlock()
}

