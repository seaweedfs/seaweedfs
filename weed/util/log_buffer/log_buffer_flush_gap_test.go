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
//
//	Request offset: 1764
//	Disk contains: 1000-1763 (764 messages)
//	Memory buffer starts at: 1800
//	Gap: 1764-1799 (36 messages) ← MISSING!
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
		if err := logBuffer.AddToBuffer(&mq_pb.DataMessage{
			Key:   []byte(fmt.Sprintf("key-%d", i)),
			Value: []byte(fmt.Sprintf("message-%d", i)),
			TsNs:  time.Now().UnixNano(),
		}); err != nil {
			t.Fatalf("Failed to add buffer: %v", err)
		}
	}

	// Force flush multiple times to simulate real workload
	t.Logf("Forcing flush...")
	logBuffer.ForceFlush()

	// Add more messages after flush
	for i := messageCount; i < messageCount+50; i++ {
		if err := logBuffer.AddToBuffer(&mq_pb.DataMessage{
			Key:   []byte(fmt.Sprintf("key-%d", i)),
			Value: []byte(fmt.Sprintf("message-%d", i)),
			TsNs:  time.Now().UnixNano(),
		}); err != nil {
			t.Fatalf("Failed to add buffer: %v", err)
		}
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
			t.Errorf("CRITICAL BUG REPRODUCED: OFFSET GAP DETECTED!")
			t.Errorf("   Disk has offsets %d-%d", minFlushedOffset, maxFlushedOffset)
			t.Errorf("   Memory buffer starts at: %d", bufferStartOffset)
			t.Errorf("   MISSING OFFSETS: %d-%d (%d messages)", maxFlushedOffset+1, bufferStartOffset-1, gap)
			t.Errorf("   These messages are LOST - neither on disk nor in memory!")
		} else if gap < 0 {
			t.Errorf("OFFSET OVERLAP: Memory buffer starts BEFORE last flushed offset!")
			t.Errorf("   This indicates data corruption or race condition")
		} else {
			t.Logf("PASS: No gap detected - offsets are continuous")
		}

		// Check if we can read all expected offsets
		t.Logf("\nREADABILITY CHECK:")
		for testOffset := int64(0); testOffset < currentOffset; testOffset += 10 {
			// Try to read from buffer
			requestPosition := NewMessagePositionFromOffset(testOffset)
			buf, _, err := logBuffer.ReadFromBuffer(requestPosition)

			isReadable := (buf != nil && len(buf.Bytes()) > 0) || err == ResumeFromDiskError
			status := "OK"
			if !isReadable && err == nil {
				status = "NOT READABLE"
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
		t.Errorf("DATA LOSS CONFIRMED: %d messages are missing!", expectedMessageCount-totalAccountedFor)
	} else {
		t.Logf("All messages accounted for")
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
			if err := logBuffer.AddToBuffer(&mq_pb.DataMessage{
				Key:   []byte(fmt.Sprintf("key-%d", offset)),
				Value: []byte(fmt.Sprintf("message-%d", offset)),
				TsNs:  time.Now().UnixNano(),
			}); err != nil {
				t.Fatalf("Failed to add buffer: %v", err)
			}
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
			t.Errorf("  bufferStartOffset mismatch!")
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
			if err := logBuffer.AddToBuffer(&mq_pb.DataMessage{
				Key:   []byte(fmt.Sprintf("key-%d", i)),
				Value: []byte(fmt.Sprintf("message-%d", i)),
				TsNs:  time.Now().UnixNano(),
			}); err != nil {
				t.Logf("Failed to add buffer: %v", err)
				return
			}
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
		t.Errorf("DATA LOSS in concurrent scenario: %d messages missing!", expectedCount-totalAccountedFor)
	}
}

// TestFlushOffsetGap_ProductionScenario reproduces the actual production scenario
// where the broker uses AddLogEntryToBuffer with explicit Kafka offsets.
// This simulates leader publishing with offset assignment.
func TestFlushOffsetGap_ProductionScenario(t *testing.T) {
	var flushedData []struct {
		minOffset int64
		maxOffset int64
		messages  []*filer_pb.LogEntry
	}
	var flushMu sync.Mutex

	flushFn := func(logBuffer *LogBuffer, startTime, stopTime time.Time, buf []byte, minOffset, maxOffset int64) {
		// Parse messages from buffer
		messages := []*filer_pb.LogEntry{}
		for pos := 0; pos+4 < len(buf); {
			size := uint32(buf[pos])<<24 | uint32(buf[pos+1])<<16 | uint32(buf[pos+2])<<8 | uint32(buf[pos+3])
			if pos+4+int(size) > len(buf) {
				break
			}
			entryData := buf[pos+4 : pos+4+int(size)]
			logEntry := &filer_pb.LogEntry{}
			if err := proto.Unmarshal(entryData, logEntry); err == nil {
				messages = append(messages, logEntry)
			}
			pos += 4 + int(size)
		}

		flushMu.Lock()
		flushedData = append(flushedData, struct {
			minOffset int64
			maxOffset int64
			messages  []*filer_pb.LogEntry
		}{minOffset, maxOffset, messages})
		flushMu.Unlock()

		t.Logf("FLUSH: minOffset=%d maxOffset=%d, parsed %d messages", minOffset, maxOffset, len(messages))
	}

	logBuffer := NewLogBuffer("test", time.Hour, flushFn, nil, nil)
	defer logBuffer.ShutdownLogBuffer()

	// Simulate broker behavior: assign Kafka offsets and add to buffer
	// This is what PublishWithOffset() does
	nextKafkaOffset := int64(0)

	// Round 1: Add 50 messages with Kafka offsets 0-49
	t.Logf("\n=== ROUND 1: Adding messages 0-49 ===")
	for i := 0; i < 50; i++ {
		logEntry := &filer_pb.LogEntry{
			Key:    []byte(fmt.Sprintf("key-%d", i)),
			Data:   []byte(fmt.Sprintf("message-%d", i)),
			TsNs:   time.Now().UnixNano(),
			Offset: nextKafkaOffset, // Explicit Kafka offset
		}
		if err := logBuffer.AddLogEntryToBuffer(logEntry); err != nil {
			t.Fatalf("Failed to add log entry: %v", err)
		}
		nextKafkaOffset++
	}

	// Check buffer state before flush
	logBuffer.RLock()
	beforeFlushOffset := logBuffer.offset
	beforeFlushStart := logBuffer.bufferStartOffset
	logBuffer.RUnlock()
	t.Logf("Before flush: logBuffer.offset=%d, bufferStartOffset=%d, nextKafkaOffset=%d",
		beforeFlushOffset, beforeFlushStart, nextKafkaOffset)

	// Flush
	logBuffer.ForceFlush()
	time.Sleep(100 * time.Millisecond)

	// Check buffer state after flush
	logBuffer.RLock()
	afterFlushOffset := logBuffer.offset
	afterFlushStart := logBuffer.bufferStartOffset
	logBuffer.RUnlock()
	t.Logf("After flush: logBuffer.offset=%d, bufferStartOffset=%d",
		afterFlushOffset, afterFlushStart)

	// Round 2: Add another 50 messages with Kafka offsets 50-99
	t.Logf("\n=== ROUND 2: Adding messages 50-99 ===")
	for i := 0; i < 50; i++ {
		logEntry := &filer_pb.LogEntry{
			Key:    []byte(fmt.Sprintf("key-%d", 50+i)),
			Data:   []byte(fmt.Sprintf("message-%d", 50+i)),
			TsNs:   time.Now().UnixNano(),
			Offset: nextKafkaOffset,
		}
		if err := logBuffer.AddLogEntryToBuffer(logEntry); err != nil {
			t.Fatalf("Failed to add log entry: %v", err)
		}
		nextKafkaOffset++
	}

	logBuffer.ForceFlush()
	time.Sleep(100 * time.Millisecond)

	// Verification: Check if all Kafka offsets are accounted for
	flushMu.Lock()
	t.Logf("\n=== VERIFICATION ===")
	t.Logf("Expected Kafka offsets: 0-%d", nextKafkaOffset-1)

	allOffsets := make(map[int64]bool)
	for flushIdx, flush := range flushedData {
		t.Logf("Flush #%d: minOffset=%d, maxOffset=%d, messages=%d",
			flushIdx, flush.minOffset, flush.maxOffset, len(flush.messages))

		for _, msg := range flush.messages {
			if allOffsets[msg.Offset] {
				t.Errorf("  DUPLICATE: Offset %d appears multiple times!", msg.Offset)
			}
			allOffsets[msg.Offset] = true
		}
	}
	flushMu.Unlock()

	// Check for missing offsets
	missingOffsets := []int64{}
	for expectedOffset := int64(0); expectedOffset < nextKafkaOffset; expectedOffset++ {
		if !allOffsets[expectedOffset] {
			missingOffsets = append(missingOffsets, expectedOffset)
		}
	}

	if len(missingOffsets) > 0 {
		t.Errorf("\nMISSING OFFSETS DETECTED: %d offsets missing", len(missingOffsets))
		if len(missingOffsets) <= 20 {
			t.Errorf("Missing: %v", missingOffsets)
		} else {
			t.Errorf("Missing: %v ... and %d more", missingOffsets[:20], len(missingOffsets)-20)
		}
		t.Errorf("\nThis reproduces the production bug!")
	} else {
		t.Logf("\nSUCCESS: All %d Kafka offsets accounted for (0-%d)", nextKafkaOffset, nextKafkaOffset-1)
	}

	// Check buffer offset consistency
	logBuffer.RLock()
	finalOffset := logBuffer.offset
	finalBufferStart := logBuffer.bufferStartOffset
	logBuffer.RUnlock()

	t.Logf("\nFinal buffer state:")
	t.Logf("  logBuffer.offset: %d", finalOffset)
	t.Logf("  bufferStartOffset: %d", finalBufferStart)
	t.Logf("  Expected (nextKafkaOffset): %d", nextKafkaOffset)

	if finalOffset != nextKafkaOffset {
		t.Errorf("logBuffer.offset mismatch: expected %d, got %d", nextKafkaOffset, finalOffset)
	}
}

// TestFlushOffsetGap_ConcurrentReadDuringFlush tests if concurrent reads
// during flush can cause messages to be missed.
func TestFlushOffsetGap_ConcurrentReadDuringFlush(t *testing.T) {
	var flushedOffsets []int64
	var flushMu sync.Mutex

	readFromDiskFn := func(startPosition MessagePosition, stopTsNs int64, eachLogEntryFn EachLogEntryFuncType) (MessagePosition, bool, error) {
		// Simulate reading from disk - return flushed offsets
		flushMu.Lock()
		defer flushMu.Unlock()

		for _, offset := range flushedOffsets {
			if offset >= startPosition.Offset {
				logEntry := &filer_pb.LogEntry{
					Key:    []byte(fmt.Sprintf("key-%d", offset)),
					Data:   []byte(fmt.Sprintf("message-%d", offset)),
					TsNs:   time.Now().UnixNano(),
					Offset: offset,
				}
				isDone, err := eachLogEntryFn(logEntry)
				if err != nil || isDone {
					return NewMessagePositionFromOffset(offset + 1), isDone, err
				}
			}
		}
		return startPosition, false, nil
	}

	flushFn := func(logBuffer *LogBuffer, startTime, stopTime time.Time, buf []byte, minOffset, maxOffset int64) {
		// Parse and store flushed offsets
		flushMu.Lock()
		defer flushMu.Unlock()

		for pos := 0; pos+4 < len(buf); {
			size := uint32(buf[pos])<<24 | uint32(buf[pos+1])<<16 | uint32(buf[pos+2])<<8 | uint32(buf[pos+3])
			if pos+4+int(size) > len(buf) {
				break
			}
			entryData := buf[pos+4 : pos+4+int(size)]
			logEntry := &filer_pb.LogEntry{}
			if err := proto.Unmarshal(entryData, logEntry); err == nil {
				flushedOffsets = append(flushedOffsets, logEntry.Offset)
			}
			pos += 4 + int(size)
		}

		t.Logf("FLUSH: Stored %d offsets to disk (minOffset=%d, maxOffset=%d)",
			len(flushedOffsets), minOffset, maxOffset)
	}

	logBuffer := NewLogBuffer("test", time.Hour, flushFn, readFromDiskFn, nil)
	defer logBuffer.ShutdownLogBuffer()

	// Add 100 messages
	t.Logf("Adding 100 messages...")
	for i := int64(0); i < 100; i++ {
		logEntry := &filer_pb.LogEntry{
			Key:    []byte(fmt.Sprintf("key-%d", i)),
			Data:   []byte(fmt.Sprintf("message-%d", i)),
			TsNs:   time.Now().UnixNano(),
			Offset: i,
		}
		if err := logBuffer.AddLogEntryToBuffer(logEntry); err != nil {
			t.Fatalf("Failed to add log entry: %v", err)
		}
	}

	// Flush (moves data to disk)
	t.Logf("Flushing...")
	logBuffer.ForceFlush()
	time.Sleep(100 * time.Millisecond)

	// Now try to read all messages using ReadMessagesAtOffset
	t.Logf("\nReading messages from offset 0...")
	messages, nextOffset, hwm, endOfPartition, err := logBuffer.ReadMessagesAtOffset(0, 1000, 1024*1024)

	t.Logf("Read result: messages=%d, nextOffset=%d, hwm=%d, endOfPartition=%v, err=%v",
		len(messages), nextOffset, hwm, endOfPartition, err)

	// Verify all offsets can be read
	readOffsets := make(map[int64]bool)
	for _, msg := range messages {
		readOffsets[msg.Offset] = true
	}

	missingOffsets := []int64{}
	for expectedOffset := int64(0); expectedOffset < 100; expectedOffset++ {
		if !readOffsets[expectedOffset] {
			missingOffsets = append(missingOffsets, expectedOffset)
		}
	}

	if len(missingOffsets) > 0 {
		t.Errorf("MISSING OFFSETS after flush: %d offsets cannot be read", len(missingOffsets))
		if len(missingOffsets) <= 20 {
			t.Errorf("Missing: %v", missingOffsets)
		} else {
			t.Errorf("Missing: %v ... and %d more", missingOffsets[:20], len(missingOffsets)-20)
		}
	} else {
		t.Logf("All 100 offsets can be read after flush")
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
			if err := logBuffer.AddToBuffer(&mq_pb.DataMessage{
				Key:   []byte(fmt.Sprintf("round-%d-msg-%d", round, i)),
				Value: []byte(fmt.Sprintf("data-%d-%d", round, i)),
				TsNs:  time.Now().UnixNano(),
			}); err != nil {
				t.Fatalf("Failed to add buffer: %v", err)
			}
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
			t.Errorf("FLUSH BUG: bufferStartOffset did NOT advance correctly!")
			t.Errorf("   Expected bufferStartOffset=%d (= offset after add)", afterAddOffset)
			t.Errorf("   Actual bufferStartOffset=%d", afterFlushStart)
			t.Errorf("   Gap: %d offsets WILL BE LOST", afterAddOffset-afterFlushStart)
		} else {
			t.Logf("bufferStartOffset correctly advanced to %d", afterFlushStart)
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
				t.Errorf("GAP between flush #%d and #%d: %d offsets missing!", i-1, i, gap)
			} else if gap < 0 {
				t.Errorf("OVERLAP between flush #%d and #%d: %d offsets duplicated!", i-1, i, -gap)
			} else {
				t.Logf("  Continuous with previous flush")
			}
		}
	}
	flushMu.Unlock()
}
