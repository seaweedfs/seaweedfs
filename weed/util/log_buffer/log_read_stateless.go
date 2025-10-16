package log_buffer

import (
	"fmt"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"google.golang.org/protobuf/proto"
)

// ReadMessagesAtOffset provides Kafka-style stateless reads from LogBuffer
// Each call is completely independent - no state maintained between calls
// Thread-safe for concurrent reads at different offsets
//
// This is the recommended API for stateless clients like Kafka gateway
// Unlike Subscribe loops, this:
// 1. Returns immediately with available data (or empty if none)
// 2. Does not maintain any session state
// 3. Safe for concurrent calls
// 4. No cancellation/restart complexity
//
// Returns:
// - messages: Array of messages starting at startOffset
// - nextOffset: Offset to use for next fetch
// - highWaterMark: Highest offset available in partition
// - endOfPartition: True if no more data available
// - err: Any error encountered
func (logBuffer *LogBuffer) ReadMessagesAtOffset(startOffset int64, maxMessages int, maxBytes int) (
	messages []*filer_pb.LogEntry,
	nextOffset int64,
	highWaterMark int64,
	endOfPartition bool,
	err error,
) {
	glog.V(4).Infof("[StatelessRead] Reading from offset %d, maxMessages=%d, maxBytes=%d",
		startOffset, maxMessages, maxBytes)

	// Quick validation
	if maxMessages <= 0 {
		maxMessages = 100 // Default reasonable batch size
	}
	if maxBytes <= 0 {
		maxBytes = 4 * 1024 * 1024 // 4MB default
	}

	messages = make([]*filer_pb.LogEntry, 0, maxMessages)
	nextOffset = startOffset

	// Try to read from in-memory buffers first (hot path)
	logBuffer.RLock()
	currentBufferEnd := logBuffer.offset
	bufferStartOffset := logBuffer.bufferStartOffset
	highWaterMark = currentBufferEnd

	// Special case: empty buffer (no data written yet)
	if currentBufferEnd == 0 && bufferStartOffset == 0 && logBuffer.pos == 0 {
		logBuffer.RUnlock()
		glog.V(4).Infof("[StatelessRead] Empty buffer, returning no data with endOfPartition=true")
		// Return empty result - partition exists but has no data yet
		// Preserve the requested offset in nextOffset
		return messages, startOffset, 0, true, nil
	}

	// Check if requested offset is in current buffer
	if startOffset >= bufferStartOffset && startOffset < currentBufferEnd {
		// Read from current buffer
		glog.V(4).Infof("[StatelessRead] Reading from current buffer: start=%d, end=%d",
			bufferStartOffset, currentBufferEnd)

		if logBuffer.pos > 0 {
			// Make a copy of the buffer to avoid concurrent modification
			bufCopy := make([]byte, logBuffer.pos)
			copy(bufCopy, logBuffer.buf[:logBuffer.pos])
			logBuffer.RUnlock() // Release lock early

			// Parse messages from buffer copy
			messages, nextOffset, _, err = parseMessagesFromBuffer(
				bufCopy, startOffset, maxMessages, maxBytes)

			if err != nil {
				return nil, startOffset, highWaterMark, false, err
			}

			glog.V(4).Infof("[StatelessRead] Read %d messages from current buffer, nextOffset=%d",
				len(messages), nextOffset)

			// Check if we reached the end
			endOfPartition = (nextOffset >= currentBufferEnd) && (len(messages) == 0 || len(messages) < maxMessages)
			return messages, nextOffset, highWaterMark, endOfPartition, nil
		}

		// Buffer is empty but offset is in range - check previous buffers
		logBuffer.RUnlock()

		// Try previous buffers
		logBuffer.RLock()
		for _, prevBuf := range logBuffer.prevBuffers.buffers {
			if startOffset >= prevBuf.startOffset && startOffset <= prevBuf.offset {
				if prevBuf.size > 0 {
					// Found in previous buffer
					bufCopy := make([]byte, prevBuf.size)
					copy(bufCopy, prevBuf.buf[:prevBuf.size])
					logBuffer.RUnlock()

					messages, nextOffset, _, err = parseMessagesFromBuffer(
						bufCopy, startOffset, maxMessages, maxBytes)

					if err != nil {
						return nil, startOffset, highWaterMark, false, err
					}

					glog.V(4).Infof("[StatelessRead] Read %d messages from previous buffer, nextOffset=%d",
						len(messages), nextOffset)

					endOfPartition = false // More data might be in current buffer
					return messages, nextOffset, highWaterMark, endOfPartition, nil
				}
				// Empty previous buffer means data was flushed
				break
			}
		}
		logBuffer.RUnlock()

		// Data not in memory - for stateless fetch, we don't do disk I/O to avoid blocking
		// Return empty with offset out of range indication
		glog.V(2).Infof("[StatelessRead] Data at offset %d not in memory (buffer: %d-%d), returning empty",
			startOffset, bufferStartOffset, currentBufferEnd)
		return messages, startOffset, highWaterMark, false, fmt.Errorf("offset %d out of range (in-memory: %d-%d)",
			startOffset, bufferStartOffset, currentBufferEnd)
	}

	logBuffer.RUnlock()

	// Offset is not in current buffer range
	if startOffset < bufferStartOffset {
		// Historical data - for stateless fetch, we don't do disk I/O to avoid blocking
		// Return empty with offset out of range indication
		glog.V(2).Infof("[StatelessRead] Requested offset %d < buffer start %d (too old), returning empty",
			startOffset, bufferStartOffset)
		return messages, startOffset, highWaterMark, false, fmt.Errorf("offset %d too old (earliest in-memory: %d)",
			startOffset, bufferStartOffset)
	}

	// startOffset > currentBufferEnd - future offset, no data available yet
	glog.V(4).Infof("[StatelessRead] Future offset %d > buffer end %d, no data available",
		startOffset, currentBufferEnd)
	return messages, startOffset, highWaterMark, true, nil
}

// parseMessagesFromBuffer parses messages from a buffer byte slice
// This is thread-safe as it operates on a copy of the buffer
func parseMessagesFromBuffer(buf []byte, startOffset int64, maxMessages int, maxBytes int) (
	messages []*filer_pb.LogEntry,
	nextOffset int64,
	totalBytes int,
	err error,
) {
	messages = make([]*filer_pb.LogEntry, 0, maxMessages)
	nextOffset = startOffset
	totalBytes = 0
	foundStart := false

	for pos := 0; pos+4 < len(buf) && len(messages) < maxMessages && totalBytes < maxBytes; {
		// Read message size
		size := util.BytesToUint32(buf[pos : pos+4])
		if pos+4+int(size) > len(buf) {
			// Incomplete message at end of buffer
			glog.V(4).Infof("[parseMessages] Incomplete message at pos %d, size %d, bufLen %d",
				pos, size, len(buf))
			break
		}

		// Parse message
		entryData := buf[pos+4 : pos+4+int(size)]
		logEntry := &filer_pb.LogEntry{}
		if err = proto.Unmarshal(entryData, logEntry); err != nil {
			glog.Warningf("[parseMessages] Failed to unmarshal message: %v", err)
			pos += 4 + int(size)
			continue
		}

		// Initialize foundStart from first message
		if !foundStart {
			// Find the first message at or after startOffset
			if logEntry.Offset >= startOffset {
				foundStart = true
				nextOffset = logEntry.Offset
			} else {
				// Skip messages before startOffset
				pos += 4 + int(size)
				continue
			}
		}

		// Check if this message matches expected offset
		if foundStart && logEntry.Offset >= startOffset {
			messages = append(messages, logEntry)
			totalBytes += 4 + int(size)
			nextOffset = logEntry.Offset + 1
		}

		pos += 4 + int(size)
	}

	glog.V(4).Infof("[parseMessages] Parsed %d messages, nextOffset=%d, totalBytes=%d",
		len(messages), nextOffset, totalBytes)

	return messages, nextOffset, totalBytes, nil
}

// readMessagesFromDisk reads messages from disk using the ReadFromDiskFn
func (logBuffer *LogBuffer) readMessagesFromDisk(startOffset int64, maxMessages int, maxBytes int, highWaterMark int64) (
	messages []*filer_pb.LogEntry,
	nextOffset int64,
	highWaterMark2 int64,
	endOfPartition bool,
	err error,
) {
	if logBuffer.ReadFromDiskFn == nil {
		return nil, startOffset, highWaterMark, true,
			fmt.Errorf("no disk read function configured")
	}

	messages = make([]*filer_pb.LogEntry, 0, maxMessages)
	nextOffset = startOffset
	totalBytes := 0

	// Use a simple callback to collect messages
	collectFn := func(logEntry *filer_pb.LogEntry) (bool, error) {
		// Check limits
		if len(messages) >= maxMessages {
			return true, nil // Done
		}

		entrySize := 4 + len(logEntry.Data) + len(logEntry.Key)
		if totalBytes+entrySize > maxBytes {
			return true, nil // Done
		}

		// Only include messages at or after startOffset
		if logEntry.Offset >= startOffset {
			messages = append(messages, logEntry)
			totalBytes += entrySize
			nextOffset = logEntry.Offset + 1
		}

		return false, nil // Continue
	}

	// Read from disk
	startPos := NewMessagePositionFromOffset(startOffset)
	_, isDone, err := logBuffer.ReadFromDiskFn(startPos, 0, collectFn)

	if err != nil {
		glog.Warningf("[StatelessRead] Disk read error: %v", err)
		return nil, startOffset, highWaterMark, false, err
	}

	glog.V(4).Infof("[StatelessRead] Read %d messages from disk, nextOffset=%d, isDone=%v",
		len(messages), nextOffset, isDone)

	// If we read from disk and got no messages, and isDone is true, we're at the end
	endOfPartition = isDone && len(messages) == 0

	return messages, nextOffset, highWaterMark, endOfPartition, nil
}

// GetHighWaterMark returns the highest offset available in this partition
// This is a lightweight operation for clients to check partition state
func (logBuffer *LogBuffer) GetHighWaterMark() int64 {
	logBuffer.RLock()
	defer logBuffer.RUnlock()
	return logBuffer.offset
}

// GetLogStartOffset returns the earliest offset available (either in memory or on disk)
// This is useful for clients to know the valid offset range
func (logBuffer *LogBuffer) GetLogStartOffset() int64 {
	logBuffer.RLock()
	defer logBuffer.RUnlock()

	// Check if we have offset information
	if !logBuffer.hasOffsets {
		return 0
	}

	// Return the current buffer start offset - this is the earliest offset in memory RIGHT NOW
	// For stateless fetch, we only return what's currently available in memory
	// We don't check prevBuffers because they may be stale or getting flushed
	return logBuffer.bufferStartOffset
}

// WaitForDataWithTimeout waits up to maxWaitMs for data to be available at startOffset
// Returns true if data became available, false if timeout
// This allows "long poll" behavior for real-time consumers
func (logBuffer *LogBuffer) WaitForDataWithTimeout(startOffset int64, maxWaitMs int) bool {
	if maxWaitMs <= 0 {
		return false
	}

	timeout := time.NewTimer(time.Duration(maxWaitMs) * time.Millisecond)
	defer timeout.Stop()

	// Register for notifications
	notifyChan := logBuffer.RegisterSubscriber(fmt.Sprintf("fetch-%d", startOffset))
	defer logBuffer.UnregisterSubscriber(fmt.Sprintf("fetch-%d", startOffset))

	// Check if data is already available
	logBuffer.RLock()
	currentEnd := logBuffer.offset
	logBuffer.RUnlock()

	if currentEnd >= startOffset {
		return true
	}

	// Wait for notification or timeout
	select {
	case <-notifyChan:
		// Data might be available now
		logBuffer.RLock()
		currentEnd := logBuffer.offset
		logBuffer.RUnlock()
		return currentEnd >= startOffset
	case <-timeout.C:
		return false
	}
}
