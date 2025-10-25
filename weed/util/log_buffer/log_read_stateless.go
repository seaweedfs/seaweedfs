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
				// Empty previous buffer means data was flushed to disk - fall through to disk read
				glog.V(2).Infof("[StatelessRead] Data at offset %d was flushed, attempting disk read", startOffset)
				break
			}
		}
		logBuffer.RUnlock()

		// Data not in memory - attempt disk read if configured
		// Don't return error here - data may be on disk!
		// Fall through to disk read logic below
		glog.V(2).Infof("[StatelessRead] Data at offset %d not in memory (buffer: %d-%d), attempting disk read",
			startOffset, bufferStartOffset, currentBufferEnd)
		// Don't return error - continue to disk read check below
	} else {
		// Offset is not in current buffer - check previous buffers FIRST before going to disk
		// This handles the case where data was just flushed but is still in prevBuffers

		for _, prevBuf := range logBuffer.prevBuffers.buffers {
			if startOffset >= prevBuf.startOffset && startOffset <= prevBuf.offset {
				if prevBuf.size > 0 {
					// Found in previous buffer!
					bufCopy := make([]byte, prevBuf.size)
					copy(bufCopy, prevBuf.buf[:prevBuf.size])
					logBuffer.RUnlock()

					messages, nextOffset, _, err = parseMessagesFromBuffer(
						bufCopy, startOffset, maxMessages, maxBytes)

					if err != nil {
						return nil, startOffset, highWaterMark, false, err
					}

					endOfPartition = false // More data might exist
					return messages, nextOffset, highWaterMark, endOfPartition, nil
				}
				// Empty previous buffer - data was flushed to disk
				glog.V(2).Infof("[StatelessRead] Found empty previous buffer for offset %d, will try disk", startOffset)
				break
			}
		}
		logBuffer.RUnlock()
	}

	// If we get here, unlock if not already unlocked
	// (Note: logBuffer.RUnlock() was called above in all paths)

	// Data not in memory - try disk read
	// This handles two cases:
	// 1. startOffset < bufferStartOffset: Historical data
	// 2. startOffset in buffer range but not in memory: Data was flushed (from fall-through above)
	if startOffset < currentBufferEnd {
		// Historical data or flushed data - try to read from disk if ReadFromDiskFn is configured
		if startOffset < bufferStartOffset {
			glog.Errorf("[StatelessRead] CASE 1: Historical data - offset %d < bufferStart %d",
				startOffset, bufferStartOffset)
		} else {
			glog.Errorf("[StatelessRead] CASE 2: Flushed data - offset %d in range [%d, %d) but not in memory",
				startOffset, bufferStartOffset, currentBufferEnd)
		}

		// Check if disk read function is configured
		if logBuffer.ReadFromDiskFn == nil {
			glog.Errorf("[StatelessRead] CRITICAL: ReadFromDiskFn is NIL! Cannot read from disk.")
			if startOffset < bufferStartOffset {
				return messages, startOffset, highWaterMark, false, fmt.Errorf("offset %d too old (earliest in-memory: %d), and ReadFromDiskFn is nil",
					startOffset, bufferStartOffset)
			}
			return messages, startOffset, highWaterMark, false, fmt.Errorf("offset %d not in memory (buffer: %d-%d), and ReadFromDiskFn is nil",
				startOffset, bufferStartOffset, currentBufferEnd)
		}

		// Read from disk (this is async/non-blocking if the ReadFromDiskFn is properly implemented)
		// The ReadFromDiskFn should handle its own timeouts and not block indefinitely
		diskMessages, diskNextOffset, diskErr := readHistoricalDataFromDisk(
			logBuffer, startOffset, maxMessages, maxBytes, highWaterMark)

		if diskErr != nil {
			glog.Errorf("[StatelessRead] CRITICAL: Disk read FAILED for offset %d: %v", startOffset, diskErr)
			// IMPORTANT: Return retryable error instead of silently returning empty!
			return messages, startOffset, highWaterMark, false, fmt.Errorf("disk read failed for offset %d: %v", startOffset, diskErr)
		}

		if len(diskMessages) == 0 {
			glog.Errorf("[StatelessRead] WARNING: Disk read returned 0 messages for offset %d (HWM=%d, bufferStart=%d)",
				startOffset, highWaterMark, bufferStartOffset)
		}

		// Return disk data
		endOfPartition = diskNextOffset >= bufferStartOffset && len(diskMessages) < maxMessages
		return diskMessages, diskNextOffset, highWaterMark, endOfPartition, nil
	}

	// startOffset >= currentBufferEnd - future offset, no data available yet
	glog.V(4).Infof("[StatelessRead] Future offset %d >= buffer end %d, no data available",
		startOffset, currentBufferEnd)
	return messages, startOffset, highWaterMark, true, nil
}

// readHistoricalDataFromDisk reads messages from disk for historical offsets
// This is called when the requested offset is older than what's in memory
// Uses an in-memory cache to avoid repeated disk I/O for the same chunks
func readHistoricalDataFromDisk(
	logBuffer *LogBuffer,
	startOffset int64,
	maxMessages int,
	maxBytes int,
	highWaterMark int64,
) (messages []*filer_pb.LogEntry, nextOffset int64, err error) {
	const chunkSize = 1000 // Size of each cached chunk

	// Calculate chunk start offset (aligned to chunkSize boundary)
	chunkStartOffset := (startOffset / chunkSize) * chunkSize

	// Try to get from cache first
	cachedMessages, cacheHit := getCachedDiskChunk(logBuffer, chunkStartOffset)

	if cacheHit {
		// Found in cache - extract requested messages
		result, nextOff, err := extractMessagesFromCache(cachedMessages, startOffset, maxMessages, maxBytes)

		if err != nil {
			// CRITICAL: Cache extraction failed because requested offset is BEYOND cached chunk
			// This means disk files only contain partial data (e.g., 1000-1763) and the
			// requested offset (e.g., 1764) is in a gap between disk and memory.
			//
			// SOLUTION: Return empty result with NO ERROR to let ReadMessagesAtOffset
			// continue to check memory buffers. The data might be in memory even though
			// it's not on disk.
			glog.Errorf("[DiskCache] Offset %d is beyond cached chunk (start=%d, size=%d)",
				startOffset, chunkStartOffset, len(cachedMessages))

			// Return empty but NO ERROR - this signals "not on disk, try memory"
			return nil, startOffset, nil
		}

		// Success - return cached data
		return result, nextOff, nil
	}

	// Not in cache - read entire chunk from disk for caching
	chunkMessages := make([]*filer_pb.LogEntry, 0, chunkSize)
	chunkNextOffset := chunkStartOffset

	// Create a position for the chunk start
	chunkPosition := MessagePosition{
		IsOffsetBased: true,
		Offset:        chunkStartOffset,
	}

	// Define callback to collect the entire chunk
	eachMessageFn := func(logEntry *filer_pb.LogEntry) (isDone bool, err error) {
		// Read up to chunkSize messages for caching
		if len(chunkMessages) >= chunkSize {
			return true, nil
		}

		chunkMessages = append(chunkMessages, logEntry)
		chunkNextOffset++

		// Continue reading the chunk
		return false, nil
	}

	// Read chunk from disk
	_, _, readErr := logBuffer.ReadFromDiskFn(chunkPosition, 0, eachMessageFn)

	if readErr != nil {
		glog.Errorf("[DiskRead] CRITICAL: ReadFromDiskFn returned ERROR: %v", readErr)
		return nil, startOffset, fmt.Errorf("failed to read from disk: %w", readErr)
	}

	// Cache the chunk for future reads
	if len(chunkMessages) > 0 {
		cacheDiskChunk(logBuffer, chunkStartOffset, chunkNextOffset-1, chunkMessages)
	} else {
		glog.Errorf("[DiskRead] WARNING: ReadFromDiskFn returned 0 messages for chunkStart=%d", chunkStartOffset)
	}

	// Extract requested messages from the chunk
	result, resNextOffset, resErr := extractMessagesFromCache(chunkMessages, startOffset, maxMessages, maxBytes)
	return result, resNextOffset, resErr
}

// getCachedDiskChunk retrieves a cached disk chunk if available
func getCachedDiskChunk(logBuffer *LogBuffer, chunkStartOffset int64) ([]*filer_pb.LogEntry, bool) {
	logBuffer.diskChunkCache.mu.RLock()
	defer logBuffer.diskChunkCache.mu.RUnlock()

	if chunk, exists := logBuffer.diskChunkCache.chunks[chunkStartOffset]; exists {
		// Update last access time
		chunk.lastAccess = time.Now()
		return chunk.messages, true
	}

	return nil, false
}

// invalidateCachedDiskChunk removes a chunk from the cache
// This is called when cached data is found to be incomplete or incorrect
func invalidateCachedDiskChunk(logBuffer *LogBuffer, chunkStartOffset int64) {
	logBuffer.diskChunkCache.mu.Lock()
	defer logBuffer.diskChunkCache.mu.Unlock()

	if _, exists := logBuffer.diskChunkCache.chunks[chunkStartOffset]; exists {
		delete(logBuffer.diskChunkCache.chunks, chunkStartOffset)
	}
}

// cacheDiskChunk stores a disk chunk in the cache with LRU eviction
func cacheDiskChunk(logBuffer *LogBuffer, startOffset, endOffset int64, messages []*filer_pb.LogEntry) {
	logBuffer.diskChunkCache.mu.Lock()
	defer logBuffer.diskChunkCache.mu.Unlock()

	// Check if we need to evict old chunks (LRU policy)
	if len(logBuffer.diskChunkCache.chunks) >= logBuffer.diskChunkCache.maxChunks {
		// Find least recently used chunk
		var oldestOffset int64
		var oldestTime time.Time
		first := true

		for offset, chunk := range logBuffer.diskChunkCache.chunks {
			if first || chunk.lastAccess.Before(oldestTime) {
				oldestOffset = offset
				oldestTime = chunk.lastAccess
				first = false
			}
		}

		// Evict oldest chunk
		delete(logBuffer.diskChunkCache.chunks, oldestOffset)
		glog.V(4).Infof("[DiskCache] Evicted chunk at offset %d (LRU)", oldestOffset)
	}

	// Store new chunk
	logBuffer.diskChunkCache.chunks[startOffset] = &CachedDiskChunk{
		startOffset: startOffset,
		endOffset:   endOffset,
		messages:    messages,
		lastAccess:  time.Now(),
	}
}

// extractMessagesFromCache extracts requested messages from a cached chunk
// chunkMessages contains messages starting from the chunk's aligned start offset
// We need to skip to the requested startOffset within the chunk
func extractMessagesFromCache(chunkMessages []*filer_pb.LogEntry, startOffset int64, maxMessages, maxBytes int) ([]*filer_pb.LogEntry, int64, error) {
	const chunkSize = 1000
	chunkStartOffset := (startOffset / chunkSize) * chunkSize

	// Calculate position within chunk
	positionInChunk := int(startOffset - chunkStartOffset)

	// Check if requested offset is within the chunk
	if positionInChunk < 0 {
		glog.Errorf("[DiskCache] CRITICAL: Requested offset %d is BEFORE chunk start %d (positionInChunk=%d < 0)",
			startOffset, chunkStartOffset, positionInChunk)
		return nil, startOffset, fmt.Errorf("offset %d before chunk start %d", startOffset, chunkStartOffset)
	}

	if positionInChunk >= len(chunkMessages) {
		// Requested offset is beyond the cached chunk
		// This happens when disk files only contain partial data
		// The requested offset might be in the gap between disk and memory

		// Return empty (data not on disk) - caller will check memory buffers
		return nil, startOffset, nil
	}

	// Extract messages starting from the requested position
	messages := make([]*filer_pb.LogEntry, 0, maxMessages)
	nextOffset := startOffset
	totalBytes := 0

	for i := positionInChunk; i < len(chunkMessages) && len(messages) < maxMessages; i++ {
		entry := chunkMessages[i]
		entrySize := proto.Size(entry)

		// Check byte limit
		if totalBytes > 0 && totalBytes+entrySize > maxBytes {
			break
		}

		messages = append(messages, entry)
		totalBytes += entrySize
		nextOffset++
	}

	glog.V(4).Infof("[DiskCache] Extracted %d messages from cache (offset %d-%d, bytes=%d)",
		len(messages), startOffset, nextOffset-1, totalBytes)

	return messages, nextOffset, nil
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

	messagesInBuffer := 0
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

		messagesInBuffer++

		// Initialize foundStart from first message
		if !foundStart {
			// Find the first message at or after startOffset
			if logEntry.Offset >= startOffset {
				foundStart = true
				nextOffset = logEntry.Offset
			} else {
				// Skip messages before startOffset
				glog.V(3).Infof("[parseMessages] Skipping message at offset %d (before startOffset %d)", logEntry.Offset, startOffset)
				pos += 4 + int(size)
				continue
			}
		}

		// Check if this message matches expected offset
		if foundStart && logEntry.Offset >= startOffset {
			glog.V(3).Infof("[parseMessages] Adding message at offset %d (count=%d)", logEntry.Offset, len(messages)+1)
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
