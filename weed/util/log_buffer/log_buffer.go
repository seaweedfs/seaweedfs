package log_buffer

import (
	"bytes"
	"fmt"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/mq_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

const BufferSize = 8 * 1024 * 1024
const PreviousBufferCount = 32

type dataToFlush struct {
	startTime time.Time
	stopTime  time.Time
	data      *bytes.Buffer
	minOffset int64
	maxOffset int64
	done      chan struct{} // Signal when flush completes
}

type EachLogEntryFuncType func(logEntry *filer_pb.LogEntry) (isDone bool, err error)
type EachLogEntryWithOffsetFuncType func(logEntry *filer_pb.LogEntry, offset int64) (isDone bool, err error)
type LogFlushFuncType func(logBuffer *LogBuffer, startTime, stopTime time.Time, buf []byte, minOffset, maxOffset int64)
type LogReadFromDiskFuncType func(startPosition MessagePosition, stopTsNs int64, eachLogEntryFn EachLogEntryFuncType) (lastReadPosition MessagePosition, isDone bool, err error)

// DiskChunkCache caches chunks of historical data read from disk
type DiskChunkCache struct {
	mu        sync.RWMutex
	chunks    map[int64]*CachedDiskChunk // Key: chunk start offset (aligned to chunkSize)
	maxChunks int                        // Maximum number of chunks to cache
}

// CachedDiskChunk represents a cached chunk of disk data
type CachedDiskChunk struct {
	startOffset int64
	endOffset   int64
	messages    []*filer_pb.LogEntry
	lastAccess  time.Time
}

type LogBuffer struct {
	LastFlushTsNs     int64
	name              string
	prevBuffers       *SealedBuffers
	buf               []byte
	offset            int64 // Last offset in current buffer (endOffset)
	bufferStartOffset int64 // First offset in current buffer
	idx               []int
	pos               int
	startTime         time.Time
	stopTime          time.Time
	lastFlushDataTime time.Time
	sizeBuf           []byte
	flushInterval     time.Duration
	flushFn           LogFlushFuncType
	ReadFromDiskFn    LogReadFromDiskFuncType
	notifyFn          func()
	// Per-subscriber notification channels for instant wake-up
	subscribersMu sync.RWMutex
	subscribers   map[string]chan struct{} // subscriberID -> notification channel
	isStopping    *atomic.Bool
	isAllFlushed  bool
	flushChan     chan *dataToFlush
	LastTsNs      atomic.Int64
	// Offset range tracking for Kafka integration
	minOffset         int64
	maxOffset         int64
	hasOffsets        bool
	lastFlushedOffset atomic.Int64 // Highest offset that has been flushed to disk (-1 = nothing flushed yet)
	lastFlushTsNs     atomic.Int64 // Latest timestamp that has been flushed to disk (0 = nothing flushed yet)
	// Disk chunk cache for historical data reads
	diskChunkCache *DiskChunkCache
	sync.RWMutex
}

func NewLogBuffer(name string, flushInterval time.Duration, flushFn LogFlushFuncType,
	readFromDiskFn LogReadFromDiskFuncType, notifyFn func()) *LogBuffer {
	lb := &LogBuffer{
		name:           name,
		prevBuffers:    newSealedBuffers(PreviousBufferCount),
		buf:            make([]byte, BufferSize),
		sizeBuf:        make([]byte, 4),
		flushInterval:  flushInterval,
		flushFn:        flushFn,
		ReadFromDiskFn: readFromDiskFn,
		notifyFn:       notifyFn,
		subscribers:    make(map[string]chan struct{}),
		flushChan:      make(chan *dataToFlush, 256),
		isStopping:     new(atomic.Bool),
		offset:         0, // Will be initialized from existing data if available
		diskChunkCache: &DiskChunkCache{
			chunks:    make(map[int64]*CachedDiskChunk),
			maxChunks: 16, // Cache up to 16 chunks (configurable)
		},
	}
	lb.lastFlushedOffset.Store(-1) // Nothing flushed to disk yet
	go lb.loopFlush()
	go lb.loopInterval()
	return lb
}

// RegisterSubscriber registers a subscriber for instant notifications when data is written
// Returns a channel that will receive notifications (<1ms latency)
func (logBuffer *LogBuffer) RegisterSubscriber(subscriberID string) chan struct{} {
	logBuffer.subscribersMu.Lock()
	defer logBuffer.subscribersMu.Unlock()

	// Check if already registered
	if existingChan, exists := logBuffer.subscribers[subscriberID]; exists {
		return existingChan
	}

	// Create buffered channel (size 1) so notifications never block
	notifyChan := make(chan struct{}, 1)
	logBuffer.subscribers[subscriberID] = notifyChan
	return notifyChan
}

// UnregisterSubscriber removes a subscriber and closes its notification channel
func (logBuffer *LogBuffer) UnregisterSubscriber(subscriberID string) {
	logBuffer.subscribersMu.Lock()
	defer logBuffer.subscribersMu.Unlock()

	if ch, exists := logBuffer.subscribers[subscriberID]; exists {
		close(ch)
		delete(logBuffer.subscribers, subscriberID)
	}
}

// IsOffsetInMemory checks if the given offset is available in the in-memory buffer
// Returns true if:
// 1. Offset is newer than what's been flushed to disk (must be in memory)
// 2. Offset is in current buffer or previous buffers (may be flushed but still in memory)
// Returns false if offset is older than memory buffers (only on disk)
func (logBuffer *LogBuffer) IsOffsetInMemory(offset int64) bool {
	logBuffer.RLock()
	defer logBuffer.RUnlock()

	// Check if we're tracking offsets at all
	if !logBuffer.hasOffsets {
		return false // No offsets tracked yet
	}

	// OPTIMIZATION: If offset is newer than what's been flushed to disk,
	// it MUST be in memory (not written to disk yet)
	lastFlushed := logBuffer.lastFlushedOffset.Load()
	if lastFlushed >= 0 && offset > lastFlushed {
		return true
	}

	// Check if offset is in current buffer range AND buffer has data
	// (data can be both on disk AND in memory during flush window)
	if offset >= logBuffer.bufferStartOffset && offset <= logBuffer.offset {
		// CRITICAL: Check if buffer actually has data (pos > 0)
		// After flush, pos=0 but range is still valid - data is on disk, not in memory
		if logBuffer.pos > 0 {
			return true
		}
		// Buffer is empty (just flushed) - data is on disk
		return false
	}

	// Check if offset is in previous buffers AND they have data
	for _, buf := range logBuffer.prevBuffers.buffers {
		if offset >= buf.startOffset && offset <= buf.offset {
			// Check if prevBuffer actually has data
			if buf.size > 0 {
				return true
			}
			// Buffer is empty (flushed) - data is on disk
			return false
		}
	}

	// Offset is older than memory buffers - only available on disk
	return false
}

// notifySubscribers sends notifications to all registered subscribers
// Non-blocking: uses select with default to avoid blocking on full channels
func (logBuffer *LogBuffer) notifySubscribers() {
	logBuffer.subscribersMu.RLock()
	defer logBuffer.subscribersMu.RUnlock()

	if len(logBuffer.subscribers) == 0 {
		return // No subscribers, skip notification
	}

	for subscriberID, notifyChan := range logBuffer.subscribers {
		select {
		case notifyChan <- struct{}{}:
			// Notification sent successfully
		default:
			// Channel full - subscriber hasn't consumed previous notification yet
			// This is OK because one notification is sufficient to wake the subscriber
		}
		_ = subscriberID
	}
}

// InitializeOffsetFromExistingData initializes the offset counter from existing data on disk
// This should be called after LogBuffer creation to ensure offset continuity on restart
func (logBuffer *LogBuffer) InitializeOffsetFromExistingData(getHighestOffsetFn func() (int64, error)) error {
	if getHighestOffsetFn == nil {
		return nil // No initialization function provided
	}

	highestOffset, err := getHighestOffsetFn()
	if err != nil {
		glog.V(0).Infof("Failed to get highest offset for %s: %v, starting from 0", logBuffer.name, err)
		return nil // Continue with offset 0 if we can't read existing data
	}

	if highestOffset >= 0 {
		// Set the next offset to be one after the highest existing offset
		nextOffset := highestOffset + 1
		logBuffer.offset = nextOffset
		// bufferStartOffset should match offset after initialization
		// This ensures that reads for old offsets (0...highestOffset) will trigger disk reads
		// New data written after this will start at nextOffset
		logBuffer.bufferStartOffset = nextOffset
		// CRITICAL: Track that data [0...highestOffset] is on disk
		logBuffer.lastFlushedOffset.Store(highestOffset)
		// Set lastFlushedTime to current time (we know data up to highestOffset is on disk)
		logBuffer.lastFlushTsNs.Store(time.Now().UnixNano())
		glog.V(0).Infof("Initialized LogBuffer %s offset to %d (highest existing: %d), buffer starts at %d, lastFlushedOffset=%d, lastFlushedTime=%v",
			logBuffer.name, nextOffset, highestOffset, nextOffset, highestOffset, time.Now())
	} else {
		logBuffer.bufferStartOffset = 0 // Start from offset 0
		// No data on disk yet
		glog.V(0).Infof("No existing data found for %s, starting from offset 0, lastFlushedOffset=-1, lastFlushedTime=0", logBuffer.name)
	}

	return nil
}

func (logBuffer *LogBuffer) AddToBuffer(message *mq_pb.DataMessage) {
	logBuffer.AddDataToBuffer(message.Key, message.Value, message.TsNs)
}

// AddLogEntryToBuffer directly adds a LogEntry to the buffer, preserving offset information
func (logBuffer *LogBuffer) AddLogEntryToBuffer(logEntry *filer_pb.LogEntry) {
	logEntryData, _ := proto.Marshal(logEntry)

	var toFlush *dataToFlush
	logBuffer.Lock()
	defer func() {
		logBuffer.Unlock()
		if toFlush != nil {
			logBuffer.flushChan <- toFlush
		}
		if logBuffer.notifyFn != nil {
			logBuffer.notifyFn()
		}
		// Notify all registered subscribers instantly (<1ms latency)
		logBuffer.notifySubscribers()
	}()

	processingTsNs := logEntry.TsNs
	ts := time.Unix(0, processingTsNs)

	// Handle timestamp collision inside lock (rare case)
	if logBuffer.LastTsNs.Load() >= processingTsNs {
		processingTsNs = logBuffer.LastTsNs.Add(1)
		ts = time.Unix(0, processingTsNs)
		// Re-marshal with corrected timestamp
		logEntry.TsNs = processingTsNs
		logEntryData, _ = proto.Marshal(logEntry)
	} else {
		logBuffer.LastTsNs.Store(processingTsNs)
	}

	size := len(logEntryData)

	if logBuffer.pos == 0 {
		logBuffer.startTime = ts
		// Reset offset tracking for new buffer
		logBuffer.hasOffsets = false
	}

	// Track offset ranges for Kafka integration
	// Use >= 0 to include offset 0 (first message in a topic)
	if logEntry.Offset >= 0 {
		if !logBuffer.hasOffsets {
			logBuffer.minOffset = logEntry.Offset
			logBuffer.maxOffset = logEntry.Offset
			logBuffer.hasOffsets = true
		} else {
			if logEntry.Offset < logBuffer.minOffset {
				logBuffer.minOffset = logEntry.Offset
			}
			if logEntry.Offset > logBuffer.maxOffset {
				logBuffer.maxOffset = logEntry.Offset
			}
		}
	}

	if logBuffer.startTime.Add(logBuffer.flushInterval).Before(ts) || len(logBuffer.buf)-logBuffer.pos < size+4 {
		toFlush = logBuffer.copyToFlush()
		logBuffer.startTime = ts
		if len(logBuffer.buf) < size+4 {
			// Validate size to prevent integer overflow in computation BEFORE allocation
			const maxBufferSize = 1 << 30 // 1 GiB practical limit
			// Ensure 2*size + 4 won't overflow int and stays within practical bounds
			if size < 0 || size > (math.MaxInt-4)/2 || size > (maxBufferSize-4)/2 {
				glog.Errorf("Buffer size out of valid range: %d bytes, skipping", size)
				return
			}
			// Safe to compute now that we've validated size is in valid range
			newSize := 2*size + 4
			logBuffer.buf = make([]byte, newSize)
		}
	}
	logBuffer.stopTime = ts

	logBuffer.idx = append(logBuffer.idx, logBuffer.pos)
	util.Uint32toBytes(logBuffer.sizeBuf, uint32(size))
	copy(logBuffer.buf[logBuffer.pos:logBuffer.pos+4], logBuffer.sizeBuf)
	copy(logBuffer.buf[logBuffer.pos+4:logBuffer.pos+4+size], logEntryData)
	logBuffer.pos += size + 4

	logBuffer.offset++
}

func (logBuffer *LogBuffer) AddDataToBuffer(partitionKey, data []byte, processingTsNs int64) {

	// PERFORMANCE OPTIMIZATION: Pre-process expensive operations OUTSIDE the lock
	var ts time.Time
	if processingTsNs == 0 {
		ts = time.Now()
		processingTsNs = ts.UnixNano()
	} else {
		ts = time.Unix(0, processingTsNs)
	}

	logEntry := &filer_pb.LogEntry{
		TsNs:             processingTsNs, // Will be updated if needed
		PartitionKeyHash: util.HashToInt32(partitionKey),
		Data:             data,
		Key:              partitionKey,
	}

	logEntryData, _ := proto.Marshal(logEntry)

	var toFlush *dataToFlush
	logBuffer.Lock()
	defer func() {
		logBuffer.Unlock()
		if toFlush != nil {
			logBuffer.flushChan <- toFlush
		}
		if logBuffer.notifyFn != nil {
			logBuffer.notifyFn()
		}
		// Notify all registered subscribers instantly (<1ms latency)
		logBuffer.notifySubscribers()
	}()

	// Handle timestamp collision inside lock (rare case)
	if logBuffer.LastTsNs.Load() >= processingTsNs {
		processingTsNs = logBuffer.LastTsNs.Add(1)
		ts = time.Unix(0, processingTsNs)
		logEntry.TsNs = processingTsNs
	} else {
		logBuffer.LastTsNs.Store(processingTsNs)
	}

	// Set the offset in the LogEntry before marshaling
	// This ensures the flushed data contains the correct offset information
	// Note: This also enables AddToBuffer to work correctly with Kafka-style offset-based reads
	logEntry.Offset = logBuffer.offset

	// DEBUG: Log data being added to buffer for GitHub Actions debugging
	dataPreview := ""
	if len(data) > 0 {
		if len(data) <= 50 {
			dataPreview = string(data)
		} else {
			dataPreview = fmt.Sprintf("%s...(total %d bytes)", string(data[:50]), len(data))
		}
	}
	glog.V(2).Infof("[LOG_BUFFER_ADD] buffer=%s offset=%d dataLen=%d dataPreview=%q",
		logBuffer.name, logBuffer.offset, len(data), dataPreview)

	// Marshal with correct timestamp and offset
	logEntryData, _ = proto.Marshal(logEntry)

	size := len(logEntryData)

	if logBuffer.pos == 0 {
		logBuffer.startTime = ts
		// Reset offset tracking for new buffer
		logBuffer.hasOffsets = false
	}

	// Track offset ranges for Kafka integration
	// Track the current offset being written
	if !logBuffer.hasOffsets {
		logBuffer.minOffset = logBuffer.offset
		logBuffer.maxOffset = logBuffer.offset
		logBuffer.hasOffsets = true
	} else {
		if logBuffer.offset < logBuffer.minOffset {
			logBuffer.minOffset = logBuffer.offset
		}
		if logBuffer.offset > logBuffer.maxOffset {
			logBuffer.maxOffset = logBuffer.offset
		}
	}

	if logBuffer.startTime.Add(logBuffer.flushInterval).Before(ts) || len(logBuffer.buf)-logBuffer.pos < size+4 {
		// glog.V(0).Infof("%s copyToFlush1 offset:%d count:%d start time %v, ts %v, remaining %d bytes", logBuffer.name, logBuffer.offset, len(logBuffer.idx), logBuffer.startTime, ts, len(logBuffer.buf)-logBuffer.pos)
		toFlush = logBuffer.copyToFlush()
		logBuffer.startTime = ts
		if len(logBuffer.buf) < size+4 {
			// Validate size to prevent integer overflow in computation BEFORE allocation
			const maxBufferSize = 1 << 30 // 1 GiB practical limit
			// Ensure 2*size + 4 won't overflow int and stays within practical bounds
			if size < 0 || size > (math.MaxInt-4)/2 || size > (maxBufferSize-4)/2 {
				glog.Errorf("Buffer size out of valid range: %d bytes, skipping", size)
				return
			}
			// Safe to compute now that we've validated size is in valid range
			newSize := 2*size + 4
			logBuffer.buf = make([]byte, newSize)
		}
	}
	logBuffer.stopTime = ts

	logBuffer.idx = append(logBuffer.idx, logBuffer.pos)
	util.Uint32toBytes(logBuffer.sizeBuf, uint32(size))
	copy(logBuffer.buf[logBuffer.pos:logBuffer.pos+4], logBuffer.sizeBuf)
	copy(logBuffer.buf[logBuffer.pos+4:logBuffer.pos+4+size], logEntryData)
	logBuffer.pos += size + 4

	logBuffer.offset++
}

func (logBuffer *LogBuffer) IsStopping() bool {
	return logBuffer.isStopping.Load()
}

// ForceFlush immediately flushes the current buffer content and WAITS for completion
// This is useful for critical topics that need immediate persistence
// CRITICAL: This function is now SYNCHRONOUS - it blocks until the flush completes
func (logBuffer *LogBuffer) ForceFlush() {
	if logBuffer.isStopping.Load() {
		return // Don't flush if we're shutting down
	}

	logBuffer.Lock()
	toFlush := logBuffer.copyToFlushWithCallback()
	logBuffer.Unlock()

	if toFlush != nil {
		// Send to flush channel (with reasonable timeout)
		select {
		case logBuffer.flushChan <- toFlush:
			// Successfully queued for flush - now WAIT for it to complete
			select {
			case <-toFlush.done:
				// Flush completed successfully
				glog.V(1).Infof("ForceFlush completed for %s", logBuffer.name)
			case <-time.After(5 * time.Second):
				// Timeout waiting for flush - this shouldn't happen
				glog.Warningf("ForceFlush timed out waiting for completion on %s", logBuffer.name)
			}
		case <-time.After(2 * time.Second):
			// If flush channel is still blocked after 2s, something is wrong
			glog.Warningf("ForceFlush channel timeout for %s - flush channel busy for 2s", logBuffer.name)
		}
	}
}

// ShutdownLogBuffer flushes the buffer and stops the log buffer
func (logBuffer *LogBuffer) ShutdownLogBuffer() {
	isAlreadyStopped := logBuffer.isStopping.Swap(true)
	if isAlreadyStopped {
		return
	}
	toFlush := logBuffer.copyToFlush()
	logBuffer.flushChan <- toFlush
	close(logBuffer.flushChan)
}

// IsAllFlushed returns true if all data in the buffer has been flushed, after calling ShutdownLogBuffer().
func (logBuffer *LogBuffer) IsAllFlushed() bool {
	return logBuffer.isAllFlushed
}

func (logBuffer *LogBuffer) loopFlush() {
	for d := range logBuffer.flushChan {
		if d != nil {
			// glog.V(4).Infof("%s flush [%v, %v] size %d", m.name, d.startTime, d.stopTime, len(d.data.Bytes()))
			logBuffer.flushFn(logBuffer, d.startTime, d.stopTime, d.data.Bytes(), d.minOffset, d.maxOffset)
			d.releaseMemory()
			// local logbuffer is different from aggregate logbuffer here
			logBuffer.lastFlushDataTime = d.stopTime

			// CRITICAL: Track what's been flushed to disk for both offset-based and time-based reads
			// Use >= 0 to include offset 0 (first message in a topic)
			if d.maxOffset >= 0 {
				logBuffer.lastFlushedOffset.Store(d.maxOffset)
			}
			if !d.stopTime.IsZero() {
				logBuffer.lastFlushTsNs.Store(d.stopTime.UnixNano())
			}

			// Signal completion if there's a callback channel
			if d.done != nil {
				close(d.done)
			}
		}
	}
	logBuffer.isAllFlushed = true
}

func (logBuffer *LogBuffer) loopInterval() {
	for !logBuffer.IsStopping() {
		time.Sleep(logBuffer.flushInterval)
		if logBuffer.IsStopping() {
			return
		}

		logBuffer.Lock()
		toFlush := logBuffer.copyToFlush()
		logBuffer.Unlock()
		if toFlush != nil {
			glog.V(4).Infof("%s flush [%v, %v] size %d", logBuffer.name, toFlush.startTime, toFlush.stopTime, len(toFlush.data.Bytes()))
			logBuffer.flushChan <- toFlush
		} else {
			// glog.V(0).Infof("%s no flush", m.name)
		}
	}
}

func (logBuffer *LogBuffer) copyToFlush() *dataToFlush {
	return logBuffer.copyToFlushInternal(false)
}

func (logBuffer *LogBuffer) copyToFlushWithCallback() *dataToFlush {
	return logBuffer.copyToFlushInternal(true)
}

func (logBuffer *LogBuffer) copyToFlushInternal(withCallback bool) *dataToFlush {

	if logBuffer.pos > 0 {
		var d *dataToFlush
		if logBuffer.flushFn != nil {
			d = &dataToFlush{
				startTime: logBuffer.startTime,
				stopTime:  logBuffer.stopTime,
				data:      copiedBytes(logBuffer.buf[:logBuffer.pos]),
				minOffset: logBuffer.minOffset,
				maxOffset: logBuffer.maxOffset,
			}
			// Add callback channel for synchronous ForceFlush
			if withCallback {
				d.done = make(chan struct{})
			}
			// glog.V(4).Infof("%s flushing [0,%d) with %d entries [%v, %v]", m.name, m.pos, len(m.idx), m.startTime, m.stopTime)
		} else {
			// glog.V(4).Infof("%s removed from memory [0,%d) with %d entries [%v, %v]", m.name, m.pos, len(m.idx), m.startTime, m.stopTime)
			logBuffer.lastFlushDataTime = logBuffer.stopTime
		}
		// CRITICAL: logBuffer.offset is the "next offset to assign", so last offset in buffer is offset-1
		lastOffsetInBuffer := logBuffer.offset - 1
		logBuffer.buf = logBuffer.prevBuffers.SealBuffer(logBuffer.startTime, logBuffer.stopTime, logBuffer.buf, logBuffer.pos, logBuffer.bufferStartOffset, lastOffsetInBuffer)
		// Use zero time (time.Time{}) not epoch time (time.Unix(0,0))
		// Epoch time (1970) breaks time-based reads after flush
		logBuffer.startTime = time.Time{}
		logBuffer.stopTime = time.Time{}
		logBuffer.pos = 0
		logBuffer.idx = logBuffer.idx[:0]
		// DON'T increment offset - it's already pointing to the next offset!
		// logBuffer.offset++ // REMOVED - this was causing offset gaps!
		logBuffer.bufferStartOffset = logBuffer.offset // Next buffer starts at current offset (which is already the next one)
		// Reset offset tracking
		logBuffer.hasOffsets = false
		logBuffer.minOffset = 0
		logBuffer.maxOffset = 0

		// Invalidate disk cache chunks after flush
		// The cache may contain stale data from before this flush
		// Invalidating ensures consumers will re-read fresh data from disk after flush
		logBuffer.invalidateAllDiskCacheChunks()

		return d
	}
	return nil
}

// invalidateAllDiskCacheChunks clears all cached disk chunks
// This should be called after a buffer flush to ensure consumers read fresh data from disk
func (logBuffer *LogBuffer) invalidateAllDiskCacheChunks() {
	logBuffer.diskChunkCache.mu.Lock()
	defer logBuffer.diskChunkCache.mu.Unlock()

	if len(logBuffer.diskChunkCache.chunks) > 0 {
		logBuffer.diskChunkCache.chunks = make(map[int64]*CachedDiskChunk)
	}
}

func (logBuffer *LogBuffer) GetEarliestTime() time.Time {
	return logBuffer.startTime
}
func (logBuffer *LogBuffer) GetEarliestPosition() MessagePosition {
	return MessagePosition{
		Time:   logBuffer.startTime,
		Offset: logBuffer.offset,
	}
}

// GetLastFlushTsNs returns the latest flushed timestamp in Unix nanoseconds.
// Returns 0 if nothing has been flushed yet.
func (logBuffer *LogBuffer) GetLastFlushTsNs() int64 {
	return logBuffer.lastFlushTsNs.Load()
}

func (d *dataToFlush) releaseMemory() {
	d.data.Reset()
	bufferPool.Put(d.data)
}

func (logBuffer *LogBuffer) ReadFromBuffer(lastReadPosition MessagePosition) (bufferCopy *bytes.Buffer, batchIndex int64, err error) {
	logBuffer.RLock()
	defer logBuffer.RUnlock()

	isOffsetBased := lastReadPosition.IsOffsetBased
	glog.V(2).Infof("[ReadFromBuffer] %s: isOffsetBased=%v, position=%+v, bufferStartOffset=%d, offset=%d, pos=%d",
		logBuffer.name, isOffsetBased, lastReadPosition, logBuffer.bufferStartOffset, logBuffer.offset, logBuffer.pos)

	// For offset-based subscriptions, use offset comparisons, not time comparisons!
	if isOffsetBased {
		requestedOffset := lastReadPosition.Offset

		// Check if the requested offset is in the current buffer range
		if requestedOffset >= logBuffer.bufferStartOffset && requestedOffset <= logBuffer.offset {
			// If current buffer is empty (pos=0), check if data is on disk or not yet written
			if logBuffer.pos == 0 {
				// If buffer is empty but offset range covers the request,
				// it means data was in memory and has been flushed/moved out.
				// The bufferStartOffset advancing to cover this offset proves data existed.
				//
				// Three cases:
				// 1. requestedOffset < logBuffer.offset: Data was here, now flushed
				// 2. requestedOffset == logBuffer.offset && bufferStartOffset > 0: Buffer advanced, data flushed
				// 3. requestedOffset == logBuffer.offset && bufferStartOffset == 0: Initial state - try disk first!
				//
				// Cases 1 & 2: try disk read
				// Case 3: try disk read (historical data might exist)
				if requestedOffset < logBuffer.offset {
					// Data was in the buffer range but buffer is now empty = flushed to disk
					return nil, -2, ResumeFromDiskError
				}
				// requestedOffset == logBuffer.offset: Current position
				// CRITICAL: For subscribers starting from offset 0, try disk read first
				// (historical data might exist from previous runs)
				if requestedOffset == 0 && logBuffer.bufferStartOffset == 0 && logBuffer.offset == 0 {
					// Initial state: try disk read before waiting for new data
					return nil, -2, ResumeFromDiskError
				}
				// Otherwise, wait for new data to arrive
				return nil, logBuffer.offset, nil
			}
			return copiedBytes(logBuffer.buf[:logBuffer.pos]), logBuffer.offset, nil
		}

		// Check previous buffers for the requested offset
		for _, buf := range logBuffer.prevBuffers.buffers {
			if requestedOffset >= buf.startOffset && requestedOffset <= buf.offset {
				// If prevBuffer is empty, it means the data was flushed to disk
				// (prevBuffers are created when buffer is flushed)
				if buf.size == 0 {
					// Empty prevBuffer covering this offset means data was flushed
					return nil, -2, ResumeFromDiskError
				}
				return copiedBytes(buf.buf[:buf.size]), buf.offset, nil
			}
		}

		// Offset not found in any buffer
		if requestedOffset < logBuffer.bufferStartOffset {
			// Data not in current buffers - must be on disk (flushed or never existed)
			// Return ResumeFromDiskError to trigger disk read
			return nil, -2, ResumeFromDiskError
		}

		if requestedOffset > logBuffer.offset {
			// Future data, not available yet
			return nil, logBuffer.offset, nil
		}

		// Offset not found - return nil
		return nil, logBuffer.offset, nil
	}

	// TIMESTAMP-BASED READ (original logic)
	// Read from disk and memory
	//	1. read from disk, last time is = td
	//	2. in memory, the earliest time = tm
	//	if tm <= td, case 2.1
	//		read from memory
	//	if tm is empty, case 2.2
	//		read from memory
	//	if td < tm, case 2.3
	//		read from disk again
	var tsMemory time.Time
	if !logBuffer.startTime.IsZero() {
		tsMemory = logBuffer.startTime
	}
	glog.V(2).Infof("[ReadFromBuffer] %s: checking prevBuffers, count=%d, currentStartTime=%v",
		logBuffer.name, len(logBuffer.prevBuffers.buffers), logBuffer.startTime)
	for i, prevBuf := range logBuffer.prevBuffers.buffers {
		glog.V(2).Infof("[ReadFromBuffer] %s: prevBuf[%d]: startTime=%v stopTime=%v size=%d startOffset=%d endOffset=%d",
			logBuffer.name, i, prevBuf.startTime, prevBuf.stopTime, prevBuf.size, prevBuf.startOffset, prevBuf.offset)
		if !prevBuf.startTime.IsZero() {
			// If tsMemory is zero, assign directly; otherwise compare
			if tsMemory.IsZero() || prevBuf.startTime.Before(tsMemory) {
				tsMemory = prevBuf.startTime
			}
		}
	}
	if tsMemory.IsZero() { // case 2.2
		return nil, -2, nil
	} else if lastReadPosition.Time.Before(tsMemory) { // case 2.3
		// For time-based reads, only check timestamp for disk reads
		// Don't use offset comparisons as they're not meaningful for time-based subscriptions

		// Special case: If requested time is zero (Unix epoch), treat as "start from beginning"
		// This handles queries that want to read all data without knowing the exact start time
		if lastReadPosition.Time.IsZero() || lastReadPosition.Time.Unix() == 0 {
			// Start from the beginning of memory
			// Fall through to case 2.1 to read from earliest buffer
		} else if lastReadPosition.Offset <= 0 && lastReadPosition.Time.Before(tsMemory) {
			// Treat first read with sentinel/zero offset as inclusive of earliest in-memory data
			glog.V(4).Infof("first read (offset=%d) at time %v before earliest memory %v, reading from memory",
				lastReadPosition.Offset, lastReadPosition.Time, tsMemory)
		} else {
			// Data not in memory buffers - read from disk
			glog.V(0).Infof("[ReadFromBuffer] %s resume from disk: requested time %v < earliest memory time %v",
				logBuffer.name, lastReadPosition.Time, tsMemory)
			return nil, -2, ResumeFromDiskError
		}
	}

	glog.V(2).Infof("[ReadFromBuffer] %s: time-based read continuing, tsMemory=%v, lastReadPos=%v",
		logBuffer.name, tsMemory, lastReadPosition.Time)

	// the following is case 2.1

	if lastReadPosition.Time.Equal(logBuffer.stopTime) && !logBuffer.stopTime.IsZero() {
		// For first-read sentinel/zero offset, allow inclusive read at the boundary
		if lastReadPosition.Offset > 0 {
			return nil, logBuffer.offset, nil
		}
	}
	if lastReadPosition.Time.After(logBuffer.stopTime) && !logBuffer.stopTime.IsZero() {
		// glog.Fatalf("unexpected last read time %v, older than latest %v", lastReadPosition, m.stopTime)
		return nil, logBuffer.offset, nil
	}
	// Also check prevBuffers when current buffer is empty (startTime is zero)
	if lastReadPosition.Time.Before(logBuffer.startTime) || logBuffer.startTime.IsZero() {
		for _, buf := range logBuffer.prevBuffers.buffers {
			if buf.startTime.After(lastReadPosition.Time) {
				// glog.V(4).Infof("%s return the %d sealed buffer %v", m.name, i, buf.startTime)
				return copiedBytes(buf.buf[:buf.size]), buf.offset, nil
			}
			if !buf.startTime.After(lastReadPosition.Time) && buf.stopTime.After(lastReadPosition.Time) {
				searchTime := lastReadPosition.Time
				if lastReadPosition.Offset <= 0 {
					searchTime = searchTime.Add(-time.Nanosecond)
				}
				pos := buf.locateByTs(searchTime)
				glog.V(2).Infof("[ReadFromBuffer] %s: found data in prevBuffer at pos %d, bufSize=%d", logBuffer.name, pos, buf.size)
				return copiedBytes(buf.buf[pos:buf.size]), buf.offset, nil
			}
		}
		// If current buffer is not empty, return it
		if logBuffer.pos > 0 {
			// glog.V(4).Infof("%s return the current buf %v", m.name, lastReadPosition)
			return copiedBytes(logBuffer.buf[:logBuffer.pos]), logBuffer.offset, nil
		}
		// Buffer is empty and no data in prevBuffers - wait for new data
		return nil, logBuffer.offset, nil
	}

	lastTs := lastReadPosition.Time.UnixNano()
	// Inclusive boundary for first-read sentinel/zero offset
	searchTs := lastTs
	if lastReadPosition.Offset <= 0 {
		if searchTs > math.MinInt64+1 { // prevent underflow
			searchTs = searchTs - 1
		}
	}
	l, h := 0, len(logBuffer.idx)-1

	/*
		for i, pos := range m.idx {
			logEntry, ts := readTs(m.buf, pos)
			event := &filer_pb.SubscribeMetadataResponse{}
			proto.Unmarshal(logEntry.Data, event)
			entry := event.EventNotification.OldEntry
			if entry == nil {
				entry = event.EventNotification.NewEntry
			}
		}
	*/

	for l <= h {
		mid := (l + h) / 2
		pos := logBuffer.idx[mid]
		_, t := readTs(logBuffer.buf, pos)
		if t <= searchTs {
			l = mid + 1
		} else if searchTs < t {
			var prevT int64
			if mid > 0 {
				_, prevT = readTs(logBuffer.buf, logBuffer.idx[mid-1])
			}
			if prevT <= searchTs {
				return copiedBytes(logBuffer.buf[pos:logBuffer.pos]), logBuffer.offset, nil
			}
			h = mid
		}
	}

	// Binary search didn't find the timestamp - data may have been flushed to disk already
	// Returning -2 signals to caller that data is not available in memory
	return nil, -2, nil

}
func (logBuffer *LogBuffer) ReleaseMemory(b *bytes.Buffer) {
	bufferPool.Put(b)
}

// GetName returns the log buffer name for metadata tracking
func (logBuffer *LogBuffer) GetName() string {
	logBuffer.RLock()
	defer logBuffer.RUnlock()
	return logBuffer.name
}

// GetOffset returns the current offset for metadata tracking
func (logBuffer *LogBuffer) GetOffset() int64 {
	logBuffer.RLock()
	defer logBuffer.RUnlock()
	return logBuffer.offset
}

var bufferPool = sync.Pool{
	New: func() interface{} {
		return new(bytes.Buffer)
	},
}

func copiedBytes(buf []byte) (copied *bytes.Buffer) {
	copied = bufferPool.Get().(*bytes.Buffer)
	copied.Reset()
	copied.Write(buf)
	return
}

func readTs(buf []byte, pos int) (size int, ts int64) {

	size = int(util.BytesToUint32(buf[pos : pos+4]))
	entryData := buf[pos+4 : pos+4+size]
	logEntry := &filer_pb.LogEntry{}

	err := proto.Unmarshal(entryData, logEntry)
	if err != nil {
		glog.Fatalf("unexpected unmarshal filer_pb.LogEntry: %v", err)
	}
	return size, logEntry.TsNs

}
