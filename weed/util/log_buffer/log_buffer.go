package log_buffer

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/mq_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"github.com/seaweedfs/seaweedfs/weed/util/mem"
)

const BufferSize = 8 * 1024 * 1024
const PreviousBufferCount = 4

// EvictionGatedOffset is a sentinel cursor offset (-2..-6 are taken by other
// sentinels) that reads like the plain -2 sentinel except below the eviction
// watermark: there ReadFromBuffer refuses with ResumeFromDiskError instead of
// silently serving from the earliest retained window. The check runs under the
// read lock, atomically with the serve decision, which callers cannot do from
// outside - an eviction can land between any caller-side check and the read.
const EvictionGatedOffset = -7

// flushQueueDepth bounds queued flush copies (BufferSize each); a full queue
// blocks producers, so a stalled flush backpressures writers instead of
// pinning hundreds of buffer copies.
const flushQueueDepth = 16

// flushQueueBudget bounds the same queue in bytes. Counting copies only holds
// if every copy is a window's worth: an entry larger than BufferSize grows its
// window to fit, and a queue of those multiplies straight through — sixteen
// 100 MB windows is 1.6 GB of flush copies alone. The ceiling is the one the
// depth was chosen for, so ordinary windows still queue sixteen deep.
//
// This covers the flush queue only. A sealed window stays reachable through
// prevBuffers for PreviousBufferCount more seals, and readers may take a
// snapshot of it, so an oversized entry still costs several times its size
// before it falls out of the ring.
const flushQueueBudget = flushQueueDepth * BufferSize

// Errors that can be returned by log buffer operations
var (
	// ErrBufferCorrupted indicates the log buffer contains corrupted data
	ErrBufferCorrupted = fmt.Errorf("log buffer is corrupted")
)

type dataToFlush struct {
	startTime time.Time
	stopTime  time.Time
	data      []byte // slab from mem.Allocate; returned via mem.Free after flush
	minOffset int64
	maxOffset int64
	seq       uint64        // seal order, so the budget admits windows in order
	budget    int           // bytes reserved from flushBudget, released after the flush
	done      chan struct{} // Signal when flush completes
}

// flushBudget accounts the bytes of sealed windows waiting to be written, so a
// producer waits for the queue to drain instead of adding another copy to it.
// Windows are admitted strictly in seal order: a producer parked here can wait
// seconds, and letting a later window overtake an earlier one would hand
// loopFlush the windows out of order.
type flushBudget struct {
	mu      sync.Mutex
	cond    *sync.Cond
	limit   int
	queued  int
	nextSeq uint64
	closed  bool
}

func newFlushBudget(limit int) *flushBudget {
	b := &flushBudget{limit: limit}
	b.cond = sync.NewCond(&b.mu)
	return b
}

// reserve blocks until it is this window's turn and its bytes fit under the
// limit, then returns the amount to hand back to release. A window larger than
// the whole budget is admitted on its own once the queue empties, so an
// oversized entry still gets through.
func (b *flushBudget) reserve(seq uint64, n int) int {
	if n > b.limit {
		n = b.limit
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	for !b.closed && (seq != b.nextSeq || (b.queued > 0 && b.queued+n > b.limit)) {
		b.cond.Wait()
	}
	if seq == b.nextSeq {
		b.nextSeq++
	}
	b.queued += n
	b.cond.Broadcast() // wake whoever is next in line
	return n
}

// waitForRoom parks until the queue has headroom for a window of n bytes,
// without charging anything. A window is copied into its slab while the write
// lock is held, before queueFlush gets to reserve, so a burst of concurrent
// oversized writers would each be holding a full copy in hand by the time they
// queue up -- memory the budget never sees. Large writers wait here first so
// they arrive at the seal a few at a time. This throttles the burst rather than
// bounding it: a writer that passes the check still seals unconditionally.
func (b *flushBudget) waitForRoom(n int) {
	if n > b.limit {
		n = b.limit
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	for !b.closed && b.queued > 0 && b.queued+n > b.limit {
		b.cond.Wait()
	}
}

func (b *flushBudget) release(n int) {
	if n == 0 {
		return
	}
	b.mu.Lock()
	b.queued -= n
	b.mu.Unlock()
	b.cond.Broadcast()
}

// close stops the budget from parking anyone, so shutdown is never held up by
// a producer waiting on a flush that will not run.
func (b *flushBudget) close() {
	b.mu.Lock()
	b.closed = true
	b.mu.Unlock()
	b.cond.Broadcast()
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
	// 8-byte aligned fields
	LastTsNs          atomic.Int64
	lastFlushTsNs     atomic.Int64
	lastFlushedOffset atomic.Int64 // Highest offset that has been flushed to disk (-1 = nothing flushed yet)
	lastEvictedTsNs   atomic.Int64 // Latest stopTime evicted from the sealed ring (0 = nothing evicted yet)
	// lastEvictedTsNs in pre-bump timestamps: gap proofs compare disk cursors,
	// which never see the bumped values out-of-order arrivals get.
	lastEvictedOriginalTsNs  atomic.Int64
	curWindowMaxOriginalTsNs int64 // max pre-bump ts in the open window, under the write lock
	offset                   int64
	bufferStartOffset        int64
	minOffset                int64
	maxOffset                int64
	flushInterval            time.Duration
	startTime                time.Time
	stopTime                 time.Time

	// Other fields
	name           string
	prevBuffers    *SealedBuffers
	buf            []byte
	idx            []int
	pos            int
	sizeBuf        []byte
	flushFn        LogFlushFuncType
	ReadFromDiskFn LogReadFromDiskFuncType
	notifyFn       func()
	// Per-subscriber notification channels for instant wake-up
	subscribersMu sync.RWMutex
	subscribers   map[string]chan struct{} // subscriberID -> notification channel
	// Notified only when a flush lands, for readers that cannot act on an append
	flushSubscribers map[string]chan struct{}
	isStopping       *atomic.Bool
	shutdownCh       chan struct{}  // closed by ShutdownLogBuffer to wake blocked subscribers
	loopsDone        sync.WaitGroup // loopFlush and loopInterval signal exit
	isAllFlushed     bool
	flushChan        chan *dataToFlush
	flushBudget      *flushBudget
	flushSeq         uint64 // seal counter, assigned under the write lock
	// Offset range tracking for Kafka integration
	hasOffsets bool
	// Disk chunk cache for historical data reads
	diskChunkCache *DiskChunkCache
	// curSnap is a GC-owned copy of the current window's append-only prefix
	// buf[:len(curSnap)], shared by all readers so each byte is copied once per
	// window instead of once per reader. Extended lazily under curSnapMu; reset
	// at seal. Existing holders keep their prefix slices, which never mutate.
	curSnapMu sync.Mutex
	curSnap   []byte
	sync.RWMutex
}

func NewLogBuffer(name string, flushInterval time.Duration, flushFn LogFlushFuncType,
	readFromDiskFn LogReadFromDiskFuncType, notifyFn func()) *LogBuffer {
	lb := &LogBuffer{
		name:             name,
		prevBuffers:      newSealedBuffers(PreviousBufferCount),
		buf:              make([]byte, BufferSize),
		sizeBuf:          make([]byte, 4),
		flushInterval:    flushInterval,
		flushFn:          flushFn,
		ReadFromDiskFn:   readFromDiskFn,
		notifyFn:         notifyFn,
		subscribers:      make(map[string]chan struct{}),
		flushSubscribers: make(map[string]chan struct{}),
		flushChan:        make(chan *dataToFlush, flushQueueDepth),
		isStopping:       new(atomic.Bool),
		shutdownCh:       make(chan struct{}),
		offset:           0, // Will be initialized from existing data if available
		flushBudget:      newFlushBudget(flushQueueBudget),
		diskChunkCache: &DiskChunkCache{
			chunks:    make(map[int64]*CachedDiskChunk),
			maxChunks: 16, // Cache up to 16 chunks (configurable)
		},
	}
	lb.lastFlushedOffset.Store(-1) // Nothing flushed to disk yet
	lb.loopsDone.Add(2)
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

// RegisterFlushSubscriber registers a subscriber woken only when a flush lands.
// A reader waiting for data it can only get from disk has nothing to do with an
// append, and taking those wake-ups off the shared channel would cost it one
// scheduling round-trip per write - and keep that channel drained, so every
// writer's non-blocking send succeeds instead of falling through.
func (logBuffer *LogBuffer) RegisterFlushSubscriber(subscriberID string) chan struct{} {
	logBuffer.subscribersMu.Lock()
	defer logBuffer.subscribersMu.Unlock()

	if existingChan, exists := logBuffer.flushSubscribers[subscriberID]; exists {
		return existingChan
	}
	notifyChan := make(chan struct{}, 1)
	logBuffer.flushSubscribers[subscriberID] = notifyChan
	return notifyChan
}

// UnregisterFlushSubscriber removes a flush subscriber and closes its channel
func (logBuffer *LogBuffer) UnregisterFlushSubscriber(subscriberID string) {
	logBuffer.subscribersMu.Lock()
	defer logBuffer.subscribersMu.Unlock()

	if ch, exists := logBuffer.flushSubscribers[subscriberID]; exists {
		close(ch)
		delete(logBuffer.flushSubscribers, subscriberID)
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

	for _, notifyChan := range logBuffer.subscribers {
		select {
		case notifyChan <- struct{}{}:
			// Notification sent successfully
		default:
			// Channel full - subscriber hasn't consumed previous notification yet
			// This is OK because one notification is sufficient to wake the subscriber
		}
	}
}

// notifyFlushSubscribers wakes the readers that only care about a flush landing
func (logBuffer *LogBuffer) notifyFlushSubscribers() {
	logBuffer.subscribersMu.RLock()
	defer logBuffer.subscribersMu.RUnlock()

	for _, notifyChan := range logBuffer.flushSubscribers {
		select {
		case notifyChan <- struct{}{}:
		default:
		}
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
		// Set lastFlushTsNs to current time (we know data up to highestOffset is on disk)
		logBuffer.lastFlushTsNs.Store(time.Now().UnixNano())
	} else {
		logBuffer.bufferStartOffset = 0 // Start from offset 0
		// No data on disk yet
	}

	return nil
}

func (logBuffer *LogBuffer) AddToBuffer(message *mq_pb.DataMessage) error {
	return logBuffer.AddDataToBuffer(message.Key, message.Value, message.TsNs)
}

// AddLogEntryToBuffer directly adds a LogEntry to the buffer, preserving offset information
func (logBuffer *LogBuffer) AddLogEntryToBuffer(logEntry *filer_pb.LogEntry) error {
	if len(logEntry.Data) > BufferSize {
		logBuffer.flushBudget.waitForRoom(len(logEntry.Data))
	}

	var toFlush *dataToFlush
	var marshalErr error
	logBuffer.Lock()
	defer func() {
		logBuffer.Unlock()
		if toFlush != nil {
			logBuffer.queueFlush(toFlush)
		}
		// Only notify if there was no error
		if marshalErr == nil {
			if logBuffer.notifyFn != nil {
				logBuffer.notifyFn()
			}
			// Notify all registered subscribers instantly (<1ms latency)
			logBuffer.notifySubscribers()
		}
	}()

	processingTsNs := logEntry.TsNs
	ts := time.Unix(0, processingTsNs)
	originalTsNs := processingTsNs

	// Handle timestamp collision inside lock (rare case)
	if logBuffer.LastTsNs.Load() >= processingTsNs {
		processingTsNs = logBuffer.LastTsNs.Add(1)
		ts = time.Unix(0, processingTsNs)
		// Re-marshal with corrected timestamp
		logEntry.TsNs = processingTsNs
	} else {
		logBuffer.LastTsNs.Store(processingTsNs)
	}

	// Size is computed without allocating; the entry is marshaled straight into
	// the buffer below via MarshalToSizedBufferVT.
	size := logEntry.SizeVT()

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
			// The window is sized size+4, so that is what has to stay in bounds
			if size < 0 || size > math.MaxInt-4 || size > maxBufferSize-4 {
				marshalErr = fmt.Errorf("message size %d exceeds maximum allowed size", size)
				glog.Errorf("%v", marshalErr)
				return marshalErr
			}
			// Fit the entry exactly. Doubling left room for a second oversized
			// record in the same window, which only doubles the flush copy and
			// the snapshot taken of it.
			logBuffer.buf = make([]byte, size+4)
		}
	}
	logBuffer.stopTime = ts

	// Marshal directly into the buffer, avoiding an intermediate slice and copy.
	// On the (practically impossible) error the entry is dropped before idx/pos
	// are advanced, leaving the buffer consistent.
	if _, err := logEntry.MarshalToSizedBufferVT(logBuffer.buf[logBuffer.pos+4 : logBuffer.pos+4+size]); err != nil {
		marshalErr = fmt.Errorf("failed to marshal LogEntry: %w", err)
		glog.Errorf("%v", marshalErr)
		return marshalErr
	}
	logBuffer.idx = append(logBuffer.idx, logBuffer.pos)
	util.Uint32toBytes(logBuffer.sizeBuf, uint32(size))
	copy(logBuffer.buf[logBuffer.pos:logBuffer.pos+4], logBuffer.sizeBuf)
	logBuffer.pos += size + 4
	// Only now is the entry's window known: a rollover above seals the previous
	// window first, and crediting this timestamp before that hands it to the
	// sealed window and loses it from the new one - corrupting the received-ts
	// eviction watermark in both directions.
	if originalTsNs > logBuffer.curWindowMaxOriginalTsNs {
		logBuffer.curWindowMaxOriginalTsNs = originalTsNs
	}

	logBuffer.offset++
	return nil
}

func (logBuffer *LogBuffer) AddDataToBuffer(partitionKey, data []byte, processingTsNs int64) error {

	// An entry this large gets a window to itself, so it will seal and copy one;
	// wait for the queue to have room before joining the queue for the lock.
	if len(data) > BufferSize {
		logBuffer.flushBudget.waitForRoom(len(data))
	}

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

	var toFlush *dataToFlush
	var marshalErr error
	logBuffer.Lock()
	defer func() {
		logBuffer.Unlock()
		if toFlush != nil {
			logBuffer.queueFlush(toFlush)
		}
		// Only notify if there was no error
		if marshalErr == nil {
			if logBuffer.notifyFn != nil {
				logBuffer.notifyFn()
			}
			// Notify all registered subscribers instantly (<1ms latency)
			logBuffer.notifySubscribers()
		}
	}()

	originalTsNs := processingTsNs
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

	// Size is computed without allocating; the entry is marshaled straight into
	// the buffer below via MarshalToSizedBufferVT.
	size := logEntry.SizeVT()

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
		toFlush = logBuffer.copyToFlush()
		logBuffer.startTime = ts
		if len(logBuffer.buf) < size+4 {
			// Validate size to prevent integer overflow in computation BEFORE allocation
			const maxBufferSize = 1 << 30 // 1 GiB practical limit
			// The window is sized size+4, so that is what has to stay in bounds
			if size < 0 || size > math.MaxInt-4 || size > maxBufferSize-4 {
				marshalErr = fmt.Errorf("message size %d exceeds maximum allowed size", size)
				glog.Errorf("%v", marshalErr)
				return marshalErr
			}
			// Fit the entry exactly. Doubling left room for a second oversized
			// record in the same window, which only doubles the flush copy and
			// the snapshot taken of it.
			logBuffer.buf = make([]byte, size+4)
		}
	}
	logBuffer.stopTime = ts

	// Marshal directly into the buffer, avoiding an intermediate slice and copy.
	// On the (practically impossible) error the entry is dropped before idx/pos
	// are advanced, leaving the buffer consistent.
	if _, err := logEntry.MarshalToSizedBufferVT(logBuffer.buf[logBuffer.pos+4 : logBuffer.pos+4+size]); err != nil {
		marshalErr = fmt.Errorf("failed to marshal LogEntry: %w", err)
		glog.Errorf("%v", marshalErr)
		return marshalErr
	}
	logBuffer.idx = append(logBuffer.idx, logBuffer.pos)
	util.Uint32toBytes(logBuffer.sizeBuf, uint32(size))
	copy(logBuffer.buf[logBuffer.pos:logBuffer.pos+4], logBuffer.sizeBuf)
	logBuffer.pos += size + 4
	// Only now is the entry's window known: a rollover above seals the previous
	// window first, and crediting this timestamp before that hands it to the
	// sealed window and loses it from the new one - corrupting the received-ts
	// eviction watermark in both directions.
	if originalTsNs > logBuffer.curWindowMaxOriginalTsNs {
		logBuffer.curWindowMaxOriginalTsNs = originalTsNs
	}

	logBuffer.offset++
	return nil
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
		// The live buffer was already sealed and reset by copyToFlushWithCallback,
		// so dropping toFlush on a timeout would lose it. Block until queued,
		// bailing out only on shutdown.
		if !logBuffer.queueFlush(toFlush) {
			return
		}
		select {
		case <-toFlush.done:
			// Flush completed
		case <-time.After(5 * time.Second):
			// Queued but not yet flushed; loopFlush will still persist it
		}
	}
}

// ShutdownLogBuffer flushes the buffer and stops the log buffer
func (logBuffer *LogBuffer) ShutdownLogBuffer() {
	isAlreadyStopped := logBuffer.isStopping.Swap(true)
	if isAlreadyStopped {
		return
	}
	// Wake any subscribers blocked in awaitNotificationOrTimeout so they can
	// notice IsStopping() and exit promptly, even on an idle buffer where no
	// flush notification would otherwise fire.
	close(logBuffer.shutdownCh)
	// Let go of the flush budget before sealing the last window, so a producer
	// parked on it wakes up and the hand-off below cannot wait on a reservation.
	logBuffer.flushBudget.close()
	logBuffer.Lock()
	toFlush := logBuffer.copyToFlush()
	logBuffer.Unlock()
	if toFlush != nil {
		toFlush.budget = logBuffer.flushBudget.reserve(toFlush.seq, cap(toFlush.data))
		logBuffer.flushChan <- toFlush
	}
	// nil is the shutdown sentinel: loopFlush drains everything queued before
	// it and exits. The channel is never closed, so a sender racing shutdown
	// can never panic on a closed channel.
	logBuffer.flushChan <- nil
}

// IsAllFlushed returns true if all data in the buffer has been flushed, after calling ShutdownLogBuffer().
func (logBuffer *LogBuffer) IsAllFlushed() bool {
	return logBuffer.isAllFlushed
}

// queueFlush hands a sealed window to loopFlush, reserving its bytes first so
// a producer waits for the queue to drain rather than adding another copy to
// it. Reports false when the buffer shut down before the hand-off.
func (logBuffer *LogBuffer) queueFlush(d *dataToFlush) bool {
	// Charge the slab, not the window: mem.Allocate rounds up to a size class,
	// so the bytes actually held are cap(data), and charging len would let the
	// queue hold up to twice the ceiling.
	d.budget = logBuffer.flushBudget.reserve(d.seq, cap(d.data))
	// The window is already sealed, so dropping it here loses records the
	// caller was told were accepted. Take any room in the queue first, and only
	// fall back to the shutdown escape when there is none.
	select {
	case logBuffer.flushChan <- d:
		return true
	default:
	}
	select {
	case logBuffer.flushChan <- d:
		return true
	case <-logBuffer.shutdownCh:
		// shutting down; loopFlush may be gone, do not park forever
		logBuffer.flushBudget.release(d.budget)
		return false
	}
}

func (logBuffer *LogBuffer) loopFlush() {
	defer logBuffer.loopsDone.Done()
	for d := range logBuffer.flushChan {
		if d == nil {
			break // shutdown sentinel
		}
		logBuffer.flushFn(logBuffer, d.startTime, d.stopTime, d.data, d.minOffset, d.maxOffset)
		d.releaseMemory()
		logBuffer.flushBudget.release(d.budget)
		// local logbuffer is different from aggregate logbuffer here
		if d.maxOffset >= 0 {
			logBuffer.lastFlushedOffset.Store(d.maxOffset)
		}
		if !d.stopTime.IsZero() {
			logBuffer.lastFlushTsNs.Store(d.stopTime.UnixNano())
		}

		// Wake readers that may be waiting to retry disk reads after the flush lands.
		// LOAD-BEARING ORDER: the watermark store above must precede these
		// notifications. A parked filer subscriber re-checks GetLastFlushTsNs on
		// wake-up and goes back to sleep if it has not moved; notifying first
		// opens a window where the wake-up looks spurious and the flush that
		// caused it is only picked up by the retry timer. Not testable from
		// outside (the window is nanoseconds on this goroutine) - keep the order.
		if logBuffer.notifyFn != nil {
			logBuffer.notifyFn()
		}
		logBuffer.notifySubscribers()
		logBuffer.notifyFlushSubscribers()

		// Signal completion if there's a callback channel
		if d.done != nil {
			close(d.done)
		}
	}
	logBuffer.isAllFlushed = true
}

func (logBuffer *LogBuffer) loopInterval() {
	defer logBuffer.loopsDone.Done()
	// Wake on shutdown instead of sleeping through the interval: a goroutine
	// parked in time.Sleep keeps the buffer and its ~40MB of slabs reachable
	// for up to flushInterval after ShutdownLogBuffer.
	ticker := time.NewTicker(logBuffer.flushInterval)
	defer ticker.Stop()
	for {
		select {
		case <-logBuffer.shutdownCh:
			return
		case <-ticker.C:
		}

		logBuffer.Lock()
		toFlush := logBuffer.copyToFlush()
		logBuffer.Unlock()
		if toFlush != nil {
			logBuffer.queueFlush(toFlush)
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
			// Stamped under the lock so the budget can admit windows in the
			// order they were sealed. Every stamped window must reach reserve
			// exactly once or the queue stalls behind the missing turn.
			d.seq = logBuffer.flushSeq
			logBuffer.flushSeq++
		}
		// CRITICAL: logBuffer.offset is the "next offset to assign", so last offset in buffer is offset-1
		lastOffsetInBuffer := logBuffer.offset - 1
		// Slot 0 falls out of the ring in SealBuffer below, so record how far
		// eviction has reached before it goes - in both timestamp spaces.
		if evicted := logBuffer.prevBuffers.buffers[0]; evicted.size > 0 && !evicted.stopTime.IsZero() {
			if ts := evicted.stopTime.UnixNano(); ts > logBuffer.lastEvictedTsNs.Load() {
				logBuffer.lastEvictedTsNs.Store(ts)
			}
			if ts := evicted.maxOriginalTsNs; ts > logBuffer.lastEvictedOriginalTsNs.Load() {
				logBuffer.lastEvictedOriginalTsNs.Store(ts)
			}
		}
		logBuffer.buf = logBuffer.prevBuffers.SealBuffer(logBuffer.startTime, logBuffer.stopTime, logBuffer.buf, logBuffer.pos, logBuffer.bufferStartOffset, lastOffsetInBuffer)
		logBuffer.prevBuffers.buffers[len(logBuffer.prevBuffers.buffers)-1].maxOriginalTsNs = logBuffer.curWindowMaxOriginalTsNs
		logBuffer.curWindowMaxOriginalTsNs = 0
		// SealBuffer hands back the oldest window array to reuse. An entry larger
		// than BufferSize grew one of these arrays to fit it, and buffers cycle
		// forever, so without this a single oversized entry leaves every later
		// window carrying — and snapshotting — its size. Growth is on demand, so
		// the next oversized entry just reallocates.
		if len(logBuffer.buf) > BufferSize {
			logBuffer.buf = make([]byte, BufferSize)
		}
		// Hand a fully extended prefix snapshot to the sealed slot so sealed
		// readers reuse it instead of re-copying the window; reset for the next
		// window either way (holders keep their immutable prefix slices).
		if len(logBuffer.curSnap) == logBuffer.pos {
			logBuffer.prevBuffers.buffers[len(logBuffer.prevBuffers.buffers)-1].snapshot = logBuffer.curSnap[:logBuffer.pos:logBuffer.pos]
		}
		logBuffer.curSnap = nil
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

// GetEarliestTime returns the oldest timestamp still resident in the buffer.
// It must consider the sealed prev buffers in addition to the active buffer,
// because ReadFromBuffer's tsMemory (and therefore ResumeFromDiskError) is
// computed from the min across both.  Returning only the active startTime
// would cause gap-detection callers to skip past data still living in prev
// buffers, and can also silently equal the consumer's lastReadTime.
func (logBuffer *LogBuffer) GetEarliestTime() time.Time {
	logBuffer.RLock()
	defer logBuffer.RUnlock()

	earliest := logBuffer.startTime
	for _, prevBuf := range logBuffer.prevBuffers.buffers {
		if prevBuf.startTime.IsZero() {
			continue
		}
		if earliest.IsZero() || prevBuf.startTime.Before(earliest) {
			earliest = prevBuf.startTime
		}
	}
	return earliest
}

func (logBuffer *LogBuffer) HasData() bool {
	logBuffer.RLock()
	defer logBuffer.RUnlock()

	if logBuffer.pos > 0 {
		return true
	}
	for _, buf := range logBuffer.prevBuffers.buffers {
		if buf.size > 0 {
			return true
		}
	}
	return false
}

func (logBuffer *LogBuffer) GetEarliestPosition() MessagePosition {
	return MessagePosition{
		Time:   logBuffer.startTime,
		Offset: logBuffer.offset,
	}
}

// FlushedThroughTsNs reports the timestamp through which this buffer's data
// is durably on disk: when nothing added is pending a flush, everything ever
// added is flushed, so the buffer is flush-complete through "now" (any later
// entry gets a newer timestamp); otherwise it is complete through the last
// flushed window's stop time.
func (logBuffer *LogBuffer) FlushedThroughTsNs(nowNs int64) int64 {
	if logBuffer.LastTsNs.Load() <= logBuffer.lastFlushTsNs.Load() {
		return nowNs
	}
	return logBuffer.lastFlushTsNs.Load()
}

// GetLastFlushTsNs returns the latest flushed timestamp in Unix nanoseconds.
// Returns 0 if nothing has been flushed yet.
func (logBuffer *LogBuffer) GetLastFlushTsNs() int64 {
	return logBuffer.lastFlushTsNs.Load()
}

// GetLastEvictedOriginalTsNs is GetLastEvictedTsNs in pre-bump timestamps -
// the space disk cursors live in.
func (logBuffer *LogBuffer) GetLastEvictedOriginalTsNs() int64 {
	return logBuffer.lastEvictedOriginalTsNs.Load()
}

// GetLastEvictedTsNs returns the stopTime of the newest window dropped from the
// sealed ring, or 0 if nothing has been evicted. A reader positioned past it
// knows the retained buffers still hold every entry after its position, which is
// the only emptiness proof available to a buffer that never flushes.
func (logBuffer *LogBuffer) GetLastEvictedTsNs() int64 {
	return logBuffer.lastEvictedTsNs.Load()
}

func (logBuffer *LogBuffer) SetLastFlushTsNs(ts int64) {
	logBuffer.lastFlushTsNs.Store(ts)
}

func (d *dataToFlush) releaseMemory() {
	// Guard nil: mem.Free(nil) would put a zero-cap slice into the smallest slot
	// pool, so a later Allocate could hand back nil and panic. Also makes a double
	// release harmless.
	if d.data != nil {
		mem.Free(d.data)
		d.data = nil
	}
}

// ReadFromBuffer returns the in-memory log data at lastReadPosition. isPooled
// reports whether the returned buffer is a private pooled copy the caller must
// return via ReleaseMemory; when false the buffer wraps a snapshot shared with
// other readers and must not be released (or written to).
func (logBuffer *LogBuffer) ReadFromBuffer(lastReadPosition MessagePosition) (bufferCopy *bytes.Buffer, batchIndex int64, isPooled bool, err error) {
	logBuffer.RLock()
	defer logBuffer.RUnlock()

	isOffsetBased := lastReadPosition.IsOffsetBased

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
					return nil, -2, false, ResumeFromDiskError
				}
				// requestedOffset == logBuffer.offset: Current position
				// CRITICAL: For subscribers starting from offset 0, try disk read first
				// (historical data might exist from previous runs)
				if requestedOffset == 0 && logBuffer.bufferStartOffset == 0 && logBuffer.offset == 0 {
					// Initial state: try disk read before waiting for new data
					return nil, -2, false, ResumeFromDiskError
				}
				// Otherwise, wait for new data to arrive
				return nil, logBuffer.offset, false, nil
			}
			return logBuffer.currentSnapshotView(0, logBuffer.pos), logBuffer.offset, false, nil
		}

		// Check previous buffers for the requested offset
		for _, buf := range logBuffer.prevBuffers.buffers {
			if requestedOffset >= buf.startOffset && requestedOffset <= buf.offset {
				// If prevBuffer is empty, it means the data was flushed to disk
				// (prevBuffers are created when buffer is flushed)
				if buf.size == 0 {
					// Empty prevBuffer covering this offset means data was flushed
					return nil, -2, false, ResumeFromDiskError
				}
				return sharedBufferView(buf, 0), buf.offset, false, nil
			}
		}

		// Offset not found in any buffer
		if requestedOffset < logBuffer.bufferStartOffset {
			// Data not in current buffers - must be on disk (flushed or never existed)
			// Return ResumeFromDiskError to trigger disk read
			return nil, -2, false, ResumeFromDiskError
		}

		if requestedOffset > logBuffer.offset {
			// Future data, not available yet
			return nil, logBuffer.offset, false, nil
		}

		// Offset not found - return nil
		return nil, logBuffer.offset, false, nil
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
	for _, prevBuf := range logBuffer.prevBuffers.buffers {
		if !prevBuf.startTime.IsZero() {
			// If tsMemory is zero, assign directly; otherwise compare
			if tsMemory.IsZero() || prevBuf.startTime.Before(tsMemory) {
				tsMemory = prevBuf.startTime
			}
		}
	}
	if tsMemory.IsZero() { // case 2.2
		// Buffer is empty - return ResumeFromDiskError so caller can read from disk
		// This fixes issue #4977 where SubscribeMetadata stalls because
		// MetaAggregator.MetaLogBuffer is empty in single-filer setups
		return nil, -2, false, ResumeFromDiskError
	} else if lastReadPosition.Time.Before(tsMemory) { // case 2.3
		// For time-based reads, only check timestamp for disk reads
		// Don't use offset comparisons as they're not meaningful for time-based subscriptions

		// A gated cursor below the eviction watermark must go to disk: serving it
		// from the earliest retained window would silently skip the evicted span.
		if lastReadPosition.Offset == EvictionGatedOffset && lastReadPosition.Time.UnixNano() < logBuffer.lastEvictedTsNs.Load() {
			return nil, -2, false, ResumeFromDiskError
		}
		// Special case: If requested time is zero (Unix epoch), treat as "start from beginning"
		// This handles queries that want to read all data without knowing the exact start time
		if lastReadPosition.Time.IsZero() || lastReadPosition.Time.Unix() == 0 {
			// Start from the beginning of memory
			// Fall through to case 2.1 to read from earliest buffer
		} else if lastReadPosition.Offset <= 0 && lastReadPosition.Time.Before(tsMemory) {
			// Treat first read with sentinel/zero offset as inclusive of earliest in-memory data
		} else {
			// Data not in memory buffers - read from disk
			return nil, -2, false, ResumeFromDiskError
		}
	}

	// the following is case 2.1

	if lastReadPosition.Time.Equal(logBuffer.stopTime) && !logBuffer.stopTime.IsZero() {
		// For first-read sentinel/zero offset, allow inclusive read at the boundary
		if lastReadPosition.Offset > 0 {
			return nil, logBuffer.offset, false, nil
		}
	}
	if lastReadPosition.Time.After(logBuffer.stopTime) && !logBuffer.stopTime.IsZero() {
		return nil, logBuffer.offset, false, nil
	}
	// Also check prevBuffers when current buffer is empty (startTime is zero)
	if lastReadPosition.Time.Before(logBuffer.startTime) || logBuffer.startTime.IsZero() {
		for _, buf := range logBuffer.prevBuffers.buffers {
			if buf.startTime.After(lastReadPosition.Time) {
				return sharedBufferView(buf, 0), buf.offset, false, nil
			}
			if !buf.startTime.After(lastReadPosition.Time) && buf.stopTime.After(lastReadPosition.Time) {
				searchTime := lastReadPosition.Time
				if lastReadPosition.Offset <= 0 {
					searchTime = searchTime.Add(-time.Nanosecond)
				}
				pos, err := buf.locateByTs(searchTime)
				if err != nil {
					// Buffer corruption detected - return error wrapped with ErrBufferCorrupted
					glog.Errorf("ReadFromBuffer: buffer corruption in prevBuffer: %v", err)
					return nil, -1, false, fmt.Errorf("%w: %v", ErrBufferCorrupted, err)
				}
				if pos < buf.size {
					return sharedBufferView(buf, pos), buf.offset, false, nil
				}
			}
		}
		// If current buffer is not empty, return it
		if logBuffer.pos > 0 {
			return logBuffer.currentSnapshotView(0, logBuffer.pos), logBuffer.offset, false, nil
		}
		// Buffer is empty and no data in prevBuffers - wait for new data
		return nil, logBuffer.offset, false, nil
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
		_, t, err := readTs(logBuffer.buf, pos)
		if err != nil {
			// Buffer corruption detected in binary search
			glog.Errorf("ReadFromBuffer: buffer corruption at idx[%d] pos %d: %v", mid, pos, err)
			return nil, -1, false, fmt.Errorf("%w: %v", ErrBufferCorrupted, err)
		}
		if t <= searchTs {
			l = mid + 1
		} else if searchTs < t {
			var prevT int64
			if mid > 0 {
				_, prevT, err = readTs(logBuffer.buf, logBuffer.idx[mid-1])
				if err != nil {
					// Buffer corruption detected in binary search (previous entry)
					glog.Errorf("ReadFromBuffer: buffer corruption at idx[%d] pos %d: %v", mid-1, logBuffer.idx[mid-1], err)
					return nil, -1, false, fmt.Errorf("%w: %v", ErrBufferCorrupted, err)
				}
			}
			if prevT <= searchTs {
				return logBuffer.currentSnapshotView(pos, logBuffer.pos), logBuffer.offset, false, nil
			}
			h = mid
		}
	}

	// Binary search didn't find the timestamp - data may have been flushed to disk already
	// Returning -2 signals to caller that data is not available in memory
	return nil, -2, false, nil

}

// ReleaseMemory returns a pooled buffer for reuse. Only call it for buffers
// ReadFromBuffer reported as pooled: recycling a shared sealed-window view
// would let the next copiedBytes overwrite bytes other readers still hold.
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

// copiedBytes returns a private copy of buf backed by the shared, size-classed
// slab pool. The caller owns it until mem.Free (see dataToFlush.releaseMemory),
// so the live buffer never outlives the flush. Routing through mem reuses slabs
// across the process instead of reallocating a window-sized array per flush.
func copiedBytes(buf []byte) []byte {
	copied := mem.Allocate(len(buf))
	copy(copied, buf)
	return copied
}

// sharedBufferView wraps the sealed window's shared snapshot from pos onward.
// The returned buffer aliases memory shared with other readers: it must be
// treated as read-only and never passed to ReleaseMemory (the full-slice cap
// makes an accidental append reallocate instead of scribbling on the snapshot).
func sharedBufferView(mb *MemBuffer, pos int) *bytes.Buffer {
	snap := mb.sharedSnapshot()
	return bytes.NewBuffer(snap[pos:len(snap):len(snap)])
}

// currentSnapshotView returns buf[from:to] of the current window as a view of
// the shared prefix snapshot, extending the snapshot to cover [0:to) first.
// buf[:pos] is append-only until the window seals, so extension only ever
// appends stable bytes; earlier holders' slices are unaffected. Callers must
// hold the read lock (keeps buf and pos stable during extension).
func (logBuffer *LogBuffer) currentSnapshotView(from, to int) *bytes.Buffer {
	logBuffer.curSnapMu.Lock()
	if cap(logBuffer.curSnap) < to {
		// First use in this window (or the rare window whose array outgrew the
		// previous one): size to the window array so extensions never reallocate.
		grown := make([]byte, len(logBuffer.curSnap), len(logBuffer.buf))
		copy(grown, logBuffer.curSnap)
		logBuffer.curSnap = grown
	}
	if len(logBuffer.curSnap) < to {
		logBuffer.curSnap = append(logBuffer.curSnap, logBuffer.buf[len(logBuffer.curSnap):to]...)
	}
	snap := logBuffer.curSnap[:to]
	logBuffer.curSnapMu.Unlock()
	return bytes.NewBuffer(snap[from:to:to])
}

func readTs(buf []byte, pos int) (size int, ts int64, err error) {
	// Bounds check for size field (overflow-safe)
	if pos < 0 || pos > len(buf)-4 {
		return 0, 0, fmt.Errorf("corrupted log buffer: cannot read size at pos %d, buffer length %d", pos, len(buf))
	}

	size = int(util.BytesToUint32(buf[pos : pos+4]))

	// Bounds check for entry data (overflow-safe, protects against negative size)
	if size < 0 || size > len(buf)-pos-4 {
		return 0, 0, fmt.Errorf("corrupted log buffer: entry size %d at pos %d exceeds buffer length %d", size, pos, len(buf))
	}

	entryData := buf[pos+4 : pos+4+size]

	// Read only LogEntry.ts_ns rather than unmarshaling the whole entry. This
	// runs on every binary-search probe in ReadFromBuffer; a full proto.Unmarshal
	// there allocates fresh slices for the data/key byte fields on each call, which
	// dominated allocation churn under metadata-subscription fan-out.
	ts, err = readTsNs(entryData)
	if err != nil {
		return 0, 0, fmt.Errorf("corrupted log buffer at pos %d, size %d: %w", pos, size, err)
	}

	return size, ts, nil
}

// readTsNs scans a marshaled LogEntry and returns its ts_ns (field 1, varint)
// without decoding the data/key byte fields. Fields serialize in number order,
// so ts_ns is normally the first tag and this returns after one varint; the full
// scan keeps it correct for any field order. A missing field 1 means the proto3
// default, ts_ns == 0.
func readTsNs(entryData []byte) (tsNs int64, err error) {
	for i := 0; i < len(entryData); {
		tag, n := binary.Uvarint(entryData[i:])
		if n <= 0 {
			return 0, fmt.Errorf("bad field tag at %d", i)
		}
		i += n
		fieldNum := tag >> 3
		switch tag & 0x7 { // wire type
		case 0: // varint
			v, m := binary.Uvarint(entryData[i:])
			if m <= 0 {
				return 0, fmt.Errorf("bad varint for field %d", fieldNum)
			}
			i += m
			if fieldNum == 1 { // ts_ns
				return int64(v), nil
			}
		case 1: // 64-bit
			i += 8
		case 2: // length-delimited: read length, skip payload without copying
			l, m := binary.Uvarint(entryData[i:])
			if m <= 0 {
				return 0, fmt.Errorf("bad length for field %d", fieldNum)
			}
			i += m
			if l > uint64(len(entryData)-i) {
				return 0, fmt.Errorf("field %d length %d overruns buffer", fieldNum, l)
			}
			i += int(l)
		case 5: // 32-bit
			i += 4
		default:
			return 0, fmt.Errorf("unknown wire type for field %d", fieldNum)
		}
		if i > len(entryData) {
			return 0, fmt.Errorf("field %d overruns buffer", fieldNum)
		}
	}
	return 0, nil
}

// unmarshalLogEntryAliased decodes a marshaled LogEntry into out, pointing the
// data and key fields at sub-slices of entryData instead of copying them. A full
// proto.Unmarshal allocates fresh slices for those byte fields on every entry
// (protobuf consumeBytesNoZero), which dominated allocation churn when many
// metadata subscribers each re-read the in-memory log window.
//
// The aliased data/key are only valid while entryData is, i.e. for the duration
// of the eachLogEntryFn callback. Callers must copy anything they retain past the
// callback; all current subscribers do (they re-decode data into their own event
// or hand it to a synchronous grpc Send).
func unmarshalLogEntryAliased(entryData []byte, out *filer_pb.LogEntry) error {
	out.TsNs = 0
	out.PartitionKeyHash = 0
	out.Data = nil
	out.Key = nil
	out.Offset = 0
	for i := 0; i < len(entryData); {
		tag, n := binary.Uvarint(entryData[i:])
		if n <= 0 {
			return fmt.Errorf("bad field tag at %d", i)
		}
		i += n
		fieldNum := tag >> 3
		switch tag & 0x7 { // wire type
		case 0: // varint: ts_ns / partition_key_hash / offset
			v, m := binary.Uvarint(entryData[i:])
			if m <= 0 {
				return fmt.Errorf("bad varint for field %d", fieldNum)
			}
			i += m
			switch fieldNum {
			case 1:
				out.TsNs = int64(v)
			case 2:
				out.PartitionKeyHash = int32(v)
			case 5:
				out.Offset = int64(v)
			}
		case 2: // length-delimited: data / key, aliased not copied
			l, m := binary.Uvarint(entryData[i:])
			if m <= 0 {
				return fmt.Errorf("bad length for field %d", fieldNum)
			}
			i += m
			if l > uint64(len(entryData)-i) {
				return fmt.Errorf("field %d length %d overruns buffer", fieldNum, l)
			}
			switch fieldNum {
			case 3:
				out.Data = entryData[i : i+int(l)]
			case 4:
				out.Key = entryData[i : i+int(l)]
			}
			i += int(l)
		case 1: // 64-bit
			i += 8
		case 5: // 32-bit
			i += 4
		default:
			return fmt.Errorf("unknown wire type for field %d", fieldNum)
		}
		if i > len(entryData) {
			return fmt.Errorf("field %d overruns buffer", fieldNum)
		}
	}
	return nil
}
