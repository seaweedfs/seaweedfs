package log_buffer

import (
	"bytes"
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
}

type EachLogEntryFuncType func(logEntry *filer_pb.LogEntry) (isDone bool, err error)
type EachLogEntryWithOffsetFuncType func(logEntry *filer_pb.LogEntry, offset int64) (isDone bool, err error)
type LogFlushFuncType func(logBuffer *LogBuffer, startTime, stopTime time.Time, buf []byte, minOffset, maxOffset int64)
type LogReadFromDiskFuncType func(startPosition MessagePosition, stopTsNs int64, eachLogEntryFn EachLogEntryFuncType) (lastReadPosition MessagePosition, isDone bool, err error)

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
	isStopping        *atomic.Bool
	isAllFlushed      bool
	flushChan         chan *dataToFlush
	LastTsNs          atomic.Int64
	// Offset range tracking for Kafka integration
	minOffset  int64
	maxOffset  int64
	hasOffsets bool
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
		flushChan:      make(chan *dataToFlush, 256),
		isStopping:     new(atomic.Bool),
		offset:         0, // Will be initialized from existing data if available
	}
	go lb.loopFlush()
	go lb.loopInterval()
	return lb
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
		// CRITICAL FIX: bufferStartOffset should always be 0 to allow reading all existing data
		// The offset field tracks the next offset to assign, but the buffer should be able to
		// read from offset 0 onwards (existing data will be read from disk)
		logBuffer.bufferStartOffset = 0
		glog.V(0).Infof("Initialized LogBuffer %s offset to %d (highest existing: %d), buffer starts at 0", logBuffer.name, nextOffset, highestOffset)
	} else {
		logBuffer.bufferStartOffset = 0 // Start from offset 0
		glog.V(0).Infof("No existing data found for %s, starting from offset 0", logBuffer.name)
	}

	return nil
}

func (logBuffer *LogBuffer) AddToBuffer(message *mq_pb.DataMessage) {
	logBuffer.AddDataToBuffer(message.Key, message.Value, message.TsNs)
}

// AddLogEntryToBuffer directly adds a LogEntry to the buffer, preserving offset information
func (logBuffer *LogBuffer) AddLogEntryToBuffer(logEntry *filer_pb.LogEntry) {
	// DEBUG: Log ALL writes to understand buffer naming
	glog.Infof("📝 ADD TO BUFFER: buffer=%s offset=%d keyLen=%d valueLen=%d",
		logBuffer.name, logEntry.Offset, len(logEntry.Key), len(logEntry.Data))

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
	if logEntry.Offset > 0 {
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
			// Validate size to prevent overflow BEFORE computation
			const maxBufferSize = 1 << 30 // 1 GB limit
			// Check size bounds before any arithmetic to prevent overflow
			if size < 0 || size > maxBufferSize/2-2 {
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
	}()

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
	}

	if logBuffer.startTime.Add(logBuffer.flushInterval).Before(ts) || len(logBuffer.buf)-logBuffer.pos < size+4 {
		// glog.V(0).Infof("%s copyToFlush1 offset:%d count:%d start time %v, ts %v, remaining %d bytes", logBuffer.name, logBuffer.offset, len(logBuffer.idx), logBuffer.startTime, ts, len(logBuffer.buf)-logBuffer.pos)
		toFlush = logBuffer.copyToFlush()
		logBuffer.startTime = ts
		if len(logBuffer.buf) < size+4 {
			// Validate size to prevent overflow BEFORE computation
			const maxBufferSize = 1 << 30 // 1 GB limit
			// Check size bounds before any arithmetic to prevent overflow
			if size < 0 || size > maxBufferSize/2-2 {
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

}

func (logBuffer *LogBuffer) IsStopping() bool {
	return logBuffer.isStopping.Load()
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
			// glog.V(4).Infof("%s flushing [0,%d) with %d entries [%v, %v]", m.name, m.pos, len(m.idx), m.startTime, m.stopTime)
		} else {
			// glog.V(4).Infof("%s removed from memory [0,%d) with %d entries [%v, %v]", m.name, m.pos, len(m.idx), m.startTime, m.stopTime)
			logBuffer.lastFlushDataTime = logBuffer.stopTime
		}
		// CRITICAL: logBuffer.offset is the "next offset to assign", so last offset in buffer is offset-1
		lastOffsetInBuffer := logBuffer.offset - 1
		logBuffer.buf = logBuffer.prevBuffers.SealBuffer(logBuffer.startTime, logBuffer.stopTime, logBuffer.buf, logBuffer.pos, logBuffer.bufferStartOffset, lastOffsetInBuffer)
		glog.V(0).Infof("🔒 SEALED BUFFER: [%d-%d] with %d bytes", logBuffer.bufferStartOffset, lastOffsetInBuffer, logBuffer.pos)
		logBuffer.startTime = time.Unix(0, 0)
		logBuffer.stopTime = time.Unix(0, 0)
		logBuffer.pos = 0
		logBuffer.idx = logBuffer.idx[:0]
		// DON'T increment offset - it's already pointing to the next offset!
		// logBuffer.offset++ // REMOVED - this was causing offset gaps!
		logBuffer.bufferStartOffset = logBuffer.offset // Next buffer starts at current offset (which is already the next one)
		// Reset offset tracking
		logBuffer.hasOffsets = false
		logBuffer.minOffset = 0
		logBuffer.maxOffset = 0
		return d
	}
	return nil
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

func (d *dataToFlush) releaseMemory() {
	d.data.Reset()
	bufferPool.Put(d.data)
}

func (logBuffer *LogBuffer) ReadFromBuffer(lastReadPosition MessagePosition) (bufferCopy *bytes.Buffer, batchIndex int64, err error) {
	logBuffer.RLock()
	defer logBuffer.RUnlock()

	isOffsetBased := lastReadPosition.IsOffsetBased
	glog.V(4).Infof("🔍 DEBUG: ReadFromBuffer called for %s, lastReadPosition=%v isOffsetBased=%v offset=%d",
		logBuffer.name, lastReadPosition, isOffsetBased, lastReadPosition.Offset)
	glog.V(4).Infof("🔍 DEBUG: Buffer state - startTime=%v, stopTime=%v, pos=%d, offset=%d", logBuffer.startTime, logBuffer.stopTime, logBuffer.pos, logBuffer.offset)
	glog.V(4).Infof("🔍 DEBUG: PrevBuffers count=%d", len(logBuffer.prevBuffers.buffers))

	// CRITICAL FIX: For offset-based subscriptions, use offset comparisons, not time comparisons!
	if isOffsetBased {
		requestedOffset := lastReadPosition.Offset
		glog.V(4).Infof("🔥 OFFSET-BASED READ: Requested offset=%d, current buffer [%d-%d]",
			requestedOffset, logBuffer.bufferStartOffset, logBuffer.offset)

		// Check if we have any data in memory
		if logBuffer.pos == 0 && len(logBuffer.prevBuffers.buffers) == 0 {
			glog.V(4).Infof("🔍 DEBUG: No memory data available - returning nil")
			return nil, -2, nil
		}

		// Check if the requested offset is in the current buffer
		if requestedOffset >= logBuffer.bufferStartOffset && requestedOffset <= logBuffer.offset {
			glog.V(0).Infof("✅ OFFSET-BASED: Returning current buffer [%d-%d] for offset %d",
				logBuffer.bufferStartOffset, logBuffer.offset, requestedOffset)
			return copiedBytes(logBuffer.buf[:logBuffer.pos]), logBuffer.offset, nil
		}

		// Check previous buffers for the requested offset
		for i, buf := range logBuffer.prevBuffers.buffers {
			if requestedOffset >= buf.startOffset && requestedOffset <= buf.offset {
				// CRITICAL FIX: If the prevBuffer is empty (flushed to disk), read from disk
				if buf.size == 0 {
					glog.V(4).Infof("⚠️  OFFSET-BASED: prevBuffer[%d] [%d-%d] is empty (flushed) - reading from disk for offset %d",
						i, buf.startOffset, buf.offset, requestedOffset)
					return nil, -2, ResumeFromDiskError
				}
				glog.V(0).Infof("✅ OFFSET-BASED: Returning prevBuffer[%d] [%d-%d] for offset %d",
					i, buf.startOffset, buf.offset, requestedOffset)
				return copiedBytes(buf.buf[:buf.size]), buf.offset, nil
			}
		}

		// Offset not found in any buffer
		if requestedOffset < logBuffer.bufferStartOffset {
			// Check if there are any prevBuffers
			if len(logBuffer.prevBuffers.buffers) > 0 {
				firstBuf := logBuffer.prevBuffers.buffers[0]
				if requestedOffset < firstBuf.startOffset {
					glog.V(4).Infof("⚠️  OFFSET-BASED: Requested offset %d < earliest buffer offset %d - data might be on disk",
						requestedOffset, firstBuf.startOffset)
					return nil, -2, ResumeFromDiskError
				}
			}
		}

		if requestedOffset > logBuffer.offset {
			// Future data, not available yet
			glog.V(4).Infof("🔍 DEBUG: Requested offset %d > latest offset %d - returning nil", requestedOffset, logBuffer.offset)
			return nil, logBuffer.offset, nil
		}

		// This shouldn't happen, but log it
		glog.Errorf("⚠️  OFFSET-BASED: Could not find buffer for offset %d (current: [%d-%d])",
			requestedOffset, logBuffer.bufferStartOffset, logBuffer.offset)
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
	var tsBatchIndex int64
	if !logBuffer.startTime.IsZero() {
		tsMemory = logBuffer.startTime
		tsBatchIndex = logBuffer.offset
		glog.V(4).Infof("🔍 DEBUG: Current buffer has data - startTime=%v, offset=%d", tsMemory, tsBatchIndex)
	}
	for i, prevBuf := range logBuffer.prevBuffers.buffers {
		if !prevBuf.startTime.IsZero() && prevBuf.startTime.Before(tsMemory) {
			tsMemory = prevBuf.startTime
			tsBatchIndex = prevBuf.offset
			glog.V(4).Infof("🔍 DEBUG: PrevBuffer[%d] has earlier data - startTime=%v, offset=%d", i, tsMemory, tsBatchIndex)
		}
	}
	if tsMemory.IsZero() { // case 2.2
		glog.V(4).Infof("🔍 DEBUG: No memory data available - returning nil")
		return nil, -2, nil
	} else if lastReadPosition.Time.Before(tsMemory) && lastReadPosition.Offset+1 < tsBatchIndex { // case 2.3
		if !logBuffer.lastFlushDataTime.IsZero() {
			glog.V(4).Infof("🔍 DEBUG: Need to resume from disk - lastFlushDataTime=%v", logBuffer.lastFlushDataTime)
			glog.V(0).Infof("resume with last flush time: %v", logBuffer.lastFlushDataTime)
			return nil, -2, ResumeFromDiskError
		}
	}

	// the following is case 2.1

	if lastReadPosition.Time.Equal(logBuffer.stopTime) {
		glog.V(4).Infof("🔍 DEBUG: lastReadPosition equals stopTime - returning nil")
		return nil, logBuffer.offset, nil
	}
	if lastReadPosition.Time.After(logBuffer.stopTime) {
		glog.V(4).Infof("🔍 DEBUG: lastReadPosition after stopTime - returning nil")
		// glog.Fatalf("unexpected last read time %v, older than latest %v", lastReadPosition, m.stopTime)
		return nil, logBuffer.offset, nil
	}
	if lastReadPosition.Time.Before(logBuffer.startTime) {
		for _, buf := range logBuffer.prevBuffers.buffers {
			if buf.startTime.After(lastReadPosition.Time) {
				// glog.V(4).Infof("%s return the %d sealed buffer %v", m.name, i, buf.startTime)
				return copiedBytes(buf.buf[:buf.size]), buf.offset, nil
			}
			if !buf.startTime.After(lastReadPosition.Time) && buf.stopTime.After(lastReadPosition.Time) {
				pos := buf.locateByTs(lastReadPosition.Time)
				return copiedBytes(buf.buf[pos:buf.size]), buf.offset, nil
			}
		}
		// glog.V(4).Infof("%s return the current buf %v", m.name, lastReadPosition)
		return copiedBytes(logBuffer.buf[:logBuffer.pos]), logBuffer.offset, nil
	}

	lastTs := lastReadPosition.Time.UnixNano()
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
		if t <= lastTs {
			l = mid + 1
		} else if lastTs < t {
			var prevT int64
			if mid > 0 {
				_, prevT = readTs(logBuffer.buf, logBuffer.idx[mid-1])
			}
			if prevT <= lastTs {
				return copiedBytes(logBuffer.buf[pos:logBuffer.pos]), logBuffer.offset, nil
			}
			h = mid
		}
	}

	// FIXME: this could be that the buffer has been flushed already
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
