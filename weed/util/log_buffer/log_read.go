package log_buffer

import (
	"bytes"
	"fmt"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

var (
	ResumeError         = fmt.Errorf("resume")
	ResumeFromDiskError = fmt.Errorf("resumeFromDisk")
)

type MessagePosition struct {
	time.Time        // this is the timestamp of the message
	BatchIndex int64 // this is only used when the timestamp is not enough to identify the next message, when the timestamp is in the previous batch.
}

func NewMessagePosition(tsNs int64, batchIndex int64) MessagePosition {
	return MessagePosition{
		Time:       time.Unix(0, tsNs).UTC(),
		BatchIndex: batchIndex,
	}
}

// Sentinel time value to clearly indicate offset-based positioning
// Using Unix timestamp 1 (1970-01-01 00:00:01 UTC) as a clear, recognizable sentinel
var OffsetBasedPositionSentinel = time.Unix(1, 0).UTC()

// NewMessagePositionFromOffset creates a MessagePosition that represents a specific offset
// Uses a clear sentinel time value and stores the offset directly in BatchIndex
func NewMessagePositionFromOffset(offset int64) MessagePosition {
	return MessagePosition{
		Time:       OffsetBasedPositionSentinel, // Clear sentinel time for offset-based positions
		BatchIndex: offset,                      // Store offset directly - much clearer!
	}
}

// IsOffsetBased returns true if this MessagePosition represents an offset rather than a timestamp
func (mp MessagePosition) IsOffsetBased() bool {
	// Offset-based positions use the clear sentinel time value
	return mp.Time.Equal(OffsetBasedPositionSentinel)
}

// GetOffset extracts the offset from an offset-based MessagePosition
func (mp MessagePosition) GetOffset() int64 {
	if !mp.IsOffsetBased() {
		return -1 // Not an offset-based position
	}
	return mp.BatchIndex // Offset is stored directly in BatchIndex
}

func (logBuffer *LogBuffer) LoopProcessLogData(readerName string, startPosition MessagePosition, stopTsNs int64,
	waitForDataFn func() bool, eachLogDataFn EachLogEntryFuncType) (lastReadPosition MessagePosition, isDone bool, err error) {
	// loop through all messages
	var bytesBuf *bytes.Buffer
	var batchIndex int64
	lastReadPosition = startPosition
	var entryCounter int64
	defer func() {
		if bytesBuf != nil {
			logBuffer.ReleaseMemory(bytesBuf)
		}
		// println("LoopProcessLogData", readerName, "sent messages total", entryCounter)
	}()

	for {

		if bytesBuf != nil {
			logBuffer.ReleaseMemory(bytesBuf)
		}
		bytesBuf, batchIndex, err = logBuffer.ReadFromBuffer(lastReadPosition)
		if err == ResumeFromDiskError {
			time.Sleep(1127 * time.Millisecond)
			return lastReadPosition, isDone, ResumeFromDiskError
		}
		readSize := 0
		if bytesBuf != nil {
			readSize = bytesBuf.Len()
		}
		glog.V(4).Infof("%s ReadFromBuffer at %v batch %d. Read bytes %v batch %d", readerName, lastReadPosition, lastReadPosition.BatchIndex, readSize, batchIndex)
		if bytesBuf == nil {
			if batchIndex >= 0 {
				lastReadPosition = NewMessagePosition(lastReadPosition.UnixNano(), batchIndex)
			}
			if stopTsNs != 0 {
				isDone = true
				return
			}
			lastTsNs := logBuffer.LastTsNs.Load()

			for lastTsNs == logBuffer.LastTsNs.Load() {
				if waitForDataFn() {
					continue
				} else {
					isDone = true
					return
				}
			}
			if logBuffer.IsStopping() {
				isDone = true
				return
			}
			continue
		}

		buf := bytesBuf.Bytes()
		// fmt.Printf("ReadFromBuffer %s by %v size %d\n", readerName, lastReadPosition, len(buf))

		batchSize := 0

		for pos := 0; pos+4 < len(buf); {

			size := util.BytesToUint32(buf[pos : pos+4])
			if pos+4+int(size) > len(buf) {
				err = ResumeError
				glog.Errorf("LoopProcessLogData: %s read buffer %v read %d entries [%d,%d) from [0,%d)", readerName, lastReadPosition, batchSize, pos, pos+int(size)+4, len(buf))
				return
			}
			entryData := buf[pos+4 : pos+4+int(size)]

			logEntry := &filer_pb.LogEntry{}
			if err = proto.Unmarshal(entryData, logEntry); err != nil {
				glog.Errorf("unexpected unmarshal mq_pb.Message: %v", err)
				pos += 4 + int(size)
				continue
			}

			// Handle offset-based filtering for offset-based start positions
			if startPosition.IsOffsetBased() {
				startOffset := startPosition.GetOffset()
				glog.V(2).Infof("[DEBUG_FETCH] Checking offset-based position: logEntry.Offset=%d startOffset=%d", logEntry.Offset, startOffset)
				if logEntry.Offset < startOffset {
					glog.V(2).Infof("[DEBUG_FETCH] Skipping entry with offset %d (before startOffset %d)", logEntry.Offset, startOffset)
					// Skip entries before the starting offset
					pos += 4 + int(size)
					batchSize++
					// advance the start position so subsequent entries use timestamp-based flow
					startPosition = NewMessagePosition(logEntry.TsNs, batchIndex)
					continue
				} else {
					glog.V(2).Infof("[DEBUG_FETCH] Found entry with offset %d >= startOffset %d, will process", logEntry.Offset, startOffset)
				}
			}

			if stopTsNs != 0 && logEntry.TsNs > stopTsNs {
				isDone = true
				// println("stopTsNs", stopTsNs, "logEntry.TsNs", logEntry.TsNs)
				return
			}
			lastReadPosition = NewMessagePosition(logEntry.TsNs, batchIndex)

			if isDone, err = eachLogDataFn(logEntry); err != nil {
				glog.Errorf("LoopProcessLogData: %s process log entry %d %v: %v", readerName, batchSize+1, logEntry, err)
				return
			}
			if isDone {
				glog.V(0).Infof("LoopProcessLogData2: %s process log entry %d", readerName, batchSize+1)
				return
			}

			pos += 4 + int(size)
			batchSize++
			entryCounter++

		}

		glog.V(4).Infof("%s sent messages ts[%+v,%+v] size %d\n", readerName, startPosition, lastReadPosition, batchSize)
	}

}

// LoopProcessLogDataWithOffset is similar to LoopProcessLogData but provides offset to the callback
func (logBuffer *LogBuffer) LoopProcessLogDataWithOffset(readerName string, startPosition MessagePosition, stopTsNs int64,
	waitForDataFn func() bool, eachLogDataFn EachLogEntryWithOffsetFuncType) (lastReadPosition MessagePosition, isDone bool, err error) {
	glog.V(0).Infof("🔍 DEBUG: LoopProcessLogDataWithOffset started for %s, startPosition=%v", readerName, startPosition)
	// loop through all messages
	var bytesBuf *bytes.Buffer
	var offset int64
	lastReadPosition = startPosition
	var entryCounter int64
	defer func() {
		if bytesBuf != nil {
			logBuffer.ReleaseMemory(bytesBuf)
		}
		// println("LoopProcessLogDataWithOffset", readerName, "sent messages total", entryCounter)
	}()

	for {

		if bytesBuf != nil {
			logBuffer.ReleaseMemory(bytesBuf)
		}
		bytesBuf, offset, err = logBuffer.ReadFromBuffer(lastReadPosition)
		glog.V(0).Infof("🔍 DEBUG: ReadFromBuffer returned bytesBuf=%v, offset=%d, err=%v", bytesBuf != nil, offset, err)
		if err == ResumeFromDiskError {
			time.Sleep(1127 * time.Millisecond)
			return lastReadPosition, isDone, ResumeFromDiskError
		}
		readSize := 0
		if bytesBuf != nil {
			readSize = bytesBuf.Len()
		}
		glog.V(0).Infof("🔍 DEBUG: %s ReadFromBuffer at %v batch %d. Read bytes %v offset %d", readerName, lastReadPosition, lastReadPosition.BatchIndex, readSize, offset)
		if bytesBuf == nil {
			if offset >= 0 {
				lastReadPosition = NewMessagePosition(lastReadPosition.UnixNano(), offset)
			}
			if stopTsNs != 0 {
				isDone = true
				return
			}
			lastTsNs := logBuffer.LastTsNs.Load()

			for lastTsNs == logBuffer.LastTsNs.Load() {
				if waitForDataFn() {
					continue
				} else {
					isDone = true
					return
				}
			}
			if logBuffer.IsStopping() {
				isDone = true
				return
			}
			continue
		}

		buf := bytesBuf.Bytes()
		// fmt.Printf("ReadFromBuffer %s by %v size %d\n", readerName, lastReadPosition, len(buf))
		glog.V(0).Infof("🔍 DEBUG: Processing buffer with %d bytes for %s", len(buf), readerName)

		batchSize := 0

		for pos := 0; pos+4 < len(buf); {

			size := util.BytesToUint32(buf[pos : pos+4])
			if pos+4+int(size) > len(buf) {
				err = ResumeError
				glog.Errorf("LoopProcessLogDataWithOffset: %s read buffer %v read %d entries [%d,%d) from [0,%d)", readerName, lastReadPosition, batchSize, pos, pos+int(size)+4, len(buf))
				return
			}
			entryData := buf[pos+4 : pos+4+int(size)]

			logEntry := &filer_pb.LogEntry{}
			if err = proto.Unmarshal(entryData, logEntry); err != nil {
				glog.Errorf("unexpected unmarshal mq_pb.Message: %v", err)
				pos += 4 + int(size)
				continue
			}

			glog.V(0).Infof("🔍 DEBUG: Unmarshaled log entry %d: TsNs=%d, Offset=%d, Key=%s", batchSize+1, logEntry.TsNs, logEntry.Offset, string(logEntry.Key))

			// Handle offset-based filtering for offset-based start positions
			if startPosition.IsOffsetBased() {
				startOffset := startPosition.GetOffset()
				glog.V(0).Infof("🔍 DEBUG: Offset-based filtering: logEntry.Offset=%d, startOffset=%d", logEntry.Offset, startOffset)
				if logEntry.Offset < startOffset {
					// Skip entries before the starting offset
					glog.V(0).Infof("🔍 DEBUG: Skipping entry due to offset filter")
					pos += 4 + int(size)
					batchSize++
					continue
				}
			}

			if stopTsNs != 0 && logEntry.TsNs > stopTsNs {
				glog.V(0).Infof("🔍 DEBUG: Stopping due to stopTsNs")
				isDone = true
				// println("stopTsNs", stopTsNs, "logEntry.TsNs", logEntry.TsNs)
				return
			}
			lastReadPosition = NewMessagePosition(logEntry.TsNs, offset)

			glog.V(0).Infof("🔍 DEBUG: Calling eachLogDataFn for entry at offset %d", offset)
			if isDone, err = eachLogDataFn(logEntry, offset); err != nil {
				glog.Errorf("LoopProcessLogDataWithOffset: %s process log entry %d %v: %v", readerName, batchSize+1, logEntry, err)
				return
			}
			if isDone {
				glog.V(0).Infof("LoopProcessLogDataWithOffset: %s process log entry %d", readerName, batchSize+1)
				return
			}

			pos += 4 + int(size)
			batchSize++
			entryCounter++

		}

	}

}
