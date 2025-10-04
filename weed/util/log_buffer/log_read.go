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
	Time          time.Time // timestamp of the message
	Offset        int64     // Kafka offset for offset-based positioning, or batch index for timestamp-based
	IsOffsetBased bool      // true if this position is offset-based, false if timestamp-based
}

func NewMessagePosition(tsNs int64, offset int64) MessagePosition {
	return MessagePosition{
		Time:          time.Unix(0, tsNs).UTC(),
		Offset:        offset,
		IsOffsetBased: false, // timestamp-based by default
	}
}

// NewMessagePositionFromOffset creates a MessagePosition that represents a specific offset
func NewMessagePositionFromOffset(offset int64) MessagePosition {
	return MessagePosition{
		Time:          time.Time{}, // Zero time for offset-based positions
		Offset:        offset,
		IsOffsetBased: true,
	}
}

// GetOffset extracts the offset from an offset-based MessagePosition
func (mp MessagePosition) GetOffset() int64 {
	if !mp.IsOffsetBased {
		return -1 // Not an offset-based position
	}
	return mp.Offset // Offset is stored directly
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
		glog.V(4).Infof("%s ReadFromBuffer at %v offset %d. Read bytes %v batchIndex %d", readerName, lastReadPosition, lastReadPosition.Offset, readSize, batchIndex)
		if bytesBuf == nil {
			if batchIndex >= 0 {
				lastReadPosition = NewMessagePosition(lastReadPosition.Time.UnixNano(), batchIndex)
			}
			if stopTsNs != 0 {
				isDone = true
				return
			}
			lastTsNs := logBuffer.LastTsNs.Load()

			for lastTsNs == logBuffer.LastTsNs.Load() {
				if waitForDataFn() {
					// Sleep to avoid CPU busy-wait if waitForDataFn returns true but no new data yet
					time.Sleep(10 * time.Millisecond)
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
			if startPosition.IsOffsetBased {
				startOffset := startPosition.GetOffset()
				glog.Infof("📍 MEMORY OFFSET CHECK: logEntry.Offset=%d startOffset=%d readerName=%s",
					logEntry.Offset, startOffset, readerName)
				if logEntry.Offset < startOffset {
					glog.Infof("📍 SKIPPING: entry offset %d < startOffset %d", logEntry.Offset, startOffset)
					// Skip entries before the starting offset
					pos += 4 + int(size)
					batchSize++
					continue
				} else {
					glog.Infof("📍 PROCESSING: entry offset %d >= startOffset %d", logEntry.Offset, startOffset)
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
			// OPTIMIZATION: Reduced sleep time from 1127ms to 50ms for faster disk reads
			// The disk read itself will take time, so we don't need a long sleep here
			time.Sleep(50 * time.Millisecond)
			return lastReadPosition, isDone, ResumeFromDiskError
		}
		readSize := 0
		if bytesBuf != nil {
			readSize = bytesBuf.Len()
		}
		glog.V(0).Infof("🔍 DEBUG: %s ReadFromBuffer at %v posOffset %d. Read bytes %v bufferOffset %d", readerName, lastReadPosition, lastReadPosition.Offset, readSize, offset)
		if bytesBuf == nil {
			// CRITICAL: Check if subscription is still active BEFORE waiting
			// This prevents infinite loops when client has disconnected
			if !waitForDataFn() {
				isDone = true
				glog.V(0).Infof("🔍 DEBUG: waitForDataFn returned false, subscription ending")
				return
			}

			if offset >= 0 {
				lastReadPosition = NewMessagePosition(lastReadPosition.Time.UnixNano(), offset)
			}
			if stopTsNs != 0 {
				isDone = true
				return
			}

			// CRITICAL FIX: If we're reading offset-based and there's no data in LogBuffer,
			// return ResumeFromDiskError to let Subscribe try reading from disk again.
			// This prevents infinite blocking when all data is on disk (e.g., after restart).
			if startPosition.IsOffsetBased {
				glog.V(0).Infof("🔍 DEBUG: No data in LogBuffer for offset-based read at %v, returning ResumeFromDiskError", lastReadPosition)
				// OPTIMIZATION: Reduced sleep time from 1127ms to 50ms for faster disk reads
				time.Sleep(50 * time.Millisecond)
				return lastReadPosition, isDone, ResumeFromDiskError
			}

			lastTsNs := logBuffer.LastTsNs.Load()

			for lastTsNs == logBuffer.LastTsNs.Load() {
				if waitForDataFn() {
					// Sleep to avoid CPU busy-wait if waitForDataFn returns true but no new data yet
					time.Sleep(10 * time.Millisecond)
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
			if startPosition.IsOffsetBased {
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
			// CRITICAL FIX: Use logEntry.Offset + 1 to move PAST the current entry
			// This prevents infinite loops where we keep requesting the same offset
			lastReadPosition = NewMessagePosition(logEntry.TsNs, logEntry.Offset+1)

			glog.V(0).Infof("🔍 DEBUG: Calling eachLogDataFn for entry at offset %d, next position will be %d", logEntry.Offset, logEntry.Offset+1)
			if isDone, err = eachLogDataFn(logEntry, logEntry.Offset); err != nil {
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
