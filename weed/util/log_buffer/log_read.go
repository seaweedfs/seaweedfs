package log_buffer

import (
	"bytes"
	"errors"
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

// notificationHealthCheckInterval bounds how long an idle subscriber blocks
// on the notification channel before re-checking state (client disconnect via
// waitForDataFn, LogBuffer shutdown, timestamp advancement). notifyChan is the
// primary wakeup path when new data arrives or a flush lands; this timeout is
// the safety net for any missed notification and also caps the latency to
// notice that the subscriber should exit. Balances idle CPU and log noise
// against client-disconnect detection latency.
const notificationHealthCheckInterval = 250 * time.Millisecond

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

// awaitNotificationOrTimeout blocks until one of:
//   - a new-data / flush notification arrives on notifyChan (returns true)
//   - the LogBuffer is shut down via ShutdownLogBuffer (returns true; callers
//     re-check IsStopping() and exit)
//   - notificationHealthCheckInterval elapses (returns false; caller
//     re-checks client-disconnect and other state)
func (logBuffer *LogBuffer) awaitNotificationOrTimeout(notifyChan <-chan struct{}) bool {
	timer := time.NewTimer(notificationHealthCheckInterval)
	defer timer.Stop()

	select {
	case <-notifyChan:
		return true
	case <-logBuffer.shutdownCh:
		return true
	case <-timer.C:
		return false
	}
}

func (logBuffer *LogBuffer) LoopProcessLogData(readerName string, startPosition MessagePosition, stopTsNs int64,
	waitForDataFn func() bool, eachLogDataFn EachLogEntryFuncType) (lastReadPosition MessagePosition, isDone bool, err error) {

	// Register for instant notifications (<1ms latency)
	notifyChan := logBuffer.RegisterSubscriber(readerName)
	defer logBuffer.UnregisterSubscriber(readerName)

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
			// Try to read from disk if readFromDiskFn is available
			if logBuffer.ReadFromDiskFn != nil {
				prevReadPosition := lastReadPosition
				lastReadPosition, isDone, err = logBuffer.ReadFromDiskFn(lastReadPosition, stopTsNs, eachLogDataFn)
				if err != nil {
					return lastReadPosition, isDone, err
				}
				if isDone {
					return lastReadPosition, isDone, nil
				}
				if lastReadPosition != prevReadPosition {
					continue
				}
			} else if logBuffer.HasData() {
				return lastReadPosition, isDone, ResumeFromDiskError
			}

			// CRITICAL: Check if client is still connected
			if !waitForDataFn() {
				// Client disconnected - exit cleanly
				glog.V(4).Infof("%s: Client disconnected after disk read attempt", readerName)
				return lastReadPosition, true, nil
			}

			// Wait for notification or timeout (instant wake-up when data arrives)
			if logBuffer.awaitNotificationOrTimeout(notifyChan) {
				glog.V(3).Infof("%s: Woke up from notification after ResumeFromDiskError", readerName)
			} else {
				glog.V(4).Infof("%s: Notification timeout after ResumeFromDiskError, rechecking state", readerName)
			}

			// Continue to next iteration (don't return ResumeFromDiskError)
			continue
		}
		if err != nil {
			// Check for buffer corruption error
			if errors.Is(err, ErrBufferCorrupted) {
				glog.Errorf("%s: Buffer corruption detected: %v", readerName, err)
				return lastReadPosition, true, fmt.Errorf("buffer corruption: %w", err)
			}
			// Other errors
			glog.Errorf("%s: ReadFromBuffer error: %v", readerName, err)
			return lastReadPosition, true, err
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
				if !waitForDataFn() {
					isDone = true
					return
				}
				// Wait for notification or timeout (instant wake-up when data arrives)
				if logBuffer.awaitNotificationOrTimeout(notifyChan) {
					glog.V(3).Infof("%s: Woke up from notification (LoopProcessLogData)", readerName)
				} else if lastTsNs != logBuffer.LastTsNs.Load() {
					break
				} else {
					glog.V(4).Infof("%s: Notification timeout (LoopProcessLogData), rechecking state", readerName)
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
				if logEntry.Offset < startOffset {
					// Skip entries before the starting offset
					pos += 4 + int(size)
					batchSize++
					continue
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
	glog.V(4).Infof("LoopProcessLogDataWithOffset started for %s, startPosition=%v", readerName, startPosition)

	// Register for instant notifications (<1ms latency)
	notifyChan := logBuffer.RegisterSubscriber(readerName)
	defer logBuffer.UnregisterSubscriber(readerName)

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
		// Check stopTsNs at the beginning of each iteration
		// This ensures we exit immediately if the stop time is in the past
		if stopTsNs != 0 && time.Now().UnixNano() > stopTsNs {
			isDone = true
			return
		}

		if bytesBuf != nil {
			logBuffer.ReleaseMemory(bytesBuf)
		}
		bytesBuf, offset, err = logBuffer.ReadFromBuffer(lastReadPosition)
		glog.V(4).Infof("ReadFromBuffer for %s returned bytesBuf=%v, offset=%d, err=%v", readerName, bytesBuf != nil, offset, err)

		// Check for buffer corruption error before other error handling
		if err != nil && errors.Is(err, ErrBufferCorrupted) {
			glog.Errorf("%s: Buffer corruption detected: %v", readerName, err)
			return lastReadPosition, true, fmt.Errorf("buffer corruption: %w", err)
		}

		if err == ResumeFromDiskError {
			// Try to read from disk if readFromDiskFn is available
			if logBuffer.ReadFromDiskFn != nil {
				prevReadPosition := lastReadPosition
				// Wrap eachLogDataFn to match the expected signature
				diskReadFn := func(logEntry *filer_pb.LogEntry) (bool, error) {
					return eachLogDataFn(logEntry, logEntry.Offset)
				}
				lastReadPosition, isDone, err = logBuffer.ReadFromDiskFn(lastReadPosition, stopTsNs, diskReadFn)
				if err != nil {
					return lastReadPosition, isDone, err
				}
				if isDone {
					return lastReadPosition, isDone, nil
				}
				if lastReadPosition != prevReadPosition {
					continue
				}
			} else if logBuffer.HasData() {
				return lastReadPosition, isDone, ResumeFromDiskError
			}

			// CRITICAL: Check if client is still connected after disk read
			if !waitForDataFn() {
				// Client disconnected - exit cleanly
				glog.V(4).Infof("%s: Client disconnected after disk read", readerName)
				return lastReadPosition, true, nil
			}

			// Wait for notification or timeout (instant wake-up when data arrives)
			if logBuffer.awaitNotificationOrTimeout(notifyChan) {
				glog.V(3).Infof("%s: Woke up from notification after disk read", readerName)
			} else {
				glog.V(4).Infof("%s: Notification timeout, rechecking state", readerName)
			}

			// Continue to next iteration (don't return ResumeFromDiskError)
			continue
		}
		readSize := 0
		if bytesBuf != nil {
			readSize = bytesBuf.Len()
		}
		glog.V(4).Infof("%s ReadFromBuffer at %v posOffset %d. Read bytes %v bufferOffset %d", readerName, lastReadPosition, lastReadPosition.Offset, readSize, offset)
		if bytesBuf == nil {
			// CRITICAL: Check if subscription is still active BEFORE waiting
			// This prevents infinite loops when client has disconnected
			if !waitForDataFn() {
				glog.V(4).Infof("%s: waitForDataFn returned false, subscription ending", readerName)
				return lastReadPosition, true, nil
			}

			if offset >= 0 {
				lastReadPosition = NewMessagePosition(lastReadPosition.Time.UnixNano(), offset)
			}
			if stopTsNs != 0 {
				isDone = true
				return
			}

			// If we're reading offset-based and there's no data in LogBuffer,
			// return ResumeFromDiskError to let Subscribe try reading from disk again.
			// This prevents infinite blocking when all data is on disk (e.g., after restart).
			if startPosition.IsOffsetBased {
				glog.V(4).Infof("%s: No data in LogBuffer for offset-based read at %v, checking if client still connected", readerName, lastReadPosition)
				// Check if client is still connected before busy-looping
				if !waitForDataFn() {
					glog.V(4).Infof("%s: Client disconnected, stopping offset-based read", readerName)
					return lastReadPosition, true, nil
				}
				// Wait for notification or timeout (instant wake-up when data arrives)
				if logBuffer.awaitNotificationOrTimeout(notifyChan) {
					glog.V(3).Infof("%s: Woke up from notification for offset-based read", readerName)
				} else {
					glog.V(4).Infof("%s: Notification timeout for offset-based, rechecking state", readerName)
				}
				return lastReadPosition, isDone, ResumeFromDiskError
			}

			lastTsNs := logBuffer.LastTsNs.Load()

			for lastTsNs == logBuffer.LastTsNs.Load() {
				if !waitForDataFn() {
					glog.V(4).Infof("%s: Client disconnected during timestamp wait", readerName)
					return lastReadPosition, true, nil
				}
				// Wait for notification or timeout (instant wake-up when data arrives)
				if logBuffer.awaitNotificationOrTimeout(notifyChan) {
					glog.V(3).Infof("%s: Woke up from notification (main loop)", readerName)
				} else if lastTsNs != logBuffer.LastTsNs.Load() {
					break
				} else {
					glog.V(4).Infof("%s: Notification timeout (main loop), rechecking state", readerName)
				}
			}
			if logBuffer.IsStopping() {
				glog.V(4).Infof("%s: LogBuffer is stopping", readerName)
				return lastReadPosition, true, nil
			}
			continue
		}

		buf := bytesBuf.Bytes()
		// fmt.Printf("ReadFromBuffer %s by %v size %d\n", readerName, lastReadPosition, len(buf))
		glog.V(4).Infof("Processing buffer with %d bytes for %s", len(buf), readerName)

		// If buffer is empty, check if client is still connected before looping
		if len(buf) == 0 {
			glog.V(4).Infof("Empty buffer for %s, checking if client still connected", readerName)
			if !waitForDataFn() {
				glog.V(4).Infof("%s: Client disconnected on empty buffer", readerName)
				return lastReadPosition, true, nil
			}
			if logBuffer.awaitNotificationOrTimeout(notifyChan) {
				glog.V(3).Infof("%s: Woke up from notification on empty buffer", readerName)
			} else {
				glog.V(4).Infof("%s: Empty buffer timeout, rechecking state", readerName)
			}
			continue
		}

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

			glog.V(4).Infof("Unmarshaled log entry %d: TsNs=%d, Offset=%d, Key=%s", batchSize+1, logEntry.TsNs, logEntry.Offset, string(logEntry.Key))

			// Handle offset-based filtering for offset-based start positions
			if startPosition.IsOffsetBased {
				startOffset := startPosition.GetOffset()
				glog.V(4).Infof("Offset-based filtering: logEntry.Offset=%d, startOffset=%d", logEntry.Offset, startOffset)
				if logEntry.Offset < startOffset {
					// Skip entries before the starting offset
					glog.V(4).Infof("Skipping entry due to offset filter")
					pos += 4 + int(size)
					batchSize++
					continue
				}
			}

			if stopTsNs != 0 && logEntry.TsNs > stopTsNs {
				glog.V(4).Infof("Stopping due to stopTsNs")
				isDone = true
				// println("stopTsNs", stopTsNs, "logEntry.TsNs", logEntry.TsNs)
				return
			}
			// Use logEntry.Offset + 1 to move PAST the current entry
			// This prevents infinite loops where we keep requesting the same offset
			lastReadPosition = NewMessagePosition(logEntry.TsNs, logEntry.Offset+1)

			glog.V(4).Infof("Calling eachLogDataFn for entry at offset %d, next position will be %d", logEntry.Offset, logEntry.Offset+1)
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
