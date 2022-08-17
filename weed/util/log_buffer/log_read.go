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

func (logBuffer *LogBuffer) LoopProcessLogData(readerName string, startReadTime time.Time, stopTsNs int64,
	waitForDataFn func() bool, eachLogDataFn func(logEntry *filer_pb.LogEntry) error) (lastReadTime time.Time, isDone bool, err error) {
	// loop through all messages
	var bytesBuf *bytes.Buffer
	lastReadTime = startReadTime
	defer func() {
		if bytesBuf != nil {
			logBuffer.ReleaseMemory(bytesBuf)
		}
	}()

	for {

		if bytesBuf != nil {
			logBuffer.ReleaseMemory(bytesBuf)
		}
		bytesBuf, err = logBuffer.ReadFromBuffer(lastReadTime)
		if err == ResumeFromDiskError {
			time.Sleep(1127 * time.Millisecond)
			return lastReadTime, isDone, ResumeFromDiskError
		}
		// glog.V(4).Infof("%s ReadFromBuffer by %v", readerName, lastReadTime)
		if bytesBuf == nil {
			if stopTsNs != 0 {
				isDone = true
				return
			}
			if waitForDataFn() {
				continue
			} else {
				return
			}
		}

		buf := bytesBuf.Bytes()
		// fmt.Printf("ReadFromBuffer %s by %v size %d\n", readerName, lastReadTime, len(buf))

		batchSize := 0

		for pos := 0; pos+4 < len(buf); {

			size := util.BytesToUint32(buf[pos : pos+4])
			if pos+4+int(size) > len(buf) {
				err = ResumeError
				glog.Errorf("LoopProcessLogData: %s read buffer %v read %d [%d,%d) from [0,%d)", readerName, lastReadTime, batchSize, pos, pos+int(size)+4, len(buf))
				return
			}
			entryData := buf[pos+4 : pos+4+int(size)]

			logEntry := &filer_pb.LogEntry{}
			if err = proto.Unmarshal(entryData, logEntry); err != nil {
				glog.Errorf("unexpected unmarshal mq_pb.Message: %v", err)
				pos += 4 + int(size)
				continue
			}
			if stopTsNs != 0 && logEntry.TsNs > stopTsNs {
				isDone = true
				return
			}
			lastReadTime = time.Unix(0, logEntry.TsNs)

			if err = eachLogDataFn(logEntry); err != nil {
				return
			}

			pos += 4 + int(size)
			batchSize++

		}

		// glog.V(4).Infof("%s sent messages ts[%+v,%+v] size %d\n", readerName, startReadTime, lastReadTime, batchSize)
	}

}
