package log_buffer

import (
	"bytes"
	"fmt"
	"time"

	"github.com/golang/protobuf/proto"

	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/pb/filer_pb"
	"github.com/chrislusf/seaweedfs/weed/util"
)

var (
	ResumeError         = fmt.Errorf("resume")
	ResumeFromDiskError = fmt.Errorf("resumeFromDisk")
)

func (logBuffer *LogBuffer) LoopProcessLogData(readerName string, startTreadTime time.Time, waitForDataFn func() bool, eachLogDataFn func(logEntry *filer_pb.LogEntry) error) (lastReadTime time.Time, err error) {
	// loop through all messages
	var bytesBuf *bytes.Buffer
	lastReadTime = startTreadTime
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
			return lastReadTime, ResumeFromDiskError
		}
		// glog.V(4).Infof("%s ReadFromBuffer by %v", readerName, lastReadTime)
		if bytesBuf == nil {
			if waitForDataFn() {
				continue
			} else {
				return
			}
		}

		buf := bytesBuf.Bytes()
		// fmt.Printf("ReadFromBuffer %s by %v size %d\n", readerName, lastReadTime, len(buf))

		batchSize := 0
		var startReadTime time.Time

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
				glog.Errorf("unexpected unmarshal messaging_pb.Message: %v", err)
				pos += 4 + int(size)
				continue
			}
			lastReadTime = time.Unix(0, logEntry.TsNs)
			if startReadTime.IsZero() {
				startReadTime = lastReadTime
			}

			if err = eachLogDataFn(logEntry); err != nil {
				return
			}

			pos += 4 + int(size)
			batchSize++

		}

		// glog.V(4).Infof("%s sent messages ts[%+v,%+v] size %d\n", readerName, startReadTime, lastReadTime, batchSize)
	}

}
