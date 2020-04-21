package log_buffer

import (
	"bytes"
	"time"

	"github.com/golang/protobuf/proto"

	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/pb/filer_pb"
	"github.com/chrislusf/seaweedfs/weed/util"
)

func (logBuffer *LogBuffer) LoopProcessLogData(
	startTreadTime time.Time, waitForDataFn func() bool,
	eachLogDataFn func(logEntry *filer_pb.LogEntry) error) (processed int64, err error) {
	// loop through all messages
	var bytesBuf *bytes.Buffer
	lastReadTime := startTreadTime
	defer func() {
		if bytesBuf != nil {
			logBuffer.ReleaseMeory(bytesBuf)
		}
	}()

	for {

		if bytesBuf != nil {
			logBuffer.ReleaseMeory(bytesBuf)
		}
		bytesBuf = logBuffer.ReadFromBuffer(lastReadTime)
		if bytesBuf == nil {
			if waitForDataFn() {
				continue
			} else {
				return
			}
		}

		buf := bytesBuf.Bytes()

		batchSize := 0
		var startReadTime time.Time

		for pos := 0; pos+4 < len(buf); {

			size := util.BytesToUint32(buf[pos : pos+4])
			entryData := buf[pos+4 : pos+4+int(size)]

			// fmt.Printf("read buffer read %d [%d,%d) from [0,%d)\n", batchSize, pos, pos+int(size)+4, len(buf))

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
			processed++
		}

		// fmt.Printf("sent message ts[%d,%d] size %d\n", startReadTime.UnixNano(), lastReadTime.UnixNano(), batchSize)
	}

}
