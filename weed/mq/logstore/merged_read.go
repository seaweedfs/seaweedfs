package logstore

import (
	"github.com/seaweedfs/seaweedfs/weed/mq/topic"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util/log_buffer"
)

func GenMergedReadFunc(filerClient filer_pb.FilerClient, t topic.Topic, p topic.Partition) log_buffer.LogReadFromDiskFuncType {
	fromParquetFn := GenParquetReadFunc(filerClient, t, p)
	readLogDirectFn := GenLogOnDiskReadFunc(filerClient, t, p)
	return mergeReadFuncs(fromParquetFn, readLogDirectFn)
}

func mergeReadFuncs(fromParquetFn, readLogDirectFn log_buffer.LogReadFromDiskFuncType) log_buffer.LogReadFromDiskFuncType {
	var exhaustedParquet bool
	var lastProcessedPosition log_buffer.MessagePosition
	return func(startPosition log_buffer.MessagePosition, stopTsNs int64, eachLogEntryFn log_buffer.EachLogEntryFuncType) (lastReadPosition log_buffer.MessagePosition, isDone bool, err error) {
		if !exhaustedParquet {
			// glog.V(4).Infof("reading from parquet startPosition: %v\n", startPosition.UTC())
			lastReadPosition, isDone, err = fromParquetFn(startPosition, stopTsNs, eachLogEntryFn)
			// glog.V(4).Infof("read from parquet: %v %v %v %v\n", startPosition, lastReadPosition, isDone, err)
			if isDone {
				isDone = false
			}
			if err != nil {
				return
			}
			lastProcessedPosition = lastReadPosition
		}
		exhaustedParquet = true

		if startPosition.Before(lastProcessedPosition.Time) {
			startPosition = lastProcessedPosition
		}

		// glog.V(4).Infof("reading from direct log startPosition: %v\n", startPosition.UTC())
		lastReadPosition, isDone, err = readLogDirectFn(startPosition, stopTsNs, eachLogEntryFn)
		return
	}
}
