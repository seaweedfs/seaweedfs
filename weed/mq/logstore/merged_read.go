package logstore

import (
	"github.com/seaweedfs/seaweedfs/weed/mq/topic"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/mq_pb"
	"github.com/seaweedfs/seaweedfs/weed/util/log_buffer"
)

func GenMergedReadFunc(filerClient filer_pb.FilerClient, t topic.Topic, partition *mq_pb.Partition) log_buffer.LogReadFromDiskFuncType {
	fromParquetFn := GenParquetReadFunc(filerClient, t, partition)
	readLogDirectFn := GenLogOnDiskReadFunc(filerClient, t, partition)
	return mergeReadFuncs(fromParquetFn, readLogDirectFn)
}

func mergeReadFuncs(fromParquetFn, readLogDirectFn log_buffer.LogReadFromDiskFuncType) log_buffer.LogReadFromDiskFuncType {
	var exhaustedParquet bool
	return func(startPosition log_buffer.MessagePosition, stopTsNs int64, eachLogEntryFn log_buffer.EachLogEntryFuncType) (lastReadPosition log_buffer.MessagePosition, isDone bool, err error) {
		if !exhaustedParquet {
			lastReadPosition, isDone, err = fromParquetFn(startPosition, stopTsNs, eachLogEntryFn)
			if isDone {
				exhaustedParquet = true
				isDone = false
			}
			if err != nil {
				return
			}
		}

		lastReadPosition, isDone, err = readLogDirectFn(startPosition, stopTsNs, eachLogEntryFn)
		return
	}
}
