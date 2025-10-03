package logstore

import (
	"github.com/seaweedfs/seaweedfs/weed/mq/topic"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util/log_buffer"
)

func GenMergedReadFunc(filerClient filer_pb.FilerClient, t topic.Topic, p topic.Partition) log_buffer.LogReadFromDiskFuncType {
	fromParquetFn := GenParquetReadFunc(filerClient, t, p)
	readLogDirectFn := GenLogOnDiskReadFunc(filerClient, t, p)
	// Reversed order: live logs first (recent), then Parquet files (historical)
	// This provides better performance for real-time analytics queries
	return mergeReadFuncs(readLogDirectFn, fromParquetFn)
}

func mergeReadFuncs(readLogDirectFn, fromParquetFn log_buffer.LogReadFromDiskFuncType) log_buffer.LogReadFromDiskFuncType {
	var exhaustedLiveLogs bool
	var lastProcessedPosition log_buffer.MessagePosition
	return func(startPosition log_buffer.MessagePosition, stopTsNs int64, eachLogEntryFn log_buffer.EachLogEntryFuncType) (lastReadPosition log_buffer.MessagePosition, isDone bool, err error) {
		if !exhaustedLiveLogs {
			lastReadPosition, isDone, err = readLogDirectFn(startPosition, stopTsNs, eachLogEntryFn)
			if isDone {
				// For very early timestamps (like timestamp=1 for RESET_TO_EARLIEST),
				// we want to continue to read from in-memory data
				isDone = false
			}
			if err != nil {
				return
			}
			lastProcessedPosition = lastReadPosition
		}
		exhaustedLiveLogs = true

		if startPosition.Time.Before(lastProcessedPosition.Time) {
			startPosition = lastProcessedPosition
		}

		lastReadPosition, isDone, err = fromParquetFn(startPosition, stopTsNs, eachLogEntryFn)

		if isDone {
			// For very early timestamps (like timestamp=1 for RESET_TO_EARLIEST),
			// parquet files won't exist, but we want to continue to in-memory data reading
			isDone = false
		}

		return
	}
}
