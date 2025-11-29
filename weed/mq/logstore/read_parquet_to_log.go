package logstore

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"strings"

	"github.com/parquet-go/parquet-go"
	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/mq"
	"github.com/seaweedfs/seaweedfs/weed/mq/schema"
	"github.com/seaweedfs/seaweedfs/weed/mq/topic"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/mq_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/schema_pb"
	"github.com/seaweedfs/seaweedfs/weed/util/chunk_cache"
	"github.com/seaweedfs/seaweedfs/weed/util/log_buffer"
	"google.golang.org/protobuf/proto"
)

var (
	chunkCache = chunk_cache.NewChunkCacheInMemory(256) // 256 entries, 8MB max per entry
)

// isControlEntry checks if a log entry is a control entry without actual data
// Based on MQ system analysis, control entries are:
// 1. DataMessages with populated Ctrl field (publisher close signals)
// 2. Entries with empty keys (as filtered by subscriber)
// 3. Entries with no data
func isControlEntry(logEntry *filer_pb.LogEntry) bool {
	// Skip entries with no data
	if len(logEntry.Data) == 0 {
		return true
	}

	// Skip entries with empty keys (same logic as subscriber)
	if len(logEntry.Key) == 0 {
		return true
	}

	// Check if this is a DataMessage with control field populated
	dataMessage := &mq_pb.DataMessage{}
	if err := proto.Unmarshal(logEntry.Data, dataMessage); err == nil {
		// If it has a control field, it's a control message
		if dataMessage.Ctrl != nil {
			return true
		}
	}

	return false
}

func GenParquetReadFunc(filerClient filer_pb.FilerClient, t topic.Topic, p topic.Partition) log_buffer.LogReadFromDiskFuncType {
	partitionDir := topic.PartitionDir(t, p)

	lookupFileIdFn := filer.LookupFn(filerClient)

	// read topic conf from filer
	var topicConf *mq_pb.ConfigureTopicResponse
	var err error
	if err := filerClient.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		topicConf, err = t.ReadConfFile(client)
		return err
	}); err != nil {
		// Return a no-op function for test environments or when topic config can't be read
		return func(startPosition log_buffer.MessagePosition, stopTsNs int64, eachLogEntryFn log_buffer.EachLogEntryFuncType) (log_buffer.MessagePosition, bool, error) {
			return startPosition, true, nil
		}
	}
	// Get schema - prefer flat schema if available
	var recordType *schema_pb.RecordType
	if topicConf.GetMessageRecordType() != nil {
		// New flat schema format - use directly
		recordType = topicConf.GetMessageRecordType()
	}

	if recordType == nil || len(recordType.Fields) == 0 {
		// Return a no-op function if no schema is available
		return func(startPosition log_buffer.MessagePosition, stopTsNs int64, eachLogEntryFn log_buffer.EachLogEntryFuncType) (log_buffer.MessagePosition, bool, error) {
			return startPosition, true, nil
		}
	}
	recordType = schema.NewRecordTypeBuilder(recordType).
		WithField(SW_COLUMN_NAME_TS, schema.TypeInt64).
		WithField(SW_COLUMN_NAME_KEY, schema.TypeBytes).
		WithField(SW_COLUMN_NAME_OFFSET, schema.TypeInt64).
		RecordTypeEnd()

	parquetLevels, err := schema.ToParquetLevels(recordType)
	if err != nil {
		return nil
	}

	// eachFileFn reads a parquet file and calls eachLogEntryFn for each log entry
	eachFileFn := func(entry *filer_pb.Entry, eachLogEntryFn log_buffer.EachLogEntryFuncType, starTsNs, stopTsNs int64) (processedTsNs int64, err error) {
		// create readerAt for the parquet file
		fileSize := filer.FileSize(entry)
		visibleIntervals, _ := filer.NonOverlappingVisibleIntervals(context.Background(), lookupFileIdFn, entry.Chunks, 0, int64(fileSize))
		chunkViews := filer.ViewFromVisibleIntervals(visibleIntervals, 0, int64(fileSize))
		readerCache := filer.NewReaderCache(32, chunkCache, lookupFileIdFn)
		readerAt := filer.NewChunkReaderAtFromClient(context.Background(), readerCache, chunkViews, int64(fileSize), filer.DefaultPrefetchCount)

		// create parquet reader
		parquetReader := parquet.NewReader(readerAt)
		rows := make([]parquet.Row, 128)
		for {
			rowCount, readErr := parquetReader.ReadRows(rows)

			// Process the rows first, even if EOF is returned
			for i := 0; i < rowCount; i++ {
				row := rows[i]
				// convert parquet row to schema_pb.RecordValue
				recordValue, err := schema.ToRecordValue(recordType, parquetLevels, row)
				if err != nil {
					return processedTsNs, fmt.Errorf("ToRecordValue failed: %w", err)
				}
				processedTsNs = recordValue.Fields[SW_COLUMN_NAME_TS].GetInt64Value()
				if processedTsNs <= starTsNs {
					continue
				}
				if stopTsNs != 0 && processedTsNs >= stopTsNs {
					return processedTsNs, nil
				}

				data, marshalErr := proto.Marshal(recordValue)
				if marshalErr != nil {
					return processedTsNs, fmt.Errorf("marshal record value: %w", marshalErr)
				}

				// Get offset from parquet, default to 0 if not present (backward compatibility)
				var offset int64 = 0
				if offsetValue, exists := recordValue.Fields[SW_COLUMN_NAME_OFFSET]; exists {
					offset = offsetValue.GetInt64Value()
				}

				logEntry := &filer_pb.LogEntry{
					Key:    recordValue.Fields[SW_COLUMN_NAME_KEY].GetBytesValue(),
					TsNs:   processedTsNs,
					Data:   data,
					Offset: offset,
				}

				// Skip control entries without actual data
				if isControlEntry(logEntry) {
					continue
				}

				// fmt.Printf(" parquet entry %s ts %v\n", string(logEntry.Key), time.Unix(0, logEntry.TsNs).UTC())

				if _, err = eachLogEntryFn(logEntry); err != nil {
					return processedTsNs, fmt.Errorf("process log entry %v: %w", logEntry, err)
				}
			}

			// Check for end conditions after processing rows
			if readErr != nil {
				if readErr == io.EOF {
					return processedTsNs, nil
				}
				return processedTsNs, readErr
			}
			if rowCount == 0 {
				return processedTsNs, nil
			}
		}
	}

	return func(startPosition log_buffer.MessagePosition, stopTsNs int64, eachLogEntryFn log_buffer.EachLogEntryFuncType) (lastReadPosition log_buffer.MessagePosition, isDone bool, err error) {
		startFileName := startPosition.Time.UTC().Format(topic.TIME_FORMAT)
		startTsNs := startPosition.Time.UnixNano()
		var processedTsNs int64

		err = filerClient.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {

			return filer_pb.SeaweedList(context.Background(), client, partitionDir, "", func(entry *filer_pb.Entry, isLast bool) error {
				if entry.IsDirectory {
					return nil
				}
				if !strings.HasSuffix(entry.Name, ".parquet") {
					return nil
				}
				if len(entry.Extended) == 0 {
					return nil
				}

				// read minTs from the parquet file
				minTsBytes := entry.Extended[mq.ExtendedAttrTimestampMin]
				if len(minTsBytes) != 8 {
					return nil
				}
				minTsNs := int64(binary.BigEndian.Uint64(minTsBytes))

				// read max ts
				maxTsBytes := entry.Extended[mq.ExtendedAttrTimestampMax]
				if len(maxTsBytes) != 8 {
					return nil
				}
				maxTsNs := int64(binary.BigEndian.Uint64(maxTsBytes))

				if stopTsNs != 0 && stopTsNs <= minTsNs {
					isDone = true
					return nil
				}

				if maxTsNs < startTsNs {
					return nil
				}

				if processedTsNs, err = eachFileFn(entry, eachLogEntryFn, startTsNs, stopTsNs); err != nil {
					return err
				}
				return nil

			}, startFileName, true, math.MaxInt32)
		})
		lastReadPosition = log_buffer.NewMessagePosition(processedTsNs, -2)
		return
	}
}
