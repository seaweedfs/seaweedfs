package logstore

import (
	"encoding/binary"
	"fmt"
	"github.com/parquet-go/parquet-go"
	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/mq/schema"
	"github.com/seaweedfs/seaweedfs/weed/mq/topic"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/mq_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/schema_pb"
	"github.com/seaweedfs/seaweedfs/weed/util/chunk_cache"
	"github.com/seaweedfs/seaweedfs/weed/util/log_buffer"
	jsonpb "google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"io"
	"math"
	"strings"
	"time"
)

var (
	chunkCache = chunk_cache.NewTieredChunkCache(256, ".", 1024, 1024*1024) // 1GB on disk cache
)

func GenParquetReadFunc(filerClient filer_pb.FilerClient, t topic.Topic, partition *mq_pb.Partition) log_buffer.LogReadFromDiskFuncType {
	topicDir := fmt.Sprintf("%s/%s/%s", filer.TopicsDir, t.Namespace, t.Name)
	partitionGeneration := time.Unix(0, partition.UnixTimeNs).UTC().Format(topic.TIME_FORMAT)
	partitionDir := fmt.Sprintf("%s/%s/%04d-%04d", topicDir, partitionGeneration, partition.RangeStart, partition.RangeStop)

	lookupFileIdFn := filer.LookupFn(filerClient)

	// read topic conf from filer
	var topicConf *mq_pb.ConfigureTopicResponse
	if err := filerClient.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		data, err := filer.ReadInsideFiler(client, topicDir, "topic.conf")
		if err != nil {
			return err
		}
		topicConf = &mq_pb.ConfigureTopicResponse{}
		if err = jsonpb.Unmarshal(data, topicConf); err != nil {
			return fmt.Errorf("unmarshal topic %v conf: %v", t, err)
		}
		return nil
	}); err != nil {
		return nil
	}
	recordType := topicConf.GetRecordType()
	recordType.Fields = append(recordType.Fields, &schema_pb.Field{
		Name: TS_COLUMN_NAME,
		Type: &schema_pb.Type{Kind: &schema_pb.Type_ScalarType{
			ScalarType: schema_pb.ScalarType_INT64,
		}},
	})
	parquetSchema, err := schema.ToParquetSchema(t.Name, recordType)
	if err != nil {
		return nil
	}
	parquetLevels, err := schema.ToParquetLevels(recordType)
	if err != nil {
		return nil
	}

	// eachFileFn reads a parquet file and calls eachLogEntryFn for each log entry
	eachFileFn := func(entry *filer_pb.Entry, eachLogEntryFn log_buffer.EachLogEntryFuncType, starTsNs, stopTsNs int64) (processedTsNs int64, err error) {
		// create readerAt for the parquet file
		fileSize := filer.FileSize(entry)
		visibleIntervals, _ := filer.NonOverlappingVisibleIntervals(lookupFileIdFn, entry.Chunks, 0, int64(fileSize))
		chunkViews := filer.ViewFromVisibleIntervals(visibleIntervals, 0, int64(fileSize))
		readerCache := filer.NewReaderCache(32, chunkCache, lookupFileIdFn)
		readerAt := filer.NewChunkReaderAtFromClient(readerCache, chunkViews, int64(fileSize))

		// create parquet reader
		parquetReader := parquet.NewReader(readerAt, parquetSchema)
		rows := make([]parquet.Row, 128)
		for {
			rowCount, readErr := parquetReader.ReadRows(rows)

			for i := 0; i < rowCount; i++ {
				row := rows[i]
				// convert parquet row to schema_pb.RecordValue
				recordValue, err := schema.ToRecordValue(recordType, parquetLevels, row)
				if err != nil {
					return processedTsNs, fmt.Errorf("ToRecordValue failed: %v", err)
				}
				processedTsNs = recordValue.Fields[TS_COLUMN_NAME].GetInt64Value()
				if processedTsNs < starTsNs {
					continue
				}
				if stopTsNs != 0 && processedTsNs >= stopTsNs {
					return processedTsNs, nil
				}

				data, marshalErr := proto.Marshal(recordValue)
				if marshalErr != nil {
					return processedTsNs, fmt.Errorf("marshal record value: %v", marshalErr)
				}

				logEntry := &filer_pb.LogEntry{
					TsNs: processedTsNs,
					Data: data,
				}

				if _, err = eachLogEntryFn(logEntry); err != nil {
					return processedTsNs, fmt.Errorf("process log entry %v: %v", logEntry, err)
				}
			}

			if readErr != nil {
				if readErr == io.EOF {
					return processedTsNs, nil
				}
				return processedTsNs, readErr
			}
		}
		return
	}

	return func(startPosition log_buffer.MessagePosition, stopTsNs int64, eachLogEntryFn log_buffer.EachLogEntryFuncType) (lastReadPosition log_buffer.MessagePosition, isDone bool, err error) {
		startFileName := startPosition.UTC().Format(topic.TIME_FORMAT)
		startTsNs := startPosition.Time.UnixNano()
		var processedTsNs int64

		err = filerClient.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {

			return filer_pb.SeaweedList(client, partitionDir, "", func(entry *filer_pb.Entry, isLast bool) error {
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
				minTsBytes := entry.Extended["min"]
				if len(minTsBytes) != 8 {
					return nil
				}
				minTsNs := int64(binary.BigEndian.Uint64(minTsBytes))

				// read max ts
				maxTsBytes := entry.Extended["max"]
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
