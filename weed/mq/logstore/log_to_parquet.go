package logstore

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/compress/zstd"
	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/mq"
	"github.com/seaweedfs/seaweedfs/weed/mq/schema"
	"github.com/seaweedfs/seaweedfs/weed/mq/topic"
	"github.com/seaweedfs/seaweedfs/weed/operation"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/schema_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
	util_http "github.com/seaweedfs/seaweedfs/weed/util/http"
	"github.com/seaweedfs/seaweedfs/weed/util/log_buffer"
	"google.golang.org/protobuf/proto"
)

const (
	SW_COLUMN_NAME_TS     = "_ts_ns"
	SW_COLUMN_NAME_KEY    = "_key"
	SW_COLUMN_NAME_OFFSET = "_offset"
	SW_COLUMN_NAME_VALUE  = "_value"
)

func CompactTopicPartitions(filerClient filer_pb.FilerClient, t topic.Topic, timeAgo time.Duration, recordType *schema_pb.RecordType, preference *operation.StoragePreference) error {
	// list the topic partition versions
	topicVersions, err := collectTopicVersions(filerClient, t, timeAgo)
	if err != nil {
		return fmt.Errorf("list topic files: %w", err)
	}

	// compact the partitions
	for _, topicVersion := range topicVersions {
		partitions, err := collectTopicVersionsPartitions(filerClient, t, topicVersion)
		if err != nil {
			return fmt.Errorf("list partitions %s/%s/%s: %v", t.Namespace, t.Name, topicVersion, err)
		}
		for _, partition := range partitions {
			err := compactTopicPartition(filerClient, t, timeAgo, recordType, partition, preference)
			if err != nil {
				return fmt.Errorf("compact partition %s/%s/%s/%s: %v", t.Namespace, t.Name, topicVersion, partition, err)
			}
		}
	}
	return nil
}

func collectTopicVersions(filerClient filer_pb.FilerClient, t topic.Topic, timeAgo time.Duration) (partitionVersions []time.Time, err error) {
	err = filer_pb.ReadDirAllEntries(context.Background(), filerClient, util.FullPath(t.Dir()), "", func(entry *filer_pb.Entry, isLast bool) error {
		t, err := topic.ParseTopicVersion(entry.Name)
		if err != nil {
			// skip non-partition directories
			return nil
		}
		if t.Unix() < time.Now().Unix()-int64(timeAgo/time.Second) {
			partitionVersions = append(partitionVersions, t)
		}
		return nil
	})
	return
}

func collectTopicVersionsPartitions(filerClient filer_pb.FilerClient, t topic.Topic, topicVersion time.Time) (partitions []topic.Partition, err error) {
	version := topicVersion.Format(topic.PartitionGenerationFormat)
	err = filer_pb.ReadDirAllEntries(context.Background(), filerClient, util.FullPath(t.Dir()).Child(version), "", func(entry *filer_pb.Entry, isLast bool) error {
		if !entry.IsDirectory {
			return nil
		}
		start, stop := topic.ParsePartitionBoundary(entry.Name)
		if start != stop {
			partitions = append(partitions, topic.Partition{
				RangeStart: start,
				RangeStop:  stop,
				RingSize:   topic.PartitionCount,
				UnixTimeNs: topicVersion.UnixNano(),
			})
		}
		return nil
	})
	return
}

func compactTopicPartition(filerClient filer_pb.FilerClient, t topic.Topic, timeAgo time.Duration, recordType *schema_pb.RecordType, partition topic.Partition, preference *operation.StoragePreference) error {
	partitionDir := topic.PartitionDir(t, partition)

	// compact the partition directory
	return compactTopicPartitionDir(filerClient, t.Name, partitionDir, timeAgo, recordType, preference)
}

func compactTopicPartitionDir(filerClient filer_pb.FilerClient, topicName, partitionDir string, timeAgo time.Duration, recordType *schema_pb.RecordType, preference *operation.StoragePreference) error {
	// read all existing parquet files
	minTsNs, maxTsNs, err := readAllParquetFiles(filerClient, partitionDir)
	if err != nil {
		return err
	}

	// read all log files
	logFiles, err := readAllLogFiles(filerClient, partitionDir, timeAgo, minTsNs, maxTsNs)
	if err != nil {
		return err
	}
	if len(logFiles) == 0 {
		return nil
	}

	// divide log files into groups of 128MB
	logFileGroups := groupFilesBySize(logFiles, 128*1024*1024)

	// write to parquet file
	parquetLevels, err := schema.ToParquetLevels(recordType)
	if err != nil {
		return fmt.Errorf("ToParquetLevels failed %+v: %v", recordType, err)
	}

	// create a parquet schema
	parquetSchema, err := schema.ToParquetSchema(topicName, recordType)
	if err != nil {
		return fmt.Errorf("ToParquetSchema failed: %w", err)
	}

	// TODO parallelize the writing
	for _, logFileGroup := range logFileGroups {
		if err = writeLogFilesToParquet(filerClient, partitionDir, recordType, logFileGroup, parquetSchema, parquetLevels, preference); err != nil {
			return err
		}
	}

	return nil
}

func groupFilesBySize(logFiles []*filer_pb.Entry, maxGroupSize int64) (logFileGroups [][]*filer_pb.Entry) {
	var logFileGroup []*filer_pb.Entry
	var groupSize int64
	for _, logFile := range logFiles {
		if groupSize+int64(logFile.Attributes.FileSize) > maxGroupSize {
			logFileGroups = append(logFileGroups, logFileGroup)
			logFileGroup = nil
			groupSize = 0
		}
		logFileGroup = append(logFileGroup, logFile)
		groupSize += int64(logFile.Attributes.FileSize)
	}
	if len(logFileGroup) > 0 {
		logFileGroups = append(logFileGroups, logFileGroup)
	}
	return
}

func readAllLogFiles(filerClient filer_pb.FilerClient, partitionDir string, timeAgo time.Duration, minTsNs, maxTsNs int64) (logFiles []*filer_pb.Entry, err error) {
	err = filer_pb.ReadDirAllEntries(context.Background(), filerClient, util.FullPath(partitionDir), "", func(entry *filer_pb.Entry, isLast bool) error {
		if strings.HasSuffix(entry.Name, ".parquet") {
			return nil
		}
		if entry.Attributes.Crtime > time.Now().Unix()-int64(timeAgo/time.Second) {
			return nil
		}
		logTime, err := time.Parse(topic.TIME_FORMAT, entry.Name)
		if err != nil {
			// glog.Warningf("parse log time %s: %v", entry.Name, err)
			return nil
		}
		if maxTsNs > 0 && logTime.UnixNano() <= maxTsNs {
			return nil
		}
		logFiles = append(logFiles, entry)
		return nil
	})
	return
}

func readAllParquetFiles(filerClient filer_pb.FilerClient, partitionDir string) (minTsNs, maxTsNs int64, err error) {
	err = filer_pb.ReadDirAllEntries(context.Background(), filerClient, util.FullPath(partitionDir), "", func(entry *filer_pb.Entry, isLast bool) error {
		if !strings.HasSuffix(entry.Name, ".parquet") {
			return nil
		}
		if len(entry.Extended) == 0 {
			return nil
		}

		// read min ts
		minTsBytes := entry.Extended[mq.ExtendedAttrTimestampMin]
		if len(minTsBytes) != 8 {
			return nil
		}
		minTs := int64(binary.BigEndian.Uint64(minTsBytes))
		if minTsNs == 0 || minTs < minTsNs {
			minTsNs = minTs
		}

		// read max ts
		maxTsBytes := entry.Extended[mq.ExtendedAttrTimestampMax]
		if len(maxTsBytes) != 8 {
			return nil
		}
		maxTs := int64(binary.BigEndian.Uint64(maxTsBytes))
		if maxTsNs == 0 || maxTs > maxTsNs {
			maxTsNs = maxTs
		}
		return nil
	})
	return
}

// isSchemalessRecordType checks if the recordType represents a schema-less topic
// Schema-less topics only have system fields: _ts_ns, _key, and _value
func isSchemalessRecordType(recordType *schema_pb.RecordType) bool {
	if recordType == nil {
		return false
	}

	// Count only non-system data fields (exclude _ts_ns and _key which are always present)
	// Schema-less topics should only have _value as the data field
	hasValue := false
	dataFieldCount := 0

	for _, field := range recordType.Fields {
		switch field.Name {
		case SW_COLUMN_NAME_TS, SW_COLUMN_NAME_KEY, SW_COLUMN_NAME_OFFSET:
			// System fields - ignore
			continue
		case SW_COLUMN_NAME_VALUE:
			hasValue = true
			dataFieldCount++
		default:
			// Any other field means it's not schema-less
			dataFieldCount++
		}
	}

	// Schema-less = only has _value field as the data field (plus system fields)
	return hasValue && dataFieldCount == 1
}

func writeLogFilesToParquet(filerClient filer_pb.FilerClient, partitionDir string, recordType *schema_pb.RecordType, logFileGroups []*filer_pb.Entry, parquetSchema *parquet.Schema, parquetLevels *schema.ParquetLevels, preference *operation.StoragePreference) (err error) {

	tempFile, err := os.CreateTemp(".", "t*.parquet")
	if err != nil {
		return fmt.Errorf("create temp file: %w", err)
	}
	defer func() {
		tempFile.Close()
		os.Remove(tempFile.Name())
	}()

	// Enable column statistics for fast aggregation queries
	writer := parquet.NewWriter(tempFile, parquetSchema,
		parquet.Compression(&zstd.Codec{Level: zstd.DefaultLevel}),
		parquet.DataPageStatistics(true), // Enable column statistics
	)
	rowBuilder := parquet.NewRowBuilder(parquetSchema)

	var startTsNs, stopTsNs int64
	var minOffset, maxOffset int64
	var hasOffsets bool
	isSchemaless := isSchemalessRecordType(recordType)

	for _, logFile := range logFileGroups {
		var rows []parquet.Row
		if err := iterateLogEntries(filerClient, logFile, func(entry *filer_pb.LogEntry) error {

			// Skip control entries without actual data (same logic as read operations)
			if isControlEntry(entry) {
				return nil
			}

			if startTsNs == 0 {
				startTsNs = entry.TsNs
			}
			stopTsNs = entry.TsNs

			// Track offset ranges for Kafka integration
			if entry.Offset > 0 {
				if !hasOffsets {
					minOffset = entry.Offset
					maxOffset = entry.Offset
					hasOffsets = true
				} else {
					if entry.Offset < minOffset {
						minOffset = entry.Offset
					}
					if entry.Offset > maxOffset {
						maxOffset = entry.Offset
					}
				}
			}

			// write to parquet file
			rowBuilder.Reset()

			record := &schema_pb.RecordValue{}

			if isSchemaless {
				// For schema-less topics, put raw entry.Data into _value field
				record.Fields = make(map[string]*schema_pb.Value)
				record.Fields[SW_COLUMN_NAME_VALUE] = &schema_pb.Value{
					Kind: &schema_pb.Value_BytesValue{
						BytesValue: entry.Data,
					},
				}
			} else {
				// For schematized topics, unmarshal entry.Data as RecordValue
				if err := proto.Unmarshal(entry.Data, record); err != nil {
					return fmt.Errorf("unmarshal record value: %w", err)
				}

				// Initialize Fields map if nil (prevents nil map assignment panic)
				if record.Fields == nil {
					record.Fields = make(map[string]*schema_pb.Value)
				}

				// Add offset field to parquet records for native offset support
				// ASSUMPTION: LogEntry.Offset field is populated by broker during message publishing
				record.Fields[SW_COLUMN_NAME_OFFSET] = &schema_pb.Value{
					Kind: &schema_pb.Value_Int64Value{
						Int64Value: entry.Offset,
					},
				}
			}

			// Add system columns (for both schematized and schema-less topics)
			record.Fields[SW_COLUMN_NAME_TS] = &schema_pb.Value{
				Kind: &schema_pb.Value_Int64Value{
					Int64Value: entry.TsNs,
				},
			}

			// Handle nil key bytes to prevent growslice panic in parquet-go
			keyBytes := entry.Key
			if keyBytes == nil {
				keyBytes = []byte{} // Use empty slice instead of nil
			}
			record.Fields[SW_COLUMN_NAME_KEY] = &schema_pb.Value{
				Kind: &schema_pb.Value_BytesValue{
					BytesValue: keyBytes,
				},
			}

			if err := schema.AddRecordValue(rowBuilder, recordType, parquetLevels, record); err != nil {
				return fmt.Errorf("add record value: %w", err)
			}

			// Build row and normalize any nil ByteArray values to empty slices
			row := rowBuilder.Row()
			for i, value := range row {
				if value.Kind() == parquet.ByteArray {
					if value.ByteArray() == nil {
						row[i] = parquet.ByteArrayValue([]byte{})
					}
				}
			}

			rows = append(rows, row)

			return nil

		}); err != nil {
			return fmt.Errorf("iterate log entry %v/%v: %w", partitionDir, logFile.Name, err)
		}

		// Nil ByteArray handling is done during row creation

		// Write all rows in a single call
		if _, err := writer.WriteRows(rows); err != nil {
			return fmt.Errorf("write rows: %w", err)
		}
	}

	if err := writer.Close(); err != nil {
		return fmt.Errorf("close writer: %w", err)
	}

	// write to parquet file to partitionDir
	parquetFileName := fmt.Sprintf("%s.parquet", time.Unix(0, startTsNs).UTC().Format("2006-01-02-15-04-05"))

	// Collect source log file names and buffer_start metadata for deduplication
	var sourceLogFiles []string
	var earliestBufferStart int64
	for _, logFile := range logFileGroups {
		sourceLogFiles = append(sourceLogFiles, logFile.Name)

		// Extract buffer_start from log file metadata
		if bufferStart := getBufferStartFromLogFile(logFile); bufferStart > 0 {
			if earliestBufferStart == 0 || bufferStart < earliestBufferStart {
				earliestBufferStart = bufferStart
			}
		}
	}

	if err := saveParquetFileToPartitionDir(filerClient, tempFile, partitionDir, parquetFileName, preference, startTsNs, stopTsNs, sourceLogFiles, earliestBufferStart, minOffset, maxOffset, hasOffsets); err != nil {
		return fmt.Errorf("save parquet file %s: %v", parquetFileName, err)
	}

	return nil

}

func saveParquetFileToPartitionDir(filerClient filer_pb.FilerClient, sourceFile *os.File, partitionDir, parquetFileName string, preference *operation.StoragePreference, startTsNs, stopTsNs int64, sourceLogFiles []string, earliestBufferStart int64, minOffset, maxOffset int64, hasOffsets bool) error {
	uploader, err := operation.NewUploader()
	if err != nil {
		return fmt.Errorf("new uploader: %w", err)
	}

	// get file size
	fileInfo, err := sourceFile.Stat()
	if err != nil {
		return fmt.Errorf("stat source file: %w", err)
	}

	// upload file in chunks
	chunkSize := int64(4 * 1024 * 1024)
	chunkCount := (fileInfo.Size() + chunkSize - 1) / chunkSize
	entry := &filer_pb.Entry{
		Name: parquetFileName,
		Attributes: &filer_pb.FuseAttributes{
			Crtime:   time.Now().Unix(),
			Mtime:    time.Now().Unix(),
			FileMode: uint32(os.FileMode(0644)),
			FileSize: uint64(fileInfo.Size()),
			Mime:     "application/vnd.apache.parquet",
		},
	}
	entry.Extended = make(map[string][]byte)
	minTsBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(minTsBytes, uint64(startTsNs))
	entry.Extended[mq.ExtendedAttrTimestampMin] = minTsBytes
	maxTsBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(maxTsBytes, uint64(stopTsNs))
	entry.Extended[mq.ExtendedAttrTimestampMax] = maxTsBytes

	// Add offset range metadata for Kafka integration (same as regular log files)
	if hasOffsets && minOffset > 0 && maxOffset >= minOffset {
		minOffsetBytes := make([]byte, 8)
		binary.BigEndian.PutUint64(minOffsetBytes, uint64(minOffset))
		entry.Extended[mq.ExtendedAttrOffsetMin] = minOffsetBytes

		maxOffsetBytes := make([]byte, 8)
		binary.BigEndian.PutUint64(maxOffsetBytes, uint64(maxOffset))
		entry.Extended[mq.ExtendedAttrOffsetMax] = maxOffsetBytes
	}

	// Store source log files for deduplication (JSON-encoded list)
	if len(sourceLogFiles) > 0 {
		sourceLogFilesJson, _ := json.Marshal(sourceLogFiles)
		entry.Extended[mq.ExtendedAttrSources] = sourceLogFilesJson
	}

	// Store earliest buffer_start for precise broker deduplication
	if earliestBufferStart > 0 {
		bufferStartBytes := make([]byte, 8)
		binary.BigEndian.PutUint64(bufferStartBytes, uint64(earliestBufferStart))
		entry.Extended[mq.ExtendedAttrBufferStart] = bufferStartBytes
	}

	for i := int64(0); i < chunkCount; i++ {
		fileId, uploadResult, err, _ := uploader.UploadWithRetry(
			filerClient,
			&filer_pb.AssignVolumeRequest{
				Count:       1,
				Replication: preference.Replication,
				Collection:  preference.Collection,
				TtlSec:      0, // TODO set ttl
				DiskType:    preference.DiskType,
				Path:        partitionDir + "/" + parquetFileName,
			},
			&operation.UploadOption{
				Filename:          parquetFileName,
				Cipher:            false,
				IsInputCompressed: false,
				MimeType:          "application/vnd.apache.parquet",
				PairMap:           nil,
			},
			func(host, fileId string) string {
				return fmt.Sprintf("http://%s/%s", host, fileId)
			},
			io.NewSectionReader(sourceFile, i*chunkSize, chunkSize),
		)
		if err != nil {
			return fmt.Errorf("upload chunk %d: %v", i, err)
		}
		if uploadResult.Error != "" {
			return fmt.Errorf("upload result: %v", uploadResult.Error)
		}
		entry.Chunks = append(entry.Chunks, uploadResult.ToPbFileChunk(fileId, i*chunkSize, time.Now().UnixNano()))
	}

	// write the entry to partitionDir
	if err := filerClient.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		return filer_pb.CreateEntry(context.Background(), client, &filer_pb.CreateEntryRequest{
			Directory: partitionDir,
			Entry:     entry,
		})
	}); err != nil {
		return fmt.Errorf("create entry: %w", err)
	}

	return nil
}

func iterateLogEntries(filerClient filer_pb.FilerClient, logFile *filer_pb.Entry, eachLogEntryFn func(entry *filer_pb.LogEntry) error) error {
	lookupFn := filer.LookupFn(filerClient)
	_, err := eachFile(logFile, lookupFn, func(logEntry *filer_pb.LogEntry) (isDone bool, err error) {
		if err := eachLogEntryFn(logEntry); err != nil {
			return true, err
		}
		return false, nil
	})
	return err
}

func eachFile(entry *filer_pb.Entry, lookupFileIdFn func(ctx context.Context, fileId string) (targetUrls []string, err error), eachLogEntryFn log_buffer.EachLogEntryFuncType) (processedTsNs int64, err error) {
	if len(entry.Content) > 0 {
		// skip .offset files
		return
	}
	var urlStrings []string
	for _, chunk := range entry.Chunks {
		if chunk.Size == 0 {
			continue
		}
		if chunk.IsChunkManifest {
			return
		}
		urlStrings, err = lookupFileIdFn(context.Background(), chunk.FileId)
		if err != nil {
			err = fmt.Errorf("lookup %s: %v", chunk.FileId, err)
			return
		}
		if len(urlStrings) == 0 {
			err = fmt.Errorf("no url found for %s", chunk.FileId)
			return
		}

		// try one of the urlString until util.Get(urlString) succeeds
		var processed bool
		for _, urlString := range urlStrings {
			var data []byte
			if data, _, err = util_http.Get(urlString); err == nil {
				processed = true
				if processedTsNs, err = eachChunk(data, eachLogEntryFn); err != nil {
					return
				}
				break
			}
		}
		if !processed {
			err = fmt.Errorf("no data processed for %s %s", entry.Name, chunk.FileId)
			return
		}

	}
	return
}

func eachChunk(buf []byte, eachLogEntryFn log_buffer.EachLogEntryFuncType) (processedTsNs int64, err error) {
	for pos := 0; pos+4 < len(buf); {

		size := util.BytesToUint32(buf[pos : pos+4])
		if pos+4+int(size) > len(buf) {
			err = fmt.Errorf("reach each log chunk: read [%d,%d) from [0,%d)", pos, pos+int(size)+4, len(buf))
			return
		}
		entryData := buf[pos+4 : pos+4+int(size)]

		logEntry := &filer_pb.LogEntry{}
		if err = proto.Unmarshal(entryData, logEntry); err != nil {
			pos += 4 + int(size)
			err = fmt.Errorf("unexpected unmarshal mq_pb.Message: %w", err)
			return
		}

		if _, err = eachLogEntryFn(logEntry); err != nil {
			err = fmt.Errorf("process log entry %v: %w", logEntry, err)
			return
		}

		processedTsNs = logEntry.TsNs

		pos += 4 + int(size)

	}

	return
}

// getBufferStartFromLogFile extracts the buffer_start index from log file extended metadata
func getBufferStartFromLogFile(logFile *filer_pb.Entry) int64 {
	if logFile.Extended == nil {
		return 0
	}

	// Parse buffer_start binary format
	if startData, exists := logFile.Extended["buffer_start"]; exists {
		if len(startData) == 8 {
			startIndex := int64(binary.BigEndian.Uint64(startData))
			if startIndex > 0 {
				return startIndex
			}
		}
	}

	return 0
}
