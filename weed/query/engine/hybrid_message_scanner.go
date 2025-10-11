package engine

import (
	"container/heap"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/parquet-go/parquet-go"
	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/mq"
	"github.com/seaweedfs/seaweedfs/weed/mq/logstore"
	"github.com/seaweedfs/seaweedfs/weed/mq/schema"
	"github.com/seaweedfs/seaweedfs/weed/mq/topic"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/mq_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/schema_pb"
	"github.com/seaweedfs/seaweedfs/weed/query/sqltypes"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"github.com/seaweedfs/seaweedfs/weed/util/chunk_cache"
	"github.com/seaweedfs/seaweedfs/weed/util/log_buffer"
	"github.com/seaweedfs/seaweedfs/weed/wdclient"
	"google.golang.org/protobuf/proto"
)

// HybridMessageScanner scans from ALL data sources:
// Architecture:
// 1. Unflushed in-memory data from brokers (mq_pb.DataMessage format) - REAL-TIME
// 2. Recent/live messages in log files (filer_pb.LogEntry format) - FLUSHED
// 3. Older messages in Parquet files (schema_pb.RecordValue format) - ARCHIVED
// 4. Seamlessly merges data from all sources chronologically
// 5. Provides complete real-time view of all messages in a topic
type HybridMessageScanner struct {
	filerClient   filer_pb.FilerClient
	brokerClient  BrokerClientInterface // For querying unflushed data
	topic         topic.Topic
	recordSchema  *schema_pb.RecordType
	schemaFormat  string // Serialization format: "AVRO", "PROTOBUF", "JSON_SCHEMA", or empty for schemaless
	parquetLevels *schema.ParquetLevels
	engine        *SQLEngine // Reference for system column formatting
}

// NewHybridMessageScanner creates a scanner that reads from all data sources
// This provides complete real-time message coverage including unflushed data
func NewHybridMessageScanner(filerClient filer_pb.FilerClient, brokerClient BrokerClientInterface, namespace, topicName string, engine *SQLEngine) (*HybridMessageScanner, error) {
	// Check if filerClient is available
	if filerClient == nil {
		return nil, fmt.Errorf("filerClient is required but not available")
	}

	// Create topic reference
	t := topic.Topic{
		Namespace: namespace,
		Name:      topicName,
	}

	// Get flat schema from broker client
	recordType, _, schemaFormat, err := brokerClient.GetTopicSchema(context.Background(), namespace, topicName)
	if err != nil {
		return nil, fmt.Errorf("failed to get topic record type: %v", err)
	}

	if recordType == nil || len(recordType.Fields) == 0 {
		// For topics without schema, create a minimal schema with system fields and _value
		recordType = schema.RecordTypeBegin().
			WithField(SW_COLUMN_NAME_TIMESTAMP, schema.TypeInt64).
			WithField(SW_COLUMN_NAME_KEY, schema.TypeBytes).
			WithField(SW_COLUMN_NAME_VALUE, schema.TypeBytes). // Raw message value
			RecordTypeEnd()
	} else {
		// Create a copy of the recordType to avoid modifying the original
		recordTypeCopy := &schema_pb.RecordType{
			Fields: make([]*schema_pb.Field, len(recordType.Fields)),
		}
		copy(recordTypeCopy.Fields, recordType.Fields)

		// Add system columns that MQ adds to all records
		recordType = schema.NewRecordTypeBuilder(recordTypeCopy).
			WithField(SW_COLUMN_NAME_TIMESTAMP, schema.TypeInt64).
			WithField(SW_COLUMN_NAME_KEY, schema.TypeBytes).
			RecordTypeEnd()
	}

	// Convert to Parquet levels for efficient reading
	parquetLevels, err := schema.ToParquetLevels(recordType)
	if err != nil {
		return nil, fmt.Errorf("failed to create Parquet levels: %v", err)
	}

	return &HybridMessageScanner{
		filerClient:   filerClient,
		brokerClient:  brokerClient,
		topic:         t,
		recordSchema:  recordType,
		schemaFormat:  schemaFormat,
		parquetLevels: parquetLevels,
		engine:        engine,
	}, nil
}

// HybridScanOptions configure how the scanner reads from both live and archived data
type HybridScanOptions struct {
	// Time range filtering (Unix nanoseconds)
	StartTimeNs int64
	StopTimeNs  int64

	// Column projection - if empty, select all columns
	Columns []string

	// Row limit - 0 means no limit
	Limit int

	// Row offset - 0 means no offset
	Offset int

	// Predicate for WHERE clause filtering
	Predicate func(*schema_pb.RecordValue) bool
}

// HybridScanResult represents a message from either live logs or Parquet files
type HybridScanResult struct {
	Values    map[string]*schema_pb.Value // Column name -> value
	Timestamp int64                       // Message timestamp (_ts_ns)
	Key       []byte                      // Message key (_key)
	Source    string                      // "live_log" or "parquet_archive" or "in_memory_broker"
}

// HybridScanStats contains statistics about data sources scanned
type HybridScanStats struct {
	BrokerBufferQueried  bool
	BrokerBufferMessages int
	BufferStartIndex     int64
	PartitionsScanned    int
	LiveLogFilesScanned  int // Number of live log files processed
}

// ParquetColumnStats holds statistics for a single column from parquet metadata
type ParquetColumnStats struct {
	ColumnName string
	MinValue   *schema_pb.Value
	MaxValue   *schema_pb.Value
	NullCount  int64
	RowCount   int64
}

// ParquetFileStats holds aggregated statistics for a parquet file
type ParquetFileStats struct {
	FileName    string
	RowCount    int64
	ColumnStats map[string]*ParquetColumnStats
	// Optional file-level timestamp range from filer extended attributes
	MinTimestampNs int64
	MaxTimestampNs int64
}

// getTimestampRangeFromStats returns (minTsNs, maxTsNs, ok) by inspecting common timestamp columns
func (h *HybridMessageScanner) getTimestampRangeFromStats(fileStats *ParquetFileStats) (int64, int64, bool) {
	if fileStats == nil {
		return 0, 0, false
	}
	// Prefer column stats for _ts_ns if present
	if len(fileStats.ColumnStats) > 0 {
		if s, ok := fileStats.ColumnStats[logstore.SW_COLUMN_NAME_TS]; ok && s != nil && s.MinValue != nil && s.MaxValue != nil {
			if minNs, okMin := h.schemaValueToNs(s.MinValue); okMin {
				if maxNs, okMax := h.schemaValueToNs(s.MaxValue); okMax {
					return minNs, maxNs, true
				}
			}
		}
	}
	// Fallback to file-level range if present in filer extended metadata
	if fileStats.MinTimestampNs != 0 || fileStats.MaxTimestampNs != 0 {
		return fileStats.MinTimestampNs, fileStats.MaxTimestampNs, true
	}
	return 0, 0, false
}

// schemaValueToNs converts a schema_pb.Value that represents a timestamp to ns
func (h *HybridMessageScanner) schemaValueToNs(v *schema_pb.Value) (int64, bool) {
	if v == nil {
		return 0, false
	}
	switch k := v.Kind.(type) {
	case *schema_pb.Value_Int64Value:
		return k.Int64Value, true
	case *schema_pb.Value_Int32Value:
		return int64(k.Int32Value), true
	default:
		return 0, false
	}
}

// StreamingDataSource provides a streaming interface for reading scan results
type StreamingDataSource interface {
	Next() (*HybridScanResult, error) // Returns next result or nil when done
	HasMore() bool                    // Returns true if more data available
	Close() error                     // Clean up resources
}

// StreamingMergeItem represents an item in the priority queue for streaming merge
type StreamingMergeItem struct {
	Result     *HybridScanResult
	SourceID   int
	DataSource StreamingDataSource
}

// StreamingMergeHeap implements heap.Interface for merging sorted streams by timestamp
type StreamingMergeHeap []*StreamingMergeItem

func (h StreamingMergeHeap) Len() int { return len(h) }

func (h StreamingMergeHeap) Less(i, j int) bool {
	// Sort by timestamp (ascending order)
	return h[i].Result.Timestamp < h[j].Result.Timestamp
}

func (h StreamingMergeHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }

func (h *StreamingMergeHeap) Push(x interface{}) {
	*h = append(*h, x.(*StreamingMergeItem))
}

func (h *StreamingMergeHeap) Pop() interface{} {
	old := *h
	n := len(old)
	item := old[n-1]
	*h = old[0 : n-1]
	return item
}

// Scan reads messages from both live logs and archived Parquet files
// Uses SeaweedFS MQ's GenMergedReadFunc for seamless integration
// Assumptions:
// 1. Chronologically merges live and archived data
// 2. Applies filtering at the lowest level for efficiency
// 3. Handles schema evolution transparently
func (hms *HybridMessageScanner) Scan(ctx context.Context, options HybridScanOptions) ([]HybridScanResult, error) {
	results, _, err := hms.ScanWithStats(ctx, options)
	return results, err
}

// ScanWithStats reads messages and returns scan statistics for execution plans
func (hms *HybridMessageScanner) ScanWithStats(ctx context.Context, options HybridScanOptions) ([]HybridScanResult, *HybridScanStats, error) {
	var results []HybridScanResult
	stats := &HybridScanStats{}

	// Get all partitions for this topic via MQ broker discovery
	partitions, err := hms.discoverTopicPartitions(ctx)
	if err != nil {
		return nil, stats, fmt.Errorf("failed to discover partitions for topic %s: %v", hms.topic.String(), err)
	}

	stats.PartitionsScanned = len(partitions)

	for _, partition := range partitions {
		partitionResults, partitionStats, err := hms.scanPartitionHybridWithStats(ctx, partition, options)
		if err != nil {
			return nil, stats, fmt.Errorf("failed to scan partition %v: %v", partition, err)
		}

		results = append(results, partitionResults...)

		// Aggregate broker buffer stats
		if partitionStats != nil {
			if partitionStats.BrokerBufferQueried {
				stats.BrokerBufferQueried = true
			}
			stats.BrokerBufferMessages += partitionStats.BrokerBufferMessages
			if partitionStats.BufferStartIndex > 0 && (stats.BufferStartIndex == 0 || partitionStats.BufferStartIndex < stats.BufferStartIndex) {
				stats.BufferStartIndex = partitionStats.BufferStartIndex
			}
		}

		// Apply global limit (without offset) across all partitions
		// When OFFSET is used, collect more data to ensure we have enough after skipping
		// Note: OFFSET will be applied at the end to avoid double-application
		if options.Limit > 0 {
			// Collect exact amount needed: LIMIT + OFFSET (no excessive doubling)
			minRequired := options.Limit + options.Offset
			// Small buffer only when needed to handle edge cases in distributed scanning
			if options.Offset > 0 && minRequired < 10 {
				minRequired = minRequired + 1 // Add 1 extra row buffer, not doubling
			}
			if len(results) >= minRequired {
				break
			}
		}
	}

	// Apply final OFFSET and LIMIT processing (done once at the end)
	// Limit semantics: -1 = no limit, 0 = LIMIT 0 (empty), >0 = limit to N rows
	if options.Offset > 0 || options.Limit >= 0 {
		// Handle LIMIT 0 special case first
		if options.Limit == 0 {
			return []HybridScanResult{}, stats, nil
		}

		// Apply OFFSET first
		if options.Offset > 0 {
			if options.Offset >= len(results) {
				results = []HybridScanResult{}
			} else {
				results = results[options.Offset:]
			}
		}

		// Apply LIMIT after OFFSET (only if limit > 0)
		if options.Limit > 0 && len(results) > options.Limit {
			results = results[:options.Limit]
		}
	}

	return results, stats, nil
}

// scanUnflushedData queries brokers for unflushed in-memory data using buffer_start deduplication
func (hms *HybridMessageScanner) scanUnflushedData(ctx context.Context, partition topic.Partition, options HybridScanOptions) ([]HybridScanResult, error) {
	results, _, err := hms.scanUnflushedDataWithStats(ctx, partition, options)
	return results, err
}

// scanUnflushedDataWithStats queries brokers for unflushed data and returns statistics
func (hms *HybridMessageScanner) scanUnflushedDataWithStats(ctx context.Context, partition topic.Partition, options HybridScanOptions) ([]HybridScanResult, *HybridScanStats, error) {
	var results []HybridScanResult
	stats := &HybridScanStats{}

	// Skip if no broker client available
	if hms.brokerClient == nil {
		return results, stats, nil
	}

	// Mark that we attempted to query broker buffer
	stats.BrokerBufferQueried = true

	// Step 1: Get unflushed data from broker using buffer_start-based method
	// This method uses buffer_start metadata to avoid double-counting with exact precision
	unflushedEntries, err := hms.brokerClient.GetUnflushedMessages(ctx, hms.topic.Namespace, hms.topic.Name, partition, options.StartTimeNs)
	if err != nil {
		// Log error but don't fail the query - continue with disk data only
		// Reset queried flag on error
		stats.BrokerBufferQueried = false
		return results, stats, nil
	}

	// Capture stats for EXPLAIN
	stats.BrokerBufferMessages = len(unflushedEntries)

	// Step 2: Process unflushed entries (already deduplicated by broker)
	for _, logEntry := range unflushedEntries {
		// Pre-decode DataMessage for reuse in both control check and conversion
		var dataMessage *mq_pb.DataMessage
		if len(logEntry.Data) > 0 {
			dataMessage = &mq_pb.DataMessage{}
			if err := proto.Unmarshal(logEntry.Data, dataMessage); err != nil {
				dataMessage = nil // Failed to decode, treat as raw data
			}
		}

		// Skip control entries without actual data
		if hms.isControlEntryWithDecoded(logEntry, dataMessage) {
			continue // Skip this entry
		}

		// Skip messages outside time range
		if options.StartTimeNs > 0 && logEntry.TsNs < options.StartTimeNs {
			continue
		}
		if options.StopTimeNs > 0 && logEntry.TsNs > options.StopTimeNs {
			continue
		}

		// Convert LogEntry to RecordValue format (same as disk data)
		recordValue, _, err := hms.convertLogEntryToRecordValueWithDecoded(logEntry, dataMessage)
		if err != nil {
			continue // Skip malformed messages
		}

		// Apply predicate filter if provided
		if options.Predicate != nil && !options.Predicate(recordValue) {
			continue
		}

		// Extract system columns for result
		timestamp := recordValue.Fields[SW_COLUMN_NAME_TIMESTAMP].GetInt64Value()
		key := recordValue.Fields[SW_COLUMN_NAME_KEY].GetBytesValue()

		// Apply column projection
		values := make(map[string]*schema_pb.Value)
		if len(options.Columns) == 0 {
			// Select all columns (excluding system columns from user view)
			for name, value := range recordValue.Fields {
				if name != SW_COLUMN_NAME_TIMESTAMP && name != SW_COLUMN_NAME_KEY {
					values[name] = value
				}
			}
		} else {
			// Select specified columns only
			for _, columnName := range options.Columns {
				if value, exists := recordValue.Fields[columnName]; exists {
					values[columnName] = value
				}
			}
		}

		// Create result with proper source tagging
		result := HybridScanResult{
			Values:    values,
			Timestamp: timestamp,
			Key:       key,
			Source:    "live_log", // Data from broker's unflushed messages
		}

		results = append(results, result)

		// Apply limit (accounting for offset) - collect exact amount needed
		if options.Limit > 0 {
			// Collect exact amount needed: LIMIT + OFFSET (no excessive doubling)
			minRequired := options.Limit + options.Offset
			// Small buffer only when needed to handle edge cases in message streaming
			if options.Offset > 0 && minRequired < 10 {
				minRequired = minRequired + 1 // Add 1 extra row buffer, not doubling
			}
			if len(results) >= minRequired {
				break
			}
		}
	}

	return results, stats, nil
}

// convertDataMessageToRecord converts mq_pb.DataMessage to schema_pb.RecordValue
func (hms *HybridMessageScanner) convertDataMessageToRecord(msg *mq_pb.DataMessage) (*schema_pb.RecordValue, string, error) {
	// Parse the message data as RecordValue
	recordValue := &schema_pb.RecordValue{}
	if err := proto.Unmarshal(msg.Value, recordValue); err != nil {
		return nil, "", fmt.Errorf("failed to unmarshal message data: %v", err)
	}

	// Add system columns
	if recordValue.Fields == nil {
		recordValue.Fields = make(map[string]*schema_pb.Value)
	}

	// Add timestamp
	recordValue.Fields[SW_COLUMN_NAME_TIMESTAMP] = &schema_pb.Value{
		Kind: &schema_pb.Value_Int64Value{Int64Value: msg.TsNs},
	}

	return recordValue, string(msg.Key), nil
}

// discoverTopicPartitions discovers the actual partitions for this topic by scanning the filesystem
// This finds real partition directories like v2025-09-01-07-16-34/0000-0630/
func (hms *HybridMessageScanner) discoverTopicPartitions(ctx context.Context) ([]topic.Partition, error) {
	if hms.filerClient == nil {
		return nil, fmt.Errorf("filerClient not available for partition discovery")
	}

	var allPartitions []topic.Partition
	var err error

	// Scan the topic directory for actual partition versions (timestamped directories)
	// List all version directories in the topic directory
	err = filer_pb.ReadDirAllEntries(ctx, hms.filerClient, util.FullPath(hms.topic.Dir()), "", func(versionEntry *filer_pb.Entry, isLast bool) error {
		if !versionEntry.IsDirectory {
			return nil // Skip non-directories
		}

		// Parse version timestamp from directory name (e.g., "v2025-09-01-07-16-34")
		versionTime, parseErr := topic.ParseTopicVersion(versionEntry.Name)
		if parseErr != nil {
			// Skip directories that don't match the version format
			return nil
		}

		// Scan partition directories within this version
		versionDir := fmt.Sprintf("%s/%s", hms.topic.Dir(), versionEntry.Name)
		return filer_pb.ReadDirAllEntries(ctx, hms.filerClient, util.FullPath(versionDir), "", func(partitionEntry *filer_pb.Entry, isLast bool) error {
			if !partitionEntry.IsDirectory {
				return nil // Skip non-directories
			}

			// Parse partition boundary from directory name (e.g., "0000-0630")
			rangeStart, rangeStop := topic.ParsePartitionBoundary(partitionEntry.Name)
			if rangeStart == rangeStop {
				return nil // Skip invalid partition names
			}

			// Create partition object
			partition := topic.Partition{
				RangeStart: rangeStart,
				RangeStop:  rangeStop,
				RingSize:   topic.PartitionCount,
				UnixTimeNs: versionTime.UnixNano(),
			}

			allPartitions = append(allPartitions, partition)
			return nil
		})
	})

	if err != nil {
		return nil, fmt.Errorf("failed to scan topic directory for partitions: %v", err)
	}

	// If no partitions found, return empty slice (valid for newly created or empty topics)
	if len(allPartitions) == 0 {
		fmt.Printf("No partitions found for topic %s - returning empty result set\n", hms.topic.String())
		return []topic.Partition{}, nil
	}

	fmt.Printf("Discovered %d partitions for topic %s\n", len(allPartitions), hms.topic.String())
	return allPartitions, nil
}

// scanPartitionHybrid scans a specific partition using the hybrid approach
// This is where the magic happens - seamlessly reading ALL data sources:
// 1. Unflushed in-memory data from brokers (REAL-TIME)
// 2. Live logs + Parquet files from disk (FLUSHED/ARCHIVED)
func (hms *HybridMessageScanner) scanPartitionHybrid(ctx context.Context, partition topic.Partition, options HybridScanOptions) ([]HybridScanResult, error) {
	results, _, err := hms.scanPartitionHybridWithStats(ctx, partition, options)
	return results, err
}

// scanPartitionHybridWithStats scans a specific partition using streaming merge for memory efficiency
// PERFORMANCE IMPROVEMENT: Uses heap-based streaming merge instead of collecting all data and sorting
// - Memory usage: O(k) where k = number of data sources, instead of O(n) where n = total records
// - Scalable: Can handle large topics without LIMIT clauses efficiently
// - Streaming: Processes data as it arrives rather than buffering everything
func (hms *HybridMessageScanner) scanPartitionHybridWithStats(ctx context.Context, partition topic.Partition, options HybridScanOptions) ([]HybridScanResult, *HybridScanStats, error) {
	stats := &HybridScanStats{}

	// STEP 1: Scan unflushed in-memory data from brokers (REAL-TIME)
	unflushedResults, unflushedStats, err := hms.scanUnflushedDataWithStats(ctx, partition, options)
	if err != nil {
		// Don't fail the query if broker scanning fails, but provide clear warning to user
		// This ensures users are aware that results may not include the most recent data
		fmt.Printf("Warning: Unable to access real-time data from message broker: %v\n", err)
		fmt.Printf("Note: Query results may not include the most recent unflushed messages\n")
	} else if unflushedStats != nil {
		stats.BrokerBufferQueried = unflushedStats.BrokerBufferQueried
		stats.BrokerBufferMessages = unflushedStats.BrokerBufferMessages
		stats.BufferStartIndex = unflushedStats.BufferStartIndex
	}

	// Count live log files for statistics
	liveLogCount, err := hms.countLiveLogFiles(partition)
	if err != nil {
		// Don't fail the query, just log warning
		fmt.Printf("Warning: Failed to count live log files: %v\n", err)
		liveLogCount = 0
	}
	stats.LiveLogFilesScanned = liveLogCount

	// STEP 2: Create streaming data sources for memory-efficient merge
	var dataSources []StreamingDataSource

	// Add unflushed data source (if we have unflushed results)
	if len(unflushedResults) > 0 {
		// Sort unflushed results by timestamp before creating stream
		if len(unflushedResults) > 1 {
			hms.mergeSort(unflushedResults, 0, len(unflushedResults)-1)
		}
		dataSources = append(dataSources, NewSliceDataSource(unflushedResults))
	}

	// Add streaming flushed data source (live logs + Parquet files)
	flushedDataSource := NewStreamingFlushedDataSource(hms, partition, options)
	dataSources = append(dataSources, flushedDataSource)

	// STEP 3: Use streaming merge for memory-efficient chronological ordering
	var results []HybridScanResult
	if len(dataSources) > 0 {
		// Calculate how many rows we need to collect during scanning (before OFFSET/LIMIT)
		// For LIMIT N OFFSET M, we need to collect at least N+M rows
		scanLimit := options.Limit
		if options.Limit > 0 && options.Offset > 0 {
			scanLimit = options.Limit + options.Offset
		}

		mergedResults, err := hms.streamingMerge(dataSources, scanLimit)
		if err != nil {
			return nil, stats, fmt.Errorf("streaming merge failed: %v", err)
		}
		results = mergedResults
	}

	return results, stats, nil
}

// countLiveLogFiles counts the number of live log files in a partition for statistics
func (hms *HybridMessageScanner) countLiveLogFiles(partition topic.Partition) (int, error) {
	partitionDir := topic.PartitionDir(hms.topic, partition)

	var fileCount int
	err := hms.filerClient.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		// List all files in partition directory
		request := &filer_pb.ListEntriesRequest{
			Directory:          partitionDir,
			Prefix:             "",
			StartFromFileName:  "",
			InclusiveStartFrom: true,
			Limit:              10000, // reasonable limit for counting
		}

		stream, err := client.ListEntries(context.Background(), request)
		if err != nil {
			return err
		}

		for {
			resp, err := stream.Recv()
			if err == io.EOF {
				break
			}
			if err != nil {
				return err
			}

			// Count files that are not .parquet files (live log files)
			// Live log files typically have timestamps or are named like log files
			fileName := resp.Entry.Name
			if !strings.HasSuffix(fileName, ".parquet") &&
				!strings.HasSuffix(fileName, ".offset") &&
				len(resp.Entry.Chunks) > 0 { // Has actual content
				fileCount++
			}
		}

		return nil
	})

	if err != nil {
		return 0, err
	}
	return fileCount, nil
}

// isControlEntry checks if a log entry is a control entry without actual data
// Based on MQ system analysis, control entries are:
// 1. DataMessages with populated Ctrl field (publisher close signals)
// 2. Entries with empty keys (as filtered by subscriber)
// NOTE: Messages with empty data but valid keys (like NOOP messages) are NOT control entries
func (hms *HybridMessageScanner) isControlEntry(logEntry *filer_pb.LogEntry) bool {
	// Pre-decode DataMessage if needed
	var dataMessage *mq_pb.DataMessage
	if len(logEntry.Data) > 0 {
		dataMessage = &mq_pb.DataMessage{}
		if err := proto.Unmarshal(logEntry.Data, dataMessage); err != nil {
			dataMessage = nil // Failed to decode, treat as raw data
		}
	}
	return hms.isControlEntryWithDecoded(logEntry, dataMessage)
}

// isControlEntryWithDecoded checks if a log entry is a control entry using pre-decoded DataMessage
// This avoids duplicate protobuf unmarshaling when the DataMessage is already decoded
func (hms *HybridMessageScanner) isControlEntryWithDecoded(logEntry *filer_pb.LogEntry, dataMessage *mq_pb.DataMessage) bool {
	// Skip entries with empty keys (same logic as subscriber)
	if len(logEntry.Key) == 0 {
		return true
	}

	// Check if this is a DataMessage with control field populated
	if dataMessage != nil && dataMessage.Ctrl != nil {
		return true
	}

	// Messages with valid keys (even if data is empty) are legitimate messages
	// Examples: NOOP messages from Schema Registry
	return false
}

// isNullOrEmpty checks if a schema_pb.Value is null or empty
func isNullOrEmpty(value *schema_pb.Value) bool {
	if value == nil {
		return true
	}

	switch v := value.Kind.(type) {
	case *schema_pb.Value_StringValue:
		return v.StringValue == ""
	case *schema_pb.Value_BytesValue:
		return len(v.BytesValue) == 0
	case *schema_pb.Value_ListValue:
		return v.ListValue == nil || len(v.ListValue.Values) == 0
	case nil:
		return true // No kind set means null
	default:
		return false
	}
}

// isSchemaless checks if the scanner is configured for a schema-less topic
// Schema-less topics only have system fields: _ts_ns, _key, and _value
func (hms *HybridMessageScanner) isSchemaless() bool {
	// Schema-less topics only have system fields: _ts_ns, _key, and _value
	// System topics like _schemas are NOT schema-less - they have structured data
	// We just need to map their fields during read

	if hms.recordSchema == nil {
		return false
	}

	// Count only non-system data fields (exclude _ts_ns and _key which are always present)
	// Schema-less topics should only have _value as the data field
	hasValue := false
	dataFieldCount := 0

	for _, field := range hms.recordSchema.Fields {
		switch field.Name {
		case SW_COLUMN_NAME_TIMESTAMP, SW_COLUMN_NAME_KEY:
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

// convertLogEntryToRecordValue converts a filer_pb.LogEntry to schema_pb.RecordValue
// This handles both:
// 1. Live log entries (raw message format)
// 2. Parquet entries (already in schema_pb.RecordValue format)
// 3. Schema-less topics (raw bytes in _value field)
func (hms *HybridMessageScanner) convertLogEntryToRecordValue(logEntry *filer_pb.LogEntry) (*schema_pb.RecordValue, string, error) {
	// For schema-less topics, put raw data directly into _value field
	if hms.isSchemaless() {
		recordValue := &schema_pb.RecordValue{
			Fields: make(map[string]*schema_pb.Value),
		}
		recordValue.Fields[SW_COLUMN_NAME_TIMESTAMP] = &schema_pb.Value{
			Kind: &schema_pb.Value_Int64Value{Int64Value: logEntry.TsNs},
		}
		recordValue.Fields[SW_COLUMN_NAME_KEY] = &schema_pb.Value{
			Kind: &schema_pb.Value_BytesValue{BytesValue: logEntry.Key},
		}
		recordValue.Fields[SW_COLUMN_NAME_VALUE] = &schema_pb.Value{
			Kind: &schema_pb.Value_BytesValue{BytesValue: logEntry.Data},
		}
		return recordValue, "live_log", nil
	}

	// Try to unmarshal as RecordValue first (Parquet format)
	recordValue := &schema_pb.RecordValue{}
	if err := proto.Unmarshal(logEntry.Data, recordValue); err == nil {
		// This is an archived message from Parquet files
		// FIX: Add system columns from LogEntry to RecordValue
		if recordValue.Fields == nil {
			recordValue.Fields = make(map[string]*schema_pb.Value)
		}

		// Add system columns from LogEntry
		recordValue.Fields[SW_COLUMN_NAME_TIMESTAMP] = &schema_pb.Value{
			Kind: &schema_pb.Value_Int64Value{Int64Value: logEntry.TsNs},
		}
		recordValue.Fields[SW_COLUMN_NAME_KEY] = &schema_pb.Value{
			Kind: &schema_pb.Value_BytesValue{BytesValue: logEntry.Key},
		}

		return recordValue, "parquet_archive", nil
	}

	// If not a RecordValue, this is raw live message data - parse with schema
	return hms.parseRawMessageWithSchema(logEntry)
}

// min returns the minimum of two integers
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// parseRawMessageWithSchema parses raw live message data using the topic's schema
// This provides proper type conversion and field mapping instead of treating everything as strings
func (hms *HybridMessageScanner) parseRawMessageWithSchema(logEntry *filer_pb.LogEntry) (*schema_pb.RecordValue, string, error) {
	recordValue := &schema_pb.RecordValue{
		Fields: make(map[string]*schema_pb.Value),
	}

	// Add system columns (always present)
	recordValue.Fields[SW_COLUMN_NAME_TIMESTAMP] = &schema_pb.Value{
		Kind: &schema_pb.Value_Int64Value{Int64Value: logEntry.TsNs},
	}
	recordValue.Fields[SW_COLUMN_NAME_KEY] = &schema_pb.Value{
		Kind: &schema_pb.Value_BytesValue{BytesValue: logEntry.Key},
	}

	// Parse message data based on schema
	if hms.recordSchema == nil || len(hms.recordSchema.Fields) == 0 {
		// Fallback: No schema available, use "_value" for schema-less topics only
		if hms.isSchemaless() {
			recordValue.Fields[SW_COLUMN_NAME_VALUE] = &schema_pb.Value{
				Kind: &schema_pb.Value_BytesValue{BytesValue: logEntry.Data},
			}
		}
		return recordValue, "live_log", nil
	}

	// Use schema format to directly choose the right decoder
	// This avoids trying multiple decoders and improves performance
	var parsedRecord *schema_pb.RecordValue
	var err error

	switch hms.schemaFormat {
	case "AVRO":
		// AVRO format - use Avro decoder
		// Note: Avro decoding requires schema registry integration
		// For now, fall through to JSON as many Avro messages are also valid JSON
		parsedRecord, err = hms.parseJSONMessage(logEntry.Data)
	case "PROTOBUF":
		// PROTOBUF format - use protobuf decoder
		parsedRecord, err = hms.parseProtobufMessage(logEntry.Data)
	case "JSON_SCHEMA", "":
		// JSON_SCHEMA format or empty (default to JSON)
		// JSON is the most common format for schema registry
		parsedRecord, err = hms.parseJSONMessage(logEntry.Data)
		if err != nil {
			// Try protobuf as fallback
			parsedRecord, err = hms.parseProtobufMessage(logEntry.Data)
		}
	default:
		// Unknown format - try JSON first, then protobuf as fallback
		parsedRecord, err = hms.parseJSONMessage(logEntry.Data)
		if err != nil {
			parsedRecord, err = hms.parseProtobufMessage(logEntry.Data)
		}
	}

	if err == nil && parsedRecord != nil {
		// Successfully parsed, merge with system columns
		for fieldName, fieldValue := range parsedRecord.Fields {
			recordValue.Fields[fieldName] = fieldValue
		}
		return recordValue, "live_log", nil
	}

	// Fallback: If schema has a single field, map the raw data to it with type conversion
	if len(hms.recordSchema.Fields) == 1 {
		field := hms.recordSchema.Fields[0]
		convertedValue, convErr := hms.convertRawDataToSchemaValue(logEntry.Data, field.Type)
		if convErr == nil {
			recordValue.Fields[field.Name] = convertedValue
			return recordValue, "live_log", nil
		}
	}

	// Final fallback: treat as bytes field for schema-less topics only
	if hms.isSchemaless() {
		recordValue.Fields[SW_COLUMN_NAME_VALUE] = &schema_pb.Value{
			Kind: &schema_pb.Value_BytesValue{BytesValue: logEntry.Data},
		}
	}

	return recordValue, "live_log", nil
}

// convertLogEntryToRecordValueWithDecoded converts a filer_pb.LogEntry to schema_pb.RecordValue
// using a pre-decoded DataMessage to avoid duplicate protobuf unmarshaling
func (hms *HybridMessageScanner) convertLogEntryToRecordValueWithDecoded(logEntry *filer_pb.LogEntry, dataMessage *mq_pb.DataMessage) (*schema_pb.RecordValue, string, error) {
	// IMPORTANT: Check for schema-less topics FIRST
	// Schema-less topics (like _schemas) should store raw data directly in _value field
	if hms.isSchemaless() {
		recordValue := &schema_pb.RecordValue{
			Fields: make(map[string]*schema_pb.Value),
		}
		recordValue.Fields[SW_COLUMN_NAME_TIMESTAMP] = &schema_pb.Value{
			Kind: &schema_pb.Value_Int64Value{Int64Value: logEntry.TsNs},
		}
		recordValue.Fields[SW_COLUMN_NAME_KEY] = &schema_pb.Value{
			Kind: &schema_pb.Value_BytesValue{BytesValue: logEntry.Key},
		}
		recordValue.Fields[SW_COLUMN_NAME_VALUE] = &schema_pb.Value{
			Kind: &schema_pb.Value_BytesValue{BytesValue: logEntry.Data},
		}
		return recordValue, "live_log", nil
	}

	// CRITICAL: The broker stores DataMessage.Value directly in LogEntry.Data
	// So we need to try unmarshaling LogEntry.Data as RecordValue first
	var recordValueBytes []byte

	if dataMessage != nil && len(dataMessage.Value) > 0 {
		// DataMessage has a Value field - use it
		recordValueBytes = dataMessage.Value
	} else {
		// DataMessage doesn't have Value, use LogEntry.Data directly
		// This is the normal case when broker stores messages
		recordValueBytes = logEntry.Data
	}

	// Try to unmarshal as RecordValue
	if len(recordValueBytes) > 0 {
		recordValue := &schema_pb.RecordValue{}
		if err := proto.Unmarshal(recordValueBytes, recordValue); err == nil {
			// Successfully unmarshaled as RecordValue

			// Ensure Fields map exists
			if recordValue.Fields == nil {
				recordValue.Fields = make(map[string]*schema_pb.Value)
			}

			// Add system columns from LogEntry
			recordValue.Fields[SW_COLUMN_NAME_TIMESTAMP] = &schema_pb.Value{
				Kind: &schema_pb.Value_Int64Value{Int64Value: logEntry.TsNs},
			}
			recordValue.Fields[SW_COLUMN_NAME_KEY] = &schema_pb.Value{
				Kind: &schema_pb.Value_BytesValue{BytesValue: logEntry.Key},
			}

			return recordValue, "live_log", nil
		}
		// If unmarshaling as RecordValue fails, fall back to schema-aware parsing
	}

	// For cases where protobuf unmarshaling failed or data is empty,
	// attempt schema-aware parsing to try JSON, protobuf, and other formats
	return hms.parseRawMessageWithSchema(logEntry)
}

// parseJSONMessage attempts to parse raw data as JSON and map to schema fields
func (hms *HybridMessageScanner) parseJSONMessage(data []byte) (*schema_pb.RecordValue, error) {
	// Try to parse as JSON
	var jsonData map[string]interface{}
	if err := json.Unmarshal(data, &jsonData); err != nil {
		return nil, fmt.Errorf("not valid JSON: %v", err)
	}

	recordValue := &schema_pb.RecordValue{
		Fields: make(map[string]*schema_pb.Value),
	}

	// Map JSON fields to schema fields
	for _, schemaField := range hms.recordSchema.Fields {
		fieldName := schemaField.Name
		if jsonValue, exists := jsonData[fieldName]; exists {
			schemaValue, err := hms.convertJSONValueToSchemaValue(jsonValue, schemaField.Type)
			if err != nil {
				// Log conversion error but continue with other fields
				continue
			}
			recordValue.Fields[fieldName] = schemaValue
		}
	}

	return recordValue, nil
}

// parseProtobufMessage attempts to parse raw data as protobuf RecordValue
func (hms *HybridMessageScanner) parseProtobufMessage(data []byte) (*schema_pb.RecordValue, error) {
	// This might be a raw protobuf message that didn't parse correctly the first time
	// Try alternative protobuf unmarshaling approaches
	recordValue := &schema_pb.RecordValue{}

	// Strategy 1: Direct unmarshaling (might work if it's actually a RecordValue)
	if err := proto.Unmarshal(data, recordValue); err == nil {
		return recordValue, nil
	}

	// Strategy 2: Check if it's a different protobuf message type
	// For now, return error as we need more specific knowledge of MQ message formats
	return nil, fmt.Errorf("could not parse as protobuf RecordValue")
}

// convertRawDataToSchemaValue converts raw bytes to a specific schema type
func (hms *HybridMessageScanner) convertRawDataToSchemaValue(data []byte, fieldType *schema_pb.Type) (*schema_pb.Value, error) {
	dataStr := string(data)

	switch fieldType.Kind.(type) {
	case *schema_pb.Type_ScalarType:
		scalarType := fieldType.GetScalarType()
		switch scalarType {
		case schema_pb.ScalarType_STRING:
			return &schema_pb.Value{
				Kind: &schema_pb.Value_StringValue{StringValue: dataStr},
			}, nil
		case schema_pb.ScalarType_INT32:
			if val, err := strconv.ParseInt(strings.TrimSpace(dataStr), 10, 32); err == nil {
				return &schema_pb.Value{
					Kind: &schema_pb.Value_Int32Value{Int32Value: int32(val)},
				}, nil
			}
		case schema_pb.ScalarType_INT64:
			if val, err := strconv.ParseInt(strings.TrimSpace(dataStr), 10, 64); err == nil {
				return &schema_pb.Value{
					Kind: &schema_pb.Value_Int64Value{Int64Value: val},
				}, nil
			}
		case schema_pb.ScalarType_FLOAT:
			if val, err := strconv.ParseFloat(strings.TrimSpace(dataStr), 32); err == nil {
				return &schema_pb.Value{
					Kind: &schema_pb.Value_FloatValue{FloatValue: float32(val)},
				}, nil
			}
		case schema_pb.ScalarType_DOUBLE:
			if val, err := strconv.ParseFloat(strings.TrimSpace(dataStr), 64); err == nil {
				return &schema_pb.Value{
					Kind: &schema_pb.Value_DoubleValue{DoubleValue: val},
				}, nil
			}
		case schema_pb.ScalarType_BOOL:
			lowerStr := strings.ToLower(strings.TrimSpace(dataStr))
			if lowerStr == "true" || lowerStr == "1" || lowerStr == "yes" {
				return &schema_pb.Value{
					Kind: &schema_pb.Value_BoolValue{BoolValue: true},
				}, nil
			} else if lowerStr == "false" || lowerStr == "0" || lowerStr == "no" {
				return &schema_pb.Value{
					Kind: &schema_pb.Value_BoolValue{BoolValue: false},
				}, nil
			}
		case schema_pb.ScalarType_BYTES:
			return &schema_pb.Value{
				Kind: &schema_pb.Value_BytesValue{BytesValue: data},
			}, nil
		}
	}

	return nil, fmt.Errorf("unsupported type conversion for %v", fieldType)
}

// convertJSONValueToSchemaValue converts a JSON value to schema_pb.Value based on schema type
func (hms *HybridMessageScanner) convertJSONValueToSchemaValue(jsonValue interface{}, fieldType *schema_pb.Type) (*schema_pb.Value, error) {
	switch fieldType.Kind.(type) {
	case *schema_pb.Type_ScalarType:
		scalarType := fieldType.GetScalarType()
		switch scalarType {
		case schema_pb.ScalarType_STRING:
			if str, ok := jsonValue.(string); ok {
				return &schema_pb.Value{
					Kind: &schema_pb.Value_StringValue{StringValue: str},
				}, nil
			}
			// Convert other types to string
			return &schema_pb.Value{
				Kind: &schema_pb.Value_StringValue{StringValue: fmt.Sprintf("%v", jsonValue)},
			}, nil
		case schema_pb.ScalarType_INT32:
			if num, ok := jsonValue.(float64); ok { // JSON numbers are float64
				return &schema_pb.Value{
					Kind: &schema_pb.Value_Int32Value{Int32Value: int32(num)},
				}, nil
			}
		case schema_pb.ScalarType_INT64:
			if num, ok := jsonValue.(float64); ok {
				return &schema_pb.Value{
					Kind: &schema_pb.Value_Int64Value{Int64Value: int64(num)},
				}, nil
			}
		case schema_pb.ScalarType_FLOAT:
			if num, ok := jsonValue.(float64); ok {
				return &schema_pb.Value{
					Kind: &schema_pb.Value_FloatValue{FloatValue: float32(num)},
				}, nil
			}
		case schema_pb.ScalarType_DOUBLE:
			if num, ok := jsonValue.(float64); ok {
				return &schema_pb.Value{
					Kind: &schema_pb.Value_DoubleValue{DoubleValue: num},
				}, nil
			}
		case schema_pb.ScalarType_BOOL:
			if boolVal, ok := jsonValue.(bool); ok {
				return &schema_pb.Value{
					Kind: &schema_pb.Value_BoolValue{BoolValue: boolVal},
				}, nil
			}
		case schema_pb.ScalarType_BYTES:
			if str, ok := jsonValue.(string); ok {
				return &schema_pb.Value{
					Kind: &schema_pb.Value_BytesValue{BytesValue: []byte(str)},
				}, nil
			}
		}
	}

	return nil, fmt.Errorf("incompatible JSON value type %T for schema type %v", jsonValue, fieldType)
}

// ConvertToSQLResult converts HybridScanResults to SQL query results
func (hms *HybridMessageScanner) ConvertToSQLResult(results []HybridScanResult, columns []string) *QueryResult {
	if len(results) == 0 {
		return &QueryResult{
			Columns:  columns,
			Rows:     [][]sqltypes.Value{},
			Database: hms.topic.Namespace,
			Table:    hms.topic.Name,
		}
	}

	// Determine columns if not specified
	if len(columns) == 0 {
		columnSet := make(map[string]bool)
		for _, result := range results {
			for columnName := range result.Values {
				columnSet[columnName] = true
			}
		}

		columns = make([]string, 0, len(columnSet))
		for columnName := range columnSet {
			columns = append(columns, columnName)
		}

		// If no data columns were found, include system columns so we have something to display
		if len(columns) == 0 {
			columns = []string{SW_DISPLAY_NAME_TIMESTAMP, SW_COLUMN_NAME_KEY}
		}
	}

	// Convert to SQL rows
	rows := make([][]sqltypes.Value, len(results))
	for i, result := range results {
		row := make([]sqltypes.Value, len(columns))
		for j, columnName := range columns {
			switch columnName {
			case SW_COLUMN_NAME_SOURCE:
				row[j] = sqltypes.NewVarChar(result.Source)
			case SW_COLUMN_NAME_TIMESTAMP, SW_DISPLAY_NAME_TIMESTAMP:
				// Format timestamp as proper timestamp type instead of raw nanoseconds
				row[j] = hms.engine.formatTimestampColumn(result.Timestamp)
			case SW_COLUMN_NAME_KEY:
				row[j] = sqltypes.NewVarBinary(string(result.Key))
			default:
				if value, exists := result.Values[columnName]; exists {
					row[j] = convertSchemaValueToSQL(value)
				} else {
					row[j] = sqltypes.NULL
				}
			}
		}
		rows[i] = row
	}

	return &QueryResult{
		Columns:  columns,
		Rows:     rows,
		Database: hms.topic.Namespace,
		Table:    hms.topic.Name,
	}
}

// ConvertToSQLResultWithMixedColumns handles SELECT *, specific_columns queries
// Combines auto-discovered columns (from *) with explicitly requested columns
func (hms *HybridMessageScanner) ConvertToSQLResultWithMixedColumns(results []HybridScanResult, explicitColumns []string) *QueryResult {
	if len(results) == 0 {
		// For empty results, combine auto-discovered columns with explicit ones
		columnSet := make(map[string]bool)

		// Add explicit columns first
		for _, col := range explicitColumns {
			columnSet[col] = true
		}

		// Build final column list
		columns := make([]string, 0, len(columnSet))
		for col := range columnSet {
			columns = append(columns, col)
		}

		return &QueryResult{
			Columns:  columns,
			Rows:     [][]sqltypes.Value{},
			Database: hms.topic.Namespace,
			Table:    hms.topic.Name,
		}
	}

	// Auto-discover columns from data (like SELECT *)
	autoColumns := make(map[string]bool)
	for _, result := range results {
		for columnName := range result.Values {
			autoColumns[columnName] = true
		}
	}

	// Combine auto-discovered and explicit columns
	columnSet := make(map[string]bool)

	// Add auto-discovered columns first (regular data columns)
	for col := range autoColumns {
		columnSet[col] = true
	}

	// Add explicit columns (may include system columns like _source)
	for _, col := range explicitColumns {
		columnSet[col] = true
	}

	// Build final column list
	columns := make([]string, 0, len(columnSet))
	for col := range columnSet {
		columns = append(columns, col)
	}

	// If no data columns were found and no explicit columns specified, include system columns
	if len(columns) == 0 {
		columns = []string{SW_DISPLAY_NAME_TIMESTAMP, SW_COLUMN_NAME_KEY}
	}

	// Convert to SQL rows
	rows := make([][]sqltypes.Value, len(results))
	for i, result := range results {
		row := make([]sqltypes.Value, len(columns))
		for j, columnName := range columns {
			switch columnName {
			case SW_COLUMN_NAME_TIMESTAMP:
				row[j] = sqltypes.NewInt64(result.Timestamp)
			case SW_COLUMN_NAME_KEY:
				row[j] = sqltypes.NewVarBinary(string(result.Key))
			case SW_COLUMN_NAME_SOURCE:
				row[j] = sqltypes.NewVarChar(result.Source)
			default:
				// Regular data column
				if value, exists := result.Values[columnName]; exists {
					row[j] = convertSchemaValueToSQL(value)
				} else {
					row[j] = sqltypes.NULL
				}
			}
		}
		rows[i] = row
	}

	return &QueryResult{
		Columns:  columns,
		Rows:     rows,
		Database: hms.topic.Namespace,
		Table:    hms.topic.Name,
	}
}

// ReadParquetStatistics efficiently reads column statistics from parquet files
// without scanning the full file content - uses parquet's built-in metadata
func (h *HybridMessageScanner) ReadParquetStatistics(partitionPath string) ([]*ParquetFileStats, error) {
	var fileStats []*ParquetFileStats

	// Use the same chunk cache as the logstore package
	chunkCache := chunk_cache.NewChunkCacheInMemory(256)
	lookupFileIdFn := filer.LookupFn(h.filerClient)

	err := filer_pb.ReadDirAllEntries(context.Background(), h.filerClient, util.FullPath(partitionPath), "", func(entry *filer_pb.Entry, isLast bool) error {
		// Only process parquet files
		if entry.IsDirectory || !strings.HasSuffix(entry.Name, ".parquet") {
			return nil
		}

		// Extract statistics from this parquet file
		stats, err := h.extractParquetFileStats(entry, lookupFileIdFn, chunkCache)
		if err != nil {
			// Log error but continue processing other files
			fmt.Printf("Warning: failed to extract stats from %s: %v\n", entry.Name, err)
			return nil
		}

		if stats != nil {
			fileStats = append(fileStats, stats)
		}
		return nil
	})

	return fileStats, err
}

// extractParquetFileStats extracts column statistics from a single parquet file
func (h *HybridMessageScanner) extractParquetFileStats(entry *filer_pb.Entry, lookupFileIdFn wdclient.LookupFileIdFunctionType, chunkCache *chunk_cache.ChunkCacheInMemory) (*ParquetFileStats, error) {
	// Create reader for the parquet file
	fileSize := filer.FileSize(entry)
	visibleIntervals, _ := filer.NonOverlappingVisibleIntervals(context.Background(), lookupFileIdFn, entry.Chunks, 0, int64(fileSize))
	chunkViews := filer.ViewFromVisibleIntervals(visibleIntervals, 0, int64(fileSize))
	readerCache := filer.NewReaderCache(32, chunkCache, lookupFileIdFn)
	readerAt := filer.NewChunkReaderAtFromClient(context.Background(), readerCache, chunkViews, int64(fileSize))

	// Create parquet reader - this only reads metadata, not data
	parquetReader := parquet.NewReader(readerAt)
	defer parquetReader.Close()

	fileView := parquetReader.File()

	fileStats := &ParquetFileStats{
		FileName:    entry.Name,
		RowCount:    fileView.NumRows(),
		ColumnStats: make(map[string]*ParquetColumnStats),
	}
	// Populate optional min/max from filer extended attributes (writer stores ns timestamps)
	if entry != nil && entry.Extended != nil {
		if minBytes, ok := entry.Extended[mq.ExtendedAttrTimestampMin]; ok && len(minBytes) == 8 {
			fileStats.MinTimestampNs = int64(binary.BigEndian.Uint64(minBytes))
		}
		if maxBytes, ok := entry.Extended[mq.ExtendedAttrTimestampMax]; ok && len(maxBytes) == 8 {
			fileStats.MaxTimestampNs = int64(binary.BigEndian.Uint64(maxBytes))
		}
	}

	// Get schema information
	schema := fileView.Schema()

	// Process each row group
	rowGroups := fileView.RowGroups()
	for _, rowGroup := range rowGroups {
		columnChunks := rowGroup.ColumnChunks()

		// Process each column chunk
		for i, chunk := range columnChunks {
			// Get column name from schema
			columnName := h.getColumnNameFromSchema(schema, i)
			if columnName == "" {
				continue
			}

			// Try to get column statistics
			columnIndex, err := chunk.ColumnIndex()
			if err != nil {
				// No column index available - skip this column
				continue
			}

			// Extract min/max values from the first page (for simplicity)
			// In a more sophisticated implementation, we could aggregate across all pages
			numPages := columnIndex.NumPages()
			if numPages == 0 {
				continue
			}

			minParquetValue := columnIndex.MinValue(0)
			maxParquetValue := columnIndex.MaxValue(numPages - 1)
			nullCount := int64(0)

			// Aggregate null counts across all pages
			for pageIdx := 0; pageIdx < numPages; pageIdx++ {
				nullCount += columnIndex.NullCount(pageIdx)
			}

			// Convert parquet values to schema_pb.Value
			minValue, err := h.convertParquetValueToSchemaValue(minParquetValue)
			if err != nil {
				continue
			}

			maxValue, err := h.convertParquetValueToSchemaValue(maxParquetValue)
			if err != nil {
				continue
			}

			// Store column statistics (aggregate across row groups if column already exists)
			if existingStats, exists := fileStats.ColumnStats[columnName]; exists {
				// Update existing statistics
				if h.compareSchemaValues(minValue, existingStats.MinValue) < 0 {
					existingStats.MinValue = minValue
				}
				if h.compareSchemaValues(maxValue, existingStats.MaxValue) > 0 {
					existingStats.MaxValue = maxValue
				}
				existingStats.NullCount += nullCount
			} else {
				// Create new column statistics
				fileStats.ColumnStats[columnName] = &ParquetColumnStats{
					ColumnName: columnName,
					MinValue:   minValue,
					MaxValue:   maxValue,
					NullCount:  nullCount,
					RowCount:   rowGroup.NumRows(),
				}
			}
		}
	}

	return fileStats, nil
}

// getColumnNameFromSchema extracts column name from parquet schema by index
func (h *HybridMessageScanner) getColumnNameFromSchema(schema *parquet.Schema, columnIndex int) string {
	// Get the leaf columns in order
	var columnNames []string
	h.collectColumnNames(schema.Fields(), &columnNames)

	if columnIndex >= 0 && columnIndex < len(columnNames) {
		return columnNames[columnIndex]
	}
	return ""
}

// collectColumnNames recursively collects leaf column names from schema
func (h *HybridMessageScanner) collectColumnNames(fields []parquet.Field, names *[]string) {
	for _, field := range fields {
		if len(field.Fields()) == 0 {
			// This is a leaf field (no sub-fields)
			*names = append(*names, field.Name())
		} else {
			// This is a group - recurse
			h.collectColumnNames(field.Fields(), names)
		}
	}
}

// convertParquetValueToSchemaValue converts parquet.Value to schema_pb.Value
func (h *HybridMessageScanner) convertParquetValueToSchemaValue(pv parquet.Value) (*schema_pb.Value, error) {
	switch pv.Kind() {
	case parquet.Boolean:
		return &schema_pb.Value{Kind: &schema_pb.Value_BoolValue{BoolValue: pv.Boolean()}}, nil
	case parquet.Int32:
		return &schema_pb.Value{Kind: &schema_pb.Value_Int32Value{Int32Value: pv.Int32()}}, nil
	case parquet.Int64:
		return &schema_pb.Value{Kind: &schema_pb.Value_Int64Value{Int64Value: pv.Int64()}}, nil
	case parquet.Float:
		return &schema_pb.Value{Kind: &schema_pb.Value_FloatValue{FloatValue: pv.Float()}}, nil
	case parquet.Double:
		return &schema_pb.Value{Kind: &schema_pb.Value_DoubleValue{DoubleValue: pv.Double()}}, nil
	case parquet.ByteArray:
		return &schema_pb.Value{Kind: &schema_pb.Value_BytesValue{BytesValue: pv.ByteArray()}}, nil
	default:
		return nil, fmt.Errorf("unsupported parquet value kind: %v", pv.Kind())
	}
}

// compareSchemaValues compares two schema_pb.Value objects
func (h *HybridMessageScanner) compareSchemaValues(v1, v2 *schema_pb.Value) int {
	if v1 == nil && v2 == nil {
		return 0
	}
	if v1 == nil {
		return -1
	}
	if v2 == nil {
		return 1
	}

	// Extract raw values and compare
	raw1 := h.extractRawValueFromSchema(v1)
	raw2 := h.extractRawValueFromSchema(v2)

	return h.compareRawValues(raw1, raw2)
}

// extractRawValueFromSchema extracts the raw value from schema_pb.Value
func (h *HybridMessageScanner) extractRawValueFromSchema(value *schema_pb.Value) interface{} {
	switch v := value.Kind.(type) {
	case *schema_pb.Value_BoolValue:
		return v.BoolValue
	case *schema_pb.Value_Int32Value:
		return v.Int32Value
	case *schema_pb.Value_Int64Value:
		return v.Int64Value
	case *schema_pb.Value_FloatValue:
		return v.FloatValue
	case *schema_pb.Value_DoubleValue:
		return v.DoubleValue
	case *schema_pb.Value_BytesValue:
		return string(v.BytesValue) // Convert to string for comparison
	case *schema_pb.Value_StringValue:
		return v.StringValue
	}
	return nil
}

// compareRawValues compares two raw values
func (h *HybridMessageScanner) compareRawValues(v1, v2 interface{}) int {
	// Handle nil cases
	if v1 == nil && v2 == nil {
		return 0
	}
	if v1 == nil {
		return -1
	}
	if v2 == nil {
		return 1
	}

	// Compare based on type
	switch val1 := v1.(type) {
	case bool:
		if val2, ok := v2.(bool); ok {
			if val1 == val2 {
				return 0
			}
			if val1 {
				return 1
			}
			return -1
		}
	case int32:
		if val2, ok := v2.(int32); ok {
			if val1 < val2 {
				return -1
			} else if val1 > val2 {
				return 1
			}
			return 0
		}
	case int64:
		if val2, ok := v2.(int64); ok {
			if val1 < val2 {
				return -1
			} else if val1 > val2 {
				return 1
			}
			return 0
		}
	case float32:
		if val2, ok := v2.(float32); ok {
			if val1 < val2 {
				return -1
			} else if val1 > val2 {
				return 1
			}
			return 0
		}
	case float64:
		if val2, ok := v2.(float64); ok {
			if val1 < val2 {
				return -1
			} else if val1 > val2 {
				return 1
			}
			return 0
		}
	case string:
		if val2, ok := v2.(string); ok {
			if val1 < val2 {
				return -1
			} else if val1 > val2 {
				return 1
			}
			return 0
		}
	}

	// Default: try string comparison
	str1 := fmt.Sprintf("%v", v1)
	str2 := fmt.Sprintf("%v", v2)
	if str1 < str2 {
		return -1
	} else if str1 > str2 {
		return 1
	}
	return 0
}

// streamingMerge merges multiple sorted data sources using a heap-based approach
// This provides memory-efficient merging without loading all data into memory
func (hms *HybridMessageScanner) streamingMerge(dataSources []StreamingDataSource, limit int) ([]HybridScanResult, error) {
	if len(dataSources) == 0 {
		return nil, nil
	}

	var results []HybridScanResult
	mergeHeap := &StreamingMergeHeap{}
	heap.Init(mergeHeap)

	// Initialize heap with first item from each data source
	for i, source := range dataSources {
		if source.HasMore() {
			result, err := source.Next()
			if err != nil {
				// Close all sources and return error
				for _, s := range dataSources {
					s.Close()
				}
				return nil, fmt.Errorf("failed to read from data source %d: %v", i, err)
			}
			if result != nil {
				heap.Push(mergeHeap, &StreamingMergeItem{
					Result:     result,
					SourceID:   i,
					DataSource: source,
				})
			}
		}
	}

	// Process results in chronological order
	for mergeHeap.Len() > 0 {
		// Get next chronologically ordered result
		item := heap.Pop(mergeHeap).(*StreamingMergeItem)
		results = append(results, *item.Result)

		// Check limit
		if limit > 0 && len(results) >= limit {
			break
		}

		// Try to get next item from the same data source
		if item.DataSource.HasMore() {
			nextResult, err := item.DataSource.Next()
			if err != nil {
				// Log error but continue with other sources
				fmt.Printf("Warning: Error reading next item from source %d: %v\n", item.SourceID, err)
			} else if nextResult != nil {
				heap.Push(mergeHeap, &StreamingMergeItem{
					Result:     nextResult,
					SourceID:   item.SourceID,
					DataSource: item.DataSource,
				})
			}
		}
	}

	// Close all data sources
	for _, source := range dataSources {
		source.Close()
	}

	return results, nil
}

// SliceDataSource wraps a pre-loaded slice of results as a StreamingDataSource
// This is used for unflushed data that is already loaded into memory
type SliceDataSource struct {
	results []HybridScanResult
	index   int
}

func NewSliceDataSource(results []HybridScanResult) *SliceDataSource {
	return &SliceDataSource{
		results: results,
		index:   0,
	}
}

func (s *SliceDataSource) Next() (*HybridScanResult, error) {
	if s.index >= len(s.results) {
		return nil, nil
	}
	result := &s.results[s.index]
	s.index++
	return result, nil
}

func (s *SliceDataSource) HasMore() bool {
	return s.index < len(s.results)
}

func (s *SliceDataSource) Close() error {
	return nil // Nothing to clean up for slice-based source
}

// StreamingFlushedDataSource provides streaming access to flushed data
type StreamingFlushedDataSource struct {
	hms          *HybridMessageScanner
	partition    topic.Partition
	options      HybridScanOptions
	mergedReadFn func(startPosition log_buffer.MessagePosition, stopTsNs int64, eachLogEntryFn log_buffer.EachLogEntryFuncType) (lastReadPosition log_buffer.MessagePosition, isDone bool, err error)
	resultChan   chan *HybridScanResult
	errorChan    chan error
	doneChan     chan struct{}
	started      bool
	finished     bool
	closed       int32 // atomic flag to prevent double close
	mu           sync.RWMutex
}

func NewStreamingFlushedDataSource(hms *HybridMessageScanner, partition topic.Partition, options HybridScanOptions) *StreamingFlushedDataSource {
	mergedReadFn := logstore.GenMergedReadFunc(hms.filerClient, hms.topic, partition)

	return &StreamingFlushedDataSource{
		hms:          hms,
		partition:    partition,
		options:      options,
		mergedReadFn: mergedReadFn,
		resultChan:   make(chan *HybridScanResult, 100), // Buffer for better performance
		errorChan:    make(chan error, 1),
		doneChan:     make(chan struct{}),
		started:      false,
		finished:     false,
	}
}

func (s *StreamingFlushedDataSource) startStreaming() {
	if s.started {
		return
	}
	s.started = true

	go func() {
		defer func() {
			// Use atomic flag to ensure channels are only closed once
			if atomic.CompareAndSwapInt32(&s.closed, 0, 1) {
				close(s.resultChan)
				close(s.errorChan)
				close(s.doneChan)
			}
		}()

		// Set up time range for scanning
		startTime := time.Unix(0, s.options.StartTimeNs)
		if s.options.StartTimeNs == 0 {
			startTime = time.Unix(0, 0)
		}

		stopTsNs := s.options.StopTimeNs
		// For SQL queries, stopTsNs = 0 means "no stop time restriction"
		// This is different from message queue consumers which want to stop at "now"
		// We detect SQL context by checking if we have a predicate function
		if stopTsNs == 0 && s.options.Predicate == nil {
			// Only set to current time for non-SQL queries (message queue consumers)
			stopTsNs = time.Now().UnixNano()
		}
		// If stopTsNs is still 0, it means this is a SQL query that wants unrestricted scanning

		// Message processing function
		eachLogEntryFn := func(logEntry *filer_pb.LogEntry) (isDone bool, err error) {
			// Pre-decode DataMessage for reuse in both control check and conversion
			var dataMessage *mq_pb.DataMessage
			if len(logEntry.Data) > 0 {
				dataMessage = &mq_pb.DataMessage{}
				if err := proto.Unmarshal(logEntry.Data, dataMessage); err != nil {
					dataMessage = nil // Failed to decode, treat as raw data
				}
			}

			// Skip control entries without actual data
			if s.hms.isControlEntryWithDecoded(logEntry, dataMessage) {
				return false, nil // Skip this entry
			}

			// Convert log entry to schema_pb.RecordValue for consistent processing
			recordValue, source, convertErr := s.hms.convertLogEntryToRecordValueWithDecoded(logEntry, dataMessage)
			if convertErr != nil {
				return false, fmt.Errorf("failed to convert log entry: %v", convertErr)
			}

			// Apply predicate filtering (WHERE clause)
			if s.options.Predicate != nil && !s.options.Predicate(recordValue) {
				return false, nil // Skip this message
			}

			// Extract system columns
			timestamp := recordValue.Fields[SW_COLUMN_NAME_TIMESTAMP].GetInt64Value()
			key := recordValue.Fields[SW_COLUMN_NAME_KEY].GetBytesValue()

			// Apply column projection
			values := make(map[string]*schema_pb.Value)
			if len(s.options.Columns) == 0 {
				// Select all columns (excluding system columns from user view)
				for name, value := range recordValue.Fields {
					if name != SW_COLUMN_NAME_TIMESTAMP && name != SW_COLUMN_NAME_KEY {
						values[name] = value
					}
				}
			} else {
				// Select specified columns only
				for _, columnName := range s.options.Columns {
					if value, exists := recordValue.Fields[columnName]; exists {
						values[columnName] = value
					}
				}
			}

			result := &HybridScanResult{
				Values:    values,
				Timestamp: timestamp,
				Key:       key,
				Source:    source,
			}

			// Check if already closed before trying to send
			if atomic.LoadInt32(&s.closed) != 0 {
				return true, nil // Stop processing if closed
			}

			// Send result to channel with proper handling of closed channels
			select {
			case s.resultChan <- result:
				return false, nil
			case <-s.doneChan:
				return true, nil // Stop processing if closed
			default:
				// Check again if closed (in case it was closed between the atomic check and select)
				if atomic.LoadInt32(&s.closed) != 0 {
					return true, nil
				}
				// If not closed, try sending again with blocking select
				select {
				case s.resultChan <- result:
					return false, nil
				case <-s.doneChan:
					return true, nil
				}
			}
		}

		// Start scanning from the specified position
		startPosition := log_buffer.MessagePosition{Time: startTime}
		_, _, err := s.mergedReadFn(startPosition, stopTsNs, eachLogEntryFn)

		if err != nil {
			// Only try to send error if not already closed
			if atomic.LoadInt32(&s.closed) == 0 {
				select {
				case s.errorChan <- fmt.Errorf("flushed data scan failed: %v", err):
				case <-s.doneChan:
				default:
					// Channel might be full or closed, ignore
				}
			}
		}

		s.finished = true
	}()
}

func (s *StreamingFlushedDataSource) Next() (*HybridScanResult, error) {
	if !s.started {
		s.startStreaming()
	}

	select {
	case result, ok := <-s.resultChan:
		if !ok {
			return nil, nil // No more results
		}
		return result, nil
	case err := <-s.errorChan:
		return nil, err
	case <-s.doneChan:
		return nil, nil
	}
}

func (s *StreamingFlushedDataSource) HasMore() bool {
	if !s.started {
		return true // Haven't started yet, so potentially has data
	}
	return !s.finished || len(s.resultChan) > 0
}

func (s *StreamingFlushedDataSource) Close() error {
	// Use atomic flag to ensure channels are only closed once
	if atomic.CompareAndSwapInt32(&s.closed, 0, 1) {
		close(s.doneChan)
		close(s.resultChan)
		close(s.errorChan)
	}
	return nil
}

// mergeSort efficiently sorts HybridScanResult slice by timestamp using merge sort algorithm
func (hms *HybridMessageScanner) mergeSort(results []HybridScanResult, left, right int) {
	if left < right {
		mid := left + (right-left)/2

		// Recursively sort both halves
		hms.mergeSort(results, left, mid)
		hms.mergeSort(results, mid+1, right)

		// Merge the sorted halves
		hms.merge(results, left, mid, right)
	}
}

// merge combines two sorted subarrays into a single sorted array
func (hms *HybridMessageScanner) merge(results []HybridScanResult, left, mid, right int) {
	// Create temporary arrays for the two subarrays
	leftArray := make([]HybridScanResult, mid-left+1)
	rightArray := make([]HybridScanResult, right-mid)

	// Copy data to temporary arrays
	copy(leftArray, results[left:mid+1])
	copy(rightArray, results[mid+1:right+1])

	// Merge the temporary arrays back into results[left..right]
	i, j, k := 0, 0, left

	for i < len(leftArray) && j < len(rightArray) {
		if leftArray[i].Timestamp <= rightArray[j].Timestamp {
			results[k] = leftArray[i]
			i++
		} else {
			results[k] = rightArray[j]
			j++
		}
		k++
	}

	// Copy remaining elements of leftArray, if any
	for i < len(leftArray) {
		results[k] = leftArray[i]
		i++
		k++
	}

	// Copy remaining elements of rightArray, if any
	for j < len(rightArray) {
		results[k] = rightArray[j]
		j++
		k++
	}
}
