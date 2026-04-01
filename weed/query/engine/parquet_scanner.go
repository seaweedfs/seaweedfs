package engine

import (
	"context"
	"fmt"
	"math/big"
	"time"

	"github.com/parquet-go/parquet-go"
	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/mq/schema"
	"github.com/seaweedfs/seaweedfs/weed/mq/topic"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/mq_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/schema_pb"
	"github.com/seaweedfs/seaweedfs/weed/query/sqltypes"
	"github.com/seaweedfs/seaweedfs/weed/util/chunk_cache"
)

// ParquetScanner scans MQ topic Parquet files for SELECT queries
// Assumptions:
// 1. All MQ messages are stored in Parquet format in topic partitions
// 2. Each partition directory contains dated Parquet files
// 3. System columns (_ts_ns, _key) are added to user schema
// 4. Predicate pushdown is used for efficient scanning
type ParquetScanner struct {
	filerClient   filer_pb.FilerClient
	chunkCache    chunk_cache.ChunkCache
	topic         topic.Topic
	recordSchema  *schema_pb.RecordType
	parquetLevels *schema.ParquetLevels
}

// NewParquetScanner creates a scanner for a specific MQ topic
// Assumption: Topic exists and has Parquet files in partition directories
func NewParquetScanner(filerClient filer_pb.FilerClient, namespace, topicName string) (*ParquetScanner, error) {
	// Check if filerClient is available
	if filerClient == nil {
		return nil, fmt.Errorf("filerClient is required but not available")
	}

	// Create topic reference
	t := topic.Topic{
		Namespace: namespace,
		Name:      topicName,
	}

	// Read topic configuration to get schema
	var topicConf *mq_pb.ConfigureTopicResponse
	var err error
	if err := filerClient.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		topicConf, err = t.ReadConfFile(client)
		return err
	}); err != nil {
		return nil, fmt.Errorf("failed to read topic config: %v", err)
	}

	// Build complete schema with system columns - prefer flat schema if available
	var recordType *schema_pb.RecordType

	if topicConf.GetMessageRecordType() != nil {
		// New flat schema format - use directly
		recordType = topicConf.GetMessageRecordType()
	}

	if recordType == nil || len(recordType.Fields) == 0 {
		// For topics without schema, create a minimal schema with system fields and _value
		recordType = schema.RecordTypeBegin().
			WithField(SW_COLUMN_NAME_TIMESTAMP, schema.TypeInt64).
			WithField(SW_COLUMN_NAME_KEY, schema.TypeBytes).
			WithField(SW_COLUMN_NAME_VALUE, schema.TypeBytes). // Raw message value
			RecordTypeEnd()
	} else {
		// Add system columns that MQ adds to all records
		recordType = schema.NewRecordTypeBuilder(recordType).
			WithField(SW_COLUMN_NAME_TIMESTAMP, schema.TypeInt64).
			WithField(SW_COLUMN_NAME_KEY, schema.TypeBytes).
			RecordTypeEnd()
	}

	// Convert to Parquet levels for efficient reading
	parquetLevels, err := schema.ToParquetLevels(recordType)
	if err != nil {
		return nil, fmt.Errorf("failed to create Parquet levels: %v", err)
	}

	return &ParquetScanner{
		filerClient:   filerClient,
		chunkCache:    chunk_cache.NewChunkCacheInMemory(256), // Same as MQ logstore
		topic:         t,
		recordSchema:  recordType,
		parquetLevels: parquetLevels,
	}, nil
}

// ScanOptions configure how the scanner reads data
type ScanOptions struct {
	// Time range filtering (Unix nanoseconds)
	StartTimeNs int64
	StopTimeNs  int64

	// Column projection - if empty, select all columns
	Columns []string

	// Row limit - 0 means no limit
	Limit int

	// Predicate for WHERE clause filtering
	Predicate func(*schema_pb.RecordValue) bool
}

// ScanResult represents a single scanned record
type ScanResult struct {
	Values    map[string]*schema_pb.Value // Column name -> value
	Timestamp int64                       // Message timestamp (_ts_ns)
	Key       []byte                      // Message key (_key)
}

// Scan reads records from the topic's Parquet files
// Assumptions:
// 1. Scans all partitions of the topic
// 2. Applies time filtering at Parquet level for efficiency
// 3. Applies predicates and projections after reading
func (ps *ParquetScanner) Scan(ctx context.Context, options ScanOptions) ([]ScanResult, error) {
	var results []ScanResult

	// Get all partitions for this topic
	// TODO: Implement proper partition discovery
	// For now, assume partition 0 exists
	partitions := []topic.Partition{{RangeStart: 0, RangeStop: 1000}}

	for _, partition := range partitions {
		partitionResults, err := ps.scanPartition(ctx, partition, options)
		if err != nil {
			return nil, fmt.Errorf("failed to scan partition %v: %v", partition, err)
		}

		results = append(results, partitionResults...)

		// Apply global limit across all partitions
		if options.Limit > 0 && len(results) >= options.Limit {
			results = results[:options.Limit]
			break
		}
	}

	return results, nil
}

// scanPartition scans a specific topic partition
func (ps *ParquetScanner) scanPartition(ctx context.Context, partition topic.Partition, options ScanOptions) ([]ScanResult, error) {
	// partitionDir := topic.PartitionDir(ps.topic, partition) // TODO: Use for actual file listing

	var results []ScanResult

	// List Parquet files in partition directory
	// TODO: Implement proper file listing with date range filtering
	// For now, this is a placeholder that would list actual Parquet files

	// Simulate file processing - in real implementation, this would:
	// 1. List files in partitionDir via filerClient
	// 2. Filter files by date range if time filtering is enabled
	// 3. Process each Parquet file in chronological order

	// Placeholder: Create sample data for testing
	if len(results) == 0 {
		// Generate sample data for demonstration
		sampleData := ps.generateSampleData(options)
		results = append(results, sampleData...)
	}

	return results, nil
}

// scanParquetFile scans a single Parquet file (real implementation)
func (ps *ParquetScanner) scanParquetFile(ctx context.Context, entry *filer_pb.Entry, options ScanOptions) ([]ScanResult, error) {
	var results []ScanResult

	// Create reader for the Parquet file (same pattern as logstore)
	lookupFileIdFn := filer.LookupFn(ps.filerClient)
	fileSize := filer.FileSize(entry)
	visibleIntervals, _ := filer.NonOverlappingVisibleIntervals(ctx, lookupFileIdFn, entry.Chunks, 0, int64(fileSize))
	chunkViews := filer.ViewFromVisibleIntervals(visibleIntervals, 0, int64(fileSize))
	readerCache := filer.NewReaderCache(32, ps.chunkCache, lookupFileIdFn)
	readerAt := filer.NewChunkReaderAtFromClient(ctx, readerCache, chunkViews, int64(fileSize), filer.DefaultPrefetchCount)

	// Create Parquet reader
	parquetReader := parquet.NewReader(readerAt)
	defer parquetReader.Close()

	rows := make([]parquet.Row, 128) // Read in batches like logstore

	for {
		rowCount, readErr := parquetReader.ReadRows(rows)

		// Process rows even if EOF
		for i := 0; i < rowCount; i++ {
			// Convert Parquet row to schema value
			recordValue, err := schema.ToRecordValue(ps.recordSchema, ps.parquetLevels, rows[i])
			if err != nil {
				return nil, fmt.Errorf("failed to convert row: %v", err)
			}

			// Extract system columns
			timestamp := recordValue.Fields[SW_COLUMN_NAME_TIMESTAMP].GetInt64Value()
			key := recordValue.Fields[SW_COLUMN_NAME_KEY].GetBytesValue()

			// Apply time filtering
			if options.StartTimeNs > 0 && timestamp < options.StartTimeNs {
				continue
			}
			if options.StopTimeNs > 0 && timestamp >= options.StopTimeNs {
				break // Assume data is time-ordered
			}

			// Apply predicate filtering (WHERE clause)
			if options.Predicate != nil && !options.Predicate(recordValue) {
				continue
			}

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

			results = append(results, ScanResult{
				Values:    values,
				Timestamp: timestamp,
				Key:       key,
			})

			// Apply row limit
			if options.Limit > 0 && len(results) >= options.Limit {
				return results, nil
			}
		}

		if readErr != nil {
			break // EOF or error
		}
	}

	return results, nil
}

// generateSampleData creates sample data for testing when no real Parquet files exist
func (ps *ParquetScanner) generateSampleData(options ScanOptions) []ScanResult {
	now := time.Now().UnixNano()

	sampleData := []ScanResult{
		{
			Values: map[string]*schema_pb.Value{
				"user_id":    {Kind: &schema_pb.Value_Int32Value{Int32Value: 1001}},
				"event_type": {Kind: &schema_pb.Value_StringValue{StringValue: "login"}},
				"data":       {Kind: &schema_pb.Value_StringValue{StringValue: `{"ip": "192.168.1.1"}`}},
			},
			Timestamp: now - 3600000000000, // 1 hour ago
			Key:       []byte("user-1001"),
		},
		{
			Values: map[string]*schema_pb.Value{
				"user_id":    {Kind: &schema_pb.Value_Int32Value{Int32Value: 1002}},
				"event_type": {Kind: &schema_pb.Value_StringValue{StringValue: "page_view"}},
				"data":       {Kind: &schema_pb.Value_StringValue{StringValue: `{"page": "/dashboard"}`}},
			},
			Timestamp: now - 1800000000000, // 30 minutes ago
			Key:       []byte("user-1002"),
		},
		{
			Values: map[string]*schema_pb.Value{
				"user_id":    {Kind: &schema_pb.Value_Int32Value{Int32Value: 1001}},
				"event_type": {Kind: &schema_pb.Value_StringValue{StringValue: "logout"}},
				"data":       {Kind: &schema_pb.Value_StringValue{StringValue: `{"session_duration": 3600}`}},
			},
			Timestamp: now - 900000000000, // 15 minutes ago
			Key:       []byte("user-1001"),
		},
	}

	// Apply predicate filtering if specified
	if options.Predicate != nil {
		var filtered []ScanResult
		for _, result := range sampleData {
			// Convert to RecordValue for predicate testing
			recordValue := &schema_pb.RecordValue{Fields: make(map[string]*schema_pb.Value)}
			for k, v := range result.Values {
				recordValue.Fields[k] = v
			}
			recordValue.Fields[SW_COLUMN_NAME_TIMESTAMP] = &schema_pb.Value{Kind: &schema_pb.Value_Int64Value{Int64Value: result.Timestamp}}
			recordValue.Fields[SW_COLUMN_NAME_KEY] = &schema_pb.Value{Kind: &schema_pb.Value_BytesValue{BytesValue: result.Key}}

			if options.Predicate(recordValue) {
				filtered = append(filtered, result)
			}
		}
		sampleData = filtered
	}

	// Apply limit
	if options.Limit > 0 && len(sampleData) > options.Limit {
		sampleData = sampleData[:options.Limit]
	}

	return sampleData
}

// ConvertToSQLResult converts ScanResults to SQL query results
func (ps *ParquetScanner) ConvertToSQLResult(results []ScanResult, columns []string) *QueryResult {
	if len(results) == 0 {
		return &QueryResult{
			Columns: columns,
			Rows:    [][]sqltypes.Value{},
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
	}

	// Convert to SQL rows
	rows := make([][]sqltypes.Value, len(results))
	for i, result := range results {
		row := make([]sqltypes.Value, len(columns))
		for j, columnName := range columns {
			if value, exists := result.Values[columnName]; exists {
				row[j] = convertSchemaValueToSQL(value)
			} else {
				row[j] = sqltypes.NULL
			}
		}
		rows[i] = row
	}

	return &QueryResult{
		Columns: columns,
		Rows:    rows,
	}
}

// convertSchemaValueToSQL converts schema_pb.Value to sqltypes.Value
func convertSchemaValueToSQL(value *schema_pb.Value) sqltypes.Value {
	if value == nil {
		return sqltypes.NULL
	}

	switch v := value.Kind.(type) {
	case *schema_pb.Value_BoolValue:
		if v.BoolValue {
			return sqltypes.NewInt32(1)
		}
		return sqltypes.NewInt32(0)
	case *schema_pb.Value_Int32Value:
		return sqltypes.NewInt32(v.Int32Value)
	case *schema_pb.Value_Int64Value:
		return sqltypes.NewInt64(v.Int64Value)
	case *schema_pb.Value_FloatValue:
		return sqltypes.NewFloat32(v.FloatValue)
	case *schema_pb.Value_DoubleValue:
		return sqltypes.NewFloat64(v.DoubleValue)
	case *schema_pb.Value_BytesValue:
		return sqltypes.NewVarBinary(string(v.BytesValue))
	case *schema_pb.Value_StringValue:
		return sqltypes.NewVarChar(v.StringValue)
	// Parquet logical types
	case *schema_pb.Value_TimestampValue:
		timestampValue := value.GetTimestampValue()
		if timestampValue == nil {
			return sqltypes.NULL
		}
		// Convert microseconds to time.Time and format as datetime string
		timestamp := time.UnixMicro(timestampValue.TimestampMicros)
		return sqltypes.MakeTrusted(sqltypes.Datetime, []byte(timestamp.Format("2006-01-02 15:04:05")))
	case *schema_pb.Value_DateValue:
		dateValue := value.GetDateValue()
		if dateValue == nil {
			return sqltypes.NULL
		}
		// Convert days since epoch to date string
		date := time.Unix(int64(dateValue.DaysSinceEpoch)*86400, 0).UTC()
		return sqltypes.MakeTrusted(sqltypes.Date, []byte(date.Format("2006-01-02")))
	case *schema_pb.Value_DecimalValue:
		decimalValue := value.GetDecimalValue()
		if decimalValue == nil {
			return sqltypes.NULL
		}
		// Convert decimal bytes to string representation
		decimalStr := decimalToStringHelper(decimalValue)
		return sqltypes.MakeTrusted(sqltypes.Decimal, []byte(decimalStr))
	case *schema_pb.Value_TimeValue:
		timeValue := value.GetTimeValue()
		if timeValue == nil {
			return sqltypes.NULL
		}
		// Convert microseconds since midnight to time string
		duration := time.Duration(timeValue.TimeMicros) * time.Microsecond
		timeOfDay := time.Date(0, 1, 1, 0, 0, 0, 0, time.UTC).Add(duration)
		return sqltypes.MakeTrusted(sqltypes.Time, []byte(timeOfDay.Format("15:04:05")))
	default:
		return sqltypes.NewVarChar(fmt.Sprintf("%v", value))
	}
}

// decimalToStringHelper converts a DecimalValue to string representation
// This is a standalone version of the engine's decimalToString method
func decimalToStringHelper(decimalValue *schema_pb.DecimalValue) string {
	if decimalValue == nil || decimalValue.Value == nil {
		return "0"
	}

	// Convert bytes back to big.Int
	intValue := new(big.Int).SetBytes(decimalValue.Value)

	// Convert to string with proper decimal placement
	str := intValue.String()

	// Handle decimal placement based on scale
	scale := int(decimalValue.Scale)
	if scale > 0 && len(str) > scale {
		// Insert decimal point
		decimalPos := len(str) - scale
		return str[:decimalPos] + "." + str[decimalPos:]
	}

	return str
}
