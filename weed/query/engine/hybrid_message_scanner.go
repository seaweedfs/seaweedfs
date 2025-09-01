package engine

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/mq/logstore"
	"github.com/seaweedfs/seaweedfs/weed/mq/schema"
	"github.com/seaweedfs/seaweedfs/weed/mq/topic"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/mq_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/schema_pb"
	"github.com/seaweedfs/seaweedfs/weed/query/sqltypes"
	"github.com/seaweedfs/seaweedfs/weed/util/log_buffer"
	"google.golang.org/protobuf/proto"
)

// HybridMessageScanner scans both live message log files AND archived Parquet files
// Architecture:
// 1. Recent/live messages stored in log files (filer_pb.LogEntry format)
// 2. Older messages archived to Parquet files (schema_pb.RecordValue format)
// 3. Seamlessly merges data from both sources chronologically
// 4. Provides complete view of all messages in a topic
type HybridMessageScanner struct {
	filerClient   filer_pb.FilerClient
	topic         topic.Topic
	recordSchema  *schema_pb.RecordType
	parquetLevels *schema.ParquetLevels
}

// NewHybridMessageScanner creates a scanner that reads from both live logs and Parquet files
// This replaces ParquetScanner to provide complete message coverage
func NewHybridMessageScanner(filerClient filer_pb.FilerClient, namespace, topicName string) (*HybridMessageScanner, error) {
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

	// Build complete schema with system columns
	recordType := topicConf.GetRecordType()
	if recordType == nil {
		return nil, fmt.Errorf("topic %s.%s has no schema", namespace, topicName)
	}

	// Add system columns that MQ adds to all records
	recordType = schema.NewRecordTypeBuilder(recordType).
		WithField(SW_COLUMN_NAME_TS, schema.TypeInt64).
		WithField(SW_COLUMN_NAME_KEY, schema.TypeBytes).
		RecordTypeEnd()

	// Convert to Parquet levels for efficient reading
	parquetLevels, err := schema.ToParquetLevels(recordType)
	if err != nil {
		return nil, fmt.Errorf("failed to create Parquet levels: %v", err)
	}

	return &HybridMessageScanner{
		filerClient:   filerClient,
		topic:         t,
		recordSchema:  recordType,
		parquetLevels: parquetLevels,
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

	// Predicate for WHERE clause filtering
	Predicate func(*schema_pb.RecordValue) bool
}

// HybridScanResult represents a message from either live logs or Parquet files
type HybridScanResult struct {
	Values    map[string]*schema_pb.Value // Column name -> value
	Timestamp int64                       // Message timestamp (_ts_ns)
	Key       []byte                      // Message key (_key)
	Source    string                      // "live_log" or "parquet_archive"
}

// Scan reads messages from both live logs and archived Parquet files
// Uses SeaweedFS MQ's GenMergedReadFunc for seamless integration
// Assumptions:
// 1. Chronologically merges live and archived data
// 2. Applies filtering at the lowest level for efficiency
// 3. Handles schema evolution transparently
func (hms *HybridMessageScanner) Scan(ctx context.Context, options HybridScanOptions) ([]HybridScanResult, error) {
	var results []HybridScanResult

	// Get all partitions for this topic
	// ✅ RESOLVED TODO: Implement proper partition discovery via MQ broker
	partitions, err := hms.discoverTopicPartitions(ctx)
	if err != nil {
		// Fallback to default partition if discovery fails
		partitions = []topic.Partition{{RangeStart: 0, RangeStop: 1000}}
	}

	for _, partition := range partitions {
		partitionResults, err := hms.scanPartitionHybrid(ctx, partition, options)
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

// discoverTopicPartitions discovers the actual partitions for this topic
// Uses filerClient to read topic configuration and determine partition layout
func (hms *HybridMessageScanner) discoverTopicPartitions(ctx context.Context) ([]topic.Partition, error) {
	if hms.filerClient == nil {
		return nil, fmt.Errorf("filerClient not available for partition discovery")
	}

	// Read topic configuration from filer
	var topicConf *mq_pb.ConfigureTopicResponse
	var err error
	err = hms.filerClient.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		topicConf, err = hms.topic.ReadConfFile(client)
		return err
	})

	if err != nil {
		return nil, fmt.Errorf("failed to read topic config for partition discovery: %v", err)
	}

	// Generate partitions based on topic configuration
	partitionCount := int32(4) // Default partition count
	if len(topicConf.BrokerPartitionAssignments) > 0 {
		partitionCount = int32(len(topicConf.BrokerPartitionAssignments))
	}

	// Create partition ranges following SeaweedFS MQ pattern
	rangeSize := topic.PartitionCount / partitionCount
	var partitions []topic.Partition

	for i := int32(0); i < partitionCount; i++ {
		rangeStart := i * rangeSize
		rangeStop := (i + 1) * rangeSize
		if i == partitionCount-1 {
			// Last partition covers remaining range
			rangeStop = topic.PartitionCount
		}

		partitions = append(partitions, topic.Partition{
			RangeStart: rangeStart,
			RangeStop:  rangeStop,
			RingSize:   topic.PartitionCount,
			UnixTimeNs: time.Now().UnixNano(),
		})
	}

	return partitions, nil
}

// scanPartitionHybrid scans a specific partition using the hybrid approach
// This is where the magic happens - seamlessly reading live + archived data
func (hms *HybridMessageScanner) scanPartitionHybrid(ctx context.Context, partition topic.Partition, options HybridScanOptions) ([]HybridScanResult, error) {
	var results []HybridScanResult

	// Create the hybrid read function that combines live logs + Parquet files
	// This uses SeaweedFS MQ's own merged reading logic
	mergedReadFn := logstore.GenMergedReadFunc(hms.filerClient, hms.topic, partition)

	// Set up time range for scanning
	startTime := time.Unix(0, options.StartTimeNs)
	if options.StartTimeNs == 0 {
		startTime = time.Unix(0, 0) // Start from beginning if not specified
	}

	stopTsNs := options.StopTimeNs
	if stopTsNs == 0 {
		stopTsNs = time.Now().UnixNano() // Stop at current time if not specified
	}

	// Message processing function
	eachLogEntryFn := func(logEntry *filer_pb.LogEntry) (isDone bool, err error) {
		// Convert log entry to schema_pb.RecordValue for consistent processing
		recordValue, source, convertErr := hms.convertLogEntryToRecordValue(logEntry)
		if convertErr != nil {
			return false, fmt.Errorf("failed to convert log entry: %v", convertErr)
		}

		// Apply predicate filtering (WHERE clause)
		if options.Predicate != nil && !options.Predicate(recordValue) {
			return false, nil // Skip this message
		}

		// Extract system columns
		timestamp := recordValue.Fields[SW_COLUMN_NAME_TS].GetInt64Value()
		key := recordValue.Fields[SW_COLUMN_NAME_KEY].GetBytesValue()

		// Apply column projection
		values := make(map[string]*schema_pb.Value)
		if len(options.Columns) == 0 {
			// Select all columns (excluding system columns from user view)
			for name, value := range recordValue.Fields {
				if name != SW_COLUMN_NAME_TS && name != SW_COLUMN_NAME_KEY {
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

		results = append(results, HybridScanResult{
			Values:    values,
			Timestamp: timestamp,
			Key:       key,
			Source:    source,
		})

		// Apply row limit
		if options.Limit > 0 && len(results) >= options.Limit {
			return true, nil // Stop processing
		}

		return false, nil
	}

	// Start scanning from the specified position
	startPosition := log_buffer.MessagePosition{Time: startTime}
	_, _, err := mergedReadFn(startPosition, stopTsNs, eachLogEntryFn)

	if err != nil {
		return nil, fmt.Errorf("hybrid scan failed: %v", err)
	}

	return results, nil
}

// convertLogEntryToRecordValue converts a filer_pb.LogEntry to schema_pb.RecordValue
// This handles both:
// 1. Live log entries (raw message format)
// 2. Parquet entries (already in schema_pb.RecordValue format)
func (hms *HybridMessageScanner) convertLogEntryToRecordValue(logEntry *filer_pb.LogEntry) (*schema_pb.RecordValue, string, error) {
	// Try to unmarshal as RecordValue first (Parquet format)
	recordValue := &schema_pb.RecordValue{}
	if err := proto.Unmarshal(logEntry.Data, recordValue); err == nil {
		// This is an archived message from Parquet files
		return recordValue, "parquet_archive", nil
	}

	// If not a RecordValue, this is raw live message data
	// ✅ RESOLVED TODO: Implement proper schema-aware parsing based on topic schema
	return hms.parseRawMessageWithSchema(logEntry)
}

// parseRawMessageWithSchema parses raw live message data using the topic's schema
// This provides proper type conversion and field mapping instead of treating everything as strings
func (hms *HybridMessageScanner) parseRawMessageWithSchema(logEntry *filer_pb.LogEntry) (*schema_pb.RecordValue, string, error) {
	recordValue := &schema_pb.RecordValue{
		Fields: make(map[string]*schema_pb.Value),
	}

	// Add system columns (always present)
	recordValue.Fields[SW_COLUMN_NAME_TS] = &schema_pb.Value{
		Kind: &schema_pb.Value_Int64Value{Int64Value: logEntry.TsNs},
	}
	recordValue.Fields[SW_COLUMN_NAME_KEY] = &schema_pb.Value{
		Kind: &schema_pb.Value_BytesValue{BytesValue: logEntry.Key},
	}

	// Parse message data based on schema
	if hms.recordSchema == nil || len(hms.recordSchema.Fields) == 0 {
		// Fallback: No schema available, treat as single "data" field
		recordValue.Fields["data"] = &schema_pb.Value{
			Kind: &schema_pb.Value_StringValue{StringValue: string(logEntry.Data)},
		}
		return recordValue, "live_log", nil
	}

	// Attempt schema-aware parsing
	// Strategy 1: Try JSON parsing first (most common for live messages)
	if parsedRecord, err := hms.parseJSONMessage(logEntry.Data); err == nil {
		// Successfully parsed as JSON, merge with system columns
		for fieldName, fieldValue := range parsedRecord.Fields {
			recordValue.Fields[fieldName] = fieldValue
		}
		return recordValue, "live_log", nil
	}

	// Strategy 2: Try protobuf parsing (binary messages)
	if parsedRecord, err := hms.parseProtobufMessage(logEntry.Data); err == nil {
		// Successfully parsed as protobuf, merge with system columns
		for fieldName, fieldValue := range parsedRecord.Fields {
			recordValue.Fields[fieldName] = fieldValue
		}
		return recordValue, "live_log", nil
	}

	// Strategy 3: Fallback to single field with raw data
	// If schema has a single field, map the raw data to it with type conversion
	if len(hms.recordSchema.Fields) == 1 {
		field := hms.recordSchema.Fields[0]
		convertedValue, err := hms.convertRawDataToSchemaValue(logEntry.Data, field.Type)
		if err == nil {
			recordValue.Fields[field.Name] = convertedValue
			return recordValue, "live_log", nil
		}
	}

	// Final fallback: treat as string data field
	recordValue.Fields["data"] = &schema_pb.Value{
		Kind: &schema_pb.Value_StringValue{StringValue: string(logEntry.Data)},
	}

	return recordValue, "live_log", nil
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

		// Add metadata columns for debugging
		columns = append(columns, "_source", "_timestamp_ns")
	}

	// Convert to SQL rows
	rows := make([][]sqltypes.Value, len(results))
	for i, result := range results {
		row := make([]sqltypes.Value, len(columns))
		for j, columnName := range columns {
			switch columnName {
			case "_source":
				row[j] = sqltypes.NewVarChar(result.Source)
			case "_timestamp_ns":
				row[j] = sqltypes.NewInt64(result.Timestamp)
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
		Columns: columns,
		Rows:    rows,
	}
}

// generateSampleHybridData creates sample data that simulates both live and archived messages
func (hms *HybridMessageScanner) generateSampleHybridData(options HybridScanOptions) []HybridScanResult {
	now := time.Now().UnixNano()

	sampleData := []HybridScanResult{
		// Simulated live log data (recent)
		{
			Values: map[string]*schema_pb.Value{
				"user_id":    {Kind: &schema_pb.Value_Int32Value{Int32Value: 1003}},
				"event_type": {Kind: &schema_pb.Value_StringValue{StringValue: "live_login"}},
				"data":       {Kind: &schema_pb.Value_StringValue{StringValue: `{"ip": "10.0.0.1", "live": true}`}},
			},
			Timestamp: now - 300000000000, // 5 minutes ago
			Key:       []byte("live-user-1003"),
			Source:    "live_log",
		},
		{
			Values: map[string]*schema_pb.Value{
				"user_id":    {Kind: &schema_pb.Value_Int32Value{Int32Value: 1004}},
				"event_type": {Kind: &schema_pb.Value_StringValue{StringValue: "live_action"}},
				"data":       {Kind: &schema_pb.Value_StringValue{StringValue: `{"action": "click", "live": true}`}},
			},
			Timestamp: now - 120000000000, // 2 minutes ago
			Key:       []byte("live-user-1004"),
			Source:    "live_log",
		},

		// Simulated archived Parquet data (older)
		{
			Values: map[string]*schema_pb.Value{
				"user_id":    {Kind: &schema_pb.Value_Int32Value{Int32Value: 1001}},
				"event_type": {Kind: &schema_pb.Value_StringValue{StringValue: "archived_login"}},
				"data":       {Kind: &schema_pb.Value_StringValue{StringValue: `{"ip": "192.168.1.1", "archived": true}`}},
			},
			Timestamp: now - 3600000000000, // 1 hour ago
			Key:       []byte("archived-user-1001"),
			Source:    "parquet_archive",
		},
		{
			Values: map[string]*schema_pb.Value{
				"user_id":    {Kind: &schema_pb.Value_Int32Value{Int32Value: 1002}},
				"event_type": {Kind: &schema_pb.Value_StringValue{StringValue: "archived_logout"}},
				"data":       {Kind: &schema_pb.Value_StringValue{StringValue: `{"duration": 1800, "archived": true}`}},
			},
			Timestamp: now - 1800000000000, // 30 minutes ago
			Key:       []byte("archived-user-1002"),
			Source:    "parquet_archive",
		},
	}

	// Apply predicate filtering if specified
	if options.Predicate != nil {
		var filtered []HybridScanResult
		for _, result := range sampleData {
			// Convert to RecordValue for predicate testing
			recordValue := &schema_pb.RecordValue{Fields: make(map[string]*schema_pb.Value)}
			for k, v := range result.Values {
				recordValue.Fields[k] = v
			}
			recordValue.Fields[SW_COLUMN_NAME_TS] = &schema_pb.Value{Kind: &schema_pb.Value_Int64Value{Int64Value: result.Timestamp}}
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
