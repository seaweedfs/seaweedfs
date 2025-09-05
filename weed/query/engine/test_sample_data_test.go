package engine

import (
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/schema_pb"
)

// generateSampleHybridData creates sample data that simulates both live and archived messages
// This function is only used for testing and is not included in production builds
func generateSampleHybridData(topicName string, options HybridScanOptions) []HybridScanResult {
	now := time.Now().UnixNano()

	// Generate different sample data based on topic name
	var sampleData []HybridScanResult

	switch topicName {
	case "user_events":
		sampleData = []HybridScanResult{
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

	case "system_logs":
		sampleData = []HybridScanResult{
			// Simulated live system logs (recent)
			{
				Values: map[string]*schema_pb.Value{
					"level":   {Kind: &schema_pb.Value_StringValue{StringValue: "INFO"}},
					"message": {Kind: &schema_pb.Value_StringValue{StringValue: "Live system startup completed"}},
					"service": {Kind: &schema_pb.Value_StringValue{StringValue: "auth-service"}},
				},
				Timestamp: now - 240000000000, // 4 minutes ago
				Key:       []byte("live-sys-001"),
				Source:    "live_log",
			},
			{
				Values: map[string]*schema_pb.Value{
					"level":   {Kind: &schema_pb.Value_StringValue{StringValue: "WARN"}},
					"message": {Kind: &schema_pb.Value_StringValue{StringValue: "Live high memory usage detected"}},
					"service": {Kind: &schema_pb.Value_StringValue{StringValue: "monitor-service"}},
				},
				Timestamp: now - 180000000000, // 3 minutes ago
				Key:       []byte("live-sys-002"),
				Source:    "live_log",
			},

			// Simulated archived system logs (older)
			{
				Values: map[string]*schema_pb.Value{
					"level":   {Kind: &schema_pb.Value_StringValue{StringValue: "ERROR"}},
					"message": {Kind: &schema_pb.Value_StringValue{StringValue: "Archived database connection failed"}},
					"service": {Kind: &schema_pb.Value_StringValue{StringValue: "db-service"}},
				},
				Timestamp: now - 7200000000000, // 2 hours ago
				Key:       []byte("archived-sys-001"),
				Source:    "parquet_archive",
			},
			{
				Values: map[string]*schema_pb.Value{
					"level":   {Kind: &schema_pb.Value_StringValue{StringValue: "INFO"}},
					"message": {Kind: &schema_pb.Value_StringValue{StringValue: "Archived batch job completed"}},
					"service": {Kind: &schema_pb.Value_StringValue{StringValue: "batch-service"}},
				},
				Timestamp: now - 3600000000000, // 1 hour ago
				Key:       []byte("archived-sys-002"),
				Source:    "parquet_archive",
			},
		}

	default:
		// For unknown topics, return empty data
		sampleData = []HybridScanResult{}
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
			recordValue.Fields[SW_COLUMN_NAME_TIMESTAMP] = &schema_pb.Value{Kind: &schema_pb.Value_Int64Value{Int64Value: result.Timestamp}}
			recordValue.Fields[SW_COLUMN_NAME_KEY] = &schema_pb.Value{Kind: &schema_pb.Value_BytesValue{BytesValue: result.Key}}

			if options.Predicate(recordValue) {
				filtered = append(filtered, result)
			}
		}
		sampleData = filtered
	}

	return sampleData
}
