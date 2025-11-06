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
			// Generate more test data to support LIMIT/OFFSET testing
			{
				Values: map[string]*schema_pb.Value{
					"id":         {Kind: &schema_pb.Value_Int64Value{Int64Value: 82460}},
					"user_id":    {Kind: &schema_pb.Value_Int32Value{Int32Value: 9465}},
					"event_type": {Kind: &schema_pb.Value_StringValue{StringValue: "live_login"}},
					"data":       {Kind: &schema_pb.Value_StringValue{StringValue: `{"ip": "10.0.0.1", "live": true}`}},
					"status":     {Kind: &schema_pb.Value_StringValue{StringValue: "active"}},
					"action":     {Kind: &schema_pb.Value_StringValue{StringValue: "login"}},
					"user_type":  {Kind: &schema_pb.Value_StringValue{StringValue: "premium"}},
					"amount":     {Kind: &schema_pb.Value_DoubleValue{DoubleValue: 43.619326294957126}},
				},
				Timestamp: now - 300000000000, // 5 minutes ago
				Key:       []byte("live-user-9465"),
				Source:    "live_log",
			},
			{
				Values: map[string]*schema_pb.Value{
					"id":         {Kind: &schema_pb.Value_Int64Value{Int64Value: 841256}},
					"user_id":    {Kind: &schema_pb.Value_Int32Value{Int32Value: 2336}},
					"event_type": {Kind: &schema_pb.Value_StringValue{StringValue: "live_action"}},
					"data":       {Kind: &schema_pb.Value_StringValue{StringValue: `{"action": "click", "live": true}`}},
					"status":     {Kind: &schema_pb.Value_StringValue{StringValue: "pending"}},
					"action":     {Kind: &schema_pb.Value_StringValue{StringValue: "click"}},
					"user_type":  {Kind: &schema_pb.Value_StringValue{StringValue: "standard"}},
					"amount":     {Kind: &schema_pb.Value_DoubleValue{DoubleValue: 550.0278410655299}},
				},
				Timestamp: now - 120000000000, // 2 minutes ago
				Key:       []byte("live-user-2336"),
				Source:    "live_log",
			},
			{
				Values: map[string]*schema_pb.Value{
					"id":         {Kind: &schema_pb.Value_Int64Value{Int64Value: 55537}},
					"user_id":    {Kind: &schema_pb.Value_Int32Value{Int32Value: 6912}},
					"event_type": {Kind: &schema_pb.Value_StringValue{StringValue: "purchase"}},
					"data":       {Kind: &schema_pb.Value_StringValue{StringValue: `{"amount": 25.99, "item": "book"}`}},
				},
				Timestamp: now - 90000000000, // 1.5 minutes ago
				Key:       []byte("live-user-6912"),
				Source:    "live_log",
			},
			{
				Values: map[string]*schema_pb.Value{
					"id":         {Kind: &schema_pb.Value_Int64Value{Int64Value: 65143}},
					"user_id":    {Kind: &schema_pb.Value_Int32Value{Int32Value: 5102}},
					"event_type": {Kind: &schema_pb.Value_StringValue{StringValue: "page_view"}},
					"data":       {Kind: &schema_pb.Value_StringValue{StringValue: `{"page": "/home", "duration": 30}`}},
				},
				Timestamp: now - 80000000000, // 80 seconds ago
				Key:       []byte("live-user-5102"),
				Source:    "live_log",
			},

			// Simulated archived Parquet data (older)
			{
				Values: map[string]*schema_pb.Value{
					"id":         {Kind: &schema_pb.Value_Int64Value{Int64Value: 686003}},
					"user_id":    {Kind: &schema_pb.Value_Int32Value{Int32Value: 2759}},
					"event_type": {Kind: &schema_pb.Value_StringValue{StringValue: "archived_login"}},
					"data":       {Kind: &schema_pb.Value_StringValue{StringValue: `{"ip": "192.168.1.1", "archived": true}`}},
				},
				Timestamp: now - 3600000000000, // 1 hour ago
				Key:       []byte("archived-user-2759"),
				Source:    "parquet_archive",
			},
			{
				Values: map[string]*schema_pb.Value{
					"id":         {Kind: &schema_pb.Value_Int64Value{Int64Value: 417224}},
					"user_id":    {Kind: &schema_pb.Value_Int32Value{Int32Value: 7810}},
					"event_type": {Kind: &schema_pb.Value_StringValue{StringValue: "archived_logout"}},
					"data":       {Kind: &schema_pb.Value_StringValue{StringValue: `{"duration": 1800, "archived": true}`}},
				},
				Timestamp: now - 1800000000000, // 30 minutes ago
				Key:       []byte("archived-user-7810"),
				Source:    "parquet_archive",
			},
			{
				Values: map[string]*schema_pb.Value{
					"id":         {Kind: &schema_pb.Value_Int64Value{Int64Value: 424297}},
					"user_id":    {Kind: &schema_pb.Value_Int32Value{Int32Value: 8897}},
					"event_type": {Kind: &schema_pb.Value_StringValue{StringValue: "purchase"}},
					"data":       {Kind: &schema_pb.Value_StringValue{StringValue: `{"amount": 45.50, "item": "electronics"}`}},
				},
				Timestamp: now - 1500000000000, // 25 minutes ago
				Key:       []byte("archived-user-8897"),
				Source:    "parquet_archive",
			},
			{
				Values: map[string]*schema_pb.Value{
					"id":         {Kind: &schema_pb.Value_Int64Value{Int64Value: 431189}},
					"user_id":    {Kind: &schema_pb.Value_Int32Value{Int32Value: 3400}},
					"event_type": {Kind: &schema_pb.Value_StringValue{StringValue: "signup"}},
					"data":       {Kind: &schema_pb.Value_StringValue{StringValue: `{"referral": "google", "plan": "free"}`}},
				},
				Timestamp: now - 1200000000000, // 20 minutes ago
				Key:       []byte("archived-user-3400"),
				Source:    "parquet_archive",
			},
			{
				Values: map[string]*schema_pb.Value{
					"id":         {Kind: &schema_pb.Value_Int64Value{Int64Value: 413249}},
					"user_id":    {Kind: &schema_pb.Value_Int32Value{Int32Value: 5175}},
					"event_type": {Kind: &schema_pb.Value_StringValue{StringValue: "update_profile"}},
					"data":       {Kind: &schema_pb.Value_StringValue{StringValue: `{"field": "email", "new_value": "user@example.com"}`}},
				},
				Timestamp: now - 900000000000, // 15 minutes ago
				Key:       []byte("archived-user-5175"),
				Source:    "parquet_archive",
			},
			{
				Values: map[string]*schema_pb.Value{
					"id":         {Kind: &schema_pb.Value_Int64Value{Int64Value: 120612}},
					"user_id":    {Kind: &schema_pb.Value_Int32Value{Int32Value: 5429}},
					"event_type": {Kind: &schema_pb.Value_StringValue{StringValue: "comment"}},
					"data":       {Kind: &schema_pb.Value_StringValue{StringValue: `{"post_id": 123, "comment": "Great post!"}`}},
				},
				Timestamp: now - 600000000000, // 10 minutes ago
				Key:       []byte("archived-user-5429"),
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
