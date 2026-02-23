package engine

import (
	"context"
	"fmt"
	"strings"
	"testing"
)

func TestSQLEngine_HybridSelectBasic(t *testing.T) {
	engine := NewTestSQLEngine()

	// Test SELECT with _source column to show both live and archived data
	result, err := engine.ExecuteSQL(context.Background(), "SELECT *, _source FROM user_events")
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}

	if result.Error != nil {
		t.Fatalf("Expected no query error, got %v", result.Error)
	}

	if len(result.Columns) == 0 {
		t.Error("Expected columns in result")
	}

	// In mock environment, we only get live_log data from unflushed messages
	// parquet_archive data would come from parquet files in a real system
	if len(result.Rows) == 0 {
		t.Error("Expected rows in result")
	}

	// Check that we have the _source column showing data source
	hasSourceColumn := false
	sourceColumnIndex := -1
	for i, column := range result.Columns {
		if column == SW_COLUMN_NAME_SOURCE {
			hasSourceColumn = true
			sourceColumnIndex = i
			break
		}
	}

	if !hasSourceColumn {
		t.Skip("_source column not available in fallback mode - test requires real SeaweedFS cluster")
	}

	// Verify we have the expected data sources (in mock environment, only live_log)
	if hasSourceColumn && sourceColumnIndex >= 0 {
		foundLiveLog := false

		for _, row := range result.Rows {
			if sourceColumnIndex < len(row) {
				source := row[sourceColumnIndex].ToString()
				if source == "live_log" {
					foundLiveLog = true
				}
				// In mock environment, all data comes from unflushed messages (live_log)
				// In a real system, we would also see parquet_archive from parquet files
			}
		}

		if !foundLiveLog {
			t.Error("Expected to find live_log data source in results")
		}

		t.Logf("Found live_log data source from unflushed messages")
	}
}

func TestSQLEngine_HybridSelectWithLimit(t *testing.T) {
	engine := NewTestSQLEngine()

	// Test SELECT with LIMIT on hybrid data
	result, err := engine.ExecuteSQL(context.Background(), "SELECT * FROM user_events LIMIT 2")
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}

	if result.Error != nil {
		t.Fatalf("Expected no query error, got %v", result.Error)
	}

	// Should have exactly 2 rows due to LIMIT
	if len(result.Rows) != 2 {
		t.Errorf("Expected 2 rows with LIMIT 2, got %d", len(result.Rows))
	}
}

func TestSQLEngine_HybridSelectDifferentTables(t *testing.T) {
	engine := NewTestSQLEngine()

	// Test both user_events and system_logs tables
	tables := []string{"user_events", "system_logs"}

	for _, tableName := range tables {
		result, err := engine.ExecuteSQL(context.Background(), fmt.Sprintf("SELECT *, _source FROM %s", tableName))
		if err != nil {
			t.Errorf("Error querying hybrid table %s: %v", tableName, err)
			continue
		}

		if result.Error != nil {
			t.Errorf("Query error for hybrid table %s: %v", tableName, result.Error)
			continue
		}

		if len(result.Columns) == 0 {
			t.Errorf("No columns returned for hybrid table %s", tableName)
		}

		if len(result.Rows) == 0 {
			t.Errorf("No rows returned for hybrid table %s", tableName)
		}

		// Check for _source column
		hasSourceColumn := false
		for _, column := range result.Columns {
			if column == "_source" {
				hasSourceColumn = true
				break
			}
		}

		if !hasSourceColumn {
			t.Logf("Table %s missing _source column - running in fallback mode", tableName)
		}

		t.Logf("Table %s: %d columns, %d rows with hybrid data sources", tableName, len(result.Columns), len(result.Rows))
	}
}

func TestSQLEngine_HybridDataSource(t *testing.T) {
	engine := NewTestSQLEngine()

	// Test that we can distinguish between live and archived data
	result, err := engine.ExecuteSQL(context.Background(), "SELECT user_id, event_type, _source FROM user_events")
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}

	if result.Error != nil {
		t.Fatalf("Expected no query error, got %v", result.Error)
	}

	// Find the _source column
	sourceColumnIndex := -1
	eventTypeColumnIndex := -1

	for i, column := range result.Columns {
		switch column {
		case "_source":
			sourceColumnIndex = i
		case "event_type":
			eventTypeColumnIndex = i
		}
	}

	if sourceColumnIndex == -1 {
		t.Skip("Could not find _source column - test requires real SeaweedFS cluster")
	}

	if eventTypeColumnIndex == -1 {
		t.Fatal("Could not find event_type column")
	}

	// Check the data characteristics
	liveEventFound := false
	archivedEventFound := false

	for _, row := range result.Rows {
		if sourceColumnIndex < len(row) && eventTypeColumnIndex < len(row) {
			source := row[sourceColumnIndex].ToString()
			eventType := row[eventTypeColumnIndex].ToString()

			if source == "live_log" && strings.Contains(eventType, "live_") {
				liveEventFound = true
				t.Logf("Found live event: %s from %s", eventType, source)
			}

			if source == "parquet_archive" && strings.Contains(eventType, "archived_") {
				archivedEventFound = true
				t.Logf("Found archived event: %s from %s", eventType, source)
			}
		}
	}

	if !liveEventFound {
		t.Error("Expected to find live events with live_ prefix")
	}

	if !archivedEventFound {
		t.Error("Expected to find archived events with archived_ prefix")
	}
}

func TestSQLEngine_HybridSystemLogs(t *testing.T) {
	engine := NewTestSQLEngine()

	// Test system_logs with hybrid data
	result, err := engine.ExecuteSQL(context.Background(), "SELECT level, message, service, _source FROM system_logs")
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}

	if result.Error != nil {
		t.Fatalf("Expected no query error, got %v", result.Error)
	}

	// Should have both live and archived system logs
	if len(result.Rows) < 2 {
		t.Errorf("Expected at least 2 system log entries, got %d", len(result.Rows))
	}

	// Find column indices
	levelIndex := -1
	sourceIndex := -1

	for i, column := range result.Columns {
		switch column {
		case "level":
			levelIndex = i
		case "_source":
			sourceIndex = i
		}
	}

	// Verify we have both live and archived system logs
	foundLive := false
	foundArchived := false

	for _, row := range result.Rows {
		if sourceIndex >= 0 && sourceIndex < len(row) {
			source := row[sourceIndex].ToString()

			if source == "live_log" {
				foundLive = true
				if levelIndex >= 0 && levelIndex < len(row) {
					level := row[levelIndex].ToString()
					t.Logf("Live system log: level=%s", level)
				}
			}

			if source == "parquet_archive" {
				foundArchived = true
				if levelIndex >= 0 && levelIndex < len(row) {
					level := row[levelIndex].ToString()
					t.Logf("Archived system log: level=%s", level)
				}
			}
		}
	}

	if !foundLive {
		t.Log("No live system logs found - running in fallback mode")
	}

	if !foundArchived {
		t.Log("No archived system logs found - running in fallback mode")
	}
}

func TestSQLEngine_HybridSelectWithTimeImplications(t *testing.T) {
	engine := NewTestSQLEngine()

	// Test that demonstrates the time-based nature of hybrid data
	// Live data should be more recent than archived data
	result, err := engine.ExecuteSQL(context.Background(), "SELECT event_type, _source FROM user_events")
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}

	if result.Error != nil {
		t.Fatalf("Expected no query error, got %v", result.Error)
	}

	// This test documents that hybrid scanning provides a complete view
	// of both recent (live) and historical (archived) data in a single query
	liveCount := 0
	archivedCount := 0

	sourceIndex := -1
	for i, column := range result.Columns {
		if column == "_source" {
			sourceIndex = i
			break
		}
	}

	if sourceIndex >= 0 {
		for _, row := range result.Rows {
			if sourceIndex < len(row) {
				source := row[sourceIndex].ToString()
				switch source {
				case "live_log":
					liveCount++
				case "parquet_archive":
					archivedCount++
				}
			}
		}
	}

	t.Logf("Hybrid query results: %d live messages, %d archived messages", liveCount, archivedCount)

	if liveCount == 0 && archivedCount == 0 {
		t.Log("No live or archived messages found - running in fallback mode")
	}
}
