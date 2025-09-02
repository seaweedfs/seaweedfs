package engine

import (
	"context"
	"fmt"
	"strings"
	"testing"
)

func TestSQLEngine_SelectBasic(t *testing.T) {
	engine := NewSQLEngine("localhost:8888")

	// Test SELECT * FROM table
	result, err := engine.ExecuteSQL(context.Background(), "SELECT * FROM user_events")
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}

	if result.Error != nil {
		t.Fatalf("Expected no query error, got %v", result.Error)
	}

	if len(result.Columns) == 0 {
		t.Error("Expected columns in result")
	}

	if len(result.Rows) == 0 {
		t.Error("Expected rows in result")
	}

	// Should have sample data with 3 columns (SELECT * excludes system columns)
	expectedColumns := []string{"user_id", "event_type", "data"}
	if len(result.Columns) != len(expectedColumns) {
		t.Errorf("Expected %d columns, got %d", len(expectedColumns), len(result.Columns))
	}

	// Should have 4 sample rows (hybrid data includes both live_log and parquet_archive)
	if len(result.Rows) != 4 {
		t.Errorf("Expected 4 rows, got %d", len(result.Rows))
	}
}

func TestSQLEngine_SelectWithLimit(t *testing.T) {
	engine := NewSQLEngine("localhost:8888")

	// Test SELECT with LIMIT
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

func TestSQLEngine_SelectSpecificColumns(t *testing.T) {
	engine := NewSQLEngine("localhost:8888")

	// Test SELECT specific columns (this will fall back to sample data)
	result, err := engine.ExecuteSQL(context.Background(), "SELECT user_id, event_type FROM user_events")
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}

	if result.Error != nil {
		t.Fatalf("Expected no query error, got %v", result.Error)
	}

	// Should have all columns for now (sample data doesn't implement projection yet)
	if len(result.Columns) == 0 {
		t.Error("Expected columns in result")
	}
}

func TestSQLEngine_SelectFromNonExistentTable(t *testing.T) {
	engine := NewSQLEngine("localhost:8888")

	// Test SELECT from non-existent table
	result, _ := engine.ExecuteSQL(context.Background(), "SELECT * FROM nonexistent_table")
	if result.Error == nil {
		t.Error("Expected error for non-existent table")
	}

	if !strings.Contains(result.Error.Error(), "not found") {
		t.Errorf("Expected 'not found' error, got: %v", result.Error)
	}
}

func TestSQLEngine_SelectDifferentTables(t *testing.T) {
	engine := NewSQLEngine("localhost:8888")

	// Test different sample tables
	tables := []string{"user_events", "system_logs"}

	for _, tableName := range tables {
		result, err := engine.ExecuteSQL(context.Background(), fmt.Sprintf("SELECT * FROM %s", tableName))
		if err != nil {
			t.Errorf("Error querying table %s: %v", tableName, err)
			continue
		}

		if result.Error != nil {
			t.Errorf("Query error for table %s: %v", tableName, result.Error)
			continue
		}

		if len(result.Columns) == 0 {
			t.Errorf("No columns returned for table %s", tableName)
		}

		if len(result.Rows) == 0 {
			t.Errorf("No rows returned for table %s", tableName)
		}

		t.Logf("Table %s: %d columns, %d rows", tableName, len(result.Columns), len(result.Rows))
	}
}
