package engine

import (
	"context"
	"testing"
)

func TestSQLEngine_ShowDatabases(t *testing.T) {
	engine := NewSQLEngine("localhost:8888")

	result, err := engine.ExecuteSQL(context.Background(), "SHOW DATABASES")
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}

	if result.Error != nil {
		t.Fatalf("Expected no query error, got %v", result.Error)
	}

	if len(result.Columns) != 1 || result.Columns[0] != "Database" {
		t.Errorf("Expected column 'Database', got %v", result.Columns)
	}

	// With no fallback sample data, may return empty results when no real MQ cluster
	t.Logf("Got %d databases (no sample data fallback)", len(result.Rows))

	// Log what we got for inspection
	for i, row := range result.Rows {
		if len(row) > 0 {
			t.Logf("Database %d: %s", i+1, row[0].ToString())
		}
	}

	// Test passes whether we get real databases or empty result (no fallback)
}

func TestSQLEngine_ShowTables(t *testing.T) {
	engine := NewSQLEngine("localhost:8888")

	result, err := engine.ExecuteSQL(context.Background(), "SHOW TABLES")
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}

	if result.Error != nil {
		t.Fatalf("Expected no query error, got %v", result.Error)
	}

	if len(result.Columns) != 1 || result.Columns[0] != "Tables_in_default" {
		t.Errorf("Expected column 'Tables_in_default', got %v", result.Columns)
	}

	// With no fallback sample data, may return empty results when no real MQ cluster
	t.Logf("Got %d tables in default namespace (no sample data fallback)", len(result.Rows))

	// Log what we got for inspection
	for i, row := range result.Rows {
		if len(row) > 0 {
			t.Logf("Table %d: %s", i+1, row[0].ToString())
		}
	}

	// Test passes whether we get real tables or empty result (no fallback)
}

func TestSQLEngine_ParseError(t *testing.T) {
	engine := NewSQLEngine("localhost:8888")

	result, err := engine.ExecuteSQL(context.Background(), "INVALID SQL")
	if err == nil {
		t.Error("Expected parse error for invalid SQL")
	}

	if result.Error == nil {
		t.Error("Expected result error for invalid SQL")
	}
}

func TestSQLEngine_UnsupportedStatement(t *testing.T) {
	engine := NewSQLEngine("localhost:8888")

	// INSERT is not yet implemented
	result, err := engine.ExecuteSQL(context.Background(), "INSERT INTO test VALUES (1)")
	if err == nil {
		t.Error("Expected error for unsupported statement")
	}

	if result.Error == nil {
		t.Error("Expected result error for unsupported statement")
	}
}
