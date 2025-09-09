package engine

import (
	"context"
	"testing"
)

// TestRealNamespaceDiscovery tests the real namespace discovery functionality
func TestRealNamespaceDiscovery(t *testing.T) {
	engine := NewSQLEngine("localhost:8888")

	// Test SHOW DATABASES with real namespace discovery
	result, err := engine.ExecuteSQL(context.Background(), "SHOW DATABASES")
	if err != nil {
		t.Fatalf("SHOW DATABASES failed: %v", err)
	}

	// Should have Database column
	if len(result.Columns) != 1 || result.Columns[0] != "Database" {
		t.Errorf("Expected 1 column 'Database', got %v", result.Columns)
	}

	// With no fallback sample data, result may be empty if no real MQ cluster
	t.Logf("Discovered %d namespaces (no fallback data):", len(result.Rows))
	if len(result.Rows) == 0 {
		t.Log("  (No namespaces found - requires real SeaweedFS MQ cluster)")
	} else {
		for _, row := range result.Rows {
			if len(row) > 0 {
				t.Logf("  - %s", row[0].ToString())
			}
		}
	}
}

// TestRealTopicDiscovery tests the real topic discovery functionality
func TestRealTopicDiscovery(t *testing.T) {
	engine := NewSQLEngine("localhost:8888")

	// Test SHOW TABLES with real topic discovery (use double quotes for PostgreSQL)
	result, err := engine.ExecuteSQL(context.Background(), "SHOW TABLES FROM \"default\"")
	if err != nil {
		t.Fatalf("SHOW TABLES failed: %v", err)
	}

	// Should have table name column
	expectedColumn := "Tables_in_default"
	if len(result.Columns) != 1 || result.Columns[0] != expectedColumn {
		t.Errorf("Expected 1 column '%s', got %v", expectedColumn, result.Columns)
	}

	// With no fallback sample data, result may be empty if no real MQ cluster or namespace doesn't exist
	t.Logf("Discovered %d topics in 'default' namespace (no fallback data):", len(result.Rows))
	if len(result.Rows) == 0 {
		t.Log("  (No topics found - requires real SeaweedFS MQ cluster with 'default' namespace)")
	} else {
		for _, row := range result.Rows {
			if len(row) > 0 {
				t.Logf("  - %s", row[0].ToString())
			}
		}
	}
}

// TestNamespaceDiscoveryNoFallback tests behavior when filer is unavailable (no sample data)
func TestNamespaceDiscoveryNoFallback(t *testing.T) {
	// This test demonstrates the no-fallback behavior when no real MQ cluster is running
	engine := NewSQLEngine("localhost:8888")

	// Get broker client to test directly
	brokerClient := engine.catalog.brokerClient
	if brokerClient == nil {
		t.Fatal("Expected brokerClient to be initialized")
	}

	// Test namespace listing (should fail without real cluster)
	namespaces, err := brokerClient.ListNamespaces(context.Background())
	if err != nil {
		t.Logf("ListNamespaces failed as expected: %v", err)
		namespaces = []string{} // Set empty for the rest of the test
	}

	// With no fallback sample data, should return empty lists
	if len(namespaces) != 0 {
		t.Errorf("Expected empty namespace list with no fallback, got %v", namespaces)
	}

	// Test topic listing (should return empty list)
	topics, err := brokerClient.ListTopics(context.Background(), "default")
	if err != nil {
		t.Fatalf("ListTopics failed: %v", err)
	}

	// Should have no fallback topics
	if len(topics) != 0 {
		t.Errorf("Expected empty topic list with no fallback, got %v", topics)
	}

	t.Log("No fallback behavior - returns empty lists when filer unavailable")
}
