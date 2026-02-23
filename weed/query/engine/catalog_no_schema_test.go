package engine

import (
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/mq/schema"
)

// TestConvertMQSchemaToTableInfo_NoSchema tests that topics without schemas
// get a default schema with system fields and _value field
func TestConvertMQSchemaToTableInfo_NoSchema(t *testing.T) {
	catalog := NewSchemaCatalog("localhost:9333")

	tests := []struct {
		name        string
		mqSchema    *schema.Schema
		expectError bool
		checkFields func(*testing.T, *TableInfo)
	}{
		{
			name:        "nil schema",
			mqSchema:    nil,
			expectError: false,
			checkFields: func(t *testing.T, info *TableInfo) {
				if info.Schema != nil {
					t.Error("Expected Schema to be nil for topics without schema")
				}
				if len(info.Columns) != 4 {
					t.Errorf("Expected 4 columns, got %d", len(info.Columns))
				}
				expectedCols := map[string]string{
					"_ts":     "TIMESTAMP",
					"_key":    "VARBINARY",
					"_source": "VARCHAR(255)",
					"_value":  "VARBINARY",
				}
				for _, col := range info.Columns {
					expectedType, ok := expectedCols[col.Name]
					if !ok {
						t.Errorf("Unexpected column: %s", col.Name)
						continue
					}
					if col.Type != expectedType {
						t.Errorf("Column %s: expected type %s, got %s", col.Name, expectedType, col.Type)
					}
				}
			},
		},
		{
			name: "schema with nil RecordType",
			mqSchema: &schema.Schema{
				RecordType: nil,
				RevisionId: 1,
			},
			expectError: false,
			checkFields: func(t *testing.T, info *TableInfo) {
				if info.Schema != nil {
					t.Error("Expected Schema to be nil for topics without RecordType")
				}
				if len(info.Columns) != 4 {
					t.Errorf("Expected 4 columns, got %d", len(info.Columns))
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tableInfo, err := catalog.convertMQSchemaToTableInfo("test_namespace", "test_topic", tt.mqSchema)

			if tt.expectError {
				if err == nil {
					t.Error("Expected error but got none")
				}
				return
			}

			if err != nil {
				t.Errorf("Unexpected error: %v", err)
				return
			}

			if tableInfo == nil {
				t.Error("Expected tableInfo but got nil")
				return
			}

			if tt.checkFields != nil {
				tt.checkFields(t, tableInfo)
			}

			// Basic checks
			if tableInfo.Name != "test_topic" {
				t.Errorf("Expected Name 'test_topic', got '%s'", tableInfo.Name)
			}
			if tableInfo.Namespace != "test_namespace" {
				t.Errorf("Expected Namespace 'test_namespace', got '%s'", tableInfo.Namespace)
			}
		})
	}
}
