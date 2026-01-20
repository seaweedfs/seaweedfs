package dash

import (
	"testing"
)

func TestDetermineMimeType(t *testing.T) {
	tests := []struct {
		filename string
		expected string
	}{
		{"test.txt", "text/plain"},
		{"test.parquet", "application/vnd.apache.parquet"},
		{"test.json", "application/json"},
		{"test.unknown", "application/octet-stream"},
		{"Test.Parquet", "application/vnd.apache.parquet"}, // Test case sensitivity
		{"path/to/file.parquet", "application/vnd.apache.parquet"},
		{"no_extension", "application/octet-stream"},
	}

	for _, tt := range tests {
		t.Run(tt.filename, func(t *testing.T) {
			got := DetermineMimeType(tt.filename)
			if got != tt.expected {
				t.Errorf("DetermineMimeType(%q) = %v; want %v", tt.filename, got, tt.expected)
			}
		})
	}
}
