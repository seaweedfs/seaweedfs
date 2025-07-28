package s3api

import (
	"testing"
)

func TestParseTagsHeader(t *testing.T) {
	tests := []struct {
		name        string
		input       string
		expected    map[string]string
		expectError bool
	}{
		{
			name:  "simple tags",
			input: "key1=value1&key2=value2",
			expected: map[string]string{
				"key1": "value1",
				"key2": "value2",
			},
			expectError: false,
		},
		{
			name:  "URL encoded timestamp - issue #7040 scenario",
			input: "Timestamp=2025-07-16%2014%3A40%3A39&Owner=user123",
			expected: map[string]string{
				"Timestamp": "2025-07-16 14:40:39",
				"Owner":     "user123",
			},
			expectError: false,
		},
		{
			name:  "URL encoded key and value",
			input: "my%20key=my%20value&normal=test",
			expected: map[string]string{
				"my key": "my value",
				"normal": "test",
			},
			expectError: false,
		},
		{
			name:  "empty value",
			input: "key1=&key2=value2",
			expected: map[string]string{
				"key1": "",
				"key2": "value2",
			},
			expectError: false,
		},
		{
			name:  "special characters encoded",
			input: "path=/tmp%2Ffile.txt&data=hello%21world",
			expected: map[string]string{
				"path": "/tmp/file.txt",
				"data": "hello!world",
			},
			expectError: false,
		},
		{
			name:        "invalid URL encoding",
			input:       "key1=value%ZZ",
			expected:    nil,
			expectError: true,
		},
		{
			name:  "plus signs and equals in values",
			input: "formula=a%2Bb%3Dc&normal=test",
			expected: map[string]string{
				"formula": "a+b=c",
				"normal":  "test",
			},
			expectError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := parseTagsHeader(tt.input)

			if tt.expectError {
				if err == nil {
					t.Errorf("Expected error but got none")
				}
				return
			}

			if err != nil {
				t.Errorf("Unexpected error: %v", err)
				return
			}

			if len(result) != len(tt.expected) {
				t.Errorf("Expected %d tags, got %d", len(tt.expected), len(result))
				return
			}

			for k, v := range tt.expected {
				if result[k] != v {
					t.Errorf("Expected tag %s=%s, got %s=%s", k, v, k, result[k])
				}
			}
		})
	}
}
