package offset

import (
	"testing"
)

func TestParseTopicPartitionKey(t *testing.T) {
	tests := []struct {
		name        string
		input       string
		expected    ConsumerOffsetKey
		expectError bool
	}{
		{
			name:  "valid topic and partition",
			input: "my-topic:0",
			expected: ConsumerOffsetKey{
				ConsumerGroup: "",
				Topic:         "my-topic",
				Partition:     0,
			},
			expectError: false,
		},
		{
			name:  "valid topic with dash and high partition",
			input: "test-topic-name:42",
			expected: ConsumerOffsetKey{
				ConsumerGroup: "",
				Topic:         "test-topic-name",
				Partition:     42,
			},
			expectError: false,
		},
		{
			name:        "missing colon",
			input:       "my-topic",
			expectError: true,
		},
		{
			name:        "empty topic",
			input:       ":0",
			expectError: true,
		},
		{
			name:        "invalid partition number",
			input:       "my-topic:abc",
			expectError: true,
		},
		{
			name:        "negative partition",
			input:       "my-topic:-1",
			expectError: true,
		},
		{
			name:        "too many colons",
			input:       "my:topic:0",
			expectError: true,
		},
		{
			name:        "empty string",
			input:       "",
			expectError: true,
		},
		{
			name:        "only colon",
			input:       ":",
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := parseTopicPartitionKey(tt.input)
			
			if tt.expectError {
				if err == nil {
					t.Errorf("Expected error for input '%s', but got none", tt.input)
				}
				return
			}
			
			if err != nil {
				t.Errorf("Unexpected error for input '%s': %v", tt.input, err)
				return
			}
			
			if result.ConsumerGroup != tt.expected.ConsumerGroup {
				t.Errorf("Expected group '%s', got '%s'", tt.expected.ConsumerGroup, result.ConsumerGroup)
			}
			
			if result.Topic != tt.expected.Topic {
				t.Errorf("Expected topic '%s', got '%s'", tt.expected.Topic, result.Topic)
			}
			
			if result.Partition != tt.expected.Partition {
				t.Errorf("Expected partition %d, got %d", tt.expected.Partition, result.Partition)
			}
		})
	}
}