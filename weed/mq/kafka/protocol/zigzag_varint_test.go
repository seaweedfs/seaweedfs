package protocol

import (
	"testing"
)

func TestEncodeVarint_ZigZagEncoding(t *testing.T) {
	tests := []struct {
		name     string
		input    int64
		expected []byte
	}{
		{
			name:     "zero",
			input:    0,
			expected: []byte{0x00},
		},
		{
			name:     "positive one",
			input:    1,
			expected: []byte{0x02},
		},
		{
			name:     "negative one (null key)",
			input:    -1,
			expected: []byte{0x01},
		},
		{
			name:     "positive small",
			input:    42,
			expected: []byte{0x54}, // 42 << 1 = 84 = 0x54
		},
		{
			name:     "negative small",
			input:    -42,
			expected: []byte{0x53}, // ((42 << 1) ^ -1) = 83 = 0x53
		},
		{
			name:     "large positive",
			input:    300,
			expected: []byte{0xD8, 0x04}, // 300 << 1 = 600, encoded as varint
		},
		{
			name:     "large negative",
			input:    -300,
			expected: []byte{0xD7, 0x04}, // ((300 << 1) ^ -1) = 599, encoded as varint
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := encodeVarint(tt.input)
			
			if len(result) != len(tt.expected) {
				t.Errorf("Expected length %d, got %d", len(tt.expected), len(result))
				return
			}
			
			for i, expected := range tt.expected {
				if result[i] != expected {
					t.Errorf("At position %d: expected 0x%02X, got 0x%02X", i, expected, result[i])
				}
			}
		})
	}
}

func TestEncodeVarint_NullKeyCompatibility(t *testing.T) {
	// This is the critical test - Kafka clients expect -1 to encode as 0x01
	result := encodeVarint(-1)
	expected := []byte{0x01}
	
	if len(result) != 1 {
		t.Fatalf("Expected single byte for -1, got %d bytes: %v", len(result), result)
	}
	
	if result[0] != expected[0] {
		t.Errorf("Critical null key encoding failed: expected 0x%02X, got 0x%02X", expected[0], result[0])
		t.Errorf("This will break Kafka client compatibility for null keys")
	}
}

func TestEncodeVarint_ZigZagFormula(t *testing.T) {
	// Test the ZigZag formula directly for a few values
	testCases := []struct {
		input    int64
		expected uint64
	}{
		{0, 0},   // 0 << 1 ^ (0 >> 63) = 0
		{1, 2},   // 1 << 1 ^ (1 >> 63) = 2
		{-1, 1},  // -1 << 1 ^ (-1 >> 63) = -2 ^ -1 = 1
		{2, 4},   // 2 << 1 ^ (2 >> 63) = 4
		{-2, 3},  // -2 << 1 ^ (-2 >> 63) = -4 ^ -1 = 3
	}
	
	for _, tc := range testCases {
		t.Run("", func(t *testing.T) {
			zigzag := uint64((tc.input << 1) ^ (tc.input >> 63))
			if zigzag != tc.expected {
				t.Errorf("ZigZag(%d): expected %d, got %d", tc.input, tc.expected, zigzag)
			}
		})
	}
}
