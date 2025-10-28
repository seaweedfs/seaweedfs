package schema

import (
	"encoding/binary"
	"testing"
)

func TestParseConfluentEnvelope(t *testing.T) {
	tests := []struct {
		name         string
		input        []byte
		expectOK     bool
		expectID     uint32
		expectFormat Format
	}{
		{
			name:         "valid Avro message",
			input:        []byte{0x00, 0x00, 0x00, 0x00, 0x01, 0x10, 0x48, 0x65, 0x6c, 0x6c, 0x6f}, // schema ID 1 + "Hello"
			expectOK:     true,
			expectID:     1,
			expectFormat: FormatAvro,
		},
		{
			name:         "valid message with larger schema ID",
			input:        []byte{0x00, 0x00, 0x00, 0x04, 0xd2, 0x02, 0x66, 0x6f, 0x6f}, // schema ID 1234 + "foo"
			expectOK:     true,
			expectID:     1234,
			expectFormat: FormatAvro,
		},
		{
			name:     "too short message",
			input:    []byte{0x00, 0x00, 0x00},
			expectOK: false,
		},
		{
			name:     "no magic byte",
			input:    []byte{0x01, 0x00, 0x00, 0x00, 0x01, 0x48, 0x65, 0x6c, 0x6c, 0x6f},
			expectOK: false,
		},
		{
			name:     "empty message",
			input:    []byte{},
			expectOK: false,
		},
		{
			name:         "minimal valid message",
			input:        []byte{0x00, 0x00, 0x00, 0x00, 0x01}, // schema ID 1, empty payload
			expectOK:     true,
			expectID:     1,
			expectFormat: FormatAvro,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			envelope, ok := ParseConfluentEnvelope(tt.input)

			if ok != tt.expectOK {
				t.Errorf("ParseConfluentEnvelope() ok = %v, want %v", ok, tt.expectOK)
				return
			}

			if !tt.expectOK {
				return // No need to check further if we expected failure
			}

			if envelope.SchemaID != tt.expectID {
				t.Errorf("ParseConfluentEnvelope() schemaID = %v, want %v", envelope.SchemaID, tt.expectID)
			}

			if envelope.Format != tt.expectFormat {
				t.Errorf("ParseConfluentEnvelope() format = %v, want %v", envelope.Format, tt.expectFormat)
			}

			// Verify payload extraction
			expectedPayloadLen := len(tt.input) - 5 // 5 bytes for magic + schema ID
			if len(envelope.Payload) != expectedPayloadLen {
				t.Errorf("ParseConfluentEnvelope() payload length = %v, want %v", len(envelope.Payload), expectedPayloadLen)
			}
		})
	}
}

func TestIsSchematized(t *testing.T) {
	tests := []struct {
		name   string
		input  []byte
		expect bool
	}{
		{
			name:   "schematized message",
			input:  []byte{0x00, 0x00, 0x00, 0x00, 0x01, 0x48, 0x65, 0x6c, 0x6c, 0x6f},
			expect: true,
		},
		{
			name:   "non-schematized message",
			input:  []byte{0x48, 0x65, 0x6c, 0x6c, 0x6f}, // Just "Hello"
			expect: false,
		},
		{
			name:   "empty message",
			input:  []byte{},
			expect: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := IsSchematized(tt.input)
			if result != tt.expect {
				t.Errorf("IsSchematized() = %v, want %v", result, tt.expect)
			}
		})
	}
}

func TestExtractSchemaID(t *testing.T) {
	tests := []struct {
		name     string
		input    []byte
		expectID uint32
		expectOK bool
	}{
		{
			name:     "valid schema ID",
			input:    []byte{0x00, 0x00, 0x00, 0x00, 0x01, 0x48, 0x65, 0x6c, 0x6c, 0x6f},
			expectID: 1,
			expectOK: true,
		},
		{
			name:     "large schema ID",
			input:    []byte{0x00, 0x00, 0x00, 0x04, 0xd2, 0x02, 0x66, 0x6f, 0x6f},
			expectID: 1234,
			expectOK: true,
		},
		{
			name:     "no magic byte",
			input:    []byte{0x01, 0x00, 0x00, 0x00, 0x01},
			expectID: 0,
			expectOK: false,
		},
		{
			name:     "too short",
			input:    []byte{0x00, 0x00},
			expectID: 0,
			expectOK: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			id, ok := ExtractSchemaID(tt.input)

			if ok != tt.expectOK {
				t.Errorf("ExtractSchemaID() ok = %v, want %v", ok, tt.expectOK)
			}

			if id != tt.expectID {
				t.Errorf("ExtractSchemaID() id = %v, want %v", id, tt.expectID)
			}
		})
	}
}

func TestCreateConfluentEnvelope(t *testing.T) {
	tests := []struct {
		name     string
		format   Format
		schemaID uint32
		indexes  []int
		payload  []byte
		expected []byte
	}{
		{
			name:     "simple Avro message",
			format:   FormatAvro,
			schemaID: 1,
			indexes:  nil,
			payload:  []byte("Hello"),
			expected: []byte{0x00, 0x00, 0x00, 0x00, 0x01, 0x48, 0x65, 0x6c, 0x6c, 0x6f},
		},
		{
			name:     "large schema ID",
			format:   FormatAvro,
			schemaID: 1234,
			indexes:  nil,
			payload:  []byte("foo"),
			expected: []byte{0x00, 0x00, 0x00, 0x04, 0xd2, 0x66, 0x6f, 0x6f},
		},
		{
			name:     "empty payload",
			format:   FormatAvro,
			schemaID: 5,
			indexes:  nil,
			payload:  []byte{},
			expected: []byte{0x00, 0x00, 0x00, 0x00, 0x05},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := CreateConfluentEnvelope(tt.format, tt.schemaID, tt.indexes, tt.payload)

			if len(result) != len(tt.expected) {
				t.Errorf("CreateConfluentEnvelope() length = %v, want %v", len(result), len(tt.expected))
				return
			}

			for i, b := range result {
				if b != tt.expected[i] {
					t.Errorf("CreateConfluentEnvelope() byte[%d] = %v, want %v", i, b, tt.expected[i])
				}
			}
		})
	}
}

func TestEnvelopeValidate(t *testing.T) {
	tests := []struct {
		name      string
		envelope  *ConfluentEnvelope
		expectErr bool
	}{
		{
			name: "valid Avro envelope",
			envelope: &ConfluentEnvelope{
				Format:   FormatAvro,
				SchemaID: 1,
				Payload:  []byte("Hello"),
			},
			expectErr: false,
		},
		{
			name: "zero schema ID",
			envelope: &ConfluentEnvelope{
				Format:   FormatAvro,
				SchemaID: 0,
				Payload:  []byte("Hello"),
			},
			expectErr: true,
		},
		{
			name: "empty payload",
			envelope: &ConfluentEnvelope{
				Format:   FormatAvro,
				SchemaID: 1,
				Payload:  []byte{},
			},
			expectErr: true,
		},
		{
			name: "unknown format",
			envelope: &ConfluentEnvelope{
				Format:   FormatUnknown,
				SchemaID: 1,
				Payload:  []byte("Hello"),
			},
			expectErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.envelope.Validate()

			if (err != nil) != tt.expectErr {
				t.Errorf("Envelope.Validate() error = %v, expectErr %v", err, tt.expectErr)
			}
		})
	}
}

func TestEnvelopeMetadata(t *testing.T) {
	envelope := &ConfluentEnvelope{
		Format:   FormatAvro,
		SchemaID: 123,
		Indexes:  []int{1, 2, 3},
		Payload:  []byte("test"),
	}

	metadata := envelope.Metadata()

	if metadata["schema_format"] != "AVRO" {
		t.Errorf("Expected schema_format=AVRO, got %s", metadata["schema_format"])
	}

	if metadata["schema_id"] != "123" {
		t.Errorf("Expected schema_id=123, got %s", metadata["schema_id"])
	}

	if metadata["protobuf_indexes"] != "1,2,3" {
		t.Errorf("Expected protobuf_indexes=1,2,3, got %s", metadata["protobuf_indexes"])
	}
}

// Benchmark tests for performance
func BenchmarkParseConfluentEnvelope(b *testing.B) {
	// Create a test message
	testMsg := make([]byte, 1024)
	testMsg[0] = 0x00                             // Magic byte
	binary.BigEndian.PutUint32(testMsg[1:5], 123) // Schema ID
	// Fill rest with dummy data
	for i := 5; i < len(testMsg); i++ {
		testMsg[i] = byte(i % 256)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = ParseConfluentEnvelope(testMsg)
	}
}

func BenchmarkIsSchematized(b *testing.B) {
	testMsg := []byte{0x00, 0x00, 0x00, 0x00, 0x01, 0x48, 0x65, 0x6c, 0x6c, 0x6f}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = IsSchematized(testMsg)
	}
}
