package schema

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEncodeDecodeVarint(t *testing.T) {
	testCases := []struct {
		name  string
		value uint64
	}{
		{"zero", 0},
		{"small", 1},
		{"medium", 127},
		{"large", 128},
		{"very_large", 16384},
		{"max_uint32", 4294967295},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Encode the value
			encoded := encodeVarint(tc.value)
			require.NotEmpty(t, encoded)

			// Decode it back
			decoded, bytesRead := readVarint(encoded)
			require.Equal(t, len(encoded), bytesRead, "Should consume all encoded bytes")
			assert.Equal(t, tc.value, decoded, "Decoded value should match original")
		})
	}
}

func TestCreateConfluentEnvelopeWithProtobufIndexes(t *testing.T) {
	testCases := []struct {
		name     string
		format   Format
		schemaID uint32
		indexes  []int
		payload  []byte
	}{
		{
			name:     "avro_no_indexes",
			format:   FormatAvro,
			schemaID: 123,
			indexes:  nil,
			payload:  []byte("avro payload"),
		},
		{
			name:     "protobuf_no_indexes",
			format:   FormatProtobuf,
			schemaID: 456,
			indexes:  nil,
			payload:  []byte("protobuf payload"),
		},
		{
			name:     "protobuf_single_index",
			format:   FormatProtobuf,
			schemaID: 789,
			indexes:  []int{1},
			payload:  []byte("protobuf with index"),
		},
		{
			name:     "protobuf_multiple_indexes",
			format:   FormatProtobuf,
			schemaID: 101112,
			indexes:  []int{0, 1, 2, 3},
			payload:  []byte("protobuf with multiple indexes"),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Create the envelope
			envelope := CreateConfluentEnvelope(tc.format, tc.schemaID, tc.indexes, tc.payload)

			// Verify basic structure
			require.True(t, len(envelope) >= 5, "Envelope should be at least 5 bytes")
			assert.Equal(t, byte(0x00), envelope[0], "Magic byte should be 0x00")

			// Extract and verify schema ID
			extractedSchemaID, ok := ExtractSchemaID(envelope)
			require.True(t, ok, "Should be able to extract schema ID")
			assert.Equal(t, tc.schemaID, extractedSchemaID, "Schema ID should match")

			// Parse the envelope based on format
			if tc.format == FormatProtobuf && len(tc.indexes) > 0 {
				// Use Protobuf-specific parser with known index count
				parsed, ok := ParseConfluentProtobufEnvelopeWithIndexCount(envelope, len(tc.indexes))
				require.True(t, ok, "Should be able to parse Protobuf envelope")
				assert.Equal(t, tc.format, parsed.Format)
				assert.Equal(t, tc.schemaID, parsed.SchemaID)
				assert.Equal(t, tc.indexes, parsed.Indexes, "Indexes should match")
				assert.Equal(t, tc.payload, parsed.Payload, "Payload should match")
			} else {
				// Use generic parser
				parsed, ok := ParseConfluentEnvelope(envelope)
				require.True(t, ok, "Should be able to parse envelope")
				assert.Equal(t, tc.schemaID, parsed.SchemaID)

				if tc.format == FormatProtobuf && len(tc.indexes) == 0 {
					// For Protobuf without indexes, payload should match
					assert.Equal(t, tc.payload, parsed.Payload, "Payload should match")
				} else if tc.format == FormatAvro {
					// For Avro, payload should match (no indexes)
					assert.Equal(t, tc.payload, parsed.Payload, "Payload should match")
				}
			}
		})
	}
}

func TestProtobufEnvelopeRoundTrip(t *testing.T) {
	// Use more realistic index values (typically small numbers for message types)
	originalIndexes := []int{0, 1, 2, 3}
	originalPayload := []byte("test protobuf message data")
	schemaID := uint32(12345)

	// Create envelope
	envelope := CreateConfluentEnvelope(FormatProtobuf, schemaID, originalIndexes, originalPayload)

	// Parse it back with known index count
	parsed, ok := ParseConfluentProtobufEnvelopeWithIndexCount(envelope, len(originalIndexes))
	require.True(t, ok, "Should be able to parse created envelope")

	// Verify all fields
	assert.Equal(t, FormatProtobuf, parsed.Format)
	assert.Equal(t, schemaID, parsed.SchemaID)
	assert.Equal(t, originalIndexes, parsed.Indexes)
	assert.Equal(t, originalPayload, parsed.Payload)
	assert.Equal(t, envelope, parsed.OriginalBytes)
}

func TestVarintEdgeCases(t *testing.T) {
	t.Run("empty_data", func(t *testing.T) {
		value, bytesRead := readVarint([]byte{})
		assert.Equal(t, uint64(0), value)
		assert.Equal(t, 0, bytesRead)
	})

	t.Run("incomplete_varint", func(t *testing.T) {
		// Create an incomplete varint (continuation bit set but no more bytes)
		incompleteVarint := []byte{0x80} // Continuation bit set, but no more bytes
		value, bytesRead := readVarint(incompleteVarint)
		assert.Equal(t, uint64(0), value)
		assert.Equal(t, 0, bytesRead)
	})

	t.Run("max_varint_length", func(t *testing.T) {
		// Create a varint that's too long (more than 10 bytes)
		tooLongVarint := make([]byte, 11)
		for i := 0; i < 10; i++ {
			tooLongVarint[i] = 0x80 // All continuation bits
		}
		tooLongVarint[10] = 0x01 // Final byte

		value, bytesRead := readVarint(tooLongVarint)
		assert.Equal(t, uint64(0), value)
		assert.Equal(t, 0, bytesRead)
	})
}

func TestProtobufEnvelopeValidation(t *testing.T) {
	t.Run("valid_envelope", func(t *testing.T) {
		indexes := []int{1, 2}
		envelope := CreateConfluentEnvelope(FormatProtobuf, 123, indexes, []byte("payload"))
		parsed, ok := ParseConfluentProtobufEnvelopeWithIndexCount(envelope, len(indexes))
		require.True(t, ok)

		err := parsed.Validate()
		assert.NoError(t, err)
	})

	t.Run("zero_schema_id", func(t *testing.T) {
		indexes := []int{1}
		envelope := CreateConfluentEnvelope(FormatProtobuf, 0, indexes, []byte("payload"))
		parsed, ok := ParseConfluentProtobufEnvelopeWithIndexCount(envelope, len(indexes))
		require.True(t, ok)

		err := parsed.Validate()
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "invalid schema ID: 0")
	})

	t.Run("empty_payload", func(t *testing.T) {
		indexes := []int{1}
		envelope := CreateConfluentEnvelope(FormatProtobuf, 123, indexes, []byte{})
		parsed, ok := ParseConfluentProtobufEnvelopeWithIndexCount(envelope, len(indexes))
		require.True(t, ok)

		err := parsed.Validate()
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "empty payload")
	})
}
