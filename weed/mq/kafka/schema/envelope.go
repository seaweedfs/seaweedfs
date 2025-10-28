package schema

import (
	"encoding/binary"
	"fmt"

	"github.com/seaweedfs/seaweedfs/weed/glog"
)

// Format represents the schema format type
type Format int

const (
	FormatUnknown Format = iota
	FormatAvro
	FormatProtobuf
	FormatJSONSchema
)

func (f Format) String() string {
	switch f {
	case FormatAvro:
		return "AVRO"
	case FormatProtobuf:
		return "PROTOBUF"
	case FormatJSONSchema:
		return "JSON_SCHEMA"
	default:
		return "UNKNOWN"
	}
}

// ConfluentEnvelope represents the parsed Confluent Schema Registry envelope
type ConfluentEnvelope struct {
	Format        Format
	SchemaID      uint32
	Indexes       []int  // For Protobuf nested message resolution
	Payload       []byte // The actual encoded data
	OriginalBytes []byte // The complete original envelope bytes
}

// ParseConfluentEnvelope parses a Confluent Schema Registry framed message
// Returns the envelope details and whether the message was successfully parsed
func ParseConfluentEnvelope(data []byte) (*ConfluentEnvelope, bool) {
	if len(data) < 5 {
		return nil, false // Too short to contain magic byte + schema ID
	}

	// Check for Confluent magic byte (0x00)
	if data[0] != 0x00 {
		return nil, false // Not a Confluent-framed message
	}

	// Extract schema ID (big-endian uint32)
	schemaID := binary.BigEndian.Uint32(data[1:5])

	envelope := &ConfluentEnvelope{
		Format:        FormatAvro, // Default assumption; will be refined by schema registry lookup
		SchemaID:      schemaID,
		Indexes:       nil,
		Payload:       data[5:], // Default: payload starts after schema ID
		OriginalBytes: data,     // Store the complete original envelope
	}

	// Note: Format detection should be done by the schema registry lookup
	// For now, we'll default to Avro and let the manager determine the actual format
	// based on the schema registry information

	return envelope, true
}

// ParseConfluentProtobufEnvelope parses a Confluent Protobuf envelope with indexes
// This is a specialized version for Protobuf that handles message indexes
//
// Note: This function uses heuristics to distinguish between index varints and
// payload data, which may not be 100% reliable in all cases. For production use,
// consider using ParseConfluentProtobufEnvelopeWithIndexCount if you know the
// expected number of indexes.
func ParseConfluentProtobufEnvelope(data []byte) (*ConfluentEnvelope, bool) {
	// For now, assume no indexes to avoid parsing issues
	// This can be enhanced later when we have better schema information
	return ParseConfluentProtobufEnvelopeWithIndexCount(data, 0)
}

// ParseConfluentProtobufEnvelopeWithIndexCount parses a Confluent Protobuf envelope
// when you know the expected number of indexes
func ParseConfluentProtobufEnvelopeWithIndexCount(data []byte, expectedIndexCount int) (*ConfluentEnvelope, bool) {
	if len(data) < 5 {
		return nil, false
	}

	// Check for Confluent magic byte
	if data[0] != 0x00 {
		return nil, false
	}

	// Extract schema ID (big-endian uint32)
	schemaID := binary.BigEndian.Uint32(data[1:5])

	envelope := &ConfluentEnvelope{
		Format:        FormatProtobuf,
		SchemaID:      schemaID,
		Indexes:       nil,
		Payload:       data[5:], // Default: payload starts after schema ID
		OriginalBytes: data,
	}

	// Parse the expected number of indexes
	offset := 5
	for i := 0; i < expectedIndexCount && offset < len(data); i++ {
		index, bytesRead := readVarint(data[offset:])
		if bytesRead == 0 {
			// Invalid varint, stop parsing
			break
		}
		envelope.Indexes = append(envelope.Indexes, int(index))
		offset += bytesRead
	}

	envelope.Payload = data[offset:]
	return envelope, true
}

// IsSchematized checks if the given bytes represent a Confluent-framed message
func IsSchematized(data []byte) bool {
	_, ok := ParseConfluentEnvelope(data)
	return ok
}

// ExtractSchemaID extracts just the schema ID without full parsing (for quick checks)
func ExtractSchemaID(data []byte) (uint32, bool) {
	if len(data) < 5 || data[0] != 0x00 {
		return 0, false
	}
	return binary.BigEndian.Uint32(data[1:5]), true
}

// CreateConfluentEnvelope creates a Confluent-framed message from components
// This will be useful for reconstructing messages on the Fetch path
func CreateConfluentEnvelope(format Format, schemaID uint32, indexes []int, payload []byte) []byte {
	// Start with magic byte + schema ID (5 bytes minimum)
	// Validate sizes to prevent overflow
	const maxSize = 1 << 30 // 1 GB limit
	indexSize := len(indexes) * 4
	totalCapacity := 5 + len(payload) + indexSize
	if len(payload) > maxSize || indexSize > maxSize || totalCapacity < 0 || totalCapacity > maxSize {
		glog.Errorf("Envelope size too large: payload=%d, indexes=%d", len(payload), len(indexes))
		return nil
	}
	result := make([]byte, 5, totalCapacity)
	result[0] = 0x00 // Magic byte
	binary.BigEndian.PutUint32(result[1:5], schemaID)

	// For Protobuf, add indexes as varints
	if format == FormatProtobuf && len(indexes) > 0 {
		for _, index := range indexes {
			varintBytes := encodeVarint(uint64(index))
			result = append(result, varintBytes...)
		}
	}

	// Append the actual payload
	result = append(result, payload...)

	return result
}

// ValidateEnvelope performs basic validation on a parsed envelope
func (e *ConfluentEnvelope) Validate() error {
	if e.SchemaID == 0 {
		return fmt.Errorf("invalid schema ID: 0")
	}

	if len(e.Payload) == 0 {
		return fmt.Errorf("empty payload")
	}

	// Format-specific validation
	switch e.Format {
	case FormatAvro:
		// Avro payloads should be valid binary data
		// More specific validation will be done by the Avro decoder
	case FormatProtobuf:
		// Protobuf validation will be implemented in Phase 5
	case FormatJSONSchema:
		// JSON Schema validation will be implemented in Phase 6
	default:
		return fmt.Errorf("unsupported format: %v", e.Format)
	}

	return nil
}

// Metadata returns a map of envelope metadata for storage
func (e *ConfluentEnvelope) Metadata() map[string]string {
	metadata := map[string]string{
		"schema_format": e.Format.String(),
		"schema_id":     fmt.Sprintf("%d", e.SchemaID),
	}

	if len(e.Indexes) > 0 {
		// Store indexes for Protobuf reconstruction
		indexStr := ""
		for i, idx := range e.Indexes {
			if i > 0 {
				indexStr += ","
			}
			indexStr += fmt.Sprintf("%d", idx)
		}
		metadata["protobuf_indexes"] = indexStr
	}

	return metadata
}

// encodeVarint encodes a uint64 as a varint
func encodeVarint(value uint64) []byte {
	if value == 0 {
		return []byte{0}
	}

	var result []byte
	for value > 0 {
		b := byte(value & 0x7F)
		value >>= 7

		if value > 0 {
			b |= 0x80 // Set continuation bit
		}

		result = append(result, b)
	}

	return result
}

// readVarint reads a varint from the byte slice and returns the value and bytes consumed
func readVarint(data []byte) (uint64, int) {
	var result uint64
	var shift uint

	for i, b := range data {
		if i >= 10 { // Prevent overflow (max varint is 10 bytes)
			return 0, 0
		}

		result |= uint64(b&0x7F) << shift

		if b&0x80 == 0 {
			// Last byte (MSB is 0)
			return result, i + 1
		}

		shift += 7
	}

	// Incomplete varint
	return 0, 0
}
