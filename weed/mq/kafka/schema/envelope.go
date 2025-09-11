package schema

import (
	"encoding/binary"
	"fmt"
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
	Format   Format
	SchemaID uint32
	Indexes  []int    // For Protobuf nested message resolution
	Payload  []byte   // The actual encoded data
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
		Format:   FormatAvro, // Default assumption; will be refined by schema registry lookup
		SchemaID: schemaID,
		Indexes:  nil,
		Payload:  data[5:], // Default: payload starts after schema ID
	}

	// Try to detect Protobuf format by looking for message indexes
	// Protobuf messages in Confluent format may have varint-encoded indexes
	// after the schema ID to identify nested message types
	if protobufEnvelope, isProtobuf := ParseConfluentProtobufEnvelope(data); isProtobuf {
		// If it looks like Protobuf (has valid indexes), use that parsing
		if len(protobufEnvelope.Indexes) > 0 {
			return protobufEnvelope, true
		}
	}
	
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
	result := make([]byte, 5, 5+len(payload)+len(indexes)*4)
	result[0] = 0x00 // Magic byte
	binary.BigEndian.PutUint32(result[1:5], schemaID)
	
	// For Protobuf, add indexes as varints (simplified for Phase 1)
	if format == FormatProtobuf && len(indexes) > 0 {
		// TODO: Implement proper varint encoding for Protobuf indexes in Phase 5
		// For now, we'll just append the payload
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
