package protocol

import (
	"encoding/binary"
	"fmt"
)

// FlexibleVersions provides utilities for handling Kafka flexible versions protocol
// Flexible versions use compact arrays/strings and tagged fields for backward compatibility

// CompactArrayLength encodes a length for compact arrays
// Compact arrays encode length as length+1, where 0 means empty array
func CompactArrayLength(length uint32) []byte {
	// Compact arrays use length+1 encoding (0 = null, 1 = empty, n+1 = array of length n)
	// For an empty array (length=0), we return 1 (not 0, which would be null)
	return EncodeUvarint(length + 1)
}

// DecodeCompactArrayLength decodes a compact array length
// Returns the actual length and number of bytes consumed
func DecodeCompactArrayLength(data []byte) (uint32, int, error) {
	if len(data) == 0 {
		return 0, 0, fmt.Errorf("no data for compact array length")
	}

	if data[0] == 0 {
		return 0, 1, nil // Empty array
	}

	length, consumed, err := DecodeUvarint(data)
	if err != nil {
		return 0, 0, fmt.Errorf("decode compact array length: %w", err)
	}

	if length == 0 {
		return 0, consumed, fmt.Errorf("invalid compact array length encoding")
	}

	return length - 1, consumed, nil
}

// CompactStringLength encodes a length for compact strings
// Compact strings encode length as length+1, where 0 means null string
func CompactStringLength(length int) []byte {
	if length < 0 {
		return []byte{0} // Null string
	}
	return EncodeUvarint(uint32(length + 1))
}

// DecodeCompactStringLength decodes a compact string length
// Returns the actual length (-1 for null), and number of bytes consumed
func DecodeCompactStringLength(data []byte) (int, int, error) {
	if len(data) == 0 {
		return 0, 0, fmt.Errorf("no data for compact string length")
	}

	if data[0] == 0 {
		return -1, 1, nil // Null string
	}

	length, consumed, err := DecodeUvarint(data)
	if err != nil {
		return 0, 0, fmt.Errorf("decode compact string length: %w", err)
	}

	if length == 0 {
		return 0, consumed, fmt.Errorf("invalid compact string length encoding")
	}

	return int(length - 1), consumed, nil
}

// EncodeUvarint encodes an unsigned integer using variable-length encoding
// This is used for compact arrays, strings, and tagged fields
func EncodeUvarint(value uint32) []byte {
	var buf []byte
	for value >= 0x80 {
		buf = append(buf, byte(value)|0x80)
		value >>= 7
	}
	buf = append(buf, byte(value))
	return buf
}

// DecodeUvarint decodes a variable-length unsigned integer
// Returns the decoded value and number of bytes consumed
func DecodeUvarint(data []byte) (uint32, int, error) {
	var value uint32
	var shift uint
	var consumed int

	for i, b := range data {
		consumed = i + 1
		value |= uint32(b&0x7F) << shift

		if (b & 0x80) == 0 {
			return value, consumed, nil
		}

		shift += 7
		if shift >= 32 {
			return 0, consumed, fmt.Errorf("uvarint overflow")
		}
	}

	return 0, consumed, fmt.Errorf("incomplete uvarint")
}

// TaggedField represents a tagged field in flexible versions
type TaggedField struct {
	Tag  uint32
	Data []byte
}

// TaggedFields represents a collection of tagged fields
type TaggedFields struct {
	Fields []TaggedField
}

// EncodeTaggedFields encodes tagged fields for flexible versions
func (tf *TaggedFields) Encode() []byte {
	if len(tf.Fields) == 0 {
		return []byte{0} // Empty tagged fields
	}

	var buf []byte

	// Number of tagged fields
	buf = append(buf, EncodeUvarint(uint32(len(tf.Fields)))...)

	for _, field := range tf.Fields {
		// Tag
		buf = append(buf, EncodeUvarint(field.Tag)...)
		// Size
		buf = append(buf, EncodeUvarint(uint32(len(field.Data)))...)
		// Data
		buf = append(buf, field.Data...)
	}

	return buf
}

// DecodeTaggedFields decodes tagged fields from flexible versions
func DecodeTaggedFields(data []byte) (*TaggedFields, int, error) {
	if len(data) == 0 {
		return &TaggedFields{}, 0, fmt.Errorf("no data for tagged fields")
	}

	if data[0] == 0 {
		return &TaggedFields{}, 1, nil // Empty tagged fields
	}

	offset := 0

	// Number of tagged fields
	numFields, consumed, err := DecodeUvarint(data[offset:])
	if err != nil {
		return nil, 0, fmt.Errorf("decode tagged fields count: %w", err)
	}
	offset += consumed

	fields := make([]TaggedField, numFields)

	for i := uint32(0); i < numFields; i++ {
		// Tag
		tag, consumed, err := DecodeUvarint(data[offset:])
		if err != nil {
			return nil, 0, fmt.Errorf("decode tagged field %d tag: %w", i, err)
		}
		offset += consumed

		// Size
		size, consumed, err := DecodeUvarint(data[offset:])
		if err != nil {
			return nil, 0, fmt.Errorf("decode tagged field %d size: %w", i, err)
		}
		offset += consumed

		// Data
		if offset+int(size) > len(data) {
			// More detailed error information
			return nil, 0, fmt.Errorf("tagged field %d data truncated: need %d bytes at offset %d, but only %d total bytes available", i, size, offset, len(data))
		}

		fields[i] = TaggedField{
			Tag:  tag,
			Data: data[offset : offset+int(size)],
		}
		offset += int(size)
	}

	return &TaggedFields{Fields: fields}, offset, nil
}

// IsFlexibleVersion determines if an API version uses flexible versions
// This is API-specific and based on when each API adopted flexible versions
func IsFlexibleVersion(apiKey, apiVersion uint16) bool {
	switch APIKey(apiKey) {
	case APIKeyApiVersions:
		return apiVersion >= 3
	case APIKeyMetadata:
		return apiVersion >= 9
	case APIKeyFetch:
		return apiVersion >= 12
	case APIKeyProduce:
		return apiVersion >= 9
	case APIKeyJoinGroup:
		return apiVersion >= 6
	case APIKeySyncGroup:
		return apiVersion >= 4
	case APIKeyOffsetCommit:
		return apiVersion >= 8
	case APIKeyOffsetFetch:
		return apiVersion >= 6
	case APIKeyFindCoordinator:
		return apiVersion >= 3
	case APIKeyHeartbeat:
		return apiVersion >= 4
	case APIKeyLeaveGroup:
		return apiVersion >= 4
	case APIKeyCreateTopics:
		return apiVersion >= 2
	case APIKeyDeleteTopics:
		return apiVersion >= 4
	default:
		return false
	}
}

// FlexibleString encodes a string for flexible versions (compact format)
func FlexibleString(s string) []byte {
	// Compact strings use length+1 encoding (0 = null, 1 = empty, n+1 = string of length n)
	// For an empty string (s=""), we return length+1 = 1 (not 0, which would be null)
	var buf []byte
	buf = append(buf, CompactStringLength(len(s))...)
	buf = append(buf, []byte(s)...)
	return buf
}

// parseCompactString parses a compact string from flexible protocol
// Returns the string bytes and the number of bytes consumed
func parseCompactString(data []byte) ([]byte, int) {
	if len(data) == 0 {
		return nil, 0
	}

	// Parse compact string length (unsigned varint - no zigzag decoding!)
	length, consumed := decodeUnsignedVarint(data)
	if consumed == 0 {
		return nil, 0
	}

	// Debug logging for compact string parsing

	if length == 0 {
		// Null string (length 0 means null)
		return nil, consumed
	}

	// In compact strings, length is actual length + 1
	// So length 1 means empty string, length > 1 means non-empty
	if length == 0 {
		return nil, consumed // Already handled above
	}
	actualLength := int(length - 1)
	if actualLength < 0 {
		return nil, 0
	}

	if actualLength == 0 {
		// Empty string (length was 1)
		return []byte{}, consumed
	}

	if consumed+actualLength > len(data) {
		return nil, 0
	}

	result := data[consumed : consumed+actualLength]
	return result, consumed + actualLength
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// decodeUnsignedVarint decodes an unsigned varint (no zigzag decoding)
func decodeUnsignedVarint(data []byte) (uint64, int) {
	if len(data) == 0 {
		return 0, 0
	}

	var result uint64
	var shift uint
	var bytesRead int

	for i, b := range data {
		if i > 9 { // varints can be at most 10 bytes
			return 0, 0 // invalid varint
		}

		bytesRead++
		result |= uint64(b&0x7F) << shift

		if (b & 0x80) == 0 {
			// Most significant bit is 0, we're done
			return result, bytesRead
		}

		shift += 7
	}

	return 0, 0 // incomplete varint
}

// FlexibleNullableString encodes a nullable string for flexible versions
func FlexibleNullableString(s *string) []byte {
	if s == nil {
		return []byte{0} // Null string
	}
	return FlexibleString(*s)
}

// DecodeFlexibleString decodes a flexible string
// Returns the string (empty for null) and bytes consumed
func DecodeFlexibleString(data []byte) (string, int, error) {
	length, consumed, err := DecodeCompactStringLength(data)
	if err != nil {
		return "", 0, err
	}

	if length < 0 {
		return "", consumed, nil // Null string -> empty string
	}

	if consumed+length > len(data) {
		return "", 0, fmt.Errorf("string data truncated")
	}

	return string(data[consumed : consumed+length]), consumed + length, nil
}

// FlexibleVersionHeader handles the request header parsing for flexible versions
type FlexibleVersionHeader struct {
	APIKey        uint16
	APIVersion    uint16
	CorrelationID uint32
	ClientID      *string
	TaggedFields  *TaggedFields
}

// parseRegularHeader parses a regular (non-flexible) Kafka request header
func parseRegularHeader(data []byte) (*FlexibleVersionHeader, []byte, error) {
	if len(data) < 8 {
		return nil, nil, fmt.Errorf("header too short")
	}

	header := &FlexibleVersionHeader{}
	offset := 0

	// API Key (2 bytes)
	header.APIKey = binary.BigEndian.Uint16(data[offset : offset+2])
	offset += 2

	// API Version (2 bytes)
	header.APIVersion = binary.BigEndian.Uint16(data[offset : offset+2])
	offset += 2

	// Correlation ID (4 bytes)
	header.CorrelationID = binary.BigEndian.Uint32(data[offset : offset+4])
	offset += 4

	// Regular versions use standard strings
	if len(data) < offset+2 {
		return nil, nil, fmt.Errorf("missing client_id length")
	}

	clientIDLen := int16(binary.BigEndian.Uint16(data[offset : offset+2]))
	offset += 2

	if clientIDLen >= 0 {
		if len(data) < offset+int(clientIDLen) {
			return nil, nil, fmt.Errorf("client_id truncated")
		}
		clientID := string(data[offset : offset+int(clientIDLen)])
		header.ClientID = &clientID
		offset += int(clientIDLen)
	}

	return header, data[offset:], nil
}

// ParseRequestHeader parses a Kafka request header, handling both regular and flexible versions
func ParseRequestHeader(data []byte) (*FlexibleVersionHeader, []byte, error) {
	if len(data) < 8 {
		return nil, nil, fmt.Errorf("header too short")
	}

	header := &FlexibleVersionHeader{}
	offset := 0

	// API Key (2 bytes)
	header.APIKey = binary.BigEndian.Uint16(data[offset : offset+2])
	offset += 2

	// API Version (2 bytes)
	header.APIVersion = binary.BigEndian.Uint16(data[offset : offset+2])
	offset += 2

	// Correlation ID (4 bytes)
	header.CorrelationID = binary.BigEndian.Uint32(data[offset : offset+4])
	offset += 4

	// Client ID handling depends on flexible version
	isFlexible := IsFlexibleVersion(header.APIKey, header.APIVersion)

	if isFlexible {
		// Flexible versions use compact strings
		clientID, consumed, err := DecodeFlexibleString(data[offset:])
		if err != nil {
			return nil, nil, fmt.Errorf("decode flexible client_id: %w", err)
		}
		offset += consumed

		if clientID != "" {
			header.ClientID = &clientID
		}

		// Parse tagged fields in header
		taggedFields, consumed, err := DecodeTaggedFields(data[offset:])
		if err != nil {
			// If tagged fields parsing fails, this might be a regular header sent by kafka-go
			// Fall back to regular header parsing
			return parseRegularHeader(data)
		}
		offset += consumed
		header.TaggedFields = taggedFields

	} else {
		// Regular versions use standard strings
		if len(data) < offset+2 {
			return nil, nil, fmt.Errorf("missing client_id length")
		}

		clientIDLen := int16(binary.BigEndian.Uint16(data[offset : offset+2]))
		offset += 2

		if clientIDLen >= 0 {
			if len(data) < offset+int(clientIDLen) {
				return nil, nil, fmt.Errorf("client_id truncated")
			}

			clientID := string(data[offset : offset+int(clientIDLen)])
			header.ClientID = &clientID
			offset += int(clientIDLen)
		}
		// No tagged fields in regular versions
	}

	return header, data[offset:], nil
}

// EncodeFlexibleResponse encodes a response with proper flexible version formatting
func EncodeFlexibleResponse(correlationID uint32, data []byte, hasTaggedFields bool) []byte {
	response := make([]byte, 4)
	binary.BigEndian.PutUint32(response, correlationID)
	response = append(response, data...)

	if hasTaggedFields {
		// Add empty tagged fields for flexible responses
		response = append(response, 0)
	}

	return response
}
