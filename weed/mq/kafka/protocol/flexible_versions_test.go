package protocol

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"testing"
)

func TestEncodeDecodeUvarint(t *testing.T) {
	testCases := []uint32{
		0, 1, 127, 128, 255, 256, 16383, 16384, 32767, 32768, 65535, 65536, 
		0x1FFFFF, 0x200000, 0x0FFFFFFF, 0x10000000, 0xFFFFFFFF,
	}
	
	for _, value := range testCases {
		t.Run(fmt.Sprintf("value_%d", value), func(t *testing.T) {
			encoded := EncodeUvarint(value)
			decoded, consumed, err := DecodeUvarint(encoded)
			
			if err != nil {
				t.Fatalf("DecodeUvarint failed: %v", err)
			}
			
			if decoded != value {
				t.Errorf("Decoded value %d != original %d", decoded, value)
			}
			
			if consumed != len(encoded) {
				t.Errorf("Consumed %d bytes but encoded %d bytes", consumed, len(encoded))
			}
		})
	}
}

func TestCompactArrayLength(t *testing.T) {
	testCases := []struct {
		name     string
		length   uint32
		expected []byte
	}{
		{"Empty array", 0, []byte{0}},
		{"Single element", 1, []byte{2}},
		{"Small array", 10, []byte{11}},
		{"Large array", 127, []byte{128, 1}}, // 128 = 127+1 encoded as varint (two bytes since >= 128)
	}
	
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			encoded := CompactArrayLength(tc.length)
			if !bytes.Equal(encoded, tc.expected) {
				t.Errorf("CompactArrayLength(%d) = %v, want %v", tc.length, encoded, tc.expected)
			}
			
			// Test round trip
			decoded, consumed, err := DecodeCompactArrayLength(encoded)
			if err != nil {
				t.Fatalf("DecodeCompactArrayLength failed: %v", err)
			}
			
			if decoded != tc.length {
				t.Errorf("Round trip failed: got %d, want %d", decoded, tc.length)
			}
			
			if consumed != len(encoded) {
				t.Errorf("Consumed %d bytes but encoded %d bytes", consumed, len(encoded))
			}
		})
	}
}

func TestCompactStringLength(t *testing.T) {
	testCases := []struct {
		name     string
		length   int
		expected []byte
	}{
		{"Null string", -1, []byte{0}},
		{"Empty string", 0, []byte{1}},
		{"Short string", 5, []byte{6}},
		{"Medium string", 100, []byte{101}}, // 101 encoded as varint (single byte since < 128)
	}
	
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			encoded := CompactStringLength(tc.length)
			if !bytes.Equal(encoded, tc.expected) {
				t.Errorf("CompactStringLength(%d) = %v, want %v", tc.length, encoded, tc.expected)
			}
			
			// Test round trip
			decoded, consumed, err := DecodeCompactStringLength(encoded)
			if err != nil {
				t.Fatalf("DecodeCompactStringLength failed: %v", err)
			}
			
			if decoded != tc.length {
				t.Errorf("Round trip failed: got %d, want %d", decoded, tc.length)
			}
			
			if consumed != len(encoded) {
				t.Errorf("Consumed %d bytes but encoded %d bytes", consumed, len(encoded))
			}
		})
	}
}

func TestFlexibleString(t *testing.T) {
	testCases := []struct {
		name     string
		input    string
		expected []byte
	}{
		{"Empty string", "", []byte{0}},
		{"Hello", "hello", []byte{6, 'h', 'e', 'l', 'l', 'o'}},
		{"Unicode", "测试", []byte{7, 0xE6, 0xB5, 0x8B, 0xE8, 0xAF, 0x95}}, // UTF-8 encoded
	}
	
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			encoded := FlexibleString(tc.input)
			if !bytes.Equal(encoded, tc.expected) {
				t.Errorf("FlexibleString(%q) = %v, want %v", tc.input, encoded, tc.expected)
			}
			
			// Test round trip
			decoded, consumed, err := DecodeFlexibleString(encoded)
			if err != nil {
				t.Fatalf("DecodeFlexibleString failed: %v", err)
			}
			
			if decoded != tc.input {
				t.Errorf("Round trip failed: got %q, want %q", decoded, tc.input)
			}
			
			if consumed != len(encoded) {
				t.Errorf("Consumed %d bytes but encoded %d bytes", consumed, len(encoded))
			}
		})
	}
}

func TestFlexibleNullableString(t *testing.T) {
	// Null string
	nullResult := FlexibleNullableString(nil)
	expected := []byte{0}
	if !bytes.Equal(nullResult, expected) {
		t.Errorf("FlexibleNullableString(nil) = %v, want %v", nullResult, expected)
	}
	
	// Non-null string  
	str := "test"
	nonNullResult := FlexibleNullableString(&str)
	expectedNonNull := []byte{5, 't', 'e', 's', 't'}
	if !bytes.Equal(nonNullResult, expectedNonNull) {
		t.Errorf("FlexibleNullableString(&%q) = %v, want %v", str, nonNullResult, expectedNonNull)
	}
}

func TestTaggedFields(t *testing.T) {
	t.Run("Empty tagged fields", func(t *testing.T) {
		tf := &TaggedFields{}
		encoded := tf.Encode()
		expected := []byte{0}
		
		if !bytes.Equal(encoded, expected) {
			t.Errorf("Empty TaggedFields.Encode() = %v, want %v", encoded, expected)
		}
		
		// Test round trip
		decoded, consumed, err := DecodeTaggedFields(encoded)
		if err != nil {
			t.Fatalf("DecodeTaggedFields failed: %v", err)
		}
		
		if len(decoded.Fields) != 0 {
			t.Errorf("Decoded tagged fields length = %d, want 0", len(decoded.Fields))
		}
		
		if consumed != len(encoded) {
			t.Errorf("Consumed %d bytes but encoded %d bytes", consumed, len(encoded))
		}
	})
	
	t.Run("Single tagged field", func(t *testing.T) {
		tf := &TaggedFields{
			Fields: []TaggedField{
				{Tag: 1, Data: []byte("test")},
			},
		}
		
		encoded := tf.Encode()
		
		// Test round trip
		decoded, consumed, err := DecodeTaggedFields(encoded)
		if err != nil {
			t.Fatalf("DecodeTaggedFields failed: %v", err)
		}
		
		if len(decoded.Fields) != 1 {
			t.Fatalf("Decoded tagged fields length = %d, want 1", len(decoded.Fields))
		}
		
		field := decoded.Fields[0]
		if field.Tag != 1 {
			t.Errorf("Decoded tag = %d, want 1", field.Tag)
		}
		
		if !bytes.Equal(field.Data, []byte("test")) {
			t.Errorf("Decoded data = %v, want %v", field.Data, []byte("test"))
		}
		
		if consumed != len(encoded) {
			t.Errorf("Consumed %d bytes but encoded %d bytes", consumed, len(encoded))
		}
	})
	
	t.Run("Multiple tagged fields", func(t *testing.T) {
		tf := &TaggedFields{
			Fields: []TaggedField{
				{Tag: 1, Data: []byte("first")},
				{Tag: 5, Data: []byte("second")},
			},
		}
		
		encoded := tf.Encode()
		
		// Test round trip
		decoded, consumed, err := DecodeTaggedFields(encoded)
		if err != nil {
			t.Fatalf("DecodeTaggedFields failed: %v", err)
		}
		
		if len(decoded.Fields) != 2 {
			t.Fatalf("Decoded tagged fields length = %d, want 2", len(decoded.Fields))
		}
		
		// Check first field
		field1 := decoded.Fields[0]
		if field1.Tag != 1 {
			t.Errorf("Decoded field 1 tag = %d, want 1", field1.Tag)
		}
		if !bytes.Equal(field1.Data, []byte("first")) {
			t.Errorf("Decoded field 1 data = %v, want %v", field1.Data, []byte("first"))
		}
		
		// Check second field
		field2 := decoded.Fields[1]
		if field2.Tag != 5 {
			t.Errorf("Decoded field 2 tag = %d, want 5", field2.Tag)
		}
		if !bytes.Equal(field2.Data, []byte("second")) {
			t.Errorf("Decoded field 2 data = %v, want %v", field2.Data, []byte("second"))
		}
		
		if consumed != len(encoded) {
			t.Errorf("Consumed %d bytes but encoded %d bytes", consumed, len(encoded))
		}
	})
}

func TestIsFlexibleVersion(t *testing.T) {
	testCases := []struct {
		apiKey     uint16
		apiVersion uint16
		expected   bool
		name       string
	}{
		// ApiVersions
		{18, 2, false, "ApiVersions v2"},
		{18, 3, true, "ApiVersions v3"},
		{18, 4, true, "ApiVersions v4"},
		
		// Metadata
		{3, 8, false, "Metadata v8"},
		{3, 9, true, "Metadata v9"},
		{3, 10, true, "Metadata v10"},
		
		// Fetch
		{1, 11, false, "Fetch v11"},
		{1, 12, true, "Fetch v12"},
		{1, 13, true, "Fetch v13"},
		
		// Produce
		{0, 8, false, "Produce v8"},
		{0, 9, true, "Produce v9"},
		{0, 10, true, "Produce v10"},
		
		// CreateTopics
		{19, 1, false, "CreateTopics v1"},
		{19, 2, true, "CreateTopics v2"},
		{19, 3, true, "CreateTopics v3"},
		
		// Unknown API
		{99, 1, false, "Unknown API"},
	}
	
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := IsFlexibleVersion(tc.apiKey, tc.apiVersion)
			if result != tc.expected {
				t.Errorf("IsFlexibleVersion(%d, %d) = %v, want %v", 
					tc.apiKey, tc.apiVersion, result, tc.expected)
			}
		})
	}
}

func TestParseRequestHeader(t *testing.T) {
	t.Run("Regular version header", func(t *testing.T) {
		// Construct a regular version header (Metadata v1)
		data := make([]byte, 0)
		data = append(data, 0, 3)         // API Key = 3 (Metadata)
		data = append(data, 0, 1)         // API Version = 1  
		data = append(data, 0, 0, 0, 123) // Correlation ID = 123
		data = append(data, 0, 4)         // Client ID length = 4
		data = append(data, 't', 'e', 's', 't') // Client ID = "test"
		data = append(data, 1, 2, 3)      // Request body
		
		header, body, err := ParseRequestHeader(data)
		if err != nil {
			t.Fatalf("ParseRequestHeader failed: %v", err)
		}
		
		if header.APIKey != 3 {
			t.Errorf("APIKey = %d, want 3", header.APIKey)
		}
		if header.APIVersion != 1 {
			t.Errorf("APIVersion = %d, want 1", header.APIVersion)
		}
		if header.CorrelationID != 123 {
			t.Errorf("CorrelationID = %d, want 123", header.CorrelationID)
		}
		if header.ClientID == nil || *header.ClientID != "test" {
			t.Errorf("ClientID = %v, want 'test'", header.ClientID)
		}
		if header.TaggedFields != nil {
			t.Errorf("TaggedFields should be nil for regular versions")
		}
		
		expectedBody := []byte{1, 2, 3}
		if !bytes.Equal(body, expectedBody) {
			t.Errorf("Body = %v, want %v", body, expectedBody)
		}
	})
	
	t.Run("Flexible version header", func(t *testing.T) {
		// Construct a flexible version header (ApiVersions v3)
		data := make([]byte, 0)
		data = append(data, 0, 18)        // API Key = 18 (ApiVersions)
		data = append(data, 0, 3)         // API Version = 3 (flexible)
		
		// Correlation ID = 456 (4 bytes, big endian)
		correlationID := make([]byte, 4)
		binary.BigEndian.PutUint32(correlationID, 456)
		data = append(data, correlationID...)
		
		data = append(data, 5, 't', 'e', 's', 't') // Client ID = "test" (compact string)
		data = append(data, 0)            // Empty tagged fields
		data = append(data, 4, 5, 6)      // Request body
		
		header, body, err := ParseRequestHeader(data)
		if err != nil {
			t.Fatalf("ParseRequestHeader failed: %v", err)
		}
		
		if header.APIKey != 18 {
			t.Errorf("APIKey = %d, want 18", header.APIKey)
		}
		if header.APIVersion != 3 {
			t.Errorf("APIVersion = %d, want 3", header.APIVersion)
		}
		if header.CorrelationID != 456 {
			t.Errorf("CorrelationID = %d, want 456", header.CorrelationID)
		}
		if header.ClientID == nil || *header.ClientID != "test" {
			t.Errorf("ClientID = %v, want 'test'", header.ClientID)
		}
		if header.TaggedFields == nil {
			t.Errorf("TaggedFields should not be nil for flexible versions")
		}
		if len(header.TaggedFields.Fields) != 0 {
			t.Errorf("TaggedFields should be empty")
		}
		
		expectedBody := []byte{4, 5, 6}
		if !bytes.Equal(body, expectedBody) {
			t.Errorf("Body = %v, want %v", body, expectedBody)
		}
	})
	
	t.Run("Null client ID", func(t *testing.T) {
		// Regular version with null client ID
		data := make([]byte, 0)
		data = append(data, 0, 3)         // API Key = 3 (Metadata)
		data = append(data, 0, 1)         // API Version = 1
		
		// Correlation ID = 789 (4 bytes, big endian)
		correlationID := make([]byte, 4)
		binary.BigEndian.PutUint32(correlationID, 789)
		data = append(data, correlationID...)
		
		data = append(data, 0xFF, 0xFF)   // Client ID length = -1 (null)
		data = append(data, 7, 8, 9)      // Request body
		
		header, body, err := ParseRequestHeader(data)
		if err != nil {
			t.Fatalf("ParseRequestHeader failed: %v", err)
		}
		
		if header.ClientID != nil {
			t.Errorf("ClientID = %v, want nil", header.ClientID)
		}
		
		expectedBody := []byte{7, 8, 9}
		if !bytes.Equal(body, expectedBody) {
			t.Errorf("Body = %v, want %v", body, expectedBody)
		}
	})
}

func TestEncodeFlexibleResponse(t *testing.T) {
	correlationID := uint32(123)
	data := []byte{1, 2, 3, 4}
	
	t.Run("Without tagged fields", func(t *testing.T) {
		result := EncodeFlexibleResponse(correlationID, data, false)
		expected := []byte{0, 0, 0, 123, 1, 2, 3, 4}
		
		if !bytes.Equal(result, expected) {
			t.Errorf("EncodeFlexibleResponse = %v, want %v", result, expected)
		}
	})
	
	t.Run("With tagged fields", func(t *testing.T) {
		result := EncodeFlexibleResponse(correlationID, data, true)
		expected := []byte{0, 0, 0, 123, 1, 2, 3, 4, 0} // 0 at end for empty tagged fields
		
		if !bytes.Equal(result, expected) {
			t.Errorf("EncodeFlexibleResponse = %v, want %v", result, expected)
		}
	})
}

func BenchmarkEncodeUvarint(b *testing.B) {
	testValues := []uint32{0, 127, 128, 16383, 16384, 65535, 65536, 0xFFFFFFFF}
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for _, val := range testValues {
			EncodeUvarint(val)
		}
	}
}

func BenchmarkDecodeUvarint(b *testing.B) {
	// Pre-encode test values
	testData := [][]byte{
		EncodeUvarint(0),
		EncodeUvarint(127),
		EncodeUvarint(128),
		EncodeUvarint(16383),
		EncodeUvarint(16384),
		EncodeUvarint(65535),
		EncodeUvarint(65536),
		EncodeUvarint(0xFFFFFFFF),
	}
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for _, data := range testData {
			DecodeUvarint(data)
		}
	}
}

func BenchmarkFlexibleString(b *testing.B) {
	testStrings := []string{"", "a", "hello", "this is a longer test string", "测试中文字符串"}
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for _, s := range testStrings {
			FlexibleString(s)
		}
	}
}
