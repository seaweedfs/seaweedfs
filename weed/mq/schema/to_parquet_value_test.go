package schema

import (
	"math/big"
	"testing"
	"time"

	"github.com/parquet-go/parquet-go"
	"github.com/seaweedfs/seaweedfs/weed/pb/schema_pb"
)

func TestToParquetValue_BasicTypes(t *testing.T) {
	tests := []struct {
		name     string
		value    *schema_pb.Value
		expected parquet.Value
		wantErr  bool
	}{
		{
			name: "BoolValue true",
			value: &schema_pb.Value{
				Kind: &schema_pb.Value_BoolValue{BoolValue: true},
			},
			expected: parquet.BooleanValue(true),
		},
		{
			name: "Int32Value",
			value: &schema_pb.Value{
				Kind: &schema_pb.Value_Int32Value{Int32Value: 42},
			},
			expected: parquet.Int32Value(42),
		},
		{
			name: "Int64Value",
			value: &schema_pb.Value{
				Kind: &schema_pb.Value_Int64Value{Int64Value: 12345678901234},
			},
			expected: parquet.Int64Value(12345678901234),
		},
		{
			name: "FloatValue",
			value: &schema_pb.Value{
				Kind: &schema_pb.Value_FloatValue{FloatValue: 3.14159},
			},
			expected: parquet.FloatValue(3.14159),
		},
		{
			name: "DoubleValue",
			value: &schema_pb.Value{
				Kind: &schema_pb.Value_DoubleValue{DoubleValue: 2.718281828},
			},
			expected: parquet.DoubleValue(2.718281828),
		},
		{
			name: "BytesValue",
			value: &schema_pb.Value{
				Kind: &schema_pb.Value_BytesValue{BytesValue: []byte("hello world")},
			},
			expected: parquet.ByteArrayValue([]byte("hello world")),
		},
		{
			name: "BytesValue empty",
			value: &schema_pb.Value{
				Kind: &schema_pb.Value_BytesValue{BytesValue: []byte{}},
			},
			expected: parquet.ByteArrayValue([]byte{}),
		},
		{
			name: "StringValue",
			value: &schema_pb.Value{
				Kind: &schema_pb.Value_StringValue{StringValue: "test string"},
			},
			expected: parquet.ByteArrayValue([]byte("test string")),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := toParquetValue(tt.value)
			if (err != nil) != tt.wantErr {
				t.Errorf("toParquetValue() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !parquetValuesEqual(result, tt.expected) {
				t.Errorf("toParquetValue() = %v, want %v", result, tt.expected)
			}
		})
	}
}

func TestToParquetValue_TimestampValue(t *testing.T) {
	tests := []struct {
		name     string
		value    *schema_pb.Value
		expected parquet.Value
		wantErr  bool
	}{
		{
			name: "Valid TimestampValue UTC",
			value: &schema_pb.Value{
				Kind: &schema_pb.Value_TimestampValue{
					TimestampValue: &schema_pb.TimestampValue{
						TimestampMicros: 1704067200000000, // 2024-01-01 00:00:00 UTC in microseconds
						IsUtc:           true,
					},
				},
			},
			expected: parquet.Int64Value(1704067200000000),
		},
		{
			name: "Valid TimestampValue local",
			value: &schema_pb.Value{
				Kind: &schema_pb.Value_TimestampValue{
					TimestampValue: &schema_pb.TimestampValue{
						TimestampMicros: 1704067200000000,
						IsUtc:           false,
					},
				},
			},
			expected: parquet.Int64Value(1704067200000000),
		},
		{
			name: "TimestampValue zero",
			value: &schema_pb.Value{
				Kind: &schema_pb.Value_TimestampValue{
					TimestampValue: &schema_pb.TimestampValue{
						TimestampMicros: 0,
						IsUtc:           true,
					},
				},
			},
			expected: parquet.Int64Value(0),
		},
		{
			name: "TimestampValue negative (before epoch)",
			value: &schema_pb.Value{
				Kind: &schema_pb.Value_TimestampValue{
					TimestampValue: &schema_pb.TimestampValue{
						TimestampMicros: -1000000, // 1 second before epoch
						IsUtc:           true,
					},
				},
			},
			expected: parquet.Int64Value(-1000000),
		},
		{
			name: "TimestampValue nil pointer",
			value: &schema_pb.Value{
				Kind: &schema_pb.Value_TimestampValue{
					TimestampValue: nil,
				},
			},
			expected: parquet.NullValue(),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := toParquetValue(tt.value)
			if (err != nil) != tt.wantErr {
				t.Errorf("toParquetValue() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !parquetValuesEqual(result, tt.expected) {
				t.Errorf("toParquetValue() = %v, want %v", result, tt.expected)
			}
		})
	}
}

func TestToParquetValue_DateValue(t *testing.T) {
	tests := []struct {
		name     string
		value    *schema_pb.Value
		expected parquet.Value
		wantErr  bool
	}{
		{
			name: "Valid DateValue (2024-01-01)",
			value: &schema_pb.Value{
				Kind: &schema_pb.Value_DateValue{
					DateValue: &schema_pb.DateValue{
						DaysSinceEpoch: 19723, // 2024-01-01 = 19723 days since epoch
					},
				},
			},
			expected: parquet.Int32Value(19723),
		},
		{
			name: "DateValue epoch (1970-01-01)",
			value: &schema_pb.Value{
				Kind: &schema_pb.Value_DateValue{
					DateValue: &schema_pb.DateValue{
						DaysSinceEpoch: 0,
					},
				},
			},
			expected: parquet.Int32Value(0),
		},
		{
			name: "DateValue before epoch",
			value: &schema_pb.Value{
				Kind: &schema_pb.Value_DateValue{
					DateValue: &schema_pb.DateValue{
						DaysSinceEpoch: -365, // 1969-01-01
					},
				},
			},
			expected: parquet.Int32Value(-365),
		},
		{
			name: "DateValue nil pointer",
			value: &schema_pb.Value{
				Kind: &schema_pb.Value_DateValue{
					DateValue: nil,
				},
			},
			expected: parquet.NullValue(),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := toParquetValue(tt.value)
			if (err != nil) != tt.wantErr {
				t.Errorf("toParquetValue() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !parquetValuesEqual(result, tt.expected) {
				t.Errorf("toParquetValue() = %v, want %v", result, tt.expected)
			}
		})
	}
}

func TestToParquetValue_DecimalValue(t *testing.T) {
	tests := []struct {
		name     string
		value    *schema_pb.Value
		expected parquet.Value
		wantErr  bool
	}{
		{
			name: "Small Decimal (precision <= 9) - positive",
			value: &schema_pb.Value{
				Kind: &schema_pb.Value_DecimalValue{
					DecimalValue: &schema_pb.DecimalValue{
						Value:     encodeBigIntToBytes(big.NewInt(12345)), // 123.45 with scale 2
						Precision: 5,
						Scale:     2,
					},
				},
			},
			expected: createFixedLenByteArray(encodeBigIntToBytes(big.NewInt(12345))), // FixedLenByteArray conversion
		},
		{
			name: "Small Decimal (precision <= 9) - negative",
			value: &schema_pb.Value{
				Kind: &schema_pb.Value_DecimalValue{
					DecimalValue: &schema_pb.DecimalValue{
						Value:     encodeBigIntToBytes(big.NewInt(-12345)),
						Precision: 5,
						Scale:     2,
					},
				},
			},
			expected: createFixedLenByteArray(encodeBigIntToBytes(big.NewInt(-12345))), // FixedLenByteArray conversion
		},
		{
			name: "Medium Decimal (9 < precision <= 18)",
			value: &schema_pb.Value{
				Kind: &schema_pb.Value_DecimalValue{
					DecimalValue: &schema_pb.DecimalValue{
						Value:     encodeBigIntToBytes(big.NewInt(123456789012345)),
						Precision: 15,
						Scale:     2,
					},
				},
			},
			expected: createFixedLenByteArray(encodeBigIntToBytes(big.NewInt(123456789012345))), // FixedLenByteArray conversion
		},
		{
			name: "Large Decimal (precision > 18)",
			value: &schema_pb.Value{
				Kind: &schema_pb.Value_DecimalValue{
					DecimalValue: &schema_pb.DecimalValue{
						Value:     []byte{0x01, 0x23, 0x45, 0x67, 0x89, 0xAB, 0xCD, 0xEF}, // Large number as bytes
						Precision: 25,
						Scale:     5,
					},
				},
			},
			expected: createFixedLenByteArray([]byte{0x01, 0x23, 0x45, 0x67, 0x89, 0xAB, 0xCD, 0xEF}), // FixedLenByteArray conversion
		},
		{
			name: "Decimal with zero precision",
			value: &schema_pb.Value{
				Kind: &schema_pb.Value_DecimalValue{
					DecimalValue: &schema_pb.DecimalValue{
						Value:     encodeBigIntToBytes(big.NewInt(0)),
						Precision: 0,
						Scale:     0,
					},
				},
			},
			expected: createFixedLenByteArray(encodeBigIntToBytes(big.NewInt(0))), // Zero as FixedLenByteArray
		},
		{
			name: "Decimal nil pointer",
			value: &schema_pb.Value{
				Kind: &schema_pb.Value_DecimalValue{
					DecimalValue: nil,
				},
			},
			expected: parquet.NullValue(),
		},
		{
			name: "Decimal with nil Value bytes",
			value: &schema_pb.Value{
				Kind: &schema_pb.Value_DecimalValue{
					DecimalValue: &schema_pb.DecimalValue{
						Value:     nil, // This was the original panic cause
						Precision: 5,
						Scale:     2,
					},
				},
			},
			expected: parquet.NullValue(),
		},
		{
			name: "Decimal with empty Value bytes",
			value: &schema_pb.Value{
				Kind: &schema_pb.Value_DecimalValue{
					DecimalValue: &schema_pb.DecimalValue{
						Value:     []byte{}, // Empty slice
						Precision: 5,
						Scale:     2,
					},
				},
			},
			expected: parquet.NullValue(), // Returns null for empty bytes
		},
		{
			name: "Decimal out of int32 range (stored as binary)",
			value: &schema_pb.Value{
				Kind: &schema_pb.Value_DecimalValue{
					DecimalValue: &schema_pb.DecimalValue{
						Value:     encodeBigIntToBytes(big.NewInt(999999999999)), // Too large for int32
						Precision: 5,                                             // But precision says int32
						Scale:     0,
					},
				},
			},
			expected: createFixedLenByteArray(encodeBigIntToBytes(big.NewInt(999999999999))), // FixedLenByteArray
		},
		{
			name: "Decimal out of int64 range (stored as binary)",
			value: &schema_pb.Value{
				Kind: &schema_pb.Value_DecimalValue{
					DecimalValue: &schema_pb.DecimalValue{
						Value: func() []byte {
							// Create a number larger than int64 max
							bigNum := new(big.Int)
							bigNum.SetString("99999999999999999999999999999", 10)
							return encodeBigIntToBytes(bigNum)
						}(),
						Precision: 15, // Says int64 but value is too large
						Scale:     0,
					},
				},
			},
			expected: createFixedLenByteArray(func() []byte {
				bigNum := new(big.Int)
				bigNum.SetString("99999999999999999999999999999", 10)
				return encodeBigIntToBytes(bigNum)
			}()), // Large number as FixedLenByteArray (truncated to 16 bytes)
		},
		{
			name: "Decimal extremely large value (should be rejected)",
			value: &schema_pb.Value{
				Kind: &schema_pb.Value_DecimalValue{
					DecimalValue: &schema_pb.DecimalValue{
						Value:     make([]byte, 100), // 100 bytes > 64 byte limit
						Precision: 100,
						Scale:     0,
					},
				},
			},
			expected: parquet.NullValue(),
			wantErr:  true, // Should return error instead of corrupting data
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := toParquetValue(tt.value)
			if (err != nil) != tt.wantErr {
				t.Errorf("toParquetValue() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !parquetValuesEqual(result, tt.expected) {
				t.Errorf("toParquetValue() = %v, want %v", result, tt.expected)
			}
		})
	}
}

func TestToParquetValue_TimeValue(t *testing.T) {
	tests := []struct {
		name     string
		value    *schema_pb.Value
		expected parquet.Value
		wantErr  bool
	}{
		{
			name: "Valid TimeValue (12:34:56.789)",
			value: &schema_pb.Value{
				Kind: &schema_pb.Value_TimeValue{
					TimeValue: &schema_pb.TimeValue{
						TimeMicros: 45296789000, // 12:34:56.789 in microseconds since midnight
					},
				},
			},
			expected: parquet.Int64Value(45296789000),
		},
		{
			name: "TimeValue midnight",
			value: &schema_pb.Value{
				Kind: &schema_pb.Value_TimeValue{
					TimeValue: &schema_pb.TimeValue{
						TimeMicros: 0,
					},
				},
			},
			expected: parquet.Int64Value(0),
		},
		{
			name: "TimeValue end of day (23:59:59.999999)",
			value: &schema_pb.Value{
				Kind: &schema_pb.Value_TimeValue{
					TimeValue: &schema_pb.TimeValue{
						TimeMicros: 86399999999, // 23:59:59.999999
					},
				},
			},
			expected: parquet.Int64Value(86399999999),
		},
		{
			name: "TimeValue nil pointer",
			value: &schema_pb.Value{
				Kind: &schema_pb.Value_TimeValue{
					TimeValue: nil,
				},
			},
			expected: parquet.NullValue(),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := toParquetValue(tt.value)
			if (err != nil) != tt.wantErr {
				t.Errorf("toParquetValue() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !parquetValuesEqual(result, tt.expected) {
				t.Errorf("toParquetValue() = %v, want %v", result, tt.expected)
			}
		})
	}
}

func TestToParquetValue_EdgeCases(t *testing.T) {
	tests := []struct {
		name     string
		value    *schema_pb.Value
		expected parquet.Value
		wantErr  bool
	}{
		{
			name: "Nil value",
			value: &schema_pb.Value{
				Kind: nil,
			},
			wantErr: true,
		},
		{
			name:    "Completely nil value",
			value:   nil,
			wantErr: true,
		},
		{
			name: "BytesValue with nil slice",
			value: &schema_pb.Value{
				Kind: &schema_pb.Value_BytesValue{BytesValue: nil},
			},
			expected: parquet.ByteArrayValue([]byte{}), // Should convert nil to empty slice
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := toParquetValue(tt.value)
			if (err != nil) != tt.wantErr {
				t.Errorf("toParquetValue() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr && !parquetValuesEqual(result, tt.expected) {
				t.Errorf("toParquetValue() = %v, want %v", result, tt.expected)
			}
		})
	}
}

// Helper function to encode a big.Int to bytes using two's complement representation
func encodeBigIntToBytes(n *big.Int) []byte {
	if n.Sign() == 0 {
		return []byte{0}
	}

	// For positive numbers, just use Bytes()
	if n.Sign() > 0 {
		return n.Bytes()
	}

	// For negative numbers, we need two's complement representation
	bitLen := n.BitLen()
	if bitLen%8 != 0 {
		bitLen += 8 - (bitLen % 8) // Round up to byte boundary
	}
	byteLen := bitLen / 8
	if byteLen == 0 {
		byteLen = 1
	}

	// Calculate 2^(byteLen*8)
	modulus := new(big.Int).Lsh(big.NewInt(1), uint(byteLen*8))

	// Convert negative to positive representation: n + 2^(byteLen*8)
	positive := new(big.Int).Add(n, modulus)

	bytes := positive.Bytes()

	// Pad with leading zeros if needed
	if len(bytes) < byteLen {
		padded := make([]byte, byteLen)
		copy(padded[byteLen-len(bytes):], bytes)
		return padded
	}

	return bytes
}

// Helper function to create a FixedLenByteArray(16) matching our conversion logic
func createFixedLenByteArray(inputBytes []byte) parquet.Value {
	fixedBytes := make([]byte, 16)
	if len(inputBytes) <= 16 {
		// Right-align the value (big-endian) - same as our conversion logic
		copy(fixedBytes[16-len(inputBytes):], inputBytes)
	} else {
		// Truncate if too large, taking the least significant bytes
		copy(fixedBytes, inputBytes[len(inputBytes)-16:])
	}
	return parquet.FixedLenByteArrayValue(fixedBytes)
}

// Helper function to compare parquet values
func parquetValuesEqual(a, b parquet.Value) bool {
	// Handle both being null
	if a.IsNull() && b.IsNull() {
		return true
	}
	if a.IsNull() != b.IsNull() {
		return false
	}

	// Compare kind first
	if a.Kind() != b.Kind() {
		return false
	}

	// Compare based on type
	switch a.Kind() {
	case parquet.Boolean:
		return a.Boolean() == b.Boolean()
	case parquet.Int32:
		return a.Int32() == b.Int32()
	case parquet.Int64:
		return a.Int64() == b.Int64()
	case parquet.Float:
		return a.Float() == b.Float()
	case parquet.Double:
		return a.Double() == b.Double()
	case parquet.ByteArray:
		aBytes := a.ByteArray()
		bBytes := b.ByteArray()
		if len(aBytes) != len(bBytes) {
			return false
		}
		for i, v := range aBytes {
			if v != bBytes[i] {
				return false
			}
		}
		return true
	case parquet.FixedLenByteArray:
		aBytes := a.ByteArray() // FixedLenByteArray also uses ByteArray() method
		bBytes := b.ByteArray()
		if len(aBytes) != len(bBytes) {
			return false
		}
		for i, v := range aBytes {
			if v != bBytes[i] {
				return false
			}
		}
		return true
	default:
		return false
	}
}

// Benchmark tests
func BenchmarkToParquetValue_BasicTypes(b *testing.B) {
	value := &schema_pb.Value{
		Kind: &schema_pb.Value_Int64Value{Int64Value: 12345678901234},
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = toParquetValue(value)
	}
}

func BenchmarkToParquetValue_TimestampValue(b *testing.B) {
	value := &schema_pb.Value{
		Kind: &schema_pb.Value_TimestampValue{
			TimestampValue: &schema_pb.TimestampValue{
				TimestampMicros: time.Now().UnixMicro(),
				IsUtc:           true,
			},
		},
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = toParquetValue(value)
	}
}

func BenchmarkToParquetValue_DecimalValue(b *testing.B) {
	value := &schema_pb.Value{
		Kind: &schema_pb.Value_DecimalValue{
			DecimalValue: &schema_pb.DecimalValue{
				Value:     encodeBigIntToBytes(big.NewInt(123456789012345)),
				Precision: 15,
				Scale:     2,
			},
		},
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = toParquetValue(value)
	}
}
