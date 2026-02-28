package iscsi

import (
	"bytes"
	"strings"
	"testing"
)

func TestParams(t *testing.T) {
	tests := []struct {
		name string
		run  func(t *testing.T)
	}{
		{"parse_single", testParseSingle},
		{"parse_multiple", testParseMultiple},
		{"parse_empty", testParseEmpty},
		{"parse_trailing_null", testParseTrailingNull},
		{"parse_malformed_no_equals", testParseMalformedNoEquals},
		{"parse_empty_key", testParseEmptyKey},
		{"parse_empty_value", testParseEmptyValue},
		{"parse_duplicate_key", testParseDuplicateKey},
		{"roundtrip", testParamsRoundtrip},
		{"encode_empty", testEncodeEmpty},
		{"set_new_key", testSetNewKey},
		{"set_existing_key", testSetExistingKey},
		{"del_key", testDelKey},
		{"del_nonexistent", testDelNonexistent},
		{"keys_order", testKeysOrder},
		{"each", testEach},
		{"negotiate_number_min", testNegotiateNumberMin},
		{"negotiate_number_clamp", testNegotiateNumberClamp},
		{"negotiate_number_invalid", testNegotiateNumberInvalid},
		{"negotiate_bool_and", testNegotiateBoolAnd},
		{"negotiate_bool_invalid", testNegotiateBoolInvalid},
		{"bool_str", testBoolStr},
		{"value_with_equals", testValueWithEquals},
		{"parse_binary_data_value", testParseBinaryDataValue},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.run(t)
		})
	}
}

func testParseSingle(t *testing.T) {
	data := []byte("TargetName=iqn.2024.com.seaweedfs:vol1\x00")
	p, err := ParseParams(data)
	if err != nil {
		t.Fatal(err)
	}
	if p.Len() != 1 {
		t.Fatalf("expected 1 param, got %d", p.Len())
	}
	v, ok := p.Get("TargetName")
	if !ok || v != "iqn.2024.com.seaweedfs:vol1" {
		t.Fatalf("got %q, %v", v, ok)
	}
}

func testParseMultiple(t *testing.T) {
	data := []byte("InitiatorName=iqn.2024.com.test\x00TargetName=iqn.2024.com.seaweedfs:vol1\x00SessionType=Normal\x00")
	p, err := ParseParams(data)
	if err != nil {
		t.Fatal(err)
	}
	if p.Len() != 3 {
		t.Fatalf("expected 3 params, got %d", p.Len())
	}
	keys := p.Keys()
	if keys[0] != "InitiatorName" || keys[1] != "TargetName" || keys[2] != "SessionType" {
		t.Fatalf("wrong order: %v", keys)
	}
}

func testParseEmpty(t *testing.T) {
	p, err := ParseParams(nil)
	if err != nil {
		t.Fatal(err)
	}
	if p.Len() != 0 {
		t.Fatalf("expected 0, got %d", p.Len())
	}

	p2, err := ParseParams([]byte{})
	if err != nil {
		t.Fatal(err)
	}
	if p2.Len() != 0 {
		t.Fatalf("expected 0, got %d", p2.Len())
	}
}

func testParseTrailingNull(t *testing.T) {
	// Multiple trailing nulls should not cause errors
	data := []byte("Key=Value\x00\x00\x00")
	p, err := ParseParams(data)
	if err != nil {
		t.Fatal(err)
	}
	if p.Len() != 1 {
		t.Fatalf("expected 1, got %d", p.Len())
	}
}

func testParseMalformedNoEquals(t *testing.T) {
	data := []byte("KeyWithoutValue\x00")
	_, err := ParseParams(data)
	if err == nil {
		t.Fatal("expected error")
	}
	if !strings.Contains(err.Error(), "malformed") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func testParseEmptyKey(t *testing.T) {
	data := []byte("=Value\x00")
	_, err := ParseParams(data)
	if err != ErrEmptyKey {
		t.Fatalf("expected ErrEmptyKey, got %v", err)
	}
}

func testParseEmptyValue(t *testing.T) {
	// Empty value is valid in iSCSI (e.g., reject with empty value)
	data := []byte("Key=\x00")
	p, err := ParseParams(data)
	if err != nil {
		t.Fatal(err)
	}
	v, ok := p.Get("Key")
	if !ok || v != "" {
		t.Fatalf("got %q, %v", v, ok)
	}
}

func testParseDuplicateKey(t *testing.T) {
	data := []byte("Key=Value1\x00Key=Value2\x00")
	_, err := ParseParams(data)
	if err == nil {
		t.Fatal("expected error")
	}
	if !strings.Contains(err.Error(), "duplicate") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func testParamsRoundtrip(t *testing.T) {
	original := []byte("InitiatorName=iqn.2024.com.test\x00MaxRecvDataSegmentLength=65536\x00")
	p, err := ParseParams(original)
	if err != nil {
		t.Fatal(err)
	}

	encoded := p.Encode()
	if !bytes.Equal(encoded, original) {
		t.Fatalf("roundtrip mismatch:\n  got:  %q\n  want: %q", encoded, original)
	}
}

func testEncodeEmpty(t *testing.T) {
	p := NewParams()
	if encoded := p.Encode(); encoded != nil {
		t.Fatalf("expected nil, got %q", encoded)
	}
}

func testSetNewKey(t *testing.T) {
	p := NewParams()
	p.Set("Key1", "Val1")
	p.Set("Key2", "Val2")
	if p.Len() != 2 {
		t.Fatalf("expected 2, got %d", p.Len())
	}
	v, _ := p.Get("Key2")
	if v != "Val2" {
		t.Fatalf("got %q", v)
	}
}

func testSetExistingKey(t *testing.T) {
	p := NewParams()
	p.Set("Key", "Old")
	p.Set("Key", "New")
	if p.Len() != 1 {
		t.Fatalf("expected 1, got %d", p.Len())
	}
	v, _ := p.Get("Key")
	if v != "New" {
		t.Fatalf("got %q", v)
	}
}

func testDelKey(t *testing.T) {
	p := NewParams()
	p.Set("A", "1")
	p.Set("B", "2")
	p.Set("C", "3")
	p.Del("B")
	if p.Len() != 2 {
		t.Fatalf("expected 2, got %d", p.Len())
	}
	if _, ok := p.Get("B"); ok {
		t.Fatal("B should be deleted")
	}
	keys := p.Keys()
	if keys[0] != "A" || keys[1] != "C" {
		t.Fatalf("wrong keys: %v", keys)
	}
}

func testDelNonexistent(t *testing.T) {
	p := NewParams()
	p.Set("A", "1")
	p.Del("Z") // should not panic
	if p.Len() != 1 {
		t.Fatalf("expected 1, got %d", p.Len())
	}
}

func testKeysOrder(t *testing.T) {
	data := []byte("Z=1\x00A=2\x00M=3\x00")
	p, err := ParseParams(data)
	if err != nil {
		t.Fatal(err)
	}
	keys := p.Keys()
	if keys[0] != "Z" || keys[1] != "A" || keys[2] != "M" {
		t.Fatalf("order not preserved: %v", keys)
	}
}

func testEach(t *testing.T) {
	p := NewParams()
	p.Set("A", "1")
	p.Set("B", "2")
	var collected []string
	p.Each(func(k, v string) {
		collected = append(collected, k+"="+v)
	})
	if len(collected) != 2 || collected[0] != "A=1" || collected[1] != "B=2" {
		t.Fatalf("Each: %v", collected)
	}
}

func testNegotiateNumberMin(t *testing.T) {
	// Both sides offer a value, result is min
	result, err := NegotiateNumber("65536", 262144, 512, 16777215)
	if err != nil {
		t.Fatal(err)
	}
	if result != 65536 {
		t.Fatalf("expected 65536, got %d", result)
	}

	// Our value is smaller
	result, err = NegotiateNumber("262144", 65536, 512, 16777215)
	if err != nil {
		t.Fatal(err)
	}
	if result != 65536 {
		t.Fatalf("expected 65536, got %d", result)
	}
}

func testNegotiateNumberClamp(t *testing.T) {
	// Offered value below minimum
	result, err := NegotiateNumber("100", 65536, 512, 16777215)
	if err != nil {
		t.Fatal(err)
	}
	if result != 512 {
		t.Fatalf("expected 512, got %d", result)
	}

	// Offered value above maximum
	result, err = NegotiateNumber("99999999", 65536, 512, 16777215)
	if err != nil {
		t.Fatal(err)
	}
	if result != 65536 {
		t.Fatalf("expected 65536, got %d", result)
	}
}

func testNegotiateNumberInvalid(t *testing.T) {
	_, err := NegotiateNumber("notanumber", 65536, 512, 16777215)
	if err == nil {
		t.Fatal("expected error")
	}
}

func testNegotiateBoolAnd(t *testing.T) {
	// Both yes
	r, err := NegotiateBool("Yes", true)
	if err != nil || !r {
		t.Fatalf("Yes+true = %v, %v", r, err)
	}
	// Initiator yes, target no
	r, err = NegotiateBool("Yes", false)
	if err != nil || r {
		t.Fatalf("Yes+false = %v, %v", r, err)
	}
	// Initiator no, target yes
	r, err = NegotiateBool("No", true)
	if err != nil || r {
		t.Fatalf("No+true = %v, %v", r, err)
	}
	// Both no
	r, err = NegotiateBool("No", false)
	if err != nil || r {
		t.Fatalf("No+false = %v, %v", r, err)
	}
}

func testNegotiateBoolInvalid(t *testing.T) {
	_, err := NegotiateBool("Maybe", true)
	if err == nil {
		t.Fatal("expected error")
	}
}

func testBoolStr(t *testing.T) {
	if BoolStr(true) != "Yes" {
		t.Fatal("true should be Yes")
	}
	if BoolStr(false) != "No" {
		t.Fatal("false should be No")
	}
}

func testValueWithEquals(t *testing.T) {
	// Value containing '=' is valid (only first '=' splits key from value)
	data := []byte("TargetAddress=10.0.0.1:3260,1\x00Key=a=b=c\x00")
	p, err := ParseParams(data)
	if err != nil {
		t.Fatal(err)
	}
	v, ok := p.Get("Key")
	if !ok || v != "a=b=c" {
		t.Fatalf("got %q, %v", v, ok)
	}
}

func testParseBinaryDataValue(t *testing.T) {
	// Values can contain arbitrary bytes except null
	data := []byte("Key=\x01\x02\x03\x00")
	p, err := ParseParams(data)
	if err != nil {
		t.Fatal(err)
	}
	v, ok := p.Get("Key")
	if !ok || v != "\x01\x02\x03" {
		t.Fatalf("got %q, %v", v, ok)
	}
}
