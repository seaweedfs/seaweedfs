package schema

import (
	"encoding/json"
	"github.com/golang/protobuf/proto"
	. "github.com/seaweedfs/seaweedfs/weed/pb/schema_pb"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestEnumScalarType(t *testing.T) {
	tests := []struct {
		name     string
		enum     ScalarType
		expected int32
	}{
		{"Boolean", ScalarType_BOOLEAN, 0},
		{"Integer", ScalarType_INTEGER, 1},
		{"Long", ScalarType_LONG, 3},
		{"Float", ScalarType_FLOAT, 4},
		{"Double", ScalarType_DOUBLE, 5},
		{"Bytes", ScalarType_BYTES, 6},
		{"String", ScalarType_STRING, 7},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, int32(tt.enum))
		})
	}
}

func TestMapType(t *testing.T) {
	mt := &MapType{
		Key:   "key",
		Value: &Type{Kind: &Type_ScalarType{ScalarType: ScalarType_BOOLEAN}},
	}
	assert.NotNil(t, mt)
}

func TestField(t *testing.T) {
	field := &Field{
		Name:        "field_name",
		Type:        &Type{Kind: &Type_ScalarType{ScalarType: ScalarType_INTEGER}},
		Index:       1,
		IsOptional:  true,
		IsRepeated:  false,
	}
	assert.NotNil(t, field)
}

func TestRecordType(t *testing.T) {
	subRecord := &RecordType{
		Fields: []*Field{
			{
				Name:        "field_1",
				Type:        &Type{Kind: &Type_ScalarType{ScalarType: ScalarType_INTEGER}},
				Index:       1,
				IsOptional:  true,
				IsRepeated:  false,
			},
			{
				Name:        "field_2",
				Type:        &Type{Kind: &Type_ScalarType{ScalarType: ScalarType_STRING}},
				Index:       2,
				IsOptional:  true,
				IsRepeated:  false,
			},
		},
	}
	record := &RecordType{
		Fields: []*Field{
			{
				Name:        "field_key",
				Type:        &Type{Kind: &Type_ScalarType{ScalarType: ScalarType_INTEGER}},
				Index:       1,
				IsOptional:  true,
				IsRepeated:  false,
			},
			{
				Name:        "field_record",
				Type:        &Type{Kind: &Type_RecordType{RecordType: subRecord}},
				Index:       2,
				IsOptional:  true,
				IsRepeated:  false,
			},
		},
	}

	// serialize record to protobuf text marshalling
	text := proto.MarshalTextString(record)
	println(text)

	bytes, _ := json.Marshal(record)
	println(string(bytes))

	assert.NotNil(t, record)
}
