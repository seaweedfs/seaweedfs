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
		{"Boolean", ScalarType_BOOL, 0},
		{"Integer", ScalarType_INT32, 1},
		{"Long", ScalarType_INT64, 3},
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

func TestField(t *testing.T) {
	field := &Field{
		Name:       "field_name",
		Type:       &Type{Kind: &Type_ScalarType{ScalarType: ScalarType_INT32}},
		FieldIndex: 1,
		IsRepeated: false,
	}
	assert.NotNil(t, field)
}

func TestRecordType(t *testing.T) {
	subRecord := &RecordType{
		Fields: []*Field{
			{
				Name:       "field_1",
				Type:       &Type{Kind: &Type_ScalarType{ScalarType: ScalarType_INT32}},
				FieldIndex: 1,
				IsRepeated: false,
			},
			{
				Name:       "field_2",
				Type:       &Type{Kind: &Type_ScalarType{ScalarType: ScalarType_STRING}},
				FieldIndex: 2,
				IsRepeated: false,
			},
		},
	}
	record := &RecordType{
		Fields: []*Field{
			{
				Name:       "field_key",
				Type:       &Type{Kind: &Type_ScalarType{ScalarType: ScalarType_INT32}},
				FieldIndex: 1,
				IsRepeated: false,
			},
			{
				Name:       "field_record",
				Type:       &Type{Kind: &Type_RecordType{RecordType: subRecord}},
				FieldIndex: 2,
				IsRepeated: false,
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
