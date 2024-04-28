package schema

import (
	"github.com/seaweedfs/seaweedfs/weed/pb/schema_pb"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestStructToSchema(t *testing.T) {
	type args struct {
		instance any
	}
	tests := []struct {
		name string
		args args
		want *schema_pb.RecordType
	}{
		{
			name: "scalar type",
			args: args{
				instance: 1,
			},
			want: nil,
		},
		{
			name: "simple struct type",
			args: args{
				instance: struct {
					Field1 int
					Field2 string
				}{},
			},
			want: RecordTypeBegin().
					SetField("Field1", TypeInteger).
					SetField("Field2", TypeString).
				RecordTypeEnd(),
		},
		{
			name: "simple list",
			args: args{
				instance: struct {
					Field1 []int
					Field2 string
				}{},
			},
			want: RecordTypeBegin().
					SetField("Field1", ListOf(TypeInteger)).
					SetField("Field2", TypeString).
				RecordTypeEnd(),
		},
		{
			name: "simple []byte",
			args: args{
				instance: struct {
					Field2 []byte
				}{},
			},
			want: RecordTypeBegin().
					SetField("Field2", TypeBytes).
				RecordTypeEnd(),
		},
		{
			name: "nested simpe structs",
			args: args{
				instance: struct {
					Field1 int
					Field2 struct {
						Field3 string
						Field4 int
					}
				}{},
			},
			want: RecordTypeBegin().
				SetField("Field1", TypeInteger).
				SetRecordField("Field2",
					RecordTypeBegin().
						SetField("Field3", TypeString).
						SetField("Field4", TypeInteger).
					RecordTypeEnd(),
				).
				RecordTypeEnd(),
		},
		{
			name: "nested struct type",
			args: args{
				instance: struct {
					Field1 int
					Field2 struct {
						Field3 string
						Field4 []int
						Field5 struct {
							Field6 string
							Field7 []byte
						}
					}
				}{},
			},
			want: RecordTypeBegin().
				SetField("Field1", TypeInteger).
				SetRecordField("Field2", RecordTypeBegin().
					SetField("Field3", TypeString).
					SetField("Field4", ListOf(TypeInteger)).
					SetRecordField("Field5",
						RecordTypeBegin().
							SetField("Field6", TypeString).
							SetField("Field7", TypeBytes).
						RecordTypeEnd(),
					).RecordTypeEnd(),
				).
				RecordTypeEnd(),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equalf(t, tt.want, StructToSchema(tt.args.instance), "StructToSchema(%v)", tt.args.instance)
		})
	}
}
