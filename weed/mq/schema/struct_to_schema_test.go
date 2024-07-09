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
				WithField("Field1", TypeInt32).
				WithField("Field2", TypeString).
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
				WithField("Field1", ListOf(TypeInt32)).
				WithField("Field2", TypeString).
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
				WithField("Field2", TypeBytes).
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
				WithField("Field1", TypeInt32).
				WithRecordField("Field2",
					RecordTypeBegin().
						WithField("Field3", TypeString).
						WithField("Field4", TypeInt32).
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
				WithField("Field1", TypeInt32).
				WithRecordField("Field2", RecordTypeBegin().
					WithField("Field3", TypeString).
					WithField("Field4", ListOf(TypeInt32)).
					WithRecordField("Field5",
						RecordTypeBegin().
							WithField("Field6", TypeString).
							WithField("Field7", TypeBytes).
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
