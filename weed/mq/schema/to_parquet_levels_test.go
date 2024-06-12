package schema

import (
	"github.com/seaweedfs/seaweedfs/weed/pb/schema_pb"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestToParquetLevels(t *testing.T) {
	type args struct {
		recordType *schema_pb.RecordType
	}
	tests := []struct {
		name string
		args args
		want *ParquetLevels
	}{
		{
			name: "nested type",
			args: args{
				RecordTypeBegin().
					WithField("ID", TypeInt64).
					WithField("CreatedAt", TypeInt64).
					WithRecordField("Person",
						RecordTypeBegin().
							WithField("zName", TypeString).
							WithField("emails", ListOf(TypeString)).
							RecordTypeEnd()).
					WithField("Company", TypeString).
					WithRecordField("Address",
						RecordTypeBegin().
							WithField("Street", TypeString).
							WithField("City", TypeString).
							RecordTypeEnd()).
					RecordTypeEnd(),
			},
			want: &ParquetLevels{
				startColumnIndex: 0,
				endColumnIndex:   7,
				definitionDepth:  0,
				levels: map[string]*ParquetLevels{
					"Address": {
						startColumnIndex: 0,
						endColumnIndex:   2,
						definitionDepth:  1,
						levels: map[string]*ParquetLevels{
							"City": {
								startColumnIndex: 0,
								endColumnIndex:   1,
								definitionDepth:  2,
							},
							"Street": {
								startColumnIndex: 1,
								endColumnIndex:   2,
								definitionDepth:  2,
							},
						},
					},
					"Company": {
						startColumnIndex: 2,
						endColumnIndex:   3,
						definitionDepth:  1,
					},
					"CreatedAt": {
						startColumnIndex: 3,
						endColumnIndex:   4,
						definitionDepth:  1,
					},
					"ID": {
						startColumnIndex: 4,
						endColumnIndex:   5,
						definitionDepth:  1,
					},
					"Person": {
						startColumnIndex: 5,
						endColumnIndex:   7,
						definitionDepth:  1,
						levels: map[string]*ParquetLevels{
							"emails": {
								startColumnIndex: 5,
								endColumnIndex:   6,
								definitionDepth:  2,
							},
							"zName": {
								startColumnIndex: 6,
								endColumnIndex:   7,
								definitionDepth:  2,
							},
						},
					},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ToParquetLevels(tt.args.recordType)
			assert.Nil(t, err)
			assert.Equalf(t, tt.want, got, "ToParquetLevels(%v)", tt.args.recordType)
		})
	}
}
