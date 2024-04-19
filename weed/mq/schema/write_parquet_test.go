package schema

import (
	"fmt"
	"testing"
)

func TestWriteParquet(t *testing.T) {
	// create a schema_pb.RecordType
	recordType := NewRecordTypeBuilder().
		AddLongField("ID").
		AddLongField("CreatedAt").
		AddRecordField("Person", NewRecordTypeBuilder().
			AddStringField("Name").
			AddListField("emails", TypeString)).Build()
	fmt.Printf("RecordType: %v\n", recordType)

	// create a parquet schema
	parquetSchema, err := ToParquetSchema("example", recordType)
	if err != nil {
		t.Fatalf("ToParquetSchema failed: %v", err)
	}
	fmt.Printf("ParquetSchema: %v\n", parquetSchema)

}
