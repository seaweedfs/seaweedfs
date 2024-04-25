package schema

import (
	"fmt"
	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/compress/zstd"
	"github.com/seaweedfs/seaweedfs/weed/pb/schema_pb"
	"io"
	"os"
	"testing"
)

func TestWriteParquet(t *testing.T) {
	// create a schema_pb.RecordType
	recordType := NewRecordTypeBuilder().
		AddLongField("ID").
		AddLongField("CreatedAt").
		AddRecordField("Person", NewRecordTypeBuilder().
			AddStringField("zName").
			AddListField("emails", TypeString)).
		AddStringField("Company").Build()
	fmt.Printf("RecordType: %v\n", recordType)

	// create a parquet schema
	parquetSchema, err := ToParquetSchema("example", recordType)
	if err != nil {
		t.Fatalf("ToParquetSchema failed: %v", err)
	}
	fmt.Printf("ParquetSchema: %v\n", parquetSchema)

	fmt.Printf("Go Type: %+v\n", parquetSchema.GoType())

	filename := "example.parquet"

	testWritingParquetFile(t, filename, parquetSchema, recordType)

	total := testReadingParquetFile(t, filename, parquetSchema, recordType)

	if total != 128*1024 {
		t.Fatalf("total != 128*1024: %v", total)
	}

}

func testWritingParquetFile(t *testing.T, filename string, parquetSchema *parquet.Schema, recordType *schema_pb.RecordType) {
	// create a parquet file
	file, err := os.OpenFile(filename, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0664)
	if err != nil {
		t.Fatalf("os.Open failed: %v", err)
	}
	defer file.Close()
	writer := parquet.NewWriter(file, parquetSchema, parquet.Compression(&zstd.Codec{Level: zstd.SpeedDefault}))
	rowBuilder := parquet.NewRowBuilder(parquetSchema)
	for i := 0; i < 128*1024; i++ {
		rowBuilder.Reset()
		// generate random data
		recordValue := NewRecordValueBuilder().
			AddLongValue("ID", 1+int64(i)).
			AddLongValue("CreatedAt", 2+2*int64(i)).
			AddRecordValue("Person", NewRecordValueBuilder().
				AddStringValue("zName", fmt.Sprintf("john_%d", i)).
				AddStringListValue("emails",
					fmt.Sprintf("john_%d@a.com", i),
					fmt.Sprintf("john_%d@b.com", i),
					fmt.Sprintf("john_%d@c.com", i),
					fmt.Sprintf("john_%d@d.com", i),
					fmt.Sprintf("john_%d@e.com", i))).
			AddStringValue("Company", fmt.Sprintf("company_%d", i)).Build()
		AddRecordValue(rowBuilder, recordType, recordValue)

		// fmt.Printf("RecordValue: %v\n", recordValue)

		row := rowBuilder.Row()

		// fmt.Printf("Row: %+v\n", row)

		if err != nil {
			t.Fatalf("rowBuilder.Build failed: %v", err)
		}

		if _, err = writer.WriteRows([]parquet.Row{row}); err != nil {
			t.Fatalf("writer.Write failed: %v", err)
		}
	}
	if err = writer.Close(); err != nil {
		t.Fatalf("writer.WriteStop failed: %v", err)
	}
}

func testReadingParquetFile(t *testing.T, filename string, parquetSchema *parquet.Schema, recordType *schema_pb.RecordType) (total int) {
	// read the parquet file
	file, err := os.Open(filename)
	if err != nil {
		t.Fatalf("os.Open failed: %v", err)
	}
	defer file.Close()
	reader := parquet.NewReader(file, parquetSchema)
	rows := make([]parquet.Row, 128)
	for {
		rowCount, err := reader.ReadRows(rows)
		if err != nil {
			if err == io.EOF {
				break
			}
			t.Fatalf("reader.Read failed: %v", err)
		}
		for i := 0; i < rowCount; i++ {
			row := rows[i]
			// convert parquet row to schema_pb.RecordValue
			_, err := ToRecordValue(recordType, row)
			if err != nil {
				t.Fatalf("ToRecordValue failed: %v", err)
			}
			// fmt.Printf("RecordValue: %v\n", recordValue)
		}
		total += rowCount
	}
	fmt.Printf("total: %v\n", total)
	return
}
