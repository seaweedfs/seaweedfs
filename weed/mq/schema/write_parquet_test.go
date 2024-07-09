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

func TestWriteReadParquet(t *testing.T) {
	// create a schema_pb.RecordType
	recordType := RecordTypeBegin().
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
		RecordTypeEnd()
	fmt.Printf("RecordType: %v\n", recordType)

	// create a parquet schema
	parquetSchema, err := ToParquetSchema("example", recordType)
	if err != nil {
		t.Fatalf("ToParquetSchema failed: %v", err)
	}
	fmt.Printf("ParquetSchema: %v\n", parquetSchema)

	fmt.Printf("Go Type: %+v\n", parquetSchema.GoType())

	filename := "example.parquet"

	count := 3

	testWritingParquetFile(t, count, filename, parquetSchema, recordType)

	total := testReadingParquetFile(t, filename, parquetSchema, recordType)

	if total != count {
		t.Fatalf("total != 128*1024: %v", total)
	}

	if err = os.Remove(filename); err != nil {
		t.Fatalf("os.Remove failed: %v", err)
	}

}

func testWritingParquetFile(t *testing.T, count int, filename string, parquetSchema *parquet.Schema, recordType *schema_pb.RecordType) {
	parquetLevels, err := ToParquetLevels(recordType)
	if err != nil {
		t.Fatalf("ToParquetLevels failed: %v", err)
	}

	// create a parquet file
	file, err := os.OpenFile(filename, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0664)
	if err != nil {
		t.Fatalf("os.Open failed: %v", err)
	}
	defer file.Close()
	writer := parquet.NewWriter(file, parquetSchema, parquet.Compression(&zstd.Codec{Level: zstd.DefaultLevel}))
	rowBuilder := parquet.NewRowBuilder(parquetSchema)
	for i := 0; i < count; i++ {
		rowBuilder.Reset()
		// generate random data
		recordValue := RecordBegin().
			SetInt64("ID", 1+int64(i)).
			SetInt64("CreatedAt", 2+2*int64(i)).
			SetRecord("Person",
				RecordBegin().
					SetString("zName", fmt.Sprintf("john_%d", i)).
					SetStringList("emails",
						fmt.Sprintf("john_%d@a.com", i),
						fmt.Sprintf("john_%d@b.com", i),
						fmt.Sprintf("john_%d@c.com", i),
						fmt.Sprintf("john_%d@d.com", i),
						fmt.Sprintf("john_%d@e.com", i)).
					RecordEnd()).
			SetString("Company", fmt.Sprintf("company_%d", i)).
			RecordEnd()
		AddRecordValue(rowBuilder, recordType, parquetLevels, recordValue)

		if count < 10 {
			fmt.Printf("RecordValue: %v\n", recordValue)
		}

		row := rowBuilder.Row()

		if count < 10 {
			fmt.Printf("Row: %+v\n", row)
		}

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
	parquetLevels, err := ToParquetLevels(recordType)
	if err != nil {
		t.Fatalf("ToParquetLevels failed: %v", err)
	}

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
			recordValue, err := ToRecordValue(recordType, parquetLevels, row)
			if err != nil {
				t.Fatalf("ToRecordValue failed: %v", err)
			}
			if rowCount < 10 {
				fmt.Printf("RecordValue: %v\n", recordValue)
			}
		}
		total += rowCount
	}
	fmt.Printf("total: %v\n", total)
	return
}
