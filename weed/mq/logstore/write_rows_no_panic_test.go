package logstore

import (
	"os"
	"testing"

	parquet "github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/compress/zstd"
	"github.com/seaweedfs/seaweedfs/weed/mq/schema"
	"github.com/seaweedfs/seaweedfs/weed/pb/schema_pb"
)

// TestWriteRowsNoPanic builds a representative schema and rows and ensures WriteRows completes without panic.
func TestWriteRowsNoPanic(t *testing.T) {
	// Build schema similar to ecommerce.user_events
	recordType := schema.RecordTypeBegin().
		WithField("id", schema.TypeInt64).
		WithField("user_id", schema.TypeInt64).
		WithField("user_type", schema.TypeString).
		WithField("action", schema.TypeString).
		WithField("status", schema.TypeString).
		WithField("amount", schema.TypeDouble).
		WithField("timestamp", schema.TypeString).
		WithField("metadata", schema.TypeString).
		RecordTypeEnd()

	// Add log columns
	recordType = schema.NewRecordTypeBuilder(recordType).
		WithField(SW_COLUMN_NAME_TS, schema.TypeInt64).
		WithField(SW_COLUMN_NAME_KEY, schema.TypeBytes).
		RecordTypeEnd()

	ps, err := schema.ToParquetSchema("synthetic", recordType)
	if err != nil {
		t.Fatalf("schema: %v", err)
	}
	levels, err := schema.ToParquetLevels(recordType)
	if err != nil {
		t.Fatalf("levels: %v", err)
	}

	tmp, err := os.CreateTemp(".", "synthetic*.parquet")
	if err != nil {
		t.Fatalf("tmp: %v", err)
	}
	defer func() {
		tmp.Close()
		os.Remove(tmp.Name())
	}()

	w := parquet.NewWriter(tmp, ps,
		parquet.Compression(&zstd.Codec{Level: zstd.DefaultLevel}),
		parquet.DataPageStatistics(true),
	)
	defer w.Close()

	rb := parquet.NewRowBuilder(ps)
	var rows []parquet.Row

	// Build a few hundred rows with various optional/missing values and nil/empty keys
	for i := 0; i < 200; i++ {
		rb.Reset()

		rec := &schema_pb.RecordValue{Fields: map[string]*schema_pb.Value{}}
		// Required-like fields present
		rec.Fields["id"] = &schema_pb.Value{Kind: &schema_pb.Value_Int64Value{Int64Value: int64(1000 + i)}}
		rec.Fields["user_id"] = &schema_pb.Value{Kind: &schema_pb.Value_Int64Value{Int64Value: int64(i)}}
		rec.Fields["user_type"] = &schema_pb.Value{Kind: &schema_pb.Value_StringValue{StringValue: "standard"}}
		rec.Fields["action"] = &schema_pb.Value{Kind: &schema_pb.Value_StringValue{StringValue: "click"}}
		rec.Fields["status"] = &schema_pb.Value{Kind: &schema_pb.Value_StringValue{StringValue: "active"}}

		// Optional fields vary: sometimes omitted, sometimes empty
		if i%3 == 0 {
			rec.Fields["amount"] = &schema_pb.Value{Kind: &schema_pb.Value_DoubleValue{DoubleValue: float64(i)}}
		}
		if i%4 == 0 {
			rec.Fields["metadata"] = &schema_pb.Value{Kind: &schema_pb.Value_StringValue{StringValue: ""}}
		}
		if i%5 == 0 {
			rec.Fields["timestamp"] = &schema_pb.Value{Kind: &schema_pb.Value_StringValue{StringValue: "2025-09-03T15:36:29Z"}}
		}

		// Log columns
		rec.Fields[SW_COLUMN_NAME_TS] = &schema_pb.Value{Kind: &schema_pb.Value_Int64Value{Int64Value: int64(1756913789000000000 + i)}}
		var keyBytes []byte
		if i%7 == 0 {
			keyBytes = nil // ensure nil-keys are handled
		} else if i%7 == 1 {
			keyBytes = []byte{} // empty
		} else {
			keyBytes = []byte("key-")
		}
		rec.Fields[SW_COLUMN_NAME_KEY] = &schema_pb.Value{Kind: &schema_pb.Value_BytesValue{BytesValue: keyBytes}}

		if err := schema.AddRecordValue(rb, recordType, levels, rec); err != nil {
			t.Fatalf("add record: %v", err)
		}
		rows = append(rows, rb.Row())
	}

	deferredPanicked := false
	defer func() {
		if r := recover(); r != nil {
			deferredPanicked = true
			t.Fatalf("unexpected panic: %v", r)
		}
	}()

	if _, err := w.WriteRows(rows); err != nil {
		t.Fatalf("WriteRows: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if deferredPanicked {
		t.Fatal("panicked")
	}
}
