package logstore

import (
	"encoding/json"
	"os"
	"testing"

	parquet "github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/compress/zstd"
	"github.com/seaweedfs/seaweedfs/weed/mq/schema"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/schema_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"google.golang.org/protobuf/proto"
)

type TopicConfig struct {
	RecordType struct {
		Fields []struct {
			Name       string `json:"name"`
			FieldIndex int    `json:"fieldIndex"`
			Type       struct {
				ScalarType string `json:"scalarType"`
			} `json:"type"`
			IsRepeated bool `json:"isRepeated"`
			IsRequired bool `json:"isRequired"`
		} `json:"fields"`
	} `json:"recordType"`
}

func TestReproGrowslice(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping repro in short mode")
	}

	// Load topic.conf
	confData, err := os.ReadFile("topic.conf")
	if err != nil {
		t.Fatalf("read topic.conf: %v", err)
	}
	var tc TopicConfig
	if err := json.Unmarshal(confData, &tc); err != nil {
		t.Fatalf("unmarshal topic.conf: %v", err)
	}

	// Build record type
	b := schema.RecordTypeBegin()
	for _, f := range tc.RecordType.Fields {
		var typ *schema_pb.Type
		switch f.Type.ScalarType {
		case "INT64":
			typ = schema.TypeInt64
		case "STRING":
			typ = schema.TypeString
		case "DOUBLE":
			typ = schema.TypeDouble
		case "BYTES":
			typ = schema.TypeBytes
		default:
			t.Fatalf("unknown scalar type: %s", f.Type.ScalarType)
		}
		b = b.WithField(f.Name, typ)
	}
	recordType := b.RecordTypeEnd()
	recordType = schema.NewRecordTypeBuilder(recordType).
		WithField(SW_COLUMN_NAME_TS, schema.TypeInt64).
		WithField(SW_COLUMN_NAME_KEY, schema.TypeBytes).
		RecordTypeEnd()

	ps, err := schema.ToParquetSchema("repro", recordType)
	if err != nil {
		t.Fatalf("schema: %v", err)
	}
	levels, err := schema.ToParquetLevels(recordType)
	if err != nil {
		t.Fatalf("levels: %v", err)
	}

	// Load production log file
	data, err := os.ReadFile("user_events_log_file")
	if err != nil {
		t.Fatalf("read log file: %v", err)
	}
	entries, err := parseReproLogFile(data)
	if err != nil {
		t.Fatalf("parse log file: %v", err)
	}

	// Create writer
	tmp, err := os.CreateTemp(".", "repro*.parquet")
	if err != nil {
		t.Fatalf("tmp parquet: %v", err)
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

	// Build rows (first 600 or available)
	rows := make([]parquet.Row, 0, 600)
	for _, e := range entries {
		if isControlEntry(e) {
			continue
		}
		rb.Reset()
		rec := &schema_pb.RecordValue{}
		if err := proto.Unmarshal(e.Data, rec); err != nil {
			continue
		}
		if rec.Fields == nil {
			rec.Fields = make(map[string]*schema_pb.Value)
		}
		rec.Fields[SW_COLUMN_NAME_TS] = &schema_pb.Value{Kind: &schema_pb.Value_Int64Value{Int64Value: e.TsNs}}
		kb := e.Key
		if kb == nil {
			kb = []byte{}
		}
		rec.Fields[SW_COLUMN_NAME_KEY] = &schema_pb.Value{Kind: &schema_pb.Value_BytesValue{BytesValue: kb}}

		if err := schema.AddRecordValue(rb, recordType, levels, rec); err != nil {
			t.Fatalf("add record: %v", err)
		}
		row := rb.Row()
		rows = append(rows, row)
		if len(rows) >= 600 {
			break
		}
	}
	if len(rows) == 0 {
		t.Fatal("no rows built")
	}

	// Attempt to reproduce panic exactly at WriteRows
	deferredPanicked := false
	defer func() {
		if r := recover(); r != nil {
			deferredPanicked = true
			t.Logf("PANIC REPRODUCED: %v", r)
		}
	}()
	_, err = w.WriteRows(rows)
	if err != nil {
		t.Fatalf("WriteRows error: %v", err)
	}
	if !deferredPanicked {
		t.Logf("WriteRows completed without panic for %d rows", len(rows))
	}
}

func parseReproLogFile(data []byte) ([]*filer_pb.LogEntry, error) {
	var entries []*filer_pb.LogEntry
	for off := 0; off < len(data); {
		if off+4 > len(data) {
			break
		}
		ln := int(util.BytesToUint32(data[off : off+4]))
		off += 4
		if off+ln > len(data) {
			break
		}
		buf := data[off : off+ln]
		off += ln
		le := &filer_pb.LogEntry{}
		if err := proto.Unmarshal(buf, le); err != nil {
			continue
		}
		entries = append(entries, le)
	}
	return entries, nil
}
