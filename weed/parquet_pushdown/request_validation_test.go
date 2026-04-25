package parquet_pushdown

import (
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "github.com/seaweedfs/seaweedfs/weed/pb/parquet_pushdown_pb"
)

func validRequest() *pb.ParquetPushdownRequest {
	return &pb.ParquetPushdownRequest{
		Table:      "db.t",
		SnapshotId: 42,
		DataFiles: []*pb.DataFileDescriptor{
			{Path: "s3://bkt/db/t/data/part-00001.parquet", SizeBytes: 1024, RecordCount: 100, DataSequenceNumber: 1},
		},
		Columns: []*pb.ColumnRef{{FieldId: 1}},
	}
}

func TestValidateRequest_OK(t *testing.T) {
	if err := validateRequest(validRequest()); err != nil {
		t.Fatalf("expected ok, got %v", err)
	}
}

func TestValidateRequest_RejectsEmpty(t *testing.T) {
	cases := []struct {
		name string
		mut  func(*pb.ParquetPushdownRequest)
	}{
		{"nil request", func(r *pb.ParquetPushdownRequest) { *r = pb.ParquetPushdownRequest{} }},
		{"missing table", func(r *pb.ParquetPushdownRequest) { r.Table = "" }},
		{"empty data files", func(r *pb.ParquetPushdownRequest) { r.DataFiles = nil }},
		{"data file missing path", func(r *pb.ParquetPushdownRequest) { r.DataFiles[0].Path = "" }},
		{"negative size", func(r *pb.ParquetPushdownRequest) { r.DataFiles[0].SizeBytes = -1 }},
		{"negative record count", func(r *pb.ParquetPushdownRequest) { r.DataFiles[0].RecordCount = -1 }},
		{"negative limit", func(r *pb.ParquetPushdownRequest) { r.Limit = -1 }},
		{"row ids without cap", func(r *pb.ParquetPushdownRequest) { r.RequestRowIds = true; r.MaxRowIds = 0 }},
		{"max row ids over cap", func(r *pb.ParquetPushdownRequest) { r.MaxRowIds = maxRowIdsCap + 1 }},
		{"predicate without kind", func(r *pb.ParquetPushdownRequest) { r.Predicate = []byte("x") }},
		{"predicate kind without bytes", func(r *pb.ParquetPushdownRequest) {
			r.PredicateKind = pb.PredicateKind_PREDICATE_KIND_SUBSTRAIT
		}},
		{"predicate without schema_id", func(r *pb.ParquetPushdownRequest) {
			r.PredicateKind = pb.PredicateKind_PREDICATE_KIND_SUBSTRAIT
			r.Predicate = []byte("bound expression")
			r.SchemaId = 0
		}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			req := validRequest()
			c.mut(req)
			err := validateRequest(req)
			if err == nil {
				t.Fatal("expected error")
			}
			if status.Code(err) != codes.InvalidArgument {
				t.Fatalf("expected InvalidArgument, got %v", status.Code(err))
			}
		})
	}
}

func TestValidateRequest_VectorQuery(t *testing.T) {
	cases := []struct {
		name string
		q    *pb.VectorQuery
		want codes.Code
	}{
		{"nil ok", nil, codes.OK},
		{"missing column", &pb.VectorQuery{Vector: []float32{1}, TopK: 1, Metric: pb.VectorMetric_VECTOR_METRIC_L2}, codes.InvalidArgument},
		{"empty vector", &pb.VectorQuery{Column: &pb.ColumnRef{FieldId: 1}, TopK: 1, Metric: pb.VectorMetric_VECTOR_METRIC_L2}, codes.InvalidArgument},
		{"top_k zero", &pb.VectorQuery{Column: &pb.ColumnRef{FieldId: 1}, Vector: []float32{1}, Metric: pb.VectorMetric_VECTOR_METRIC_L2}, codes.InvalidArgument},
		{"unspecified metric", &pb.VectorQuery{Column: &pb.ColumnRef{FieldId: 1}, Vector: []float32{1}, TopK: 1}, codes.InvalidArgument},
		{"ok", &pb.VectorQuery{Column: &pb.ColumnRef{FieldId: 1}, Vector: []float32{1}, TopK: 1, Metric: pb.VectorMetric_VECTOR_METRIC_L2}, codes.OK},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			req := validRequest()
			req.VectorQuery = c.q
			err := validateRequest(req)
			if status.Code(err) != c.want {
				t.Fatalf("got %v, want %v (err=%v)", status.Code(err), c.want, err)
			}
		})
	}
}

func TestValidateRequest_DeleteFiles(t *testing.T) {
	cases := []struct {
		name string
		del  *pb.DeleteFileRef
		want codes.Code
	}{
		{
			"position-delete file ok",
			&pb.DeleteFileRef{Path: "p", Content: pb.DeleteContent_POSITION_DELETES, FileFormat: pb.FileFormat_FILE_FORMAT_PARQUET},
			codes.OK,
		},
		{
			"deletion vector ok",
			&pb.DeleteFileRef{Path: "p", Content: pb.DeleteContent_POSITION_DELETES, FileFormat: pb.FileFormat_FILE_FORMAT_PUFFIN, BlobLength: 16, ReferencedDataFile: "data.parquet"},
			codes.OK,
		},
		{
			"deletion vector missing blob length",
			&pb.DeleteFileRef{Path: "p", Content: pb.DeleteContent_POSITION_DELETES, FileFormat: pb.FileFormat_FILE_FORMAT_PUFFIN, ReferencedDataFile: "data.parquet"},
			codes.InvalidArgument,
		},
		{
			"deletion vector missing referenced data file",
			&pb.DeleteFileRef{Path: "p", Content: pb.DeleteContent_POSITION_DELETES, FileFormat: pb.FileFormat_FILE_FORMAT_PUFFIN, BlobLength: 16},
			codes.InvalidArgument,
		},
		{
			"equality delete missing field ids",
			&pb.DeleteFileRef{Path: "p", Content: pb.DeleteContent_EQUALITY_DELETES, FileFormat: pb.FileFormat_FILE_FORMAT_PARQUET},
			codes.InvalidArgument,
		},
		{
			"equality delete ok",
			&pb.DeleteFileRef{Path: "p", Content: pb.DeleteContent_EQUALITY_DELETES, FileFormat: pb.FileFormat_FILE_FORMAT_PARQUET, EqualityFieldIds: []int32{1}},
			codes.OK,
		},
		{
			"unspecified content rejected",
			&pb.DeleteFileRef{Path: "p"},
			codes.InvalidArgument,
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			req := validRequest()
			req.DataFiles[0].Deletes = []*pb.DeleteFileRef{c.del}
			err := validateRequest(req)
			if status.Code(err) != c.want {
				t.Fatalf("got %v, want %v (err=%v)", status.Code(err), c.want, err)
			}
		})
	}
}
