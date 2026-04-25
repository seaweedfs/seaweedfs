package parquet_pushdown

import (
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "github.com/seaweedfs/seaweedfs/weed/pb/parquet_pushdown_pb"
)

// Hard caps applied to every incoming request before any work is done.
// These guard against accidental or hostile blow-up; they sit well
// above any plausible legitimate request and are not user-tunable.
const (
	maxDataFiles      = 100_000
	maxColumns        = 10_000
	maxPredicateBytes = 16 * 1024 * 1024 // matches gRPC default max message size
	maxRowIdsCap      = 10_000_000
	maxVectorDim      = 4096
	maxTopK           = 10_000
	maxDeletesPerFile = 10_000
)

// validateRequest enforces request-shape limits before the server
// touches any data. Returns a gRPC status error with InvalidArgument
// when something is wrong, nil otherwise.
//
// This is intentionally schema-only validation — it does not consult
// the Iceberg catalog (that is M3 / catalog-validated mode) or read
// any files. Schema-only checks run in every trust mode.
func validateRequest(req *pb.ParquetPushdownRequest) error {
	if req == nil {
		return status.Error(codes.InvalidArgument, "request is nil")
	}
	if req.Table == "" {
		return status.Error(codes.InvalidArgument, "table is required")
	}
	if len(req.DataFiles) == 0 {
		return status.Error(codes.InvalidArgument, "data_files must not be empty")
	}
	if len(req.DataFiles) > maxDataFiles {
		return status.Errorf(codes.InvalidArgument, "data_files exceeds cap %d", maxDataFiles)
	}
	if len(req.Columns) > maxColumns {
		return status.Errorf(codes.InvalidArgument, "columns exceeds cap %d", maxColumns)
	}
	if len(req.Predicate) > maxPredicateBytes {
		return status.Errorf(codes.InvalidArgument, "predicate exceeds cap %d bytes", maxPredicateBytes)
	}
	if (req.PredicateKind != pb.PredicateKind_PREDICATE_KIND_UNSPECIFIED) != (len(req.Predicate) > 0) {
		return status.Error(codes.InvalidArgument, "predicate_kind and predicate must be set together")
	}
	// A bound predicate is meaningless without naming the schema its
	// field references resolve under, so require schema_id whenever a
	// predicate is present. The deeper check (every field id is in
	// the snapshot's schema) needs catalog access and runs in M3.
	if len(req.Predicate) > 0 && req.SchemaId == 0 {
		return status.Error(codes.InvalidArgument, "predicate requires schema_id so field references can be bound")
	}
	if req.MaxRowIds < 0 || req.MaxRowIds > maxRowIdsCap {
		return status.Errorf(codes.InvalidArgument, "max_row_ids must be in [0, %d]", maxRowIdsCap)
	}
	if req.RequestRowIds && req.MaxRowIds == 0 {
		return status.Error(codes.InvalidArgument, "request_row_ids requires a positive max_row_ids")
	}
	if req.Limit < 0 {
		return status.Error(codes.InvalidArgument, "limit must be non-negative")
	}
	if err := validateVectorQuery(req.VectorQuery); err != nil {
		return err
	}
	for i, df := range req.DataFiles {
		if err := validateDataFile(df); err != nil {
			return status.Errorf(codes.InvalidArgument, "data_files[%d]: %s", i, err.Error())
		}
	}
	return nil
}

func validateVectorQuery(q *pb.VectorQuery) error {
	if q == nil {
		return nil
	}
	if q.Column == nil || (q.Column.FieldId == 0 && q.Column.Path == "") {
		return status.Error(codes.InvalidArgument, "vector_query.column must identify a column")
	}
	if len(q.Vector) == 0 {
		return status.Error(codes.InvalidArgument, "vector_query.vector must not be empty")
	}
	if len(q.Vector) > maxVectorDim {
		return status.Errorf(codes.InvalidArgument, "vector_query.vector dim exceeds cap %d", maxVectorDim)
	}
	if q.TopK <= 0 || q.TopK > maxTopK {
		return status.Errorf(codes.InvalidArgument, "vector_query.top_k must be in [1, %d]", maxTopK)
	}
	if q.Metric == pb.VectorMetric_VECTOR_METRIC_UNSPECIFIED {
		return status.Error(codes.InvalidArgument, "vector_query.metric must be set")
	}
	return nil
}

func validateDataFile(df *pb.DataFileDescriptor) error {
	if df == nil {
		return status.Error(codes.InvalidArgument, "descriptor is nil")
	}
	if df.Path == "" {
		return status.Error(codes.InvalidArgument, "path is required")
	}
	if df.SizeBytes < 0 {
		return status.Error(codes.InvalidArgument, "size_bytes must be non-negative")
	}
	if df.RecordCount < 0 {
		return status.Error(codes.InvalidArgument, "record_count must be non-negative")
	}
	if len(df.Deletes) > maxDeletesPerFile {
		return status.Errorf(codes.InvalidArgument, "deletes exceeds cap %d", maxDeletesPerFile)
	}
	for i, del := range df.Deletes {
		if err := validateDeleteFile(del); err != nil {
			return status.Errorf(codes.InvalidArgument, "deletes[%d]: %s", i, err.Error())
		}
	}
	return nil
}

func validateDeleteFile(del *pb.DeleteFileRef) error {
	if del == nil {
		return status.Error(codes.InvalidArgument, "delete is nil")
	}
	if del.Path == "" {
		return status.Error(codes.InvalidArgument, "path is required")
	}
	switch del.Content {
	case pb.DeleteContent_POSITION_DELETES:
		if del.FileFormat == pb.FileFormat_FILE_FORMAT_PUFFIN {
			// Deletion vector: blob bounds are mandatory and the DV
			// must reference a single data file.
			if del.BlobLength <= 0 {
				return status.Error(codes.InvalidArgument, "deletion vector requires positive blob_length")
			}
			if del.ReferencedDataFile == "" {
				return status.Error(codes.InvalidArgument, "deletion vector requires referenced_data_file")
			}
		}
	case pb.DeleteContent_EQUALITY_DELETES:
		if len(del.EqualityFieldIds) == 0 {
			return status.Error(codes.InvalidArgument, "equality delete requires equality_field_ids")
		}
	default:
		return status.Error(codes.InvalidArgument, "unsupported delete content type")
	}
	return nil
}
