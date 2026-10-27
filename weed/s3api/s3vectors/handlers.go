package s3vectors

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/vector_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3tables"
	"google.golang.org/protobuf/types/known/structpb"
)

var handlers = map[string]func(*Server, http.ResponseWriter, *http.Request){
	"CreateVectorBucket": (*Server).handleCreateVectorBucket,
	"GetVectorBucket":    (*Server).handleGetVectorBucket,
	"ListVectorBuckets":  (*Server).handleListVectorBuckets,
	"DeleteVectorBucket": (*Server).handleDeleteVectorBucket,
	"CreateIndex":        (*Server).handleCreateIndex,
	"GetIndex":           (*Server).handleGetIndex,
	"ListIndexes":        (*Server).handleListIndexes,
	"DeleteIndex":        (*Server).handleDeleteIndex,
	"PutVectors":         (*Server).handlePutVectors,
	"GetVectors":         (*Server).handleGetVectors,
	"ListVectors":        (*Server).handleListVectors,
	"DeleteVectors":      (*Server).handleDeleteVectors,
	"QueryVectors":       (*Server).handleQueryVectors,
}

// lance table ids put indexes under a "vector" namespace inside their bucket.
const indexNamespace = "vector"

// Vector bucket records are s3tables table buckets; the S3 Vectors ARN form is
// arn:aws:s3vectors:{region}:{account}:bucket/{name}[/index/{name}].
func (s *Server) vectorBucketArn(bucket string) string {
	return fmt.Sprintf("arn:aws:s3vectors:%s:%s:bucket/%s", s.region, s.accountID, bucket)
}

func (s *Server) indexArn(bucket, index string) string {
	return fmt.Sprintf("%s/index/%s", s.vectorBucketArn(bucket), index)
}

// s3tablesArn renders the same bucket in the table-bucket ARN form the S3
// Tables catalog keys records by.
func (s *Server) s3tablesArn(bucket string) string {
	return fmt.Sprintf("arn:aws:s3tables:%s:%s:bucket/%s", s.region, s.accountID, bucket)
}

func arnParts(v *vectorBucketARNs, name, arn string) error {
	if arn == "" {
		v.bucket = name
		return nil
	}
	if !strings.HasPrefix(arn, "arn:") {
		v.bucket = arn
		return nil
	}
	i := strings.Index(arn, "bucket/")
	if i < 0 {
		return fmt.Errorf("malformed ARN %q", arn)
	}
	rest := arn[i+len("bucket/"):]
	parts := strings.Split(rest, "/index/")
	v.bucket = parts[0]
	if len(parts) == 2 {
		v.index = parts[1]
	}
	return nil
}

// resolveIndexARNs fills bucket and index from any combination of plain names,
// a bucket ARN, and a full index ARN — callers may supply only an indexArn.
func resolveIndexARNs(bucketName, bucketArn, indexName, indexArn string) (vectorBucketARNs, error) {
	var v vectorBucketARNs
	if strings.HasPrefix(indexArn, "arn:") {
		if err := arnParts(&v, "", indexArn); err != nil {
			return v, err
		}
	} else if indexArn != "" {
		return v, fmt.Errorf("indexArn must be an ARN, got %q", indexArn)
	}
	if strings.HasPrefix(bucketArn, "arn:") {
		if err := arnParts(&v, "", bucketArn); err != nil {
			return v, err
		}
	}
	if v.bucket == "" {
		if bucketArn != "" {
			v.bucket = bucketArn
		} else {
			v.bucket = bucketName
		}
	}
	if v.index == "" {
		v.index = indexName
	}
	if v.bucket == "" || v.index == "" {
		return v, fmt.Errorf("vectorBucketName and indexName (or indexArn) are required")
	}
	return v, nil
}

// indexRecord fetches the index's catalog record; the call also authorizes
// the request identity against the index resource.
func (s *Server) indexRecord(ctx context.Context, r *http.Request, bucket, index string) (map[string]interface{}, error) {
	var resp map[string]interface{}
	err := s.catalog(ctx, "GetTable", &s3tables.GetTableRequest{
		TableBucketARN: s.s3tablesArn(bucket),
		Namespace:      []string{indexNamespace},
		Name:           index,
	}, &resp, r)
	return resp, err
}

func (s *Server) catalog(ctx context.Context, operation string, req, resp interface{}, r *http.Request) error {
	return s.tables.Execute(ctx, s.filerClient, operation, req, resp, identityOf(r))
}

// catalogErrCode returns the error code the in-process catalog reports;
// errors from other sources carry no code.
func catalogErrCode(err error) string {
	var e *s3tables.S3TablesError
	if errors.As(err, &e) {
		return e.Type
	}
	return ""
}

func isNotFound(err error) bool {
	switch catalogErrCode(err) {
	case s3tables.ErrCodeNoSuchBucket, s3tables.ErrCodeNoSuchTable, s3tables.ErrCodeNoSuchNamespace:
		return true
	}
	return err != nil && (strings.Contains(err.Error(), "NoSuchTableBucket") ||
		strings.Contains(err.Error(), "NoSuchTable") ||
		strings.Contains(err.Error(), "NotFoundException") ||
		strings.Contains(err.Error(), "NoSuchBucket"))
}

func isAlreadyExists(err error) bool {
	code := catalogErrCode(err)
	return strings.HasSuffix(code, "AlreadyExists") || code == s3tables.ErrCodeConflict ||
		(err != nil && (strings.Contains(err.Error(), "AlreadyExists") || strings.Contains(err.Error(), "Conflict")))
}

func catalogErr(w http.ResponseWriter, err error) {
	msg := err.Error()
	switch {
	case isNotFound(err):
		writeError(w, http.StatusNotFound, "NotFoundException", msg)
	case catalogErrCode(err) == s3tables.ErrCodeAccessDenied ||
		strings.Contains(msg, "AccessDenied") || strings.Contains(msg, "PermissionDenied"):
		writeError(w, http.StatusForbidden, "AccessDeniedException", "access denied")
	case isAlreadyExists(err):
		writeError(w, http.StatusConflict, "ConflictException", msg)
	default:
		writeError(w, http.StatusBadRequest, "ValidationException", msg)
	}
}

func (s *Server) handleCreateVectorBucket(w http.ResponseWriter, r *http.Request) {
	var req CreateVectorBucketRequest
	if err := readJSON(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, "ValidationException", err.Error())
		return
	}
	if req.VectorBucketName == "" {
		writeError(w, http.StatusBadRequest, "ValidationException", "vectorBucketName is required")
		return
	}
	var resp map[string]interface{}
	err := s.catalog(r.Context(), "CreateTableBucket",
		&s3tables.CreateTableBucketRequest{Name: req.VectorBucketName, Format: s3tables.FormatLance}, &resp, r)
	if err != nil {
		catalogErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, &CreateVectorBucketResponse{})
}

func (s *Server) handleGetVectorBucket(w http.ResponseWriter, r *http.Request) {
	var req GetVectorBucketRequest
	if err := readJSON(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, "ValidationException", err.Error())
		return
	}
	var arns vectorBucketARNs
	if err := arnParts(&arns, req.VectorBucketName, req.VectorBucketArn); err != nil {
		writeError(w, http.StatusBadRequest, "ValidationException", err.Error())
		return
	}
	var resp map[string]interface{}
	err := s.catalog(r.Context(), "GetTableBucket",
		&s3tables.GetTableBucketRequest{TableBucketARN: s.s3tablesArn(arns.bucket)}, &resp, r)
	if err != nil {
		catalogErr(w, err)
		return
	}
	created, _ := time.Parse(time.RFC3339, fmt.Sprint(resp["createdAt"]))
	writeJSON(w, http.StatusOK, &GetVectorBucketResponse{VectorBucket: &VectorBucket{
		VectorBucketArn:  s.vectorBucketArn(arns.bucket),
		VectorBucketName: arns.bucket,
		CreationTime:     created,
	}})
}

func (s *Server) handleListVectorBuckets(w http.ResponseWriter, r *http.Request) {
	var req ListVectorBucketsRequest
	if err := readJSON(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, "ValidationException", err.Error())
		return
	}
	var resp struct {
		TableBuckets []struct {
			Arn       string `json:"arn"`
			Name      string `json:"name"`
			CreatedAt string `json:"createdAt"`
			Format    string `json:"format"`
		} `json:"tableBuckets"`
		ContinuationToken string `json:"continuationToken"`
	}
	err := s.catalog(r.Context(), "ListTableBuckets",
		&s3tables.ListTableBucketsRequest{
			MaxBuckets: req.MaxResults, ContinuationToken: req.NextToken, Prefix: req.Prefix,
		}, &resp, r)
	if err != nil {
		catalogErr(w, err)
		return
	}
	out := &ListVectorBucketsResponse{NextToken: resp.ContinuationToken}
	for _, b := range resp.TableBuckets {
		// Buckets pre-dating declared formats accept any format; only a bucket
		// that declared another format is excluded here.
		if b.Format != "" && b.Format != s3tables.FormatLance {
			continue
		}
		created, _ := time.Parse(time.RFC3339, b.CreatedAt)
		out.VectorBuckets = append(out.VectorBuckets, &VectorBucket{
			VectorBucketArn:  s.vectorBucketArn(b.Name),
			VectorBucketName: b.Name,
			CreationTime:     created,
		})
	}
	writeJSON(w, http.StatusOK, out)
}

func (s *Server) handleDeleteVectorBucket(w http.ResponseWriter, r *http.Request) {
	var req DeleteVectorBucketRequest
	if err := readJSON(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, "ValidationException", err.Error())
		return
	}
	var arns vectorBucketARNs
	if err := arnParts(&arns, req.VectorBucketName, req.VectorBucketArn); err != nil {
		writeError(w, http.StatusBadRequest, "ValidationException", err.Error())
		return
	}
	var resp map[string]interface{}
	// Indexes live in an implicit namespace; dropping it best-effort lets an
	// emptied bucket delete, while surviving indexes keep it non-empty.
	_ = s.catalog(r.Context(), "DeleteNamespace", &s3tables.DeleteNamespaceRequest{
		TableBucketARN: s.s3tablesArn(arns.bucket),
		Namespace:      []string{indexNamespace},
	}, &resp, r)
	err := s.catalog(r.Context(), "DeleteTableBucket",
		&s3tables.DeleteTableBucketRequest{TableBucketARN: s.s3tablesArn(arns.bucket)}, &resp, r)
	if err != nil {
		catalogErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, &DeleteVectorBucketResponse{})
}

func distanceMetric(metric string) (vector_pb.DistanceMetric, bool) {
	switch strings.ToLower(metric) {
	case "", "euclidean":
		return vector_pb.DistanceMetric_L2, true
	case "cosine":
		return vector_pb.DistanceMetric_COSINE, true
	case "dotproduct":
		return vector_pb.DistanceMetric_DOT_PRODUCT, true
	}
	return vector_pb.DistanceMetric_L2, false
}

func tableID(bucket, index string) *vector_pb.Table {
	return &vector_pb.Table{Id: []string{bucket, indexNamespace, index}}
}

func (s *Server) handleCreateIndex(w http.ResponseWriter, r *http.Request) {
	var req CreateIndexRequest
	if err := readJSON(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, "ValidationException", err.Error())
		return
	}
	if strings.ToLower(req.DataType) != "float32" {
		writeError(w, http.StatusBadRequest, "ValidationException", "only float32 vectors are supported")
		return
	}
	metric, ok := distanceMetric(req.DistanceMetric)
	if !ok {
		writeError(w, http.StatusBadRequest, "ValidationException", "unknown distanceMetric "+req.DistanceMetric)
		return
	}
	arns, err := resolveIndexARNs(req.VectorBucketName, req.VectorBucketArn, req.IndexName, req.IndexArn)
	if err != nil {
		writeError(w, http.StatusBadRequest, "ValidationException", err.Error())
		return
	}
	if req.Dimension <= 0 || req.Dimension > 4096 {
		writeError(w, http.StatusBadRequest, "ValidationException", "dimension must be 1..=4096")
		return
	}
	metadata := map[string]string{
		"dimension":      fmt.Sprint(req.Dimension),
		"distanceMetric": req.DistanceMetric,
	}
	if req.MetadataConfiguration != nil {
		if keys, err := json.Marshal(req.MetadataConfiguration.NonFilterableMetadataKeys); err == nil {
			metadata["nonFilterableMetadataKeys"] = string(keys)
		}
	}
	metadataJSON, _ := json.Marshal(metadata)
	var nsResp map[string]interface{}
	// The index namespace is implicit on AWS; declare it, tolerating an
	// already-existing namespace.
	if err := s.catalog(r.Context(), "CreateNamespace", &s3tables.CreateNamespaceRequest{
		TableBucketARN: s.s3tablesArn(arns.bucket),
		Namespace:      []string{indexNamespace},
	}, &nsResp, r); err != nil && !isAlreadyExists(err) {
		catalogErr(w, err)
		return
	}
	var resp map[string]interface{}
	err = s.catalog(r.Context(), "CreateTable", &s3tables.CreateTableRequest{
		TableBucketARN: s.s3tablesArn(arns.bucket),
		Namespace:      []string{indexNamespace},
		Name:           arns.index,
		Format:         s3tables.FormatLance,
		Metadata:       &s3tables.TableMetadata{FullMetadata: metadataJSON},
	}, &resp, r)
	if err != nil {
		catalogErr(w, err)
		return
	}
	var nonFilterable []string
	if req.MetadataConfiguration != nil {
		nonFilterable = req.MetadataConfiguration.NonFilterableMetadataKeys
	}
	_, err = s.vectors.CreateIndex(r.Context(), &vector_pb.CreateIndexRequest{
		Table:                     tableID(arns.bucket, arns.index),
		Dimension:                 uint32(req.Dimension),
		DistanceMetric:            metric,
		NonFilterableMetadataKeys: nonFilterable,
	})
	if err != nil {
		// Roll the index record back so a half-created index does not linger.
		var delResp map[string]interface{}
		_ = s.catalog(r.Context(), "DeleteTable", &s3tables.DeleteTableRequest{
			TableBucketARN: s.s3tablesArn(arns.bucket),
			Namespace:      []string{indexNamespace},
			Name:           arns.index,
		}, &delResp, r)
		writeError(w, http.StatusInternalServerError, "InternalFailure", err.Error())
		return
	}
	writeJSON(w, http.StatusOK, &CreateIndexResponse{})
}

func indexFromRecord(s *Server, bucket, name string, m map[string]interface{}) *Index {
	idx := &Index{
		IndexArn:         s.indexArn(bucket, name),
		IndexName:        name,
		VectorBucketName: bucket,
		DataType:         "float32",
	}
	if created, ok := m["createdAt"].(string); ok {
		idx.CreationTime, _ = time.Parse(time.RFC3339, created)
	}
	if meta, ok := m["metadata"].(map[string]interface{}); ok {
		if fm, ok := meta["fullMetadata"].(map[string]interface{}); ok {
			var dim float64
			fmt.Sscan(fmt.Sprint(fm["dimension"]), &dim)
			idx.Dimension = int32(dim)
			idx.DistanceMetric = fmt.Sprint(fm["distanceMetric"])
			if keys, ok := fm["nonFilterableMetadataKeys"].([]interface{}); ok {
				var list []string
				for _, k := range keys {
					list = append(list, fmt.Sprint(k))
				}
				idx.MetadataConfiguration = &MetadataConfiguration{NonFilterableMetadataKeys: list}
			}
		}
	}
	if idx.DistanceMetric == "" || idx.DistanceMetric == "<nil>" {
		idx.DistanceMetric = "euclidean"
	}
	return idx
}

func (s *Server) handleGetIndex(w http.ResponseWriter, r *http.Request) {
	var req GetIndexRequest
	if err := readJSON(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, "ValidationException", err.Error())
		return
	}
	arns, err := resolveIndexARNs(req.VectorBucketName, req.VectorBucketArn, req.IndexName, req.IndexArn)
	if err != nil {
		writeError(w, http.StatusBadRequest, "ValidationException", err.Error())
		return
	}
	var resp map[string]interface{}
	err = s.catalog(r.Context(), "GetTable", &s3tables.GetTableRequest{
		TableBucketARN: s.s3tablesArn(arns.bucket),
		Namespace:      []string{indexNamespace},
		Name:           arns.index,
	}, &resp, r)
	if err != nil {
		catalogErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, &GetIndexResponse{Index: indexFromRecord(s, arns.bucket, req.IndexName, resp)})
}

func (s *Server) handleListIndexes(w http.ResponseWriter, r *http.Request) {
	var req ListIndexesRequest
	if err := readJSON(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, "ValidationException", err.Error())
		return
	}
	var arns vectorBucketARNs
	if err := arnParts(&arns, req.VectorBucketName, req.VectorBucketArn); err != nil {
		writeError(w, http.StatusBadRequest, "ValidationException", err.Error())
		return
	}
	var resp struct {
		TableSummaries []struct {
			Name      string `json:"name"`
			CreatedAt string `json:"createdAt"`
		} `json:"tables"`
		ContinuationToken string `json:"continuationToken"`
	}
	err := s.catalog(r.Context(), "ListTables", &s3tables.ListTablesRequest{
		TableBucketARN:    s.s3tablesArn(arns.bucket),
		Namespace:         []string{indexNamespace},
		MaxTables:         req.MaxResults,
		ContinuationToken: req.NextToken,
		Prefix:            req.Prefix,
	}, &resp, r)
	if err != nil {
		catalogErr(w, err)
		return
	}
	out := &ListIndexesResponse{NextToken: resp.ContinuationToken}
	for _, t := range resp.TableSummaries {
		out.Indexes = append(out.Indexes, indexFromRecord(s, arns.bucket, t.Name, nil))
	}
	writeJSON(w, http.StatusOK, out)
}

func (s *Server) handleDeleteIndex(w http.ResponseWriter, r *http.Request) {
	var req DeleteIndexRequest
	if err := readJSON(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, "ValidationException", err.Error())
		return
	}
	arns, err := resolveIndexARNs(req.VectorBucketName, req.VectorBucketArn, req.IndexName, req.IndexArn)
	if err != nil {
		writeError(w, http.StatusBadRequest, "ValidationException", err.Error())
		return
	}
	_, _ = s.vectors.DropIndex(r.Context(), &vector_pb.DropIndexRequest{
		Table: tableID(arns.bucket, arns.index),
	})
	var resp map[string]interface{}
	err = s.catalog(r.Context(), "DeleteTable", &s3tables.DeleteTableRequest{
		TableBucketARN: s.s3tablesArn(arns.bucket),
		Namespace:      []string{indexNamespace},
		Name:           arns.index,
	}, &resp, r)
	if err != nil {
		catalogErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, &DeleteIndexResponse{})
}

func metadataStruct(m map[string]interface{}) (*structpb.Struct, error) {
	if m == nil {
		return nil, nil
	}
	return structpb.NewStruct(m)
}

func structToMap(s *structpb.Struct) map[string]interface{} {
	if s == nil {
		return nil
	}
	return s.AsMap()
}

func (s *Server) handlePutVectors(w http.ResponseWriter, r *http.Request) {
	var req PutVectorsRequest
	if err := readJSON(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, "ValidationException", err.Error())
		return
	}
	arns, err := resolveIndexARNs(req.VectorBucketName, req.VectorBucketArn, req.IndexName, req.IndexArn)
	if err != nil {
		writeError(w, http.StatusBadRequest, "ValidationException", err.Error())
		return
	}
	if _, err := s.indexRecord(r.Context(), r, arns.bucket, arns.index); err != nil {
		catalogErr(w, err)
		return
	}
	vectors := make([]*vector_pb.Vector, 0, len(req.Vectors))
	for _, v := range req.Vectors {
		if v == nil {
			writeError(w, http.StatusBadRequest, "ValidationException", "vectors contains a null element")
			return
		}
		metadata, err := metadataStruct(v.Metadata)
		if err != nil {
			writeError(w, http.StatusBadRequest, "ValidationException", err.Error())
			return
		}
		var data []float32
		if v.Data != nil {
			data = v.Data.Float32
		}
		vectors = append(vectors, &vector_pb.Vector{Key: v.Key, Data: data, Metadata: metadata})
	}
	_, err = s.vectors.PutVectors(r.Context(), &vector_pb.PutVectorsRequest{
		Table:   tableID(arns.bucket, arns.index),
		Vectors: vectors,
	})
	if err != nil {
		writeError(w, http.StatusInternalServerError, "InternalFailure", err.Error())
		return
	}
	writeJSON(w, http.StatusOK, &PutVectorsResponse{})
}

func toOutputVectors(vectors []*vector_pb.Vector, returnData, returnMetadata bool) []*OutputVector {
	out := make([]*OutputVector, 0, len(vectors))
	for _, v := range vectors {
		o := &OutputVector{Key: v.Key}
		if returnData {
			o.Data = &VectorData{Float32: v.Data}
		}
		if returnMetadata {
			o.Metadata = structToMap(v.Metadata)
		}
		out = append(out, o)
	}
	return out
}

func (s *Server) handleGetVectors(w http.ResponseWriter, r *http.Request) {
	var req GetVectorsRequest
	if err := readJSON(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, "ValidationException", err.Error())
		return
	}
	arns, err := resolveIndexARNs(req.VectorBucketName, req.VectorBucketArn, req.IndexName, req.IndexArn)
	if err != nil {
		writeError(w, http.StatusBadRequest, "ValidationException", err.Error())
		return
	}
	if _, err := s.indexRecord(r.Context(), r, arns.bucket, arns.index); err != nil {
		catalogErr(w, err)
		return
	}
	resp, err := s.vectors.GetVectors(r.Context(), &vector_pb.GetVectorsRequest{
		Table:          tableID(arns.bucket, arns.index),
		Keys:           req.Keys,
		ReturnData:     req.ReturnData,
		ReturnMetadata: req.ReturnMetadata,
	})
	if err != nil {
		writeError(w, http.StatusInternalServerError, "InternalFailure", err.Error())
		return
	}
	writeJSON(w, http.StatusOK, &GetVectorsResponse{
		Vectors: toOutputVectors(resp.Vectors, req.ReturnData, req.ReturnMetadata),
	})
}

func (s *Server) handleListVectors(w http.ResponseWriter, r *http.Request) {
	var req ListVectorsRequest
	if err := readJSON(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, "ValidationException", err.Error())
		return
	}
	arns, err := resolveIndexARNs(req.VectorBucketName, req.VectorBucketArn, req.IndexName, req.IndexArn)
	if err != nil {
		writeError(w, http.StatusBadRequest, "ValidationException", err.Error())
		return
	}
	if _, err := s.indexRecord(r.Context(), r, arns.bucket, arns.index); err != nil {
		catalogErr(w, err)
		return
	}
	if req.MaxResults < 0 {
		writeError(w, http.StatusBadRequest, "ValidationException", "maxResults must be non-negative")
		return
	}
	// AWS exposes a static split: segmentIndex/segmentCount divides the full
	// listing. The engine service does not know AWS segments, so page through
	// the index here and keep only the caller's share — O(index) per call,
	// acceptable for the MVP. The engine's next_token is a row offset, so the
	// caller's token is the offset to resume scanning from.
	if req.SegmentCount > 0 {
		if req.SegmentIndex < 0 || req.SegmentIndex >= req.SegmentCount {
			writeError(w, http.StatusBadRequest, "ValidationException", "segmentIndex must be in [0, segmentCount)")
			return
		}
		var offset uint64
		if req.NextToken != "" {
			var err error
			if offset, err = strconv.ParseUint(req.NextToken, 10, 64); err != nil {
				writeError(w, http.StatusBadRequest, "ValidationException", "invalid nextToken")
				return
			}
		}
		out, nextToken, err := s.listSegmentVectors(r.Context(), tableID(arns.bucket, arns.index),
			offset, req.MaxResults, uint64(req.SegmentCount), uint64(req.SegmentIndex), req.ReturnData, req.ReturnMetadata)
		if err != nil {
			writeError(w, http.StatusInternalServerError, "InternalFailure", err.Error())
			return
		}
		writeJSON(w, http.StatusOK, &ListVectorsResponse{
			NextToken: nextToken,
			Vectors:   toOutputVectors(out, req.ReturnData, req.ReturnMetadata),
		})
		return
	}
	resp, err := s.vectors.ListVectors(r.Context(), &vector_pb.ListVectorsRequest{
		Table:          tableID(arns.bucket, arns.index),
		MaxResults:     uint32(req.MaxResults),
		NextToken:      req.NextToken,
		ReturnData:     req.ReturnData,
		ReturnMetadata: req.ReturnMetadata,
	})
	if err != nil {
		writeError(w, http.StatusInternalServerError, "InternalFailure", err.Error())
		return
	}
	writeJSON(w, http.StatusOK, &ListVectorsResponse{
		NextToken: resp.NextToken,
		Vectors:   toOutputVectors(resp.Vectors, req.ReturnData, req.ReturnMetadata),
	})
}

// Mirrors the engine's page sizing so a segmented listing pages like a plain
// one does.
const (
	vectorsDefaultPage = 500
	vectorsMaxPage     = 1000
)

// listSegmentVectors scans the index from offset, keeping the rows that fall
// in the caller's segment until limit are collected or the index runs out.
// The engine's next_token is a row offset, so the returned token is the offset
// scanning resumes from.
func (s *Server) listSegmentVectors(ctx context.Context, table *vector_pb.Table, offset uint64, maxResults int, segmentCount, segmentIndex uint64, returnData, returnMetadata bool) ([]*vector_pb.Vector, string, error) {
	limit := maxResults
	if limit <= 0 {
		limit = vectorsDefaultPage
	} else if limit > vectorsMaxPage {
		limit = vectorsMaxPage
	}
	var out []*vector_pb.Vector
	nextToken := ""
	token := strconv.FormatUint(offset, 10)
	for {
		page, err := s.vectors.ListVectors(ctx, &vector_pb.ListVectorsRequest{
			Table:          table,
			MaxResults:     vectorsMaxPage,
			NextToken:      token,
			ReturnData:     returnData,
			ReturnMetadata: returnMetadata,
		})
		if err != nil {
			return nil, "", err
		}
		base := offset
		offset += uint64(len(page.Vectors))
		full := false
		for j, v := range page.Vectors {
			if (base+uint64(j))%segmentCount == segmentIndex {
				out = append(out, v)
				if len(out) == limit {
					if j+1 < len(page.Vectors) || page.NextToken != "" {
						nextToken = strconv.FormatUint(base+uint64(j)+1, 10)
					}
					full = true
					break
				}
			}
		}
		if full || len(page.Vectors) == 0 || page.NextToken == "" {
			break
		}
		token = page.NextToken
	}
	return out, nextToken, nil
}

func (s *Server) handleDeleteVectors(w http.ResponseWriter, r *http.Request) {
	var req DeleteVectorsRequest
	if err := readJSON(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, "ValidationException", err.Error())
		return
	}
	arns, err := resolveIndexARNs(req.VectorBucketName, req.VectorBucketArn, req.IndexName, req.IndexArn)
	if err != nil {
		writeError(w, http.StatusBadRequest, "ValidationException", err.Error())
		return
	}
	if _, err := s.indexRecord(r.Context(), r, arns.bucket, arns.index); err != nil {
		catalogErr(w, err)
		return
	}
	_, err = s.vectors.DeleteVectors(r.Context(), &vector_pb.DeleteVectorsRequest{
		Table: tableID(arns.bucket, arns.index),
		Keys:  req.Keys,
	})
	if err != nil {
		writeError(w, http.StatusInternalServerError, "InternalFailure", err.Error())
		return
	}
	writeJSON(w, http.StatusOK, &DeleteVectorsResponse{})
}

// translateFilter renders an AWS S3 Vectors filter document as a Lance SQL
// predicate over the metadata JSON column. Only the documented operators are
// translated; anything else fails loudly instead of silently overmatching.
func translateFilter(filter map[string]interface{}) (string, error) {
	if len(filter) == 0 {
		return "", nil
	}
	return filterExpr(filter)
}

func filterExpr(filter map[string]interface{}) (string, error) {
	parts := make([]string, 0, len(filter))
	for key, cond := range filter {
		if strings.HasPrefix(key, "$") {
			sub, ok := cond.([]interface{})
			if !ok {
				return "", fmt.Errorf("%s expects a list", key)
			}
			exprs := make([]string, 0, len(sub))
			for _, item := range sub {
				m, ok := item.(map[string]interface{})
				if !ok {
					return "", fmt.Errorf("%s expects objects", key)
				}
				e, err := filterExpr(m)
				if err != nil {
					return "", err
				}
				exprs = append(exprs, e)
			}
			joiner := " AND "
			if key == "$or" {
				joiner = " OR "
			}
			if key != "$and" && key != "$or" {
				return "", fmt.Errorf("unsupported operator %s", key)
			}
			parts = append(parts, "("+strings.Join(exprs, joiner)+")")
			continue
		}
		clause, err := fieldClause(key, cond)
		if err != nil {
			return "", err
		}
		parts = append(parts, clause)
	}
	return strings.Join(parts, " AND "), nil
}

func fieldClause(field string, cond interface{}) (string, error) {
	col := fmt.Sprintf("json_get_string(`metadata`, '%s')", strings.ReplaceAll(field, "'", "''"))
	switch c := cond.(type) {
	case map[string]interface{}:
		parts := []string{}
		for op, operand := range c {
			switch op {
			case "$exists":
				if operand == true {
					parts = append(parts, fmt.Sprintf("json_exists(`metadata`, '%s')", strings.ReplaceAll(field, "'", "''")))
				} else {
					parts = append(parts, fmt.Sprintf("NOT json_exists(`metadata`, '%s')", strings.ReplaceAll(field, "'", "''")))
				}
			case "$eq", "$ne":
				lit, err := sqlLiteral(operand)
				if err != nil {
					return "", err
				}
				sym := "="
				if op == "$ne" {
					sym = "<>"
				}
				parts = append(parts, fmt.Sprintf("%s %s %s", col, sym, lit))
			case "$in", "$nin":
				list, ok := operand.([]interface{})
				if !ok {
					return "", fmt.Errorf("%s expects a list", op)
				}
				vals := make([]string, 0, len(list))
				for _, item := range list {
					lit, err := sqlLiteral(item)
					if err != nil {
						return "", err
					}
					vals = append(vals, lit)
				}
				sym := "IN"
				if op == "$nin" {
					sym = "NOT IN"
				}
				parts = append(parts, fmt.Sprintf("%s %s (%s)", col, sym, strings.Join(vals, ", ")))
			case "$lt", "$lte", "$gt", "$gte":
				lit, err := sqlLiteral(operand)
				if err != nil {
					return "", err
				}
				sym := map[string]string{"$lt": "<", "$lte": "<=", "$gt": ">", "$gte": ">="}[op]
				numCol := fmt.Sprintf("json_get_float(`metadata`, '%s')", strings.ReplaceAll(field, "'", "''"))
				parts = append(parts, fmt.Sprintf("%s %s %s", numCol, sym, lit))
			default:
				return "", fmt.Errorf("unsupported operator %s", op)
			}
		}
		return strings.Join(parts, " AND "), nil
	default:
		lit, err := sqlLiteral(cond)
		if err != nil {
			return "", err
		}
		return fmt.Sprintf("%s = %s", col, lit), nil
	}
}

func sqlLiteral(v interface{}) (string, error) {
	switch t := v.(type) {
	case string:
		return "'" + strings.ReplaceAll(t, "'", "''") + "'", nil
	case bool:
		if t {
			return "true", nil
		}
		return "false", nil
	case float64:
		return fmt.Sprintf("%v", t), nil
	}
	return "", fmt.Errorf("unsupported filter operand %v", v)
}

func (s *Server) handleQueryVectors(w http.ResponseWriter, r *http.Request) {
	var req QueryVectorsRequest
	if err := readJSON(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, "ValidationException", err.Error())
		return
	}
	arns, err := resolveIndexARNs(req.VectorBucketName, req.VectorBucketArn, req.IndexName, req.IndexArn)
	if err != nil {
		writeError(w, http.StatusBadRequest, "ValidationException", err.Error())
		return
	}
	var queryVector []float32
	if req.QueryVector != nil {
		queryVector = req.QueryVector.Float32
	}
	if len(queryVector) == 0 {
		writeError(w, http.StatusBadRequest, "ValidationException", "queryVector is required")
		return
	}
	record, err := s.indexRecord(r.Context(), r, arns.bucket, arns.index)
	if err != nil {
		catalogErr(w, err)
		return
	}
	metric := indexFromRecord(s, arns.bucket, arns.index, record).DistanceMetric
	filter, err := translateFilter(req.Filter)
	if err != nil {
		writeError(w, http.StatusBadRequest, "ValidationException", err.Error())
		return
	}
	if req.TopK < 0 {
		writeError(w, http.StatusBadRequest, "ValidationException", "topK must be non-negative")
		return
	}
	var filterJSON []byte
	if len(req.Filter) > 0 {
		filterJSON, _ = json.Marshal(req.Filter)
	}
	resp, err := s.vectors.QueryVectors(r.Context(), &vector_pb.QueryVectorsRequest{
		Table:          tableID(arns.bucket, arns.index),
		QueryVector:    queryVector,
		TopK:           uint32(req.TopK),
		Filter:         filter,
		FilterJson:     string(filterJSON),
		ReturnMetadata: req.ReturnMetadata,
		ReturnDistance: req.ReturnDistance,
	})
	if err != nil {
		writeError(w, http.StatusInternalServerError, "InternalFailure", err.Error())
		return
	}
	out := &QueryVectorsResponse{DistanceMetric: metric}
	for _, v := range resp.Vectors {
		o := &QueryOutputVector{Key: v.Key}
		if req.ReturnDistance {
			o.Distance = &v.Distance
		}
		if req.ReturnMetadata {
			o.Metadata = structToMap(v.Metadata)
		}
		out.Vectors = append(out.Vectors, o)
	}
	writeJSON(w, http.StatusOK, out)
}
