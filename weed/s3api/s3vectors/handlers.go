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

	"github.com/google/uuid"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/vector_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3tables"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
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
	if v.bucket == "" {
		return fmt.Errorf("malformed ARN %q", arn)
	}
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
	if bucketArn != "" {
		if !strings.HasPrefix(bucketArn, "arn:") {
			return v, fmt.Errorf("vectorBucketArn must be an ARN, got %q", bucketArn)
		}
		if err := arnParts(&v, "", bucketArn); err != nil {
			return v, err
		}
	}
	if v.bucket == "" {
		v.bucket = bucketName
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

// internalCatalog runs catalog bookkeeping under the trusted identity. It is
// for reads and writes that accompany — never replace — the operation's own
// permission check: bucket format lookups, the implicit index namespace, and
// rollback of a record this request just created.
func (s *Server) internalCatalog(ctx context.Context, operation string, req, resp interface{}) error {
	return s.internal.Execute(ctx, s.filerClient, operation, req, resp, "")
}

func catalogErrCode(err error) string {
	var apiErr *s3tables.S3TablesError
	if errors.As(err, &apiErr) {
		return apiErr.Type
	}
	return ""
}

func catalogErr(w http.ResponseWriter, err error) {
	var apiErr *s3tables.S3TablesError
	if !errors.As(err, &apiErr) {
		writeError(w, http.StatusInternalServerError, "InternalFailure", "internal error")
		return
	}
	switch {
	case apiErr.Type == s3tables.ErrCodeAccessDenied:
		writeError(w, http.StatusForbidden, "AccessDeniedException", "access denied")
	case strings.HasPrefix(apiErr.Type, "NoSuch"):
		writeError(w, http.StatusNotFound, "NotFoundException", apiErr.Message)
	case strings.Contains(apiErr.Type, "AlreadyExists") || strings.Contains(apiErr.Type, "NotEmpty") ||
		apiErr.Type == s3tables.ErrCodeConflict:
		writeError(w, http.StatusConflict, "ConflictException", apiErr.Message)
	case apiErr.Type == s3tables.ErrCodeInternalError:
		writeError(w, http.StatusInternalServerError, "InternalFailure", apiErr.Message)
	default:
		writeError(w, http.StatusBadRequest, "ValidationException", apiErr.Message)
	}
}

// vectorErr maps a VectorService gRPC status onto the API's error shape.
// Engine messages reach clients only for codes that describe the request
// itself; transport and internal details stay in the server log.
func vectorErr(w http.ResponseWriter, err error) {
	msg := status.Convert(err).Message()
	switch status.Code(err) {
	case codes.NotFound:
		writeError(w, http.StatusNotFound, "NotFoundException", msg)
	case codes.InvalidArgument, codes.FailedPrecondition, codes.OutOfRange:
		writeError(w, http.StatusBadRequest, "ValidationException", msg)
	case codes.PermissionDenied, codes.Unauthenticated:
		writeError(w, http.StatusForbidden, "AccessDeniedException", "access denied")
	case codes.Unavailable:
		writeError(w, http.StatusServiceUnavailable, "ServiceUnavailableException", "vector service unavailable")
	default:
		glog.V(1).Infof("S3 Vectors: engine error: %v", err)
		writeError(w, http.StatusInternalServerError, "InternalFailure", "internal error")
	}
}

// lanceBucket reports whether bucket is a table bucket that can hold vector
// indexes: a declared Lance bucket, or one made before formats existed. An
// explicit ICEBERG bucket is not a vector bucket and reads as absent. The
// lookup is internal so the call needs no read grant — the requested
// operation's own permission still gates it.
func (s *Server) lanceBucket(ctx context.Context, bucket string) error {
	var resp struct {
		Format string `json:"format"`
	}
	err := s.internalCatalog(ctx, "GetTableBucket",
		&s3tables.GetTableBucketRequest{TableBucketARN: s.s3tablesArn(bucket)}, &resp)
	if err != nil {
		return err
	}
	if resp.Format != "" && resp.Format != s3tables.FormatLance {
		return &s3tables.S3TablesError{
			Type:    s3tables.ErrCodeNoSuchBucket,
			Message: fmt.Sprintf("vector bucket %s not found", bucket),
		}
	}
	return nil
}

// ensureIndexNamespace declares the implicit index namespace when absent. The
// calls are internal: a caller's CreateTable grant is what authorizes the
// index, so it must not also need namespace reads or writes.
func (s *Server) ensureIndexNamespace(ctx context.Context, bucket string) error {
	var resp map[string]interface{}
	err := s.internalCatalog(ctx, "GetNamespace", &s3tables.GetNamespaceRequest{
		TableBucketARN: s.s3tablesArn(bucket),
		Namespace:      []string{indexNamespace},
	}, &resp)
	if err == nil {
		return nil
	}
	if code := catalogErrCode(err); code != s3tables.ErrCodeNoSuchNamespace && code != s3tables.ErrCodeNoSuchBucket {
		return err
	}
	err = s.internalCatalog(ctx, "CreateNamespace", &s3tables.CreateNamespaceRequest{
		TableBucketARN: s.s3tablesArn(bucket),
		Namespace:      []string{indexNamespace},
	}, &resp)
	// A concurrent CreateIndex may have won the create.
	if err != nil && catalogErrCode(err) != s3tables.ErrCodeNamespaceAlreadyExists {
		return err
	}
	return nil
}

// authorizeIndexWrite requires the update grant on the index's catalog table
// — the same check credential vending uses — so a read-only GetTable grant
// never reaches a mutation.
func (s *Server) authorizeIndexWrite(ctx context.Context, r *http.Request, bucket, index string) error {
	return s.tables.AuthorizeUpdateTable(ctx, s.filerClient, &s3tables.UpdateTableRequest{
		TableBucketARN: s.s3tablesArn(bucket),
		Namespace:      []string{indexNamespace},
		Name:           index,
	}, identityOf(r))
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
	// The catalog has no encryption boundary to honor the configuration
	// against; accepting it would silently promise semantics we do not keep.
	if len(req.EncryptionConfiguration) > 0 {
		writeError(w, http.StatusBadRequest, "ValidationException",
			"encryptionConfiguration is not supported")
		return
	}
	var resp map[string]interface{}
	err := s.catalog(r.Context(), "CreateTableBucket",
		&s3tables.CreateTableBucketRequest{Name: req.VectorBucketName, Format: s3tables.FormatLance}, &resp, r)
	if err != nil {
		catalogErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, &CreateVectorBucketResponse{VectorBucketArn: s.vectorBucketArn(req.VectorBucketName)})
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
	// An explicit ICEBERG bucket is not a vector bucket.
	if format, _ := resp["format"].(string); format != "" && format != s3tables.FormatLance {
		writeError(w, http.StatusNotFound, "NotFoundException",
			fmt.Sprintf("vector bucket %s not found", arns.bucket))
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
	// The catalog token continues the filtered page: ICEBERG buckets drop out
	// of the listing, Lance and format-less legacy buckets stay.
	out := &ListVectorBucketsResponse{NextToken: resp.ContinuationToken}
	for _, b := range resp.TableBuckets {
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
	// The format check needs no read grant — the delete below is the
	// authorization boundary — but an ICEBERG bucket is not a vector bucket.
	if err := s.lanceBucket(r.Context(), arns.bucket); err != nil {
		catalogErr(w, err)
		return
	}
	var resp map[string]interface{}
	err := s.catalog(r.Context(), "DeleteTableBucket",
		&s3tables.DeleteTableBucketRequest{TableBucketARN: s.s3tablesArn(arns.bucket)}, &resp, r)
	if err != nil && catalogErrCode(err) == s3tables.ErrCodeBucketNotEmpty {
		// The implicit index namespace counts as bucket content even when it
		// holds no indexes; drop it under the delete's own authorization and
		// retry once. A namespace still holding indexes keeps the conflict.
		var nsResp map[string]interface{}
		nsErr := s.internalCatalog(context.WithoutCancel(r.Context()), "DeleteNamespace",
			&s3tables.DeleteNamespaceRequest{
				TableBucketARN: s.s3tablesArn(arns.bucket),
				Namespace:      []string{indexNamespace},
			}, &nsResp)
		if nsErr == nil {
			err = s.catalog(r.Context(), "DeleteTableBucket",
				&s3tables.DeleteTableBucketRequest{TableBucketARN: s.s3tablesArn(arns.bucket)}, &resp, r)
		} else if code := catalogErrCode(nsErr); code != s3tables.ErrCodeNoSuchNamespace {
			glog.V(1).Infof("S3 Vectors: drop internal namespace in %s: %v", arns.bucket, nsErr)
		}
	}
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
	if err := s.lanceBucket(r.Context(), arns.bucket); err != nil {
		catalogErr(w, err)
		return
	}
	// The create grant is exercised before any write: a denied caller must not
	// leave even the implicit namespace behind. The namespace may not exist
	// yet, so the check tolerates its absence.
	if err := s.tables.AuthorizeCreateTable(r.Context(), s.filerClient, &s3tables.CreateTableRequest{
		TableBucketARN:        s.s3tablesArn(arns.bucket),
		Namespace:             []string{indexNamespace},
		Name:                  arns.index,
		AllowMissingNamespace: true,
	}, identityOf(r)); err != nil {
		catalogErr(w, err)
		return
	}
	// The namespace is implicit on AWS and internal bookkeeping here.
	if err := s.ensureIndexNamespace(r.Context(), arns.bucket); err != nil {
		catalogErr(w, err)
		return
	}
	// Only a caller who passed the create check may learn an index exists.
	var existing map[string]interface{}
	getErr := s.internalCatalog(r.Context(), "GetTable", &s3tables.GetTableRequest{
		TableBucketARN: s.s3tablesArn(arns.bucket),
		Namespace:      []string{indexNamespace},
		Name:           arns.index,
	}, &existing)
	switch {
	case getErr == nil:
		writeError(w, http.StatusConflict, "ConflictException",
			fmt.Sprintf("index %s already exists", arns.index))
		return
	case catalogErrCode(getErr) != s3tables.ErrCodeNoSuchTable:
		catalogErr(w, getErr)
		return
	}
	metadata := map[string]string{
		"dimension":      fmt.Sprint(req.Dimension),
		"distanceMetric": req.DistanceMetric,
		// The nonce marks the record this request created, so a rollback after
		// an engine failure can never erase a pre-existing index.
		"createNonce": uuid.NewString(),
	}
	var nonFilterable []string
	if req.MetadataConfiguration != nil {
		nonFilterable = req.MetadataConfiguration.NonFilterableMetadataKeys
		if keys, err := json.Marshal(nonFilterable); err == nil {
			metadata["nonFilterableMetadataKeys"] = string(keys)
		}
	}
	metadataJSON, _ := json.Marshal(metadata)
	err = s.catalog(r.Context(), "CreateTable", &s3tables.CreateTableRequest{
		TableBucketARN: s.s3tablesArn(arns.bucket),
		Namespace:      []string{indexNamespace},
		Name:           arns.index,
		Format:         s3tables.FormatLance,
		Metadata:       &s3tables.TableMetadata{FullMetadata: metadataJSON},
	}, nil, r)
	if err != nil {
		catalogErr(w, err)
		return
	}
	_, err = s.vectors.CreateIndex(r.Context(), &vector_pb.CreateIndexRequest{
		Table:                     tableID(arns.bucket, arns.index),
		Dimension:                 uint32(req.Dimension),
		DistanceMetric:            metric,
		NonFilterableMetadataKeys: nonFilterable,
	})
	if err != nil {
		// Roll the index record back so a half-created index does not linger.
		// The delete runs past a client disconnect and only removes the record
		// this request created: the nonce must match, and the versionToken
		// compare-and-swap keeps a racing create from being erased.
		ctx := context.WithoutCancel(r.Context())
		var rec map[string]interface{}
		if gerr := s.internalCatalog(ctx, "GetTable", &s3tables.GetTableRequest{
			TableBucketARN: s.s3tablesArn(arns.bucket),
			Namespace:      []string{indexNamespace},
			Name:           arns.index,
		}, &rec); gerr == nil && recordNonce(rec) == metadata["createNonce"] {
			var delResp map[string]interface{}
			if derr := s.internalCatalog(ctx, "DeleteTable", &s3tables.DeleteTableRequest{
				TableBucketARN: s.s3tablesArn(arns.bucket),
				Namespace:      []string{indexNamespace},
				Name:           arns.index,
				VersionToken:   recordVersionToken(rec),
			}, &delResp); derr != nil {
				glog.Errorf("S3 Vectors: rollback of catalog table %s/%s failed: %v", arns.bucket, arns.index, derr)
			}
		} else if gerr != nil {
			glog.Errorf("S3 Vectors: rollback lookup for %s/%s failed: %v", arns.bucket, arns.index, gerr)
		}
		vectorErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, &CreateIndexResponse{IndexArn: s.indexArn(arns.bucket, arns.index)})
}

// recordNonce and recordVersionToken read the catalog record's internal
// bookkeeping for the create-index rollback.
func recordNonce(record map[string]interface{}) string {
	if meta, ok := record["metadata"].(map[string]interface{}); ok {
		if fm, ok := meta["fullMetadata"].(map[string]interface{}); ok {
			return fmt.Sprint(fm["createNonce"])
		}
	}
	return ""
}

func recordVersionToken(record map[string]interface{}) string {
	token, _ := record["versionToken"].(string)
	return token
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
	writeJSON(w, http.StatusOK, &GetIndexResponse{Index: indexFromRecord(s, arns.bucket, arns.index, resp)})
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
			Format    string `json:"format"`
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
	// Summaries carry only what the catalog listing knows; a summary that
	// asserted a dimension or metric without a record lookup would lie.
	out := &ListIndexesResponse{NextToken: resp.ContinuationToken}
	for _, t := range resp.TableSummaries {
		// A non-Lance table in the index namespace is not an index.
		if t.Format != "" && t.Format != s3tables.FormatLance {
			continue
		}
		created, _ := time.Parse(time.RFC3339, t.CreatedAt)
		out.Indexes = append(out.Indexes, &IndexSummary{
			IndexName:        t.Name,
			IndexArn:         s.indexArn(arns.bucket, t.Name),
			VectorBucketName: arns.bucket,
			CreationTime:     created,
		})
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
	// Deleting the catalog record is the delete: the engine derives an index's
	// existence from its table, and DropIndex is unimplemented on the shared
	// service.
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
	// A read grant on the index does not carry a write: mutations require the
	// catalog's update grant, the same check credential vending applies.
	if err := s.authorizeIndexWrite(r.Context(), r, arns.bucket, arns.index); err != nil {
		catalogErr(w, err)
		return
	}
	vectors := make([]*vector_pb.Vector, 0, len(req.Vectors))
	for _, v := range req.Vectors {
		if v == nil {
			writeError(w, http.StatusBadRequest, "ValidationException", "vectors contains a null element")
			return
		}
		if v.Key == "" {
			writeError(w, http.StatusBadRequest, "ValidationException", "vector key must not be empty")
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
		vectorErr(w, err)
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
		vectorErr(w, err)
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
			vectorErr(w, err)
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
		vectorErr(w, err)
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
	if err := s.authorizeIndexWrite(r.Context(), r, arns.bucket, arns.index); err != nil {
		catalogErr(w, err)
		return
	}
	_, err = s.vectors.DeleteVectors(r.Context(), &vector_pb.DeleteVectorsRequest{
		Table: tableID(arns.bucket, arns.index),
		Keys:  req.Keys,
	})
	if err != nil {
		vectorErr(w, err)
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

// fieldAccess renders the JSONB extractor for the operand's type. The second
// argument of the json_get_* family is a plain field name, not a JSONPath —
// only json_exists parses JSONPath.
func fieldAccess(field string, operand interface{}) (string, error) {
	fn := "json_get_string"
	switch operand.(type) {
	case float64:
		fn = "json_get_float"
	case bool:
		fn = "json_get_bool"
	case string:
	default:
		return "", fmt.Errorf("unsupported filter operand %v", operand)
	}
	return fmt.Sprintf("%s(`metadata`, '%s')", fn, sqlEscape(field)), nil
}

// existsPath renders the field as a JSONPath for json_exists: bracket notation
// keeps dots, spaces, and other specials inside the key literal.
func existsPath(field string) string {
	field = strings.ReplaceAll(field, `\`, `\\`)
	field = strings.ReplaceAll(field, `"`, `\"`)
	return `$["` + field + `"]`
}

func sqlEscape(s string) string {
	return strings.ReplaceAll(s, "'", "''")
}

func fieldClause(field string, cond interface{}) (string, error) {
	switch c := cond.(type) {
	case map[string]interface{}:
		parts := []string{}
		for op, operand := range c {
			switch op {
			case "$exists":
				if b, ok := operand.(bool); ok {
					expr := fmt.Sprintf("json_exists(`metadata`, '%s')", sqlEscape(existsPath(field)))
					if !b {
						expr = "NOT " + expr
					}
					parts = append(parts, expr)
				} else {
					return "", fmt.Errorf("$exists expects a boolean")
				}
			case "$eq", "$ne":
				col, err := fieldAccess(field, operand)
				if err != nil {
					return "", err
				}
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
				if len(list) == 0 {
					if op == "$in" {
						parts = append(parts, "false")
					} else {
						parts = append(parts, "true")
					}
					continue
				}
				// Members may mix types; each extractor only sees its own.
				var groups []string
				for _, item := range list {
					col, err := fieldAccess(field, item)
					if err != nil {
						return "", err
					}
					lit, err := sqlLiteral(item)
					if err != nil {
						return "", err
					}
					groups = append(groups, fmt.Sprintf("%s in (%s)", col, lit))
				}
				expr := "(" + strings.Join(groups, " OR ") + ")"
				if op == "$nin" {
					expr = "NOT " + expr
				}
				parts = append(parts, expr)
			case "$lt", "$lte", "$gt", "$gte":
				if _, ok := operand.(float64); !ok {
					return "", fmt.Errorf("%s expects a number", op)
				}
				lit, err := sqlLiteral(operand)
				if err != nil {
					return "", err
				}
				sym := map[string]string{"$lt": "<", "$lte": "<=", "$gt": ">", "$gte": ">="}[op]
				col := fmt.Sprintf("json_get_float(`metadata`, '%s')", sqlEscape(field))
				parts = append(parts, fmt.Sprintf("%s %s %s", col, sym, lit))
			default:
				return "", fmt.Errorf("unsupported operator %s", op)
			}
		}
		return strings.Join(parts, " AND "), nil
	default:
		col, err := fieldAccess(field, cond)
		if err != nil {
			return "", err
		}
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
	if req.TopK <= 0 {
		writeError(w, http.StatusBadRequest, "ValidationException", "topK must be positive")
		return
	}
	resp, err := s.vectors.QueryVectors(r.Context(), &vector_pb.QueryVectorsRequest{
		Table:          tableID(arns.bucket, arns.index),
		QueryVector:    queryVector,
		TopK:           uint32(req.TopK),
		Filter:         filter,
		ReturnMetadata: req.ReturnMetadata,
		ReturnDistance: req.ReturnDistance,
	})
	if err != nil {
		vectorErr(w, err)
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
