package dash

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/lance"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3tables"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3vectors"
)

type S3VectorsBucketsData struct {
	Username     string                   `json:"username"`
	Buckets      []S3VectorsBucketSummary `json:"buckets"`
	Operations   VectorOperationsData     `json:"operations"`
	TotalBuckets int                      `json:"total_buckets"`
	Truncated    bool                     `json:"truncated,omitempty"`
	LastUpdated  time.Time                `json:"last_updated"`
}

type S3VectorsBucketSummary struct {
	Name      string    `json:"name"`
	ARN       string    `json:"arn"`
	CreatedAt time.Time `json:"createdAt"`
	// Encryption is the bucket's sseType, or empty when it declares none.
	Encryption string `json:"encryption,omitempty"`
}

type S3VectorsBucketData struct {
	Username     string                 `json:"username"`
	Bucket       S3VectorsBucketSummary `json:"bucket"`
	Policy       string                 `json:"policy,omitempty"`
	Indexes      []*s3vectors.Index     `json:"indexes"`
	Operations   VectorOperationsData   `json:"operations"`
	TotalIndexes int                    `json:"total_indexes"`
	Truncated    bool                   `json:"truncated,omitempty"`
	LastUpdated  time.Time              `json:"last_updated"`
}

// The S3 Vectors operations reach the filer through the admin server's own
// filer connection.
type s3VectorsFilerClient struct{ s *AdminServer }

func (c s3VectorsFilerClient) WithFilerClient(_ bool, fn func(client filer_pb.SeaweedFilerClient) error) error {
	return c.s.WithFilerClient(fn)
}

func newS3VectorsServer(s *AdminServer) *s3vectors.Server {
	client := s3VectorsFilerClient{s}
	backend := s3vectors.NewVectorDiscovery(s.masterClient.ListCachedMasters, s.grpcDialOption, s.filerGroup)
	server := s3vectors.NewServer(client, nil, backend)
	catalog := lance.NewServer(client, nil)
	server.SetDescribe(func(ctx context.Context, id []string) (string, map[string]string, error) {
		request := (&http.Request{Method: http.MethodPost, Header: make(http.Header)}).WithContext(ctx)
		return catalog.DescribeTableInternal(request, id)
	})
	server.SetRegion(s3tables.DefaultRegion)
	server.SetAccountID(s3tables.DefaultAccountID)
	return server
}

func (s *AdminServer) executeS3VectorsOperation(ctx context.Context, operation string, req, resp interface{}) error {
	return s.s3VectorsServer.Execute(ctx, operation, req, resp, s3_constants.AccountAdminId)
}

const s3VectorsAdminPage = 500

func (s *AdminServer) s3VectorsBucketSummary(ctx context.Context, name string) (S3VectorsBucketSummary, error) {
	var resp s3vectors.GetVectorBucketResponse
	if err := s.executeS3VectorsOperation(ctx, "GetVectorBucket", &s3vectors.GetVectorBucketRequest{VectorBucketName: name}, &resp); err != nil {
		return S3VectorsBucketSummary{}, err
	}
	b := resp.VectorBucket
	summary := S3VectorsBucketSummary{Name: b.VectorBucketName, ARN: b.VectorBucketArn, CreatedAt: b.CreationTime}
	if sse, ok := b.EncryptionConfiguration["sseType"].(string); ok {
		summary.Encryption = sse
	}
	return summary, nil
}

func (s *AdminServer) GetS3VectorsBucketsData(ctx context.Context) (S3VectorsBucketsData, error) {
	data := S3VectorsBucketsData{Buckets: []S3VectorsBucketSummary{}}
	page := s3VectorsAdminPage
	req := &s3vectors.ListVectorBucketsRequest{MaxResults: &page}
	for {
		var resp s3vectors.ListVectorBucketsResponse
		if err := s.executeS3VectorsOperation(ctx, "ListVectorBuckets", req, &resp); err != nil {
			return S3VectorsBucketsData{}, err
		}
		for _, b := range resp.VectorBuckets {
			if len(data.Buckets) == s3TablesAdminListLimit {
				data.Truncated = true
				break
			}
			summary, err := s.s3VectorsBucketSummary(ctx, b.VectorBucketName)
			if isS3VectorsNotFound(err) {
				continue
			}
			if err != nil {
				return S3VectorsBucketsData{}, err
			}
			data.Buckets = append(data.Buckets, summary)
		}
		if data.Truncated || resp.NextToken == "" {
			break
		}
		req.NextToken = resp.NextToken
	}
	data.Operations = s.GetVectorOperationsData(ctx)
	data.TotalBuckets = len(data.Buckets)
	data.LastUpdated = time.Now()
	return data, nil
}

func (s *AdminServer) GetS3VectorsBucketData(ctx context.Context, bucket string) (S3VectorsBucketData, error) {
	summary, err := s.s3VectorsBucketSummary(ctx, bucket)
	if err != nil {
		return S3VectorsBucketData{}, err
	}
	data := S3VectorsBucketData{Bucket: summary, Indexes: []*s3vectors.Index{}, LastUpdated: time.Now()}
	var policy s3vectors.GetVectorBucketPolicyResponse
	switch err := s.executeS3VectorsOperation(ctx, "GetVectorBucketPolicy", &s3vectors.GetVectorBucketPolicyRequest{VectorBucketName: bucket}, &policy); {
	case err == nil:
		data.Policy = policy.Policy
	case !isS3VectorsNotFound(err):
		return S3VectorsBucketData{}, err
	}
	page := s3VectorsAdminPage
	req := &s3vectors.ListIndexesRequest{VectorBucketName: bucket, MaxResults: &page}
	for {
		var resp s3vectors.ListIndexesResponse
		if err := s.executeS3VectorsOperation(ctx, "ListIndexes", req, &resp); err != nil {
			return S3VectorsBucketData{}, err
		}
		for _, summary := range resp.Indexes {
			if len(data.Indexes) == s3TablesAdminListLimit {
				data.Truncated = true
				break
			}
			var index s3vectors.GetIndexResponse
			err := s.executeS3VectorsOperation(ctx, "GetIndex", &s3vectors.GetIndexRequest{VectorBucketName: bucket, IndexName: summary.IndexName}, &index)
			if isS3VectorsNotFound(err) {
				continue
			}
			if err != nil {
				return S3VectorsBucketData{}, err
			}
			data.Indexes = append(data.Indexes, index.Index)
		}
		if data.Truncated || resp.NextToken == "" {
			break
		}
		req.NextToken = resp.NextToken
	}
	data.Operations = s.GetVectorOperationsData(ctx)
	data.TotalIndexes = len(data.Indexes)
	return data, nil
}

func (s *AdminServer) ListS3VectorsBucketsAPI(w http.ResponseWriter, r *http.Request) {
	data, err := s.GetS3VectorsBucketsData(r.Context())
	if err != nil {
		writeS3VectorsError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, data)
}

func (s *AdminServer) CreateS3VectorsBucket(w http.ResponseWriter, r *http.Request) {
	if !requireSessionCSRFToken(w, r) {
		return
	}
	var req struct {
		Name                    string                 `json:"name"`
		EncryptionConfiguration map[string]interface{} `json:"encryptionConfiguration,omitempty"`
		Tags                    map[string]string      `json:"tags,omitempty"`
	}
	if err := decodeJSONBody(newJSONMaxReader(w, r), &req); err != nil {
		writeJSONError(w, http.StatusBadRequest, "Invalid request: "+err.Error())
		return
	}
	if req.EncryptionConfiguration["sseType"] == "AES256" && !s.vectorEncryptionConfigured(r.Context()) {
		writeJSONError(w, http.StatusBadRequest, "Cannot verify SSE-S3 support on the S3 gateway; configure s3.public_endpoint and an SSE-S3 key on the gateway and vector engines")
		return
	}
	var resp s3vectors.CreateVectorBucketResponse
	if err := s.executeS3VectorsOperation(r.Context(), "CreateVectorBucket", &s3vectors.CreateVectorBucketRequest{VectorBucketName: req.Name, EncryptionConfiguration: req.EncryptionConfiguration, Tags: req.Tags}, &resp); err != nil {
		writeS3VectorsError(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, map[string]interface{}{"arn": resp.VectorBucketArn})
}

func (s *AdminServer) DeleteS3VectorsBucket(w http.ResponseWriter, r *http.Request) {
	if !requireSessionCSRFToken(w, r) {
		return
	}
	bucket := r.URL.Query().Get("bucket")
	if bucket == "" {
		writeJSONError(w, http.StatusBadRequest, "bucket query parameter is required")
		return
	}
	if err := s.executeS3VectorsOperation(r.Context(), "DeleteVectorBucket", &s3vectors.DeleteVectorBucketRequest{VectorBucketName: bucket}, nil); err != nil {
		writeS3VectorsError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{"message": "Vector bucket deleted"})
}

func (s *AdminServer) GetS3VectorsBucketAPI(w http.ResponseWriter, r *http.Request) {
	bucket := r.URL.Query().Get("bucket")
	if bucket == "" {
		writeJSONError(w, http.StatusBadRequest, "bucket query parameter is required")
		return
	}
	data, err := s.GetS3VectorsBucketData(r.Context(), bucket)
	if err != nil {
		writeS3VectorsError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, data)
}

func (s *AdminServer) DeleteS3VectorsIndex(w http.ResponseWriter, r *http.Request) {
	if !requireSessionCSRFToken(w, r) {
		return
	}
	bucket, index := r.URL.Query().Get("bucket"), r.URL.Query().Get("index")
	if bucket == "" || index == "" {
		writeJSONError(w, http.StatusBadRequest, "bucket and index query parameters are required")
		return
	}
	if err := s.executeS3VectorsOperation(r.Context(), "DeleteIndex", &s3vectors.DeleteIndexRequest{VectorBucketName: bucket, IndexName: index}, nil); err != nil {
		writeS3VectorsError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{"message": "Index deleted"})
}

func isS3VectorsNotFound(err error) bool {
	var e *s3vectors.Error
	return errors.As(err, &e) && e.Code == "NotFoundException"
}

func writeS3VectorsError(w http.ResponseWriter, err error) {
	var e *s3vectors.Error
	if !errors.As(err, &e) {
		writeJSONError(w, http.StatusInternalServerError, err.Error())
		return
	}
	status := http.StatusInternalServerError
	switch e.Code {
	case "ValidationException":
		status = http.StatusBadRequest
	case "AccessDeniedException":
		status = http.StatusForbidden
	case "NotFoundException":
		status = http.StatusNotFound
	case "ConflictException":
		status = http.StatusConflict
	case "ServiceUnavailableException":
		status = http.StatusServiceUnavailable
	}
	writeJSONError(w, status, e.Message)
}

func (s *AdminServer) ExecuteS3VectorsOperationAPI(w http.ResponseWriter, r *http.Request) {
	if !requireSessionCSRFToken(w, r) {
		return
	}
	var req struct {
		Operation string          `json:"operation"`
		Request   json.RawMessage `json:"request"`
	}
	if err := decodeJSONBody(newJSONMaxReader(w, r), &req); err != nil {
		writeJSONError(w, http.StatusBadRequest, "Invalid request: "+err.Error())
		return
	}
	switch req.Operation {
	case "CreateIndex", "PutVectorBucketPolicy", "DeleteVectorBucketPolicy", "TagResource", "UntagResource", "ListTagsForResource", "PutVectors", "GetVectors", "ListVectors", "DeleteVectors", "QueryVectors":
	default:
		writeJSONError(w, http.StatusBadRequest, "Unsupported vector operation")
		return
	}
	switch req.Operation {
	case "QueryVectors", "GetVectors", "ListVectors", "ListTagsForResource":
	default:
		if role := RoleFromContext(r.Context()); role != "admin" && (role != "" || UsernameFromContext(r.Context()) != "") {
			writeJSONError(w, http.StatusForbidden, "This operation requires admin access")
			return
		}
	}
	var resp json.RawMessage
	if err := s.executeS3VectorsOperation(r.Context(), req.Operation, req.Request, &resp); err != nil {
		writeS3VectorsError(w, err)
		return
	}
	if len(resp) == 0 {
		resp = json.RawMessage(`{}`)
	}
	writeJSON(w, http.StatusOK, resp)
}
