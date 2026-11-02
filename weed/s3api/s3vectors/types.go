package s3vectors

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
)

func contextWithIdentity(ctx context.Context, identity string) context.Context {
	return s3_constants.SetIdentityNameInContext(ctx, identity)
}

func identityOf(r *http.Request) string {
	return s3_constants.GetIdentityNameFromContext(r)
}

func writeJSON(w http.ResponseWriter, status int, payload interface{}) {
	w.Header().Set("Content-Type", "application/x-amz-json-1.1")
	w.WriteHeader(status)
	if payload != nil {
		_ = json.NewEncoder(w).Encode(payload)
	}
}

func writeError(w http.ResponseWriter, status int, code, message string) {
	writeJSON(w, status, map[string]string{
		"__type":    code,
		"code":      code,
		"message":   message,
		"requestId": "local",
	})
}

const maxRequestBodyBytes = s3_constants.S3VectorsMaxRequestBodyBytes

var errRequestBodyTooLarge = fmt.Errorf("request body exceeds %d MiB", maxRequestBodyBytes>>20)

func readJSON(r *http.Request, req interface{}) error {
	body, err := io.ReadAll(io.LimitReader(r.Body, maxRequestBodyBytes+1))
	if err != nil {
		return err
	}
	if len(body) > maxRequestBodyBytes {
		return errRequestBodyTooLarge
	}
	return json.NewDecoder(bytes.NewReader(body)).Decode(req)
}

// Request/response shapes for the AWS S3 Vectors API (awsJson1_1).

type vectorBucketARNs struct {
	bucket string
	index  string
}

type CreateVectorBucketRequest struct {
	VectorBucketName        string                 `json:"vectorBucketName"`
	EncryptionConfiguration map[string]interface{} `json:"encryptionConfiguration,omitempty"`
	MetadataResourceTags    map[string]string      `json:"-"`
}

type CreateVectorBucketResponse struct {
	VectorBucketArn string `json:"vectorBucketArn"`
}

type GetVectorBucketRequest struct {
	VectorBucketName string `json:"vectorBucketName"`
	VectorBucketArn  string `json:"vectorBucketArn"`
}

type GetVectorBucketResponse struct {
	VectorBucket *VectorBucket `json:"vectorBucket"`
}

type VectorBucket struct {
	VectorBucketArn         string                 `json:"vectorBucketArn"`
	VectorBucketName        string                 `json:"vectorBucketName"`
	CreationTime            time.Time              `json:"creationTime"`
	EncryptionConfiguration map[string]interface{} `json:"encryptionConfiguration,omitempty"`
}

type ListVectorBucketsRequest struct {
	MaxResults *int   `json:"maxResults"`
	NextToken  string `json:"nextToken"`
	Prefix     string `json:"prefix"`
}

type ListVectorBucketsResponse struct {
	NextToken     string          `json:"nextToken,omitempty"`
	VectorBuckets []*VectorBucket `json:"vectorBuckets"`
}

type DeleteVectorBucketRequest struct {
	VectorBucketName string `json:"vectorBucketName"`
	VectorBucketArn  string `json:"vectorBucketArn"`
}

type DeleteVectorBucketResponse struct{}

type PutVectorBucketPolicyRequest struct {
	VectorBucketName string `json:"vectorBucketName"`
	VectorBucketArn  string `json:"vectorBucketArn"`
	Policy           string `json:"policy"`
}

type PutVectorBucketPolicyResponse struct{}

type GetVectorBucketPolicyRequest struct {
	VectorBucketName string `json:"vectorBucketName"`
	VectorBucketArn  string `json:"vectorBucketArn"`
}

type GetVectorBucketPolicyResponse struct {
	Policy string `json:"policy"`
}

type DeleteVectorBucketPolicyRequest struct {
	VectorBucketName string `json:"vectorBucketName"`
	VectorBucketArn  string `json:"vectorBucketArn"`
}

type DeleteVectorBucketPolicyResponse struct{}

type CreateIndexRequest struct {
	DataType              string                 `json:"dataType"`
	Dimension             int32                  `json:"dimension"`
	DistanceMetric        string                 `json:"distanceMetric"`
	IndexArn              string                 `json:"indexArn"`
	IndexName             string                 `json:"indexName"`
	MetadataConfiguration *MetadataConfiguration `json:"metadataConfiguration,omitempty"`
	VectorBucketName      string                 `json:"vectorBucketName"`
	VectorBucketArn       string                 `json:"vectorBucketArn"`
}

type MetadataConfiguration struct {
	NonFilterableMetadataKeys []string `json:"nonFilterableMetadataKeys"`
}

type CreateIndexResponse struct {
	IndexArn string `json:"indexArn"`
}

type GetIndexRequest struct {
	IndexArn         string `json:"indexArn"`
	IndexName        string `json:"indexName"`
	VectorBucketName string `json:"vectorBucketName"`
	VectorBucketArn  string `json:"vectorBucketArn"`
}

type Index struct {
	CreationTime          time.Time              `json:"creationTime"`
	DataType              string                 `json:"dataType"`
	Dimension             int32                  `json:"dimension"`
	DistanceMetric        string                 `json:"distanceMetric"`
	IndexArn              string                 `json:"indexArn"`
	IndexName             string                 `json:"indexName"`
	MetadataConfiguration *MetadataConfiguration `json:"metadataConfiguration,omitempty"`
	VectorBucketName      string                 `json:"vectorBucketName"`
}

type GetIndexResponse struct {
	Index *Index `json:"index"`
}

type ListIndexesRequest struct {
	MaxResults       *int   `json:"maxResults"`
	NextToken        string `json:"nextToken"`
	Prefix           string `json:"prefix"`
	VectorBucketName string `json:"vectorBucketName"`
	VectorBucketArn  string `json:"vectorBucketArn"`
}

// IndexSummary is the listing shape: the catalog knows the name, ARN, and
// creation time without a per-index metadata read.
type IndexSummary struct {
	CreationTime     time.Time `json:"creationTime"`
	IndexArn         string    `json:"indexArn"`
	IndexName        string    `json:"indexName"`
	VectorBucketName string    `json:"vectorBucketName"`
}

type ListIndexesResponse struct {
	Indexes   []*IndexSummary `json:"indexes"`
	NextToken string          `json:"nextToken,omitempty"`
}

type DeleteIndexRequest struct {
	IndexArn         string `json:"indexArn"`
	IndexName        string `json:"indexName"`
	VectorBucketName string `json:"vectorBucketName"`
	VectorBucketArn  string `json:"vectorBucketArn"`
}

type DeleteIndexResponse struct{}

type PutInputVector struct {
	Data     *VectorData            `json:"data"`
	Key      string                 `json:"key"`
	Metadata map[string]interface{} `json:"metadata,omitempty"`
}

type VectorData struct {
	Float32 []float32 `json:"float32"`
}

type PutVectorsRequest struct {
	VectorBucketName string            `json:"vectorBucketName"`
	VectorBucketArn  string            `json:"vectorBucketArn"`
	IndexArn         string            `json:"indexArn"`
	IndexName        string            `json:"indexName"`
	Vectors          []*PutInputVector `json:"vectors"`
}

type PutVectorsResponse struct{}

type GetVectorsRequest struct {
	IndexArn         string   `json:"indexArn"`
	IndexName        string   `json:"indexName"`
	VectorBucketName string   `json:"vectorBucketName"`
	VectorBucketArn  string   `json:"vectorBucketArn"`
	Keys             []string `json:"keys"`
	ReturnData       bool     `json:"returnData"`
	ReturnMetadata   bool     `json:"returnMetadata"`
}

type OutputVector struct {
	Data     *VectorData            `json:"data,omitempty"`
	Key      string                 `json:"key"`
	Metadata map[string]interface{} `json:"metadata,omitempty"`
}

type GetVectorsResponse struct {
	Vectors []*OutputVector `json:"vectors"`
}

type ListVectorsRequest struct {
	IndexArn         string `json:"indexArn"`
	IndexName        string `json:"indexName"`
	VectorBucketName string `json:"vectorBucketName"`
	VectorBucketArn  string `json:"vectorBucketArn"`
	MaxResults       *int   `json:"maxResults"`
	NextToken        string `json:"nextToken"`
	ReturnData       bool   `json:"returnData"`
	ReturnMetadata   bool   `json:"returnMetadata"`
	SegmentCount     *int   `json:"segmentCount"`
	SegmentIndex     *int   `json:"segmentIndex"`
}

type ListVectorsResponse struct {
	NextToken string          `json:"nextToken,omitempty"`
	Vectors   []*OutputVector `json:"vectors"`
}

type DeleteVectorsRequest struct {
	IndexArn         string   `json:"indexArn"`
	IndexName        string   `json:"indexName"`
	VectorBucketName string   `json:"vectorBucketName"`
	VectorBucketArn  string   `json:"vectorBucketArn"`
	Keys             []string `json:"keys"`
}

type DeleteVectorsResponse struct{}

type QueryVector struct {
	Float32 []float32 `json:"float32"`
}

type QueryVectorsRequest struct {
	DistanceMetric   string                 `json:"distanceMetric"`
	Filter           map[string]interface{} `json:"filter"`
	IndexArn         string                 `json:"indexArn"`
	IndexName        string                 `json:"indexName"`
	QueryVector      *QueryVector           `json:"queryVector"`
	ReturnDistance   bool                   `json:"returnDistance"`
	ReturnMetadata   bool                   `json:"returnMetadata"`
	TopK             int32                  `json:"topK"`
	VectorBucketName string                 `json:"vectorBucketName"`
	VectorBucketArn  string                 `json:"vectorBucketArn"`
}

type QueryOutputVector struct {
	Data     *VectorData            `json:"data,omitempty"`
	Distance *float32               `json:"distance,omitempty"`
	Key      string                 `json:"key"`
	Metadata map[string]interface{} `json:"metadata,omitempty"`
}

type QueryVectorsResponse struct {
	DistanceMetric string               `json:"distanceMetric"`
	Vectors        []*QueryOutputVector `json:"vectors"`
}
