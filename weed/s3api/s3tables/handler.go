package s3tables

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
)

const (
	TablesPath       = "/table-buckets"
	DefaultAccountID = "000000000000"
	DefaultRegion    = "us-east-1"

	// Extended entry attributes for metadata storage
	ExtendedKeyMetadata = "s3tables.metadata"
	ExtendedKeyPolicy   = "s3tables.policy"
	ExtendedKeyTags     = "s3tables.tags"

	// Maximum request body size (10MB)
	maxRequestBodySize = 10 * 1024 * 1024
)

var (
	ErrVersionTokenMismatch = errors.New("version token mismatch")
	ErrAccessDenied         = errors.New("access denied")
)

type ResourceType string

const (
	ResourceTypeBucket ResourceType = "bucket"
	ResourceTypeTable  ResourceType = "table"
)

// S3TablesHandler handles S3 Tables API requests
type S3TablesHandler struct {
	region    string
	accountID string
}

// NewS3TablesHandler creates a new S3 Tables handler
func NewS3TablesHandler() *S3TablesHandler {
	return &S3TablesHandler{
		region:    DefaultRegion,
		accountID: DefaultAccountID,
	}
}

// SetRegion sets the AWS region for ARN generation
func (h *S3TablesHandler) SetRegion(region string) {
	if region != "" {
		h.region = region
	}
}

// SetAccountID sets the AWS account ID for ARN generation
func (h *S3TablesHandler) SetAccountID(accountID string) {
	if accountID != "" {
		h.accountID = accountID
	}
}

// FilerClient interface for filer operations
type FilerClient interface {
	WithFilerClient(streamingMode bool, fn func(client filer_pb.SeaweedFilerClient) error) error
}

// HandleRequest is the main entry point for S3 Tables API requests
func (h *S3TablesHandler) HandleRequest(w http.ResponseWriter, r *http.Request, filerClient FilerClient) {
	// S3 Tables API uses x-amz-target header to specify the operation
	target := r.Header.Get("X-Amz-Target")
	if target == "" {
		// Try to get from query parameter for CLI compatibility
		target = r.URL.Query().Get("Action")
	}

	// Extract operation name (e.g., "S3Tables.CreateTableBucket" -> "CreateTableBucket")
	operation := target
	if idx := strings.LastIndex(target, "."); idx != -1 {
		operation = target[idx+1:]
	}

	glog.V(3).Infof("S3Tables: handling operation %s", operation)

	var err error
	switch operation {
	// Table Bucket operations
	case "CreateTableBucket":
		err = h.handleCreateTableBucket(w, r, filerClient)
	case "GetTableBucket":
		err = h.handleGetTableBucket(w, r, filerClient)
	case "ListTableBuckets":
		err = h.handleListTableBuckets(w, r, filerClient)
	case "DeleteTableBucket":
		err = h.handleDeleteTableBucket(w, r, filerClient)

	// Table Bucket Policy operations
	case "PutTableBucketPolicy":
		err = h.handlePutTableBucketPolicy(w, r, filerClient)
	case "GetTableBucketPolicy":
		err = h.handleGetTableBucketPolicy(w, r, filerClient)
	case "DeleteTableBucketPolicy":
		err = h.handleDeleteTableBucketPolicy(w, r, filerClient)

	// Namespace operations
	case "CreateNamespace":
		err = h.handleCreateNamespace(w, r, filerClient)
	case "GetNamespace":
		err = h.handleGetNamespace(w, r, filerClient)
	case "ListNamespaces":
		err = h.handleListNamespaces(w, r, filerClient)
	case "DeleteNamespace":
		err = h.handleDeleteNamespace(w, r, filerClient)

	// Table operations
	case "CreateTable":
		err = h.handleCreateTable(w, r, filerClient)
	case "GetTable":
		err = h.handleGetTable(w, r, filerClient)
	case "ListTables":
		err = h.handleListTables(w, r, filerClient)
	case "DeleteTable":
		err = h.handleDeleteTable(w, r, filerClient)

	// Table Policy operations
	case "PutTablePolicy":
		err = h.handlePutTablePolicy(w, r, filerClient)
	case "GetTablePolicy":
		err = h.handleGetTablePolicy(w, r, filerClient)
	case "DeleteTablePolicy":
		err = h.handleDeleteTablePolicy(w, r, filerClient)

	// Tagging operations
	case "TagResource":
		err = h.handleTagResource(w, r, filerClient)
	case "ListTagsForResource":
		err = h.handleListTagsForResource(w, r, filerClient)
	case "UntagResource":
		err = h.handleUntagResource(w, r, filerClient)

	default:
		h.writeError(w, http.StatusBadRequest, ErrCodeInvalidRequest, fmt.Sprintf("Unknown operation: %s", operation))
		return
	}

	if err != nil {
		glog.Errorf("S3Tables: error handling %s: %v", operation, err)
	}
}

// Principal/authorization helpers

// getAccountID returns the authenticated account ID from the request or the handler's default.
// This is also used as the principal for permission checks, ensuring alignment between
// the caller identity and ownership verification when IAM is enabled.
func (h *S3TablesHandler) getAccountID(r *http.Request) string {
	if identityName := s3_constants.GetIdentityNameFromContext(r); identityName != "" {
		return identityName
	}
	if accountID := r.Header.Get(s3_constants.AmzAccountId); accountID != "" {
		return accountID
	}
	return h.accountID
}

// Request/Response helpers

func (h *S3TablesHandler) readRequestBody(r *http.Request, v interface{}) error {
	defer r.Body.Close()

	// Limit request body size to prevent unbounded reads
	limitedReader := io.LimitReader(r.Body, maxRequestBodySize+1)
	body, err := io.ReadAll(limitedReader)
	if err != nil {
		return fmt.Errorf("failed to read request body: %w", err)
	}

	// Check if body exceeds size limit
	if len(body) > maxRequestBodySize {
		return fmt.Errorf("request body too large: exceeds maximum size of %d bytes", maxRequestBodySize)
	}

	if len(body) == 0 {
		return nil
	}

	if err := json.Unmarshal(body, v); err != nil {
		return fmt.Errorf("failed to decode request: %w", err)
	}

	return nil
}

// Response writing helpers

func (h *S3TablesHandler) writeJSON(w http.ResponseWriter, status int, data interface{}) {
	w.Header().Set("Content-Type", "application/x-amz-json-1.1")
	w.WriteHeader(status)
	if data != nil {
		if err := json.NewEncoder(w).Encode(data); err != nil {
			glog.Errorf("S3Tables: failed to encode response: %v", err)
		}
	}
}

func (h *S3TablesHandler) writeError(w http.ResponseWriter, status int, code, message string) {
	w.Header().Set("Content-Type", "application/x-amz-json-1.1")
	w.WriteHeader(status)
	errorResponse := map[string]interface{}{
		"__type":  code,
		"message": message,
	}
	if err := json.NewEncoder(w).Encode(errorResponse); err != nil {
		glog.Errorf("S3Tables: failed to encode error response: %v", err)
	}
}

// ARN generation helpers

func (h *S3TablesHandler) generateTableBucketARN(ownerAccountID, bucketName string) string {
	return fmt.Sprintf("arn:aws:s3tables:%s:%s:bucket/%s", h.region, ownerAccountID, bucketName)
}

func (h *S3TablesHandler) generateTableARN(ownerAccountID, bucketName, tableID string) string {
	return fmt.Sprintf("arn:aws:s3tables:%s:%s:bucket/%s/table/%s", h.region, ownerAccountID, bucketName, tableID)
}

func isAuthError(err error) bool {
	var authErr *AuthError
	return errors.As(err, &authErr) || errors.Is(err, ErrAccessDenied)
}
