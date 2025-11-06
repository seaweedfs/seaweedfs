package s3api

import (
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3err"
)

// S3MultipartIAMManager handles IAM integration for multipart upload operations
type S3MultipartIAMManager struct {
	s3iam *S3IAMIntegration
}

// NewS3MultipartIAMManager creates a new multipart IAM manager
func NewS3MultipartIAMManager(s3iam *S3IAMIntegration) *S3MultipartIAMManager {
	return &S3MultipartIAMManager{
		s3iam: s3iam,
	}
}

// MultipartUploadRequest represents a multipart upload request
type MultipartUploadRequest struct {
	Bucket       string            `json:"bucket"`        // S3 bucket name
	ObjectKey    string            `json:"object_key"`    // S3 object key
	UploadID     string            `json:"upload_id"`     // Multipart upload ID
	PartNumber   int               `json:"part_number"`   // Part number for upload part
	Operation    string            `json:"operation"`     // Multipart operation type
	SessionToken string            `json:"session_token"` // JWT session token
	Headers      map[string]string `json:"headers"`       // Request headers
	ContentSize  int64             `json:"content_size"`  // Content size for validation
}

// MultipartUploadPolicy represents security policies for multipart uploads
type MultipartUploadPolicy struct {
	MaxPartSize         int64         `json:"max_part_size"`         // Maximum part size (5GB AWS limit)
	MinPartSize         int64         `json:"min_part_size"`         // Minimum part size (5MB AWS limit, except last part)
	MaxParts            int           `json:"max_parts"`             // Maximum number of parts (10,000 AWS limit)
	MaxUploadDuration   time.Duration `json:"max_upload_duration"`   // Maximum time to complete multipart upload
	AllowedContentTypes []string      `json:"allowed_content_types"` // Allowed content types
	RequiredHeaders     []string      `json:"required_headers"`      // Required headers for validation
	IPWhitelist         []string      `json:"ip_whitelist"`          // Allowed IP addresses/ranges
}

// MultipartOperation represents different multipart upload operations
type MultipartOperation string

const (
	MultipartOpInitiate   MultipartOperation = "initiate"
	MultipartOpUploadPart MultipartOperation = "upload_part"
	MultipartOpComplete   MultipartOperation = "complete"
	MultipartOpAbort      MultipartOperation = "abort"
	MultipartOpList       MultipartOperation = "list"
	MultipartOpListParts  MultipartOperation = "list_parts"
)

// ValidateMultipartOperationWithIAM validates multipart operations using IAM policies
func (iam *IdentityAccessManagement) ValidateMultipartOperationWithIAM(r *http.Request, identity *Identity, operation MultipartOperation) s3err.ErrorCode {
	if iam.iamIntegration == nil {
		// Fall back to standard validation
		return s3err.ErrNone
	}

	// Extract bucket and object from request
	bucket, object := s3_constants.GetBucketAndObject(r)

	// Determine the S3 action based on multipart operation
	action := determineMultipartS3Action(operation)

	// Extract session token from request
	sessionToken := extractSessionTokenFromRequest(r)
	if sessionToken == "" {
		// No session token - use standard auth
		return s3err.ErrNone
	}

	// Retrieve the actual principal ARN from the request header
	// This header is set during initial authentication and contains the correct assumed role ARN
	principalArn := r.Header.Get("X-SeaweedFS-Principal")
	if principalArn == "" {
		glog.V(0).Info("IAM authorization for multipart operation failed: missing principal ARN in request header")
		return s3err.ErrAccessDenied
	}

	// Create IAM identity for authorization
	iamIdentity := &IAMIdentity{
		Name:         identity.Name,
		Principal:    principalArn,
		SessionToken: sessionToken,
		Account:      identity.Account,
	}

	// Authorize using IAM
	ctx := r.Context()
	errCode := iam.iamIntegration.AuthorizeAction(ctx, iamIdentity, action, bucket, object, r)
	if errCode != s3err.ErrNone {
		glog.V(3).Infof("IAM authorization failed for multipart operation: principal=%s operation=%s action=%s bucket=%s object=%s",
			iamIdentity.Principal, operation, action, bucket, object)
		return errCode
	}

	glog.V(3).Infof("IAM authorization succeeded for multipart operation: principal=%s operation=%s action=%s bucket=%s object=%s",
		iamIdentity.Principal, operation, action, bucket, object)
	return s3err.ErrNone
}

// ValidateMultipartRequestWithPolicy validates multipart request against security policy
func (policy *MultipartUploadPolicy) ValidateMultipartRequestWithPolicy(req *MultipartUploadRequest) error {
	if req == nil {
		return fmt.Errorf("multipart request cannot be nil")
	}

	// Validate part size for upload part operations
	if req.Operation == string(MultipartOpUploadPart) {
		if req.ContentSize > policy.MaxPartSize {
			return fmt.Errorf("part size %d exceeds maximum allowed %d", req.ContentSize, policy.MaxPartSize)
		}

		// Minimum part size validation (except for last part)
		// Note: Last part validation would require knowing if this is the final part
		if req.ContentSize < policy.MinPartSize && req.ContentSize > 0 {
			glog.V(2).Infof("Part size %d is below minimum %d - assuming last part", req.ContentSize, policy.MinPartSize)
		}

		// Validate part number
		if req.PartNumber < 1 || req.PartNumber > policy.MaxParts {
			return fmt.Errorf("part number %d is invalid (must be 1-%d)", req.PartNumber, policy.MaxParts)
		}
	}

	// Validate required headers first
	if req.Headers != nil {
		for _, requiredHeader := range policy.RequiredHeaders {
			if _, exists := req.Headers[requiredHeader]; !exists {
				// Check lowercase version
				if _, exists := req.Headers[strings.ToLower(requiredHeader)]; !exists {
					return fmt.Errorf("required header %s is missing", requiredHeader)
				}
			}
		}
	}

	// Validate content type if specified
	if len(policy.AllowedContentTypes) > 0 && req.Headers != nil {
		contentType := req.Headers["Content-Type"]
		if contentType == "" {
			contentType = req.Headers["content-type"]
		}

		allowed := false
		for _, allowedType := range policy.AllowedContentTypes {
			if contentType == allowedType {
				allowed = true
				break
			}
		}

		if !allowed {
			return fmt.Errorf("content type %s is not allowed", contentType)
		}
	}

	return nil
}

// Enhanced multipart handlers with IAM integration

// NewMultipartUploadWithIAM handles initiate multipart upload with IAM validation
func (s3a *S3ApiServer) NewMultipartUploadWithIAM(w http.ResponseWriter, r *http.Request) {
	// Validate IAM permissions first
	if s3a.iam.iamIntegration != nil {
		if identity, errCode := s3a.iam.authRequest(r, s3_constants.ACTION_WRITE); errCode != s3err.ErrNone {
			s3err.WriteErrorResponse(w, r, errCode)
			return
		} else {
			// Additional multipart-specific IAM validation
			if errCode := s3a.iam.ValidateMultipartOperationWithIAM(r, identity, MultipartOpInitiate); errCode != s3err.ErrNone {
				s3err.WriteErrorResponse(w, r, errCode)
				return
			}
		}
	}

	// Delegate to existing handler
	s3a.NewMultipartUploadHandler(w, r)
}

// CompleteMultipartUploadWithIAM handles complete multipart upload with IAM validation
func (s3a *S3ApiServer) CompleteMultipartUploadWithIAM(w http.ResponseWriter, r *http.Request) {
	// Validate IAM permissions first
	if s3a.iam.iamIntegration != nil {
		if identity, errCode := s3a.iam.authRequest(r, s3_constants.ACTION_WRITE); errCode != s3err.ErrNone {
			s3err.WriteErrorResponse(w, r, errCode)
			return
		} else {
			// Additional multipart-specific IAM validation
			if errCode := s3a.iam.ValidateMultipartOperationWithIAM(r, identity, MultipartOpComplete); errCode != s3err.ErrNone {
				s3err.WriteErrorResponse(w, r, errCode)
				return
			}
		}
	}

	// Delegate to existing handler
	s3a.CompleteMultipartUploadHandler(w, r)
}

// AbortMultipartUploadWithIAM handles abort multipart upload with IAM validation
func (s3a *S3ApiServer) AbortMultipartUploadWithIAM(w http.ResponseWriter, r *http.Request) {
	// Validate IAM permissions first
	if s3a.iam.iamIntegration != nil {
		if identity, errCode := s3a.iam.authRequest(r, s3_constants.ACTION_WRITE); errCode != s3err.ErrNone {
			s3err.WriteErrorResponse(w, r, errCode)
			return
		} else {
			// Additional multipart-specific IAM validation
			if errCode := s3a.iam.ValidateMultipartOperationWithIAM(r, identity, MultipartOpAbort); errCode != s3err.ErrNone {
				s3err.WriteErrorResponse(w, r, errCode)
				return
			}
		}
	}

	// Delegate to existing handler
	s3a.AbortMultipartUploadHandler(w, r)
}

// ListMultipartUploadsWithIAM handles list multipart uploads with IAM validation
func (s3a *S3ApiServer) ListMultipartUploadsWithIAM(w http.ResponseWriter, r *http.Request) {
	// Validate IAM permissions first
	if s3a.iam.iamIntegration != nil {
		if identity, errCode := s3a.iam.authRequest(r, s3_constants.ACTION_LIST); errCode != s3err.ErrNone {
			s3err.WriteErrorResponse(w, r, errCode)
			return
		} else {
			// Additional multipart-specific IAM validation
			if errCode := s3a.iam.ValidateMultipartOperationWithIAM(r, identity, MultipartOpList); errCode != s3err.ErrNone {
				s3err.WriteErrorResponse(w, r, errCode)
				return
			}
		}
	}

	// Delegate to existing handler
	s3a.ListMultipartUploadsHandler(w, r)
}

// UploadPartWithIAM handles upload part with IAM validation
func (s3a *S3ApiServer) UploadPartWithIAM(w http.ResponseWriter, r *http.Request) {
	// Validate IAM permissions first
	if s3a.iam.iamIntegration != nil {
		if identity, errCode := s3a.iam.authRequest(r, s3_constants.ACTION_WRITE); errCode != s3err.ErrNone {
			s3err.WriteErrorResponse(w, r, errCode)
			return
		} else {
			// Additional multipart-specific IAM validation
			if errCode := s3a.iam.ValidateMultipartOperationWithIAM(r, identity, MultipartOpUploadPart); errCode != s3err.ErrNone {
				s3err.WriteErrorResponse(w, r, errCode)
				return
			}

			// Validate part size and other policies
			if err := s3a.validateUploadPartRequest(r); err != nil {
				glog.Errorf("Upload part validation failed: %v", err)
				s3err.WriteErrorResponse(w, r, s3err.ErrInvalidRequest)
				return
			}
		}
	}

	// Delegate to existing object PUT handler (which handles upload part)
	s3a.PutObjectHandler(w, r)
}

// Helper functions

// determineMultipartS3Action maps multipart operations to granular S3 actions
// This enables fine-grained IAM policies for multipart upload operations
func determineMultipartS3Action(operation MultipartOperation) Action {
	switch operation {
	case MultipartOpInitiate:
		return s3_constants.ACTION_CREATE_MULTIPART_UPLOAD
	case MultipartOpUploadPart:
		return s3_constants.ACTION_UPLOAD_PART
	case MultipartOpComplete:
		return s3_constants.ACTION_COMPLETE_MULTIPART
	case MultipartOpAbort:
		return s3_constants.ACTION_ABORT_MULTIPART
	case MultipartOpList:
		return s3_constants.ACTION_LIST_MULTIPART_UPLOADS
	case MultipartOpListParts:
		return s3_constants.ACTION_LIST_PARTS
	default:
		// Fail closed for unmapped operations to prevent unintended access
		glog.Errorf("unmapped multipart operation: %s", operation)
		return "s3:InternalErrorUnknownMultipartAction" // Non-existent action ensures denial
	}
}

// extractSessionTokenFromRequest extracts session token from various request sources
func extractSessionTokenFromRequest(r *http.Request) string {
	// Check Authorization header for Bearer token
	if authHeader := r.Header.Get("Authorization"); authHeader != "" {
		if strings.HasPrefix(authHeader, "Bearer ") {
			return strings.TrimPrefix(authHeader, "Bearer ")
		}
	}

	// Check X-Amz-Security-Token header
	if token := r.Header.Get("X-Amz-Security-Token"); token != "" {
		return token
	}

	// Check query parameters for presigned URL tokens
	if token := r.URL.Query().Get("X-Amz-Security-Token"); token != "" {
		return token
	}

	return ""
}

// validateUploadPartRequest validates upload part request against policies
func (s3a *S3ApiServer) validateUploadPartRequest(r *http.Request) error {
	// Get default multipart policy
	policy := DefaultMultipartUploadPolicy()

	// Extract part number from query
	partNumberStr := r.URL.Query().Get("partNumber")
	if partNumberStr == "" {
		return fmt.Errorf("missing partNumber parameter")
	}

	partNumber, err := strconv.Atoi(partNumberStr)
	if err != nil {
		return fmt.Errorf("invalid partNumber: %v", err)
	}

	// Get content length
	contentLength := r.ContentLength
	if contentLength < 0 {
		contentLength = 0
	}

	// Create multipart request for validation
	bucket, object := s3_constants.GetBucketAndObject(r)
	multipartReq := &MultipartUploadRequest{
		Bucket:      bucket,
		ObjectKey:   object,
		PartNumber:  partNumber,
		Operation:   string(MultipartOpUploadPart),
		ContentSize: contentLength,
		Headers:     make(map[string]string),
	}

	// Copy relevant headers
	for key, values := range r.Header {
		if len(values) > 0 {
			multipartReq.Headers[key] = values[0]
		}
	}

	// Validate against policy
	return policy.ValidateMultipartRequestWithPolicy(multipartReq)
}

// DefaultMultipartUploadPolicy returns a default multipart upload security policy
func DefaultMultipartUploadPolicy() *MultipartUploadPolicy {
	return &MultipartUploadPolicy{
		MaxPartSize:         5 * 1024 * 1024 * 1024, // 5GB AWS limit
		MinPartSize:         5 * 1024 * 1024,        // 5MB AWS minimum (except last part)
		MaxParts:            10000,                  // AWS limit
		MaxUploadDuration:   7 * 24 * time.Hour,     // 7 days to complete upload
		AllowedContentTypes: []string{},             // Empty means all types allowed
		RequiredHeaders:     []string{},             // No required headers by default
		IPWhitelist:         []string{},             // Empty means no IP restrictions
	}
}

// MultipartUploadSession represents an ongoing multipart upload session
type MultipartUploadSession struct {
	UploadID     string                 `json:"upload_id"`
	Bucket       string                 `json:"bucket"`
	ObjectKey    string                 `json:"object_key"`
	Initiator    string                 `json:"initiator"`     // User who initiated the upload
	Owner        string                 `json:"owner"`         // Object owner
	CreatedAt    time.Time              `json:"created_at"`    // When upload was initiated
	Parts        []MultipartUploadPart  `json:"parts"`         // Uploaded parts
	Metadata     map[string]string      `json:"metadata"`      // Object metadata
	Policy       *MultipartUploadPolicy `json:"policy"`        // Applied security policy
	SessionToken string                 `json:"session_token"` // IAM session token
}

// MultipartUploadPart represents an uploaded part
type MultipartUploadPart struct {
	PartNumber   int       `json:"part_number"`
	Size         int64     `json:"size"`
	ETag         string    `json:"etag"`
	LastModified time.Time `json:"last_modified"`
	Checksum     string    `json:"checksum"` // Optional integrity checksum
}

// GetMultipartUploadSessions retrieves active multipart upload sessions for a bucket
func (s3a *S3ApiServer) GetMultipartUploadSessions(bucket string) ([]*MultipartUploadSession, error) {
	// This would typically query the filer for active multipart uploads
	// For now, return empty list as this is a placeholder for the full implementation
	return []*MultipartUploadSession{}, nil
}

// CleanupExpiredMultipartUploads removes expired multipart upload sessions
func (s3a *S3ApiServer) CleanupExpiredMultipartUploads(maxAge time.Duration) error {
	// This would typically scan for and remove expired multipart uploads
	// Implementation would depend on how multipart sessions are stored in the filer
	glog.V(2).Infof("Cleanup expired multipart uploads older than %v", maxAge)
	return nil
}
