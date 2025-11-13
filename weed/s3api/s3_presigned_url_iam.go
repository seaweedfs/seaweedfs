package s3api

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3err"
)

// S3PresignedURLManager handles IAM integration for presigned URLs
type S3PresignedURLManager struct {
	s3iam *S3IAMIntegration
}

// NewS3PresignedURLManager creates a new presigned URL manager with IAM integration
func NewS3PresignedURLManager(s3iam *S3IAMIntegration) *S3PresignedURLManager {
	return &S3PresignedURLManager{
		s3iam: s3iam,
	}
}

// PresignedURLRequest represents a request to generate a presigned URL
type PresignedURLRequest struct {
	Method       string            `json:"method"`        // HTTP method (GET, PUT, POST, DELETE)
	Bucket       string            `json:"bucket"`        // S3 bucket name
	ObjectKey    string            `json:"object_key"`    // S3 object key
	Expiration   time.Duration     `json:"expiration"`    // URL expiration duration
	SessionToken string            `json:"session_token"` // JWT session token for IAM
	Headers      map[string]string `json:"headers"`       // Additional headers to sign
	QueryParams  map[string]string `json:"query_params"`  // Additional query parameters
}

// PresignedURLResponse represents the generated presigned URL
type PresignedURLResponse struct {
	URL            string            `json:"url"`             // The presigned URL
	Method         string            `json:"method"`          // HTTP method
	Headers        map[string]string `json:"headers"`         // Required headers
	ExpiresAt      time.Time         `json:"expires_at"`      // URL expiration time
	SignedHeaders  []string          `json:"signed_headers"`  // List of signed headers
	CanonicalQuery string            `json:"canonical_query"` // Canonical query string
}

// ValidatePresignedURLWithIAM validates a presigned URL request using IAM policies
func (iam *IdentityAccessManagement) ValidatePresignedURLWithIAM(r *http.Request, identity *Identity) s3err.ErrorCode {
	if iam.iamIntegration == nil {
		// Fall back to standard validation
		return s3err.ErrNone
	}

	// Extract bucket and object from request
	bucket, object := s3_constants.GetBucketAndObject(r)

	// Determine the S3 action from HTTP method and path
	action := determineS3ActionFromRequest(r, bucket, object)

	// Check if the user has permission for this action
	ctx := r.Context()
	sessionToken := extractSessionTokenFromPresignedURL(r)
	if sessionToken == "" {
		// No session token in presigned URL - use standard auth
		return s3err.ErrNone
	}

	// Parse JWT token to extract role and session information
	tokenClaims, err := parseJWTToken(sessionToken)
	if err != nil {
		glog.V(3).Infof("Failed to parse JWT token in presigned URL: %v", err)
		return s3err.ErrAccessDenied
	}

	// Extract role information from token claims
	roleName, ok := tokenClaims["role"].(string)
	if !ok || roleName == "" {
		glog.V(3).Info("No role found in JWT token for presigned URL")
		return s3err.ErrAccessDenied
	}

	sessionName, ok := tokenClaims["snam"].(string)
	if !ok || sessionName == "" {
		sessionName = "presigned-session" // Default fallback
	}

	// Use the principal ARN directly from token claims, or build it if not available
	principalArn, ok := tokenClaims["principal"].(string)
	if !ok || principalArn == "" {
		// Fallback: extract role name from role ARN and build principal ARN
		roleNameOnly := roleName
		if strings.Contains(roleName, "/") {
			parts := strings.Split(roleName, "/")
			roleNameOnly = parts[len(parts)-1]
		}
		principalArn = fmt.Sprintf("arn:aws:sts::assumed-role/%s/%s", roleNameOnly, sessionName)
	}

	// Create IAM identity for authorization using extracted information
	iamIdentity := &IAMIdentity{
		Name:         identity.Name,
		Principal:    principalArn,
		SessionToken: sessionToken,
		Account:      identity.Account,
	}

	// Authorize using IAM
	errCode := iam.iamIntegration.AuthorizeAction(ctx, iamIdentity, action, bucket, object, r)
	if errCode != s3err.ErrNone {
		glog.V(3).Infof("IAM authorization failed for presigned URL: principal=%s action=%s bucket=%s object=%s",
			iamIdentity.Principal, action, bucket, object)
		return errCode
	}

	glog.V(3).Infof("IAM authorization succeeded for presigned URL: principal=%s action=%s bucket=%s object=%s",
		iamIdentity.Principal, action, bucket, object)
	return s3err.ErrNone
}

// GeneratePresignedURLWithIAM generates a presigned URL with IAM policy validation
func (pm *S3PresignedURLManager) GeneratePresignedURLWithIAM(ctx context.Context, req *PresignedURLRequest, baseURL string) (*PresignedURLResponse, error) {
	if pm.s3iam == nil || !pm.s3iam.enabled {
		return nil, fmt.Errorf("IAM integration not enabled")
	}

	// Validate session token and get identity
	// Use a proper ARN format for the principal
	principalArn := fmt.Sprintf("arn:aws:sts::assumed-role/PresignedUser/presigned-session")
	iamIdentity := &IAMIdentity{
		SessionToken: req.SessionToken,
		Principal:    principalArn,
		Name:         "presigned-user",
		Account:      &AccountAdmin,
	}

	// Determine S3 action from method
	action := determineS3ActionFromMethodAndPath(req.Method, req.Bucket, req.ObjectKey)

	// Check IAM permissions before generating URL
	authRequest := &http.Request{
		Method: req.Method,
		URL:    &url.URL{Path: "/" + req.Bucket + "/" + req.ObjectKey},
		Header: make(http.Header),
	}
	authRequest.Header.Set("Authorization", "Bearer "+req.SessionToken)
	authRequest = authRequest.WithContext(ctx)

	errCode := pm.s3iam.AuthorizeAction(ctx, iamIdentity, action, req.Bucket, req.ObjectKey, authRequest)
	if errCode != s3err.ErrNone {
		return nil, fmt.Errorf("IAM authorization failed: user does not have permission for action %s on resource %s/%s", action, req.Bucket, req.ObjectKey)
	}

	// Generate presigned URL with validated permissions
	return pm.generatePresignedURL(req, baseURL, iamIdentity)
}

// generatePresignedURL creates the actual presigned URL
func (pm *S3PresignedURLManager) generatePresignedURL(req *PresignedURLRequest, baseURL string, identity *IAMIdentity) (*PresignedURLResponse, error) {
	// Calculate expiration time
	expiresAt := time.Now().Add(req.Expiration)

	// Build the base URL
	urlPath := "/" + req.Bucket
	if req.ObjectKey != "" {
		urlPath += "/" + req.ObjectKey
	}

	// Create query parameters for AWS signature v4
	queryParams := make(map[string]string)
	for k, v := range req.QueryParams {
		queryParams[k] = v
	}

	// Add AWS signature v4 parameters
	queryParams["X-Amz-Algorithm"] = "AWS4-HMAC-SHA256"
	queryParams["X-Amz-Credential"] = fmt.Sprintf("seaweedfs/%s/us-east-1/s3/aws4_request", expiresAt.Format("20060102"))
	queryParams["X-Amz-Date"] = expiresAt.Format("20060102T150405Z")
	queryParams["X-Amz-Expires"] = strconv.Itoa(int(req.Expiration.Seconds()))
	queryParams["X-Amz-SignedHeaders"] = "host"

	// Add session token if available
	if identity.SessionToken != "" {
		queryParams["X-Amz-Security-Token"] = identity.SessionToken
	}

	// Build canonical query string
	canonicalQuery := buildCanonicalQuery(queryParams)

	// For now, we'll create a mock signature
	// In production, this would use proper AWS signature v4 signing
	mockSignature := generateMockSignature(req.Method, urlPath, canonicalQuery, identity.SessionToken)
	queryParams["X-Amz-Signature"] = mockSignature

	// Build final URL
	finalQuery := buildCanonicalQuery(queryParams)
	fullURL := baseURL + urlPath + "?" + finalQuery

	// Prepare response
	headers := make(map[string]string)
	for k, v := range req.Headers {
		headers[k] = v
	}

	return &PresignedURLResponse{
		URL:            fullURL,
		Method:         req.Method,
		Headers:        headers,
		ExpiresAt:      expiresAt,
		SignedHeaders:  []string{"host"},
		CanonicalQuery: canonicalQuery,
	}, nil
}

// Helper functions

// determineS3ActionFromRequest determines the S3 action based on HTTP request
func determineS3ActionFromRequest(r *http.Request, bucket, object string) Action {
	return determineS3ActionFromMethodAndPath(r.Method, bucket, object)
}

// determineS3ActionFromMethodAndPath determines the S3 action based on method and path
func determineS3ActionFromMethodAndPath(method, bucket, object string) Action {
	switch method {
	case "GET":
		if object == "" {
			return s3_constants.ACTION_LIST // ListBucket
		} else {
			return s3_constants.ACTION_READ // GetObject
		}
	case "PUT", "POST":
		return s3_constants.ACTION_WRITE // PutObject
	case "DELETE":
		if object == "" {
			return s3_constants.ACTION_DELETE_BUCKET // DeleteBucket
		} else {
			return s3_constants.ACTION_WRITE // DeleteObject (uses WRITE action)
		}
	case "HEAD":
		if object == "" {
			return s3_constants.ACTION_LIST // HeadBucket
		} else {
			return s3_constants.ACTION_READ // HeadObject
		}
	default:
		return s3_constants.ACTION_READ // Default to read
	}
}

// extractSessionTokenFromPresignedURL extracts session token from presigned URL query parameters
func extractSessionTokenFromPresignedURL(r *http.Request) string {
	// Check for X-Amz-Security-Token in query parameters
	if token := r.URL.Query().Get("X-Amz-Security-Token"); token != "" {
		return token
	}

	// Check for session token in other possible locations
	if token := r.URL.Query().Get("SessionToken"); token != "" {
		return token
	}

	return ""
}

// buildCanonicalQuery builds a canonical query string for AWS signature
func buildCanonicalQuery(params map[string]string) string {
	var keys []string
	for k := range params {
		keys = append(keys, k)
	}

	// Sort keys for canonical order
	for i := 0; i < len(keys); i++ {
		for j := i + 1; j < len(keys); j++ {
			if keys[i] > keys[j] {
				keys[i], keys[j] = keys[j], keys[i]
			}
		}
	}

	var parts []string
	for _, k := range keys {
		parts = append(parts, fmt.Sprintf("%s=%s", url.QueryEscape(k), url.QueryEscape(params[k])))
	}

	return strings.Join(parts, "&")
}

// generateMockSignature generates a mock signature for testing purposes
func generateMockSignature(method, path, query, sessionToken string) string {
	// This is a simplified signature for demonstration
	// In production, use proper AWS signature v4 calculation
	data := fmt.Sprintf("%s\n%s\n%s\n%s", method, path, query, sessionToken)
	hash := sha256.Sum256([]byte(data))
	return hex.EncodeToString(hash[:])[:16] // Truncate for readability
}

// ValidatePresignedURLExpiration validates that a presigned URL hasn't expired
func ValidatePresignedURLExpiration(r *http.Request) error {
	query := r.URL.Query()

	// Get X-Amz-Date and X-Amz-Expires
	dateStr := query.Get("X-Amz-Date")
	expiresStr := query.Get("X-Amz-Expires")

	if dateStr == "" || expiresStr == "" {
		return fmt.Errorf("missing required presigned URL parameters")
	}

	// Parse date (always in UTC)
	signedDate, err := time.Parse("20060102T150405Z", dateStr)
	if err != nil {
		return fmt.Errorf("invalid X-Amz-Date format: %v", err)
	}

	// Parse expires
	expires, err := strconv.Atoi(expiresStr)
	if err != nil {
		return fmt.Errorf("invalid X-Amz-Expires format: %v", err)
	}

	// Check expiration - compare in UTC
	expirationTime := signedDate.Add(time.Duration(expires) * time.Second)
	now := time.Now().UTC()
	if now.After(expirationTime) {
		return fmt.Errorf("presigned URL has expired")
	}

	return nil
}

// PresignedURLSecurityPolicy represents security constraints for presigned URL generation
type PresignedURLSecurityPolicy struct {
	MaxExpirationDuration time.Duration `json:"max_expiration_duration"` // Maximum allowed expiration
	AllowedMethods        []string      `json:"allowed_methods"`         // Allowed HTTP methods
	RequiredHeaders       []string      `json:"required_headers"`        // Headers that must be present
	IPWhitelist           []string      `json:"ip_whitelist"`            // Allowed IP addresses/ranges
	MaxFileSize           int64         `json:"max_file_size"`           // Maximum file size for uploads
}

// DefaultPresignedURLSecurityPolicy returns a default security policy
func DefaultPresignedURLSecurityPolicy() *PresignedURLSecurityPolicy {
	return &PresignedURLSecurityPolicy{
		MaxExpirationDuration: 7 * 24 * time.Hour, // 7 days max
		AllowedMethods:        []string{"GET", "PUT", "POST", "HEAD"},
		RequiredHeaders:       []string{},
		IPWhitelist:           []string{},             // Empty means no IP restrictions
		MaxFileSize:           5 * 1024 * 1024 * 1024, // 5GB default
	}
}

// ValidatePresignedURLRequest validates a presigned URL request against security policy
func (policy *PresignedURLSecurityPolicy) ValidatePresignedURLRequest(req *PresignedURLRequest) error {
	// Check expiration duration
	if req.Expiration > policy.MaxExpirationDuration {
		return fmt.Errorf("expiration duration %v exceeds maximum allowed %v", req.Expiration, policy.MaxExpirationDuration)
	}

	// Check HTTP method
	methodAllowed := false
	for _, allowedMethod := range policy.AllowedMethods {
		if req.Method == allowedMethod {
			methodAllowed = true
			break
		}
	}
	if !methodAllowed {
		return fmt.Errorf("HTTP method %s is not allowed", req.Method)
	}

	// Check required headers
	for _, requiredHeader := range policy.RequiredHeaders {
		if _, exists := req.Headers[requiredHeader]; !exists {
			return fmt.Errorf("required header %s is missing", requiredHeader)
		}
	}

	return nil
}
