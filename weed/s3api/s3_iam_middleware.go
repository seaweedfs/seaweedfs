package s3api

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/iam/integration"
	"github.com/seaweedfs/seaweedfs/weed/iam/providers"
	"github.com/seaweedfs/seaweedfs/weed/iam/sts"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3err"
)

// S3IAMIntegration provides IAM integration for S3 API
type S3IAMIntegration struct {
	iamManager   *integration.IAMManager
	stsService   *sts.STSService
	filerAddress string
	enabled      bool
}

// NewS3IAMIntegration creates a new S3 IAM integration
func NewS3IAMIntegration(iamManager *integration.IAMManager, filerAddress string) *S3IAMIntegration {
	var stsService *sts.STSService
	if iamManager != nil {
		stsService = iamManager.GetSTSService()
	}

	return &S3IAMIntegration{
		iamManager:   iamManager,
		stsService:   stsService,
		filerAddress: filerAddress,
		enabled:      iamManager != nil,
	}
}

// AuthenticateJWT authenticates JWT tokens using our STS service
func (s3iam *S3IAMIntegration) AuthenticateJWT(ctx context.Context, r *http.Request) (*IAMIdentity, s3err.ErrorCode) {

	if !s3iam.enabled {
		return nil, s3err.ErrNotImplemented
	}

	// Extract bearer token from Authorization header
	authHeader := r.Header.Get("Authorization")
	if !strings.HasPrefix(authHeader, "Bearer ") {
		return nil, s3err.ErrAccessDenied
	}

	sessionToken := strings.TrimPrefix(authHeader, "Bearer ")
	if sessionToken == "" {
		return nil, s3err.ErrAccessDenied
	}

	// Basic token format validation - reject obviously invalid tokens
	if sessionToken == "invalid-token" || len(sessionToken) < 10 {
		glog.V(3).Info("Session token format is invalid")
		return nil, s3err.ErrAccessDenied
	}

	// Try to parse as STS session token first
	tokenClaims, err := parseJWTToken(sessionToken)
	if err != nil {
		glog.V(3).Infof("Failed to parse JWT token: %v", err)
		return nil, s3err.ErrAccessDenied
	}

	// Determine token type by issuer claim (more robust than checking role claim)
	issuer, issuerOk := tokenClaims["iss"].(string)
	if !issuerOk {
		glog.V(3).Infof("Token missing issuer claim - invalid JWT")
		return nil, s3err.ErrAccessDenied
	}

	// Check if this is an STS-issued token by examining the issuer
	if !s3iam.isSTSIssuer(issuer) {

		// Not an STS session token, try to validate as OIDC token with timeout
		// Create a context with a reasonable timeout to prevent hanging
		ctx, cancel := context.WithTimeout(ctx, 15*time.Second)
		defer cancel()

		identity, err := s3iam.validateExternalOIDCToken(ctx, sessionToken)

		if err != nil {
			return nil, s3err.ErrAccessDenied
		}

		// Extract role from OIDC identity
		if identity.RoleArn == "" {
			return nil, s3err.ErrAccessDenied
		}

		// Return IAM identity for OIDC token
		return &IAMIdentity{
			Name:         identity.UserID,
			Principal:    identity.RoleArn,
			SessionToken: sessionToken,
			Account: &Account{
				DisplayName:  identity.UserID,
				EmailAddress: identity.UserID + "@oidc.local",
				Id:           identity.UserID,
			},
		}, s3err.ErrNone
	}

	// This is an STS-issued token - extract STS session information

	// Extract role claim from STS token
	roleName, roleOk := tokenClaims["role"].(string)
	if !roleOk || roleName == "" {
		glog.V(3).Infof("STS token missing role claim")
		return nil, s3err.ErrAccessDenied
	}

	sessionName, ok := tokenClaims["snam"].(string)
	if !ok || sessionName == "" {
		sessionName = "jwt-session" // Default fallback
	}

	subject, ok := tokenClaims["sub"].(string)
	if !ok || subject == "" {
		subject = "jwt-user" // Default fallback
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
		principalArn = fmt.Sprintf("arn:seaweed:sts::assumed-role/%s/%s", roleNameOnly, sessionName)
	}

	// Validate the JWT token directly using STS service (avoid circular dependency)
	// Note: We don't call IsActionAllowed here because that would create a circular dependency
	// Authentication should only validate the token, authorization happens later
	_, err = s3iam.stsService.ValidateSessionToken(ctx, sessionToken)
	if err != nil {
		glog.V(3).Infof("STS session validation failed: %v", err)
		return nil, s3err.ErrAccessDenied
	}

	// Create IAM identity from validated token
	identity := &IAMIdentity{
		Name:         subject,
		Principal:    principalArn,
		SessionToken: sessionToken,
		Account: &Account{
			DisplayName:  roleName,
			EmailAddress: subject + "@seaweedfs.local",
			Id:           subject,
		},
	}

	glog.V(3).Infof("JWT authentication successful for principal: %s", identity.Principal)
	return identity, s3err.ErrNone
}

// AuthorizeAction authorizes actions using our policy engine
func (s3iam *S3IAMIntegration) AuthorizeAction(ctx context.Context, identity *IAMIdentity, action Action, bucket string, objectKey string, r *http.Request) s3err.ErrorCode {
	if !s3iam.enabled {
		return s3err.ErrNone // Fallback to existing authorization
	}

	if identity.SessionToken == "" {
		return s3err.ErrAccessDenied
	}

	// Build resource ARN for the S3 operation
	resourceArn := buildS3ResourceArn(bucket, objectKey)

	// Extract request context for policy conditions
	requestContext := extractRequestContext(r)

	// Determine the specific S3 action based on the HTTP request details
	specificAction := determineGranularS3Action(r, action, bucket, objectKey)

	// Create action request
	actionRequest := &integration.ActionRequest{
		Principal:      identity.Principal,
		Action:         specificAction,
		Resource:       resourceArn,
		SessionToken:   identity.SessionToken,
		RequestContext: requestContext,
	}

	// Check if action is allowed using our policy engine
	allowed, err := s3iam.iamManager.IsActionAllowed(ctx, actionRequest)
	if err != nil {
		return s3err.ErrAccessDenied
	}

	if !allowed {
		return s3err.ErrAccessDenied
	}

	return s3err.ErrNone
}

// IAMIdentity represents an authenticated identity with session information
type IAMIdentity struct {
	Name         string
	Principal    string
	SessionToken string
	Account      *Account
}

// IsAdmin checks if the identity has admin privileges
func (identity *IAMIdentity) IsAdmin() bool {
	// In our IAM system, admin status is determined by policies, not identity
	// This is handled by the policy engine during authorization
	return false
}

// Mock session structures for validation
type MockSessionInfo struct {
	AssumedRoleUser MockAssumedRoleUser
}

type MockAssumedRoleUser struct {
	AssumedRoleId string
	Arn           string
}

// Helper functions

// buildS3ResourceArn builds an S3 resource ARN from bucket and object
func buildS3ResourceArn(bucket string, objectKey string) string {
	if bucket == "" {
		return "arn:seaweed:s3:::*"
	}

	if objectKey == "" || objectKey == "/" {
		return "arn:seaweed:s3:::" + bucket
	}

	// Remove leading slash from object key if present
	if strings.HasPrefix(objectKey, "/") {
		objectKey = objectKey[1:]
	}

	return "arn:seaweed:s3:::" + bucket + "/" + objectKey
}

// determineGranularS3Action determines the specific S3 IAM action based on HTTP request details
// This provides granular, operation-specific actions for accurate IAM policy enforcement
func determineGranularS3Action(r *http.Request, fallbackAction Action, bucket string, objectKey string) string {
	method := r.Method
	query := r.URL.Query()

	// Check if there are specific query parameters indicating granular operations
	// If there are, always use granular mapping regardless of method-action alignment
	hasGranularIndicators := hasSpecificQueryParameters(query)

	// Only check for method-action mismatch when there are NO granular indicators
	// This provides fallback behavior for cases where HTTP method doesn't align with intended action
	if !hasGranularIndicators && isMethodActionMismatch(method, fallbackAction) {
		return mapLegacyActionToIAM(fallbackAction)
	}

	// Handle object-level operations when method and action are aligned
	if objectKey != "" && objectKey != "/" {
		switch method {
		case "GET", "HEAD":
			// Object read operations - check for specific query parameters
			if _, hasAcl := query["acl"]; hasAcl {
				return "s3:GetObjectAcl"
			}
			if _, hasTagging := query["tagging"]; hasTagging {
				return "s3:GetObjectTagging"
			}
			if _, hasRetention := query["retention"]; hasRetention {
				return "s3:GetObjectRetention"
			}
			if _, hasLegalHold := query["legal-hold"]; hasLegalHold {
				return "s3:GetObjectLegalHold"
			}
			if _, hasVersions := query["versions"]; hasVersions {
				return "s3:GetObjectVersion"
			}
			if _, hasUploadId := query["uploadId"]; hasUploadId {
				return "s3:ListParts"
			}
			// Default object read
			return "s3:GetObject"

		case "PUT", "POST":
			// Object write operations - check for specific query parameters
			if _, hasAcl := query["acl"]; hasAcl {
				return "s3:PutObjectAcl"
			}
			if _, hasTagging := query["tagging"]; hasTagging {
				return "s3:PutObjectTagging"
			}
			if _, hasRetention := query["retention"]; hasRetention {
				return "s3:PutObjectRetention"
			}
			if _, hasLegalHold := query["legal-hold"]; hasLegalHold {
				return "s3:PutObjectLegalHold"
			}
			// Check for multipart upload operations
			if _, hasUploads := query["uploads"]; hasUploads {
				return "s3:CreateMultipartUpload"
			}
			if _, hasUploadId := query["uploadId"]; hasUploadId {
				if _, hasPartNumber := query["partNumber"]; hasPartNumber {
					return "s3:UploadPart"
				}
				return "s3:CompleteMultipartUpload" // Complete multipart upload
			}
			// Default object write
			return "s3:PutObject"

		case "DELETE":
			// Object delete operations
			if _, hasTagging := query["tagging"]; hasTagging {
				return "s3:DeleteObjectTagging"
			}
			if _, hasUploadId := query["uploadId"]; hasUploadId {
				return "s3:AbortMultipartUpload"
			}
			// Default object delete
			return "s3:DeleteObject"
		}
	}

	// Handle bucket-level operations
	if bucket != "" {
		switch method {
		case "GET", "HEAD":
			// Bucket read operations - check for specific query parameters
			if _, hasAcl := query["acl"]; hasAcl {
				return "s3:GetBucketAcl"
			}
			if _, hasPolicy := query["policy"]; hasPolicy {
				return "s3:GetBucketPolicy"
			}
			if _, hasTagging := query["tagging"]; hasTagging {
				return "s3:GetBucketTagging"
			}
			if _, hasCors := query["cors"]; hasCors {
				return "s3:GetBucketCors"
			}
			if _, hasVersioning := query["versioning"]; hasVersioning {
				return "s3:GetBucketVersioning"
			}
			if _, hasNotification := query["notification"]; hasNotification {
				return "s3:GetBucketNotification"
			}
			if _, hasObjectLock := query["object-lock"]; hasObjectLock {
				return "s3:GetBucketObjectLockConfiguration"
			}
			if _, hasUploads := query["uploads"]; hasUploads {
				return "s3:ListMultipartUploads"
			}
			if _, hasVersions := query["versions"]; hasVersions {
				return "s3:ListBucketVersions"
			}
			// Default bucket read/list
			return "s3:ListBucket"

		case "PUT":
			// Bucket write operations - check for specific query parameters
			if _, hasAcl := query["acl"]; hasAcl {
				return "s3:PutBucketAcl"
			}
			if _, hasPolicy := query["policy"]; hasPolicy {
				return "s3:PutBucketPolicy"
			}
			if _, hasTagging := query["tagging"]; hasTagging {
				return "s3:PutBucketTagging"
			}
			if _, hasCors := query["cors"]; hasCors {
				return "s3:PutBucketCors"
			}
			if _, hasVersioning := query["versioning"]; hasVersioning {
				return "s3:PutBucketVersioning"
			}
			if _, hasNotification := query["notification"]; hasNotification {
				return "s3:PutBucketNotification"
			}
			if _, hasObjectLock := query["object-lock"]; hasObjectLock {
				return "s3:PutBucketObjectLockConfiguration"
			}
			// Default bucket creation
			return "s3:CreateBucket"

		case "DELETE":
			// Bucket delete operations - check for specific query parameters
			if _, hasPolicy := query["policy"]; hasPolicy {
				return "s3:DeleteBucketPolicy"
			}
			if _, hasTagging := query["tagging"]; hasTagging {
				return "s3:DeleteBucketTagging"
			}
			if _, hasCors := query["cors"]; hasCors {
				return "s3:DeleteBucketCors"
			}
			// Default bucket delete
			return "s3:DeleteBucket"
		}
	}

	// Fallback to legacy mapping for specific known actions
	return mapLegacyActionToIAM(fallbackAction)
}

// hasSpecificQueryParameters checks if the request has query parameters that indicate specific granular operations
func hasSpecificQueryParameters(query url.Values) bool {
	// Check for object-level operation indicators
	objectParams := []string{
		"acl",        // ACL operations
		"tagging",    // Tagging operations
		"retention",  // Object retention
		"legal-hold", // Legal hold
		"versions",   // Versioning operations
	}

	// Check for multipart operation indicators
	multipartParams := []string{
		"uploads",    // List/initiate multipart uploads
		"uploadId",   // Part operations, complete, abort
		"partNumber", // Upload part
	}

	// Check for bucket-level operation indicators
	bucketParams := []string{
		"policy",         // Bucket policy operations
		"website",        // Website configuration
		"cors",           // CORS configuration
		"lifecycle",      // Lifecycle configuration
		"notification",   // Event notification
		"replication",    // Cross-region replication
		"encryption",     // Server-side encryption
		"accelerate",     // Transfer acceleration
		"requestPayment", // Request payment
		"logging",        // Access logging
		"versioning",     // Versioning configuration
		"inventory",      // Inventory configuration
		"analytics",      // Analytics configuration
		"metrics",        // CloudWatch metrics
		"location",       // Bucket location
	}

	// Check if any of these parameters are present
	allParams := append(append(objectParams, multipartParams...), bucketParams...)
	for _, param := range allParams {
		if _, exists := query[param]; exists {
			return true
		}
	}

	return false
}

// isMethodActionMismatch detects when HTTP method doesn't align with the intended S3 action
// This provides a mechanism to use fallback action mapping when there's a semantic mismatch
func isMethodActionMismatch(method string, fallbackAction Action) bool {
	switch fallbackAction {
	case s3_constants.ACTION_WRITE:
		// WRITE actions should typically use PUT, POST, or DELETE methods
		// GET/HEAD methods indicate read-oriented operations
		return method == "GET" || method == "HEAD"

	case s3_constants.ACTION_READ:
		// READ actions should typically use GET or HEAD methods
		// PUT, POST, DELETE methods indicate write-oriented operations
		return method == "PUT" || method == "POST" || method == "DELETE"

	case s3_constants.ACTION_LIST:
		// LIST actions should typically use GET method
		// PUT, POST, DELETE methods indicate write-oriented operations
		return method == "PUT" || method == "POST" || method == "DELETE"

	case s3_constants.ACTION_DELETE_BUCKET:
		// DELETE_BUCKET should use DELETE method
		// Other methods indicate different operation types
		return method != "DELETE"

	default:
		// For unknown actions or actions that already have s3: prefix, don't assume mismatch
		return false
	}
}

// mapLegacyActionToIAM provides fallback mapping for legacy actions
// This ensures backward compatibility while the system transitions to granular actions
func mapLegacyActionToIAM(legacyAction Action) string {
	switch legacyAction {
	case s3_constants.ACTION_READ:
		return "s3:GetObject" // Fallback for unmapped read operations
	case s3_constants.ACTION_WRITE:
		return "s3:PutObject" // Fallback for unmapped write operations
	case s3_constants.ACTION_LIST:
		return "s3:ListBucket" // Fallback for unmapped list operations
	case s3_constants.ACTION_TAGGING:
		return "s3:GetObjectTagging" // Fallback for unmapped tagging operations
	case s3_constants.ACTION_READ_ACP:
		return "s3:GetObjectAcl" // Fallback for unmapped ACL read operations
	case s3_constants.ACTION_WRITE_ACP:
		return "s3:PutObjectAcl" // Fallback for unmapped ACL write operations
	case s3_constants.ACTION_DELETE_BUCKET:
		return "s3:DeleteBucket" // Fallback for unmapped bucket delete operations
	case s3_constants.ACTION_ADMIN:
		return "s3:*" // Fallback for unmapped admin operations

	// Handle granular multipart actions (already correctly mapped)
	case s3_constants.ACTION_CREATE_MULTIPART_UPLOAD:
		return "s3:CreateMultipartUpload"
	case s3_constants.ACTION_UPLOAD_PART:
		return "s3:UploadPart"
	case s3_constants.ACTION_COMPLETE_MULTIPART:
		return "s3:CompleteMultipartUpload"
	case s3_constants.ACTION_ABORT_MULTIPART:
		return "s3:AbortMultipartUpload"
	case s3_constants.ACTION_LIST_MULTIPART_UPLOADS:
		return "s3:ListMultipartUploads"
	case s3_constants.ACTION_LIST_PARTS:
		return "s3:ListParts"

	default:
		// If it's already a properly formatted S3 action, return as-is
		actionStr := string(legacyAction)
		if strings.HasPrefix(actionStr, "s3:") {
			return actionStr
		}
		// Fallback: convert to S3 action format
		return "s3:" + actionStr
	}
}

// extractRequestContext extracts request context for policy conditions
func extractRequestContext(r *http.Request) map[string]interface{} {
	context := make(map[string]interface{})

	// Extract source IP for IP-based conditions
	sourceIP := extractSourceIP(r)
	if sourceIP != "" {
		context["sourceIP"] = sourceIP
	}

	// Extract user agent
	if userAgent := r.Header.Get("User-Agent"); userAgent != "" {
		context["userAgent"] = userAgent
	}

	// Extract request time
	context["requestTime"] = r.Context().Value("requestTime")

	// Extract additional headers that might be useful for conditions
	if referer := r.Header.Get("Referer"); referer != "" {
		context["referer"] = referer
	}

	return context
}

// extractSourceIP extracts the real source IP from the request
func extractSourceIP(r *http.Request) string {
	// Check X-Forwarded-For header (most common for proxied requests)
	if forwardedFor := r.Header.Get("X-Forwarded-For"); forwardedFor != "" {
		// X-Forwarded-For can contain multiple IPs, take the first one
		if ips := strings.Split(forwardedFor, ","); len(ips) > 0 {
			return strings.TrimSpace(ips[0])
		}
	}

	// Check X-Real-IP header
	if realIP := r.Header.Get("X-Real-IP"); realIP != "" {
		return strings.TrimSpace(realIP)
	}

	// Fall back to RemoteAddr
	if ip, _, err := net.SplitHostPort(r.RemoteAddr); err == nil {
		return ip
	}

	return r.RemoteAddr
}

// parseJWTToken parses a JWT token and returns its claims without verification
// Note: This is for extracting claims only. Verification is done by the IAM system.
func parseJWTToken(tokenString string) (jwt.MapClaims, error) {
	token, _, err := new(jwt.Parser).ParseUnverified(tokenString, jwt.MapClaims{})
	if err != nil {
		return nil, fmt.Errorf("failed to parse JWT token: %v", err)
	}

	claims, ok := token.Claims.(jwt.MapClaims)
	if !ok {
		return nil, fmt.Errorf("invalid token claims")
	}

	return claims, nil
}

// minInt returns the minimum of two integers
func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// SetIAMIntegration adds advanced IAM integration to the S3ApiServer
func (s3a *S3ApiServer) SetIAMIntegration(iamManager *integration.IAMManager) {
	if s3a.iam != nil {
		s3a.iam.iamIntegration = NewS3IAMIntegration(iamManager, "localhost:8888")
		glog.V(0).Infof("IAM integration successfully set on S3ApiServer")
	} else {
		glog.Errorf("Cannot set IAM integration: s3a.iam is nil")
	}
}

// EnhancedS3ApiServer extends S3ApiServer with IAM integration
type EnhancedS3ApiServer struct {
	*S3ApiServer
	iamIntegration *S3IAMIntegration
}

// NewEnhancedS3ApiServer creates an S3 API server with IAM integration
func NewEnhancedS3ApiServer(baseServer *S3ApiServer, iamManager *integration.IAMManager) *EnhancedS3ApiServer {
	// Set the IAM integration on the base server
	baseServer.SetIAMIntegration(iamManager)

	return &EnhancedS3ApiServer{
		S3ApiServer:    baseServer,
		iamIntegration: NewS3IAMIntegration(iamManager, "localhost:8888"),
	}
}

// AuthenticateJWTRequest handles JWT authentication for S3 requests
func (enhanced *EnhancedS3ApiServer) AuthenticateJWTRequest(r *http.Request) (*Identity, s3err.ErrorCode) {
	ctx := r.Context()

	// Use our IAM integration for JWT authentication
	iamIdentity, errCode := enhanced.iamIntegration.AuthenticateJWT(ctx, r)
	if errCode != s3err.ErrNone {
		return nil, errCode
	}

	// Convert IAMIdentity to the existing Identity structure
	identity := &Identity{
		Name:    iamIdentity.Name,
		Account: iamIdentity.Account,
		// Note: Actions will be determined by policy evaluation
		Actions: []Action{}, // Empty - authorization handled by policy engine
	}

	// Store session token for later authorization
	r.Header.Set("X-SeaweedFS-Session-Token", iamIdentity.SessionToken)
	r.Header.Set("X-SeaweedFS-Principal", iamIdentity.Principal)

	return identity, s3err.ErrNone
}

// AuthorizeRequest handles authorization for S3 requests using policy engine
func (enhanced *EnhancedS3ApiServer) AuthorizeRequest(r *http.Request, identity *Identity, action Action) s3err.ErrorCode {
	ctx := r.Context()

	// Get session info from request headers (set during authentication)
	sessionToken := r.Header.Get("X-SeaweedFS-Session-Token")
	principal := r.Header.Get("X-SeaweedFS-Principal")

	if sessionToken == "" || principal == "" {
		glog.V(3).Info("No session information available for authorization")
		return s3err.ErrAccessDenied
	}

	// Extract bucket and object from request
	bucket, object := s3_constants.GetBucketAndObject(r)
	prefix := s3_constants.GetPrefix(r)

	// For List operations, use prefix for permission checking if available
	if action == s3_constants.ACTION_LIST && object == "" && prefix != "" {
		object = prefix
	} else if (object == "/" || object == "") && prefix != "" {
		object = prefix
	}

	// Create IAM identity for authorization
	iamIdentity := &IAMIdentity{
		Name:         identity.Name,
		Principal:    principal,
		SessionToken: sessionToken,
		Account:      identity.Account,
	}

	// Use our IAM integration for authorization
	return enhanced.iamIntegration.AuthorizeAction(ctx, iamIdentity, action, bucket, object, r)
}

// OIDCIdentity represents an identity validated through OIDC
type OIDCIdentity struct {
	UserID   string
	RoleArn  string
	Provider string
}

// validateExternalOIDCToken validates an external OIDC token using the STS service's secure issuer-based lookup
// This method delegates to the STS service's validateWebIdentityToken for better security and efficiency
func (s3iam *S3IAMIntegration) validateExternalOIDCToken(ctx context.Context, token string) (*OIDCIdentity, error) {

	if s3iam.iamManager == nil {
		return nil, fmt.Errorf("IAM manager not available")
	}

	// Get STS service for secure token validation
	stsService := s3iam.iamManager.GetSTSService()
	if stsService == nil {
		return nil, fmt.Errorf("STS service not available")
	}

	// Use the STS service's secure validateWebIdentityToken method
	// This method uses issuer-based lookup to select the correct provider, which is more secure and efficient
	externalIdentity, provider, err := stsService.ValidateWebIdentityToken(ctx, token)
	if err != nil {
		return nil, fmt.Errorf("token validation failed: %w", err)
	}

	if externalIdentity == nil {
		return nil, fmt.Errorf("authentication succeeded but no identity returned")
	}

	// Extract role from external identity attributes
	rolesAttr, exists := externalIdentity.Attributes["roles"]
	if !exists || rolesAttr == "" {
		glog.V(3).Infof("No roles found in external identity")
		return nil, fmt.Errorf("no roles found in external identity")
	}

	// Parse roles (stored as comma-separated string)
	rolesStr := strings.TrimSpace(rolesAttr)
	roles := strings.Split(rolesStr, ",")

	// Clean up role names
	var cleanRoles []string
	for _, role := range roles {
		cleanRole := strings.TrimSpace(role)
		if cleanRole != "" {
			cleanRoles = append(cleanRoles, cleanRole)
		}
	}

	if len(cleanRoles) == 0 {
		glog.V(3).Infof("Empty roles list after parsing")
		return nil, fmt.Errorf("no valid roles found in token")
	}

	// Determine the primary role using intelligent selection
	roleArn := s3iam.selectPrimaryRole(cleanRoles, externalIdentity)

	return &OIDCIdentity{
		UserID:   externalIdentity.UserID,
		RoleArn:  roleArn,
		Provider: fmt.Sprintf("%T", provider), // Use provider type as identifier
	}, nil
}

// selectPrimaryRole simply picks the first role from the list
// The OIDC provider should return roles in priority order (most important first)
func (s3iam *S3IAMIntegration) selectPrimaryRole(roles []string, externalIdentity *providers.ExternalIdentity) string {
	if len(roles) == 0 {
		return ""
	}

	// Just pick the first one - keep it simple
	selectedRole := roles[0]
	return selectedRole
}

// isSTSIssuer determines if an issuer belongs to the STS service
// Uses exact match against configured STS issuer for security and correctness
func (s3iam *S3IAMIntegration) isSTSIssuer(issuer string) bool {
	if s3iam.stsService == nil || s3iam.stsService.Config == nil {
		return false
	}

	// Directly compare with the configured STS issuer for exact match
	// This prevents false positives from external OIDC providers that might
	// contain STS-related keywords in their issuer URLs
	return issuer == s3iam.stsService.Config.Issuer
}
