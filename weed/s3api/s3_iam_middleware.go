package s3api

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"strings"

	"github.com/golang-jwt/jwt/v5"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/iam/integration"
	"github.com/seaweedfs/seaweedfs/weed/iam/sts"
	"github.com/seaweedfs/seaweedfs/weed/iam/utils"
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

// GetIAMManager returns the IAM manager
func (s3iam *S3IAMIntegration) GetIAMManager() *integration.IAMManager {
	return s3iam.iamManager
}

// OnRoleUpdate invalidates the cache when a role is updated
func (s3iam *S3IAMIntegration) OnRoleUpdate(roleName string) {
	if s3iam.iamManager == nil {
		return
	}
	s3iam.iamManager.InvalidateRoleCache(roleName)
	glog.V(3).Infof("OnRoleUpdate: invalidated cache for role %s", roleName)
}

// OnPolicyUpdate invalidates the cache when a policy is updated
func (s3iam *S3IAMIntegration) OnPolicyUpdate(policyName string) {
	if s3iam.iamManager == nil {
		return
	}
	s3iam.iamManager.InvalidatePolicyCache(policyName)
	glog.V(3).Infof("OnPolicyUpdate: invalidated cache for policy %s", policyName)
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
		glog.V(3).Infof("Token issuer %s is not the trusted STS issuer", issuer)
		return nil, s3err.ErrAccessDenied
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
		principalArn = fmt.Sprintf("arn:aws:sts::assumed-role/%s/%s", roleNameOnly, sessionName)
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
	specificAction := ResolveS3Action(r, string(action), bucket, objectKey)

	// Create action request
	actionRequest := &integration.ActionRequest{
		Principal:        identity.Principal,
		Action:           specificAction,
		Resource:         resourceArn,
		SessionToken:     identity.SessionToken,
		RequestContext:   requestContext,
		AttachedPolicies: identity.AttachedPolicies,
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

// ValidateTrustPolicyForPrincipal validates if a principal is allowed to assume a role
func (s3iam *S3IAMIntegration) ValidateTrustPolicyForPrincipal(ctx context.Context, roleArn, principalArn string) error {
	if !s3iam.enabled {
		return fmt.Errorf("IAM integration not enabled")
	}
	return s3iam.iamManager.ValidateTrustPolicyForPrincipal(ctx, roleArn, principalArn)
}

// IAMIdentity represents an authenticated identity with session information
type IAMIdentity struct {
	Name             string
	Principal        string
	SessionToken     string
	Account          *Account
	AttachedPolicies []string
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
		return "arn:aws:s3:::*"
	}

	if objectKey == "" || objectKey == "/" {
		return "arn:aws:s3:::" + bucket
	}

	// Remove leading slash from object key if present
	objectKey = strings.TrimPrefix(objectKey, "/")

	return "arn:aws:s3:::" + bucket + "/" + objectKey
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
		return s3_constants.S3_ACTION_LIST_MULTIPART_UPLOADS
	case s3_constants.ACTION_LIST_PARTS:
		return s3_constants.S3_ACTION_LIST_PARTS

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
		glog.V(1).Infof("IAM integration successfully set on S3ApiServer")
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
// GetCredentialAndIdentityForSession retrieves temporary credentials and identity for a session token
func (s3iam *S3IAMIntegration) GetCredentialAndIdentityForSession(sessionToken string) (*Credential, *Identity, error) {
	if !s3iam.enabled || s3iam.stsService == nil {
		return nil, nil, fmt.Errorf("STS service not available")
	}

	stsCreds, claims, err := s3iam.stsService.GetCredentialsForSession(sessionToken)
	if err != nil {
		return nil, nil, err
	}

	// Map STS Credentials to s3api.Credential
	cred := &Credential{
		AccessKey: stsCreds.AccessKeyId,
		SecretKey: stsCreds.SecretAccessKey,
	}

	// Extract role name from role ARN
	roleName := utils.ExtractRoleNameFromArn(claims.RoleArn)

	// Create Identity
	identity := &Identity{
		Name: claims.Subject, // Subject is the user ID
		Account: &Account{
			DisplayName:  roleName,
			EmailAddress: claims.Subject + "@seaweedfs.local",
			Id:           claims.Subject,
		},
		PrincipalArn: claims.Principal, // Use assumed-role ARN (arn:aws:sts::assumed-role/RoleName/SessionName)
		Credentials:  []*Credential{cred},
		Actions:      []Action{}, // Actions handled by policy
	}

	return cred, identity, nil
}
