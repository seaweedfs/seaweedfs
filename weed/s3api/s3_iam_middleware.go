package s3api

import (
	"context"
	"fmt"
	"net"
	"net/http"
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
	glog.V(0).Infof("🔐 AuthenticateJWT: Starting JWT authentication for %s %s", r.Method, r.URL.Path)
	
	if !s3iam.enabled {
		glog.V(0).Infof("🔐 AuthenticateJWT: IAM integration not enabled")
		return nil, s3err.ErrNotImplemented
	}

	// Extract bearer token from Authorization header
	authHeader := r.Header.Get("Authorization")
	if !strings.HasPrefix(authHeader, "Bearer ") {
		glog.V(0).Infof("🔐 AuthenticateJWT: No Bearer token found")
		return nil, s3err.ErrAccessDenied
	}

	sessionToken := strings.TrimPrefix(authHeader, "Bearer ")
	if sessionToken == "" {
		glog.V(0).Infof("🔐 AuthenticateJWT: Empty session token")
		return nil, s3err.ErrAccessDenied
	}
	
	glog.V(0).Infof("🔐 AuthenticateJWT: Processing JWT token (length: %d)", len(sessionToken))

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

	// Check if this is an STS session token (has "role" claim)
	roleName, ok := tokenClaims["role"].(string)
	if !ok || roleName == "" {
		glog.V(0).Infof("🔐 AuthenticateJWT: No 'role' claim found, treating as OIDC token")
		
		// Not an STS session token, try to validate as OIDC token with timeout
		// Create a context with a reasonable timeout to prevent hanging
		glog.V(0).Infof("🔐 AuthenticateJWT: Starting OIDC token validation with 15s timeout...")
		start := time.Now()
		
		ctx, cancel := context.WithTimeout(ctx, 15*time.Second)
		defer cancel()
		
		identity, err := s3iam.validateOIDCToken(ctx, sessionToken)
		elapsed := time.Since(start)
		
		if err != nil {
			// Check if it's a timeout error and log accordingly
			if ctx.Err() == context.DeadlineExceeded {
				glog.V(0).Infof("🔐 AuthenticateJWT: OIDC token validation TIMED OUT after %v: %v", elapsed, err)
			} else {
				glog.V(0).Infof("🔐 AuthenticateJWT: OIDC token validation FAILED after %v: %v", elapsed, err)
			}
			return nil, s3err.ErrAccessDenied
		}
		
		glog.V(0).Infof("🔐 AuthenticateJWT: OIDC token validation SUCCEEDED after %v", elapsed)

		// Extract role from OIDC identity
		if identity.RoleArn == "" {
			glog.V(0).Info("No role found in OIDC token")
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

	// Create action request
	actionRequest := &integration.ActionRequest{
		Principal:      identity.Principal,
		Action:         mapS3ActionToIAMAction(action),
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

// mapS3ActionToIAMAction maps S3 API actions to IAM policy actions
func mapS3ActionToIAMAction(s3Action Action) string {
	// Map S3 actions to standard IAM policy actions
	actionMap := map[Action]string{
		s3_constants.ACTION_READ:          "s3:GetObject",
		s3_constants.ACTION_WRITE:         "s3:PutObject",
		s3_constants.ACTION_LIST:          "s3:ListBucket",
		s3_constants.ACTION_TAGGING:       "s3:GetObjectTagging",
		s3_constants.ACTION_READ_ACP:      "s3:GetObjectAcl",
		s3_constants.ACTION_WRITE_ACP:     "s3:PutObjectAcl",
		s3_constants.ACTION_DELETE_BUCKET: "s3:DeleteBucket",
		s3_constants.ACTION_ADMIN:         "s3:*",
	}

	if iamAction, exists := actionMap[s3Action]; exists {
		return iamAction
	}

	// Default to the string representation of the action
	return string(s3Action)
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

// extractRoleNameFromPrincipal extracts role name from assumed role principal ARN
func extractRoleNameFromPrincipal(principal string) string {
	// Expected format: arn:seaweed:sts::assumed-role/RoleName/SessionName
	prefix := "arn:seaweed:sts::assumed-role/"
	if len(principal) > len(prefix) && principal[:len(prefix)] == prefix {
		remainder := principal[len(prefix):]
		// Split on first '/' to get role name
		if slashIndex := strings.Index(remainder, "/"); slashIndex != -1 {
			return remainder[:slashIndex]
		}
	}
	return principal // Return original if parsing fails
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

// validateOIDCToken validates an OIDC token using registered identity providers
func (s3iam *S3IAMIntegration) validateOIDCToken(ctx context.Context, token string) (*OIDCIdentity, error) {
	glog.V(0).Infof("🔍 validateOIDCToken: Starting OIDC token validation")
	
	if s3iam.iamManager == nil {
		glog.V(0).Infof("🔍 validateOIDCToken: IAM manager not available")
		return nil, fmt.Errorf("IAM manager not available")
	}

	// Get STS service to access identity providers
	stsService := s3iam.iamManager.GetSTSService()
	if stsService == nil {
		glog.V(0).Infof("🔍 validateOIDCToken: STS service not available")
		return nil, fmt.Errorf("STS service not available")
	}

	// Try to validate token with each registered OIDC provider
	providers := stsService.GetProviders()
	glog.V(0).Infof("🔍 validateOIDCToken: Found %d providers to try", len(providers))
	
	for providerName, provider := range providers {
		glog.V(0).Infof("🔍 validateOIDCToken: Trying provider '%s'...", providerName)
		start := time.Now()
		
		// Try to authenticate with this provider
		externalIdentity, err := provider.Authenticate(ctx, token)
		elapsed := time.Since(start)
		
		if err != nil {
			glog.V(0).Infof("🔍 validateOIDCToken: Provider '%s' FAILED after %v: %v", providerName, elapsed, err)
			continue
		}
		
		glog.V(0).Infof("🔍 validateOIDCToken: Provider '%s' SUCCEEDED after %v", providerName, elapsed)

		// Extract role from external identity attributes
		rolesAttr, exists := externalIdentity.Attributes["roles"]
		if !exists || rolesAttr == "" {
			glog.V(3).Infof("No roles found in external identity from provider %s", providerName)
			continue
		}

		// Parse roles (stored as comma-separated string)
		roles := strings.Split(rolesAttr, ",")
		if len(roles) == 0 {
			glog.V(3).Infof("Empty roles list from provider %s", providerName)
			continue
		}

		// Use the first role as the primary role
		roleArn := roles[0]

		return &OIDCIdentity{
			UserID:   externalIdentity.UserID,
			RoleArn:  roleArn,
			Provider: providerName,
		}, nil
	}

	return nil, fmt.Errorf("token not valid for any registered OIDC provider")
}

// getProviderNames returns a list of provider names for debugging
func getProviderNames(providers map[string]providers.IdentityProvider) []string {
	names := make([]string, 0, len(providers))
	for name := range providers {
		names = append(names, name)
	}
	return names
}
