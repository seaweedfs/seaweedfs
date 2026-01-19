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

// privateNetworks contains pre-parsed private IP ranges for efficient lookups
var privateNetworks []*net.IPNet

func init() {
	// Private IPv4 ranges (RFC1918) and IPv6 Unique Local Addresses (ULA)
	privateRanges := []string{
		"10.0.0.0/8",     // IPv4 private
		"172.16.0.0/12",  // IPv4 private
		"192.168.0.0/16", // IPv4 private
		"fc00::/7",       // IPv6 Unique Local Addresses (ULA)
	}

	for _, cidr := range privateRanges {
		_, network, err := net.ParseCIDR(cidr)
		if err == nil {
			privateNetworks = append(privateNetworks, network)
		}
	}
}

// IAMIntegration defines the interface for IAM integration
type IAMIntegration interface {
	AuthenticateJWT(ctx context.Context, r *http.Request) (*IAMIdentity, s3err.ErrorCode)
	AuthorizeAction(ctx context.Context, identity *IAMIdentity, action Action, bucket string, objectKey string, r *http.Request) s3err.ErrorCode
	ValidateSessionToken(ctx context.Context, token string) (*sts.SessionInfo, error)
	ValidateTrustPolicyForPrincipal(ctx context.Context, roleArn, principalArn string) error
}

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

	// SECURITY NOTE: ParseJWTToken parses without cryptographic verification
	// This is SAFE because we only use the unverified claims to route to the correct
	// verification method. All code paths below perform full cryptographic verification:
	// - OIDC tokens: validated via validateExternalOIDCToken (line 98)
	// - STS tokens: validated via ValidateSessionToken (line 156)
	// The unverified issuer claim is only used for routing, never for authorization.
	tokenClaims, err := ParseUnverifiedJWTToken(sessionToken)
	if err != nil {
		glog.V(3).Infof("Failed to parse JWT token: %v", err)
		return nil, s3err.ErrAccessDenied
	}

	// Determine token type by issuer claim (more robust than checking role claim)
	// We use the unverified claims ONLY for routing to the correct verification method.
	// We DO NOT use these claims for building the identity.
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

		// Create claims map and populate with standard claims and attributes
		claims := make(map[string]interface{}, len(identity.Attributes)+5)

		// Add all attributes from the identity to the claims
		// This makes attributes like "preferred_username" available for policy substitution
		for k, v := range identity.Attributes {
			claims[k] = v
		}

		// Add standard OIDC fields to claims so they are available as variables
		// This ensures ${jwt:email}, ${jwt:name}, etc. work as documented in the wiki
		if identity.Email != "" {
			claims["email"] = identity.Email
		}
		if identity.DisplayName != "" {
			claims["name"] = identity.DisplayName
		}
		if len(identity.Groups) > 0 {
			claims["groups"] = identity.Groups
		}

		// Set critical claims explicitly, overwriting any from attributes to ensure correctness.
		claims["sub"] = identity.UserID
		claims["role"] = identity.RoleArn

		// Use real email address if available
		emailAddress := identity.UserID + "@oidc.local"
		if identity.Email != "" {
			emailAddress = identity.Email
		}

		displayName := identity.UserID
		if identity.DisplayName != "" {
			displayName = identity.DisplayName
		}

		// Return IAM identity for OIDC token
		return &IAMIdentity{
			Name:         identity.UserID,
			Principal:    identity.RoleArn,
			SessionToken: sessionToken,
			Account: &Account{
				DisplayName:  displayName,
				EmailAddress: emailAddress,
				Id:           identity.UserID,
			},
			Claims: claims,
		}, s3err.ErrNone
	}

	// This is an STS-issued token - validate with STS service
	// ValidateSessionToken performs cryptographic verification and extraction of trusted claims
	sessionInfo, err := s3iam.stsService.ValidateSessionToken(ctx, sessionToken)
	if err != nil {
		glog.V(3).Infof("STS session validation failed: %v", err)
		return nil, s3err.ErrAccessDenied
	}

	// Create claims map starting with request context (which holds custom claims)
	claims := make(map[string]interface{})
	if sessionInfo.RequestContext != nil {
		for k, v := range sessionInfo.RequestContext {
			claims[k] = v
		}
	}

	// Add standard claims
	claims["sub"] = sessionInfo.Subject
	claims["role"] = sessionInfo.RoleArn
	claims["principal"] = sessionInfo.Principal
	claims["snam"] = sessionInfo.SessionName

	// Create IAM identity from VALIDATED session info
	// We use the trusted data returned by the STS service, not the unverified token claims
	identity := &IAMIdentity{
		Name:         sessionInfo.Subject,
		Principal:    sessionInfo.Principal,
		SessionToken: sessionToken,
		Account: &Account{
			DisplayName:  sessionInfo.SessionName,
			EmailAddress: sessionInfo.Subject + "@seaweedfs.local",
			Id:           sessionInfo.Subject,
		},
		Claims: claims,
	}

	glog.V(3).Infof("JWT authentication successful for principal: %s", identity.Principal)
	return identity, s3err.ErrNone
}

// ValidateSessionToken checks the validity of an STS session token
func (s3iam *S3IAMIntegration) ValidateSessionToken(ctx context.Context, token string) (*sts.SessionInfo, error) {
	if s3iam.stsService == nil {
		return nil, fmt.Errorf("STS service not available")
	}
	return s3iam.stsService.ValidateSessionToken(ctx, token)
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

	// Add s3:prefix to request context based on object key
	// This ensures that policy conditions referencing s3:prefix (like StringLike)
	// work correctly for both ListObjects (where objectKey is the prefix) and
	// object operations (where we treat the object key as the prefix for matching)
	if objectKey != "" && objectKey != "/" {
		requestContext["s3:prefix"] = objectKey
	}

	// Add identity claims to request context for policy variables
	// Only add claim keys if they don't already exist (to avoid overwriting request-derived context)
	if identity.Claims != nil {
		for k, v := range identity.Claims {
			// Only add the claim if this key doesn't already exist in request context
			if _, exists := requestContext[k]; !exists {
				requestContext[k] = v
			}

			// If the claim doesn't have a namespace prefix (e.g. "email"), add "jwt:" prefix
			// This allows ${jwt:email} or ${jwt:preferred_username} to work
			// Only add namespaced version if it doesn't already exist
			if !strings.Contains(k, ":") {
				jwtKey := "jwt:" + k
				if _, exists := requestContext[jwtKey]; !exists {
					requestContext[jwtKey] = v
				}
			}
		}
	}

	// Determine the specific S3 action based on the HTTP request details
	specificAction := ResolveS3Action(r, string(action), bucket, objectKey)

	// Create action request
	actionRequest := &integration.ActionRequest{
		Principal:      identity.Principal,
		Action:         specificAction,
		Resource:       resourceArn,
		SessionToken:   identity.SessionToken,
		RequestContext: requestContext,
		PolicyNames:    identity.PolicyNames,
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

// ValidateTrustPolicyForPrincipal delegates to IAMManager to validate trust policy
func (s3iam *S3IAMIntegration) ValidateTrustPolicyForPrincipal(ctx context.Context, roleArn, principalArn string) error {
	if s3iam.iamManager == nil {
		return fmt.Errorf("IAM manager not available")
	}
	return s3iam.iamManager.ValidateTrustPolicyForPrincipal(ctx, roleArn, principalArn)
}

// IAMIdentity represents an authenticated identity with session information
type IAMIdentity struct {
	Name         string
	Principal    string
	SessionToken string
	Account      *Account
	PolicyNames  []string
	Claims       map[string]interface{}
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
	// Use AWS-compatible key name for policy variable substitution
	sourceIP := extractSourceIP(r)
	if sourceIP != "" {
		context["aws:SourceIp"] = sourceIP
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
// SECURITY: Prioritizes RemoteAddr over client-controlled headers to prevent spoofing
// Only trusts X-Forwarded-For/X-Real-IP if RemoteAddr appears to be from a trusted proxy
func extractSourceIP(r *http.Request) string {
	// Always start with RemoteAddr as the most trustworthy source
	remoteIP := r.RemoteAddr
	if ip, _, err := net.SplitHostPort(remoteIP); err == nil {
		remoteIP = ip
	}

	// NOTE: The current heuristic of using isPrivateIP assumes reverse proxies are on a
	// private/local network. This may be insufficient for some cloud, CDN, or multi-tier
	// proxy deployments where proxies terminate connections from public IPs. In such
	// environments, deployment-specific controls (e.g., network ACLs or proxy configs)
	// should be used to ensure only trusted components can set forwarding headers.
	// Future enhancements may introduce an explicit, configurable trusted proxy CIDR list.
	isTrustedProxy := isPrivateIP(remoteIP)

	if isTrustedProxy {
		// Check X-Real-IP header first (single IP, more reliable than X-Forwarded-For)
		if realIP := r.Header.Get("X-Real-IP"); realIP != "" {
			return strings.TrimSpace(realIP)
		}

		// Check X-Forwarded-For header (can contain multiple IPs, take the first one)
		if forwardedFor := r.Header.Get("X-Forwarded-For"); forwardedFor != "" {
			if ips := strings.Split(forwardedFor, ","); len(ips) > 0 {
				return strings.TrimSpace(ips[0])
			}
		}
	}

	// Fall back to RemoteAddr (most secure)
	return remoteIP
}

// isPrivateIP checks if an IP is in a private range (localhost or RFC1918)
func isPrivateIP(ipStr string) bool {
	ip := net.ParseIP(ipStr)
	if ip == nil {
		return false
	}

	// Check for localhost and link-local addresses (IPv4/IPv6)
	if ip.IsLoopback() || ip.IsLinkLocalUnicast() || ip.IsLinkLocalMulticast() {
		return true
	}

	// Check against pre-parsed private CIDR ranges
	for _, network := range privateNetworks {
		if network.Contains(ip) {
			return true
		}
	}

	return false
}

// ParseUnverifiedJWTToken parses a JWT token and returns its claims WITHOUT cryptographic verification
//
// SECURITY WARNING: This function does NOT validate the token signature!
// It should ONLY be used for:
// 1. Routing tokens to the appropriate verification method (e.g., checking issuer to determine STS vs OIDC)
// 2. Extracting claims for logging/debugging AFTER the token has been cryptographically verified
//
// NEVER use the returned claims for authorization decisions without first calling a proper
// verification function like ValidateSessionToken() or validateExternalOIDCToken().
func ParseUnverifiedJWTToken(tokenString string) (jwt.MapClaims, error) {
	// Parse token without verification to get claims
	// This token IS NOT VERIFIED at this stage.
	// It is only used to peek at claims (like issuer) to determine which verification key/strategy to use.
	token, _, err := new(jwt.Parser).ParseUnverified(tokenString, jwt.MapClaims{})
	if err != nil {
		return nil, err
	}

	if claims, ok := token.Claims.(jwt.MapClaims); ok {
		return claims, nil
	}

	return nil, fmt.Errorf("invalid token claims")
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
	iamIntegration IAMIntegration
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
		Actions:     []Action{}, // Empty - authorization handled by policy engine
		PolicyNames: iamIdentity.PolicyNames,
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
	UserID      string
	RoleArn     string
	Provider    string
	Email       string
	DisplayName string
	Groups      []string
	Attributes  map[string]string
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
		UserID:      externalIdentity.UserID,
		RoleArn:     roleArn,
		Provider:    fmt.Sprintf("%T", provider), // Use provider type as identifier
		Email:       externalIdentity.Email,
		DisplayName: externalIdentity.DisplayName,
		Groups:      externalIdentity.Groups,
		Attributes:  externalIdentity.Attributes,
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
