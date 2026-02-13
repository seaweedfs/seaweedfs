package s3api

// This file provides STS (Security Token Service) HTTP endpoints for AWS SDK compatibility.
// It exposes AssumeRoleWithWebIdentity as an HTTP endpoint that can be used with
// AWS SDKs to obtain temporary credentials using OIDC/JWT tokens.

import (
	"encoding/json"
	"encoding/xml"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/iam/ldap"
	"github.com/seaweedfs/seaweedfs/weed/iam/policy"
	"github.com/seaweedfs/seaweedfs/weed/iam/sts"
	"github.com/seaweedfs/seaweedfs/weed/iam/utils"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3err"
)

// STS API constants matching AWS STS specification
const (
	stsAPIVersion       = "2011-06-15"
	stsAction           = "Action"
	stsVersion          = "Version"
	stsWebIdentityToken = "WebIdentityToken"
	stsRoleArn          = "RoleArn"
	stsRoleSessionName  = "RoleSessionName"
	stsDurationSeconds  = "DurationSeconds"

	// STS Action names
	actionAssumeRole                 = "AssumeRole"
	actionAssumeRoleWithWebIdentity  = "AssumeRoleWithWebIdentity"
	actionAssumeRoleWithLDAPIdentity = "AssumeRoleWithLDAPIdentity"

	// LDAP parameter names
	stsLDAPUsername     = "LDAPUsername"
	stsLDAPPassword     = "LDAPPassword"
	stsLDAPProviderName = "LDAPProviderName"
)

// STS duration constants (AWS specification)
const (
	minDurationSeconds = int64(900)   // 15 minutes
	maxDurationSeconds = int64(43200) // 12 hours
)

// parseDurationSeconds parses and validates the DurationSeconds parameter
// Returns nil if the parameter is not provided, or a pointer to the parsed value
func parseDurationSeconds(r *http.Request) (*int64, STSErrorCode, error) {
	dsStr := r.FormValue("DurationSeconds")
	if dsStr == "" {
		return nil, "", nil
	}

	ds, err := strconv.ParseInt(dsStr, 10, 64)
	if err != nil {
		return nil, STSErrInvalidParameterValue, fmt.Errorf("invalid DurationSeconds: %w", err)
	}

	if ds < minDurationSeconds || ds > maxDurationSeconds {
		return nil, STSErrInvalidParameterValue,
			fmt.Errorf("DurationSeconds must be between %d and %d seconds", minDurationSeconds, maxDurationSeconds)
	}

	return &ds, "", nil
}

// Removed generateSecureCredentials - now using STS service's JWT token generation
// The STS service generates proper JWT tokens with embedded claims that can be validated
// across distributed instances without shared state.

// STSHandlers provides HTTP handlers for STS operations
type STSHandlers struct {
	stsService *sts.STSService
	iam        *IdentityAccessManagement
}

// NewSTSHandlers creates a new STSHandlers instance
func NewSTSHandlers(stsService *sts.STSService, iam *IdentityAccessManagement) *STSHandlers {
	return &STSHandlers{
		stsService: stsService,
		iam:        iam,
	}
}

func (h *STSHandlers) getAccountID() string {
	if h.stsService != nil && h.stsService.Config != nil && h.stsService.Config.AccountId != "" {
		return h.stsService.Config.AccountId
	}
	return defaultAccountID
}

// HandleSTSRequest is the main entry point for STS requests
// It routes requests based on the Action parameter
func (h *STSHandlers) HandleSTSRequest(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		h.writeSTSErrorResponse(w, r, STSErrInvalidParameterValue, err)
		return
	}

	// Validate API version
	version := r.Form.Get(stsVersion)
	if version != "" && version != stsAPIVersion {
		h.writeSTSErrorResponse(w, r, STSErrInvalidParameterValue,
			fmt.Errorf("invalid STS API version %s, expecting %s", version, stsAPIVersion))
		return
	}

	// Route based on action
	action := r.Form.Get(stsAction)
	switch action {
	case actionAssumeRole:
		h.handleAssumeRole(w, r)
	case actionAssumeRoleWithWebIdentity:
		h.handleAssumeRoleWithWebIdentity(w, r)
	case actionAssumeRoleWithLDAPIdentity:
		h.handleAssumeRoleWithLDAPIdentity(w, r)
	default:
		h.writeSTSErrorResponse(w, r, STSErrInvalidAction,
			fmt.Errorf("unsupported action: %s", action))
	}
}

// handleAssumeRoleWithWebIdentity handles the AssumeRoleWithWebIdentity API action
func (h *STSHandlers) handleAssumeRoleWithWebIdentity(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	// Extract parameters from form (supports both query and POST body)
	roleArn := r.FormValue("RoleArn")
	webIdentityToken := r.FormValue("WebIdentityToken")
	roleSessionName := r.FormValue("RoleSessionName")

	// Validate required parameters
	if webIdentityToken == "" {
		h.writeSTSErrorResponse(w, r, STSErrMissingParameter,
			fmt.Errorf("WebIdentityToken is required"))
		return
	}

	if roleArn == "" {
		h.writeSTSErrorResponse(w, r, STSErrMissingParameter,
			fmt.Errorf("RoleArn is required"))
		return
	}

	if roleSessionName == "" {
		h.writeSTSErrorResponse(w, r, STSErrMissingParameter,
			fmt.Errorf("RoleSessionName is required"))
		return
	}

	// Parse and validate DurationSeconds using helper
	durationSeconds, errCode, err := parseDurationSeconds(r)
	if err != nil {
		h.writeSTSErrorResponse(w, r, errCode, err)
		return
	}

	// Check if STS service is initialized
	if h.stsService == nil || !h.stsService.IsInitialized() {
		h.writeSTSErrorResponse(w, r, STSErrSTSNotReady,
			fmt.Errorf("STS service not initialized"))
		return
	}

	// Build request for STS service
	request := &sts.AssumeRoleWithWebIdentityRequest{
		RoleArn:          roleArn,
		WebIdentityToken: webIdentityToken,
		RoleSessionName:  roleSessionName,
		DurationSeconds:  durationSeconds,
	}

	// Call STS service
	response, err := h.stsService.AssumeRoleWithWebIdentity(ctx, request)
	if err != nil {
		glog.V(2).Infof("AssumeRoleWithWebIdentity failed: %v", err)

		// Use typed errors for robust error checking
		// This decouples HTTP layer from service implementation details
		errCode := STSErrAccessDenied
		if errors.Is(err, sts.ErrTypedTokenExpired) {
			errCode = STSErrExpiredToken
		} else if errors.Is(err, sts.ErrTypedInvalidToken) {
			errCode = STSErrInvalidParameterValue
		} else if errors.Is(err, sts.ErrTypedInvalidIssuer) {
			errCode = STSErrInvalidParameterValue
		} else if errors.Is(err, sts.ErrTypedInvalidAudience) {
			errCode = STSErrInvalidParameterValue
		} else if errors.Is(err, sts.ErrTypedMissingClaims) {
			errCode = STSErrInvalidParameterValue
		}

		h.writeSTSErrorResponse(w, r, errCode, err)
		return
	}

	// Build and return XML response
	xmlResponse := &AssumeRoleWithWebIdentityResponse{
		Result: WebIdentityResult{
			Credentials: STSCredentials{
				AccessKeyId:     response.Credentials.AccessKeyId,
				SecretAccessKey: response.Credentials.SecretAccessKey,
				SessionToken:    response.Credentials.SessionToken,
				Expiration:      response.Credentials.Expiration.Format(time.RFC3339),
			},
			SubjectFromWebIdentityToken: response.AssumedRoleUser.Subject,
		},
	}
	xmlResponse.ResponseMetadata.RequestId = fmt.Sprintf("%d", time.Now().UnixNano())

	s3err.WriteXMLResponse(w, r, http.StatusOK, xmlResponse)
}

// handleAssumeRole handles the AssumeRole API action
// This requires AWS Signature V4 authentication
// NOTE: Session policy support (Policy parameter) is implemented for AssumeRole.
// AssumeRoleWithWebIdentity and AssumeRoleWithLDAPIdentity do not currently support
// inline session policies. This can be extended in future work if needed.
func (h *STSHandlers) handleAssumeRole(w http.ResponseWriter, r *http.Request) {
	// Extract parameters from form
	roleArn := r.FormValue("RoleArn")
	roleSessionName := r.FormValue("RoleSessionName")

	// Validate required parameters
	if roleArn == "" {
		h.writeSTSErrorResponse(w, r, STSErrMissingParameter,
			fmt.Errorf("RoleArn is required"))
		return
	}

	if roleSessionName == "" {
		h.writeSTSErrorResponse(w, r, STSErrMissingParameter,
			fmt.Errorf("RoleSessionName is required"))
		return
	}

	// Parse and validate DurationSeconds using helper
	durationSeconds, errCode, err := parseDurationSeconds(r)
	if err != nil {
		h.writeSTSErrorResponse(w, r, errCode, err)
		return
	}

	// Check if STS service is initialized
	if h.stsService == nil || !h.stsService.IsInitialized() {
		h.writeSTSErrorResponse(w, r, STSErrSTSNotReady,
			fmt.Errorf("STS service not initialized"))
		return
	}

	// Check if IAM is available for SigV4 verification
	if h.iam == nil {
		h.writeSTSErrorResponse(w, r, STSErrSTSNotReady,
			fmt.Errorf("IAM not configured for STS"))
		return
	}

	// Validate AWS SigV4 authentication
	identity, _, _, _, sigErrCode := h.iam.verifyV4Signature(r, false)
	if sigErrCode != s3err.ErrNone {
		glog.V(2).Infof("AssumeRole SigV4 verification failed: %v", sigErrCode)
		h.writeSTSErrorResponse(w, r, STSErrAccessDenied,
			fmt.Errorf("invalid AWS signature: %v", sigErrCode))
		return
	}

	if identity == nil {
		h.writeSTSErrorResponse(w, r, STSErrAccessDenied,
			fmt.Errorf("unable to identify caller"))
		return
	}

	glog.V(2).Infof("AssumeRole: caller identity=%s, roleArn=%s, sessionName=%s",
		identity.Name, roleArn, roleSessionName)

	// Check if the caller is authorized to assume the role (sts:AssumeRole permission)
	// This validates that the caller has a policy allowing sts:AssumeRole on the target role
	if authErr := h.iam.VerifyActionPermission(r, identity, Action("sts:AssumeRole"), "", roleArn); authErr != s3err.ErrNone {
		glog.V(2).Infof("AssumeRole: caller %s is not authorized to assume role %s", identity.Name, roleArn)
		h.writeSTSErrorResponse(w, r, STSErrAccessDenied,
			fmt.Errorf("user %s is not authorized to assume role %s", identity.Name, roleArn))
		return
	}

	// Validate that the target role trusts the caller (Trust Policy)
	// This ensures the role's trust policy explicitly allows the principal to assume it
	if err := h.iam.ValidateTrustPolicyForPrincipal(r.Context(), roleArn, identity.PrincipalArn); err != nil {
		glog.V(2).Infof("AssumeRole: trust policy validation failed for %s to assume %s: %v", identity.Name, roleArn, err)
		h.writeSTSErrorResponse(w, r, STSErrAccessDenied, fmt.Errorf("trust policy denies access"))
		return
	}

	// Parse optional inline session policy for downscoping
	sessionPolicyJSON := ""
	policyParam := strings.TrimSpace(r.FormValue("Policy"))
	if policyParam != "" {
		var policyDoc policy.PolicyDocument
		if err := json.Unmarshal([]byte(policyParam), &policyDoc); err != nil {
			h.writeSTSErrorResponse(w, r, STSErrMalformedPolicyDocument,
				fmt.Errorf("invalid Policy JSON: %w", err))
			return
		}
		if err := policy.ValidatePolicyDocument(&policyDoc); err != nil {
			h.writeSTSErrorResponse(w, r, STSErrMalformedPolicyDocument,
				fmt.Errorf("invalid Policy document: %w", err))
			return
		}
		normalized, err := json.Marshal(&policyDoc)
		if err != nil {
			h.writeSTSErrorResponse(w, r, STSErrInternalError,
				fmt.Errorf("failed to normalize Policy document: %w", err))
			return
		}
		sessionPolicyJSON = string(normalized)
	}

	// Generate common STS components
	stsCreds, assumedUser, err := h.prepareSTSCredentials(roleArn, roleSessionName, durationSeconds, sessionPolicyJSON, nil)
	if err != nil {
		h.writeSTSErrorResponse(w, r, STSErrInternalError, err)
		return
	}

	// Build and return response
	xmlResponse := &AssumeRoleResponse{
		Result: AssumeRoleResult{
			Credentials:     stsCreds,
			AssumedRoleUser: assumedUser,
		},
	}
	xmlResponse.ResponseMetadata.RequestId = fmt.Sprintf("%d", time.Now().UnixNano())

	s3err.WriteXMLResponse(w, r, http.StatusOK, xmlResponse)
}

// handleAssumeRoleWithLDAPIdentity handles the AssumeRoleWithLDAPIdentity API action
func (h *STSHandlers) handleAssumeRoleWithLDAPIdentity(w http.ResponseWriter, r *http.Request) {
	// Extract parameters from form
	roleArn := r.FormValue("RoleArn")
	roleSessionName := r.FormValue("RoleSessionName")
	ldapUsername := r.FormValue(stsLDAPUsername)
	ldapPassword := r.FormValue(stsLDAPPassword)

	// Validate required parameters
	if roleArn == "" {
		h.writeSTSErrorResponse(w, r, STSErrMissingParameter,
			fmt.Errorf("RoleArn is required"))
		return
	}

	if roleSessionName == "" {
		h.writeSTSErrorResponse(w, r, STSErrMissingParameter,
			fmt.Errorf("RoleSessionName is required"))
		return
	}

	if ldapUsername == "" {
		h.writeSTSErrorResponse(w, r, STSErrMissingParameter,
			fmt.Errorf("LDAPUsername is required"))
		return
	}

	if ldapPassword == "" {
		h.writeSTSErrorResponse(w, r, STSErrMissingParameter,
			fmt.Errorf("LDAPPassword is required"))
		return
	}

	// Parse and validate DurationSeconds using helper
	durationSeconds, errCode, err := parseDurationSeconds(r)
	if err != nil {
		h.writeSTSErrorResponse(w, r, errCode, err)
		return
	}

	// Check if STS service is initialized
	if h.stsService == nil || !h.stsService.IsInitialized() {
		h.writeSTSErrorResponse(w, r, STSErrSTSNotReady,
			fmt.Errorf("STS service not initialized"))
		return
	}

	// Optional: specific LDAP provider name
	ldapProviderName := r.FormValue(stsLDAPProviderName)

	// Find an LDAP provider from the registered providers
	var ldapProvider *ldap.LDAPProvider
	ldapProvidersFound := 0
	for _, provider := range h.stsService.GetProviders() {
		// Check if this is an LDAP provider by type assertion
		if p, ok := provider.(*ldap.LDAPProvider); ok {
			if ldapProviderName != "" && p.Name() == ldapProviderName {
				ldapProvider = p
				break
			} else if ldapProviderName == "" && ldapProvider == nil {
				ldapProvider = p
			}
			ldapProvidersFound++
		}
	}

	if ldapProvidersFound > 1 && ldapProviderName == "" {
		glog.Warningf("Multiple LDAP providers found (%d). Using the first one found (non-deterministic). Consider specifying LDAPProviderName.", ldapProvidersFound)
	}

	if ldapProvider == nil {
		glog.V(2).Infof("AssumeRoleWithLDAPIdentity: no LDAP provider configured")
		h.writeSTSErrorResponse(w, r, STSErrSTSNotReady,
			fmt.Errorf("no LDAP provider configured - please add an LDAP provider to IAM configuration"))
		return
	}

	// Authenticate with LDAP provider
	// The provider expects credentials in "username:password" format
	credentials := ldapUsername + ":" + ldapPassword
	identity, err := ldapProvider.Authenticate(r.Context(), credentials)
	if err != nil {
		glog.V(2).Infof("AssumeRoleWithLDAPIdentity: LDAP authentication failed for user %s: %v", ldapUsername, err)
		h.writeSTSErrorResponse(w, r, STSErrAccessDenied,
			fmt.Errorf("authentication failed"))
		return
	}

	glog.V(2).Infof("AssumeRoleWithLDAPIdentity: user %s authenticated successfully, groups=%v",
		ldapUsername, identity.Groups)

	accountID := h.getAccountID()

	ldapUserIdentity := &Identity{
		Name: identity.UserID,
		Account: &Account{
			DisplayName:  identity.DisplayName,
			EmailAddress: identity.Email,
			Id:           identity.UserID,
		},
		PrincipalArn: fmt.Sprintf("arn:aws:iam::%s:user/%s", accountID, identity.UserID),
	}

	// Verify that the identity is allowed to assume the role by checking the Trust Policy
	// The LDAP user doesn't have identity policies, so we strictly check if the Role trusts this principal.
	if err := h.iam.ValidateTrustPolicyForPrincipal(r.Context(), roleArn, ldapUserIdentity.PrincipalArn); err != nil {
		glog.V(2).Infof("AssumeRoleWithLDAPIdentity: trust policy validation failed for %s to assume %s: %v", ldapUsername, roleArn, err)
		h.writeSTSErrorResponse(w, r, STSErrAccessDenied, fmt.Errorf("trust policy denies access"))
		return
	}

	// Generate common STS components with LDAP-specific claims
	modifyClaims := func(claims *sts.STSSessionClaims) {
		claims.WithIdentityProvider("ldap", identity.UserID, identity.Provider)
	}

	stsCreds, assumedUser, err := h.prepareSTSCredentials(roleArn, roleSessionName, durationSeconds, "", modifyClaims)
	if err != nil {
		h.writeSTSErrorResponse(w, r, STSErrInternalError, err)
		return
	}

	// Build and return response
	xmlResponse := &AssumeRoleWithLDAPIdentityResponse{
		Result: LDAPIdentityResult{
			Credentials:     stsCreds,
			AssumedRoleUser: assumedUser,
		},
	}
	xmlResponse.ResponseMetadata.RequestId = fmt.Sprintf("%d", time.Now().UnixNano())

	s3err.WriteXMLResponse(w, r, http.StatusOK, xmlResponse)
}

// prepareSTSCredentials extracts common shared logic for credential generation
func (h *STSHandlers) prepareSTSCredentials(roleArn, roleSessionName string,
	durationSeconds *int64, sessionPolicy string, modifyClaims func(*sts.STSSessionClaims)) (STSCredentials, *AssumedRoleUser, error) {

	// Calculate duration
	duration := time.Hour // Default 1 hour
	if durationSeconds != nil {
		duration = time.Duration(*durationSeconds) * time.Second
	}

	// Generate session ID
	sessionId, err := sts.GenerateSessionId()
	if err != nil {
		return STSCredentials{}, nil, fmt.Errorf("failed to generate session ID: %w", err)
	}

	expiration := time.Now().Add(duration)

	// Extract role name from ARN for proper response formatting
	roleName := utils.ExtractRoleNameFromArn(roleArn)
	if roleName == "" {
		roleName = roleArn // Fallback to full ARN if extraction fails
	}

	accountID := h.getAccountID()

	// Construct AssumedRoleUser ARN - this will be used as the principal for the vended token
	assumedRoleArn := fmt.Sprintf("arn:aws:sts::%s:assumed-role/%s/%s", accountID, roleName, roleSessionName)

	// Create session claims with role information
	// SECURITY: Use the assumedRoleArn as the principal in the token.
	// This ensures that subsequent requests using this token are correctly identified as the assumed role.
	claims := sts.NewSTSSessionClaims(sessionId, h.stsService.Config.Issuer, expiration).
		WithSessionName(roleSessionName).
		WithRoleInfo(roleArn, fmt.Sprintf("%s:%s", roleName, roleSessionName), assumedRoleArn)

	if sessionPolicy != "" {
		claims.WithSessionPolicy(sessionPolicy)
	}

	// Apply custom claims if provided (e.g., LDAP identity)
	if modifyClaims != nil {
		modifyClaims(claims)
	}

	// Generate JWT session token
	sessionToken, err := h.stsService.GetTokenGenerator().GenerateJWTWithClaims(claims)
	if err != nil {
		return STSCredentials{}, nil, fmt.Errorf("failed to generate session token: %w", err)
	}

	// Generate temporary credentials (deterministic based on sessionId)
	stsCredGen := sts.NewCredentialGenerator()
	stsCredsDet, err := stsCredGen.GenerateTemporaryCredentials(sessionId, expiration)
	if err != nil {
		return STSCredentials{}, nil, fmt.Errorf("failed to generate temporary credentials: %w", err)
	}
	accessKeyId := stsCredsDet.AccessKeyId
	secretAccessKey := stsCredsDet.SecretAccessKey

	stsCreds := STSCredentials{
		AccessKeyId:     accessKeyId,
		SecretAccessKey: secretAccessKey,
		SessionToken:    sessionToken,
		Expiration:      expiration.Format(time.RFC3339),
	}

	assumedUser := &AssumedRoleUser{
		AssumedRoleId: fmt.Sprintf("%s:%s", roleName, roleSessionName),
		Arn:           assumedRoleArn,
	}

	return stsCreds, assumedUser, nil
}

// STS Response types for XML marshaling

// AssumeRoleWithWebIdentityResponse is the response for AssumeRoleWithWebIdentity
type AssumeRoleWithWebIdentityResponse struct {
	XMLName          xml.Name          `xml:"https://sts.amazonaws.com/doc/2011-06-15/ AssumeRoleWithWebIdentityResponse"`
	Result           WebIdentityResult `xml:"AssumeRoleWithWebIdentityResult"`
	ResponseMetadata struct {
		RequestId string `xml:"RequestId,omitempty"`
	} `xml:"ResponseMetadata,omitempty"`
}

// WebIdentityResult contains the result of AssumeRoleWithWebIdentity
type WebIdentityResult struct {
	Credentials                 STSCredentials   `xml:"Credentials"`
	SubjectFromWebIdentityToken string           `xml:"SubjectFromWebIdentityToken,omitempty"`
	AssumedRoleUser             *AssumedRoleUser `xml:"AssumedRoleUser,omitempty"`
}

// STSCredentials represents temporary security credentials
type STSCredentials struct {
	AccessKeyId     string `xml:"AccessKeyId"`
	SecretAccessKey string `xml:"SecretAccessKey"`
	SessionToken    string `xml:"SessionToken"`
	Expiration      string `xml:"Expiration"`
}

// AssumedRoleUser contains information about the assumed role
type AssumedRoleUser struct {
	AssumedRoleId string `xml:"AssumedRoleId"`
	Arn           string `xml:"Arn"`
}

// AssumeRoleResponse is the response for AssumeRole
type AssumeRoleResponse struct {
	XMLName          xml.Name         `xml:"https://sts.amazonaws.com/doc/2011-06-15/ AssumeRoleResponse"`
	Result           AssumeRoleResult `xml:"AssumeRoleResult"`
	ResponseMetadata struct {
		RequestId string `xml:"RequestId,omitempty"`
	} `xml:"ResponseMetadata,omitempty"`
}

// AssumeRoleResult contains the result of AssumeRole
type AssumeRoleResult struct {
	Credentials     STSCredentials   `xml:"Credentials"`
	AssumedRoleUser *AssumedRoleUser `xml:"AssumedRoleUser,omitempty"`
}

// AssumeRoleWithLDAPIdentityResponse is the response for AssumeRoleWithLDAPIdentity
type AssumeRoleWithLDAPIdentityResponse struct {
	XMLName          xml.Name           `xml:"https://sts.amazonaws.com/doc/2011-06-15/ AssumeRoleWithLDAPIdentityResponse"`
	Result           LDAPIdentityResult `xml:"AssumeRoleWithLDAPIdentityResult"`
	ResponseMetadata struct {
		RequestId string `xml:"RequestId,omitempty"`
	} `xml:"ResponseMetadata,omitempty"`
}

// LDAPIdentityResult contains the result of AssumeRoleWithLDAPIdentity
type LDAPIdentityResult struct {
	Credentials     STSCredentials   `xml:"Credentials"`
	AssumedRoleUser *AssumedRoleUser `xml:"AssumedRoleUser,omitempty"`
}

// STS Error types

// STSErrorCode represents STS error codes
type STSErrorCode string

const (
	STSErrAccessDenied            STSErrorCode = "AccessDenied"
	STSErrExpiredToken            STSErrorCode = "ExpiredTokenException"
	STSErrInvalidAction           STSErrorCode = "InvalidAction"
	STSErrInvalidParameterValue   STSErrorCode = "InvalidParameterValue"
	STSErrMalformedPolicyDocument STSErrorCode = "MalformedPolicyDocument"
	STSErrMissingParameter        STSErrorCode = "MissingParameter"
	STSErrSTSNotReady             STSErrorCode = "ServiceUnavailable"
	STSErrInternalError           STSErrorCode = "InternalError"
)

// stsErrorResponses maps error codes to HTTP status and messages
var stsErrorResponses = map[STSErrorCode]struct {
	HTTPStatusCode int
	Message        string
}{
	STSErrAccessDenied:            {http.StatusForbidden, "Access Denied"},
	STSErrExpiredToken:            {http.StatusBadRequest, "Token has expired"},
	STSErrInvalidAction:           {http.StatusBadRequest, "Invalid action"},
	STSErrInvalidParameterValue:   {http.StatusBadRequest, "Invalid parameter value"},
	STSErrMalformedPolicyDocument: {http.StatusBadRequest, "Malformed policy document"},
	STSErrMissingParameter:        {http.StatusBadRequest, "Missing required parameter"},
	STSErrSTSNotReady:             {http.StatusServiceUnavailable, "STS service not ready"},
	STSErrInternalError:           {http.StatusInternalServerError, "Internal error"},
}

// STSErrorResponse is the XML error response format
type STSErrorResponse struct {
	XMLName xml.Name `xml:"https://sts.amazonaws.com/doc/2011-06-15/ ErrorResponse"`
	Error   struct {
		Type    string `xml:"Type"`
		Code    string `xml:"Code"`
		Message string `xml:"Message"`
	} `xml:"Error"`
	RequestId string `xml:"RequestId"`
}

// writeSTSErrorResponse writes an STS error response
func (h *STSHandlers) writeSTSErrorResponse(w http.ResponseWriter, r *http.Request, code STSErrorCode, err error) {
	errInfo, ok := stsErrorResponses[code]
	if !ok {
		errInfo = stsErrorResponses[STSErrInternalError]
	}

	message := errInfo.Message
	if err != nil {
		message = err.Error()
	}

	response := STSErrorResponse{
		RequestId: fmt.Sprintf("%d", time.Now().UnixNano()),
	}

	// Server-side errors use "Receiver" type per AWS spec
	if code == STSErrInternalError || code == STSErrSTSNotReady {
		response.Error.Type = "Receiver"
	} else {
		response.Error.Type = "Sender"
	}

	response.Error.Code = string(code)
	response.Error.Message = message

	glog.V(1).Infof("STS error response: code=%s, type=%s, message=%s", code, response.Error.Type, message)
	s3err.WriteXMLResponse(w, r, errInfo.HTTPStatusCode, response)
}
