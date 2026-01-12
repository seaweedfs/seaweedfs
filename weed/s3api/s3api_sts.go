package s3api

// This file provides STS (Security Token Service) HTTP endpoints for AWS SDK compatibility.
// It exposes AssumeRoleWithWebIdentity as an HTTP endpoint that can be used with
// AWS SDKs to obtain temporary credentials using OIDC/JWT tokens.

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/xml"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/iam/ldap"
	"github.com/seaweedfs/seaweedfs/weed/iam/sts"
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
	stsLDAPUsername = "LDAPUsername"
	stsLDAPPassword = "LDAPPassword"
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

// generateSecureCredentials generates cryptographically secure temporary credentials
func generateSecureCredentials(userPrefix string, duration time.Duration) (accessKey, secretKey, sessionToken string, expiration time.Time, err error) {
	// Generate access key with prefix and random suffix
	accessKeyBytes := make([]byte, 10)
	if _, err = rand.Read(accessKeyBytes); err != nil {
		return "", "", "", time.Time{}, fmt.Errorf("failed to generate access key: %w", err)
	}
	// Use ASIA prefix for temporary credentials (AWS convention)
	prefixLen := len(userPrefix)
	if prefixLen > 4 {
		prefixLen = 4
	}
	accessKey = fmt.Sprintf("ASIA%s%s", strings.ToUpper(userPrefix[:prefixLen]), hex.EncodeToString(accessKeyBytes)[:12])

	// Generate cryptographically secure secret key (40 hex characters = 20 bytes)
	secretKeyBytes := make([]byte, 20)
	if _, err = rand.Read(secretKeyBytes); err != nil {
		return "", "", "", time.Time{}, fmt.Errorf("failed to generate secret key: %w", err)
	}
	secretKey = hex.EncodeToString(secretKeyBytes)

	// Generate session token (64 hex characters = 32 bytes)
	sessionTokenBytes := make([]byte, 32)
	if _, err = rand.Read(sessionTokenBytes); err != nil {
		return "", "", "", time.Time{}, fmt.Errorf("failed to generate session token: %w", err)
	}
	sessionToken = hex.EncodeToString(sessionTokenBytes)

	expiration = time.Now().Add(duration)
	return accessKey, secretKey, sessionToken, expiration, nil
}

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

	// Calculate duration
	duration := time.Hour // Default 1 hour
	if durationSeconds != nil {
		duration = time.Duration(*durationSeconds) * time.Second
	}

	// Generate cryptographically secure temporary credentials
	tempAccessKey, tempSecretKey, sessionToken, expiration, err := generateSecureCredentials(identity.Name, duration)
	if err != nil {
		glog.Errorf("AssumeRole: failed to generate credentials: %v", err)
		h.writeSTSErrorResponse(w, r, STSErrInternalError, err)
		return
	}

	// Build and return response
	xmlResponse := &AssumeRoleResponse{
		Result: AssumeRoleResult{
			Credentials: STSCredentials{
				AccessKeyId:     tempAccessKey,
				SecretAccessKey: tempSecretKey,
				SessionToken:    sessionToken,
				Expiration:      expiration.Format(time.RFC3339),
			},
			AssumedRoleUser: &AssumedRoleUser{
				AssumedRoleId: fmt.Sprintf("%s:%s", roleArn, roleSessionName),
				Arn:           fmt.Sprintf("arn:aws:sts::assumed-role/%s/%s", roleArn, roleSessionName),
			},
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

	// Find an LDAP provider from the registered providers
	// Find an LDAP provider from the registered providers
	var ldapProvider *ldap.LDAPProvider
	for _, provider := range h.stsService.GetProviders() {
		// Check if this is an LDAP provider by type assertion
		if p, ok := provider.(*ldap.LDAPProvider); ok {
			ldapProvider = p
			break
		}
	}

	if ldapProvider == nil {
		glog.V(2).Infof("AssumeRoleWithLDAPIdentity: no LDAP provider configured")
		h.writeSTSErrorResponse(w, r, STSErrAccessDenied,
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
			fmt.Errorf("LDAP authentication failed: %v", err))
		return
	}

	glog.V(2).Infof("AssumeRoleWithLDAPIdentity: user %s authenticated successfully, groups=%v",
		ldapUsername, identity.Groups)

	// Verify that the identity is allowed to assume the role
	// We create a temporary identity to represent the LDAP user for permission checking
	// The checking logic will verify if the role's trust policy allows this principal
	ldapUserIdentity := &Identity{
		Name: identity.UserID,
		Account: &Account{
			DisplayName:  identity.DisplayName,
			EmailAddress: identity.Email,
			Id:           identity.UserID,
		},
		PrincipalArn: fmt.Sprintf("arn:aws:iam::%s:user/%s", "aws", identity.UserID),
	}

	if authErr := h.iam.VerifyActionPermission(r, ldapUserIdentity, Action("sts:AssumeRole"), "", roleArn); authErr != s3err.ErrNone {
		glog.V(2).Infof("AssumeRoleWithLDAPIdentity: authorization failed for %s to assume %s: %v", ldapUsername, roleArn, authErr)
		h.writeSTSErrorResponse(w, r, STSErrAccessDenied, fmt.Errorf("access denied"))
		return
	}

	// Calculate duration
	duration := time.Hour // Default 1 hour
	if durationSeconds != nil {
		duration = time.Duration(*durationSeconds) * time.Second
	}

	// Generate cryptographically secure temporary credentials
	tempAccessKey, tempSecretKey, sessionToken, expiration, err := generateSecureCredentials(ldapUsername, duration)
	if err != nil {
		glog.Errorf("AssumeRoleWithLDAPIdentity: failed to generate credentials: %v", err)
		h.writeSTSErrorResponse(w, r, STSErrInternalError, err)
		return
	}

	// Build and return response
	xmlResponse := &AssumeRoleWithLDAPIdentityResponse{
		Result: LDAPIdentityResult{
			Credentials: STSCredentials{
				AccessKeyId:     tempAccessKey,
				SecretAccessKey: tempSecretKey,
				SessionToken:    sessionToken,
				Expiration:      expiration.Format(time.RFC3339),
			},
			AssumedRoleUser: &AssumedRoleUser{
				AssumedRoleId: fmt.Sprintf("%s:%s", roleArn, roleSessionName),
				Arn:           fmt.Sprintf("arn:aws:sts::assumed-role/%s/%s", roleArn, roleSessionName),
			},
		},
	}
	xmlResponse.ResponseMetadata.RequestId = fmt.Sprintf("%d", time.Now().UnixNano())

	s3err.WriteXMLResponse(w, r, http.StatusOK, xmlResponse)
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
	STSErrAccessDenied          STSErrorCode = "AccessDenied"
	STSErrExpiredToken          STSErrorCode = "ExpiredTokenException"
	STSErrInvalidAction         STSErrorCode = "InvalidAction"
	STSErrInvalidParameterValue STSErrorCode = "InvalidParameterValue"
	STSErrMissingParameter      STSErrorCode = "MissingParameter"
	STSErrSTSNotReady           STSErrorCode = "ServiceUnavailable"
	STSErrInternalError         STSErrorCode = "InternalError"
)

// stsErrorResponses maps error codes to HTTP status and messages
var stsErrorResponses = map[STSErrorCode]struct {
	HTTPStatusCode int
	Message        string
}{
	STSErrAccessDenied:          {http.StatusForbidden, "Access Denied"},
	STSErrExpiredToken:          {http.StatusBadRequest, "Token has expired"},
	STSErrInvalidAction:         {http.StatusBadRequest, "Invalid action"},
	STSErrInvalidParameterValue: {http.StatusBadRequest, "Invalid parameter value"},
	STSErrMissingParameter:      {http.StatusBadRequest, "Missing required parameter"},
	STSErrSTSNotReady:           {http.StatusServiceUnavailable, "STS service not ready"},
	STSErrInternalError:         {http.StatusInternalServerError, "Internal error"},
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
