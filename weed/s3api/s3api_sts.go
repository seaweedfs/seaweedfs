package s3api

// This file provides STS (Security Token Service) HTTP endpoints for AWS SDK compatibility.
// It exposes AssumeRoleWithWebIdentity as an HTTP endpoint that can be used with
// AWS SDKs to obtain temporary credentials using OIDC/JWT tokens.

import (
	"encoding/xml"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/iam/integration"
	"github.com/seaweedfs/seaweedfs/weed/iam/providers"
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

	// Default account ID for federated users
	defaultAccountId = "111122223333"
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
	stsAdapter integration.STSAdapter
	iam        *IdentityAccessManagement
}

// NewSTSHandlers creates a new STSHandlers instance
func NewSTSHandlers(stsAdapter integration.STSAdapter, iam *IdentityAccessManagement) *STSHandlers {
	return &STSHandlers{
		stsAdapter: stsAdapter,
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
	glog.V(1).Infof("AssumeRoleWithWebIdentity called - not yet implemented (will be available when STS service is integrated)")
	
	h.writeSTSErrorResponse(w, r, STSErrNotImplemented, 
		fmt.Errorf("AssumeRoleWithWebIdentity requires full STS service integration which will be available in a future release. "+
			"Please use AssumeRole with existing IAM user credentials instead. "+
			"See documentation for alternative authentication methods"))
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

	// Use STSAdapter AssumeRole
	request := &integration.AssumeRoleRequest{
		RoleArn:         roleArn,
		SessionName:     roleSessionName,
		DurationSeconds: durationSeconds,
		Identity: &providers.ExternalIdentity{
			UserID:   identity.Name, // Use caller name as ID
			Provider: "seaweedfs",
		},
	}

	resp, err := h.stsAdapter.AssumeRole(r.Context(), request)
	if err != nil {
		h.writeSTSErrorResponse(w, r, STSErrAccessDenied, err)
		return
	}

	// Build and return response
	xmlResponse := &AssumeRoleResponse{
		Result: AssumeRoleResult{
			Credentials: STSCredentials{
				AccessKeyId:     resp.AccessKeyID,
				SecretAccessKey: resp.SecretAccessKey,
				SessionToken:    resp.SessionToken,
				Expiration:      resp.Expiration.Format(time.RFC3339),
			},
			// Fix response construction matching STSAdapter response
			AssumedRoleUser: &AssumedRoleUser{
				AssumedRoleId: roleArn + ":" + roleSessionName, // Approximated as STSAdapter doesn't return ID
				Arn:           roleArn, // Approximated
			},
		},
	}
	xmlResponse.ResponseMetadata.RequestId = fmt.Sprintf("%d", time.Now().UnixNano())

	// Log credentials in redacted form (security: never log SecretAccessKey)
	safeCreds := RedactSTSCredentials(xmlResponse.Result.Credentials)
	glog.V(2).Infof("AssumeRole successful for %s: AccessKeyId=%s, SessionToken=%s, Expiration=%s",
		identity.Name, safeCreds.AccessKeyId, safeCreds.SessionToken, safeCreds.Expiration)

	s3err.WriteXMLResponse(w, r, http.StatusOK, xmlResponse)
}

// handleAssumeRoleWithLDAPIdentity handles the AssumeRoleWithLDAPIdentity API action
func (h *STSHandlers) handleAssumeRoleWithLDAPIdentity(w http.ResponseWriter, r *http.Request) {
	glog.V(1).Infof("AssumeRoleWithLDAPIdentity called - not yet implemented (will be available when STS service is integrated)")
	
	h.writeSTSErrorResponse(w, r, STSErrNotImplemented, 
		fmt.Errorf("AssumeRoleWithLDAPIdentity requires full STS service integration which will be available in a future release. "+
			"Please use AssumeRole with existing IAM user credentials or configure LDAP as an identity provider. "+
			"See documentation for alternative authentication methods"))
}

// SafeSTSCredentials provides a logging-safe view of STS credentials with sensitive fields redacted
type SafeSTSCredentials struct {
	AccessKeyId  string
	SessionToken string // Redacted value shown for logging
	Expiration   string
}

// RedactSTSCredentials creates a safe-to-log version of STS credentials
func RedactSTSCredentials(creds STSCredentials) SafeSTSCredentials {
	// Show first 8 chars of session token for debugging, rest as asterisks
	redactedToken := "****"
	if len(creds.SessionToken) > 8 {
		redactedToken = creds.SessionToken[:8] + "****"
	}
	
	return SafeSTSCredentials{
		AccessKeyId:  creds.AccessKeyId,
		SessionToken: redactedToken,
		Expiration:   creds.Expiration,
		// SecretAccessKey intentionally omitted - never logged
	}
}

// Replaced prepareSTSCredentials with direct STSAdapter calls

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
	STSErrNotImplemented        STSErrorCode = "NotImplemented"
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
	STSErrNotImplemented:        {http.StatusNotImplemented, "Feature not yet implemented"},
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
