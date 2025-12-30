package s3api

// This file provides STS (Security Token Service) HTTP endpoints for AWS SDK compatibility.
// It exposes AssumeRoleWithWebIdentity as an HTTP endpoint that can be used with
// AWS SDKs to obtain temporary credentials using OIDC/JWT tokens.

import (
	"encoding/xml"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
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
	actionAssumeRoleWithWebIdentity = "AssumeRoleWithWebIdentity"
)

// STSHandlers provides HTTP handlers for STS operations
type STSHandlers struct {
	stsService *sts.STSService
}

// NewSTSHandlers creates a new STSHandlers instance
func NewSTSHandlers(stsService *sts.STSService) *STSHandlers {
	return &STSHandlers{
		stsService: stsService,
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
	case actionAssumeRoleWithWebIdentity:
		h.handleAssumeRoleWithWebIdentity(w, r)
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

	// Parse and validate DurationSeconds
	var durationSeconds *int64
	if dsStr := r.FormValue("DurationSeconds"); dsStr != "" {
		ds, err := strconv.ParseInt(dsStr, 10, 64)
		if err != nil {
			h.writeSTSErrorResponse(w, r, STSErrInvalidParameterValue,
				fmt.Errorf("invalid DurationSeconds: %w", err))
			return
		}

		// Enforce AWS STS-compatible duration range for AssumeRoleWithWebIdentity
		// AWS allows 900 seconds (15 minutes) to 43200 seconds (12 hours)
		const (
			minDurationSeconds = int64(900)
			maxDurationSeconds = int64(43200)
		)
		if ds < minDurationSeconds || ds > maxDurationSeconds {
			h.writeSTSErrorResponse(w, r, STSErrInvalidParameterValue,
				fmt.Errorf("DurationSeconds must be between %d and %d seconds", minDurationSeconds, maxDurationSeconds))
			return
		}

		durationSeconds = &ds
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
