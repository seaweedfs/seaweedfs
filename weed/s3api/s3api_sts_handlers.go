package s3api

import (
	"bytes"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"time"

	"github.com/google/uuid"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/iam/providers"
	"github.com/seaweedfs/seaweedfs/weed/iam/sts"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3err"
)
// STS API Actions
const (
	ActionAssumeRole               = "AssumeRole"
	ActionGetCallerIdentity        = "GetCallerIdentity"
)

// STS XML Response Structures
// Strictly following AWS STS API Reference

// STS XML Response Structures
// Strictly following AWS STS API Reference

type STSAssumedRoleUser struct {
	Arn           string `xml:"Arn"`
	AssumedRoleId string `xml:"AssumedRoleId"`
}

type GetCallerIdentityResponse struct {
	XMLName                 xml.Name                `xml:"https://sts.amazonaws.com/doc/2011-06-15/ GetCallerIdentityResponse"`
	GetCallerIdentityResult GetCallerIdentityResult `xml:"GetCallerIdentityResult"`
	ResponseMetadata        ResponseMetadata        `xml:"ResponseMetadata"`
}

type GetCallerIdentityResult struct {
	Arn     string `xml:"Arn"`
	UserId  string `xml:"UserId"`
	Account string `xml:"Account"`
}

type ResponseMetadata struct {
	RequestId string `xml:"RequestId"`
}

// STSHandler handles STS API requests
func (s3a *S3ApiServer) STSHandler(w http.ResponseWriter, r *http.Request) {
	// 1. Parse Action
	// AWS STS requests use POST with form parameters or GET with query parameters
	
	// Read and buffer body to allow multiple reads (ParseForm uses it, and Authenticate needs it)
	var bodyBytes []byte
	if r.Body != nil {
		var err error
		bodyBytes, err = io.ReadAll(r.Body)
		if err != nil {
			writeSTSError(w, r, "Sender", "InternalError", "Failed to read request body")
			return
		}
		// Restore body for ParseForm
		r.Body = io.NopCloser(bytes.NewBuffer(bodyBytes))

	}

	if err := r.ParseForm(); err != nil {
		writeSTSError(w, r, "Sender", "InvalidParameter", "Cannot parse form parameters")
		return
	}

	// Restore body again for Authenticate (called in handlers)
	if bodyBytes != nil {
		r.Body = io.NopCloser(bytes.NewBuffer(bodyBytes))
	}

	action := r.Form.Get("Action")
	if action == "" {
		writeSTSError(w, r, "Sender", "MissingAction", "Action parameter is missing")
		return
	}

	// 2. Dispatch based on Action
	switch action {
	case ActionAssumeRole:
		s3a.handleAssumeRole(w, r)
	case ActionGetCallerIdentity:
		s3a.handleGetCallerIdentity(w, r)
	default:
		writeSTSError(w, r, "Sender", "InvalidAction", fmt.Sprintf("The action %s is not supported", action))
	}
}

func (s3a *S3ApiServer) handleAssumeRole(w http.ResponseWriter, r *http.Request) {
	if s3a.iam == nil || s3a.iam.iamIntegration == nil {
		writeSTSError(w, r, "Server", "ServiceUnavailable", "IAM service not configured")
		return
	}

	roleArn := r.Form.Get("RoleArn")
	roleSessionName := r.Form.Get("RoleSessionName")
	durationSecondsStr := r.Form.Get("DurationSeconds")
	// policy := r.Form.Get("Policy") // TODO: Support inline session policies in future

	if roleArn == "" || roleSessionName == "" {
		writeSTSError(w, r, "Sender", "ValidationError", "RoleArn and RoleSessionName are required")
		return
	}

	// Authenticate the caller (S3 SigV4)
	identity, errCode := s3a.iam.Authenticate(r)
	if errCode != s3err.ErrNone {
		writeSTSError(w, r, "Sender", "AccessDenied", fmt.Sprintf("Valid credentials required (SigV4): %v", errCode))
		return
	}
	if identity == nil {
		writeSTSError(w, r, "Sender", "AccessDenied", "Valid credentials required")
		return
	}

	// Prepare request
	req := &sts.AssumeRoleRequest{
		RoleArn:         roleArn,
		RoleSessionName: roleSessionName,
		ExternalIdentity: &providers.ExternalIdentity{
			Provider: "seaweedfs", // Internal provider for local users
			UserID:   identity.Name,
			// Add any group info if available in identity
		},
	}

	if durationSecondsStr != "" {
		dur, err := strconv.ParseInt(durationSecondsStr, 10, 64)
		if err != nil {
			writeSTSError(w, r, "Sender", "ValidationError", "Invalid DurationSeconds")
			return
		}
		req.DurationSeconds = &dur
	}

	// Call Backend
	if s3a.iamIntegration.iamManager == nil {
		writeSTSError(w, r, "Server", "ServiceUnavailable", "IAM manager not initialized")
		return
	}

	resp, err := s3a.iamIntegration.iamManager.AssumeRole(r.Context(), req)
	if err != nil {
		glog.Errorf("AssumeRole failed: %v", err)
		writeSTSError(w, r, "Sender", "AccessDenied", err.Error())
		return
	}

	// Marshaling Response
	xmlResp := AssumeRoleResponse{
		Result: AssumeRoleResult{
			Credentials: STSCredentials{
				AccessKeyId:     resp.Credentials.AccessKeyId,
				SecretAccessKey: resp.Credentials.SecretAccessKey,
				SessionToken:    resp.Credentials.SessionToken,
				Expiration:      resp.Credentials.Expiration.Format(time.RFC3339),
			},
			AssumedRoleUser: &AssumedRoleUser{
				Arn:           resp.AssumedRoleUser.Arn,
				AssumedRoleId: resp.AssumedRoleUser.AssumedRoleId,
			},
		},
	}
	xmlResp.ResponseMetadata.RequestId = uuid.New().String()

	writeXML(w, xmlResp)
}

func (s3a *S3ApiServer) handleGetCallerIdentity(w http.ResponseWriter, r *http.Request) {
    // Manually authenticate the request since we are not using the standard S3 Auth middleware
    // This endpoint requires a valid signature (SigV4)
    if s3a.iam == nil {
         writeSTSError(w, r, "Server", "ServiceUnavailable", "IAM service not configured")
         return
    }

    identity, errCode := s3a.iam.Authenticate(r)
    if errCode != s3err.ErrNone {
         // Map s3err to STS XML error
         writeSTSError(w, r, "Sender", "AccessDenied", fmt.Sprintf("Valid credentials required: %v", errCode))
         return
    }

    if identity == nil {
         writeSTSError(w, r, "Sender", "AccessDenied", "Valid credentials required")
         return
    }
    
    // Construct ARN and Account. 
    // Usually Identity in SeaweedFS S3 is simple (Name, Credentials).
    // If it's an IAM identity, Name should be the user/role name.
    // If it's a root/config identity, Name is used.
    
    account := "seaweedfs" // Default account
    if identity.Account != nil && identity.Account.Id != "" {
        account = identity.Account.Id
    }
    
    // Synthesize ARN
    // arn:aws:iam::account:user/name
    arn := fmt.Sprintf("arn:aws:iam::%s:user/%s", account, identity.Name)
    
    // If we have specific principal info in headers (set by Authenticate path for JWT/etc), use it?
    // Note: authenticateJWTWithIAM sets X-SeaweedFS-Principal. VerifyRequest might set headers too?
    // authRequest sets generic headers.
    // For now, synthesized ARN is acceptable.
    
    xmlResp := GetCallerIdentityResponse{
        GetCallerIdentityResult: GetCallerIdentityResult{
            Arn:     arn,
            Account: account,
            UserId:  identity.Name,
        },
        ResponseMetadata: ResponseMetadata{
            RequestId: uuid.New().String(),
        },
    }
    writeXML(w, xmlResp)
}

// Helpers

func writeSTSError(w http.ResponseWriter, r *http.Request, errType, code, message string) {
	errResp := STSErrorResponse{
		Error: STSError{
			Type:    errType,
			Code:    code,
			Message: message,
		},
		RequestId: uuid.New().String(),
	}
	
	// Determine appropriate HTTP status code BEFORE writing header
	statusCode := http.StatusForbidden // Default for STS errors
	if code == "ServiceUnavailable" {
		statusCode = http.StatusServiceUnavailable
	} else if code == "ValidationError" || code == "InvalidParameter" || code == "MissingAction" {
		statusCode = http.StatusBadRequest
	}
	
	// Write header ONCE with the correct status
	w.WriteHeader(statusCode)
	writeXML(w, errResp)
}

func writeXML(w http.ResponseWriter, v interface{}) {
	w.Header().Set("Content-Type", "text/xml")
	output, err := xml.Marshal(v)
	if err != nil {
		glog.Errorf("XML marshal failed: %v", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Write(output)
}

// Helper to get identity from context safely
func (s3a *S3ApiServer) getIdentity(r *http.Request) interface{} {
    return s3_constants.GetIdentityFromContext(r)
}
