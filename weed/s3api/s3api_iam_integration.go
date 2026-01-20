package s3api

import (
	"bytes"
	"io"
	"net/http"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/iam/handler"
)

// -----------------------------------------------------------------------
// IAM / STS Handler Routing
// -----------------------------------------------------------------------

// IAMOrSTSHandler routes incoming POST requests to either IAM or STS handler
// based on the Action parameter. This provides a single endpoint for all
// IAM and STS operations, following AWS's approach and industry patterns (MinIO, Ceph RGW).
func (s3a *S3ApiServer) IAMOrSTSHandler(w http.ResponseWriter, r *http.Request) {
	// Read and buffer body to allow multiple reads (ParseForm uses it, and downstream handlers need it)
	var bodyBytes []byte
	if r.Body != nil {
		var err error
		bodyBytes, err = io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, "Failed to read request body", http.StatusInternalServerError)
			return
		}
		// Restore body for ParseForm
		r.Body = io.NopCloser(bytes.NewBuffer(bodyBytes))
	}

	// Parse the request form to get the Action parameter
	if err := r.ParseForm(); err != nil {
		http.Error(w, "Invalid request", http.StatusBadRequest)
		return
	}

	// Restore body for downstream handlers (STSHandler, etc.)
	if bodyBytes != nil {
		r.Body = io.NopCloser(bytes.NewBuffer(bodyBytes))
	}

	action := r.Form.Get("Action")
	if action == "" {
		// Use STS error format for missing action
		writeSTSError(w, r, "Sender", "MissingAction", "Action parameter is required")
		return
	}

	// Route based on action type
	// STS actions: AssumeRole, AssumeRoleWithWebIdentity, GetCallerIdentity
	// IAM actions: CreateRole, GetRole, ListRoles, DeleteRole, AttachRolePolicy, etc.
	switch action {
	case "AssumeRole", "AssumeRoleWithWebIdentity", "GetCallerIdentity":
		s3a.STSHandler(w, r)
	default:
		// All other actions are IAM management operations
		s3a.handleIAMAction(w, r)
	}
}

// -----------------------------------------------------------------------
// Dependency Injection & Accessors
// -----------------------------------------------------------------------

// SetIAMActionHandler injects the IAM action handler into the S3 server
// This enables dependency injection to avoid circular dependencies between
// s3api and iamapi packages
func (s3a *S3ApiServer) SetIAMActionHandler(h handler.IAMActionHandler) {
	s3a.iamActionHandler = h
}

// GetIAM exposes the IdentityAccessManagement instance
// This is used for dependency injection to ensure IamApiServer uses the same IAM state
func (s3a *S3ApiServer) GetIAM() *IdentityAccessManagement {
	return s3a.iam
}

// -----------------------------------------------------------------------
// Internal Helper
// -----------------------------------------------------------------------

// handleIAMAction delegates IAM API requests to the injected IAM handler
// This uses dependency injection via interface to avoid circular dependencies
func (s3a *S3ApiServer) handleIAMAction(w http.ResponseWriter, r *http.Request) {
	if s3a.iamActionHandler == nil {
		http.Error(w, "IAM API not configured - start S3 service with -iam.config parameter", http.StatusServiceUnavailable)
		glog.Warning("IAM action handler not configured")
		return
	}

	// Delegate to the injected IAM handler implementation
	s3a.iamActionHandler.HandleIAMAction(w, r)
}
