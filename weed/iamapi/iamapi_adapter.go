package iamapi

import (
	"net/http"

	"github.com/seaweedfs/seaweedfs/weed/iam/handler"
	"github.com/seaweedfs/seaweedfs/weed/s3api"
)

// Ensure IamApiServer implements the IAMActionHandler interface
var _ handler.IAMActionHandler = (*IamApiServer)(nil)

// -----------------------------------------------------------------------
// IAM Adapter Pattern Implementation
// -----------------------------------------------------------------------

// HandleIAMAction implements the handler.IAMActionHandler interface
// This allows the S3 service to call IAM handlers without importing the iamapi package
// avoiding circular dependencies.
func (iama *IamApiServer) HandleIAMAction(w http.ResponseWriter, r *http.Request) {
	// Delegate to the existing DoActions method which contains all the IAM logic
	iama.DoActions(w, r)
}

// SetIAM allows injecting an existing IdentityAccessManagement instance
// This is critical for integrating with S3ApiServer to share the same IAM state
// ensuring that operations performed via IamApiServer adapter affect the S3 server's IAM state.
func (iama *IamApiServer) SetIAM(iam *s3api.IdentityAccessManagement) {
	iama.iam = iam
}
