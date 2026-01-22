package handler

import "net/http"

// IAMActionHandler defines the interface for processing IAM API requests.
// This interface enables dependency inversion, allowing s3api to accept
// IAM handlers without importing the iamapi package (avoiding circular dependency).
//
// This follows the Go proverb: "Accept interfaces, return structs"
// and is the standard pattern used by major Go projects (Kubernetes, Docker, etc.)
type IAMActionHandler interface {
	// HandleIAMAction processes an IAM API request (CreateRole, GetRole, AttachRolePolicy, etc.)
	// The implementation is responsible for:
	// - Parsing the Action parameter from the request
	// - Dispatching to the appropriate handler
	// - Writing the response in AWS IAM XML format
	HandleIAMAction(w http.ResponseWriter, r *http.Request)
}
