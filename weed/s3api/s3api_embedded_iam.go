package s3api

// This file provides IAM API functionality embedded in the S3 server.
// Common IAM types and helpers are imported from the shared weed/iam package.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"sync"

	"github.com/aws/aws-sdk-go/service/iam"
	"github.com/seaweedfs/seaweedfs/weed/credential"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	iamlib "github.com/seaweedfs/seaweedfs/weed/iam"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/iam_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/policy_engine"
	. "github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3err"
	"google.golang.org/protobuf/proto"
)

// EmbeddedIamApi provides IAM API functionality embedded in the S3 server.
// This allows running a single server that handles both S3 and IAM requests.
type EmbeddedIamApi struct {
	credentialManager *credential.CredentialManager
	iam               *IdentityAccessManagement
	policyLock        sync.RWMutex
}

// NewEmbeddedIamApi creates a new embedded IAM API handler.
func NewEmbeddedIamApi(credentialManager *credential.CredentialManager, iam *IdentityAccessManagement) *EmbeddedIamApi {
	return &EmbeddedIamApi{
		credentialManager: credentialManager,
		iam:               iam,
	}
}

// Type aliases for IAM response types from shared package
type (
	iamCommonResponse           = iamlib.CommonResponse
	iamListUsersResponse        = iamlib.ListUsersResponse
	iamListAccessKeysResponse   = iamlib.ListAccessKeysResponse
	iamDeleteAccessKeyResponse  = iamlib.DeleteAccessKeyResponse
	iamCreatePolicyResponse     = iamlib.CreatePolicyResponse
	iamCreateUserResponse       = iamlib.CreateUserResponse
	iamDeleteUserResponse       = iamlib.DeleteUserResponse
	iamGetUserResponse          = iamlib.GetUserResponse
	iamUpdateUserResponse       = iamlib.UpdateUserResponse
	iamCreateAccessKeyResponse  = iamlib.CreateAccessKeyResponse
	iamPutUserPolicyResponse    = iamlib.PutUserPolicyResponse
	iamDeleteUserPolicyResponse = iamlib.DeleteUserPolicyResponse
	iamGetUserPolicyResponse    = iamlib.GetUserPolicyResponse
	iamSetUserStatusResponse    = iamlib.SetUserStatusResponse
	iamUpdateAccessKeyResponse  = iamlib.UpdateAccessKeyResponse
	iamErrorResponse            = iamlib.ErrorResponse
	iamError                    = iamlib.Error
)

// Helper function wrappers using shared package
func iamHash(s *string) string {
	return iamlib.Hash(s)
}

func iamStringWithCharset(length int, charset string) (string, error) {
	return iamlib.GenerateRandomString(length, charset)
}

func iamStringSlicesEqual(a, b []string) bool {
	return iamlib.StringSlicesEqual(a, b)
}

func iamMapToStatementAction(action string) string {
	return iamlib.MapToStatementAction(action)
}

func iamMapToIdentitiesAction(action string) string {
	return iamlib.MapToIdentitiesAction(action)
}

// iamValidateStatus validates that status is either Active or Inactive.
func iamValidateStatus(status string) error {
	switch status {
	case iamAccessKeyStatusActive, iamAccessKeyStatusInactive:
		return nil
	case "":
		return fmt.Errorf("Status parameter is required")
	default:
		return fmt.Errorf("Status must be '%s' or '%s'", iamAccessKeyStatusActive, iamAccessKeyStatusInactive)
	}
}

// Constants from shared package
const (
	iamCharsetUpper            = iamlib.CharsetUpper
	iamCharset                 = iamlib.Charset
	iamPolicyDocumentVersion   = iamlib.PolicyDocumentVersion
	iamUserDoesNotExist        = iamlib.UserDoesNotExist
	iamAccessKeyStatusActive   = iamlib.AccessKeyStatusActive
	iamAccessKeyStatusInactive = iamlib.AccessKeyStatusInactive
)

func newIamErrorResponse(errCode string, errMsg string) iamErrorResponse {
	errorResp := iamErrorResponse{}
	errorResp.Error.Type = "Sender"
	errorResp.Error.Code = &errCode
	errorResp.Error.Message = &errMsg
	return errorResp
}

func (e *EmbeddedIamApi) writeIamErrorResponse(w http.ResponseWriter, r *http.Request, iamErr *iamError) {
	if iamErr == nil {
		glog.Errorf("No error found")
		return
	}

	errCode := iamErr.Code
	errMsg := iamErr.Error.Error()
	glog.Errorf("IAM Response %+v", errMsg)

	errorResp := newIamErrorResponse(errCode, errMsg)
	internalErrorResponse := newIamErrorResponse(iam.ErrCodeServiceFailureException, "Internal server error")

	switch errCode {
	case iam.ErrCodeNoSuchEntityException:
		s3err.WriteXMLResponse(w, r, http.StatusNotFound, errorResp)
	case iam.ErrCodeEntityAlreadyExistsException:
		s3err.WriteXMLResponse(w, r, http.StatusConflict, errorResp)
	case iam.ErrCodeMalformedPolicyDocumentException, iam.ErrCodeInvalidInputException:
		s3err.WriteXMLResponse(w, r, http.StatusBadRequest, errorResp)
	case iam.ErrCodeServiceFailureException:
		s3err.WriteXMLResponse(w, r, http.StatusInternalServerError, internalErrorResponse)
	default:
		s3err.WriteXMLResponse(w, r, http.StatusInternalServerError, internalErrorResponse)
	}
}

// GetS3ApiConfiguration loads the S3 API configuration from the credential manager.
func (e *EmbeddedIamApi) GetS3ApiConfiguration(s3cfg *iam_pb.S3ApiConfiguration) error {
	config, err := e.credentialManager.LoadConfiguration(context.Background())
	if err != nil {
		return fmt.Errorf("failed to load configuration: %w", err)
	}
	proto.Merge(s3cfg, config)
	return nil
}

// PutS3ApiConfiguration saves the S3 API configuration to the credential manager.
func (e *EmbeddedIamApi) PutS3ApiConfiguration(s3cfg *iam_pb.S3ApiConfiguration) error {
	return e.credentialManager.SaveConfiguration(context.Background(), s3cfg)
}

// ListUsers lists all IAM users.
func (e *EmbeddedIamApi) ListUsers(s3cfg *iam_pb.S3ApiConfiguration, values url.Values) iamListUsersResponse {
	var resp iamListUsersResponse
	for _, ident := range s3cfg.Identities {
		resp.ListUsersResult.Users = append(resp.ListUsersResult.Users, &iam.User{UserName: &ident.Name})
	}
	return resp
}

// ListAccessKeys lists access keys for a user.
func (e *EmbeddedIamApi) ListAccessKeys(s3cfg *iam_pb.S3ApiConfiguration, values url.Values) iamListAccessKeysResponse {
	var resp iamListAccessKeysResponse
	userName := values.Get("UserName")
	for _, ident := range s3cfg.Identities {
		if userName != "" && userName != ident.Name {
			continue
		}
		for _, cred := range ident.Credentials {
			// Return actual status from credential, default to Active if not set
			status := cred.Status
			if status == "" {
				status = iamAccessKeyStatusActive
			}
			// Capture copies to avoid loop variable pointer aliasing
			identName := ident.Name
			accessKey := cred.AccessKey
			statusCopy := status
			resp.ListAccessKeysResult.AccessKeyMetadata = append(resp.ListAccessKeysResult.AccessKeyMetadata,
				&iam.AccessKeyMetadata{UserName: &identName, AccessKeyId: &accessKey, Status: &statusCopy},
			)
		}
	}
	return resp
}

// CreateUser creates a new IAM user.
func (e *EmbeddedIamApi) CreateUser(s3cfg *iam_pb.S3ApiConfiguration, values url.Values) (iamCreateUserResponse, *iamError) {
	var resp iamCreateUserResponse
	userName := values.Get("UserName")

	// Validate UserName is not empty
	if userName == "" {
		return resp, &iamError{Code: iam.ErrCodeInvalidInputException, Error: fmt.Errorf("UserName is required")}
	}

	// Check for duplicate user
	for _, ident := range s3cfg.Identities {
		if ident.Name == userName {
			return resp, &iamError{Code: iam.ErrCodeEntityAlreadyExistsException, Error: fmt.Errorf("user %s already exists", userName)}
		}
	}

	resp.CreateUserResult.User.UserName = &userName
	s3cfg.Identities = append(s3cfg.Identities, &iam_pb.Identity{Name: userName}) // Disabled defaults to false (enabled)
	return resp, nil
}

// DeleteUser deletes an IAM user.
func (e *EmbeddedIamApi) DeleteUser(s3cfg *iam_pb.S3ApiConfiguration, userName string) (iamDeleteUserResponse, *iamError) {
	var resp iamDeleteUserResponse
	for i, ident := range s3cfg.Identities {
		if userName == ident.Name {
			s3cfg.Identities = append(s3cfg.Identities[:i], s3cfg.Identities[i+1:]...)
			return resp, nil
		}
	}
	return resp, &iamError{Code: iam.ErrCodeNoSuchEntityException, Error: fmt.Errorf(iamUserDoesNotExist, userName)}
}

// GetUser gets an IAM user.
func (e *EmbeddedIamApi) GetUser(s3cfg *iam_pb.S3ApiConfiguration, userName string) (iamGetUserResponse, *iamError) {
	var resp iamGetUserResponse
	for _, ident := range s3cfg.Identities {
		if userName == ident.Name {
			resp.GetUserResult.User = iam.User{UserName: &ident.Name}
			return resp, nil
		}
	}
	return resp, &iamError{Code: iam.ErrCodeNoSuchEntityException, Error: fmt.Errorf(iamUserDoesNotExist, userName)}
}

// UpdateUser updates an IAM user.
func (e *EmbeddedIamApi) UpdateUser(s3cfg *iam_pb.S3ApiConfiguration, values url.Values) (iamUpdateUserResponse, *iamError) {
	var resp iamUpdateUserResponse
	userName := values.Get("UserName")
	newUserName := values.Get("NewUserName")
	if newUserName != "" {
		for _, ident := range s3cfg.Identities {
			if userName == ident.Name {
				ident.Name = newUserName
				return resp, nil
			}
		}
	} else {
		return resp, nil
	}
	return resp, &iamError{Code: iam.ErrCodeNoSuchEntityException, Error: fmt.Errorf(iamUserDoesNotExist, userName)}
}

// CreateAccessKey creates an access key for a user.
func (e *EmbeddedIamApi) CreateAccessKey(s3cfg *iam_pb.S3ApiConfiguration, values url.Values) (iamCreateAccessKeyResponse, *iamError) {
	var resp iamCreateAccessKeyResponse
	userName := values.Get("UserName")
	status := iam.StatusTypeActive

	accessKeyId, err := iamStringWithCharset(21, iamCharsetUpper)
	if err != nil {
		return resp, &iamError{Code: iam.ErrCodeServiceFailureException, Error: fmt.Errorf("failed to generate access key: %w", err)}
	}
	secretAccessKey, err := iamStringWithCharset(42, iamCharset)
	if err != nil {
		return resp, &iamError{Code: iam.ErrCodeServiceFailureException, Error: fmt.Errorf("failed to generate secret key: %w", err)}
	}

	resp.CreateAccessKeyResult.AccessKey.AccessKeyId = &accessKeyId
	resp.CreateAccessKeyResult.AccessKey.SecretAccessKey = &secretAccessKey
	resp.CreateAccessKeyResult.AccessKey.UserName = &userName
	resp.CreateAccessKeyResult.AccessKey.Status = &status

	for _, ident := range s3cfg.Identities {
		if userName == ident.Name {
			ident.Credentials = append(ident.Credentials,
				&iam_pb.Credential{AccessKey: accessKeyId, SecretKey: secretAccessKey, Status: iamAccessKeyStatusActive})
			return resp, nil
		}
	}
	// User not found - return error instead of implicitly creating the user
	return resp, &iamError{Code: iam.ErrCodeNoSuchEntityException, Error: fmt.Errorf(iamUserDoesNotExist, userName)}
}

// DeleteAccessKey deletes an access key for a user.
func (e *EmbeddedIamApi) DeleteAccessKey(s3cfg *iam_pb.S3ApiConfiguration, values url.Values) iamDeleteAccessKeyResponse {
	var resp iamDeleteAccessKeyResponse
	userName := values.Get("UserName")
	accessKeyId := values.Get("AccessKeyId")
	for _, ident := range s3cfg.Identities {
		if userName == ident.Name {
			for i, cred := range ident.Credentials {
				if cred.AccessKey == accessKeyId {
					ident.Credentials = append(ident.Credentials[:i], ident.Credentials[i+1:]...)
					break
				}
			}
			break
		}
	}
	return resp
}

// GetPolicyDocument parses a policy document string.
func (e *EmbeddedIamApi) GetPolicyDocument(policy *string) (policy_engine.PolicyDocument, error) {
	var policyDocument policy_engine.PolicyDocument
	if err := json.Unmarshal([]byte(*policy), &policyDocument); err != nil {
		return policy_engine.PolicyDocument{}, err
	}
	return policyDocument, nil
}

// CreatePolicy validates and creates a new IAM managed policy.
// NOTE: Currently this only validates the policy document and returns policy metadata.
// The policy is not persisted to a managed policy store. To apply permissions to a user,
// use PutUserPolicy which stores the policy inline on the user's identity.
// TODO: Implement managed policy storage for full AWS IAM compatibility (ListPolicies, GetPolicy, AttachUserPolicy).
func (e *EmbeddedIamApi) CreatePolicy(s3cfg *iam_pb.S3ApiConfiguration, values url.Values) (iamCreatePolicyResponse, *iamError) {
	var resp iamCreatePolicyResponse
	policyName := values.Get("PolicyName")
	policyDocumentString := values.Get("PolicyDocument")
	_, err := e.GetPolicyDocument(&policyDocumentString)
	if err != nil {
		return resp, &iamError{Code: iam.ErrCodeMalformedPolicyDocumentException, Error: err}
	}
	policyId := iamHash(&policyDocumentString)
	arn := fmt.Sprintf("arn:aws:iam:::policy/%s", policyName)
	resp.CreatePolicyResult.Policy.PolicyName = &policyName
	resp.CreatePolicyResult.Policy.Arn = &arn
	resp.CreatePolicyResult.Policy.PolicyId = &policyId
	return resp, nil
}

// getActions extracts actions from a policy document.
// S3 ARN format: arn:aws:s3:::bucket or arn:aws:s3:::bucket/path/*
// res[5] contains the bucket and optional path after :::
func (e *EmbeddedIamApi) getActions(policy *policy_engine.PolicyDocument) ([]string, error) {
	var actions []string

	for _, statement := range policy.Statement {
		if statement.Effect != policy_engine.PolicyEffectAllow {
			return nil, fmt.Errorf("not a valid effect: '%s'. Only 'Allow' is possible", statement.Effect)
		}
		for _, resource := range statement.Resource.Strings() {
			res := strings.Split(resource, ":")
			if len(res) != 6 || res[0] != "arn" || res[1] != "aws" || res[2] != "s3" {
				continue
			}
			for _, action := range statement.Action.Strings() {
				act := strings.Split(action, ":")
				if len(act) != 2 || act[0] != "s3" {
					continue
				}
				statementAction := iamMapToStatementAction(act[1])
				if statementAction == "" {
					return nil, fmt.Errorf("not a valid action: '%s'", act[1])
				}

				resourcePath := res[5]
				if resourcePath == "*" {
					// Wildcard - applies to all buckets
					actions = append(actions, statementAction)
					continue
				}

				// Parse bucket and optional object path
				// Examples: "mybucket", "mybucket/*", "mybucket/prefix/*"
				bucket, objectPath, hasSep := strings.Cut(resourcePath, "/")
				if bucket == "" {
					continue // Invalid: empty bucket name
				}

				if !hasSep || objectPath == "" || objectPath == "*" {
					// Bucket-level or bucket/* - use just bucket name
					actions = append(actions, fmt.Sprintf("%s:%s", statementAction, bucket))
				} else {
					// Path-specific: bucket/path/* -> Action:bucket/path
					// Remove trailing /* if present for cleaner action format
					objectPath = strings.TrimSuffix(objectPath, "/*")
					objectPath = strings.TrimSuffix(objectPath, "*")
					if objectPath == "" {
						actions = append(actions, fmt.Sprintf("%s:%s", statementAction, bucket))
					} else {
						actions = append(actions, fmt.Sprintf("%s:%s/%s", statementAction, bucket, objectPath))
					}
				}
			}
		}
	}

	if len(actions) == 0 {
		return nil, fmt.Errorf("no valid actions found in policy document")
	}
	return actions, nil
}

// PutUserPolicy attaches a policy to a user.
func (e *EmbeddedIamApi) PutUserPolicy(s3cfg *iam_pb.S3ApiConfiguration, values url.Values) (iamPutUserPolicyResponse, *iamError) {
	var resp iamPutUserPolicyResponse
	userName := values.Get("UserName")
	policyDocumentString := values.Get("PolicyDocument")
	policyDocument, err := e.GetPolicyDocument(&policyDocumentString)
	if err != nil {
		return resp, &iamError{Code: iam.ErrCodeMalformedPolicyDocumentException, Error: err}
	}
	actions, err := e.getActions(&policyDocument)
	if err != nil {
		return resp, &iamError{Code: iam.ErrCodeMalformedPolicyDocumentException, Error: err}
	}
	glog.V(3).Infof("PutUserPolicy: actions=%v", actions)
	for _, ident := range s3cfg.Identities {
		if userName != ident.Name {
			continue
		}
		ident.Actions = actions
		return resp, nil
	}
	return resp, &iamError{Code: iam.ErrCodeNoSuchEntityException, Error: fmt.Errorf("the user with name %s cannot be found", userName)}
}

// GetUserPolicy gets the policy attached to a user.
func (e *EmbeddedIamApi) GetUserPolicy(s3cfg *iam_pb.S3ApiConfiguration, values url.Values) (iamGetUserPolicyResponse, *iamError) {
	var resp iamGetUserPolicyResponse
	userName := values.Get("UserName")
	policyName := values.Get("PolicyName")
	for _, ident := range s3cfg.Identities {
		if userName != ident.Name {
			continue
		}

		resp.GetUserPolicyResult.UserName = userName
		resp.GetUserPolicyResult.PolicyName = policyName
		if len(ident.Actions) == 0 {
			return resp, &iamError{Code: iam.ErrCodeNoSuchEntityException, Error: errors.New("no actions found")}
		}

		policyDocument := policy_engine.PolicyDocument{Version: iamPolicyDocumentVersion}
		statements := make(map[string][]string)
		for _, action := range ident.Actions {
			// Action format: "ActionType" (global) or "ActionType:bucket" or "ActionType:bucket/path"
			actionType, bucketPath, hasPath := strings.Cut(action, ":")
			var resource string
			if !hasPath {
				// Global action (no bucket specified)
				resource = "*"
			} else if strings.Contains(bucketPath, "/") {
				// Path-specific: bucket/path -> arn:aws:s3:::bucket/path/*
				resource = fmt.Sprintf("arn:aws:s3:::%s/*", bucketPath)
			} else {
				// Bucket-level: bucket -> arn:aws:s3:::bucket/*
				resource = fmt.Sprintf("arn:aws:s3:::%s/*", bucketPath)
			}
			statements[resource] = append(statements[resource],
				fmt.Sprintf("s3:%s", iamMapToIdentitiesAction(actionType)),
			)
		}
		for resource, actions := range statements {
			isEqAction := false
			for i, statement := range policyDocument.Statement {
				// Use order-independent comparison to avoid duplicates from different action orderings
				if iamStringSlicesEqual(statement.Action.Strings(), actions) {
					policyDocument.Statement[i].Resource = policy_engine.NewStringOrStringSlice(append(
						policyDocument.Statement[i].Resource.Strings(), resource)...)
					isEqAction = true
					break
				}
			}
			if isEqAction {
				continue
			}
			policyDocumentStatement := policy_engine.PolicyStatement{
				Effect:   policy_engine.PolicyEffectAllow,
				Action:   policy_engine.NewStringOrStringSlice(actions...),
				Resource: policy_engine.NewStringOrStringSlice(resource),
			}
			policyDocument.Statement = append(policyDocument.Statement, policyDocumentStatement)
		}
		policyDocumentJSON, err := json.Marshal(policyDocument)
		if err != nil {
			return resp, &iamError{Code: iam.ErrCodeServiceFailureException, Error: err}
		}
		resp.GetUserPolicyResult.PolicyDocument = string(policyDocumentJSON)
		return resp, nil
	}
	return resp, &iamError{Code: iam.ErrCodeNoSuchEntityException, Error: fmt.Errorf(iamUserDoesNotExist, userName)}
}

// DeleteUserPolicy removes the inline policy from a user (clears their actions).
func (e *EmbeddedIamApi) DeleteUserPolicy(s3cfg *iam_pb.S3ApiConfiguration, values url.Values) (iamDeleteUserPolicyResponse, *iamError) {
	var resp iamDeleteUserPolicyResponse
	userName := values.Get("UserName")
	for _, ident := range s3cfg.Identities {
		if ident.Name == userName {
			ident.Actions = nil
			return resp, nil
		}
	}
	return resp, &iamError{Code: iam.ErrCodeNoSuchEntityException, Error: fmt.Errorf(iamUserDoesNotExist, userName)}
}

// SetUserStatus enables or disables a user without deleting them.
// This is a SeaweedFS extension for temporary user suspension, offboarding, etc.
// When a user is disabled, all API requests using their credentials will return AccessDenied.
func (e *EmbeddedIamApi) SetUserStatus(s3cfg *iam_pb.S3ApiConfiguration, values url.Values) (iamSetUserStatusResponse, *iamError) {
	var resp iamSetUserStatusResponse
	userName := values.Get("UserName")
	status := values.Get("Status")

	// Validate UserName
	if userName == "" {
		return resp, &iamError{Code: iam.ErrCodeInvalidInputException, Error: fmt.Errorf("UserName is required")}
	}

	// Validate Status - must be "Active" or "Inactive"
	if err := iamValidateStatus(status); err != nil {
		return resp, &iamError{Code: iam.ErrCodeInvalidInputException, Error: err}
	}

	for _, ident := range s3cfg.Identities {
		if ident.Name == userName {
			// Set disabled based on status: Active = not disabled, Inactive = disabled
			ident.Disabled = (status == iamAccessKeyStatusInactive)
			return resp, nil
		}
	}
	return resp, &iamError{Code: iam.ErrCodeNoSuchEntityException, Error: fmt.Errorf(iamUserDoesNotExist, userName)}
}

// UpdateAccessKey updates the status of an access key (Active or Inactive).
// This allows key rotation workflows where old keys are deactivated before deletion.
func (e *EmbeddedIamApi) UpdateAccessKey(s3cfg *iam_pb.S3ApiConfiguration, values url.Values) (iamUpdateAccessKeyResponse, *iamError) {
	var resp iamUpdateAccessKeyResponse
	userName := values.Get("UserName")
	accessKeyId := values.Get("AccessKeyId")
	status := values.Get("Status")

	// Validate required parameters
	if userName == "" {
		return resp, &iamError{Code: iam.ErrCodeInvalidInputException, Error: fmt.Errorf("UserName is required")}
	}
	if accessKeyId == "" {
		return resp, &iamError{Code: iam.ErrCodeInvalidInputException, Error: fmt.Errorf("AccessKeyId is required")}
	}
	if err := iamValidateStatus(status); err != nil {
		return resp, &iamError{Code: iam.ErrCodeInvalidInputException, Error: err}
	}

	for _, ident := range s3cfg.Identities {
		if ident.Name != userName {
			continue
		}
		for _, cred := range ident.Credentials {
			if cred.AccessKey == accessKeyId {
				cred.Status = status
				return resp, nil
			}
		}
		// User found but access key not found
		return resp, &iamError{Code: iam.ErrCodeNoSuchEntityException, Error: fmt.Errorf("the access key with id %s for user %s cannot be found", accessKeyId, userName)}
	}

	return resp, &iamError{Code: iam.ErrCodeNoSuchEntityException, Error: fmt.Errorf(iamUserDoesNotExist, userName)}
}

// handleImplicitUsername adds username who signs the request to values if 'username' is not specified.
// According to AWS documentation: "If you do not specify a user name, IAM determines the user name
// implicitly based on the Amazon Web Services access key ID signing the request."
// This function extracts the AccessKeyId from the SigV4 credential and looks up the corresponding
// identity name in the credential store.
func (e *EmbeddedIamApi) handleImplicitUsername(r *http.Request, values url.Values) {
	if len(r.Header["Authorization"]) == 0 || values.Get("UserName") != "" {
		return
	}
	// Log presence of auth header without exposing sensitive signature material
	glog.V(4).Infof("Authorization header present, extracting access key")
	// Parse AWS SigV4 Authorization header format:
	// "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/iam/aws4_request, ..."
	s := strings.Split(r.Header["Authorization"][0], "Credential=")
	if len(s) < 2 {
		return
	}
	s = strings.Split(s[1], ",")
	if len(s) < 1 {
		return
	}
	s = strings.Split(s[0], "/")
	if len(s) < 1 {
		return
	}
	// s[0] is the AccessKeyId
	accessKeyId := s[0]
	if accessKeyId == "" {
		return
	}
	// Nil-guard: ensure iam is initialized before lookup
	if e.iam == nil {
		glog.V(4).Infof("IAM not initialized, cannot look up access key")
		return
	}
	// Look up the identity by access key to get the username
	identity, _, found := e.iam.LookupByAccessKey(accessKeyId)
	if !found {
		// Mask access key in logs - show only first 4 chars
		maskedKey := accessKeyId
		if len(accessKeyId) > 4 {
			maskedKey = accessKeyId[:4] + "***"
		}
		glog.V(4).Infof("Access key %s not found in credential store", maskedKey)
		return
	}
	values.Set("UserName", identity.Name)
}

// iamSelfServiceActions are actions that users can perform on their own resources without admin rights.
// According to AWS IAM, users can manage their own access keys without requiring full admin permissions.
var iamSelfServiceActions = map[string]bool{
	"CreateAccessKey": true,
	"DeleteAccessKey": true,
	"ListAccessKeys":  true,
	"GetUser":         true,
	"UpdateAccessKey": true,
}

// iamRequiresAdminForOthers returns true if the action requires admin rights when operating on other users.
func iamRequiresAdminForOthers(action string) bool {
	return iamSelfServiceActions[action]
}

// AuthIam provides IAM-specific authentication that allows self-service operations.
// Users can manage their own access keys without admin rights, but need admin for operations on other users.
// The action parameter is accepted for interface compatibility with cb.Limit but is not used
// since IAM permission checking is done based on the IAM Action parameter in the request.
func (e *EmbeddedIamApi) AuthIam(f http.HandlerFunc, _ Action) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// If auth is not enabled, allow all
		if !e.iam.isEnabled() {
			f(w, r)
			return
		}

		// Parse form to get Action and UserName
		if err := r.ParseForm(); err != nil {
			s3err.WriteErrorResponse(w, r, s3err.ErrInvalidRequest)
			return
		}

		action := r.Form.Get("Action")
		targetUserName := r.PostForm.Get("UserName")

		// Authenticate the request using signature-only verification.
		// This bypasses S3 authorization checks (identity.canDo) since IAM operations
		// have their own permission model based on self-service vs admin operations.
		identity, errCode := e.iam.AuthSignatureOnly(r)
		if errCode != s3err.ErrNone {
			s3err.WriteErrorResponse(w, r, errCode)
			return
		}

		// IAM API requests must be authenticated - reject nil identity
		// (can happen for authTypePostPolicy or authTypeStreamingUnsigned)
		if identity == nil {
			s3err.WriteErrorResponse(w, r, s3err.ErrAccessDenied)
			return
		}

		// Store identity in context
		if identity != nil && identity.Name != "" {
			ctx := SetIdentityNameInContext(r.Context(), identity.Name)
			ctx = SetIdentityInContext(ctx, identity)
			r = r.WithContext(ctx)
		}

		// Check permissions based on action type
		if iamRequiresAdminForOthers(action) {
			// Self-service action: allow if operating on own resources or no target specified
			if targetUserName == "" || targetUserName == identity.Name {
				// Self-service: allowed
				f(w, r)
				return
			}
			// Operating on another user: require admin
			if !identity.isAdmin() {
				s3err.WriteErrorResponse(w, r, s3err.ErrAccessDenied)
				return
			}
		} else {
			// All other IAM actions require admin (CreateUser, DeleteUser, PutUserPolicy, etc.)
			if !identity.isAdmin() {
				s3err.WriteErrorResponse(w, r, s3err.ErrAccessDenied)
				return
			}
		}

		f(w, r)
	}
}

// DoActions handles IAM API actions.
func (e *EmbeddedIamApi) DoActions(w http.ResponseWriter, r *http.Request) {
	// Lock to prevent concurrent read-modify-write race conditions
	e.policyLock.Lock()
	defer e.policyLock.Unlock()

	if err := r.ParseForm(); err != nil {
		s3err.WriteErrorResponse(w, r, s3err.ErrInvalidRequest)
		return
	}
	values := r.PostForm
	s3cfg := &iam_pb.S3ApiConfiguration{}
	if err := e.GetS3ApiConfiguration(s3cfg); err != nil && !errors.Is(err, filer_pb.ErrNotFound) {
		s3err.WriteErrorResponse(w, r, s3err.ErrInternalError)
		return
	}

	glog.V(4).Infof("IAM DoActions: %+v", values)
	var response interface{}
	var iamErr *iamError
	changed := true
	switch r.Form.Get("Action") {
	case "ListUsers":
		response = e.ListUsers(s3cfg, values)
		changed = false
	case "ListAccessKeys":
		e.handleImplicitUsername(r, values)
		response = e.ListAccessKeys(s3cfg, values)
		changed = false
	case "CreateUser":
		response, iamErr = e.CreateUser(s3cfg, values)
		if iamErr != nil {
			e.writeIamErrorResponse(w, r, iamErr)
			return
		}
	case "GetUser":
		userName := values.Get("UserName")
		response, iamErr = e.GetUser(s3cfg, userName)
		if iamErr != nil {
			e.writeIamErrorResponse(w, r, iamErr)
			return
		}
		changed = false
	case "UpdateUser":
		response, iamErr = e.UpdateUser(s3cfg, values)
		if iamErr != nil {
			e.writeIamErrorResponse(w, r, iamErr)
			return
		}
	case "DeleteUser":
		userName := values.Get("UserName")
		response, iamErr = e.DeleteUser(s3cfg, userName)
		if iamErr != nil {
			e.writeIamErrorResponse(w, r, iamErr)
			return
		}
	case "CreateAccessKey":
		e.handleImplicitUsername(r, values)
		response, iamErr = e.CreateAccessKey(s3cfg, values)
		if iamErr != nil {
			glog.Errorf("CreateAccessKey: %+v", iamErr.Error)
			e.writeIamErrorResponse(w, r, iamErr)
			return
		}
	case "DeleteAccessKey":
		e.handleImplicitUsername(r, values)
		response = e.DeleteAccessKey(s3cfg, values)
	case "CreatePolicy":
		response, iamErr = e.CreatePolicy(s3cfg, values)
		if iamErr != nil {
			glog.Errorf("CreatePolicy: %+v", iamErr.Error)
			s3err.WriteErrorResponse(w, r, s3err.ErrInvalidRequest)
			return
		}
		// CreatePolicy only validates the policy document and returns metadata.
		// Policies are not stored separately; they are attached inline via PutUserPolicy.
		changed = false
	case "PutUserPolicy":
		response, iamErr = e.PutUserPolicy(s3cfg, values)
		if iamErr != nil {
			glog.Errorf("PutUserPolicy: %+v", iamErr.Error)
			e.writeIamErrorResponse(w, r, iamErr)
			return
		}
	case "GetUserPolicy":
		response, iamErr = e.GetUserPolicy(s3cfg, values)
		if iamErr != nil {
			e.writeIamErrorResponse(w, r, iamErr)
			return
		}
		changed = false
	case "DeleteUserPolicy":
		response, iamErr = e.DeleteUserPolicy(s3cfg, values)
		if iamErr != nil {
			e.writeIamErrorResponse(w, r, iamErr)
			return
		}
	case "SetUserStatus":
		response, iamErr = e.SetUserStatus(s3cfg, values)
		if iamErr != nil {
			e.writeIamErrorResponse(w, r, iamErr)
			return
		}
	case "UpdateAccessKey":
		e.handleImplicitUsername(r, values)
		response, iamErr = e.UpdateAccessKey(s3cfg, values)
		if iamErr != nil {
			e.writeIamErrorResponse(w, r, iamErr)
			return
		}
	default:
		errNotImplemented := s3err.GetAPIError(s3err.ErrNotImplemented)
		errorResponse := iamErrorResponse{}
		errorResponse.Error.Code = &errNotImplemented.Code
		errorResponse.Error.Message = &errNotImplemented.Description
		s3err.WriteXMLResponse(w, r, errNotImplemented.HTTPStatusCode, errorResponse)
		return
	}
	if changed {
		if err := e.PutS3ApiConfiguration(s3cfg); err != nil {
			iamErr = &iamError{Code: iam.ErrCodeServiceFailureException, Error: err}
			e.writeIamErrorResponse(w, r, iamErr)
			return
		}
		// Reload in-memory identity maps so subsequent LookupByAccessKey calls
		// can see newly created or deleted keys immediately
		if err := e.iam.LoadS3ApiConfigurationFromCredentialManager(); err != nil {
			glog.Warningf("Failed to reload IAM configuration after mutation: %v", err)
			// Don't fail the request since the persistent save succeeded
		}
	}
	// Set RequestId for AWS compatibility
	if r, ok := response.(interface{ SetRequestId() }); ok {
		r.SetRequestId()
	}
	s3err.WriteXMLResponse(w, r, http.StatusOK, response)
}
