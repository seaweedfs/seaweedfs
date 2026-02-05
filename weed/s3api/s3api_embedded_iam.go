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
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/service/iam"
	"github.com/seaweedfs/seaweedfs/weed/credential"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	iamlib "github.com/seaweedfs/seaweedfs/weed/iam"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/iam_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/policy_engine"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
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
	// Test hook
	getS3ApiConfigurationFunc func(*iam_pb.S3ApiConfiguration) error
	putS3ApiConfigurationFunc func(*iam_pb.S3ApiConfiguration) error
	reloadConfigurationFunc   func() error
	readOnly                  bool
}

// NewEmbeddedIamApi creates a new embedded IAM API handler.
func NewEmbeddedIamApi(credentialManager *credential.CredentialManager, iam *IdentityAccessManagement, readOnly bool) *EmbeddedIamApi {
	return &EmbeddedIamApi{
		credentialManager: credentialManager,
		iam:               iam,
		readOnly:          readOnly,
	}
}

// Constants for service account identifiers
const (
	ServiceAccountIDLength  = 12 // Length of the service account ID
	AccessKeyLength         = 20 // AWS standard access key length
	SecretKeyLength         = 40 // AWS standard secret key length (base64 encoded)
	ServiceAccountIDPrefix  = "sa"
	ServiceAccountKeyPrefix = "ABIA" // Service account access keys start with ABIA
	UserAccessKeyPrefix     = "AKIA" // User access keys start with AKIA

	// Operational limits (AWS IAM compatible)
	MaxServiceAccountsPerUser = 100  // Maximum service accounts per user
	MaxDescriptionLength      = 1000 // Maximum description length in characters
)

// Type aliases for IAM response types from shared package
type (
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
	// Service account response types
	iamServiceAccountInfo           = iamlib.ServiceAccountInfo
	iamCreateServiceAccountResponse = iamlib.CreateServiceAccountResponse
	iamDeleteServiceAccountResponse = iamlib.DeleteServiceAccountResponse
	iamListServiceAccountsResponse  = iamlib.ListServiceAccountsResponse
	iamGetServiceAccountResponse    = iamlib.GetServiceAccountResponse
	iamUpdateServiceAccountResponse = iamlib.UpdateServiceAccountResponse
	// Managed policy attachment response types
	iamAttachUserPolicyResponse        = iamlib.AttachUserPolicyResponse
	iamDetachUserPolicyResponse        = iamlib.DetachUserPolicyResponse
	iamListAttachedUserPoliciesResponse = iamlib.ListAttachedUserPoliciesResponse
	iamAttachedPolicy                   = iamlib.AttachedPolicy
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
	case "AccessDenied", iam.ErrCodeLimitExceededException:
		s3err.WriteXMLResponse(w, r, http.StatusForbidden, errorResp)
	case iam.ErrCodeServiceFailureException:
		s3err.WriteXMLResponse(w, r, http.StatusInternalServerError, internalErrorResponse)
	default:
		s3err.WriteXMLResponse(w, r, http.StatusInternalServerError, internalErrorResponse)
	}
}

// GetS3ApiConfiguration loads the S3 API configuration from the credential manager.
func (e *EmbeddedIamApi) GetS3ApiConfiguration(s3cfg *iam_pb.S3ApiConfiguration) error {
	if e.getS3ApiConfigurationFunc != nil {
		return e.getS3ApiConfigurationFunc(s3cfg)
	}
	config, err := e.credentialManager.LoadConfiguration(context.Background())
	if err != nil {
		return fmt.Errorf("failed to load configuration: %w", err)
	}
	proto.Merge(s3cfg, config)
	return nil
}

// PutS3ApiConfiguration saves the S3 API configuration to the credential manager.
func (e *EmbeddedIamApi) PutS3ApiConfiguration(s3cfg *iam_pb.S3ApiConfiguration) error {
	if e.putS3ApiConfigurationFunc != nil {
		return e.putS3ApiConfigurationFunc(s3cfg)
	}
	return e.credentialManager.SaveConfiguration(context.Background(), s3cfg)
}

// ReloadConfiguration reloads the IAM configuration from the credential manager.
func (e *EmbeddedIamApi) ReloadConfiguration() error {
	glog.V(4).Infof("IAM: reloading configuration via EmbeddedIamApi")
	if e.reloadConfigurationFunc != nil {
		return e.reloadConfigurationFunc()
	}
	return e.iam.LoadS3ApiConfigurationFromCredentialManager()
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
			// AWS IAM behavior: prevent deletion if user has service accounts
			// This ensures explicit cleanup and prevents orphaned resources
			if len(ident.ServiceAccountIds) > 0 {
				return resp, &iamError{
					Code: iam.ErrCodeDeleteConflictException,
					Error: fmt.Errorf("cannot delete user %s: user has %d service account(s). Delete service accounts first",
						userName, len(ident.ServiceAccountIds)),
				}
			}
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

	// Generate AWS-standard access key: AKIA prefix + 16 random uppercase chars = 20 total
	randomPart, err := iamStringWithCharset(AccessKeyLength-len(UserAccessKeyPrefix), iamCharsetUpper)
	if err != nil {
		return resp, &iamError{Code: iam.ErrCodeServiceFailureException, Error: fmt.Errorf("failed to generate access key: %w", err)}
	}
	accessKeyId := UserAccessKeyPrefix + randomPart

	secretAccessKey, err := iamStringWithCharset(SecretKeyLength, iamCharset)
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

// findIdentityByName is a helper function to find an identity by name.
// Returns the identity or nil if not found.
func findIdentityByName(s3cfg *iam_pb.S3ApiConfiguration, name string) *iam_pb.Identity {
	for _, ident := range s3cfg.Identities {
		if ident.Name == name {
			return ident
		}
	}
	return nil
}

// CreateServiceAccount creates a new service account for a user.
func (e *EmbeddedIamApi) CreateServiceAccount(s3cfg *iam_pb.S3ApiConfiguration, values url.Values, createdBy string) (iamCreateServiceAccountResponse, *iamError) {
	var resp iamCreateServiceAccountResponse
	parentUser := values.Get("ParentUser")
	description := values.Get("Description")
	expirationStr := values.Get("Expiration") // Unix timestamp as string

	if parentUser == "" {
		return resp, &iamError{Code: iam.ErrCodeInvalidInputException, Error: fmt.Errorf("ParentUser is required")}
	}

	// Validate description length
	if len(description) > MaxDescriptionLength {
		return resp, &iamError{
			Code:  iam.ErrCodeInvalidInputException,
			Error: fmt.Errorf("description exceeds maximum length of %d characters", MaxDescriptionLength),
		}
	}

	// Verify parent user exists
	parentIdent := findIdentityByName(s3cfg, parentUser)
	if parentIdent == nil {
		return resp, &iamError{Code: iam.ErrCodeNoSuchEntityException, Error: fmt.Errorf(iamUserDoesNotExist, parentUser)}
	}

	// Check service account limit per user
	if len(parentIdent.ServiceAccountIds) >= MaxServiceAccountsPerUser {
		return resp, &iamError{
			Code: iam.ErrCodeLimitExceededException,
			Error: fmt.Errorf("user %s has reached the maximum limit of %d service accounts",
				parentUser, MaxServiceAccountsPerUser),
		}
	}

	// Generate unique ID and credentials
	saId, err := iamStringWithCharset(ServiceAccountIDLength, iamCharsetUpper)
	if err != nil {
		return resp, &iamError{Code: iam.ErrCodeServiceFailureException, Error: fmt.Errorf("failed to generate ID: %w", err)}
	}
	saId = ServiceAccountIDPrefix + "-" + saId

	// Generate access key ID with correct length (20 chars total including prefix)
	// AWS access keys are always 20 characters: 4-char prefix (ABIA) + 16 random chars
	accessKeyId, err := iamStringWithCharset(AccessKeyLength-len(ServiceAccountKeyPrefix), iamCharsetUpper)
	if err != nil {
		return resp, &iamError{Code: iam.ErrCodeServiceFailureException, Error: fmt.Errorf("failed to generate access key: %w", err)}
	}
	accessKeyId = ServiceAccountKeyPrefix + accessKeyId

	secretAccessKey, err := iamStringWithCharset(SecretKeyLength, iamCharset)
	if err != nil {
		return resp, &iamError{Code: iam.ErrCodeServiceFailureException, Error: fmt.Errorf("failed to generate secret key: %w", err)}
	}

	// Parse expiration if provided
	var expiration int64
	if expirationStr != "" {
		var err error
		expiration, err = strconv.ParseInt(expirationStr, 10, 64)
		if err != nil {
			return resp, &iamError{Code: iam.ErrCodeInvalidInputException, Error: fmt.Errorf("invalid expiration format: %w", err)}
		}
		if expiration > 0 && expiration < time.Now().Unix() {
			return resp, &iamError{Code: iam.ErrCodeInvalidInputException, Error: fmt.Errorf("expiration must be in the future")}
		}
	}

	now := time.Now()

	// Copy parent's actions to avoid shared slice reference
	actions := make([]string, len(parentIdent.Actions))
	copy(actions, parentIdent.Actions)

	sa := &iam_pb.ServiceAccount{
		Id:          saId,
		ParentUser:  parentUser,
		Description: description,
		Credential: &iam_pb.Credential{
			AccessKey: accessKeyId,
			SecretKey: secretAccessKey,
			Status:    iamAccessKeyStatusActive,
		},
		Actions:    actions, // Independent copy of parent's actions
		Expiration: expiration,
		Disabled:   false,
		CreatedAt:  now.Unix(),
		CreatedBy:  createdBy,
	}

	s3cfg.ServiceAccounts = append(s3cfg.ServiceAccounts, sa)
	parentIdent.ServiceAccountIds = append(parentIdent.ServiceAccountIds, saId)

	// Build response
	resp.CreateServiceAccountResult.ServiceAccount = iamServiceAccountInfo{
		ServiceAccountId: saId,
		ParentUser:       parentUser,
		Description:      description,
		AccessKeyId:      accessKeyId,
		SecretAccessKey:  &secretAccessKey,
		Status:           iamAccessKeyStatusActive,
		CreateDate:       now.Format(time.RFC3339),
	}
	if expiration > 0 {
		expStr := time.Unix(expiration, 0).Format(time.RFC3339)
		resp.CreateServiceAccountResult.ServiceAccount.Expiration = &expStr
	}

	return resp, nil
}

// DeleteServiceAccount deletes a service account.
func (e *EmbeddedIamApi) DeleteServiceAccount(s3cfg *iam_pb.S3ApiConfiguration, values url.Values) (iamDeleteServiceAccountResponse, *iamError) {
	var resp iamDeleteServiceAccountResponse
	saId := values.Get("ServiceAccountId")

	if saId == "" {
		return resp, &iamError{Code: iam.ErrCodeInvalidInputException, Error: fmt.Errorf("ServiceAccountId is required")}
	}

	// Find and remove the service account
	for i, sa := range s3cfg.ServiceAccounts {
		if sa.Id == saId {
			// Remove from parent's list
			if parentIdent := findIdentityByName(s3cfg, sa.ParentUser); parentIdent != nil {
				// Remove service account ID from parent's list using filter pattern
				// This avoids mutating the slice during iteration
				filtered := parentIdent.ServiceAccountIds[:0]
				for _, id := range parentIdent.ServiceAccountIds {
					if id != saId {
						filtered = append(filtered, id)
					}
				}
				parentIdent.ServiceAccountIds = filtered
			}
			// Remove service account
			s3cfg.ServiceAccounts = append(s3cfg.ServiceAccounts[:i], s3cfg.ServiceAccounts[i+1:]...)
			return resp, nil
		}
	}

	return resp, &iamError{Code: iam.ErrCodeNoSuchEntityException, Error: fmt.Errorf("service account %s not found", saId)}
}

// ListServiceAccounts lists service accounts, optionally filtered by parent user.
func (e *EmbeddedIamApi) ListServiceAccounts(s3cfg *iam_pb.S3ApiConfiguration, values url.Values) iamListServiceAccountsResponse {
	var resp iamListServiceAccountsResponse
	parentUser := values.Get("ParentUser") // Optional filter

	for _, sa := range s3cfg.ServiceAccounts {
		if parentUser != "" && sa.ParentUser != parentUser {
			continue
		}
		if sa.Credential == nil {
			glog.Warningf("Service account %s has nil credential, skipping", sa.Id)
			continue
		}
		status := iamAccessKeyStatusActive
		if sa.Disabled {
			status = iamAccessKeyStatusInactive
		}
		info := &iamServiceAccountInfo{
			ServiceAccountId: sa.Id,
			ParentUser:       sa.ParentUser,
			Description:      sa.Description,
			AccessKeyId:      sa.Credential.AccessKey,
			Status:           status,
			CreateDate:       time.Unix(sa.CreatedAt, 0).Format(time.RFC3339),
		}
		if sa.Expiration > 0 {
			expStr := time.Unix(sa.Expiration, 0).Format(time.RFC3339)
			info.Expiration = &expStr
		}
		resp.ListServiceAccountsResult.ServiceAccounts = append(resp.ListServiceAccountsResult.ServiceAccounts, info)
	}

	return resp
}

// GetServiceAccount retrieves a service account by ID.
func (e *EmbeddedIamApi) GetServiceAccount(s3cfg *iam_pb.S3ApiConfiguration, values url.Values) (iamGetServiceAccountResponse, *iamError) {
	var resp iamGetServiceAccountResponse
	saId := values.Get("ServiceAccountId")

	if saId == "" {
		return resp, &iamError{Code: iam.ErrCodeInvalidInputException, Error: fmt.Errorf("ServiceAccountId is required")}
	}

	for _, sa := range s3cfg.ServiceAccounts {
		if sa.Id == saId {
			if sa.Credential == nil {
				return resp, &iamError{Code: iam.ErrCodeServiceFailureException, Error: fmt.Errorf("service account %s has no credentials", saId)}
			}
			status := iamAccessKeyStatusActive
			if sa.Disabled {
				status = iamAccessKeyStatusInactive
			}
			resp.GetServiceAccountResult.ServiceAccount = iamServiceAccountInfo{
				ServiceAccountId: sa.Id,
				ParentUser:       sa.ParentUser,
				Description:      sa.Description,
				AccessKeyId:      sa.Credential.AccessKey,
				Status:           status,
				CreateDate:       time.Unix(sa.CreatedAt, 0).Format(time.RFC3339),
			}
			if sa.Expiration > 0 {
				expStr := time.Unix(sa.Expiration, 0).Format(time.RFC3339)
				resp.GetServiceAccountResult.ServiceAccount.Expiration = &expStr
			}
			return resp, nil
		}
	}

	return resp, &iamError{Code: iam.ErrCodeNoSuchEntityException, Error: fmt.Errorf("service account %s not found", saId)}
}

// UpdateServiceAccount updates a service account's status, description, or expiration.
func (e *EmbeddedIamApi) UpdateServiceAccount(s3cfg *iam_pb.S3ApiConfiguration, values url.Values) (iamUpdateServiceAccountResponse, *iamError) {
	var resp iamUpdateServiceAccountResponse
	saId := values.Get("ServiceAccountId")
	newStatus := values.Get("Status")
	newDescription := values.Get("Description")
	newExpirationStr := values.Get("Expiration")

	if saId == "" {
		return resp, &iamError{Code: iam.ErrCodeInvalidInputException, Error: fmt.Errorf("ServiceAccountId is required")}
	}

	for _, sa := range s3cfg.ServiceAccounts {
		if sa.Id == saId {
			// Update status if provided
			if newStatus != "" {
				if err := iamValidateStatus(newStatus); err != nil {
					return resp, &iamError{Code: iam.ErrCodeInvalidInputException, Error: err}
				}
				sa.Disabled = (newStatus == iamAccessKeyStatusInactive)
			}
			// Update description if provided (check for key existence to allow clearing)
			if _, hasDescription := values["Description"]; hasDescription {
				if len(newDescription) > MaxDescriptionLength {
					return resp, &iamError{
						Code:  iam.ErrCodeInvalidInputException,
						Error: fmt.Errorf("description exceeds maximum length of %d characters", MaxDescriptionLength),
					}
				}
				sa.Description = newDescription
			}
			// Update expiration if provided (check for key existence to allow clearing to 0)
			if _, hasExpiration := values["Expiration"]; hasExpiration {
				if newExpirationStr != "" {
					newExpiration, err := strconv.ParseInt(newExpirationStr, 10, 64)
					if err != nil {
						return resp, &iamError{Code: iam.ErrCodeInvalidInputException, Error: fmt.Errorf("invalid expiration format: %w", err)}
					}
					// Validate expiration value
					if newExpiration < 0 {
						return resp, &iamError{Code: iam.ErrCodeInvalidInputException, Error: fmt.Errorf("expiration must not be negative")}
					}
					if newExpiration > 0 && newExpiration < time.Now().Unix() {
						return resp, &iamError{Code: iam.ErrCodeInvalidInputException, Error: fmt.Errorf("expiration must be in the future")}
					}
					// 0 is explicitly allowed to clear expiration
					sa.Expiration = newExpiration
				} else {
					// Empty string means clear expiration (set to 0 = no expiration)
					sa.Expiration = 0
				}
			}
			return resp, nil
		}
	}

	return resp, &iamError{Code: iam.ErrCodeNoSuchEntityException, Error: fmt.Errorf("service account %s not found", saId)}
}

// extractPolicyNameFromArn extracts policy name from ARN
// ARN format: arn:aws:iam:::policy/PolicyName or arn:aws:iam::account:policy/PolicyName
func extractPolicyNameFromArn(arn string) string {
	parts := strings.Split(arn, "/")
	if len(parts) >= 2 && strings.Contains(arn, ":policy/") {
		return parts[len(parts)-1]
	}
	return ""
}

// AttachUserPolicy attaches a managed policy to a user
func (e *EmbeddedIamApi) AttachUserPolicy(s3cfg *iam_pb.S3ApiConfiguration, values url.Values) (iamAttachUserPolicyResponse, *iamError) {
	var resp iamAttachUserPolicyResponse
	userName := values.Get("UserName")
	policyArn := values.Get("PolicyArn")

	if userName == "" {
		return resp, &iamError{Code: iam.ErrCodeInvalidInputException, Error: fmt.Errorf("UserName is required")}
	}
	if policyArn == "" {
		return resp, &iamError{Code: iam.ErrCodeInvalidInputException, Error: fmt.Errorf("PolicyArn is required")}
	}

	// Extract policy name from ARN
	policyName := extractPolicyNameFromArn(policyArn)
	if policyName == "" {
		return resp, &iamError{Code: iam.ErrCodeInvalidInputException, Error: fmt.Errorf("invalid policy ARN format")}
	}

	// Verify policy exists
	ctx := context.Background()
	policy, err := e.credentialManager.GetPolicy(ctx, policyName)
	if err != nil || policy == nil {
		return resp, &iamError{Code: iam.ErrCodeNoSuchEntityException, Error: fmt.Errorf("policy %s not found", policyName)}
	}

	// Find user and attach policy
	for _, ident := range s3cfg.Identities {
		if ident.Name == userName {
			// Check if already attached - idempotent success
			for _, p := range ident.PolicyNames {
				if p == policyName {
					return resp, nil
				}
			}
			ident.PolicyNames = append(ident.PolicyNames, policyName)
			return resp, nil
		}
	}

	return resp, &iamError{Code: iam.ErrCodeNoSuchEntityException, Error: fmt.Errorf(iamUserDoesNotExist, userName)}
}

// DetachUserPolicy detaches a managed policy from a user
func (e *EmbeddedIamApi) DetachUserPolicy(s3cfg *iam_pb.S3ApiConfiguration, values url.Values) (iamDetachUserPolicyResponse, *iamError) {
	var resp iamDetachUserPolicyResponse
	userName := values.Get("UserName")
	policyArn := values.Get("PolicyArn")

	if userName == "" {
		return resp, &iamError{Code: iam.ErrCodeInvalidInputException, Error: fmt.Errorf("UserName is required")}
	}
	if policyArn == "" {
		return resp, &iamError{Code: iam.ErrCodeInvalidInputException, Error: fmt.Errorf("PolicyArn is required")}
	}

	policyName := extractPolicyNameFromArn(policyArn)
	if policyName == "" {
		return resp, &iamError{Code: iam.ErrCodeInvalidInputException, Error: fmt.Errorf("invalid policy ARN format")}
	}

	for _, ident := range s3cfg.Identities {
		if ident.Name == userName {
			found := false
			var newPolicies []string
			for _, p := range ident.PolicyNames {
				if p == policyName {
					found = true
				} else {
					newPolicies = append(newPolicies, p)
				}
			}
			if !found {
				return resp, &iamError{Code: iam.ErrCodeNoSuchEntityException, Error: fmt.Errorf("policy %s is not attached to user %s", policyName, userName)}
			}
			ident.PolicyNames = newPolicies
			return resp, nil
		}
	}

	return resp, &iamError{Code: iam.ErrCodeNoSuchEntityException, Error: fmt.Errorf(iamUserDoesNotExist, userName)}
}

// ListAttachedUserPolicies lists managed policies attached to a user
func (e *EmbeddedIamApi) ListAttachedUserPolicies(s3cfg *iam_pb.S3ApiConfiguration, values url.Values) (iamListAttachedUserPoliciesResponse, *iamError) {
	var resp iamListAttachedUserPoliciesResponse
	userName := values.Get("UserName")

	if userName == "" {
		return resp, &iamError{Code: iam.ErrCodeInvalidInputException, Error: fmt.Errorf("UserName is required")}
	}

	for _, ident := range s3cfg.Identities {
		if ident.Name == userName {
			for _, policyName := range ident.PolicyNames {
				resp.ListAttachedUserPoliciesResult.AttachedPolicies = append(
					resp.ListAttachedUserPoliciesResult.AttachedPolicies,
					&iamAttachedPolicy{
						PolicyName: policyName,
						PolicyArn:  fmt.Sprintf("arn:aws:iam:::policy/%s", policyName),
					},
				)
			}
			return resp, nil
		}
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

		// Authenticate BEFORE parsing form.
		// ParseForm() reads and consumes the request body, but signature verification
		// needs to hash the body for IAM requests (service != "s3").
		// The streamHashRequestBody function in auth_signature_v4.go preserves the body
		// after reading it, so ParseForm() will work correctly after authentication.
		identity, errCode := e.iam.AuthSignatureOnly(r)
		if errCode != s3err.ErrNone {
			s3err.WriteErrorResponse(w, r, errCode)
			return
		}

		// Now parse form to get Action and UserName (body was preserved by auth)
		if err := r.ParseForm(); err != nil {
			s3err.WriteErrorResponse(w, r, s3err.ErrInvalidRequest)
			return
		}

		action := r.Form.Get("Action")
		targetUserName := r.PostForm.Get("UserName")

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
			// Operating on another user: require admin or permission
			if !identity.isAdmin() {
				if e.iam.VerifyActionPermission(r, identity, Action("iam:"+action), "arn:aws:iam:::*", "") != s3err.ErrNone {
					s3err.WriteErrorResponse(w, r, s3err.ErrAccessDenied)
					return
				}
			}
		} else {
			// All other IAM actions require admin or permission
			if !identity.isAdmin() {
				if e.iam.VerifyActionPermission(r, identity, Action("iam:"+action), "arn:aws:iam:::*", "") != s3err.ErrNone {
					s3err.WriteErrorResponse(w, r, s3err.ErrAccessDenied)
					return
				}
			}
		}

		f(w, r)
	}
}

// ExecuteAction executes an IAM action with the given values.
// If skipPersist is true, the changed configuration is not saved to the persistent store.
func (e *EmbeddedIamApi) ExecuteAction(values url.Values, skipPersist bool) (interface{}, *iamError) {
	// Lock to prevent concurrent read-modify-write race conditions
	e.policyLock.Lock()
	defer e.policyLock.Unlock()

	action := values.Get("Action")
	if e.readOnly {
		switch action {
		case "ListUsers", "ListAccessKeys", "GetUser", "GetUserPolicy", "ListServiceAccounts", "GetServiceAccount", "ListAttachedUserPolicies":
			// Allowed read-only actions
		default:
			return nil, &iamError{Code: s3err.GetAPIError(s3err.ErrAccessDenied).Code, Error: fmt.Errorf("IAM write operations are disabled on this server")}
		}
	}

	s3cfg := &iam_pb.S3ApiConfiguration{}
	if err := e.GetS3ApiConfiguration(s3cfg); err != nil && !errors.Is(err, filer_pb.ErrNotFound) {
		return nil, &iamError{Code: s3err.GetAPIError(s3err.ErrInternalError).Code, Error: fmt.Errorf("failed to get s3 api configuration: %v", err)}
	}

	glog.V(4).Infof("IAM ExecuteAction: %+v", values)
	var response interface{}
	var iamErr *iamError
	changed := true
	switch values.Get("Action") {
	case "ListUsers":
		response = e.ListUsers(s3cfg, values)
		changed = false
	case "ListAccessKeys":
		// Note: handleImplicitUsername requires request context which we don't have here for gRPC
		// gRPC callers must provide UserName explicitly
		response = e.ListAccessKeys(s3cfg, values)
		changed = false
	case "CreateUser":
		response, iamErr = e.CreateUser(s3cfg, values)
		if iamErr != nil {
			return nil, iamErr
		}
	case "GetUser":
		userName := values.Get("UserName")
		response, iamErr = e.GetUser(s3cfg, userName)
		if iamErr != nil {
			return nil, iamErr
		}
		changed = false
	case "UpdateUser":
		response, iamErr = e.UpdateUser(s3cfg, values)
		if iamErr != nil {
			return nil, iamErr
		}
	case "DeleteUser":
		userName := values.Get("UserName")
		response, iamErr = e.DeleteUser(s3cfg, userName)
		if iamErr != nil {
			return nil, iamErr
		}
	case "CreateAccessKey":
		response, iamErr = e.CreateAccessKey(s3cfg, values)
		if iamErr != nil {
			glog.Errorf("CreateAccessKey: %+v", iamErr.Error)
			return nil, iamErr
		}
	case "DeleteAccessKey":
		response = e.DeleteAccessKey(s3cfg, values)
	case "CreatePolicy":
		response, iamErr = e.CreatePolicy(s3cfg, values)
		if iamErr != nil {
			glog.Errorf("CreatePolicy: %+v", iamErr.Error)
			return nil, iamErr
		}
	case "DeletePolicy":
		// Managed policies are not stored separately, so deletion is a no-op.
		// Returns success for AWS compatibility.
		response = struct{}{}
		changed = false
	case "PutUserPolicy":
		response, iamErr = e.PutUserPolicy(s3cfg, values)
		if iamErr != nil {
			glog.Errorf("PutUserPolicy: %+v", iamErr.Error)
			return nil, iamErr
		}
	case "GetUserPolicy":
		response, iamErr = e.GetUserPolicy(s3cfg, values)
		if iamErr != nil {
			return nil, iamErr
		}
		changed = false
	case "DeleteUserPolicy":
		response, iamErr = e.DeleteUserPolicy(s3cfg, values)
		if iamErr != nil {
			return nil, iamErr
		}
	case "SetUserStatus":
		response, iamErr = e.SetUserStatus(s3cfg, values)
		if iamErr != nil {
			return nil, iamErr
		}
	case "UpdateAccessKey":
		response, iamErr = e.UpdateAccessKey(s3cfg, values)
		if iamErr != nil {
			return nil, iamErr
		}
	// Service Account actions
	case "CreateServiceAccount":
		createdBy := values.Get("CreatedBy")
		response, iamErr = e.CreateServiceAccount(s3cfg, values, createdBy)
		if iamErr != nil {
			return nil, iamErr
		}
	case "DeleteServiceAccount":
		response, iamErr = e.DeleteServiceAccount(s3cfg, values)
		if iamErr != nil {
			return nil, iamErr
		}
	case "ListServiceAccounts":
		response = e.ListServiceAccounts(s3cfg, values)
		changed = false
	case "GetServiceAccount":
		response, iamErr = e.GetServiceAccount(s3cfg, values)
		if iamErr != nil {
			return nil, iamErr
		}
		changed = false
	case "UpdateServiceAccount":
		response, iamErr = e.UpdateServiceAccount(s3cfg, values)
		if iamErr != nil {
			return nil, iamErr
		}
	// Managed policy attachment actions
	case "AttachUserPolicy":
		response, iamErr = e.AttachUserPolicy(s3cfg, values)
		if iamErr != nil {
			return nil, iamErr
		}
	case "DetachUserPolicy":
		response, iamErr = e.DetachUserPolicy(s3cfg, values)
		if iamErr != nil {
			return nil, iamErr
		}
	case "ListAttachedUserPolicies":
		response, iamErr = e.ListAttachedUserPolicies(s3cfg, values)
		if iamErr != nil {
			return nil, iamErr
		}
		changed = false
	default:
		return nil, &iamError{Code: s3err.GetAPIError(s3err.ErrNotImplemented).Code, Error: errors.New(s3err.GetAPIError(s3err.ErrNotImplemented).Description)}
	}
	if changed {
		if !skipPersist {
			if err := e.PutS3ApiConfiguration(s3cfg); err != nil {
				iamErr = &iamError{Code: iam.ErrCodeServiceFailureException, Error: err}
				return nil, iamErr
			}
		}
		// Reload in-memory identity maps so subsequent LookupByAccessKey calls
		// can see newly created or deleted keys immediately
		if err := e.ReloadConfiguration(); err != nil {
			glog.Errorf("Failed to reload IAM configuration after mutation: %v", err)
			// Don't fail the request since the persistent save succeeded
		}
	}
	return response, nil
}

// DoActions handles IAM API actions.
func (e *EmbeddedIamApi) DoActions(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		s3err.WriteErrorResponse(w, r, s3err.ErrInvalidRequest)
		return
	}
	values := r.PostForm

	// Handle implicit username for HTTP requests
	switch r.Form.Get("Action") {
	case "ListAccessKeys", "CreateAccessKey", "DeleteAccessKey", "UpdateAccessKey":
		e.handleImplicitUsername(r, values)
	case "CreateServiceAccount":
		createdBy := s3_constants.GetIdentityNameFromContext(r)
		values.Set("CreatedBy", createdBy)
	}

	response, iamErr := e.ExecuteAction(values, false)
	if iamErr != nil {
		e.writeIamErrorResponse(w, r, iamErr)
		return
	}

	// Set RequestId for AWS compatibility
	if r, ok := response.(interface{ SetRequestId() }); ok {
		r.SetRequestId()
	}
	s3err.WriteXMLResponse(w, r, http.StatusOK, response)
}
