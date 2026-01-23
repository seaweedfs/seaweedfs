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
	"github.com/seaweedfs/seaweedfs/weed/stats"
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

	stats.IamRequestCounter.WithLabelValues(r.Form.Get("Action"), iamErr.Code).Inc()

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
func (e *EmbeddedIamApi) ListUsers(ctx context.Context, values url.Values) (iamListUsersResponse, *iamError) {
	var resp iamListUsersResponse
	// Get list of usernames
	usernames, err := e.credentialManager.ListUsers(ctx)
	if err != nil {
		return resp, &iamError{Code: iam.ErrCodeServiceFailureException, Error: err}
	}

	for _, name := range usernames {
		// Capture copy of name for pointer
		n := name 
		resp.ListUsersResult.Users = append(resp.ListUsersResult.Users, &iam.User{UserName: &n})
	}
	return resp, nil
}

// ListAccessKeys lists access keys for a user.
func (e *EmbeddedIamApi) ListAccessKeys(ctx context.Context, values url.Values) (iamListAccessKeysResponse, *iamError) {
	var resp iamListAccessKeysResponse
	userName := values.Get("UserName")

	if userName == "" {
		return resp, &iamError{Code: iam.ErrCodeInvalidInputException, Error: fmt.Errorf("UserName is required")}
	}

	user, err := e.credentialManager.GetUser(ctx, userName)
	if err != nil {
		if err == credential.ErrUserNotFound {
			return resp, &iamError{Code: iam.ErrCodeNoSuchEntityException, Error: fmt.Errorf(iamUserDoesNotExist, userName)}
		}
		return resp, &iamError{Code: iam.ErrCodeServiceFailureException, Error: err}
	}

	for _, cred := range user.Credentials {
		status := cred.Status
		if status == "" {
			status = iamAccessKeyStatusActive
		}
		identName := user.Name
		accessKey := cred.AccessKey
		statusCopy := status
		resp.ListAccessKeysResult.AccessKeyMetadata = append(resp.ListAccessKeysResult.AccessKeyMetadata,
			&iam.AccessKeyMetadata{UserName: &identName, AccessKeyId: &accessKey, Status: &statusCopy},
		)
	}
	return resp, nil
}

// CreateUser creates a new IAM user.
func (e *EmbeddedIamApi) CreateUser(ctx context.Context, values url.Values) (iamCreateUserResponse, *iamError) {
	var resp iamCreateUserResponse
	userName := values.Get("UserName")

	// Validate UserName is not empty
	if userName == "" {
		return resp, &iamError{Code: iam.ErrCodeInvalidInputException, Error: fmt.Errorf("UserName is required")}
	}

	// Check for duplicate user handled by CredentialManager/Store
	
	identity := &iam_pb.Identity{
		Name: userName,
		Actions: []string{},
		Credentials: []*iam_pb.Credential{},
	}

	if err := e.credentialManager.CreateUser(ctx, identity); err != nil {
		if err == credential.ErrUserAlreadyExists {
			return resp, &iamError{Code: iam.ErrCodeEntityAlreadyExistsException, Error: fmt.Errorf("user %s already exists", userName)}
		}
		return resp, &iamError{Code: iam.ErrCodeServiceFailureException, Error: err}
	}

	resp.CreateUserResult.User.UserName = &userName
	return resp, nil
}

// DeleteUser deletes an IAM user.
func (e *EmbeddedIamApi) DeleteUser(ctx context.Context, userName string) (iamDeleteUserResponse, *iamError) {
	var resp iamDeleteUserResponse
	
	// Check if user has service accounts before deleting
	// We need to fetch the user to check this constraint
	user, err := e.credentialManager.GetUser(ctx, userName)
	if err != nil {
		if err == credential.ErrUserNotFound {
			return resp, &iamError{Code: iam.ErrCodeNoSuchEntityException, Error: fmt.Errorf(iamUserDoesNotExist, userName)}
		}
		return resp, &iamError{Code: iam.ErrCodeServiceFailureException, Error: err}
	}

	if len(user.ServiceAccountIds) > 0 {
		return resp, &iamError{
			Code: iam.ErrCodeDeleteConflictException,
			Error: fmt.Errorf("cannot delete user %s: user has %d service account(s). Delete service accounts first",
				userName, len(user.ServiceAccountIds)),
		}
	}

	if err := e.credentialManager.DeleteUser(ctx, userName); err != nil {
		if err == credential.ErrUserNotFound {
			return resp, &iamError{Code: iam.ErrCodeNoSuchEntityException, Error: fmt.Errorf(iamUserDoesNotExist, userName)}
		}
		return resp, &iamError{Code: iam.ErrCodeServiceFailureException, Error: err}
	}
	
	return resp, nil
}

// GetUser gets an IAM user.
func (e *EmbeddedIamApi) GetUser(ctx context.Context, userName string) (iamGetUserResponse, *iamError) {
	var resp iamGetUserResponse
	
	user, err := e.credentialManager.GetUser(ctx, userName)
	if err != nil {
		if err == credential.ErrUserNotFound {
			return resp, &iamError{Code: iam.ErrCodeNoSuchEntityException, Error: fmt.Errorf(iamUserDoesNotExist, userName)}
		}
		return resp, &iamError{Code: iam.ErrCodeServiceFailureException, Error: err}
	}

	resp.GetUserResult.User = iam.User{UserName: &user.Name}
	// Verify formatting: AWS returns UserId, Arn, CreateDate, etc.
	// For now we return what we have (UserName)
	return resp, nil
}

// UpdateUser updates an IAM user.
func (e *EmbeddedIamApi) UpdateUser(ctx context.Context, values url.Values) (iamUpdateUserResponse, *iamError) {
	var resp iamUpdateUserResponse
	userName := values.Get("UserName")
	newUserName := values.Get("NewUserName")
	
	if newUserName == "" {
		return resp, nil
	}

	// Fetch existing user to ensure it exists and get its data
	user, err := e.credentialManager.GetUser(ctx, userName)
	if err != nil {
		if err == credential.ErrUserNotFound {
			return resp, &iamError{Code: iam.ErrCodeNoSuchEntityException, Error: fmt.Errorf(iamUserDoesNotExist, userName)}
		}
		return resp, &iamError{Code: iam.ErrCodeServiceFailureException, Error: err}
	}
	
	// Create a copy to avoid modifying shared references
	userCopy := proto.Clone(user).(*iam_pb.Identity)
	userCopy.Name = newUserName
	
	if err := e.credentialManager.UpdateUser(ctx, userName, userCopy); err != nil {
		if err == credential.ErrUserNotFound {
			return resp, &iamError{Code: iam.ErrCodeNoSuchEntityException, Error: fmt.Errorf(iamUserDoesNotExist, userName)}
		}
		if err == credential.ErrUserAlreadyExists {
			return resp, &iamError{Code: iam.ErrCodeEntityAlreadyExistsException, Error: fmt.Errorf("user %s already exists", newUserName)}
		}
		return resp, &iamError{Code: iam.ErrCodeServiceFailureException, Error: err}
	}

	return resp, nil
}

// CreateAccessKey creates an access key for a user.
func (e *EmbeddedIamApi) CreateAccessKey(ctx context.Context, values url.Values) (iamCreateAccessKeyResponse, *iamError) {
	var resp iamCreateAccessKeyResponse
	userName := values.Get("UserName")
	status := iam.StatusTypeActive

	// Generate AWS-standard access key
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

	cred := &iam_pb.Credential{
		AccessKey: accessKeyId, 
		SecretKey: secretAccessKey, 
		Status: iamAccessKeyStatusActive,
	}

	// Defensive check
	if cred.Status == "" {
		cred.Status = "Active"
	}

	if err := e.credentialManager.CreateAccessKey(ctx, userName, cred); err != nil {
		if err == credential.ErrUserNotFound {
			return resp, &iamError{Code: iam.ErrCodeNoSuchEntityException, Error: fmt.Errorf(iamUserDoesNotExist, userName)}
		}
		return resp, &iamError{Code: iam.ErrCodeServiceFailureException, Error: err}
	}
	
	return resp, nil
}

// DeleteAccessKey deletes an access key for a user.
func (e *EmbeddedIamApi) DeleteAccessKey(ctx context.Context, values url.Values) iamDeleteAccessKeyResponse {
	var resp iamDeleteAccessKeyResponse
	userName := values.Get("UserName")
	accessKeyId := values.Get("AccessKeyId")
	
	// Error handling is swallowed in original implementation (it returns empty resp on error)
	// We mimic this but log error
	if err := e.credentialManager.DeleteAccessKey(ctx, userName, accessKeyId); err != nil {
		glog.V(1).Infof("DeleteAccessKey: %v", err)
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
func (e *EmbeddedIamApi) PutUserPolicy(ctx context.Context, values url.Values) (iamPutUserPolicyResponse, *iamError) {
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

	user, err := e.credentialManager.GetUser(ctx, userName)
	if err != nil {
		if err == credential.ErrUserNotFound {
			return resp, &iamError{Code: iam.ErrCodeNoSuchEntityException, Error: fmt.Errorf(iamUserDoesNotExist, userName)}
		}
		return resp, &iamError{Code: iam.ErrCodeServiceFailureException, Error: err}
	}

	user.Actions = actions
	
	if err := e.credentialManager.UpdateUser(ctx, userName, user); err != nil {
		return resp, &iamError{Code: iam.ErrCodeServiceFailureException, Error: err}
	}
	
	return resp, nil
}

// GetUserPolicy gets the policy attached to a user.
func (e *EmbeddedIamApi) GetUserPolicy(ctx context.Context, values url.Values) (iamGetUserPolicyResponse, *iamError) {
	var resp iamGetUserPolicyResponse
	userName := values.Get("UserName")
	policyName := values.Get("PolicyName")

	user, err := e.credentialManager.GetUser(ctx, userName)
	if err != nil {
		if err == credential.ErrUserNotFound {
			return resp, &iamError{Code: iam.ErrCodeNoSuchEntityException, Error: fmt.Errorf(iamUserDoesNotExist, userName)}
		}
		return resp, &iamError{Code: iam.ErrCodeServiceFailureException, Error: err}
	}

	resp.GetUserPolicyResult.UserName = userName
	resp.GetUserPolicyResult.PolicyName = policyName
	if len(user.Actions) == 0 {
		return resp, &iamError{Code: iam.ErrCodeNoSuchEntityException, Error: errors.New("no actions found")}
	}

	policyDocument := policy_engine.PolicyDocument{Version: iamPolicyDocumentVersion}
	statements := make(map[string][]string)
	for _, action := range user.Actions {
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

// DeleteUserPolicy removes the inline policy from a user (clears their actions).
func (e *EmbeddedIamApi) DeleteUserPolicy(ctx context.Context, values url.Values) (iamDeleteUserPolicyResponse, *iamError) {
	var resp iamDeleteUserPolicyResponse
	userName := values.Get("UserName")

	user, err := e.credentialManager.GetUser(ctx, userName)
	if err != nil {
		if err == credential.ErrUserNotFound {
			return resp, &iamError{Code: iam.ErrCodeNoSuchEntityException, Error: fmt.Errorf(iamUserDoesNotExist, userName)}
		}
		return resp, &iamError{Code: iam.ErrCodeServiceFailureException, Error: err}
	}

	user.Actions = nil
	if err := e.credentialManager.UpdateUser(ctx, userName, user); err != nil {
		return resp, &iamError{Code: iam.ErrCodeServiceFailureException, Error: err}
	}

	return resp, nil
}

// SetUserStatus enables or disables a user without deleting them.
func (e *EmbeddedIamApi) SetUserStatus(ctx context.Context, values url.Values) (iamSetUserStatusResponse, *iamError) {
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

	user, err := e.credentialManager.GetUser(ctx, userName)
	if err != nil {
		if err == credential.ErrUserNotFound {
			return resp, &iamError{Code: iam.ErrCodeNoSuchEntityException, Error: fmt.Errorf(iamUserDoesNotExist, userName)}
		}
		return resp, &iamError{Code: iam.ErrCodeServiceFailureException, Error: err}
	}

	user.Disabled = (status == iamAccessKeyStatusInactive)
	
	if err := e.credentialManager.UpdateUser(ctx, userName, user); err != nil {
		return resp, &iamError{Code: iam.ErrCodeServiceFailureException, Error: err}
	}

	return resp, nil
}

// UpdateAccessKey updates the status of an access key (Active or Inactive).
func (e *EmbeddedIamApi) UpdateAccessKey(ctx context.Context, values url.Values) (iamUpdateAccessKeyResponse, *iamError) {
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

	user, err := e.credentialManager.GetUser(ctx, userName)
	if err != nil {
		if err == credential.ErrUserNotFound {
			return resp, &iamError{Code: iam.ErrCodeNoSuchEntityException, Error: fmt.Errorf(iamUserDoesNotExist, userName)}
		}
		return resp, &iamError{Code: iam.ErrCodeServiceFailureException, Error: err}
	}

	found := false
	for _, cred := range user.Credentials {
		if cred.AccessKey == accessKeyId {
			cred.Status = status
			found = true
			break
		}
	}

	if !found {
		return resp, &iamError{Code: iam.ErrCodeNoSuchEntityException, Error: fmt.Errorf("the access key with id %s for user %s cannot be found", accessKeyId, userName)}
	}

	if err := e.credentialManager.UpdateUser(ctx, userName, user); err != nil {
		return resp, &iamError{Code: iam.ErrCodeServiceFailureException, Error: err}
	}
	
	return resp, nil
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
func (e *EmbeddedIamApi) CreateServiceAccount(ctx context.Context, values url.Values, createdBy string) (iamCreateServiceAccountResponse, *iamError) {
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

	// Verify parent user exists and get data
	parentIdent, err := e.credentialManager.GetUser(ctx, parentUser)
	if err != nil {
		if err == credential.ErrUserNotFound {
			return resp, &iamError{Code: iam.ErrCodeNoSuchEntityException, Error: fmt.Errorf(iamUserDoesNotExist, parentUser)}
		}
		return resp, &iamError{Code: iam.ErrCodeServiceFailureException, Error: err}
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

	// Save Service Account
	if err := e.credentialManager.CreateServiceAccount(ctx, sa); err != nil {
		return resp, &iamError{Code: iam.ErrCodeServiceFailureException, Error: err}
	}

	// Update Parent User Record
	parentIdent.ServiceAccountIds = append(parentIdent.ServiceAccountIds, saId)
	if err := e.credentialManager.UpdateUser(ctx, parentUser, parentIdent); err != nil {
		glog.Errorf("Failed to update parent user %s after creating service account %s: %v. Rolling back...", parentUser, saId, err)

		// ROLLBACK: Delete the service account we just created
		if delErr := e.credentialManager.DeleteServiceAccount(ctx, saId); delErr != nil {
			glog.Errorf("CRITICAL: Failed to rollback service account %s: %v", saId, delErr)
		}

		return resp, &iamError{Code: iam.ErrCodeServiceFailureException, Error: fmt.Errorf("failed to link service account to user")}
	}

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
func (e *EmbeddedIamApi) DeleteServiceAccount(ctx context.Context, values url.Values) (iamDeleteServiceAccountResponse, *iamError) {
	var resp iamDeleteServiceAccountResponse
	saId := values.Get("ServiceAccountId")

	if saId == "" {
		return resp, &iamError{Code: iam.ErrCodeInvalidInputException, Error: fmt.Errorf("ServiceAccountId is required")}
	}

	// Get Service Account to find Parent User
	sa, err := e.credentialManager.GetServiceAccount(ctx, saId)
	if err != nil {
		// If not found, it's already deleted (idempotent), return success
		// But AWS usually returns NoSuchEntity. 
		// We'll return NoSuchEntity to be safe unless it's a specific "not found" error from manager
		// Assuming manager returns error if not found.
		return resp, &iamError{Code: iam.ErrCodeNoSuchEntityException, Error: fmt.Errorf("service account %s not found", saId)}
	}

	// Remove from Parent User
	if sa.ParentUser != "" {
		parentIdent, err := e.credentialManager.GetUser(ctx, sa.ParentUser)
		if err == nil {
			// Filter out the ID
			filtered := parentIdent.ServiceAccountIds[:0]
			changed := false
			for _, id := range parentIdent.ServiceAccountIds {
				if id != saId {
					filtered = append(filtered, id)
				} else {
					changed = true
				}
			}
			if changed {
				parentIdent.ServiceAccountIds = filtered
				// Update parent
				if err := e.credentialManager.UpdateUser(ctx, sa.ParentUser, parentIdent); err != nil {
					glog.Errorf("Failed to update parent user %s when deleting service account %s: %v", sa.ParentUser, saId, err)
					// Proceed to delete SA anyway
				}
			}
		} else {
			glog.Warningf("Parent user %s not found for service account %s", sa.ParentUser, saId)
		}
	}

	// Delete Service Account
	if err := e.credentialManager.DeleteServiceAccount(ctx, saId); err != nil {
		return resp, &iamError{Code: iam.ErrCodeServiceFailureException, Error: err}
	}

	return resp, nil
}

// ListServiceAccounts lists service accounts, optionally filtered by parent user.
func (e *EmbeddedIamApi) ListServiceAccounts(ctx context.Context, values url.Values) iamListServiceAccountsResponse {
	var resp iamListServiceAccountsResponse
	parentUser := values.Get("ParentUser") // Optional filter

	accounts, err := e.credentialManager.ListServiceAccounts(ctx, parentUser)
	if err != nil {
		glog.Errorf("ListServiceAccounts: %v", err)
		return resp
	}

	for _, sa := range accounts {
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
func (e *EmbeddedIamApi) GetServiceAccount(ctx context.Context, values url.Values) (iamGetServiceAccountResponse, *iamError) {
	var resp iamGetServiceAccountResponse
	saId := values.Get("ServiceAccountId")

	if saId == "" {
		return resp, &iamError{Code: iam.ErrCodeInvalidInputException, Error: fmt.Errorf("ServiceAccountId is required")}
	}

	sa, err := e.credentialManager.GetServiceAccount(ctx, saId)
	if err != nil {
		return resp, &iamError{Code: iam.ErrCodeNoSuchEntityException, Error: fmt.Errorf("service account %s not found", saId)}
	}
	
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

// UpdateServiceAccount updates a service account's status, description, or expiration.
func (e *EmbeddedIamApi) UpdateServiceAccount(ctx context.Context, values url.Values) (iamUpdateServiceAccountResponse, *iamError) {
	var resp iamUpdateServiceAccountResponse
	saId := values.Get("ServiceAccountId")
	newStatus := values.Get("Status")
	newDescription := values.Get("Description")
	newExpirationStr := values.Get("Expiration")

	if saId == "" {
		return resp, &iamError{Code: iam.ErrCodeInvalidInputException, Error: fmt.Errorf("ServiceAccountId is required")}
	}

	sa, err := e.credentialManager.GetServiceAccount(ctx, saId)
	if err != nil {
		return resp, &iamError{Code: iam.ErrCodeNoSuchEntityException, Error: fmt.Errorf("service account %s not found", saId)}
	}

	// Update status if provided
	if newStatus != "" {
		if err := iamValidateStatus(newStatus); err != nil {
			return resp, &iamError{Code: iam.ErrCodeInvalidInputException, Error: err}
		}
		sa.Disabled = (newStatus == iamAccessKeyStatusInactive)
	}
	// Update description if provided
	if _, hasDescription := values["Description"]; hasDescription {
		if len(newDescription) > MaxDescriptionLength {
			return resp, &iamError{
				Code:  iam.ErrCodeInvalidInputException,
				Error: fmt.Errorf("description exceeds maximum length of %d characters", MaxDescriptionLength),
			}
		}
		sa.Description = newDescription
	}
	// Update expiration if provided
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

	if err := e.credentialManager.UpdateServiceAccount(ctx, saId, sa); err != nil {
		return resp, &iamError{Code: iam.ErrCodeServiceFailureException, Error: err}
	}

	return resp, nil
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
		response, iamErr = e.ListUsers(r.Context(), values)
		if iamErr != nil {
			e.writeIamErrorResponse(w, r, iamErr)
			return
		}
		changed = false
	case "ListAccessKeys":
		e.handleImplicitUsername(r, values)
		response, iamErr = e.ListAccessKeys(r.Context(), values)
		if iamErr != nil {
			e.writeIamErrorResponse(w, r, iamErr) // ListAccessKeys now can error
			return
		}
		changed = false
	case "CreateUser":
		response, iamErr = e.CreateUser(r.Context(), values)
		if iamErr != nil {
			e.writeIamErrorResponse(w, r, iamErr)
			return
		}
		changed = false
	case "GetUser":
		userName := values.Get("UserName")
		response, iamErr = e.GetUser(r.Context(), userName)
		if iamErr != nil {
			e.writeIamErrorResponse(w, r, iamErr)
			return
		}
		changed = false
	case "UpdateUser":
		response, iamErr = e.UpdateUser(r.Context(), values)
		if iamErr != nil {
			e.writeIamErrorResponse(w, r, iamErr)
			return
		}
		changed = false
	case "DeleteUser":
		userName := values.Get("UserName")
		response, iamErr = e.DeleteUser(r.Context(), userName)
		if iamErr != nil {
			e.writeIamErrorResponse(w, r, iamErr)
			return
		}
		changed = false
	case "CreateAccessKey":
		e.handleImplicitUsername(r, values)
		response, iamErr = e.CreateAccessKey(r.Context(), values)
		if iamErr != nil {
			glog.Errorf("CreateAccessKey: %+v", iamErr.Error)
			e.writeIamErrorResponse(w, r, iamErr)
			return
		}
		changed = false
	case "DeleteAccessKey":
		e.handleImplicitUsername(r, values)
		response = e.DeleteAccessKey(r.Context(), values)
		changed = false
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
		response, iamErr = e.PutUserPolicy(r.Context(), values)
		if iamErr != nil {
			glog.Errorf("PutUserPolicy: %+v", iamErr.Error)
			e.writeIamErrorResponse(w, r, iamErr)
			return
		}
		changed = false
	case "GetUserPolicy":
		response, iamErr = e.GetUserPolicy(r.Context(), values)
		if iamErr != nil {
			e.writeIamErrorResponse(w, r, iamErr)
			return
		}
		changed = false
	case "DeleteUserPolicy":
		response, iamErr = e.DeleteUserPolicy(r.Context(), values)
		if iamErr != nil {
			e.writeIamErrorResponse(w, r, iamErr)
			return
		}
		changed = false
	case "SetUserStatus":
		response, iamErr = e.SetUserStatus(r.Context(), values)
		if iamErr != nil {
			e.writeIamErrorResponse(w, r, iamErr)
			return
		}
		changed = false
	case "UpdateAccessKey":
		e.handleImplicitUsername(r, values)
		response, iamErr = e.UpdateAccessKey(r.Context(), values)
		if iamErr != nil {
			e.writeIamErrorResponse(w, r, iamErr)
			return
		}
		changed = false
	// Service Account actions
	case "CreateServiceAccount":
		createdBy := s3_constants.GetIdentityNameFromContext(r)
		response, iamErr = e.CreateServiceAccount(r.Context(), values, createdBy)
		if iamErr != nil {
			e.writeIamErrorResponse(w, r, iamErr)
			return
		}
		changed = false
	case "DeleteServiceAccount":
		response, iamErr = e.DeleteServiceAccount(r.Context(), values)
		if iamErr != nil {
			e.writeIamErrorResponse(w, r, iamErr)
			return
		}
		changed = false
	case "ListServiceAccounts":
		response = e.ListServiceAccounts(r.Context(), values)
		changed = false
	case "GetServiceAccount":
		response, iamErr = e.GetServiceAccount(r.Context(), values)
		if iamErr != nil {
			e.writeIamErrorResponse(w, r, iamErr)
			return
		}
		changed = false
	case "UpdateServiceAccount":
		response, iamErr = e.UpdateServiceAccount(r.Context(), values)
		if iamErr != nil {
			e.writeIamErrorResponse(w, r, iamErr)
			return
		}
		changed = false
	default:
		errNotImplemented := s3err.GetAPIError(s3err.ErrNotImplemented)
		errorResponse := iamErrorResponse{}
		errorResponse.Error.Code = &errNotImplemented.Code
		errorResponse.Error.Message = &errNotImplemented.Description
		s3err.WriteXMLResponse(w, r, errNotImplemented.HTTPStatusCode, errorResponse)
		return
	}
	// Determine if we need to reload the cache based on the action
	// We do this based on action name to catch all modification actions
	action := r.Form.Get("Action")
	shouldReload := strings.HasPrefix(action, "Create") || 
		strings.HasPrefix(action, "Delete") || 
		strings.HasPrefix(action, "Update") || 
		strings.HasPrefix(action, "Put") || 
		strings.HasPrefix(action, "Set")
	
	if action == "CreatePolicy" {
		shouldReload = false
	}

	if changed {
		// This path is for legacy or unrefactored actions that still rely on monolithic save
		if err := e.PutS3ApiConfiguration(s3cfg); err != nil {
			iamErr = &iamError{Code: iam.ErrCodeServiceFailureException, Error: err}
			e.writeIamErrorResponse(w, r, iamErr)
			return
		}
		shouldReload = true
	}
	
	if shouldReload {
		// Reload in-memory identity maps so subsequent LookupByAccessKey calls
		// can see newly created or deleted keys immediately
		if err := e.iam.LoadS3ApiConfigurationFromCredentialManager(); err != nil {
			glog.Warningf("Failed to reload IAM configuration after mutation: %v", err)
			// Don't fail the request since the persistent save succeeded
		}
	}
	if r, ok := response.(interface{ SetRequestId() }); ok {
		r.SetRequestId()
	}
	s3err.WriteXMLResponse(w, r, http.StatusOK, response)
	stats.IamRequestCounter.WithLabelValues(action, "200").Inc()
}
