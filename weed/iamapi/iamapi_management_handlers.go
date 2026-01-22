package iamapi

import (
	"context"
	"crypto/sha1"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"reflect"
	"strings"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/credential"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/iam/integration"
	"github.com/seaweedfs/seaweedfs/weed/iam/policy"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/iam_pb"
	"github.com/seaweedfs/seaweedfs/weed/iam/policy_engine"
	"github.com/seaweedfs/seaweedfs/weed/iam/constants"
	iam_errors "github.com/seaweedfs/seaweedfs/weed/iam/errors"

	"github.com/aws/aws-sdk-go/service/iam"
)

const (
	charsetUpper            = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	charset                 = charsetUpper + "abcdefghijklmnopqrstuvwxyz/"
	policyDocumentVersion   = "2012-10-17"
	StatementActionAdmin    = "*"
	StatementActionWrite    = "Put*"
	StatementActionWriteAcp = "PutBucketAcl"
	StatementActionRead     = "Get*"
	StatementActionReadAcp  = "GetBucketAcl"
	StatementActionList     = "List*"
	StatementActionTagging  = "Tagging*"
	StatementActionDelete   = "DeleteBucket*"
)

func MapToStatementAction(action string) string {
	switch action {
	case StatementActionAdmin:
		return constants.ActionAdmin
	case StatementActionWrite:
		return constants.ActionWrite
	case StatementActionWriteAcp:
		return constants.ActionWriteAcp
	case StatementActionRead:
		return constants.ActionRead
	case StatementActionReadAcp:
		return constants.ActionReadAcp
	case StatementActionList:
		return constants.ActionList
	case StatementActionTagging:
		return constants.ActionTagging
	case StatementActionDelete:
		return constants.ActionDeleteBucket
	default:
		return ""
	}
}

func MapToIdentitiesAction(action string) string {
	switch action {
	case constants.ActionAdmin:
		return StatementActionAdmin
	case constants.ActionWrite:
		return StatementActionWrite
	case constants.ActionWriteAcp:
		return StatementActionWriteAcp
	case constants.ActionRead:
		return StatementActionRead
	case constants.ActionReadAcp:
		return StatementActionReadAcp
	case constants.ActionList:
		return StatementActionList
	case constants.ActionTagging:
		return StatementActionTagging
	case constants.ActionDeleteBucket:
		return StatementActionDelete
	default:
		return ""
	}
}

const (
	USER_DOES_NOT_EXIST = "the user with name %s cannot be found."
)

type Policies struct {
	Policies map[string]policy_engine.PolicyDocument `json:"policies"`
}

func Hash(s *string) string {
	h := sha1.New()
	h.Write([]byte(*s))
	return fmt.Sprintf("%x", h.Sum(nil))
}


func (iama *IamApiServer) ListUsers(s3cfg *iam_pb.S3ApiConfiguration, values url.Values) (resp ListUsersResponse) {
	// Use credential store instead of in-memory s3cfg
	usernames, err := iama.s3ApiConfig.ListUsers()
	if err != nil {
		glog.Errorf("ListUsers error: %v", err)
		return resp
	}
	
	for _, username := range usernames {
		t := time.Now()
		resp.ListUsersResult.Users = append(resp.ListUsersResult.Users, &iam.User{
			UserName:   &username,
			UserId:     &username,
			Arn:        StringPtr(fmt.Sprintf("arn:aws:iam:::user/%s", username)),
			CreateDate: &t,
		})
	}
	return resp
}

func (iama *IamApiServer) ListAccessKeys(s3cfg *iam_pb.S3ApiConfiguration, values url.Values) (resp ListAccessKeysResponse) {
	userName := values.Get("UserName")
	
	// Use credential store
	user, err := iama.s3ApiConfig.GetUser(userName)
	if err != nil {
		glog.Errorf("ListAccessKeys: GetUser(%s) error: %v", userName, err)
		return resp
	}
	
	for _, cred := range user.Credentials {
		status := cred.Status
		if status == "" {
			status = iam.StatusTypeActive
		}
		
		var createDate *time.Time
		// CreateDate not available in Credential proto
		
		resp.ListAccessKeysResult.AccessKeyMetadata = append(resp.ListAccessKeysResult.AccessKeyMetadata,
			&iam.AccessKeyMetadata{
				UserName:    &user.Name,
				AccessKeyId: &cred.AccessKey,
				Status:      &status,
				CreateDate:  createDate,
			},
		)
	}
	return resp
}

func (iama *IamApiServer) CreateUser(s3cfg *iam_pb.S3ApiConfiguration, values url.Values) (resp CreateUserResponse, iamError *IamError) {
	userName := values.Get("UserName")
	
	identity := &iam_pb.Identity{
		Name:       userName,
		// CreateDate not available in proto
	}
	if err := iama.s3ApiConfig.CreateUser(identity); err != nil {
		glog.Errorf("CreateUser %s: %v", userName, err)
		return resp, &IamError{Code: iam.ErrCodeEntityAlreadyExistsException, Error: err}
	}
	
	resp.CreateUserResult.User.UserName = &userName
	resp.CreateUserResult.User.UserId = &userName
	resp.CreateUserResult.User.Arn = StringPtr(fmt.Sprintf("arn:aws:iam:::user/%s", userName))
	// Return the creation date we just set
	t := time.Now()
	resp.CreateUserResult.User.CreateDate = &t

	return resp, nil
}

func (iama *IamApiServer) DeleteUser(s3cfg *iam_pb.S3ApiConfiguration, userName string) (resp DeleteUserResponse, err *IamError) {
	_, getErr := iama.s3ApiConfig.GetUser(userName)
	if getErr != nil {
		if getErr == credential.ErrUserNotFound || getErr == filer_pb.ErrNotFound {
			return resp, &IamError{Code: iam.ErrCodeNoSuchEntityException, Error: fmt.Errorf(USER_DOES_NOT_EXIST, userName)}
		}
		return resp, &IamError{Code: iam.ErrCodeServiceFailureException, Error: getErr}
	}
	if err := iama.s3ApiConfig.DeleteUser(userName); err != nil {
		return resp, &IamError{Code: iam.ErrCodeNoSuchEntityException, Error: fmt.Errorf(USER_DOES_NOT_EXIST, userName)}
	}
	return resp, nil
}

func (iama *IamApiServer) GetUser(s3cfg *iam_pb.S3ApiConfiguration, userName string) (resp GetUserResponse, err *IamError) {
	// Use credential store instead of in-memory s3cfg
	user, getErr := iama.s3ApiConfig.GetUser(userName)
	if getErr != nil {
		return resp, &IamError{Code: iam.ErrCodeNoSuchEntityException, Error: fmt.Errorf(USER_DOES_NOT_EXIST, userName)}
	}
	
	t := time.Now()
	resp.GetUserResult.User = iam.User{
		UserName:   &user.Name,
		UserId:     &user.Name,
		Arn:        StringPtr(fmt.Sprintf("arn:aws:iam:::user/%s", user.Name)),
		CreateDate: &t,
	}
	return resp, nil
}

func (iama *IamApiServer) UpdateUser(s3cfg *iam_pb.S3ApiConfiguration, values url.Values) (resp UpdateUserResponse, err *IamError) {
	userName := values.Get("UserName")
	newUserName := values.Get("NewUserName")
	
	if newUserName == "" || userName == newUserName {
		return resp, nil
	}

	// Fetch existing
	user, getErr := iama.s3ApiConfig.GetUser(userName)
	if getErr != nil {
		return resp, &IamError{Code: iam.ErrCodeNoSuchEntityException, Error: fmt.Errorf(USER_DOES_NOT_EXIST, userName)}
	}

	// Create new - DEEP COPY to avoid aliasing bugs
	newUser := &iam_pb.Identity{
		Name: newUserName,
		Credentials: make([]*iam_pb.Credential, len(user.Credentials)),
		Actions: make([]string, len(user.Actions)),
	}
	copy(newUser.Credentials, user.Credentials)
	copy(newUser.Actions, user.Actions)
	
	if createErr := iama.s3ApiConfig.CreateUser(newUser); createErr != nil {
		return resp, &IamError{Code: iam.ErrCodeEntityAlreadyExistsException, Error: createErr}
	}
	
	// Delete old - MUST SUCCEED, rollback if it fails
	if deleteErr := iama.s3ApiConfig.DeleteUser(userName); deleteErr != nil {
		glog.Errorf("Failed to delete old user %s after rename, rolling back: %v", userName, deleteErr)
		// Rollback: delete the new user we just created
		if rollbackErr := iama.s3ApiConfig.DeleteUser(newUserName); rollbackErr != nil {
			glog.Errorf("CRITICAL: Failed to rollback user creation for %s: %v", newUserName, rollbackErr)
		}
		return resp, &IamError{
			Code: iam.ErrCodeServiceFailureException, 
			Error: fmt.Errorf("failed to complete user rename: %w", deleteErr),
		}
	}
	
	return resp, nil
}

func GetPolicyDocument(policy *string) (policy_engine.PolicyDocument, error) {
	var policyDocument policy_engine.PolicyDocument
	if err := json.Unmarshal([]byte(*policy), &policyDocument); err != nil {
		return policy_engine.PolicyDocument{}, err
	}
	return policyDocument, nil
}

func (iama *IamApiServer) CreatePolicy(s3cfg *iam_pb.S3ApiConfiguration, values url.Values) (resp CreatePolicyResponse, iamError *IamError) {
	policyName := values.Get("PolicyName")
	policyDocumentString := values.Get("PolicyDocument")
	policyDocument, err := GetPolicyDocument(&policyDocumentString)
	if err != nil {
		return CreatePolicyResponse{}, &IamError{Code: iam.ErrCodeMalformedPolicyDocumentException, Error: err}
	}
	policyId := Hash(&policyDocumentString)
	arn := fmt.Sprintf("arn:aws:iam:::policy/%s", policyName)
	
	// Populate policy metadata
	description := values.Get("Description")
	now := time.Now()
	policyDocument.Metadata = &policy_engine.PolicyMetadata{
		Name:        policyName,
		PolicyId:    policyId,
		Arn:         arn,
		Description: description,
		CreatedAt:   now,
		UpdatedAt:   now,
	}

	resp.CreatePolicyResult.Policy.PolicyName = &policyName
	resp.CreatePolicyResult.Policy.Arn = &arn
	resp.CreatePolicyResult.Policy.PolicyId = &policyId
	// Check if policy already exists
	_, err = iama.s3ApiConfig.GetPolicy(policyName)
	if err == nil {
		return resp, &IamError{Code: iam.ErrCodeEntityAlreadyExistsException, Error: fmt.Errorf("policy with name %s already exists", policyName)}
	}
	if err != filer_pb.ErrNotFound {
		return resp, &IamError{Code: iam.ErrCodeServiceFailureException, Error: err}
	}

	if err = iama.s3ApiConfig.PutPolicy(policyName, &policyDocument); err != nil {
		return resp, &IamError{Code: iam.ErrCodeServiceFailureException, Error: err}
	}
	return resp, nil
}

type IamError struct {
	Code  string
	Error error
}

// https://docs.aws.amazon.com/IAM/latest/APIReference/API_PutUserPolicy.html
func (iama *IamApiServer) PutUserPolicy(s3cfg *iam_pb.S3ApiConfiguration, values url.Values) (resp PutUserPolicyResponse, iamError *IamError) {
	userName := values.Get("UserName")
	policyName := values.Get("PolicyName")
	policyDocumentString := values.Get("PolicyDocument")
	policyDocument, err := GetPolicyDocument(&policyDocumentString)
	if err != nil {
		return PutUserPolicyResponse{}, &IamError{Code: iam.ErrCodeMalformedPolicyDocumentException, Error: err}
	}
	iama.setPolicyCache(policyName, &policyDocument)
	actions, err := GetActions(&policyDocument)
	if err != nil {
		return PutUserPolicyResponse{}, &IamError{Code: iam.ErrCodeMalformedPolicyDocumentException, Error: err}
	}
	// Log the actions
	glog.V(3).Infof("PutUserPolicy: actions=%v", actions)
	// Atomic Update
	user, err := iama.s3ApiConfig.GetUser(userName)
	if err != nil {
		return PutUserPolicyResponse{}, &IamError{Code: iam.ErrCodeNoSuchEntityException, Error: fmt.Errorf(USER_DOES_NOT_EXIST, userName)}
	}
	
	user.Actions = actions
	
	if err := iama.s3ApiConfig.UpdateUser(userName, user); err != nil {
		return PutUserPolicyResponse{}, &IamError{Code: iam.ErrCodeServiceFailureException, Error: err}
	}
	return resp, nil
}

func (iama *IamApiServer) GetUserPolicy(s3cfg *iam_pb.S3ApiConfiguration, values url.Values) (resp GetUserPolicyResponse, err *IamError) {
	userName := values.Get("UserName")
	policyName := values.Get("PolicyName")
	
	// Use credential store
	user, getErr := iama.s3ApiConfig.GetUser(userName)
	if getErr != nil {
		return resp, &IamError{Code: iam.ErrCodeNoSuchEntityException, Error: fmt.Errorf(USER_DOES_NOT_EXIST, userName)}
	}

	resp.GetUserPolicyResult.UserName = userName
	resp.GetUserPolicyResult.PolicyName = policyName
	if len(user.Actions) == 0 {
		return resp, &IamError{Code: iam.ErrCodeNoSuchEntityException, Error: errors.New("no actions found")}
	}

	policyDocument := policy_engine.PolicyDocument{Version: policyDocumentVersion}
	statements := make(map[string][]string)
	for _, action := range user.Actions {
		// parse "Read:EXAMPLE-BUCKET"
		act := strings.Split(action, ":")

		resource := "*"
		if len(act) == 2 {
			resource = fmt.Sprintf("arn:aws:s3:::%s/*", act[1])
		}
		statements[resource] = append(statements[resource],
			fmt.Sprintf("s3:%s", MapToIdentitiesAction(act[0])),
		)
	}
	for resource, actions := range statements {
		isEqAction := false
		for i, statement := range policyDocument.Statement {
			if reflect.DeepEqual(statement.Action.Strings(), actions) {
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
	policyDocumentJSON, marshalErr := json.Marshal(policyDocument)
	if marshalErr != nil {
		return resp, &IamError{Code: iam.ErrCodeServiceFailureException, Error: marshalErr}
	}
	resp.GetUserPolicyResult.PolicyDocument = string(policyDocumentJSON)
	return resp, nil
}

func (iama *IamApiServer) DeleteUserPolicy(s3cfg *iam_pb.S3ApiConfiguration, values url.Values) (resp PutUserPolicyResponse, err *IamError) {
	userName := values.Get("UserName")
	
	user, getErr := iama.s3ApiConfig.GetUser(userName)
	if getErr != nil {
		return resp, &IamError{Code: iam.ErrCodeNoSuchEntityException, Error: fmt.Errorf(USER_DOES_NOT_EXIST, userName)}
	}
	
	// Clear actions (remove inline policy)
	// We should probably check PolicyName if we supported multiple?
	// But PutUserPolicy overwrites all actions. So Delete should clear all.
	// Existing code (buggy) deleted user. We fix it to clear actions.
	user.Actions = []string{}
	
	if updateErr := iama.s3ApiConfig.UpdateUser(userName, user); updateErr != nil {
		return resp, &IamError{Code: iam.ErrCodeServiceFailureException, Error: updateErr}
	}
	
	return resp, nil
}

func GetActions(policy *policy_engine.PolicyDocument) ([]string, error) {
	var actions []string

	for _, statement := range policy.Statement {
		if statement.Effect != policy_engine.PolicyEffectAllow {
			return nil, fmt.Errorf("not a valid effect: '%s'. Only 'Allow' is possible", statement.Effect)
		}
		for _, resource := range statement.Resource.Strings() {
			var path string
			if resource == "*" {
				path = "*"
			} else {
				// Parse "arn:aws:s3:::my-bucket/shared/*"
				res := strings.Split(resource, ":")
				if len(res) == 6 && res[0] == "arn" && res[1] == "aws" && res[2] == "s3" {
					path = res[5]
				} else {
					// Fallback or ignore? strict mode ignored it.
					// If strict, we continue
					continue
				}
			}

			for _, action := range statement.Action.Strings() {
				// Parse "s3:Get*" or "*"
				var statementAction string
				if action == "*" {
					statementAction = MapToStatementAction(StatementActionAdmin)
				} else {
					act := strings.Split(action, ":")
					if len(act) == 2 && act[0] == "s3" {
						statementAction = MapToStatementAction(act[1])
					}
				}

				if statementAction == "" {
					continue  // Ignore unknown actions instead of erroring? Or strict? 
					// Old code error'd on unknown action map.
					// MapToStatementAction returns "" for unknown.
				}
				
				if path == "*" {
					actions = append(actions, statementAction)
					continue
				}
				actions = append(actions, fmt.Sprintf("%s:%s", statementAction, path))
			}
		}
	}
	return actions, nil
}

func (iama *IamApiServer) CreateAccessKey(s3cfg *iam_pb.S3ApiConfiguration, values url.Values) (resp CreateAccessKeyResponse, iamError *IamError) {
	userName := values.Get("UserName")
	status := iam.StatusTypeActive
	accessKeyId, err := generateSecureAccessKey()
	if err != nil {
		return resp, &IamError{
			Code:  iam.ErrCodeServiceFailureException,
			Error: err,
		}
	}
	secretAccessKey, err := generateSecureSecretKey()
	if err != nil {
		return resp, &IamError{
			Code:  iam.ErrCodeServiceFailureException,
			Error: err,
		}
	}
	resp.CreateAccessKeyResult.AccessKey.AccessKeyId = &accessKeyId
	resp.CreateAccessKeyResult.AccessKey.SecretAccessKey = &secretAccessKey
	resp.CreateAccessKeyResult.AccessKey.UserName = &userName
	resp.CreateAccessKeyResult.AccessKey.Status = &status
	
	cred := &iam_pb.Credential{
		AccessKey: accessKeyId,
		SecretKey: secretAccessKey,
		// IsActive and CreateDate not available in proto
	}

	if err := iama.s3ApiConfig.CreateAccessKey(userName, cred); err != nil {
		return resp, &IamError{Code: iam.ErrCodeServiceFailureException, Error: err}
	}

	// Log success WITHOUT exposing the secret key (security)
	glog.V(2).Infof("CreateAccessKey successful for user %s: AccessKeyId=%s", userName, accessKeyId)

	return resp, nil
}

func (iama *IamApiServer) DeleteAccessKey(s3cfg *iam_pb.S3ApiConfiguration, values url.Values) (resp DeleteAccessKeyResponse, iamError *IamError) {
	userName := values.Get("UserName")
	accessKeyId := values.Get("AccessKeyId")
	
	if err := iama.s3ApiConfig.DeleteAccessKey(userName, accessKeyId); err != nil {
		return resp, &IamError{Code: iam.ErrCodeNoSuchEntityException, Error: err}
	}
	return resp, nil
}

// UpdateAccessKey updates the status of an access key
// https://docs.aws.amazon.com/IAM/latest/APIReference/API_UpdateAccessKey.html
func (iama *IamApiServer) UpdateAccessKey(s3cfg *iam_pb.S3ApiConfiguration, values url.Values) (resp UpdateAccessKeyResponse, iamError *IamError) {
	userName := values.Get("UserName")
	accessKeyId := values.Get("AccessKeyId")
	status := values.Get("Status")

	if accessKeyId == "" || status == "" {
		return resp, &IamError{Code: "ValidationError", Error: fmt.Errorf("AccessKeyId and Status are required")}
	}

	if status != "Active" && status != "Inactive" {
		return resp, &IamError{Code: "ValidationError", Error: fmt.Errorf("Status must be 'Active' or 'Inactive'")}
	}

	// isActive := status == "Active"

	// If UserName is not provided, find the user that owns this access key
	if userName == "" {
		// Must scan all users to find the owner (expensive but correct)
		usernames, err := iama.s3ApiConfig.ListUsers()
		if err != nil {
			return resp, &IamError{Code: iam.ErrCodeServiceFailureException, Error: err}
		}
		
		found := false
		for _, username := range usernames {
			user, err := iama.s3ApiConfig.GetUser(username)
			if err != nil {
				continue // Skip users we can't load
			}
			for _, cred := range user.Credentials {
				if cred.AccessKey == accessKeyId {
					userName = username
					found = true
					break
				}
			}
			if found {
				break
			}
		}
		if !found {
			return resp, &IamError{Code: iam.ErrCodeNoSuchEntityException, Error: fmt.Errorf("The Access Key with id %s cannot be found", accessKeyId)}
		}
		// Fall through to atomic update logic with userName now populated
	}

	// Atomic Update
	// 1. Get User
	user, err := iama.s3ApiConfig.GetUser(userName)
	if err != nil {
		return resp, &IamError{Code: iam.ErrCodeNoSuchEntityException, Error: fmt.Errorf("user %s not found", userName)}
	}

	// 2. Modify Credential
	found := false
	for _, cred := range user.Credentials {
		if cred.AccessKey == accessKeyId {
			cred.Status = status
			found = true
			break
		}
	}
	if !found {
		return resp, &IamError{Code: iam.ErrCodeNoSuchEntityException, Error: fmt.Errorf("The Access Key with id %s cannot be found for user %s", accessKeyId, userName)}
	}

	// 3. Update User
	if err := iama.s3ApiConfig.UpdateUser(userName, user); err != nil {
		return resp, &IamError{Code: iam.ErrCodeServiceFailureException, Error: err}
	}
	
	return resp, nil
}

// GetAccessKeyLastUsed retrieves information about when the specified access key was last used
// https://docs.aws.amazon.com/IAM/latest/APIReference/API_GetAccessKeyLastUsed.html
func (iama *IamApiServer) GetAccessKeyLastUsed(s3cfg *iam_pb.S3ApiConfiguration, values url.Values) (resp GetAccessKeyLastUsedResponse, iamError *IamError) {
	accessKeyId := values.Get("AccessKeyId")
	if accessKeyId == "" {
		return resp, &IamError{Code: "ValidationError", Error: fmt.Errorf("AccessKeyId is required")}
	}

	// Use credential store - scan all users
	usernames, err := iama.s3ApiConfig.ListUsers()
	if err != nil {
		return resp, &IamError{Code: iam.ErrCodeServiceFailureException, Error: err}
	}
	
	for _, username := range usernames {
		user, err := iama.s3ApiConfig.GetUser(username)
		if err != nil {
			continue
		}
		for _, cred := range user.Credentials {
			if cred.AccessKey == accessKeyId {
				resp.GetAccessKeyLastUsedResult.UserName = &user.Name
				resp.GetAccessKeyLastUsedResult.AccessKeyLastUsed = &iam.AccessKeyLastUsed{
					Region:      StringPtr("us-east-1"), // Default region
					ServiceName: StringPtr("s3"),        // Default service
				}
				// If we tracked LastUsedDate, we would set it here.
				// Since we don't, AWS documentation says it can be null if not used.
				return resp, nil
			}
		}
	}
	return resp, &IamError{Code: iam.ErrCodeNoSuchEntityException, Error: fmt.Errorf("The Access Key with id %s cannot be found", accessKeyId)}
}

func StringPtr(s string) *string {
	return &s
}

// handleImplicitUsername adds username who signs the request to values if 'username' is not specified
// According to https://awscli.amazonaws.com/v2/documentation/api/latest/reference/iam/create-access-key.html/
// "If you do not specify a user name, IAM determines the user name implicitly based on the Amazon Web
// Services access key ID signing the request."
func handleImplicitUsername(r *http.Request, values url.Values) {
	if len(r.Header["Authorization"]) == 0 || values.Get("UserName") != "" {
		return
	}
	// get username who signs the request. For a typical Authorization:
	// "AWS4-HMAC-SHA256 Credential=197FSAQ7HHTA48X64O3A/20220420/test1/iam/aws4_request, SignedHeaders=content-type;
	// host;x-amz-date, Signature=6757dc6b3d7534d67e17842760310e99ee695408497f6edc4fdb84770c252dc8",
	// the "test1" will be extracted as the username
	glog.V(4).Infof("Authorization field: %v", r.Header["Authorization"][0])
	s := strings.Split(r.Header["Authorization"][0], "Credential=")
	if len(s) < 2 {
		return
	}
	s = strings.Split(s[1], ",")
	if len(s) < 2 {
		return
	}
	s = strings.Split(s[0], "/")
	if len(s) < 5 {
		return
	}
	userName := s[2]
	values.Set("UserName", userName)
}

func checkStrPtr(s string) *string {
	return &s
}

func checkIntPtr(i int) *int {
	return &i
}

func checkInt64Ptr(i int64) *int64 {
	return &i
}

func checkBoolPtr(b bool) *bool {
	return &b
}

// getIamManager returns the IAM manager instance
// This is guaranteed to be non-nil by constructor validation
func (iama *IamApiServer) getIamManager() *integration.IAMManager {
	return iama.iamManager
}

func (iama *IamApiServer) CreateRole(ctx context.Context, values url.Values) (resp CreateRoleResponse, iamError *IamError) {
	roleName := values.Get("RoleName")
	assumeRolePolicy := values.Get("AssumeRolePolicyDocument")
	description := values.Get("Description")

	if roleName == "" {
		return resp, &IamError{Code: "ValidationError", Error: fmt.Errorf("RoleName is required")}
	}

	iamManager := iama.getIamManager()
	if iamManager == nil {
		return resp, &IamError{Code: iam.ErrCodeServiceFailureException, Error: fmt.Errorf("IAM Manager not initialized")}
	}

	var trustPolicy policy.PolicyDocument
	if assumeRolePolicy != "" {
		if err := json.Unmarshal([]byte(assumeRolePolicy), &trustPolicy); err != nil {
			return resp, &IamError{Code: iam.ErrCodeMalformedPolicyDocumentException, Error: fmt.Errorf("malformed AssumeRolePolicyDocument: %v", err)}
		}
	}

	// Check if role already exists (strict AWS compliance)
	existingRole, _ := iamManager.GetRoleStore().GetRole(ctx, "", roleName)
	if existingRole != nil {
		return resp, &IamError{Code: iam.ErrCodeEntityAlreadyExistsException, Error: fmt.Errorf("role with name %s already exists", roleName)}
	}

	// Handle MaxSessionDuration
	maxSessionDurationStr := values.Get("MaxSessionDuration")
	var maxSessionDuration int
	if maxSessionDurationStr != "" {
		if _, err := fmt.Sscanf(maxSessionDurationStr, "%d", &maxSessionDuration); err != nil {
			return resp, &IamError{Code: "ValidationError", Error: fmt.Errorf("MaxSessionDuration must be an integer")}
		}
		if maxSessionDuration < 3600 || maxSessionDuration > 43200 {
			return resp, &IamError{Code: "ValidationError", Error: fmt.Errorf("MaxSessionDuration must be between 3600 and 43200 seconds")}
		}
	}

	roleDef := &integration.RoleDefinition{
		RoleName:           roleName,
		Description:        description,
		TrustPolicy:        &trustPolicy,
		MaxSessionDuration: maxSessionDuration,
	}

	if err := iamManager.CreateRole(ctx, "", roleName, roleDef); err != nil {
		return resp, &IamError{Code: iam.ErrCodeServiceFailureException, Error: err}
	}

	// Fetch back to get full details (ARN, etc.)
	fullRole, err := iamManager.GetRoleStore().GetRole(ctx, "", roleName)
	if err == nil && fullRole != nil {
		resp.CreateRoleResult.Role = iam.Role{
			RoleName:           &fullRole.RoleName,
			RoleId:             &fullRole.RoleName,
			Arn:                &fullRole.RoleArn,
			Path:               checkStrPtr("/"),
			MaxSessionDuration: checkInt64Ptr(int64(fullRole.MaxSessionDuration)),
		}
		if fullRole.TrustPolicy != nil {
			if b, err := json.Marshal(fullRole.TrustPolicy); err == nil {
				encoded := url.QueryEscape(string(b))
				resp.CreateRoleResult.Role.AssumeRolePolicyDocument = &encoded
			}
		}
	} else {
		// Fallback if fetch fails
		resp.CreateRoleResult.Role = iam.Role{
			RoleName:           &roleName,
			Path:               checkStrPtr("/"),
			MaxSessionDuration: checkInt64Ptr(int64(maxSessionDuration)),
		}
	}

	return resp, nil
}

func (iama *IamApiServer) DeleteRole(ctx context.Context, values url.Values) (resp DeleteRoleResponse, iamError *IamError) {
	roleName := values.Get("RoleName")
	if roleName == "" {
		return resp, &IamError{Code: "ValidationError", Error: fmt.Errorf("RoleName is required")}
	}

	iamManager := iama.getIamManager()
	if iamManager == nil {
		return resp, &IamError{Code: iam.ErrCodeServiceFailureException, Error: fmt.Errorf("IAM Manager not initialized")}
	}

	// Check for attached policies preventing deletion (strict AWS compliance)
	attachedPolicies, err := iamManager.GetRoleStore().ListAttachedRolePolicies(ctx, "", roleName)
	if err == nil && len(attachedPolicies) > 0 {
		return resp, &IamError{Code: iam.ErrCodeDeleteConflictException, Error: fmt.Errorf("Cannot delete entity, must detach all policies first.")}
	}
	
	// Check for INLINE policies preventing deletion
	role, err := iamManager.GetRoleStore().GetRole(ctx, "", roleName)
	if err == nil {
		if len(role.InlinePolicies) > 0 {
			return resp, &IamError{Code: iam.ErrCodeDeleteConflictException, Error: fmt.Errorf("Cannot delete entity, must delete all inline policies first.")}
		}
	} else {
		// If getting role fails (e.g. not found), we can proceed to "delete" (idempotent),
		// OR we should have probably failed earlier if strict.
		// Current logic falls through to DeleteRole which handles "not found".
	}

	if err := iamManager.GetRoleStore().DeleteRole(ctx, "", roleName); err != nil {
		// AWS returns success if role doesn't exist? RoleStore.DeleteRole logic handles idempotency ideally.
		// iam_manager.go/role_store calls DeleteEntry which might error if not found?
		// RoleStore implementation: FilerRoleStore checks "not found" and returns nil (idempotent).
		return resp, &IamError{Code: iam.ErrCodeServiceFailureException, Error: err}
	}

	return resp, nil
}

func (iama *IamApiServer) GetRole(ctx context.Context, values url.Values) (resp GetRoleResponse, iamError *IamError) {
	roleName := values.Get("RoleName")
	if roleName == "" {
		return resp, &IamError{Code: "ValidationError", Error: fmt.Errorf("RoleName is required")}
	}

	iamManager := iama.getIamManager()
	if iamManager == nil {
		return resp, &IamError{Code: iam.ErrCodeServiceFailureException, Error: fmt.Errorf("IAM Manager not initialized")}
	}

	role, err := iamManager.GetRoleStore().GetRole(ctx, "", roleName)
	if err != nil {
		return resp, &IamError{Code: iam.ErrCodeNoSuchEntityException, Error: fmt.Errorf("role not found: %s", roleName)}
	}

	resp.GetRoleResult.Role = iam.Role{
		RoleName:           &role.RoleName,
		RoleId:             &role.RoleName,
		Arn:                &role.RoleArn,
		Path:               checkStrPtr("/"),
		Description:        &role.Description,
		MaxSessionDuration: checkInt64Ptr(int64(role.MaxSessionDuration)),
	}
	if role.TrustPolicy != nil {
		if b, err := json.Marshal(role.TrustPolicy); err == nil {
			encoded := url.QueryEscape(string(b))
			resp.GetRoleResult.Role.AssumeRolePolicyDocument = &encoded
		}
	}

	return resp, nil
}

func (iama *IamApiServer) ListRoles(ctx context.Context, values url.Values) (resp ListRolesResponse, iamError *IamError) {
	iamManager := iama.getIamManager()
	if iamManager == nil {
		return resp, &IamError{Code: iam.ErrCodeServiceFailureException, Error: fmt.Errorf("IAM Manager not initialized")}
	}

	roles, err := iamManager.GetRoleStore().ListRolesDefinitions(ctx, "")
	if err != nil {
		return resp, &IamError{Code: iam.ErrCodeServiceFailureException, Error: err}
	}

	for _, role := range roles {
		iamRole := &iam.Role{
			RoleName:           &role.RoleName,
			RoleId:             &role.RoleName,
			Arn:                &role.RoleArn,
			Path:               checkStrPtr("/"),
			Description:        &role.Description,
			MaxSessionDuration: checkInt64Ptr(int64(role.MaxSessionDuration)),
		}
		if role.TrustPolicy != nil {
			if b, err := json.Marshal(role.TrustPolicy); err == nil {
				encoded := url.QueryEscape(string(b))
				iamRole.AssumeRolePolicyDocument = &encoded
			}
		}
		resp.ListRolesResult.Roles = append(resp.ListRolesResult.Roles, iamRole)
	}

	return resp, nil
}

func extractPolicyNameFromArn(arn string) string {
	if strings.Contains(arn, "/") {
		parts := strings.Split(arn, "/")
		return parts[len(parts)-1]
	}
	return arn
}

func (iama *IamApiServer) AttachRolePolicy(ctx context.Context, values url.Values) (resp AttachRolePolicyResponse, iamError *IamError) {
	roleName := values.Get("RoleName")
	policyArn := values.Get("PolicyArn")

	if roleName == "" || policyArn == "" {
		return resp, &IamError{Code: "ValidationError", Error: fmt.Errorf("RoleName and PolicyArn are required")}
	}

	iamManager := iama.getIamManager()
	if iamManager == nil {
		return resp, &IamError{Code: iam.ErrCodeServiceFailureException, Error: fmt.Errorf("IAM Manager not initialized")}
	}

	store := iamManager.GetRoleStore()
	role, err := store.GetRole(ctx, "", roleName)
	if err != nil {
		return resp, &IamError{Code: iam.ErrCodeNoSuchEntityException, Error: fmt.Errorf("role not found: %s", roleName)}
	}

	policyName := extractPolicyNameFromArn(policyArn)

	// Check if already attached
	for _, p := range role.AttachedPolicies {
		if p == policyName {
			return resp, nil
		}
	}

	role.AttachedPolicies = append(role.AttachedPolicies, policyName)
	if err := store.StoreRole(ctx, "", roleName, role); err != nil {
		return resp, &IamError{Code: iam.ErrCodeServiceFailureException, Error: err}
	}

	return resp, nil
}

func (iama *IamApiServer) DetachRolePolicy(ctx context.Context, values url.Values) (resp DetachRolePolicyResponse, iamError *IamError) {
	roleName := values.Get("RoleName")
	policyArn := values.Get("PolicyArn")

	if roleName == "" || policyArn == "" {
		return resp, &IamError{Code: "ValidationError", Error: fmt.Errorf("RoleName and PolicyArn are required")}
	}

	iamManager := iama.getIamManager()
	if iamManager == nil {
		return resp, &IamError{Code: iam.ErrCodeServiceFailureException, Error: fmt.Errorf("IAM Manager not initialized")}
	}

	store := iamManager.GetRoleStore()
	role, err := store.GetRole(ctx, "", roleName)
	if err != nil {
		return resp, &IamError{Code: iam.ErrCodeNoSuchEntityException, Error: fmt.Errorf("role not found: %s", roleName)}
	}

	policyName := extractPolicyNameFromArn(policyArn)

	newPolicies := []string{}
	found := false
	for _, p := range role.AttachedPolicies {
		if p == policyName {
			found = true
			continue
		}
		newPolicies = append(newPolicies, p)
	}

	if found {
		role.AttachedPolicies = newPolicies
		if err := store.StoreRole(ctx, "", roleName, role); err != nil {
			return resp, &IamError{Code: iam.ErrCodeServiceFailureException, Error: err}
		}
	}

	return resp, nil
}

func (iama *IamApiServer) ListAttachedRolePolicies(ctx context.Context, values url.Values) (resp ListAttachedRolePoliciesResponse, iamError *IamError) {
	roleName := values.Get("RoleName")
	if roleName == "" {
		return resp, &IamError{Code: "ValidationError", Error: fmt.Errorf("RoleName is required")}
	}

	iamManager := iama.getIamManager()
	if iamManager == nil {
		return resp, &IamError{Code: iam.ErrCodeServiceFailureException, Error: fmt.Errorf("IAM Manager not initialized")}
	}

	role, err := iamManager.GetRoleStore().GetRole(ctx, "", roleName)
	if err != nil {
		return resp, &IamError{Code: iam.ErrCodeNoSuchEntityException, Error: fmt.Errorf("role not found: %s", roleName)}
	}

	for _, policyName := range role.AttachedPolicies {
		arn := fmt.Sprintf("arn:aws:iam:::policy/%s", policyName)
		resp.ListAttachedRolePoliciesResult.AttachedPolicies = append(resp.ListAttachedRolePoliciesResult.AttachedPolicies, &iam.AttachedPolicy{
			PolicyName: &policyName,
			PolicyArn:  &arn,
		})
	}

	return resp, nil
}

// UpdateRole updates a role's description and/or maximum session duration
// https://docs.aws.amazon.com/IAM/latest/APIReference/API_UpdateRole.html
func (iama *IamApiServer) UpdateRole(ctx context.Context, values url.Values) (resp UpdateRoleResponse, iamError *IamError) {
	roleName := values.Get("RoleName")
	description := values.Get("Description")
	maxSessionDurationStr := values.Get("MaxSessionDuration")

	if roleName == "" {
		return resp, &IamError{Code: "ValidationError", Error: fmt.Errorf("RoleName is required")}
	}

	iamManager := iama.getIamManager()
	if iamManager == nil {
		return resp, &IamError{Code: iam.ErrCodeServiceFailureException, Error: fmt.Errorf("IAM Manager not initialized")}
	}

	store := iamManager.GetRoleStore()
	role, err := store.GetRole(ctx, "", roleName)
	if err != nil {
		return resp, &IamError{Code: iam.ErrCodeNoSuchEntityException, Error: fmt.Errorf("role not found: %s", roleName)}
	}

	// Update description if provided
	if description != "" {
		role.Description = description
	}

	// Update max session duration if provided
	if maxSessionDurationStr != "" {
		var maxSessionDuration int
		if _, err := fmt.Sscanf(maxSessionDurationStr, "%d", &maxSessionDuration); err != nil {
			return resp, &IamError{Code: "ValidationError", Error: fmt.Errorf("MaxSessionDuration must be an integer")}
		}
		if maxSessionDuration < 3600 || maxSessionDuration > 43200 {
			return resp, &IamError{Code: "ValidationError", Error: fmt.Errorf("MaxSessionDuration must be between 3600 and 43200 seconds")}
		}
		role.MaxSessionDuration = maxSessionDuration
	}

	if err := store.StoreRole(ctx, "", roleName, role); err != nil {
		return resp, &IamError{Code: iam.ErrCodeServiceFailureException, Error: err}
	}

	return resp, nil
}

// UpdateAssumeRolePolicy updates a role's trust policy (AssumeRolePolicyDocument)
// https://docs.aws.amazon.com/IAM/latest/APIReference/API_UpdateAssumeRolePolicy.html
func (iama *IamApiServer) UpdateAssumeRolePolicy(ctx context.Context, values url.Values) (resp UpdateAssumeRolePolicyResponse, iamError *IamError) {
	roleName := values.Get("RoleName")
	policyDocument := values.Get("PolicyDocument")

	if roleName == "" {
		return resp, &IamError{Code: "ValidationError", Error: fmt.Errorf("RoleName is required")}
	}
	if policyDocument == "" {
		return resp, &IamError{Code: "ValidationError", Error: fmt.Errorf("PolicyDocument is required")}
	}

	iamManager := iama.getIamManager()
	if iamManager == nil {
		return resp, &IamError{Code: iam.ErrCodeServiceFailureException, Error: fmt.Errorf("IAM Manager not initialized")}
	}

	store := iamManager.GetRoleStore()
	role, err := store.GetRole(ctx, "", roleName)
	if err != nil {
		return resp, &IamError{Code: iam.ErrCodeNoSuchEntityException, Error: fmt.Errorf("role not found: %s", roleName)}
	}

	// Parse the new trust policy
	var trustPolicy policy.PolicyDocument
	if err := json.Unmarshal([]byte(policyDocument), &trustPolicy); err != nil {
		return resp, &IamError{Code: iam.ErrCodeMalformedPolicyDocumentException, Error: fmt.Errorf("malformed PolicyDocument: %v", err)}
	}

	role.TrustPolicy = &trustPolicy

	if err := store.StoreRole(ctx, "", roleName, role); err != nil {
		return resp, &IamError{Code: iam.ErrCodeServiceFailureException, Error: err}
	}

	return resp, nil
}

// PutRolePolicy adds or updates an inline policy document embedded in a role
// https://docs.aws.amazon.com/IAM/latest/APIReference/API_PutRolePolicy.html
func (iama *IamApiServer) PutRolePolicy(ctx context.Context, values url.Values) (resp PutRolePolicyResponse, iamError *IamError) {
	roleName := values.Get("RoleName")
	policyName := values.Get("PolicyName")
	policyDocument := values.Get("PolicyDocument")

	if roleName == "" {
		return resp, &IamError{Code: "ValidationError", Error: fmt.Errorf("RoleName is required")}
	}
	if policyName == "" {
		return resp, &IamError{Code: "ValidationError", Error: fmt.Errorf("PolicyName is required")}
	}
	if policyDocument == "" {
		return resp, &IamError{Code: "ValidationError", Error: fmt.Errorf("PolicyDocument is required")}
	}

	// Validate the policy document JSON
	var policyDoc policy_engine.PolicyDocument
	if err := json.Unmarshal([]byte(policyDocument), &policyDoc); err != nil {
		return resp, &IamError{Code: iam.ErrCodeMalformedPolicyDocumentException, Error: fmt.Errorf("malformed PolicyDocument: %v", err)}
	}

	iamManager := iama.getIamManager()
	if iamManager == nil {
		return resp, &IamError{Code: iam.ErrCodeServiceFailureException, Error: fmt.Errorf("IAM Manager not initialized")}
	}

	store := iamManager.GetRoleStore()
	role, err := store.GetRole(ctx, "", roleName)
	if err != nil {
		return resp, &IamError{Code: iam.ErrCodeNoSuchEntityException, Error: fmt.Errorf("role not found: %s", roleName)}
	}

	// Initialize inline policies map if nil
	if role.InlinePolicies == nil {
		role.InlinePolicies = make(map[string]string)
	}

	// Store policy document URL-encoded (AWS standard - RFC 3986)
	// Note: url.QueryEscape encodes spaces as '+', but AWS expects '%20'
	// So we need to replace + with %20 for proper JSON encoding
	encoded := url.QueryEscape(policyDocument)
	encoded = strings.ReplaceAll(encoded, "+", "%20")
	role.InlinePolicies[policyName] = encoded

	if err := store.StoreRole(ctx, "", roleName, role); err != nil {
		return resp, &IamError{Code: iam.ErrCodeServiceFailureException, Error: err}
	}

	return resp, nil
}

// GetRolePolicy retrieves an inline policy document embedded in a role
// https://docs.aws.amazon.com/IAM/latest/APIReference/API_GetRolePolicy.html
func (iama *IamApiServer) GetRolePolicy(ctx context.Context, values url.Values) (resp GetRolePolicyResponse, iamError *IamError) {
	roleName := values.Get("RoleName")
	policyName := values.Get("PolicyName")

	if roleName == "" {
		return resp, &IamError{Code: "ValidationError", Error: fmt.Errorf("RoleName is required")}
	}
	if policyName == "" {
		return resp, &IamError{Code: "ValidationError", Error: fmt.Errorf("PolicyName is required")}
	}

	iamManager := iama.getIamManager()
	if iamManager == nil {
		return resp, &IamError{Code: iam.ErrCodeServiceFailureException, Error: fmt.Errorf("IAM Manager not initialized")}
	}

	store := iamManager.GetRoleStore()
	role, err := store.GetRole(ctx, "", roleName)
	if err != nil {
		return resp, &IamError{Code: iam.ErrCodeNoSuchEntityException, Error: fmt.Errorf("role not found: %s", roleName)}
	}

	if role.InlinePolicies == nil {
		return resp, &IamError{Code: iam.ErrCodeNoSuchEntityException, Error: fmt.Errorf("inline policy not found: %s", policyName)}
	}

	policyDoc, exists := role.InlinePolicies[policyName]
	if !exists {
		return resp, &IamError{Code: iam.ErrCodeNoSuchEntityException, Error: fmt.Errorf("inline policy not found: %s", policyName)}
	}

	resp.GetRolePolicyResult.RoleName = roleName
	resp.GetRolePolicyResult.PolicyName = policyName
	resp.GetRolePolicyResult.PolicyDocument = policyDoc // Already URL-encoded

	return resp, nil
}

// DeleteRolePolicy deletes an inline policy embedded in a role
// https://docs.aws.amazon.com/IAM/latest/APIReference/API_DeleteRolePolicy.html
func (iama *IamApiServer) DeleteRolePolicy(ctx context.Context, values url.Values) (resp DeleteRolePolicyResponse, iamError *IamError) {
	roleName := values.Get("RoleName")
	policyName := values.Get("PolicyName")

	if roleName == "" {
		return resp, &IamError{Code: "ValidationError", Error: fmt.Errorf("RoleName is required")}
	}
	if policyName == "" {
		return resp, &IamError{Code: "ValidationError", Error: fmt.Errorf("PolicyName is required")}
	}

	iamManager := iama.getIamManager()
	if iamManager == nil {
		return resp, &IamError{Code: iam.ErrCodeServiceFailureException, Error: fmt.Errorf("IAM Manager not initialized")}
	}

	store := iamManager.GetRoleStore()
	role, err := store.GetRole(ctx, "", roleName)
	if err != nil {
		return resp, &IamError{Code: iam.ErrCodeNoSuchEntityException, Error: fmt.Errorf("role not found: %s", roleName)}
	}

	if role.InlinePolicies == nil {
		return resp, &IamError{Code: iam.ErrCodeNoSuchEntityException, Error: fmt.Errorf("inline policy not found: %s", policyName)}
	}

	if _, exists := role.InlinePolicies[policyName]; !exists {
		return resp, &IamError{Code: iam.ErrCodeNoSuchEntityException, Error: fmt.Errorf("inline policy not found: %s", policyName)}
	}

	delete(role.InlinePolicies, policyName)

	if err := store.StoreRole(ctx, "", roleName, role); err != nil {
		return resp, &IamError{Code: iam.ErrCodeServiceFailureException, Error: err}
	}

	return resp, nil
}

// ListRolePolicies lists the names of inline policies embedded in a role
// https://docs.aws.amazon.com/IAM/latest/APIReference/API_ListRolePolicies.html
func (iama *IamApiServer) ListRolePolicies(ctx context.Context, values url.Values) (resp ListRolePoliciesResponse, iamError *IamError) {
	roleName := values.Get("RoleName")

	if roleName == "" {
		return resp, &IamError{Code: "ValidationError", Error: fmt.Errorf("RoleName is required")}
	}

	iamManager := iama.getIamManager()
	if iamManager == nil {
		return resp, &IamError{Code: iam.ErrCodeServiceFailureException, Error: fmt.Errorf("IAM Manager not initialized")}
	}

	store := iamManager.GetRoleStore()
	role, err := store.GetRole(ctx, "", roleName)
	if err != nil {
		return resp, &IamError{Code: iam.ErrCodeNoSuchEntityException, Error: fmt.Errorf("role not found: %s", roleName)}
	}

	for policyName := range role.InlinePolicies {
		name := policyName // Copy for pointer
		resp.ListRolePoliciesResult.PolicyNames = append(resp.ListRolePoliciesResult.PolicyNames, &name)
	}

	return resp, nil
}

func (iama *IamApiServer) DoActions(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		writeIamErrorResponse(w, r, &IamError{Code: "InvalidRequest", Error: err})
		return
	}
	values := r.PostForm
	s3cfg := &iam_pb.S3ApiConfiguration{}
	if err := iama.s3ApiConfig.GetS3ApiConfiguration(s3cfg); err != nil && !errors.Is(err, filer_pb.ErrNotFound) {
		writeIamErrorResponse(w, r, &IamError{Code: iam.ErrCodeServiceFailureException, Error: err})
		return
	}

	glog.V(4).Infof("DoActions: %+v", values)
	var response interface{}
	var iamError *IamError
	changed := true
	switch r.Form.Get("Action") {
	case "ListUsers":
		response = iama.ListUsers(s3cfg, values)
		changed = false
	case "ListAccessKeys":
		handleImplicitUsername(r, values)
		response = iama.ListAccessKeys(s3cfg, values)
		changed = false
	case "CreateUser":
		response, iamError = iama.CreateUser(s3cfg, values)
		if iamError != nil {
			glog.Errorf("CreateUser: %+v", iamError.Error)
			writeIamErrorResponse(w, r, iamError)
			return
		}
		changed = false
	case "GetUser":
		userName := values.Get("UserName")
		response, iamError = iama.GetUser(s3cfg, userName)
		if iamError != nil {
			writeIamErrorResponse(w, r, iamError)
			return
		}
		changed = false
	case "UpdateUser":
		response, iamError = iama.UpdateUser(s3cfg, values)
		if iamError != nil {
			glog.Errorf("UpdateUser: %+v", iamError.Error)
			writeIamErrorResponse(w, r, iamError)
			return
		}
		changed = false
	case "DeleteUser":
		userName := values.Get("UserName")
		response, iamError = iama.DeleteUser(s3cfg, userName)
		if iamError != nil {
			writeIamErrorResponse(w, r, iamError)
			return
		}
		changed = false
	case "CreateAccessKey":
		handleImplicitUsername(r, values)
		response, iamError = iama.CreateAccessKey(s3cfg, values)
		if iamError != nil {
			writeIamErrorResponse(w, r, iamError)
			return
		}
		changed = false
	case "DeleteAccessKey":
		handleImplicitUsername(r, values)
		response, iamError = iama.DeleteAccessKey(s3cfg, values)
		if iamError != nil {
			writeIamErrorResponse(w, r, iamError)
			return
		}
		changed = false
	case "UpdateAccessKey":
		handleImplicitUsername(r, values)
		response, iamError = iama.UpdateAccessKey(s3cfg, values)
		if iamError != nil {
			writeIamErrorResponse(w, r, iamError)
			return
		}
		changed = false
	case "GetAccessKeyLastUsed":
		response, iamError = iama.GetAccessKeyLastUsed(s3cfg, values)
		if iamError != nil {
			writeIamErrorResponse(w, r, iamError)
			return
		}
		changed = false
	case "CreatePolicy":
		response, iamError = iama.CreatePolicy(s3cfg, values)
		if iamError != nil {
			glog.Errorf("CreatePolicy: %+v", iamError.Error)
			writeIamErrorResponse(w, r, iamError)
			return
		}
		changed = false
	case "AttachUserPolicy":
		response, iamError = iama.AttachUserPolicy(s3cfg, values)
		if iamError != nil {
			glog.Errorf("AttachUserPolicy: %+v", iamError.Error)
			writeIamErrorResponse(w, r, iamError)
			return
		}
		changed = false
	case "DetachUserPolicy":
		response, iamError = iama.DetachUserPolicy(s3cfg, values)
		if iamError != nil {
			glog.Errorf("DetachUserPolicy: %+v", iamError.Error)
			writeIamErrorResponse(w, r, iamError)
			return
		}
		changed = false
	case "ListAttachedUserPolicies":
		response, iamError = iama.ListAttachedUserPolicies(s3cfg, values)
		if iamError != nil {
			glog.Errorf("ListAttachedUserPolicies: %+v", iamError.Error)
			writeIamErrorResponse(w, r, iamError)
			return
		}
		changed = false
		if iamError != nil {
			glog.Errorf("CreatePolicy:  %+v", iamError.Error)
			writeIamErrorResponse(w, r, iamError)
			return
		}
		changed = false
	case "GetPolicy":
		response, iamError = iama.GetPolicy(s3cfg, values)
		if iamError != nil {
			writeIamErrorResponse(w, r, iamError)
			return
		}
		changed = false
	case "GetPolicyVersion":
		response, iamError = iama.GetPolicyVersion(s3cfg, values)
		if iamError != nil {
			writeIamErrorResponse(w, r, iamError)
			return
		}
		changed = false
	case "CreatePolicyVersion":
		response, iamError = iama.CreatePolicyVersion(s3cfg, values)
		if iamError != nil {
			writeIamErrorResponse(w, r, iamError)
			return
		}
		changed = true
	case "DeletePolicy":
		response, iamError = iama.DeletePolicy(s3cfg, values)
		if iamError != nil {
			writeIamErrorResponse(w, r, iamError)
			return
		}
		changed = false
	case "ListPolicies":
		response, iamError = iama.ListPolicies(s3cfg, values)
		if iamError != nil {
			writeIamErrorResponse(w, r, iamError)
			return
		}
		changed = false
	case "PutUserPolicy":
		var iamError *IamError
		response, iamError = iama.PutUserPolicy(s3cfg, values)
		if iamError != nil {
			glog.Errorf("PutUserPolicy:  %+v", iamError.Error)

			writeIamErrorResponse(w, r, iamError)
			return
		}
		changed = false
	case "GetUserPolicy":
		response, iamError = iama.GetUserPolicy(s3cfg, values)
		if iamError != nil {
			writeIamErrorResponse(w, r, iamError)
			return
		}
		changed = false
	case "DeleteUserPolicy":
		if response, iamError = iama.DeleteUserPolicy(s3cfg, values); iamError != nil {
			writeIamErrorResponse(w, r, iamError)
			return
		}
		changed = false
	case "CreateRole":
		response, iamError = iama.CreateRole(r.Context(), values)
		if iamError != nil {
			writeIamErrorResponse(w, r, iamError)
			return
		}
		changed = false
	case "DeleteRole":
		response, iamError = iama.DeleteRole(r.Context(), values)
		if iamError != nil {
			writeIamErrorResponse(w, r, iamError)
			return
		}
		changed = false
	case "GetRole":
		response, iamError = iama.GetRole(r.Context(), values)
		if iamError != nil {
			writeIamErrorResponse(w, r, iamError)
			return
		}
		changed = false
	case "ListRoles":
		response, iamError = iama.ListRoles(r.Context(), values)
		if iamError != nil {
			writeIamErrorResponse(w, r, iamError)
			return
		}
		changed = false
	case "AttachRolePolicy":
		response, iamError = iama.AttachRolePolicy(r.Context(), values)
		if iamError != nil {
			writeIamErrorResponse(w, r, iamError)
			return
		}
		changed = false
	case "DetachRolePolicy":
		response, iamError = iama.DetachRolePolicy(r.Context(), values)
		if iamError != nil {
			writeIamErrorResponse(w, r, iamError)
			return
		}
		changed = false
	case "ListAttachedRolePolicies":
		response, iamError = iama.ListAttachedRolePolicies(r.Context(), values)
		if iamError != nil {
			writeIamErrorResponse(w, r, iamError)
			return
		}
		changed = false
	case "UpdateRole":
		response, iamError = iama.UpdateRole(r.Context(), values)
		if iamError != nil {
			writeIamErrorResponse(w, r, iamError)
			return
		}
		changed = false
	case "UpdateAssumeRolePolicy":
		response, iamError = iama.UpdateAssumeRolePolicy(r.Context(), values)
		if iamError != nil {
			writeIamErrorResponse(w, r, iamError)
			return
		}
		changed = false
	case "PutRolePolicy":
		response, iamError = iama.PutRolePolicy(r.Context(), values)
		if iamError != nil {
			writeIamErrorResponse(w, r, iamError)
			return
		}
		changed = false
	case "GetRolePolicy":
		response, iamError = iama.GetRolePolicy(r.Context(), values)
		if iamError != nil {
			writeIamErrorResponse(w, r, iamError)
			return
		}
		changed = false
	case "DeleteRolePolicy":
		response, iamError = iama.DeleteRolePolicy(r.Context(), values)
		if iamError != nil {
			writeIamErrorResponse(w, r, iamError)
			return
		}
		changed = false
	case "ListRolePolicies":
		response, iamError = iama.ListRolePolicies(r.Context(), values)
		if iamError != nil {
			writeIamErrorResponse(w, r, iamError)
			return
		}
		changed = false
	case "CreateGroup":
		response, iamError = iama.CreateGroup(r.Context(), values)
		if iamError != nil {
			writeIamErrorResponse(w, r, iamError)
			return
		}
		changed = false
	case "GetGroup":
		response, iamError = iama.GetGroup(r.Context(), values)
		if iamError != nil {
			writeIamErrorResponse(w, r, iamError)
			return
		}
		changed = false
	case "UpdateGroup":
		response, iamError = iama.UpdateGroup(r.Context(), values)
		if iamError != nil {
			writeIamErrorResponse(w, r, iamError)
			return
		}
		changed = false
	case "DeleteGroup":
		response, iamError = iama.DeleteGroup(r.Context(), values)
		if iamError != nil {
			writeIamErrorResponse(w, r, iamError)
			return
		}
		changed = false
	case "ListGroups":
		response, iamError = iama.ListGroups(r.Context(), values)
		if iamError != nil {
			writeIamErrorResponse(w, r, iamError)
			return
		}
		changed = false
	case "AddUserToGroup":
		response, iamError = iama.AddUserToGroup(r.Context(), s3cfg, values)
		if iamError != nil {
			writeIamErrorResponse(w, r, iamError)
			return
		}
		changed = false
	case "RemoveUserFromGroup":
		response, iamError = iama.RemoveUserFromGroup(r.Context(), values)
		if iamError != nil {
			writeIamErrorResponse(w, r, iamError)
			return
		}
		changed = false
	case "ListGroupsForUser":
		response, iamError = iama.ListGroupsForUser(r.Context(), values)
		if iamError != nil {
			writeIamErrorResponse(w, r, iamError)
			return
		}
		changed = false
	case "AttachGroupPolicy":
		response, iamError = iama.AttachGroupPolicy(r.Context(), values)
		if iamError != nil {
			writeIamErrorResponse(w, r, iamError)
			return
		}
		changed = false
	case "DetachGroupPolicy":
		response, iamError = iama.DetachGroupPolicy(r.Context(), values)
		if iamError != nil {
			writeIamErrorResponse(w, r, iamError)
			return
		}
		changed = false
	case "ListAttachedGroupPolicies":
		response, iamError = iama.ListAttachedGroupPolicies(r.Context(), values)
		if iamError != nil {
			writeIamErrorResponse(w, r, iamError)
			return
		}
		changed = false
	default:
		iamError := &IamError{Code: iam.ErrCodeServiceFailureException, Error: fmt.Errorf("Not Implemented")}
		writeIamErrorResponse(w, r, iamError)
		return
	}
	if changed {
		err := iama.s3ApiConfig.PutS3ApiConfiguration(s3cfg)
		if err != nil {
			var iamError = IamError{Code: iam.ErrCodeServiceFailureException, Error: err}
			writeIamErrorResponse(w, r, &iamError)
			return
		}
	}
	iam_errors.WriteXMLResponse(w, r, http.StatusOK, response)
}
// GetPolicy retrieves a managed policy by ARN
// https://docs.aws.amazon.com/IAM/latest/APIReference/API_GetPolicy.html
func (iama *IamApiServer) GetPolicy(s3cfg *iam_pb.S3ApiConfiguration, values url.Values) (resp GetPolicyResponse, iamError *IamError) {
	policyArn := values.Get("PolicyArn")
	
	if policyArn == "" {
		return resp, &IamError{Code: "ValidationError", Error: fmt.Errorf("PolicyArn is required")}
	}
	
	// Extract policy name from ARN: arn:aws:iam:::policy/PolicyName
	policyName := extractPolicyNameFromArn(policyArn)
	if policyName == "" {
		return resp, &IamError{Code: "ValidationError", Error: fmt.Errorf("Invalid PolicyArn format")}
	}
	
	// Get policies from s3cfg
	policies := Policies{}
	
	if err := iama.s3ApiConfig.GetPolicies(&policies); err != nil {
		return resp, &IamError{Code: iam.ErrCodeServiceFailureException, Error: err}
	}
	
	// Find the policy
	policyDoc, exists := policies.Policies[policyName]
	if !exists {
		return resp, &IamError{Code: iam.ErrCodeNoSuchEntityException, Error: fmt.Errorf("policy not found: %s", policyName)}
	}
	
	// Build response - populate Policy metadata
	// Build response - populate Policy metadata
	var policyId string
	var createDate, updateDate time.Time
	path := "/"

	if policyDoc.Metadata != nil {
		if policyDoc.Metadata.PolicyId != "" {
			policyId = policyDoc.Metadata.PolicyId
		} else {
			policyId = Hash(nil)
		}
		createDate = policyDoc.Metadata.CreatedAt
		updateDate = policyDoc.Metadata.UpdatedAt
		
		if policyDoc.Metadata.Description != "" {
			resp.GetPolicyResult.Policy.Description = &policyDoc.Metadata.Description
		}
	} else {
		policyId = Hash(nil)
		createDate = time.Now()
		updateDate = time.Now()
	}

	resp.GetPolicyResult.Policy.PolicyName = &policyName
	resp.GetPolicyResult.Policy.Arn = &policyArn
	resp.GetPolicyResult.Policy.PolicyId = &policyId
	resp.GetPolicyResult.Policy.Path = &path
	resp.GetPolicyResult.Policy.DefaultVersionId = checkStrPtr("v1")
	resp.GetPolicyResult.Policy.AttachmentCount = checkInt64Ptr(0) // TODO: calculate attachment count
	resp.GetPolicyResult.Policy.CreateDate = &createDate
	resp.GetPolicyResult.Policy.UpdateDate = &updateDate
	
	return resp, nil
}

// GetPolicyVersion retrieves a policy version
// https://docs.aws.amazon.com/IAM/latest/APIReference/API_GetPolicyVersion.html
func (iama *IamApiServer) GetPolicyVersion(s3cfg *iam_pb.S3ApiConfiguration, values url.Values) (resp GetPolicyVersionResponse, iamError *IamError) {
	policyArn := values.Get("PolicyArn")
	versionId := values.Get("VersionId")
	
	if policyArn == "" || versionId == "" {
		return resp, &IamError{Code: "ValidationError", Error: fmt.Errorf("PolicyArn and VersionId are required")}
	}
	
	// Extract policy name from ARN
	policyName := extractPolicyNameFromArn(policyArn)
	if policyName == "" {
		return resp, &IamError{Code: "ValidationError", Error: fmt.Errorf("Invalid PolicyArn format")}
	}
	
	// For now we only support "v1"
	if versionId != "v1" {
		return resp, &IamError{Code: iam.ErrCodeNoSuchEntityException, Error: fmt.Errorf("Version %s not found", versionId)}
	}
	
	// Get policies from s3cfg
	policies := Policies{}
	
	if err := iama.s3ApiConfig.GetPolicies(&policies); err != nil {
		return resp, &IamError{Code: iam.ErrCodeServiceFailureException, Error: err}
	}
	
	// Find the policy
	policyDoc, exists := policies.Policies[policyName]
	if !exists {
		return resp, &IamError{Code: iam.ErrCodeNoSuchEntityException, Error: fmt.Errorf("policy not found: %s", policyName)}
	}
	
	// Serialize document to JSON
	// We need to respect "omitempty" for fields like "Id", "Sid" etc to match what users uploaded if possible
	// But policy_engine.PolicyDocument struct might serve well enough
	docBytes, err := json.Marshal(policyDoc)
	if err != nil {
		return resp, &IamError{Code: iam.ErrCodeServiceFailureException, Error: fmt.Errorf("failed to marshal policy document: %v", err)}
	}
	
	docStr := string(docBytes)
	encodedDoc := url.QueryEscape(docStr)
	// AWS expects %20 for spaces
	encodedDoc = strings.ReplaceAll(encodedDoc, "+", "%20")
	
	var createDate time.Time
	if policyDoc.Metadata != nil {
		createDate = policyDoc.Metadata.CreatedAt
	} else {
		createDate = time.Now()
	}
	
	resp.GetPolicyVersionResult.PolicyVersion = iam.PolicyVersion{
		VersionId: &versionId,
		Document:  &encodedDoc,
		IsDefaultVersion: checkBoolPtr(true),
		CreateDate: &createDate,
	}
	
	return resp, nil
}

// DeletePolicy deletes a managed policy
// https://docs.aws.amazon.com/IAM/latest/APIReference/API_DeletePolicy.html
func (iama *IamApiServer) DeletePolicy(s3cfg *iam_pb.S3ApiConfiguration, values url.Values) (resp DeletePolicyResponse, iamError *IamError) {
	policyArn := values.Get("PolicyArn")
	
	if policyArn == "" {
		return resp, &IamError{Code: "ValidationError", Error: fmt.Errorf("PolicyArn is required")}
	}
	
	// Extract policy name from ARN
	policyName := extractPolicyNameFromArn(policyArn)
	if policyName == "" {
		return resp, &IamError{Code: "ValidationError", Error: fmt.Errorf("Invalid PolicyArn format")}
	}
	
	// Check if policy is attached to any roles
	// AWS requires detaching before deletion
	if isAttached := iama.checkPolicyAttachments(s3cfg, policyName); isAttached {
		return resp, &IamError{Code: iam.ErrCodeDeleteConflictException, 
			Error: fmt.Errorf("Cannot delete a policy attached to entities. Detach the policy from all entities and try again.")}
	}
	
	// Delete from policies
	// Check if policy exists
	_, err := iama.s3ApiConfig.GetPolicy(policyName)
	if err != nil {
		if err == filer_pb.ErrNotFound {
			return resp, &IamError{Code: iam.ErrCodeNoSuchEntityException, Error: fmt.Errorf("policy not found: %s", policyName)}
		}
		return resp, &IamError{Code: iam.ErrCodeServiceFailureException, Error: err}
	}
	
	if err := iama.s3ApiConfig.DeletePolicy(policyName); err != nil {
		return resp, &IamError{Code: iam.ErrCodeServiceFailureException, Error: err}
	}
	
	return resp, nil
}

// ListPolicies lists all managed policies
// https://docs.aws.amazon.com/IAM/latest/APIReference/API_ListPolicies.html
func (iama *IamApiServer) ListPolicies(s3cfg *iam_pb.S3ApiConfiguration, values url.Values) (resp ListPoliciesResponse, iamError *IamError) {
	// Get all policies
	// Get all policies
	policies := Policies{}
	// policyLock.Lock() - Not needed as GetPolicies returns a fresh map from files
	// defer policyLock.Unlock()
	
	if err := iama.s3ApiConfig.GetPolicies(&policies); err != nil {
		return resp, &IamError{Code: iam.ErrCodeServiceFailureException, Error: err}
	}
	
	// Convert to IAM policy format
	for policyName, policyDoc := range policies.Policies {
		var policyId string
		var createDate, updateDate time.Time
		path := "/"
		arn := fmt.Sprintf("arn:aws:iam:::policy/%s", policyName)

		if policyDoc.Metadata != nil {
			if policyDoc.Metadata.PolicyId != "" {
				policyId = policyDoc.Metadata.PolicyId
			} else {
				policyId = Hash(nil)
			}
			createDate = policyDoc.Metadata.CreatedAt
			updateDate = policyDoc.Metadata.UpdatedAt
		} else {
			policyId = Hash(nil)
			createDate = time.Now()
			updateDate = time.Now()
		}
		
		iamPolicy := &iam.Policy{
			PolicyName: &policyName,
			PolicyId:   &policyId,
			Arn:        &arn,
			Path:       &path,
			CreateDate: &createDate,
			UpdateDate: &updateDate,
		}
		
		// Add description if available from metadata
		if policyDoc.Metadata != nil && policyDoc.Metadata.Description != "" {
			iamPolicy.Description = &policyDoc.Metadata.Description
		}
		
		resp.ListPoliciesResult.Policies = append(resp.ListPoliciesResult.Policies, iamPolicy)
	}
	
	resp.ListPoliciesResult.IsTruncated = false
	
	return resp, nil
}


// Helper function to check if a policy is attached to any users, groups, or roles
// Helper function to check if a policy is attached to any users, groups, or roles
func (iama *IamApiServer) checkPolicyAttachments(s3cfg *iam_pb.S3ApiConfiguration, policyName string) bool {
	// Check if policy is attached to any roles
	iamManager := iama.getIamManager()
	if iamManager == nil {
		return false
	}
	
	roleStore := iamManager.GetRoleStore()
	roles, err := roleStore.ListRoles(context.Background(), "")
	if err != nil {
		return false
	}
	
	policyArn := fmt.Sprintf("arn:aws:iam:::policy/%s", policyName)
	
	for _, roleName := range roles {
		role, err := roleStore.GetRole(context.Background(), "", roleName)
		if err != nil {
			continue
		}
		
		// Check if this policy is attached to the role
		for _, attachedPolicy := range role.AttachedPolicies {
			if attachedPolicy == policyArn || attachedPolicy == policyName {
				return true // Policy is attached
			}
		}
	}
	
	// TODO: Also check users and groups when those are implemented
	
	return false // Policy is not attached
}

// CreateGroup creates a new group
// https://docs.aws.amazon.com/IAM/latest/APIReference/API_CreateGroup.html
func (iama *IamApiServer) CreateGroup(ctx context.Context, values url.Values) (resp CreateGroupResponse, iamError *IamError) {
	groupName := values.Get("GroupName")
	if groupName == "" {
		return resp, &IamError{Code: "ValidationError", Error: fmt.Errorf("GroupName is required")}
	}

	iamManager := iama.getIamManager()
	if iamManager == nil {
		return resp, &IamError{Code: iam.ErrCodeServiceFailureException, Error: fmt.Errorf("IAM Manager not initialized")}
	}

	store := iamManager.GetGroupStore()
	
	existing, _ := store.GetGroup(ctx, "", groupName)
	if existing != nil {
		return resp, &IamError{Code: iam.ErrCodeEntityAlreadyExistsException, Error: fmt.Errorf("Group %s already exists", groupName)}
	}

	now := time.Now()
	groupId := Hash(&groupName) // Generate unique ID
	group := &integration.GroupDefinition{
		GroupName: groupName,
		GroupId:   groupId,
		Arn:       fmt.Sprintf("arn:aws:iam::seaweedfs:group/%s", groupName),
		CreateDate: now,
	}

	if err := store.StoreGroup(ctx, "", groupName, group); err != nil {
		return resp, &IamError{Code: iam.ErrCodeServiceFailureException, Error: err}
	}

	resp.CreateGroupResult.Group = iam.Group{
		GroupName:  &group.GroupName,
		GroupId:    &group.GroupId,
		Arn:        &group.Arn,
		Path:       checkStrPtr("/"),
		CreateDate: &now,
	}

	return resp, nil
}

// GetGroup retrieves group details and members
// https://docs.aws.amazon.com/IAM/latest/APIReference/API_GetGroup.html
func (iama *IamApiServer) GetGroup(ctx context.Context, values url.Values) (resp GetGroupResponse, iamError *IamError) {
	groupName := values.Get("GroupName")
	if groupName == "" {
		return resp, &IamError{Code: "ValidationError", Error: fmt.Errorf("GroupName is required")}
	}

	iamManager := iama.getIamManager()
	if iamManager == nil {
		return resp, &IamError{Code: iam.ErrCodeServiceFailureException, Error: fmt.Errorf("IAM Manager not initialized")}
	}

	store := iamManager.GetGroupStore()
	group, err := store.GetGroup(ctx, "", groupName)
	if err != nil {
		return resp, &IamError{Code: iam.ErrCodeNoSuchEntityException, Error: fmt.Errorf("group not found: %s", groupName)}
	}

	resp.GetGroupResult.Group = iam.Group{
		GroupName:  &group.GroupName,
		GroupId:    &group.GroupId,
		Arn:        &group.Arn,
		Path:       checkStrPtr("/"),
		CreateDate: &group.CreateDate,
	}
	
	// Members
	for _, memberName := range group.Members {
		resp.GetGroupResult.Users = append(resp.GetGroupResult.Users, &iam.User{
			UserName: &memberName,
			// Ideally we would fetch UserId/Arn/CreateDate for the user too, but purely name is sufficient for basic clients
			Path: checkStrPtr("/"), 
		})
	}

	return resp, nil
}

// DeleteGroup deletes a group
// https://docs.aws.amazon.com/IAM/latest/APIReference/API_DeleteGroup.html
func (iama *IamApiServer) DeleteGroup(ctx context.Context, values url.Values) (resp DeleteGroupResponse, iamError *IamError) {
	groupName := values.Get("GroupName")
	if groupName == "" {
		return resp, &IamError{Code: "ValidationError", Error: fmt.Errorf("GroupName is required")}
	}

	iamManager := iama.getIamManager()
	if iamManager == nil {
		return resp, &IamError{Code: iam.ErrCodeServiceFailureException, Error: fmt.Errorf("IAM Manager not initialized")}
	}

	store := iamManager.GetGroupStore()
	
	// Check content (members)
	group, err := store.GetGroup(ctx, "", groupName)
	if err == nil {
		if len(group.Members) > 0 {
			return resp, &IamError{Code: iam.ErrCodeDeleteConflictException, Error: fmt.Errorf("Group %s is not empty", groupName)}
		}
		if len(group.AttachedPolicies) > 0 {
			return resp, &IamError{Code: iam.ErrCodeDeleteConflictException, Error: fmt.Errorf("Group %s has attached policies", groupName)}
		}
	}

	if err := store.DeleteGroup(ctx, "", groupName); err != nil {
		return resp, &IamError{Code: iam.ErrCodeServiceFailureException, Error: err}
	}

	return resp, nil
}

// UpdateGroup updates group name (and path)
// https://docs.aws.amazon.com/IAM/latest/APIReference/API_UpdateGroup.html
func (iama *IamApiServer) UpdateGroup(ctx context.Context, values url.Values) (resp UpdateGroupResponse, iamError *IamError) {
	groupName := values.Get("GroupName")
	newGroupName := values.Get("NewGroupName")
	// NewPath := values.Get("NewPath") // we ignore path for now as we hardcode /

	if groupName == "" {
		return resp, &IamError{Code: "ValidationError", Error: fmt.Errorf("GroupName is required")}
	}
	
	if newGroupName == "" {
		// Nothing to update if only path changes (which we ignore)
		return resp, nil 
	}

	iamManager := iama.getIamManager()
	if iamManager == nil {
		return resp, &IamError{Code: iam.ErrCodeServiceFailureException, Error: fmt.Errorf("IAM Manager not initialized")}
	}

	store := iamManager.GetGroupStore()
	group, err := store.GetGroup(ctx, "", groupName)
	if err != nil {
		return resp, &IamError{Code: iam.ErrCodeNoSuchEntityException, Error: fmt.Errorf("group not found: %s", groupName)}
	}

	// Rename: Store new, then delete old
	// NOTE: This is not atomic. Best-effort semantics:
	// - If StoreGroup fails, no changes made (safe)
	// - If DeleteGroup fails after StoreGroup, both groups exist (detectable inconsistency)
	// Production systems should use distributed transactions or accept eventual consistency
	
	// Check if new name exists
	if _, err := store.GetGroup(ctx, "", newGroupName); err == nil {
		return resp, &IamError{Code: iam.ErrCodeEntityAlreadyExistsException, Error: fmt.Errorf("Group %s already exists", newGroupName)}
	}

	// Update fields (preserve GroupId - it's immutable)
	group.GroupName = newGroupName
	group.Arn = fmt.Sprintf("arn:aws:iam::seaweedfs:group/%s", newGroupName)
	
	// Store under new name first
	if err := store.StoreGroup(ctx, "", newGroupName, group); err != nil {
		return resp, &IamError{Code: iam.ErrCodeServiceFailureException, Error: fmt.Errorf("failed to create renamed group: %w", err)}
	}
	
	// Delete old group - if this fails, we have both copies but can detect/cleanup
	if err := store.DeleteGroup(ctx, "", groupName); err != nil {
		glog.Errorf("UpdateGroup: created new group %s but failed to delete old %s: %v", newGroupName, groupName, err)
		// Attempt rollback
		if rollbackErr := store.DeleteGroup(ctx, "", newGroupName); rollbackErr != nil {
			glog.Errorf("UpdateGroup: rollback failed, both groups exist: %v", rollbackErr)
		}
		return resp, &IamError{Code: iam.ErrCodeServiceFailureException, Error: fmt.Errorf("failed to complete group rename: %w", err)}
	}

	return resp, nil
}

// ListGroups lists groups
// https://docs.aws.amazon.com/IAM/latest/APIReference/API_ListGroups.html
func (iama *IamApiServer) ListGroups(ctx context.Context, values url.Values) (resp ListGroupsResponse, iamError *IamError) {
	iamManager := iama.getIamManager()
	if iamManager == nil {
		return resp, &IamError{Code: iam.ErrCodeServiceFailureException, Error: fmt.Errorf("IAM Manager not initialized")}
	}

	store := iamManager.GetGroupStore()
	groupNames, err := store.ListGroups(ctx, "")
	if err != nil {
		return resp, &IamError{Code: iam.ErrCodeServiceFailureException, Error: err}
	}
	
	// Fetch details for each group? Or just fill basic info
	// ListGroups needs ARNs etc, so we should fetch
	// For performance, we might want parallel fetch similar to RoleStore.ListRolesDefinitions
	// For now, serial is fine
	
	for _, name := range groupNames {
		group, err := store.GetGroup(ctx, "", name)
		if err != nil {
			continue
		}
		resp.ListGroupsResult.Groups = append(resp.ListGroupsResult.Groups, &iam.Group{
			GroupName: &group.GroupName,
			GroupId: &group.GroupId,
			Arn: &group.Arn,
			Path: checkStrPtr("/"),
			CreateDate: &group.CreateDate,
		})
	}
	
	return resp, nil
}

// AddUserToGroup adds a user to a group
// https://docs.aws.amazon.com/IAM/latest/APIReference/API_AddUserToGroup.html
func (iama *IamApiServer) AddUserToGroup(ctx context.Context, s3cfg *iam_pb.S3ApiConfiguration, values url.Values) (resp AddUserToGroupResponse, iamError *IamError) {
	groupName := values.Get("GroupName")
	userName := values.Get("UserName")

	if groupName == "" || userName == "" {
		return resp, &IamError{Code: "ValidationError", Error: fmt.Errorf("GroupName and UserName are required")}
	}

	// Validate user exists (AWS compliance)
	_, err := iama.s3ApiConfig.GetUser(userName)
	if err != nil {
		return resp, &IamError{Code: iam.ErrCodeNoSuchEntityException, Error: fmt.Errorf("user not found: %s", userName)}
	}

	iamManager := iama.getIamManager()
	if iamManager == nil {
		return resp, &IamError{Code: iam.ErrCodeServiceFailureException, Error: fmt.Errorf("IAM Manager not initialized")}
	}

	store := iamManager.GetGroupStore()
	group, err := store.GetGroup(ctx, "", groupName)
	if err != nil {
		return resp, &IamError{Code: iam.ErrCodeNoSuchEntityException, Error: fmt.Errorf("group not found: %s", groupName)}
	}
	
	// Check if already member
	for _, m := range group.Members {
		if m == userName {
			return resp, nil // Idempotent
		}
	}
	
	group.Members = append(group.Members, userName)
	
	if err := store.StoreGroup(ctx, "", groupName, group); err != nil {
		return resp, &IamError{Code: iam.ErrCodeServiceFailureException, Error: err}
	}
	
	return resp, nil
}

// RemoveUserFromGroup removes a user from a group
// https://docs.aws.amazon.com/IAM/latest/APIReference/API_RemoveUserFromGroup.html
func (iama *IamApiServer) RemoveUserFromGroup(ctx context.Context, values url.Values) (resp RemoveUserFromGroupResponse, iamError *IamError) {
	groupName := values.Get("GroupName")
	userName := values.Get("UserName")

	if groupName == "" || userName == "" {
		return resp, &IamError{Code: "ValidationError", Error: fmt.Errorf("GroupName and UserName are required")}
	}

	iamManager := iama.getIamManager()
	if iamManager == nil {
		return resp, &IamError{Code: iam.ErrCodeServiceFailureException, Error: fmt.Errorf("IAM Manager not initialized")}
	}

	store := iamManager.GetGroupStore()
	group, err := store.GetGroup(ctx, "", groupName)
	if err != nil {
		return resp, &IamError{Code: iam.ErrCodeNoSuchEntityException, Error: fmt.Errorf("group not found: %s", groupName)}
	}
	
	newMembers := []string{}
	found := false
	for _, m := range group.Members {
		if m == userName {
			found = true
			continue
		}
		newMembers = append(newMembers, m)
	}
	
	if found {
		group.Members = newMembers
		if err := store.StoreGroup(ctx, "", groupName, group); err != nil {
			return resp, &IamError{Code: iam.ErrCodeServiceFailureException, Error: err}
		}
	}
	// AWS behavior: succeed silently if user not in group (idempotent)
	
	return resp, nil
}

// ListGroupsForUser lists groups for a user
// https://docs.aws.amazon.com/IAM/latest/APIReference/API_ListGroupsForUser.html
func (iama *IamApiServer) ListGroupsForUser(ctx context.Context, values url.Values) (resp ListGroupsForUserResponse, iamError *IamError) {
	userName := values.Get("UserName")
	if userName == "" {
		return resp, &IamError{Code: "ValidationError", Error: fmt.Errorf("UserName is required")}
	}

	iamManager := iama.getIamManager()
	if iamManager == nil {
		return resp, &IamError{Code: iam.ErrCodeServiceFailureException, Error: fmt.Errorf("IAM Manager not initialized")}
	}

	store := iamManager.GetGroupStore()
	groupNames, err := store.ListGroups(ctx, "")
	if err != nil {
		return resp, &IamError{Code: iam.ErrCodeServiceFailureException, Error: err}
	}
	
	for _, name := range groupNames {
		group, err := store.GetGroup(ctx, "", name)
		if err != nil {
			continue
		}
		
		isMember := false
		for _, m := range group.Members {
			if m == userName {
				isMember = true
				break
			}
		}
		
		if isMember {
			resp.ListGroupsForUserResult.Groups = append(resp.ListGroupsForUserResult.Groups, &iam.Group{
				GroupName: &group.GroupName,
				GroupId: &group.GroupId,
				Arn: &group.Arn,
				Path: checkStrPtr("/"),
				CreateDate: &group.CreateDate,
			})
		}
	}
	return resp, nil
}

// AttachGroupPolicy attaches a policy to a group
// https://docs.aws.amazon.com/IAM/latest/APIReference/API_AttachGroupPolicy.html
func (iama *IamApiServer) AttachGroupPolicy(ctx context.Context, values url.Values) (resp AttachGroupPolicyResponse, iamError *IamError) {
	groupName := values.Get("GroupName")
	policyArn := values.Get("PolicyArn")

	if groupName == "" || policyArn == "" {
		return resp, &IamError{Code: "ValidationError", Error: fmt.Errorf("GroupName and PolicyArn are required")}
	}

	iamManager := iama.getIamManager()
	if iamManager == nil {
		return resp, &IamError{Code: iam.ErrCodeServiceFailureException, Error: fmt.Errorf("IAM Manager not initialized")}
	}

	store := iamManager.GetGroupStore()
	group, err := store.GetGroup(ctx, "", groupName)
	if err != nil {
		return resp, &IamError{Code: iam.ErrCodeNoSuchEntityException, Error: fmt.Errorf("group not found: %s", groupName)}
	}

	policyName := extractPolicyNameFromArn(policyArn)

	// Check if already attached
	for _, p := range group.AttachedPolicies {
		if p == policyName {
			return resp, nil
		}
	}

	group.AttachedPolicies = append(group.AttachedPolicies, policyName)
	if err := store.StoreGroup(ctx, "", groupName, group); err != nil {
		return resp, &IamError{Code: iam.ErrCodeServiceFailureException, Error: err}
	}

	return resp, nil
}

// DetachGroupPolicy detaches a policy from a group
// https://docs.aws.amazon.com/IAM/latest/APIReference/API_DetachGroupPolicy.html
func (iama *IamApiServer) DetachGroupPolicy(ctx context.Context, values url.Values) (resp DetachGroupPolicyResponse, iamError *IamError) {
	groupName := values.Get("GroupName")
	policyArn := values.Get("PolicyArn")

	if groupName == "" || policyArn == "" {
		return resp, &IamError{Code: "ValidationError", Error: fmt.Errorf("GroupName and PolicyArn are required")}
	}

	iamManager := iama.getIamManager()
	if iamManager == nil {
		return resp, &IamError{Code: iam.ErrCodeServiceFailureException, Error: fmt.Errorf("IAM Manager not initialized")}
	}

	store := iamManager.GetGroupStore()
	group, err := store.GetGroup(ctx, "", groupName)
	if err != nil {
		return resp, &IamError{Code: iam.ErrCodeNoSuchEntityException, Error: fmt.Errorf("group not found: %s", groupName)}
	}

	policyName := extractPolicyNameFromArn(policyArn)

	newPolicies := []string{}
	found := false
	for _, p := range group.AttachedPolicies {
		if p == policyName {
			found = true
			continue
		}
		newPolicies = append(newPolicies, p)
	}

	if found {
		group.AttachedPolicies = newPolicies
		if err := store.StoreGroup(ctx, "", groupName, group); err != nil {
			return resp, &IamError{Code: iam.ErrCodeServiceFailureException, Error: err}
		}
	}

	return resp, nil
}

// ListAttachedGroupPolicies lists policies attached to a group
// https://docs.aws.amazon.com/IAM/latest/APIReference/API_ListAttachedGroupPolicies.html
func (iama *IamApiServer) ListAttachedGroupPolicies(ctx context.Context, values url.Values) (resp ListAttachedGroupPoliciesResponse, iamError *IamError) {
	groupName := values.Get("GroupName")
	if groupName == "" {
		return resp, &IamError{Code: "ValidationError", Error: fmt.Errorf("GroupName is required")}
	}

	iamManager := iama.getIamManager()
	if iamManager == nil {
		return resp, &IamError{Code: iam.ErrCodeServiceFailureException, Error: fmt.Errorf("IAM Manager not initialized")}
	}

	group, err := iamManager.GetGroupStore().GetGroup(ctx, "", groupName)
	if err != nil {
		return resp, &IamError{Code: iam.ErrCodeNoSuchEntityException, Error: fmt.Errorf("group not found: %s", groupName)}
	}

	for _, policyName := range group.AttachedPolicies {
		arn := fmt.Sprintf("arn:aws:iam:::policy/%s", policyName)
		resp.ListAttachedGroupPoliciesResult.AttachedPolicies = append(resp.ListAttachedGroupPoliciesResult.AttachedPolicies, &iam.AttachedPolicy{
			PolicyName: &policyName,
			PolicyArn:  &arn,
		})
	}

	return resp, nil
}

// CreatePolicyVersion creates a new version of the specified managed policy
// https://docs.aws.amazon.com/IAM/latest/APIReference/API_CreatePolicyVersion.html
func (iama *IamApiServer) CreatePolicyVersion(s3cfg *iam_pb.S3ApiConfiguration, values url.Values) (resp CreatePolicyVersionResponse, iamError *IamError) {
	policyArn := values.Get("PolicyArn")
	policyDocumentString := values.Get("PolicyDocument")

	if policyArn == "" {
		return resp, &IamError{Code: "ValidationError", Error: fmt.Errorf("PolicyArn is required")}
	}
	if policyDocumentString == "" {
		return resp, &IamError{Code: "ValidationError", Error: fmt.Errorf("PolicyDocument is required")}
	}

	// Extract policy name from ARN
	policyName := extractPolicyNameFromArn(policyArn)
	if policyName == "" {
		return resp, &IamError{Code: "ValidationError", Error: fmt.Errorf("Invalid PolicyArn format")}
	}

	// Validate SetAsDefault parameter
	// In our simplified v1-only versioning model, we only support SetAsDefault=true
	setAsDefault := values.Get("SetAsDefault")
	if setAsDefault != "" && setAsDefault != "true" {
		return resp, &IamError{Code: "InvalidParameterValue", Error: fmt.Errorf("SetAsDefault must be true. Non-default policy versions are not supported in this implementation.")}
	}

	// Parse new policy document
	newPolicyDoc, err := GetPolicyDocument(&policyDocumentString)
	if err != nil {
		return resp, &IamError{Code: iam.ErrCodeMalformedPolicyDocumentException, Error: err}
	}

	// Get existing policy to modify
	// Check if policy exists
	existingPolicy, err := iama.s3ApiConfig.GetPolicy(policyName)
	if err != nil {
		if err == filer_pb.ErrNotFound {
			return resp, &IamError{Code: iam.ErrCodeNoSuchEntityException, Error: fmt.Errorf("policy not found: %s", policyName)}
		}
		return resp, &IamError{Code: iam.ErrCodeServiceFailureException, Error: err}
	}

	// Update the policy document while preserving metadata
	// We are effectively "overwriting" the current version since we don't support full version history yet
	// But this satisfies the "Update" requirement
	
	// Preserve critical metadata
	if existingPolicy.Metadata != nil {
		newPolicyDoc.Metadata = existingPolicy.Metadata
		newPolicyDoc.Metadata.UpdatedAt = time.Now()
	} else {
		// Should not happen for managed policies, but handle gracefully
		newPolicyDoc.Metadata = &policy_engine.PolicyMetadata{
			Name:      policyName,
			PolicyId:  Hash(nil),
			Arn:       policyArn,
			CreatedAt: time.Now(),
			UpdatedAt: time.Now(),
		}
	}

	// Save the updated policy
	if err = iama.s3ApiConfig.PutPolicy(policyName, &newPolicyDoc); err != nil {
		return resp, &IamError{Code: iam.ErrCodeServiceFailureException, Error: err}
	}

	// Construct response with proper PolicyVersion metadata
	versionId := "v1"
	isDefault := true
	createDate := newPolicyDoc.Metadata.UpdatedAt

	// Encode document for response
	docBytes, err := json.Marshal(&newPolicyDoc)
	if err != nil {
		return resp, &IamError{Code: iam.ErrCodeServiceFailureException, Error: fmt.Errorf("failed to marshal policy document: %v", err)}
	}
	encodedDoc := url.QueryEscape(string(docBytes))
	encodedDoc = strings.ReplaceAll(encodedDoc, "+", "%20")

	resp.CreatePolicyVersionResult.PolicyVersion = iam.PolicyVersion{
		VersionId:        &versionId,
		Document:         &encodedDoc,
		IsDefaultVersion: &isDefault,
		CreateDate:       &createDate,
	}

	return resp, nil
}

// AttachUserPolicy attaches a managed policy to a user
// https://docs.aws.amazon.com/IAM/latest/APIReference/API_AttachUserPolicy.html
func (iama *IamApiServer) AttachUserPolicy(s3cfg *iam_pb.S3ApiConfiguration, values url.Values) (resp AttachUserPolicyResponse, iamError *IamError) {
	userName := values.Get("UserName")
	policyArn := values.Get("PolicyArn")

	if userName == "" || policyArn == "" {
		return resp, &IamError{Code: "ValidationError", Error: fmt.Errorf("UserName and PolicyArn are required")}
	}

	// 1. Get User
	user, err := iama.s3ApiConfig.GetUser(userName)
	if err != nil {
		if err == filer_pb.ErrNotFound {
			return resp, &IamError{Code: iam.ErrCodeNoSuchEntityException, Error: fmt.Errorf(USER_DOES_NOT_EXIST, userName)}
		}
		return resp, &IamError{Code: iam.ErrCodeServiceFailureException, Error: err}
	}

	// 2. Validate Policy ARN (basic check)
	policyName := extractPolicyNameFromArn(policyArn)
	if policyName == "" {
		return resp, &IamError{Code: "ValidationError", Error: fmt.Errorf("invalid PolicyArn format")}
	}

	// 3. Add to User's PolicyNames if not present
	found := false
	for _, p := range user.PolicyNames {
		if p == policyName {
			found = true
			break
		}
	}

	if !found {
		user.PolicyNames = append(user.PolicyNames, policyName)
		if err := iama.s3ApiConfig.UpdateUser(userName, user); err != nil {
			return resp, &IamError{Code: iam.ErrCodeServiceFailureException, Error: err}
		}
	}

	return resp, nil
}

// DetachUserPolicy detaches a managed policy from a user
// https://docs.aws.amazon.com/IAM/latest/APIReference/API_DetachUserPolicy.html
func (iama *IamApiServer) DetachUserPolicy(s3cfg *iam_pb.S3ApiConfiguration, values url.Values) (resp DetachUserPolicyResponse, iamError *IamError) {
	userName := values.Get("UserName")
	policyArn := values.Get("PolicyArn")

	if userName == "" || policyArn == "" {
		return resp, &IamError{Code: "ValidationError", Error: fmt.Errorf("UserName and PolicyArn are required")}
	}

	// 1. Get User
	user, err := iama.s3ApiConfig.GetUser(userName)
	if err != nil {
		if err == filer_pb.ErrNotFound {
			return resp, &IamError{Code: iam.ErrCodeNoSuchEntityException, Error: fmt.Errorf(USER_DOES_NOT_EXIST, userName)}
		}
		return resp, &IamError{Code: iam.ErrCodeServiceFailureException, Error: err}
	}

	// 2. Remove from User's PolicyNames
	policyName := extractPolicyNameFromArn(policyArn)
	newPolicies := []string{}
	found := false
	for _, p := range user.PolicyNames {
		if p == policyName {
			found = true
			continue
		}
		newPolicies = append(newPolicies, p)
	}

	if found {
		user.PolicyNames = newPolicies
		if err := iama.s3ApiConfig.UpdateUser(userName, user); err != nil {
			return resp, &IamError{Code: iam.ErrCodeServiceFailureException, Error: err}
		}
	}

	return resp, nil
}

// ListAttachedUserPolicies lists managed policies attached to a user
// https://docs.aws.amazon.com/IAM/latest/APIReference/API_ListAttachedUserPolicies.html
func (iama *IamApiServer) ListAttachedUserPolicies(s3cfg *iam_pb.S3ApiConfiguration, values url.Values) (resp ListAttachedUserPoliciesResponse, iamError *IamError) {
	userName := values.Get("UserName")
	if userName == "" {
		return resp, &IamError{Code: "ValidationError", Error: fmt.Errorf("UserName is required")}
	}

	// 1. Get User
	user, err := iama.s3ApiConfig.GetUser(userName)
	if err != nil {
		if err == filer_pb.ErrNotFound {
			return resp, &IamError{Code: iam.ErrCodeNoSuchEntityException, Error: fmt.Errorf(USER_DOES_NOT_EXIST, userName)}
		}
		return resp, &IamError{Code: iam.ErrCodeServiceFailureException, Error: err}
	}

	// 2. Return policies
	for _, policyName := range user.PolicyNames {
		pName := policyName // Copy for pointer
		arn := fmt.Sprintf("arn:aws:iam:::policy/%s", policyName)
		resp.ListAttachedUserPoliciesResult.AttachedPolicies = append(resp.ListAttachedUserPoliciesResult.AttachedPolicies, &iam.AttachedPolicy{
			PolicyName: &pName,
			PolicyArn:  &arn,
		})
	}

	return resp, nil
}
