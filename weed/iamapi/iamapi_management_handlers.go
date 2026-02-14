package iamapi

// This file provides IAM API handlers for the standalone IAM server.
// Common IAM types and helpers are imported from the shared weed/iam package.

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"sync"

	"github.com/aws/aws-sdk-go/service/iam"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	iamlib "github.com/seaweedfs/seaweedfs/weed/iam"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/iam_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/policy_engine"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3err"
)

// Constants from shared package
const (
	charsetUpper            = iamlib.CharsetUpper
	charset                 = iamlib.Charset
	policyDocumentVersion   = iamlib.PolicyDocumentVersion
	StatementActionAdmin    = iamlib.StatementActionAdmin
	StatementActionWrite    = iamlib.StatementActionWrite
	StatementActionWriteAcp = iamlib.StatementActionWriteAcp
	StatementActionRead     = iamlib.StatementActionRead
	StatementActionReadAcp  = iamlib.StatementActionReadAcp
	StatementActionList     = iamlib.StatementActionList
	StatementActionTagging  = iamlib.StatementActionTagging
	StatementActionDelete   = iamlib.StatementActionDelete
	USER_DOES_NOT_EXIST     = iamlib.UserDoesNotExist
	accessKeyStatusActive   = iamlib.AccessKeyStatusActive
	accessKeyStatusInactive = iamlib.AccessKeyStatusInactive
)

var (
	policyDocuments = map[string]*policy_engine.PolicyDocument{}
	policyLock      = sync.RWMutex{}
)

// Helper function wrappers using shared package
func MapToStatementAction(action string) string {
	return iamlib.MapToStatementAction(action)
}

func MapToIdentitiesAction(action string) string {
	return iamlib.MapToIdentitiesAction(action)
}

type Policies struct {
	Policies map[string]policy_engine.PolicyDocument `json:"policies"`
}

func Hash(s *string) string {
	return iamlib.Hash(s)
}

func StringWithCharset(length int, charset string) (string, error) {
	return iamlib.GenerateRandomString(length, charset)
}

func stringSlicesEqual(a, b []string) bool {
	return iamlib.StringSlicesEqual(a, b)
}

func validateAccessKeyStatus(status string) error {
	switch status {
	case accessKeyStatusActive, accessKeyStatusInactive:
		return nil
	case "":
		return fmt.Errorf("Status parameter is required")
	default:
		return fmt.Errorf("Status must be '%s' or '%s'", accessKeyStatusActive, accessKeyStatusInactive)
	}
}

func (iama *IamApiServer) ListUsers(s3cfg *iam_pb.S3ApiConfiguration, values url.Values) (resp ListUsersResponse) {
	for _, ident := range s3cfg.Identities {
		resp.ListUsersResult.Users = append(resp.ListUsersResult.Users, &iam.User{UserName: &ident.Name})
	}
	return resp
}

func (iama *IamApiServer) ListAccessKeys(s3cfg *iam_pb.S3ApiConfiguration, values url.Values) (resp ListAccessKeysResponse) {
	userName := values.Get("UserName")
	for _, ident := range s3cfg.Identities {
		if userName != "" && userName != ident.Name {
			continue
		}
		for _, cred := range ident.Credentials {
			status := cred.Status
			if status == "" {
				status = accessKeyStatusActive
			}
			identName := ident.Name
			accessKey := cred.AccessKey
			resp.ListAccessKeysResult.AccessKeyMetadata = append(resp.ListAccessKeysResult.AccessKeyMetadata,
				&iam.AccessKeyMetadata{UserName: &identName, AccessKeyId: &accessKey, Status: &status},
			)
		}
	}
	return resp
}

func (iama *IamApiServer) CreateUser(s3cfg *iam_pb.S3ApiConfiguration, values url.Values) (resp CreateUserResponse) {
	userName := values.Get("UserName")
	resp.CreateUserResult.User.UserName = &userName
	s3cfg.Identities = append(s3cfg.Identities, &iam_pb.Identity{Name: userName})
	return resp
}

func (iama *IamApiServer) DeleteUser(s3cfg *iam_pb.S3ApiConfiguration, userName string) (resp DeleteUserResponse, err *IamError) {
	for i, ident := range s3cfg.Identities {
		if userName == ident.Name {
			s3cfg.Identities = append(s3cfg.Identities[:i], s3cfg.Identities[i+1:]...)
			return resp, nil
		}
	}
	return resp, &IamError{Code: iam.ErrCodeNoSuchEntityException, Error: fmt.Errorf(USER_DOES_NOT_EXIST, userName)}
}

func (iama *IamApiServer) GetUser(s3cfg *iam_pb.S3ApiConfiguration, userName string) (resp GetUserResponse, err *IamError) {
	for _, ident := range s3cfg.Identities {
		if userName == ident.Name {
			resp.GetUserResult.User = iam.User{UserName: &ident.Name}
			return resp, nil
		}
	}
	return resp, &IamError{Code: iam.ErrCodeNoSuchEntityException, Error: fmt.Errorf(USER_DOES_NOT_EXIST, userName)}
}

func (iama *IamApiServer) UpdateUser(s3cfg *iam_pb.S3ApiConfiguration, values url.Values) (resp UpdateUserResponse, err *IamError) {
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
	return resp, &IamError{Code: iam.ErrCodeNoSuchEntityException, Error: fmt.Errorf(USER_DOES_NOT_EXIST, userName)}
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
	resp.CreatePolicyResult.Policy.PolicyName = &policyName
	resp.CreatePolicyResult.Policy.Arn = &arn
	resp.CreatePolicyResult.Policy.PolicyId = &policyId
	policies := Policies{}
	// Note: Lock is already held by DoActions, no need to acquire here
	if err = iama.s3ApiConfig.GetPolicies(&policies); err != nil {
		return resp, &IamError{Code: iam.ErrCodeServiceFailureException, Error: err}
	}
	policies.Policies[policyName] = policyDocument
	if err = iama.s3ApiConfig.PutPolicies(&policies); err != nil {
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
	policyDocuments[policyName] = &policyDocument
	actions, err := GetActions(&policyDocument)
	if err != nil {
		return PutUserPolicyResponse{}, &IamError{Code: iam.ErrCodeMalformedPolicyDocumentException, Error: err}
	}
	// Log the actions
	glog.V(3).Infof("PutUserPolicy: actions=%v", actions)
	for _, ident := range s3cfg.Identities {
		if userName != ident.Name {
			continue
		}
		ident.Actions = actions
		return resp, nil
	}
	return PutUserPolicyResponse{}, &IamError{Code: iam.ErrCodeNoSuchEntityException, Error: fmt.Errorf("the user with name %s cannot be found", userName)}
}

func (iama *IamApiServer) GetUserPolicy(s3cfg *iam_pb.S3ApiConfiguration, values url.Values) (resp GetUserPolicyResponse, err *IamError) {
	userName := values.Get("UserName")
	policyName := values.Get("PolicyName")
	for _, ident := range s3cfg.Identities {
		if userName != ident.Name {
			continue
		}

		resp.GetUserPolicyResult.UserName = userName
		resp.GetUserPolicyResult.PolicyName = policyName
		if len(ident.Actions) == 0 {
			return resp, &IamError{Code: iam.ErrCodeNoSuchEntityException, Error: errors.New("no actions found")}
		}

		policyDocument := policy_engine.PolicyDocument{Version: policyDocumentVersion}
		statements := make(map[string][]string)
		for _, action := range ident.Actions {
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
				// Use order-independent comparison to avoid duplicates from different action orderings
				if stringSlicesEqual(statement.Action.Strings(), actions) {
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
			return resp, &IamError{Code: iam.ErrCodeServiceFailureException, Error: err}
		}
		resp.GetUserPolicyResult.PolicyDocument = string(policyDocumentJSON)
		return resp, nil
	}
	return resp, &IamError{Code: iam.ErrCodeNoSuchEntityException, Error: fmt.Errorf(USER_DOES_NOT_EXIST, userName)}
}

// DeleteUserPolicy removes the inline policy from a user (clears their actions).
func (iama *IamApiServer) DeleteUserPolicy(s3cfg *iam_pb.S3ApiConfiguration, values url.Values) (resp DeleteUserPolicyResponse, err *IamError) {
	userName := values.Get("UserName")
	for _, ident := range s3cfg.Identities {
		if ident.Name == userName {
			ident.Actions = nil
			return resp, nil
		}
	}
	return resp, &IamError{Code: iam.ErrCodeNoSuchEntityException, Error: fmt.Errorf(USER_DOES_NOT_EXIST, userName)}
}

func GetActions(policy *policy_engine.PolicyDocument) ([]string, error) {
	var actions []string

	for _, statement := range policy.Statement {
		if statement.Effect != policy_engine.PolicyEffectAllow {
			return nil, fmt.Errorf("not a valid effect: '%s'. Only 'Allow' is possible", statement.Effect)
		}
		for _, resource := range statement.Resource.Strings() {
			// Parse "arn:aws:s3:::my-bucket/shared/*"
			res := strings.Split(resource, ":")
			if len(res) != 6 || res[0] != "arn" || res[1] != "aws" || res[2] != "s3" {
				continue
			}
			for _, action := range statement.Action.Strings() {
				// Parse "s3:Get*"
				act := strings.Split(action, ":")
				if len(act) != 2 || act[0] != "s3" {
					continue
				}
				statementAction := MapToStatementAction(act[1])

				if statementAction == "" {
					return nil, fmt.Errorf("not a valid action: '%s'", act[1])
				}

				path := res[5]
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

func (iama *IamApiServer) CreateAccessKey(s3cfg *iam_pb.S3ApiConfiguration, values url.Values) (resp CreateAccessKeyResponse, iamErr *IamError) {
	userName := values.Get("UserName")
	status := iam.StatusTypeActive

	accessKeyId, err := StringWithCharset(21, charsetUpper)
	if err != nil {
		return resp, &IamError{Code: iam.ErrCodeServiceFailureException, Error: fmt.Errorf("failed to generate access key: %w", err)}
	}
	secretAccessKey, err := StringWithCharset(42, charset)
	if err != nil {
		return resp, &IamError{Code: iam.ErrCodeServiceFailureException, Error: fmt.Errorf("failed to generate secret key: %w", err)}
	}

	resp.CreateAccessKeyResult.AccessKey.AccessKeyId = &accessKeyId
	resp.CreateAccessKeyResult.AccessKey.SecretAccessKey = &secretAccessKey
	resp.CreateAccessKeyResult.AccessKey.UserName = &userName
	resp.CreateAccessKeyResult.AccessKey.Status = &status
	changed := false
	for _, ident := range s3cfg.Identities {
		if userName == ident.Name {
			ident.Credentials = append(ident.Credentials,
				&iam_pb.Credential{AccessKey: accessKeyId, SecretKey: secretAccessKey, Status: accessKeyStatusActive})
			changed = true
			break
		}
	}
	if !changed {
		s3cfg.Identities = append(s3cfg.Identities,
			&iam_pb.Identity{
				Name: userName,
				Credentials: []*iam_pb.Credential{
					{
						AccessKey: accessKeyId,
						SecretKey: secretAccessKey,
						Status:    accessKeyStatusActive,
					},
				},
			},
		)
	}
	return resp, nil
}

// UpdateAccessKey updates the status of an access key (Active or Inactive).
func (iama *IamApiServer) UpdateAccessKey(s3cfg *iam_pb.S3ApiConfiguration, values url.Values) (resp UpdateAccessKeyResponse, err *IamError) {
	userName := values.Get("UserName")
	accessKeyId := values.Get("AccessKeyId")
	status := values.Get("Status")

	if userName == "" {
		return resp, &IamError{Code: iam.ErrCodeInvalidInputException, Error: fmt.Errorf("UserName is required")}
	}
	if accessKeyId == "" {
		return resp, &IamError{Code: iam.ErrCodeInvalidInputException, Error: fmt.Errorf("AccessKeyId is required")}
	}
	if err := validateAccessKeyStatus(status); err != nil {
		return resp, &IamError{Code: iam.ErrCodeInvalidInputException, Error: err}
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
		return resp, &IamError{Code: iam.ErrCodeNoSuchEntityException, Error: fmt.Errorf("the access key with id %s for user %s cannot be found", accessKeyId, userName)}
	}
	return resp, &IamError{Code: iam.ErrCodeNoSuchEntityException, Error: fmt.Errorf(USER_DOES_NOT_EXIST, userName)}
}

func (iama *IamApiServer) DeleteAccessKey(s3cfg *iam_pb.S3ApiConfiguration, values url.Values) (resp DeleteAccessKeyResponse) {
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

// handleImplicitUsername adds username who signs the request to values if 'username' is not specified.
// According to AWS documentation: "If you do not specify a user name, IAM determines the user name
// implicitly based on the Amazon Web Services access key ID signing the request."
// This function extracts the AccessKeyId from the SigV4 credential and looks up the corresponding
// identity name in the credential store.
func (iama *IamApiServer) handleImplicitUsername(r *http.Request, values url.Values) {
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
	if iama.iam == nil {
		glog.V(4).Infof("IAM not initialized, cannot look up access key")
		return
	}
	// Look up the identity by access key to get the username
	identity, _, found := iama.iam.LookupByAccessKey(accessKeyId)
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

func (iama *IamApiServer) DoActions(w http.ResponseWriter, r *http.Request) {
	// Lock to prevent concurrent read-modify-write race conditions
	policyLock.Lock()
	defer policyLock.Unlock()

	if err := r.ParseForm(); err != nil {
		s3err.WriteErrorResponse(w, r, s3err.ErrInvalidRequest)
		return
	}
	values := r.PostForm
	s3cfg := &iam_pb.S3ApiConfiguration{}
	if err := iama.s3ApiConfig.GetS3ApiConfiguration(s3cfg); err != nil && !errors.Is(err, filer_pb.ErrNotFound) {
		s3err.WriteErrorResponse(w, r, s3err.ErrInternalError)
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
		iama.handleImplicitUsername(r, values)
		response = iama.ListAccessKeys(s3cfg, values)
		changed = false
	case "CreateUser":
		response = iama.CreateUser(s3cfg, values)
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
			s3err.WriteErrorResponse(w, r, s3err.ErrInvalidRequest)
			return
		}
	case "DeleteUser":
		userName := values.Get("UserName")
		response, iamError = iama.DeleteUser(s3cfg, userName)
		if iamError != nil {
			writeIamErrorResponse(w, r, iamError)
			return
		}
	case "CreateAccessKey":
		iama.handleImplicitUsername(r, values)
		response, iamError = iama.CreateAccessKey(s3cfg, values)
		if iamError != nil {
			glog.Errorf("CreateAccessKey: %+v", iamError.Error)
			writeIamErrorResponse(w, r, iamError)
			return
		}
	case "DeleteAccessKey":
		iama.handleImplicitUsername(r, values)
		response = iama.DeleteAccessKey(s3cfg, values)
	case "UpdateAccessKey":
		iama.handleImplicitUsername(r, values)
		response, iamError = iama.UpdateAccessKey(s3cfg, values)
		if iamError != nil {
			writeIamErrorResponse(w, r, iamError)
			return
		}
	case "CreatePolicy":
		response, iamError = iama.CreatePolicy(s3cfg, values)
		if iamError != nil {
			glog.Errorf("CreatePolicy:  %+v", iamError.Error)
			s3err.WriteErrorResponse(w, r, s3err.ErrInvalidRequest)
			return
		}
		// CreatePolicy persists the policy document via iama.s3ApiConfig.PutPolicies().
		// The `changed` flag is false because this does not modify the main s3cfg.Identities configuration.
		changed = false
	case "PutUserPolicy":
		var iamError *IamError
		response, iamError = iama.PutUserPolicy(s3cfg, values)
		if iamError != nil {
			glog.Errorf("PutUserPolicy:  %+v", iamError.Error)

			writeIamErrorResponse(w, r, iamError)
			return
		}
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
	default:
		errNotImplemented := s3err.GetAPIError(s3err.ErrNotImplemented)
		errorResponse := ErrorResponse{}
		errorResponse.Error.Code = &errNotImplemented.Code
		errorResponse.Error.Message = &errNotImplemented.Description
		s3err.WriteXMLResponse(w, r, errNotImplemented.HTTPStatusCode, errorResponse)
		return
	}
	if changed {
		err := iama.s3ApiConfig.PutS3ApiConfiguration(s3cfg)
		if err != nil {
			var iamError = IamError{Code: iam.ErrCodeServiceFailureException, Error: err}
			writeIamErrorResponse(w, r, &iamError)
			return
		}
		// Reload in-memory identity maps so subsequent LookupByAccessKey calls
		// can see newly created or deleted keys immediately
		if iama.iam != nil {
			if err := iama.iam.LoadS3ApiConfigurationFromCredentialManager(); err != nil {
				glog.Warningf("Failed to reload IAM configuration after mutation: %v", err)
				// Don't fail the request since the persistent save succeeded
			}
		}
	}
	s3err.WriteXMLResponse(w, r, http.StatusOK, response)
}
