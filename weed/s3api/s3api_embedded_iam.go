package s3api

// This file provides IAM API functionality embedded in the S3 server.
// NOTE: There is code duplication with weed/iamapi/iamapi_management_handlers.go.
// See GitHub issue #7747 for the planned refactoring to extract common IAM logic
// into a shared package.

import (
	"context"
	"crypto/rand"
	"crypto/sha1"
	"encoding/json"
	"encoding/xml"
	"errors"
	"fmt"
	"math/big"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/service/iam"
	"github.com/seaweedfs/seaweedfs/weed/credential"
	"github.com/seaweedfs/seaweedfs/weed/glog"
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

// IAM response types
type iamCommonResponse struct {
	ResponseMetadata struct {
		RequestId string `xml:"RequestId"`
	} `xml:"ResponseMetadata"`
}

func (r *iamCommonResponse) SetRequestId() {
	r.ResponseMetadata.RequestId = fmt.Sprintf("%d", time.Now().UnixNano())
}

type iamListUsersResponse struct {
	iamCommonResponse
	XMLName         xml.Name `xml:"https://iam.amazonaws.com/doc/2010-05-08/ ListUsersResponse"`
	ListUsersResult struct {
		Users       []*iam.User `xml:"Users>member"`
		IsTruncated bool        `xml:"IsTruncated"`
	} `xml:"ListUsersResult"`
}

type iamListAccessKeysResponse struct {
	iamCommonResponse
	XMLName              xml.Name `xml:"https://iam.amazonaws.com/doc/2010-05-08/ ListAccessKeysResponse"`
	ListAccessKeysResult struct {
		AccessKeyMetadata []*iam.AccessKeyMetadata `xml:"AccessKeyMetadata>member"`
		IsTruncated       bool                     `xml:"IsTruncated"`
	} `xml:"ListAccessKeysResult"`
}

type iamDeleteAccessKeyResponse struct {
	iamCommonResponse
	XMLName xml.Name `xml:"https://iam.amazonaws.com/doc/2010-05-08/ DeleteAccessKeyResponse"`
}

type iamCreatePolicyResponse struct {
	iamCommonResponse
	XMLName            xml.Name `xml:"https://iam.amazonaws.com/doc/2010-05-08/ CreatePolicyResponse"`
	CreatePolicyResult struct {
		Policy iam.Policy `xml:"Policy"`
	} `xml:"CreatePolicyResult"`
}

type iamCreateUserResponse struct {
	iamCommonResponse
	XMLName          xml.Name `xml:"https://iam.amazonaws.com/doc/2010-05-08/ CreateUserResponse"`
	CreateUserResult struct {
		User iam.User `xml:"User"`
	} `xml:"CreateUserResult"`
}

type iamDeleteUserResponse struct {
	iamCommonResponse
	XMLName xml.Name `xml:"https://iam.amazonaws.com/doc/2010-05-08/ DeleteUserResponse"`
}

type iamGetUserResponse struct {
	iamCommonResponse
	XMLName       xml.Name `xml:"https://iam.amazonaws.com/doc/2010-05-08/ GetUserResponse"`
	GetUserResult struct {
		User iam.User `xml:"User"`
	} `xml:"GetUserResult"`
}

type iamUpdateUserResponse struct {
	iamCommonResponse
	XMLName xml.Name `xml:"https://iam.amazonaws.com/doc/2010-05-08/ UpdateUserResponse"`
}

type iamCreateAccessKeyResponse struct {
	iamCommonResponse
	XMLName               xml.Name `xml:"https://iam.amazonaws.com/doc/2010-05-08/ CreateAccessKeyResponse"`
	CreateAccessKeyResult struct {
		AccessKey iam.AccessKey `xml:"AccessKey"`
	} `xml:"CreateAccessKeyResult"`
}

type iamPutUserPolicyResponse struct {
	iamCommonResponse
	XMLName xml.Name `xml:"https://iam.amazonaws.com/doc/2010-05-08/ PutUserPolicyResponse"`
}

type iamDeleteUserPolicyResponse struct {
	iamCommonResponse
	XMLName xml.Name `xml:"https://iam.amazonaws.com/doc/2010-05-08/ DeleteUserPolicyResponse"`
}

type iamGetUserPolicyResponse struct {
	iamCommonResponse
	XMLName             xml.Name `xml:"https://iam.amazonaws.com/doc/2010-05-08/ GetUserPolicyResponse"`
	GetUserPolicyResult struct {
		UserName       string `xml:"UserName"`
		PolicyName     string `xml:"PolicyName"`
		PolicyDocument string `xml:"PolicyDocument"`
	} `xml:"GetUserPolicyResult"`
}

type iamErrorResponse struct {
	iamCommonResponse
	XMLName xml.Name `xml:"https://iam.amazonaws.com/doc/2010-05-08/ ErrorResponse"`
	Error   struct {
		iam.ErrorDetails
		Type string `xml:"Type"`
	} `xml:"Error"`
}

type iamError struct {
	Code  string
	Error error
}

// Policies stores IAM policies
type iamPolicies struct {
	Policies map[string]policy_engine.PolicyDocument `json:"policies"`
}

const (
	iamCharsetUpper          = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	iamCharset               = iamCharsetUpper + "abcdefghijklmnopqrstuvwxyz/"
	iamPolicyDocumentVersion = "2012-10-17"
	iamUserDoesNotExist      = "the user with name %s cannot be found."
)

// Statement action constants
const (
	iamStatementActionAdmin    = "*"
	iamStatementActionWrite    = "Put*"
	iamStatementActionWriteAcp = "PutBucketAcl"
	iamStatementActionRead     = "Get*"
	iamStatementActionReadAcp  = "GetBucketAcl"
	iamStatementActionList     = "List*"
	iamStatementActionTagging  = "Tagging*"
	iamStatementActionDelete   = "DeleteBucket*"
)

func iamHash(s *string) string {
	h := sha1.New()
	h.Write([]byte(*s))
	return fmt.Sprintf("%x", h.Sum(nil))
}

// iamStringWithCharset generates a cryptographically secure random string.
// Uses crypto/rand for security-sensitive credential generation.
func iamStringWithCharset(length int, charset string) (string, error) {
	b := make([]byte, length)
	for i := range b {
		n, err := rand.Int(rand.Reader, big.NewInt(int64(len(charset))))
		if err != nil {
			return "", fmt.Errorf("failed to generate random index: %w", err)
		}
		b[i] = charset[n.Int64()]
	}
	return string(b), nil
}

// iamStringSlicesEqual compares two string slices for equality, ignoring order.
// This is used instead of reflect.DeepEqual to avoid order-dependent comparisons.
func iamStringSlicesEqual(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	// Make copies to avoid modifying the originals
	aCopy := make([]string, len(a))
	bCopy := make([]string, len(b))
	copy(aCopy, a)
	copy(bCopy, b)
	sort.Strings(aCopy)
	sort.Strings(bCopy)
	for i := range aCopy {
		if aCopy[i] != bCopy[i] {
			return false
		}
	}
	return true
}

func iamMapToStatementAction(action string) string {
	switch action {
	case iamStatementActionAdmin:
		return ACTION_ADMIN
	case iamStatementActionWrite:
		return ACTION_WRITE
	case iamStatementActionWriteAcp:
		return ACTION_WRITE_ACP
	case iamStatementActionRead:
		return ACTION_READ
	case iamStatementActionReadAcp:
		return ACTION_READ_ACP
	case iamStatementActionList:
		return ACTION_LIST
	case iamStatementActionTagging:
		return ACTION_TAGGING
	case iamStatementActionDelete:
		return ACTION_DELETE_BUCKET
	default:
		return ""
	}
}

func iamMapToIdentitiesAction(action string) string {
	switch action {
	case ACTION_ADMIN:
		return iamStatementActionAdmin
	case ACTION_WRITE:
		return iamStatementActionWrite
	case ACTION_WRITE_ACP:
		return iamStatementActionWriteAcp
	case ACTION_READ:
		return iamStatementActionRead
	case ACTION_READ_ACP:
		return iamStatementActionReadAcp
	case ACTION_LIST:
		return iamStatementActionList
	case ACTION_TAGGING:
		return iamStatementActionTagging
	case ACTION_DELETE_BUCKET:
		return iamStatementActionDelete
	default:
		return ""
	}
}

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
	case iam.ErrCodeMalformedPolicyDocumentException:
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
	status := iam.StatusTypeActive
	userName := values.Get("UserName")
	for _, ident := range s3cfg.Identities {
		if userName != "" && userName != ident.Name {
			continue
		}
		for _, cred := range ident.Credentials {
			resp.ListAccessKeysResult.AccessKeyMetadata = append(resp.ListAccessKeysResult.AccessKeyMetadata,
				&iam.AccessKeyMetadata{UserName: &ident.Name, AccessKeyId: &cred.AccessKey, Status: &status},
			)
		}
	}
	return resp
}

// CreateUser creates a new IAM user.
func (e *EmbeddedIamApi) CreateUser(s3cfg *iam_pb.S3ApiConfiguration, values url.Values) iamCreateUserResponse {
	var resp iamCreateUserResponse
	userName := values.Get("UserName")
	resp.CreateUserResult.User.UserName = &userName
	s3cfg.Identities = append(s3cfg.Identities, &iam_pb.Identity{Name: userName})
	return resp
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

	changed := false
	for _, ident := range s3cfg.Identities {
		if userName == ident.Name {
			ident.Credentials = append(ident.Credentials,
				&iam_pb.Credential{AccessKey: accessKeyId, SecretKey: secretAccessKey})
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
					},
				},
			},
		)
	}
	return resp, nil
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
			act := strings.Split(action, ":")
			resource := "*"
			if len(act) == 2 {
				resource = fmt.Sprintf("arn:aws:s3:::%s/*", act[1])
			}
			statements[resource] = append(statements[resource],
				fmt.Sprintf("s3:%s", iamMapToIdentitiesAction(act[0])),
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

// handleImplicitUsername adds username who signs the request to values if 'username' is not specified.
// According to AWS documentation: "If you do not specify a user name, IAM determines the user name
// implicitly based on the Amazon Web Services access key ID signing the request."
// This function extracts the AccessKeyId from the SigV4 credential and looks up the corresponding
// identity name in the credential store.
func (e *EmbeddedIamApi) handleImplicitUsername(r *http.Request, values url.Values) {
	if len(r.Header["Authorization"]) == 0 || values.Get("UserName") != "" {
		return
	}
	glog.V(4).Infof("Authorization field: %v", r.Header["Authorization"][0])
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
	// Look up the identity by access key to get the username
	identity, _, found := e.iam.LookupByAccessKey(accessKeyId)
	if !found {
		glog.V(4).Infof("Access key %s not found in credential store", accessKeyId)
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

		// Authenticate the request (signature verification)
		// We use ACTION_READ as a minimal action; actual permission checks are done below
		// based on the specific IAM action and whether it's a self-service operation
		identity, errCode := e.iam.authRequest(r, ACTION_READ)
		if errCode != s3err.ErrNone {
			s3err.WriteErrorResponse(w, r, errCode)
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
		response = e.CreateUser(s3cfg, values)
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
			glog.Errorf("UpdateUser: %+v", iamErr.Error)
			s3err.WriteErrorResponse(w, r, s3err.ErrInvalidRequest)
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
	}
	// Set RequestId for AWS compatibility
	if r, ok := response.(interface{ SetRequestId() }); ok {
		r.SetRequestId()
	}
	s3err.WriteXMLResponse(w, r, http.StatusOK, response)
}
