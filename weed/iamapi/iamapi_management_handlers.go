package iamapi

import (
	"crypto/sha1"
	"encoding/json"
	"fmt"
	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/pb/iam_pb"
	"github.com/chrislusf/seaweedfs/weed/s3api/s3_constants"
	"github.com/chrislusf/seaweedfs/weed/s3api/s3err"
	"math/rand"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/service/iam"
)

const (
	charsetUpper = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	charset      = charsetUpper + "abcdefghijklmnopqrstuvwxyz/"
)

var (
	seededRand *rand.Rand = rand.New(
		rand.NewSource(time.Now().UnixNano()))
	policyDocuments = map[string]*PolicyDocument{}
)

type Statement struct {
	Effect   string   `json:"Effect"`
	Action   []string `json:"Action"`
	Resource []string `json:"Resource"`
}

type PolicyDocument struct {
	Version   string       `json:"Version"`
	Statement []*Statement `json:"Statement"`
}

func Hash(s *string) string {
	h := sha1.New()
	h.Write([]byte(*s))
	return fmt.Sprintf("%x", h.Sum(nil))
}

func StringWithCharset(length int, charset string) string {
	b := make([]byte, length)
	for i := range b {
		b[i] = charset[seededRand.Intn(len(charset))]
	}
	return string(b)
}

func (iama *IamApiServer) ListUsers(s3cfg *iam_pb.S3ApiConfiguration, values url.Values) (resp ListUsersResponse) {
	for _, ident := range s3cfg.Identities {
		resp.ListUsersResult.Users = append(resp.ListUsersResult.Users, &iam.User{UserName: &ident.Name})
	}
	return resp
}

func (iama *IamApiServer) ListAccessKeys(s3cfg *iam_pb.S3ApiConfiguration, values url.Values) (resp ListAccessKeysResponse) {
	status := iam.StatusTypeActive
	for _, ident := range s3cfg.Identities {
		for _, cred := range ident.Credentials {
			resp.ListAccessKeysResult.AccessKeyMetadata = append(resp.ListAccessKeysResult.AccessKeyMetadata,
				&iam.AccessKeyMetadata{UserName: &ident.Name, AccessKeyId: &cred.AccessKey, Status: &status},
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

func (iama *IamApiServer) DeleteUser(s3cfg *iam_pb.S3ApiConfiguration, userName string) (resp DeleteUserResponse, err error) {
	for i, ident := range s3cfg.Identities {
		if userName == ident.Name {
			ident.Credentials = append(ident.Credentials[:i], ident.Credentials[i+1:]...)
			return resp, nil
		}
	}
	return resp, fmt.Errorf(iam.ErrCodeNoSuchEntityException)
}

func (iama *IamApiServer) GetUser(s3cfg *iam_pb.S3ApiConfiguration, userName string) (resp GetUserResponse, err error) {
	for _, ident := range s3cfg.Identities {
		if userName == ident.Name {
			resp.GetUserResult.User = iam.User{UserName: &ident.Name}
			return resp, nil
		}
	}
	return resp, fmt.Errorf(iam.ErrCodeNoSuchEntityException)
}

func GetPolicyDocument(policy *string) (policyDocument PolicyDocument, err error) {
	if err = json.Unmarshal([]byte(*policy), &policyDocument); err != nil {
		return PolicyDocument{}, err
	}
	return policyDocument, err
}

func (iama *IamApiServer) CreatePolicy(s3cfg *iam_pb.S3ApiConfiguration, values url.Values) (resp CreatePolicyResponse, err error) {
	policyName := values.Get("PolicyName")
	policyDocumentString := values.Get("PolicyDocument")
	policyDocument, err := GetPolicyDocument(&policyDocumentString)
	if err != nil {
		return CreatePolicyResponse{}, err
	}
	policyId := Hash(&policyDocumentString)
	arn := fmt.Sprintf("arn:aws:iam:::policy/%s", policyName)
	resp.CreatePolicyResult.Policy.PolicyName = &policyName
	resp.CreatePolicyResult.Policy.Arn = &arn
	resp.CreatePolicyResult.Policy.PolicyId = &policyId
	policyDocuments[policyName] = &policyDocument
	return resp, nil
}

func (iama *IamApiServer) PutUserPolicy(s3cfg *iam_pb.S3ApiConfiguration, values url.Values) (resp PutUserPolicyResponse, err error) {
	userName := values.Get("UserName")
	policyName := values.Get("PolicyName")
	policyDocumentString := values.Get("PolicyDocument")
	policyDocument, err := GetPolicyDocument(&policyDocumentString)
	if err != nil {
		return PutUserPolicyResponse{}, err
	}
	policyDocuments[policyName] = &policyDocument
	actions := GetActions(&policyDocument)
	for _, ident := range s3cfg.Identities {
		if userName == ident.Name {
			for _, action := range actions {
				ident.Actions = append(ident.Actions, action)
			}
			break
		}
	}
	return resp, nil
}

func (iama *IamApiServer) DeleteUserPolicy(s3cfg *iam_pb.S3ApiConfiguration, values url.Values) (resp PutUserPolicyResponse, err error) {
	userName := values.Get("UserName")
	for i, ident := range s3cfg.Identities {
		if ident.Name == userName {
			s3cfg.Identities = append(s3cfg.Identities[:i], s3cfg.Identities[i+1:]...)
			return resp, nil
		}
	}
	return resp, fmt.Errorf(iam.ErrCodeNoSuchEntityException)
}

func MapAction(action string) string {
	switch action {
	case "*":
		return s3_constants.ACTION_ADMIN
	case "Put*":
		return s3_constants.ACTION_WRITE
	case "Get*":
		return s3_constants.ACTION_READ
	case "List*":
		return s3_constants.ACTION_LIST
	default:
		return s3_constants.ACTION_TAGGING
	}
}

func GetActions(policy *PolicyDocument) (actions []string) {
	for _, statement := range policy.Statement {
		if statement.Effect != "Allow" {
			continue
		}
		for _, resource := range statement.Resource {
			// Parse "arn:aws:s3:::my-bucket/shared/*"
			res := strings.Split(resource, ":")
			if len(res) != 6 || res[0] != "arn" || res[1] != "aws" || res[2] != "s3" {
				glog.Infof("not match resource: %s", res)
				continue
			}
			for _, action := range statement.Action {
				// Parse "s3:Get*"
				act := strings.Split(action, ":")
				if len(act) != 2 || act[0] != "s3" {
					glog.Infof("not match action: %s", act)
					continue
				}
				if res[5] == "*" {
					actions = append(actions, MapAction(act[1]))
					continue
				}
				// Parse my-bucket/shared/*
				path := strings.Split(res[5], "/")
				if len(path) != 2 || path[1] != "*" {
					glog.Infof("not match bucket: %s", path)
					continue
				}
				actions = append(actions, fmt.Sprintf("%s:%s", MapAction(act[1]), path[0]))
			}
		}
	}
	return actions
}

func (iama *IamApiServer) CreateAccessKey(s3cfg *iam_pb.S3ApiConfiguration, values url.Values) (resp CreateAccessKeyResponse) {
	userName := values.Get("UserName")
	status := iam.StatusTypeActive
	accessKeyId := StringWithCharset(21, charsetUpper)
	secretAccessKey := StringWithCharset(42, charset)
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
			&iam_pb.Identity{Name: userName,
				Credentials: []*iam_pb.Credential{
					{
						AccessKey: accessKeyId,
						SecretKey: secretAccessKey,
					},
				},
			},
		)
	}
	return resp
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

func (iama *IamApiServer) DoActions(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		writeErrorResponse(w, s3err.ErrInvalidRequest, r.URL)
		return
	}
	values := r.PostForm
	var s3cfgLock sync.RWMutex
	s3cfgLock.RLock()
	s3cfg := &iam_pb.S3ApiConfiguration{}
	if err := iama.s3ApiConfig.GetS3ApiConfiguration(s3cfg); err != nil {
		writeErrorResponse(w, s3err.ErrInternalError, r.URL)
		return
	}
	s3cfgLock.RUnlock()

	glog.V(4).Infof("DoActions: %+v", values)
	var response interface{}
	var err error
	changed := true
	switch r.Form.Get("Action") {
	case "ListUsers":
		response = iama.ListUsers(s3cfg, values)
		changed = false
	case "ListAccessKeys":
		response = iama.ListAccessKeys(s3cfg, values)
		changed = false
	case "CreateUser":
		response = iama.CreateUser(s3cfg, values)
	case "GetUser":
		userName := values.Get("UserName")
		response, err = iama.GetUser(s3cfg, userName)
		if err != nil {
			writeIamErrorResponse(w, err, "user", userName, nil)
			return
		}
	case "DeleteUser":
		userName := values.Get("UserName")
		response, err = iama.DeleteUser(s3cfg, userName)
		if err != nil {
			writeIamErrorResponse(w, err, "user", userName, nil)
			return
		}
	case "CreateAccessKey":
		response = iama.CreateAccessKey(s3cfg, values)
	case "DeleteAccessKey":
		response = iama.DeleteAccessKey(s3cfg, values)
	case "CreatePolicy":
		response, err = iama.CreatePolicy(s3cfg, values)
		if err != nil {
			glog.Errorf("CreatePolicy:  %+v", err)
			writeErrorResponse(w, s3err.ErrInvalidRequest, r.URL)
			return
		}
	case "PutUserPolicy":
		response, err = iama.PutUserPolicy(s3cfg, values)
		if err != nil {
			glog.Errorf("PutUserPolicy:  %+v", err)
			writeErrorResponse(w, s3err.ErrInvalidRequest, r.URL)
			return
		}
	case "DeleteUserPolicy":
		if response, err = iama.DeleteUserPolicy(s3cfg, values); err != nil {
			writeIamErrorResponse(w, err, "user", values.Get("UserName"), nil)
		}
	default:
		errNotImplemented := s3err.GetAPIError(s3err.ErrNotImplemented)
		errorResponse := ErrorResponse{}
		errorResponse.Error.Code = &errNotImplemented.Code
		errorResponse.Error.Message = &errNotImplemented.Description
		writeResponse(w, errNotImplemented.HTTPStatusCode, encodeResponse(errorResponse), mimeXML)
		return
	}
	if changed {
		s3cfgLock.Lock()
		err := iama.s3ApiConfig.PutS3ApiConfiguration(s3cfg)
		s3cfgLock.Unlock()
		if err != nil {
			writeIamErrorResponse(w, fmt.Errorf(iam.ErrCodeServiceFailureException), "", "", err)
			return
		}
	}
	writeSuccessResponseXML(w, encodeResponse(response))
}
