package iamapi

import (
	"bytes"
	"crypto/sha1"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"github.com/chrislusf/seaweedfs/weed/filer"
	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/pb/iam_pb"
	"github.com/chrislusf/seaweedfs/weed/s3api/s3_constants"
	"github.com/chrislusf/seaweedfs/weed/s3api/s3err"
	"math/rand"
	"net/http"
	"net/url"
	"strings"
	"time"

	//	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/iam"
)

const (
	version      = "2010-05-08"
	charsetUpper = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	charset      = charsetUpper + "abcdefghijklmnopqrstuvwxyz/"
)

var (
	seededRand *rand.Rand = rand.New(
		rand.NewSource(time.Now().UnixNano()))
	policyDocuments = map[string]*PolicyDocument{}
)

type PolicyDocument struct {
	Version   string `json:"Version"`
	Statement []struct {
		Effect   string   `json:"Effect"`
		Action   []string `json:"Action"`
		Resource []string `json:"Resource"`
	} `json:"Statement"`
}

type CommonResponse struct {
	ResponseMetadata struct {
		RequestId string `xml:"RequestId"`
	} `xml:"ResponseMetadata"`
}

type ListUsersResponse struct {
	CommonResponse
	XMLName         xml.Name `xml:"https://iam.amazonaws.com/doc/2010-05-08/ ListUsersResponse"`
	ListUsersResult struct {
		Users       []*iam.User `xml:"Users>member"`
		IsTruncated bool        `xml:"IsTruncated"`
	} `xml:"ListUsersResult"`
}

type ListAccessKeysResponse struct {
	CommonResponse
	XMLName              xml.Name `xml:"https://iam.amazonaws.com/doc/2010-05-08/ ListAccessKeysResponse"`
	ListAccessKeysResult struct {
		AccessKeyMetadata []*iam.AccessKeyMetadata `xml:"AccessKeyMetadata>member"`
		IsTruncated       bool                     `xml:"IsTruncated"`
	} `xml:"ListAccessKeysResult"`
}

type DeleteUserResponse struct {
	CommonResponse
	XMLName xml.Name `xml:"https://iam.amazonaws.com/doc/2010-05-08/ DeleteUserResponse"`
}

type DeleteAccessKeyResponse struct {
	CommonResponse
	XMLName xml.Name `xml:"https://iam.amazonaws.com/doc/2010-05-08/ DeleteAccessKeyResponse"`
}

type CreatePolicyResponse struct {
	CommonResponse
	XMLName            xml.Name `xml:"https://iam.amazonaws.com/doc/2010-05-08/ CreatePolicyResponse"`
	CreatePolicyResult struct {
		Policy iam.Policy `xml:"Policy"`
	} `xml:"CreatePolicyResult"`
}

type CreateUserResponse struct {
	CommonResponse
	XMLName          xml.Name `xml:"https://iam.amazonaws.com/doc/2010-05-08/ CreateUserResponse"`
	CreateUserResult struct {
		User iam.User `xml:"User"`
	} `xml:"CreateUserResult"`
}

type CreateAccessKeyResponse struct {
	CommonResponse
	XMLName               xml.Name `xml:"https://iam.amazonaws.com/doc/2010-05-08/ CreateAccessKeyResponse"`
	CreateAccessKeyResult struct {
		AccessKey iam.AccessKey `xml:"AccessKey"`
	} `xml:"CreateAccessKeyResult"`
}

type PutUserPolicyResponse struct {
	CommonResponse
	XMLName xml.Name `xml:"https://iam.amazonaws.com/doc/2010-05-08/ PutUserPolicyResponse"`
}

func (r *CommonResponse) SetRequestId() {
	r.ResponseMetadata.RequestId = fmt.Sprintf("%d", time.Now().UnixNano())
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
				glog.Infof("not math resource: %s", res)
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

func (iama *IamApiServer) DeleteUser(s3cfg *iam_pb.S3ApiConfiguration, values url.Values) (resp DeleteUserResponse) {
	userName := values.Get("UserName")
	for i, ident := range s3cfg.Identities {
		if userName == ident.Name {
			ident.Credentials = append(ident.Credentials[:i], ident.Credentials[i+1:]...)
			break
		}
	}
	return resp
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
	s3cfg := &iam_pb.S3ApiConfiguration{}
	if err := iama.GetS3ApiConfiguration(s3cfg); err != nil {
		writeErrorResponse(w, s3err.ErrInternalError, r.URL)
		return
	}

	glog.Info("values ", values)
	var response interface{}
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
	case "DeleteUser":
		response = iama.DeleteUser(s3cfg, values)
	case "CreateAccessKey":
		response = iama.CreateAccessKey(s3cfg, values)
	case "DeleteAccessKey":
		response = iama.DeleteAccessKey(s3cfg, values)
	case "CreatePolicy":
		var err error
		response, err = iama.CreatePolicy(s3cfg, values)
		if err != nil {
			writeErrorResponse(w, s3err.ErrInvalidRequest, r.URL)
			return
		}
	case "PutUserPolicy":
		var err error
		response, err = iama.PutUserPolicy(s3cfg, values)
		if err != nil {
			writeErrorResponse(w, s3err.ErrInvalidRequest, r.URL)
			return
		}
	default:
		writeErrorResponse(w, s3err.ErrNotImplemented, r.URL)
		return
	}
	if changed {
		buf := bytes.Buffer{}
		if err := filer.S3ConfigurationToText(&buf, s3cfg); err != nil {
			glog.Error("S3ConfigurationToText: ", err)
			writeErrorResponse(w, s3err.ErrInternalError, r.URL)
			return
		}
		if err := filer.SaveAs(
			iama.option.Filer,
			0,
			filer.IamConfigDirecotry,
			filer.IamIdentityFile,
			"text/plain; charset=utf-8",
			&buf); err != nil {
			glog.Error("SaveAs: ", err)
			writeErrorResponse(w, s3err.ErrInternalError, r.URL)
			return
		}
	}
	writeSuccessResponseXML(w, encodeResponse(response))
}
