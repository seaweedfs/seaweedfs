package iamapi

import (
	"encoding/xml"
	"net/http"
	"net/http/httptest"
	"net/url"
	"regexp"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/iam"
	"github.com/gorilla/mux"
	"github.com/jinzhu/copier"
	"github.com/seaweedfs/seaweedfs/weed/pb/iam_pb"
	"github.com/stretchr/testify/assert"
)

var GetS3ApiConfiguration func(s3cfg *iam_pb.S3ApiConfiguration) (err error)
var PutS3ApiConfiguration func(s3cfg *iam_pb.S3ApiConfiguration) (err error)
var GetPolicies func(policies *Policies) (err error)
var PutPolicies func(policies *Policies) (err error)

var s3config = iam_pb.S3ApiConfiguration{}
var policiesFile = Policies{Policies: make(map[string]PolicyDocument)}
var ias = IamApiServer{s3ApiConfig: iamS3ApiConfigureMock{}}

type iamS3ApiConfigureMock struct{}

func (iam iamS3ApiConfigureMock) GetS3ApiConfiguration(s3cfg *iam_pb.S3ApiConfiguration) (err error) {
	_ = copier.Copy(&s3cfg.Identities, &s3config.Identities)
	return nil
}

func (iam iamS3ApiConfigureMock) PutS3ApiConfiguration(s3cfg *iam_pb.S3ApiConfiguration) (err error) {
	_ = copier.Copy(&s3config.Identities, &s3cfg.Identities)
	return nil
}

func (iam iamS3ApiConfigureMock) GetPolicies(policies *Policies) (err error) {
	_ = copier.Copy(&policies, &policiesFile)
	return nil
}

func (iam iamS3ApiConfigureMock) PutPolicies(policies *Policies) (err error) {
	_ = copier.Copy(&policiesFile, &policies)
	return nil
}

func TestCreateUser(t *testing.T) {
	userName := aws.String("Test")
	params := &iam.CreateUserInput{UserName: userName}
	req, _ := iam.New(session.New()).CreateUserRequest(params)
	_ = req.Build()
	out := CreateUserResponse{}
	response, err := executeRequest(req.HTTPRequest, out)
	assert.Equal(t, nil, err)
	assert.Equal(t, http.StatusOK, response.Code)
	//assert.Equal(t, out.XMLName, "lol")
}

func TestListUsers(t *testing.T) {
	params := &iam.ListUsersInput{}
	req, _ := iam.New(session.New()).ListUsersRequest(params)
	_ = req.Build()
	out := ListUsersResponse{}
	response, err := executeRequest(req.HTTPRequest, out)
	assert.Equal(t, nil, err)
	assert.Equal(t, http.StatusOK, response.Code)
}

func TestListAccessKeys(t *testing.T) {
	svc := iam.New(session.New())
	params := &iam.ListAccessKeysInput{}
	req, _ := svc.ListAccessKeysRequest(params)
	_ = req.Build()
	out := ListAccessKeysResponse{}
	response, err := executeRequest(req.HTTPRequest, out)
	assert.Equal(t, nil, err)
	assert.Equal(t, http.StatusOK, response.Code)
}

func TestGetUser(t *testing.T) {
	userName := aws.String("Test")
	params := &iam.GetUserInput{UserName: userName}
	req, _ := iam.New(session.New()).GetUserRequest(params)
	_ = req.Build()
	out := GetUserResponse{}
	response, err := executeRequest(req.HTTPRequest, out)
	assert.Equal(t, nil, err)
	assert.Equal(t, http.StatusOK, response.Code)
}

// Todo flat statement
func TestCreatePolicy(t *testing.T) {
	params := &iam.CreatePolicyInput{
		PolicyName: aws.String("S3-read-only-example-bucket"),
		PolicyDocument: aws.String(`
			{
			  "Version": "2012-10-17",
			  "Statement": [
				{
				  "Effect": "Allow",
				  "Action": [
					"s3:Get*",
					"s3:List*"
				  ],
				  "Resource": [
					"arn:aws:s3:::EXAMPLE-BUCKET",
					"arn:aws:s3:::EXAMPLE-BUCKET/*"
				  ]
				}
			  ]
			}`),
	}
	req, _ := iam.New(session.New()).CreatePolicyRequest(params)
	_ = req.Build()
	out := CreatePolicyResponse{}
	response, err := executeRequest(req.HTTPRequest, out)
	assert.Equal(t, nil, err)
	assert.Equal(t, http.StatusOK, response.Code)
}

func TestPutUserPolicy(t *testing.T) {
	userName := aws.String("Test")
	params := &iam.PutUserPolicyInput{
		UserName:   userName,
		PolicyName: aws.String("S3-read-only-example-bucket"),
		PolicyDocument: aws.String(
			`{
				  "Version": "2012-10-17",
				  "Statement": [
					{
					  "Effect": "Allow",
					  "Action": [
						"s3:Get*",
						"s3:List*"
					  ],
					  "Resource": [
						"arn:aws:s3:::EXAMPLE-BUCKET",
						"arn:aws:s3:::EXAMPLE-BUCKET/*"
					  ]
					}
				  ]
			}`),
	}
	req, _ := iam.New(session.New()).PutUserPolicyRequest(params)
	_ = req.Build()
	out := PutUserPolicyResponse{}
	response, err := executeRequest(req.HTTPRequest, out)
	assert.Equal(t, nil, err)
	assert.Equal(t, http.StatusOK, response.Code)
}

func TestPutUserPolicyError(t *testing.T) {
	userName := aws.String("InvalidUser")
	params := &iam.PutUserPolicyInput{
		UserName:   userName,
		PolicyName: aws.String("S3-read-only-example-bucket"),
		PolicyDocument: aws.String(
			`{
				  "Version": "2012-10-17",
				  "Statement": [
					{
					  "Effect": "Allow",
					  "Action": [
						"s3:Get*",
						"s3:List*"
					  ],
					  "Resource": [
						"arn:aws:s3:::EXAMPLE-BUCKET",
						"arn:aws:s3:::EXAMPLE-BUCKET/*"
					  ]
					}
				  ]
			}`),
	}
	req, _ := iam.New(session.New()).PutUserPolicyRequest(params)
	_ = req.Build()
	response, err := executeRequest(req.HTTPRequest, nil)
	assert.Equal(t, nil, err)
	assert.Equal(t, http.StatusNotFound, response.Code)

	expectedMessage := "the user with name InvalidUser cannot be found"
	expectedCode := "NoSuchEntity"

	code, message := extractErrorCodeAndMessage(response)

	assert.Equal(t, expectedMessage, message)
	assert.Equal(t, expectedCode, code)
}

func extractErrorCodeAndMessage(response *httptest.ResponseRecorder) (string, string) {
	pattern := `<Error><Code>(.*)</Code><Message>(.*)</Message><Type>(.*)</Type></Error>`
	re := regexp.MustCompile(pattern)

	code := re.FindStringSubmatch(response.Body.String())[1]
	message := re.FindStringSubmatch(response.Body.String())[2]
	return code, message
}

func TestGetUserPolicy(t *testing.T) {
	userName := aws.String("Test")
	params := &iam.GetUserPolicyInput{UserName: userName, PolicyName: aws.String("S3-read-only-example-bucket")}
	req, _ := iam.New(session.New()).GetUserPolicyRequest(params)
	_ = req.Build()
	out := GetUserPolicyResponse{}
	response, err := executeRequest(req.HTTPRequest, out)
	assert.Equal(t, nil, err)
	assert.Equal(t, http.StatusOK, response.Code)
}

func TestUpdateUser(t *testing.T) {
	userName := aws.String("Test")
	newUserName := aws.String("Test-New")
	params := &iam.UpdateUserInput{NewUserName: newUserName, UserName: userName}
	req, _ := iam.New(session.New()).UpdateUserRequest(params)
	_ = req.Build()
	out := UpdateUserResponse{}
	response, err := executeRequest(req.HTTPRequest, out)
	assert.Equal(t, nil, err)
	assert.Equal(t, http.StatusOK, response.Code)
}

func TestDeleteUser(t *testing.T) {
	userName := aws.String("Test-New")
	params := &iam.DeleteUserInput{UserName: userName}
	req, _ := iam.New(session.New()).DeleteUserRequest(params)
	_ = req.Build()
	out := DeleteUserResponse{}
	response, err := executeRequest(req.HTTPRequest, out)
	assert.Equal(t, nil, err)
	assert.Equal(t, http.StatusOK, response.Code)
}

func executeRequest(req *http.Request, v interface{}) (*httptest.ResponseRecorder, error) {
	rr := httptest.NewRecorder()
	apiRouter := mux.NewRouter().SkipClean(true)
	apiRouter.Path("/").Methods(http.MethodPost).HandlerFunc(ias.DoActions)
	apiRouter.ServeHTTP(rr, req)
	return rr, xml.Unmarshal(rr.Body.Bytes(), &v)
}

func TestHandleImplicitUsername(t *testing.T) {
	var tests = []struct {
		r        *http.Request
		values   url.Values
		userName string
	}{
		{&http.Request{}, url.Values{}, ""},
		{&http.Request{Header: http.Header{"Authorization": []string{"AWS4-HMAC-SHA256 Credential=197FSAQ7HHTA48X64O3A/20220420/test1/iam/aws4_request, SignedHeaders=content-type;host;x-amz-date, Signature=6757dc6b3d7534d67e17842760310e99ee695408497f6edc4fdb84770c252dc8"}}}, url.Values{}, "test1"},
		{&http.Request{Header: http.Header{"Authorization": []string{"AWS4-HMAC-SHA256 =197FSAQ7HHTA48X64O3A/20220420/test1/iam/aws4_request, SignedHeaders=content-type;host;x-amz-date, Signature=6757dc6b3d7534d67e17842760310e99ee695408497f6edc4fdb84770c252dc8"}}}, url.Values{}, ""},
		{&http.Request{Header: http.Header{"Authorization": []string{"AWS4-HMAC-SHA256 Credential=197FSAQ7HHTA48X64O3A/20220420/test1/iam/aws4_request SignedHeaders=content-type;host;x-amz-date Signature=6757dc6b3d7534d67e17842760310e99ee695408497f6edc4fdb84770c252dc8"}}}, url.Values{}, ""},
		{&http.Request{Header: http.Header{"Authorization": []string{"AWS4-HMAC-SHA256 Credential=197FSAQ7HHTA48X64O3A/20220420/test1/iam, SignedHeaders=content-type;host;x-amz-date, Signature=6757dc6b3d7534d67e17842760310e99ee695408497f6edc4fdb84770c252dc8"}}}, url.Values{}, ""},
	}

	for i, test := range tests {
		handleImplicitUsername(test.r, test.values)
		if un := test.values.Get("UserName"); un != test.userName {
			t.Errorf("No.%d: Got: %v, Expected: %v", i, un, test.userName)
		}
	}
}
