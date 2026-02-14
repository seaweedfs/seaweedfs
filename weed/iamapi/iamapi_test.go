package iamapi

import (
	"encoding/json"
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
	"github.com/seaweedfs/seaweedfs/weed/s3api"
	"github.com/seaweedfs/seaweedfs/weed/s3api/policy_engine"
	"github.com/stretchr/testify/assert"
)

var GetS3ApiConfiguration func(s3cfg *iam_pb.S3ApiConfiguration) (err error)
var PutS3ApiConfiguration func(s3cfg *iam_pb.S3ApiConfiguration) (err error)
var GetPolicies func(policies *Policies) (err error)
var PutPolicies func(policies *Policies) (err error)

var s3config = iam_pb.S3ApiConfiguration{}
var policiesFile = Policies{Policies: make(map[string]policy_engine.PolicyDocument)}
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

func TestUpdateAccessKey(t *testing.T) {
	svc := iam.New(session.New())

	createReq, _ := svc.CreateAccessKeyRequest(&iam.CreateAccessKeyInput{UserName: aws.String("Test")})
	_ = createReq.Build()
	createOut := CreateAccessKeyResponse{}
	response, err := executeRequest(createReq.HTTPRequest, createOut)
	assert.Equal(t, nil, err)
	assert.Equal(t, http.StatusOK, response.Code)

	var createResp CreateAccessKeyResponse
	err = xml.Unmarshal(response.Body.Bytes(), &createResp)
	assert.Equal(t, nil, err)
	accessKeyId := createResp.CreateAccessKeyResult.AccessKey.AccessKeyId
	if accessKeyId == nil {
		t.Fatalf("expected access key id to be set")
	}

	updateReq, _ := svc.UpdateAccessKeyRequest(&iam.UpdateAccessKeyInput{
		UserName:    aws.String("Test"),
		AccessKeyId: accessKeyId,
		Status:      aws.String("Inactive"),
	})
	_ = updateReq.Build()
	updateOut := UpdateAccessKeyResponse{}
	response, err = executeRequest(updateReq.HTTPRequest, updateOut)
	assert.Equal(t, nil, err)
	assert.Equal(t, http.StatusOK, response.Code)

	listReq, _ := svc.ListAccessKeysRequest(&iam.ListAccessKeysInput{UserName: aws.String("Test")})
	_ = listReq.Build()
	listOut := ListAccessKeysResponse{}
	response, err = executeRequest(listReq.HTTPRequest, listOut)
	assert.Equal(t, nil, err)
	assert.Equal(t, http.StatusOK, response.Code)

	var listResp ListAccessKeysResponse
	err = xml.Unmarshal(response.Body.Bytes(), &listResp)
	assert.Equal(t, nil, err)
	found := false
	for _, key := range listResp.ListAccessKeysResult.AccessKeyMetadata {
		if key.AccessKeyId != nil && *key.AccessKeyId == *accessKeyId {
			found = true
			if assert.NotNil(t, key.Status) {
				assert.Equal(t, "Inactive", *key.Status)
			}
			break
		}
	}
	assert.True(t, found)
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
	// Create a mock IamApiServer with credential store
	// The handleImplicitUsername function now looks up the username from the
	// credential store based on AccessKeyId, not from the region field in the auth header.
	// Note: Using obviously fake access keys to avoid CI secret scanner false positives

	// Create IAM directly as struct literal (same pattern as other tests)
	iam := &s3api.IdentityAccessManagement{}

	// Load test credentials - map access key to identity name
	testConfig := &iam_pb.S3ApiConfiguration{
		Identities: []*iam_pb.Identity{
			{
				Name: "testuser1",
				Credentials: []*iam_pb.Credential{
					{AccessKey: "AKIATESTFAKEKEY000001", SecretKey: "testsecretfake"},
				},
			},
		},
	}
	err := iam.LoadS3ApiConfigurationFromBytes(mustMarshalJSON(t, testConfig))
	if err != nil {
		t.Fatalf("Failed to load test config: %v", err)
	}

	iama := &IamApiServer{
		iam: iam,
	}

	var tests = []struct {
		r        *http.Request
		values   url.Values
		userName string
	}{
		// No authorization header - should not set username
		{&http.Request{}, url.Values{}, ""},
		// Valid auth header with known access key - should look up and find "testuser1"
		{&http.Request{Header: http.Header{"Authorization": []string{"AWS4-HMAC-SHA256 Credential=AKIATESTFAKEKEY000001/20220420/us-east-1/iam/aws4_request, SignedHeaders=content-type;host;x-amz-date, Signature=fakesignature0123456789abcdef"}}}, url.Values{}, "testuser1"},
		// Malformed auth header (no Credential=) - should not set username
		{&http.Request{Header: http.Header{"Authorization": []string{"AWS4-HMAC-SHA256 =AKIATESTFAKEKEY000001/20220420/test1/iam/aws4_request, SignedHeaders=content-type;host;x-amz-date, Signature=fakesignature0123456789abcdef"}}}, url.Values{}, ""},
		// Unknown access key - should not set username
		{&http.Request{Header: http.Header{"Authorization": []string{"AWS4-HMAC-SHA256 Credential=AKIATESTUNKNOWN000000/20220420/us-east-1/iam/aws4_request, SignedHeaders=content-type;host;x-amz-date, Signature=fakesignature0123456789abcdef"}}}, url.Values{}, ""},
	}

	for i, test := range tests {
		iama.handleImplicitUsername(test.r, test.values)
		if un := test.values.Get("UserName"); un != test.userName {
			t.Errorf("No.%d: Got: %v, Expected: %v", i, un, test.userName)
		}
	}
}

func mustMarshalJSON(t *testing.T, v interface{}) []byte {
	t.Helper()
	data, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("failed to marshal JSON: %v", err)
	}
	return data
}
