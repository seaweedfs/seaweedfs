package s3api

import (
	"encoding/json"
	"encoding/xml"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/iam"
	"github.com/gorilla/mux"
	"github.com/seaweedfs/seaweedfs/weed/pb/iam_pb"
	. "github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3err"
	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/proto"
)

// EmbeddedIamApiForTest is a testable version of EmbeddedIamApi
type EmbeddedIamApiForTest struct {
	*EmbeddedIamApi
	mockConfig *iam_pb.S3ApiConfiguration
}

func NewEmbeddedIamApiForTest() *EmbeddedIamApiForTest {
	e := &EmbeddedIamApiForTest{
		EmbeddedIamApi: &EmbeddedIamApi{
			iam: &IdentityAccessManagement{},
		},
		mockConfig: &iam_pb.S3ApiConfiguration{},
	}
	e.getS3ApiConfigurationFunc = func(s3cfg *iam_pb.S3ApiConfiguration) error {
		if e.mockConfig != nil {
			cloned := proto.Clone(e.mockConfig).(*iam_pb.S3ApiConfiguration)
			proto.Merge(s3cfg, cloned)
		}
		return nil
	}
	e.putS3ApiConfigurationFunc = func(s3cfg *iam_pb.S3ApiConfiguration) error {
		e.mockConfig = proto.Clone(s3cfg).(*iam_pb.S3ApiConfiguration)
		return nil
	}
	e.reloadConfigurationFunc = func() error {
		return nil
	}
	return e
}

// Override GetS3ApiConfiguration for testing
func (e *EmbeddedIamApiForTest) GetS3ApiConfiguration(s3cfg *iam_pb.S3ApiConfiguration) error {
	// Use proto.Clone for proper deep copy semantics
	if e.mockConfig != nil {
		cloned := proto.Clone(e.mockConfig).(*iam_pb.S3ApiConfiguration)
		proto.Merge(s3cfg, cloned)
	}
	return nil
}

// Override PutS3ApiConfiguration for testing
func (e *EmbeddedIamApiForTest) PutS3ApiConfiguration(s3cfg *iam_pb.S3ApiConfiguration) error {
	// Use proto.Clone for proper deep copy semantics
	e.mockConfig = proto.Clone(s3cfg).(*iam_pb.S3ApiConfiguration)
	return nil
}

// DoActions handles IAM API actions for testing
func (e *EmbeddedIamApiForTest) DoActions(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		http.Error(w, "Invalid request", http.StatusBadRequest)
		return
	}
	values := r.PostForm
	s3cfg := &iam_pb.S3ApiConfiguration{}
	if err := e.GetS3ApiConfiguration(s3cfg); err != nil {
		http.Error(w, "Internal error", http.StatusInternalServerError)
		return
	}

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
			http.Error(w, "Invalid request", http.StatusBadRequest)
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
			http.Error(w, "Internal error", http.StatusInternalServerError)
			return
		}
	case "DeleteAccessKey":
		e.handleImplicitUsername(r, values)
		response = e.DeleteAccessKey(s3cfg, values)
	case "CreatePolicy":
		response, iamErr = e.CreatePolicy(s3cfg, values)
		if iamErr != nil {
			http.Error(w, "Invalid request", http.StatusBadRequest)
			return
		}
	case "PutUserPolicy":
		response, iamErr = e.PutUserPolicy(s3cfg, values)
		if iamErr != nil {
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
		http.Error(w, "Not implemented", http.StatusNotImplemented)
		return
	}

	if changed {
		if err := e.PutS3ApiConfiguration(s3cfg); err != nil {
			http.Error(w, "Internal error", http.StatusInternalServerError)
			return
		}
	}

	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(http.StatusOK)
	xmlBytes, err := xml.Marshal(response)
	if err != nil {
		// This should not happen in tests, but log it for debugging
		http.Error(w, "Internal error: failed to marshal response", http.StatusInternalServerError)
		return
	}
	_, _ = w.Write(xmlBytes)
}

// executeEmbeddedIamRequest executes an IAM request against the given API instance.
// If v is non-nil, the response body is unmarshalled into it.
func executeEmbeddedIamRequest(api *EmbeddedIamApiForTest, req *http.Request, v interface{}) (*httptest.ResponseRecorder, error) {
	rr := httptest.NewRecorder()
	apiRouter := mux.NewRouter().SkipClean(true)
	apiRouter.Path("/").Methods(http.MethodPost).HandlerFunc(api.DoActions)
	apiRouter.ServeHTTP(rr, req)
	if v != nil {
		if err := xml.Unmarshal(rr.Body.Bytes(), v); err != nil {
			return rr, err
		}
	}
	return rr, nil
}

// embeddedIamErrorResponseForTest is used for parsing IAM error responses in tests
type embeddedIamErrorResponseForTest struct {
	Error struct {
		Code    string `xml:"Code"`
		Message string `xml:"Message"`
	} `xml:"Error"`
}

func extractEmbeddedIamErrorCodeAndMessage(response *httptest.ResponseRecorder) (string, string) {
	var er embeddedIamErrorResponseForTest
	if err := xml.Unmarshal(response.Body.Bytes(), &er); err != nil {
		return "", ""
	}
	return er.Error.Code, er.Error.Message
}

// TestEmbeddedIamCreateUser tests creating a user via the embedded IAM API
func TestEmbeddedIamCreateUser(t *testing.T) {
	api := NewEmbeddedIamApiForTest()
	api.mockConfig = &iam_pb.S3ApiConfiguration{}

	userName := aws.String("TestUser")
	params := &iam.CreateUserInput{UserName: userName}
	req, _ := iam.New(session.New()).CreateUserRequest(params)
	_ = req.Build()
	out := iamCreateUserResponse{}
	response, err := executeEmbeddedIamRequest(api, req.HTTPRequest, &out)
	assert.NoError(t, err)
	assert.Equal(t, http.StatusOK, response.Code)

	// Verify response contains correct username
	assert.NotNil(t, out.CreateUserResult.User.UserName)
	assert.Equal(t, "TestUser", *out.CreateUserResult.User.UserName)

	// Verify user was persisted in config
	assert.Len(t, api.mockConfig.Identities, 1)
	assert.Equal(t, "TestUser", api.mockConfig.Identities[0].Name)
}

// TestEmbeddedIamListUsers tests listing users via the embedded IAM API
func TestEmbeddedIamListUsers(t *testing.T) {
	api := NewEmbeddedIamApiForTest()
	api.mockConfig = &iam_pb.S3ApiConfiguration{
		Identities: []*iam_pb.Identity{
			{Name: "User1"},
			{Name: "User2"},
		},
	}

	params := &iam.ListUsersInput{}
	req, _ := iam.New(session.New()).ListUsersRequest(params)
	_ = req.Build()
	out := iamListUsersResponse{}
	response, err := executeEmbeddedIamRequest(api, req.HTTPRequest, &out)
	assert.NoError(t, err)
	assert.Equal(t, http.StatusOK, response.Code)

	// Verify response contains the users
	assert.Len(t, out.ListUsersResult.Users, 2)
}

// TestEmbeddedIamListAccessKeys tests listing access keys via the embedded IAM API
func TestEmbeddedIamListAccessKeys(t *testing.T) {
	api := NewEmbeddedIamApiForTest()
	svc := iam.New(session.New())
	params := &iam.ListAccessKeysInput{}
	req, _ := svc.ListAccessKeysRequest(params)
	_ = req.Build()
	out := iamListAccessKeysResponse{}
	response, err := executeEmbeddedIamRequest(api, req.HTTPRequest, &out)
	assert.NoError(t, err)
	assert.Equal(t, http.StatusOK, response.Code)
}

// TestEmbeddedIamGetUser tests getting a user via the embedded IAM API
func TestEmbeddedIamGetUser(t *testing.T) {
	api := NewEmbeddedIamApiForTest()
	api.mockConfig = &iam_pb.S3ApiConfiguration{
		Identities: []*iam_pb.Identity{
			{Name: "TestUser"},
		},
	}

	userName := aws.String("TestUser")
	params := &iam.GetUserInput{UserName: userName}
	req, _ := iam.New(session.New()).GetUserRequest(params)
	_ = req.Build()
	out := iamGetUserResponse{}
	response, err := executeEmbeddedIamRequest(api, req.HTTPRequest, &out)
	assert.NoError(t, err)
	assert.Equal(t, http.StatusOK, response.Code)

	// Verify response contains correct username
	assert.NotNil(t, out.GetUserResult.User.UserName)
	assert.Equal(t, "TestUser", *out.GetUserResult.User.UserName)
}

// TestEmbeddedIamCreatePolicy tests creating a policy via the embedded IAM API
func TestEmbeddedIamCreatePolicy(t *testing.T) {
	api := NewEmbeddedIamApiForTest()
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
	out := iamCreatePolicyResponse{}
	response, err := executeEmbeddedIamRequest(api, req.HTTPRequest, &out)
	assert.NoError(t, err)
	assert.Equal(t, http.StatusOK, response.Code)

	// Verify response contains policy metadata
	assert.NotNil(t, out.CreatePolicyResult.Policy.PolicyName)
	assert.Equal(t, "S3-read-only-example-bucket", *out.CreatePolicyResult.Policy.PolicyName)
	assert.NotNil(t, out.CreatePolicyResult.Policy.Arn)
	assert.NotNil(t, out.CreatePolicyResult.Policy.PolicyId)
}

// TestEmbeddedIamPutUserPolicy tests attaching a policy to a user
func TestEmbeddedIamPutUserPolicy(t *testing.T) {
	api := NewEmbeddedIamApiForTest()
	api.mockConfig = &iam_pb.S3ApiConfiguration{
		Identities: []*iam_pb.Identity{
			{Name: "TestUser"},
		},
	}

	userName := aws.String("TestUser")
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
	out := iamPutUserPolicyResponse{}
	response, err := executeEmbeddedIamRequest(api, req.HTTPRequest, &out)
	assert.NoError(t, err)
	assert.Equal(t, http.StatusOK, response.Code)

	// Verify policy was attached to the user (actions should be set)
	assert.Len(t, api.mockConfig.Identities, 1)
	assert.NotEmpty(t, api.mockConfig.Identities[0].Actions)
}

// TestEmbeddedIamPutUserPolicyError tests error handling when user doesn't exist
func TestEmbeddedIamPutUserPolicyError(t *testing.T) {
	api := NewEmbeddedIamApiForTest()
	api.mockConfig = &iam_pb.S3ApiConfiguration{}

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
	response, err := executeEmbeddedIamRequest(api, req.HTTPRequest, nil)
	assert.NoError(t, err)
	assert.Equal(t, http.StatusNotFound, response.Code)

	expectedCode := "NoSuchEntity"
	code, _ := extractEmbeddedIamErrorCodeAndMessage(response)
	assert.Equal(t, expectedCode, code)
}

// TestEmbeddedIamGetUserPolicy tests getting a user's policy
func TestEmbeddedIamGetUserPolicy(t *testing.T) {
	api := NewEmbeddedIamApiForTest()
	api.mockConfig = &iam_pb.S3ApiConfiguration{
		Identities: []*iam_pb.Identity{
			{
				Name:    "TestUser",
				Actions: []string{"Read", "List"},
			},
		},
	}

	userName := aws.String("TestUser")
	params := &iam.GetUserPolicyInput{
		UserName:   userName,
		PolicyName: aws.String("S3-read-only-example-bucket"),
	}
	req, _ := iam.New(session.New()).GetUserPolicyRequest(params)
	_ = req.Build()
	out := iamGetUserPolicyResponse{}
	response, err := executeEmbeddedIamRequest(api, req.HTTPRequest, &out)
	assert.NoError(t, err)
	assert.Equal(t, http.StatusOK, response.Code)
}

// TestEmbeddedIamDeleteUserPolicy tests deleting a user's policy (clears actions)
func TestEmbeddedIamDeleteUserPolicy(t *testing.T) {
	api := NewEmbeddedIamApiForTest()
	api.mockConfig = &iam_pb.S3ApiConfiguration{
		Identities: []*iam_pb.Identity{
			{
				Name:    "TestUser",
				Actions: []string{"Read", "Write", "List"},
				Credentials: []*iam_pb.Credential{
					{AccessKey: UserAccessKeyPrefix + "TEST12345", SecretKey: "secret"},
				},
			},
		},
	}

	// Use direct form post for DeleteUserPolicy
	form := url.Values{}
	form.Set("Action", "DeleteUserPolicy")
	form.Set("UserName", "TestUser")
	form.Set("PolicyName", "TestPolicy")

	req, _ := http.NewRequest("POST", "/", nil)
	req.PostForm = form
	req.Form = form
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	rr := httptest.NewRecorder()
	apiRouter := mux.NewRouter().SkipClean(true)
	apiRouter.Path("/").Methods(http.MethodPost).HandlerFunc(api.DoActions)
	apiRouter.ServeHTTP(rr, req)

	assert.Equal(t, http.StatusOK, rr.Code)

	// CRITICAL: Verify user still exists (was NOT deleted)
	assert.Len(t, api.mockConfig.Identities, 1, "User should NOT be deleted")
	assert.Equal(t, "TestUser", api.mockConfig.Identities[0].Name)

	// Verify credentials are still intact
	assert.Len(t, api.mockConfig.Identities[0].Credentials, 1, "Credentials should NOT be deleted")
	assert.Equal(t, UserAccessKeyPrefix+"TEST12345", api.mockConfig.Identities[0].Credentials[0].AccessKey)

	// Verify actions/policy was cleared
	assert.Nil(t, api.mockConfig.Identities[0].Actions, "Actions should be cleared")
}

// TestEmbeddedIamDeleteUserPolicyUserNotFound tests error when user doesn't exist
func TestEmbeddedIamDeleteUserPolicyUserNotFound(t *testing.T) {
	api := NewEmbeddedIamApiForTest()
	api.mockConfig = &iam_pb.S3ApiConfiguration{}

	form := url.Values{}
	form.Set("Action", "DeleteUserPolicy")
	form.Set("UserName", "NonExistentUser")
	form.Set("PolicyName", "TestPolicy")

	req, _ := http.NewRequest("POST", "/", nil)
	req.PostForm = form
	req.Form = form
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	rr := httptest.NewRecorder()
	apiRouter := mux.NewRouter().SkipClean(true)
	apiRouter.Path("/").Methods(http.MethodPost).HandlerFunc(api.DoActions)
	apiRouter.ServeHTTP(rr, req)

	assert.Equal(t, http.StatusNotFound, rr.Code)
}

// TestEmbeddedIamUpdateUser tests updating a user
func TestEmbeddedIamUpdateUser(t *testing.T) {
	api := NewEmbeddedIamApiForTest()
	api.mockConfig = &iam_pb.S3ApiConfiguration{
		Identities: []*iam_pb.Identity{
			{Name: "TestUser"},
		},
	}

	userName := aws.String("TestUser")
	newUserName := aws.String("TestUser-New")
	params := &iam.UpdateUserInput{NewUserName: newUserName, UserName: userName}
	req, _ := iam.New(session.New()).UpdateUserRequest(params)
	_ = req.Build()
	out := iamUpdateUserResponse{}
	response, err := executeEmbeddedIamRequest(api, req.HTTPRequest, &out)
	assert.NoError(t, err)
	assert.Equal(t, http.StatusOK, response.Code)
}

// TestEmbeddedIamDeleteUser tests deleting a user
func TestEmbeddedIamDeleteUser(t *testing.T) {
	api := NewEmbeddedIamApiForTest()
	api.mockConfig = &iam_pb.S3ApiConfiguration{
		Identities: []*iam_pb.Identity{
			{Name: "TestUser-New"},
		},
	}

	userName := aws.String("TestUser-New")
	params := &iam.DeleteUserInput{UserName: userName}
	req, _ := iam.New(session.New()).DeleteUserRequest(params)
	_ = req.Build()
	out := iamDeleteUserResponse{}
	response, err := executeEmbeddedIamRequest(api, req.HTTPRequest, &out)
	assert.NoError(t, err)
	assert.Equal(t, http.StatusOK, response.Code)
}

// TestEmbeddedIamCreateAccessKey tests creating an access key
func TestEmbeddedIamCreateAccessKey(t *testing.T) {
	api := NewEmbeddedIamApiForTest()
	api.mockConfig = &iam_pb.S3ApiConfiguration{
		Identities: []*iam_pb.Identity{
			{Name: "TestUser"},
		},
	}

	userName := aws.String("TestUser")
	params := &iam.CreateAccessKeyInput{UserName: userName}
	req, _ := iam.New(session.New()).CreateAccessKeyRequest(params)
	_ = req.Build()
	out := iamCreateAccessKeyResponse{}
	response, err := executeEmbeddedIamRequest(api, req.HTTPRequest, &out)
	assert.NoError(t, err)
	assert.Equal(t, http.StatusOK, response.Code)

	// Verify response contains access key credentials
	assert.NotNil(t, out.CreateAccessKeyResult.AccessKey.AccessKeyId)
	assert.NotEmpty(t, *out.CreateAccessKeyResult.AccessKey.AccessKeyId)
	assert.NotNil(t, out.CreateAccessKeyResult.AccessKey.SecretAccessKey)
	assert.NotEmpty(t, *out.CreateAccessKeyResult.AccessKey.SecretAccessKey)
	assert.NotNil(t, out.CreateAccessKeyResult.AccessKey.UserName)
	assert.Equal(t, "TestUser", *out.CreateAccessKeyResult.AccessKey.UserName)

	// Verify credentials were persisted
	assert.Len(t, api.mockConfig.Identities[0].Credentials, 1)
}

// TestEmbeddedIamDeleteAccessKey tests deleting an access key via direct form post
func TestEmbeddedIamDeleteAccessKey(t *testing.T) {
	api := NewEmbeddedIamApiForTest()
	api.mockConfig = &iam_pb.S3ApiConfiguration{
		Identities: []*iam_pb.Identity{
			{
				Name: "TestUser",
				Credentials: []*iam_pb.Credential{
					{AccessKey: UserAccessKeyPrefix + "TEST12345", SecretKey: "secret"},
				},
			},
		},
	}

	// Use direct form post since AWS SDK may format differently
	form := url.Values{}
	form.Set("Action", "DeleteAccessKey")
	form.Set("UserName", "TestUser")
	form.Set("AccessKeyId", UserAccessKeyPrefix+"TEST12345")

	req, _ := http.NewRequest("POST", "/", nil)
	req.PostForm = form
	req.Form = form
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	rr := httptest.NewRecorder()
	apiRouter := mux.NewRouter().SkipClean(true)
	apiRouter.Path("/").Methods(http.MethodPost).HandlerFunc(api.DoActions)
	apiRouter.ServeHTTP(rr, req)

	assert.Equal(t, http.StatusOK, rr.Code)

	// Verify the access key was deleted
	assert.Len(t, api.mockConfig.Identities[0].Credentials, 0)
}

// TestEmbeddedIamHandleImplicitUsername tests implicit username extraction from authorization header
func TestEmbeddedIamHandleImplicitUsername(t *testing.T) {
	// Create IAM with test credentials - the handleImplicitUsername function now looks
	// up the username from the credential store based on AccessKeyId
	// Note: Using obviously fake access keys to avoid secret scanner false positives
	iam := &IdentityAccessManagement{}
	testConfig := &iam_pb.S3ApiConfiguration{
		Identities: []*iam_pb.Identity{
			{
				Name: "testuser1",
				Credentials: []*iam_pb.Credential{
					{AccessKey: UserAccessKeyPrefix + "TESTFAKEKEY000001", SecretKey: "testsecretfake"},
				},
			},
		},
	}
	err := iam.LoadS3ApiConfigurationFromBytes(mustMarshalJSON(testConfig))
	if err != nil {
		t.Fatalf("Failed to load test config: %v", err)
	}

	embeddedApi := &EmbeddedIamApi{
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
		{&http.Request{Header: http.Header{"Authorization": []string{"AWS4-HMAC-SHA256 Credential=" + UserAccessKeyPrefix + "TESTFAKEKEY000001/20220420/us-east-1/iam/aws4_request, SignedHeaders=content-type;host;x-amz-date, Signature=fakesignature0123456789abcdef"}}}, url.Values{}, "testuser1"},
		// Malformed auth header (no Credential=) - should not set username
		{&http.Request{Header: http.Header{"Authorization": []string{"AWS4-HMAC-SHA256 =" + UserAccessKeyPrefix + "TESTFAKEKEY000001/20220420/test1/iam/aws4_request, SignedHeaders=content-type;host;x-amz-date, Signature=fakesignature0123456789abcdef"}}}, url.Values{}, ""},
		// Unknown access key - should not set username
		{&http.Request{Header: http.Header{"Authorization": []string{"AWS4-HMAC-SHA256 Credential=" + UserAccessKeyPrefix + "TESTUNKNOWN000000/20220420/us-east-1/iam/aws4_request, SignedHeaders=content-type;host;x-amz-date, Signature=fakesignature0123456789abcdef"}}}, url.Values{}, ""},
	}

	for i, test := range tests {
		embeddedApi.handleImplicitUsername(test.r, test.values)
		if un := test.values.Get("UserName"); un != test.userName {
			t.Errorf("No.%d: Got: %v, Expected: %v", i, un, test.userName)
		}
	}
}

func mustMarshalJSON(v interface{}) []byte {
	data, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return data
}

// TestEmbeddedIamFullWorkflow tests a complete user lifecycle
func TestEmbeddedIamFullWorkflow(t *testing.T) {
	api := NewEmbeddedIamApiForTest()
	api.mockConfig = &iam_pb.S3ApiConfiguration{}

	// 1. Create user
	t.Run("CreateUser", func(t *testing.T) {
		userName := aws.String("WorkflowUser")
		params := &iam.CreateUserInput{UserName: userName}
		req, _ := iam.New(session.New()).CreateUserRequest(params)
		_ = req.Build()
		response, err := executeEmbeddedIamRequest(api, req.HTTPRequest, nil)
		assert.NoError(t, err)
		assert.Equal(t, http.StatusOK, response.Code)
	})

	// 2. Create access key for user
	t.Run("CreateAccessKey", func(t *testing.T) {
		userName := aws.String("WorkflowUser")
		params := &iam.CreateAccessKeyInput{UserName: userName}
		req, _ := iam.New(session.New()).CreateAccessKeyRequest(params)
		_ = req.Build()
		response, err := executeEmbeddedIamRequest(api, req.HTTPRequest, nil)
		assert.NoError(t, err)
		assert.Equal(t, http.StatusOK, response.Code)
	})

	// 3. Attach policy to user
	t.Run("PutUserPolicy", func(t *testing.T) {
		params := &iam.PutUserPolicyInput{
			UserName:   aws.String("WorkflowUser"),
			PolicyName: aws.String("ReadWritePolicy"),
			PolicyDocument: aws.String(`{
				"Version": "2012-10-17",
				"Statement": [{
					"Effect": "Allow",
					"Action": ["s3:Get*", "s3:Put*"],
					"Resource": ["arn:aws:s3:::*"]
				}]
			}`),
		}
		req, _ := iam.New(session.New()).PutUserPolicyRequest(params)
		_ = req.Build()
		response, err := executeEmbeddedIamRequest(api, req.HTTPRequest, nil)
		assert.NoError(t, err)
		assert.Equal(t, http.StatusOK, response.Code)
	})

	// 4. List users to verify
	t.Run("ListUsers", func(t *testing.T) {
		params := &iam.ListUsersInput{}
		req, _ := iam.New(session.New()).ListUsersRequest(params)
		_ = req.Build()
		response, err := executeEmbeddedIamRequest(api, req.HTTPRequest, nil)
		assert.NoError(t, err)
		assert.Equal(t, http.StatusOK, response.Code)
	})

	// 5. Delete user
	t.Run("DeleteUser", func(t *testing.T) {
		params := &iam.DeleteUserInput{UserName: aws.String("WorkflowUser")}
		req, _ := iam.New(session.New()).DeleteUserRequest(params)
		_ = req.Build()
		response, err := executeEmbeddedIamRequest(api, req.HTTPRequest, nil)
		assert.NoError(t, err)
		assert.Equal(t, http.StatusOK, response.Code)
	})
}

// TestIamStringSlicesEqual tests the iamStringSlicesEqual helper function
func TestIamStringSlicesEqual(t *testing.T) {
	tests := []struct {
		name     string
		a        []string
		b        []string
		expected bool
	}{
		{"both empty", []string{}, []string{}, true},
		{"both nil", nil, nil, true},
		{"same elements same order", []string{"a", "b", "c"}, []string{"a", "b", "c"}, true},
		{"same elements different order", []string{"c", "a", "b"}, []string{"a", "b", "c"}, true},
		{"different lengths", []string{"a", "b"}, []string{"a", "b", "c"}, false},
		{"different elements", []string{"a", "b", "c"}, []string{"a", "b", "d"}, false},
		{"one empty one not", []string{}, []string{"a"}, false},
		{"duplicates same", []string{"a", "a", "b"}, []string{"a", "b", "a"}, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := iamStringSlicesEqual(tt.a, tt.b)
			assert.Equal(t, tt.expected, result)
		})
	}
}

// TestIamHash tests the iamHash function
func TestIamHash(t *testing.T) {
	input := "test-policy-document"
	hash := iamHash(&input)

	// Hash should be non-empty
	assert.NotEmpty(t, hash)

	// Same input should produce same hash
	hash2 := iamHash(&input)
	assert.Equal(t, hash, hash2)

	// Different input should produce different hash
	input2 := "different-policy"
	hash3 := iamHash(&input2)
	assert.NotEqual(t, hash, hash3)
}

// TestIamStringWithCharset tests the cryptographically secure random string generator
func TestIamStringWithCharset(t *testing.T) {
	charset := "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	length := 20

	str, err := iamStringWithCharset(length, charset)
	assert.NoError(t, err)
	assert.Len(t, str, length)

	// All characters should be from the charset
	for _, c := range str {
		assert.Contains(t, charset, string(c))
	}

	// Two calls should produce different strings (with very high probability)
	str2, err := iamStringWithCharset(length, charset)
	assert.NoError(t, err)
	assert.NotEqual(t, str, str2)
}

// TestIamMapToStatementAction tests action mapping
func TestIamMapToStatementAction(t *testing.T) {
	// iamMapToStatementAction maps IAM statement action patterns to internal action names
	tests := []struct {
		input    string
		expected string
	}{
		{"*", "Admin"},
		{"Get*", "Read"},
		{"Put*", "Write"},
		{"List*", "List"},
		{"Tagging*", "Tagging"},
		{"DeleteBucket*", "DeleteBucket"},
		{"PutBucketAcl", "WriteAcp"},
		{"GetBucketAcl", "ReadAcp"},
		{"InvalidAction", ""},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			result := iamMapToStatementAction(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}

// TestIamMapToIdentitiesAction tests reverse action mapping
func TestIamMapToIdentitiesAction(t *testing.T) {
	// iamMapToIdentitiesAction maps internal action names to IAM statement action patterns
	tests := []struct {
		input    string
		expected string
	}{
		{"Admin", "*"},
		{"Read", "Get*"},
		{"Write", "Put*"},
		{"List", "List*"},
		{"Tagging", "Tagging*"},
		{"Unknown", ""},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			result := iamMapToIdentitiesAction(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}

// TestEmbeddedIamGetUserNotFound tests GetUser with non-existent user
func TestEmbeddedIamGetUserNotFound(t *testing.T) {
	api := NewEmbeddedIamApiForTest()
	api.mockConfig = &iam_pb.S3ApiConfiguration{
		Identities: []*iam_pb.Identity{
			{Name: "ExistingUser"},
		},
	}

	userName := aws.String("NonExistentUser")
	params := &iam.GetUserInput{UserName: userName}
	req, _ := iam.New(session.New()).GetUserRequest(params)
	_ = req.Build()
	response, _ := executeEmbeddedIamRequest(api, req.HTTPRequest, nil)
	assert.Equal(t, http.StatusNotFound, response.Code)
}

// TestEmbeddedIamDeleteUserNotFound tests DeleteUser with non-existent user
func TestEmbeddedIamDeleteUserNotFound(t *testing.T) {
	api := NewEmbeddedIamApiForTest()
	api.mockConfig = &iam_pb.S3ApiConfiguration{}

	userName := aws.String("NonExistentUser")
	params := &iam.DeleteUserInput{UserName: userName}
	req, _ := iam.New(session.New()).DeleteUserRequest(params)
	_ = req.Build()
	response, _ := executeEmbeddedIamRequest(api, req.HTTPRequest, nil)
	assert.Equal(t, http.StatusNotFound, response.Code)
}

// TestEmbeddedIamUpdateUserNotFound tests UpdateUser with non-existent user
func TestEmbeddedIamUpdateUserNotFound(t *testing.T) {
	api := NewEmbeddedIamApiForTest()
	api.mockConfig = &iam_pb.S3ApiConfiguration{}

	params := &iam.UpdateUserInput{
		UserName:    aws.String("NonExistentUser"),
		NewUserName: aws.String("NewName"),
	}
	req, _ := iam.New(session.New()).UpdateUserRequest(params)
	_ = req.Build()
	response, _ := executeEmbeddedIamRequest(api, req.HTTPRequest, nil)
	assert.Equal(t, http.StatusBadRequest, response.Code)
}

// TestEmbeddedIamCreateAccessKeyForExistingUser tests CreateAccessKey creates credentials for existing user
func TestEmbeddedIamCreateAccessKeyForExistingUser(t *testing.T) {
	api := NewEmbeddedIamApiForTest()
	api.mockConfig = &iam_pb.S3ApiConfiguration{
		Identities: []*iam_pb.Identity{
			{Name: "ExistingUser"},
		},
	}

	// Use direct form post
	form := url.Values{}
	form.Set("Action", "CreateAccessKey")
	form.Set("UserName", "ExistingUser")

	req, _ := http.NewRequest("POST", "/", nil)
	req.PostForm = form
	req.Form = form
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	rr := httptest.NewRecorder()
	apiRouter := mux.NewRouter().SkipClean(true)
	apiRouter.Path("/").Methods(http.MethodPost).HandlerFunc(api.DoActions)
	apiRouter.ServeHTTP(rr, req)

	assert.Equal(t, http.StatusOK, rr.Code)
	// Verify credentials were created
	assert.Len(t, api.mockConfig.Identities[0].Credentials, 1)
}

// TestEmbeddedIamGetUserPolicyUserNotFound tests GetUserPolicy with non-existent user
func TestEmbeddedIamGetUserPolicyUserNotFound(t *testing.T) {
	api := NewEmbeddedIamApiForTest()
	api.mockConfig = &iam_pb.S3ApiConfiguration{}

	params := &iam.GetUserPolicyInput{
		UserName:   aws.String("NonExistentUser"),
		PolicyName: aws.String("TestPolicy"),
	}
	req, _ := iam.New(session.New()).GetUserPolicyRequest(params)
	_ = req.Build()
	response, _ := executeEmbeddedIamRequest(api, req.HTTPRequest, nil)
	assert.Equal(t, http.StatusNotFound, response.Code)
}

// TestEmbeddedIamCreatePolicyMalformed tests CreatePolicy with invalid policy document
func TestEmbeddedIamCreatePolicyMalformed(t *testing.T) {
	api := NewEmbeddedIamApiForTest()

	params := &iam.CreatePolicyInput{
		PolicyName:     aws.String("TestPolicy"),
		PolicyDocument: aws.String("invalid json"),
	}
	req, _ := iam.New(session.New()).CreatePolicyRequest(params)
	_ = req.Build()
	response, _ := executeEmbeddedIamRequest(api, req.HTTPRequest, nil)
	assert.Equal(t, http.StatusBadRequest, response.Code)
}

// TestEmbeddedIamListAccessKeysForUser tests listing access keys for a specific user
func TestEmbeddedIamListAccessKeysForUser(t *testing.T) {
	api := NewEmbeddedIamApiForTest()
	api.mockConfig = &iam_pb.S3ApiConfiguration{
		Identities: []*iam_pb.Identity{
			{
				Name: "TestUser",
				Credentials: []*iam_pb.Credential{
					{AccessKey: UserAccessKeyPrefix + "TEST1", SecretKey: "secret1"},
					{AccessKey: UserAccessKeyPrefix + "TEST2", SecretKey: "secret2"},
				},
			},
		},
	}

	params := &iam.ListAccessKeysInput{UserName: aws.String("TestUser")}
	req, _ := iam.New(session.New()).ListAccessKeysRequest(params)
	_ = req.Build()
	out := iamListAccessKeysResponse{}
	response, err := executeEmbeddedIamRequest(api, req.HTTPRequest, &out)
	assert.NoError(t, err)
	assert.Equal(t, http.StatusOK, response.Code)

	// Verify both access keys are listed
	assert.Len(t, out.ListAccessKeysResult.AccessKeyMetadata, 2)
}

// TestEmbeddedIamNotImplementedAction tests handling of unimplemented actions
func TestEmbeddedIamNotImplementedAction(t *testing.T) {
	api := NewEmbeddedIamApiForTest()

	form := url.Values{}
	form.Set("Action", "SomeUnknownAction")

	req, _ := http.NewRequest("POST", "/", nil)
	req.PostForm = form
	req.Form = form
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	rr := httptest.NewRecorder()
	apiRouter := mux.NewRouter().SkipClean(true)
	apiRouter.Path("/").Methods(http.MethodPost).HandlerFunc(api.DoActions)
	apiRouter.ServeHTTP(rr, req)

	assert.Equal(t, http.StatusNotImplemented, rr.Code)
}

// TestGetPolicyDocument tests parsing of policy documents
func TestGetPolicyDocument(t *testing.T) {
	api := NewEmbeddedIamApiForTest()

	validPolicy := `{
		"Version": "2012-10-17",
		"Statement": [{
			"Effect": "Allow",
			"Action": ["s3:GetObject"],
			"Resource": ["arn:aws:s3:::bucket/*"]
		}]
	}`

	doc, err := api.GetPolicyDocument(&validPolicy)
	assert.NoError(t, err)
	assert.Equal(t, "2012-10-17", doc.Version)
	assert.Len(t, doc.Statement, 1)

	// Test invalid JSON
	invalidPolicy := "not valid json"
	_, err = api.GetPolicyDocument(&invalidPolicy)
	assert.Error(t, err)
}

// TestEmbeddedIamGetActionsFromPolicy tests action extraction from policy documents
func TestEmbeddedIamGetActionsFromPolicy(t *testing.T) {
	api := NewEmbeddedIamApiForTest()

	// Actions must use wildcards (Get*, Put*, List*, etc.) as expected by the mapper
	policyDoc := `{
		"Version": "2012-10-17",
		"Statement": [{
			"Effect": "Allow",
			"Action": ["s3:Get*", "s3:Put*"],
			"Resource": ["arn:aws:s3:::mybucket/*"]
		}]
	}`

	policy, err := api.GetPolicyDocument(&policyDoc)
	assert.NoError(t, err)

	actions, err := api.getActions(&policy)
	assert.NoError(t, err)
	assert.NotEmpty(t, actions)
	// Should have Read and Write actions for the bucket
	// arn:aws:s3:::mybucket/* means all objects in mybucket, represented as "Action:mybucket"
	assert.Contains(t, actions, "Read:mybucket")
	assert.Contains(t, actions, "Write:mybucket")
}

// TestEmbeddedIamSetUserStatus tests enabling/disabling a user
func TestEmbeddedIamSetUserStatus(t *testing.T) {
	api := NewEmbeddedIamApiForTest()

	t.Run("DisableUser", func(t *testing.T) {
		// Reset state for test isolation
		api.mockConfig = &iam_pb.S3ApiConfiguration{
			Identities: []*iam_pb.Identity{
				{Name: "TestUser", Disabled: false},
			},
		}

		form := url.Values{}
		form.Set("Action", "SetUserStatus")
		form.Set("UserName", "TestUser")
		form.Set("Status", "Inactive")

		req, _ := http.NewRequest("POST", "/", nil)
		req.PostForm = form
		req.Form = form
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

		rr := httptest.NewRecorder()
		apiRouter := mux.NewRouter().SkipClean(true)
		apiRouter.Path("/").Methods(http.MethodPost).HandlerFunc(api.DoActions)
		apiRouter.ServeHTTP(rr, req)

		assert.Equal(t, http.StatusOK, rr.Code)
		// Verify user is now disabled
		assert.True(t, api.mockConfig.Identities[0].Disabled)
	})

	t.Run("EnableUser", func(t *testing.T) {
		// Reset state for test isolation - start with disabled user
		api.mockConfig = &iam_pb.S3ApiConfiguration{
			Identities: []*iam_pb.Identity{
				{Name: "TestUser", Disabled: true},
			},
		}

		form := url.Values{}
		form.Set("Action", "SetUserStatus")
		form.Set("UserName", "TestUser")
		form.Set("Status", "Active")

		req, _ := http.NewRequest("POST", "/", nil)
		req.PostForm = form
		req.Form = form
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

		rr := httptest.NewRecorder()
		apiRouter := mux.NewRouter().SkipClean(true)
		apiRouter.Path("/").Methods(http.MethodPost).HandlerFunc(api.DoActions)
		apiRouter.ServeHTTP(rr, req)

		assert.Equal(t, http.StatusOK, rr.Code)
		// Verify user is now enabled
		assert.False(t, api.mockConfig.Identities[0].Disabled)
	})
}

// TestEmbeddedIamSetUserStatusErrors tests error handling for SetUserStatus
func TestEmbeddedIamSetUserStatusErrors(t *testing.T) {
	api := NewEmbeddedIamApiForTest()
	api.mockConfig = &iam_pb.S3ApiConfiguration{
		Identities: []*iam_pb.Identity{
			{Name: "TestUser"},
		},
	}

	t.Run("UserNotFound", func(t *testing.T) {
		form := url.Values{}
		form.Set("Action", "SetUserStatus")
		form.Set("UserName", "NonExistentUser")
		form.Set("Status", "Inactive")

		req, _ := http.NewRequest("POST", "/", nil)
		req.PostForm = form
		req.Form = form
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

		rr := httptest.NewRecorder()
		apiRouter := mux.NewRouter().SkipClean(true)
		apiRouter.Path("/").Methods(http.MethodPost).HandlerFunc(api.DoActions)
		apiRouter.ServeHTTP(rr, req)

		assert.Equal(t, http.StatusNotFound, rr.Code)
	})

	t.Run("InvalidStatus", func(t *testing.T) {
		form := url.Values{}
		form.Set("Action", "SetUserStatus")
		form.Set("UserName", "TestUser")
		form.Set("Status", "InvalidStatus")

		req, _ := http.NewRequest("POST", "/", nil)
		req.PostForm = form
		req.Form = form
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

		rr := httptest.NewRecorder()
		apiRouter := mux.NewRouter().SkipClean(true)
		apiRouter.Path("/").Methods(http.MethodPost).HandlerFunc(api.DoActions)
		apiRouter.ServeHTTP(rr, req)

		assert.Equal(t, http.StatusBadRequest, rr.Code)
	})

	t.Run("MissingUserName", func(t *testing.T) {
		form := url.Values{}
		form.Set("Action", "SetUserStatus")
		form.Set("Status", "Inactive")

		req, _ := http.NewRequest("POST", "/", nil)
		req.PostForm = form
		req.Form = form
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

		rr := httptest.NewRecorder()
		apiRouter := mux.NewRouter().SkipClean(true)
		apiRouter.Path("/").Methods(http.MethodPost).HandlerFunc(api.DoActions)
		apiRouter.ServeHTTP(rr, req)

		assert.Equal(t, http.StatusBadRequest, rr.Code)
	})

	t.Run("MissingStatus", func(t *testing.T) {
		form := url.Values{}
		form.Set("Action", "SetUserStatus")
		form.Set("UserName", "TestUser")

		req, _ := http.NewRequest("POST", "/", nil)
		req.PostForm = form
		req.Form = form
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

		rr := httptest.NewRecorder()
		apiRouter := mux.NewRouter().SkipClean(true)
		apiRouter.Path("/").Methods(http.MethodPost).HandlerFunc(api.DoActions)
		apiRouter.ServeHTTP(rr, req)

		assert.Equal(t, http.StatusBadRequest, rr.Code)
	})
}

// TestEmbeddedIamUpdateAccessKey tests updating access key status
func TestEmbeddedIamUpdateAccessKey(t *testing.T) {
	api := NewEmbeddedIamApiForTest()

	t.Run("DeactivateAccessKey", func(t *testing.T) {
		// Reset state for test isolation
		api.mockConfig = &iam_pb.S3ApiConfiguration{
			Identities: []*iam_pb.Identity{
				{
					Name: "TestUser",
					Credentials: []*iam_pb.Credential{
						{AccessKey: UserAccessKeyPrefix + "TEST12345", SecretKey: "secret", Status: "Active"},
					},
				},
			},
		}

		form := url.Values{}
		form.Set("Action", "UpdateAccessKey")
		form.Set("UserName", "TestUser")
		form.Set("AccessKeyId", UserAccessKeyPrefix+"TEST12345")
		form.Set("Status", "Inactive")

		req, _ := http.NewRequest("POST", "/", nil)
		req.PostForm = form
		req.Form = form
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

		rr := httptest.NewRecorder()
		apiRouter := mux.NewRouter().SkipClean(true)
		apiRouter.Path("/").Methods(http.MethodPost).HandlerFunc(api.DoActions)
		apiRouter.ServeHTTP(rr, req)

		assert.Equal(t, http.StatusOK, rr.Code)
		// Verify access key is now inactive
		assert.Equal(t, "Inactive", api.mockConfig.Identities[0].Credentials[0].Status)
	})

	t.Run("ActivateAccessKey", func(t *testing.T) {
		// Reset state for test isolation - start with inactive key
		api.mockConfig = &iam_pb.S3ApiConfiguration{
			Identities: []*iam_pb.Identity{
				{
					Name: "TestUser",
					Credentials: []*iam_pb.Credential{
						{AccessKey: UserAccessKeyPrefix + "TEST12345", SecretKey: "secret", Status: "Inactive"},
					},
				},
			},
		}

		form := url.Values{}
		form.Set("Action", "UpdateAccessKey")
		form.Set("UserName", "TestUser")
		form.Set("AccessKeyId", UserAccessKeyPrefix+"TEST12345")
		form.Set("Status", "Active")

		req, _ := http.NewRequest("POST", "/", nil)
		req.PostForm = form
		req.Form = form
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

		rr := httptest.NewRecorder()
		apiRouter := mux.NewRouter().SkipClean(true)
		apiRouter.Path("/").Methods(http.MethodPost).HandlerFunc(api.DoActions)
		apiRouter.ServeHTTP(rr, req)

		assert.Equal(t, http.StatusOK, rr.Code)
		// Verify access key is now active
		assert.Equal(t, "Active", api.mockConfig.Identities[0].Credentials[0].Status)
	})
}

// TestEmbeddedIamUpdateAccessKeyErrors tests error handling for UpdateAccessKey
func TestEmbeddedIamUpdateAccessKeyErrors(t *testing.T) {
	api := NewEmbeddedIamApiForTest()
	api.mockConfig = &iam_pb.S3ApiConfiguration{
		Identities: []*iam_pb.Identity{
			{
				Name: "TestUser",
				Credentials: []*iam_pb.Credential{
					{AccessKey: UserAccessKeyPrefix + "TEST12345", SecretKey: "secret"},
				},
			},
		},
	}

	t.Run("AccessKeyNotFound", func(t *testing.T) {
		form := url.Values{}
		form.Set("Action", "UpdateAccessKey")
		form.Set("UserName", "TestUser")
		form.Set("AccessKeyId", "NONEXISTENT123")
		form.Set("Status", "Inactive")

		req, _ := http.NewRequest("POST", "/", nil)
		req.PostForm = form
		req.Form = form
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

		rr := httptest.NewRecorder()
		apiRouter := mux.NewRouter().SkipClean(true)
		apiRouter.Path("/").Methods(http.MethodPost).HandlerFunc(api.DoActions)
		apiRouter.ServeHTTP(rr, req)

		assert.Equal(t, http.StatusNotFound, rr.Code)
	})

	t.Run("InvalidStatus", func(t *testing.T) {
		form := url.Values{}
		form.Set("Action", "UpdateAccessKey")
		form.Set("UserName", "TestUser")
		form.Set("AccessKeyId", UserAccessKeyPrefix+"TEST12345")
		form.Set("Status", "InvalidStatus")

		req, _ := http.NewRequest("POST", "/", nil)
		req.PostForm = form
		req.Form = form
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

		rr := httptest.NewRecorder()
		apiRouter := mux.NewRouter().SkipClean(true)
		apiRouter.Path("/").Methods(http.MethodPost).HandlerFunc(api.DoActions)
		apiRouter.ServeHTTP(rr, req)

		assert.Equal(t, http.StatusBadRequest, rr.Code)
	})

	t.Run("MissingUserName", func(t *testing.T) {
		form := url.Values{}
		form.Set("Action", "UpdateAccessKey")
		form.Set("AccessKeyId", UserAccessKeyPrefix+"TEST12345")
		form.Set("Status", "Inactive")

		req, _ := http.NewRequest("POST", "/", nil)
		req.PostForm = form
		req.Form = form
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

		rr := httptest.NewRecorder()
		apiRouter := mux.NewRouter().SkipClean(true)
		apiRouter.Path("/").Methods(http.MethodPost).HandlerFunc(api.DoActions)
		apiRouter.ServeHTTP(rr, req)

		assert.Equal(t, http.StatusBadRequest, rr.Code)
	})

	t.Run("MissingAccessKeyId", func(t *testing.T) {
		form := url.Values{}
		form.Set("Action", "UpdateAccessKey")
		form.Set("UserName", "TestUser")
		form.Set("Status", "Inactive")

		req, _ := http.NewRequest("POST", "/", nil)
		req.PostForm = form
		req.Form = form
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

		rr := httptest.NewRecorder()
		apiRouter := mux.NewRouter().SkipClean(true)
		apiRouter.Path("/").Methods(http.MethodPost).HandlerFunc(api.DoActions)
		apiRouter.ServeHTTP(rr, req)

		assert.Equal(t, http.StatusBadRequest, rr.Code)
	})

	t.Run("UserNotFound", func(t *testing.T) {
		form := url.Values{}
		form.Set("Action", "UpdateAccessKey")
		form.Set("UserName", "NonExistentUser")
		form.Set("AccessKeyId", UserAccessKeyPrefix+"TEST12345")
		form.Set("Status", "Inactive")

		req, _ := http.NewRequest("POST", "/", nil)
		req.PostForm = form
		req.Form = form
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

		rr := httptest.NewRecorder()
		apiRouter := mux.NewRouter().SkipClean(true)
		apiRouter.Path("/").Methods(http.MethodPost).HandlerFunc(api.DoActions)
		apiRouter.ServeHTTP(rr, req)

		assert.Equal(t, http.StatusNotFound, rr.Code)
	})

	t.Run("MissingStatus", func(t *testing.T) {
		form := url.Values{}
		form.Set("Action", "UpdateAccessKey")
		form.Set("UserName", "TestUser")
		form.Set("AccessKeyId", UserAccessKeyPrefix+"TEST12345")

		req, _ := http.NewRequest("POST", "/", nil)
		req.PostForm = form
		req.Form = form
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

		rr := httptest.NewRecorder()
		apiRouter := mux.NewRouter().SkipClean(true)
		apiRouter.Path("/").Methods(http.MethodPost).HandlerFunc(api.DoActions)
		apiRouter.ServeHTTP(rr, req)

		assert.Equal(t, http.StatusBadRequest, rr.Code)
	})
}

// TestEmbeddedIamListAccessKeysShowsStatus tests that ListAccessKeys returns the access key status
func TestEmbeddedIamListAccessKeysShowsStatus(t *testing.T) {
	api := NewEmbeddedIamApiForTest()
	api.mockConfig = &iam_pb.S3ApiConfiguration{
		Identities: []*iam_pb.Identity{
			{
				Name: "TestUser",
				Credentials: []*iam_pb.Credential{
					{AccessKey: UserAccessKeyPrefix + "ACTIVE123", SecretKey: "secret1", Status: "Active"},
					{AccessKey: UserAccessKeyPrefix + "INACTIVE1", SecretKey: "secret2", Status: "Inactive"},
					{AccessKey: UserAccessKeyPrefix + "DEFAULT12", SecretKey: "secret3"}, // No status set, should default to Active
				},
			},
		},
	}

	params := &iam.ListAccessKeysInput{UserName: aws.String("TestUser")}
	req, _ := iam.New(session.New()).ListAccessKeysRequest(params)
	_ = req.Build()
	out := iamListAccessKeysResponse{}
	response, err := executeEmbeddedIamRequest(api, req.HTTPRequest, &out)
	assert.NoError(t, err)
	assert.Equal(t, http.StatusOK, response.Code)

	// Verify all three access keys are listed with correct status
	assert.Len(t, out.ListAccessKeysResult.AccessKeyMetadata, 3)

	// Find each key and verify status
	statusMap := make(map[string]string)
	for _, meta := range out.ListAccessKeysResult.AccessKeyMetadata {
		statusMap[*meta.AccessKeyId] = *meta.Status
	}

	assert.Equal(t, "Active", statusMap[UserAccessKeyPrefix+"ACTIVE123"])
	assert.Equal(t, "Inactive", statusMap[UserAccessKeyPrefix+"INACTIVE1"])
	assert.Equal(t, "Active", statusMap[UserAccessKeyPrefix+"DEFAULT12"]) // Default to Active
}

// TestDisabledUserLookupFails tests that disabled users cannot authenticate
func TestDisabledUserLookupFails(t *testing.T) {
	iam := &IdentityAccessManagement{}
	testConfig := &iam_pb.S3ApiConfiguration{
		Identities: []*iam_pb.Identity{
			{
				Name:     "enabledUser",
				Disabled: false,
				Credentials: []*iam_pb.Credential{
					{AccessKey: UserAccessKeyPrefix + "ENABLED123", SecretKey: "secret1"},
				},
			},
			{
				Name:     "disabledUser",
				Disabled: true,
				Credentials: []*iam_pb.Credential{
					{AccessKey: UserAccessKeyPrefix + "DISABLED12", SecretKey: "secret2"},
				},
			},
		},
	}
	err := iam.LoadS3ApiConfigurationFromBytes(mustMarshalJSON(testConfig))
	assert.NoError(t, err)

	// Enabled user should be found
	identity, cred, found := iam.LookupByAccessKey(UserAccessKeyPrefix + "ENABLED123")
	assert.True(t, found)
	assert.NotNil(t, identity)
	assert.NotNil(t, cred)
	assert.Equal(t, "enabledUser", identity.Name)

	// Disabled user should NOT be found
	identity, cred, found = iam.LookupByAccessKey(UserAccessKeyPrefix + "DISABLED12")
	assert.False(t, found)
	assert.Nil(t, identity)
	assert.Nil(t, cred)
}

// TestInactiveAccessKeyLookupFails tests that inactive access keys cannot authenticate
func TestInactiveAccessKeyLookupFails(t *testing.T) {
	iam := &IdentityAccessManagement{}
	testConfig := &iam_pb.S3ApiConfiguration{
		Identities: []*iam_pb.Identity{
			{
				Name: "testUser",
				Credentials: []*iam_pb.Credential{
					{AccessKey: UserAccessKeyPrefix + "ACTIVE123", SecretKey: "secret1", Status: "Active"},
					{AccessKey: UserAccessKeyPrefix + "INACTIVE1", SecretKey: "secret2", Status: "Inactive"},
					{AccessKey: UserAccessKeyPrefix + "DEFAULT12", SecretKey: "secret3"}, // No status = Active
				},
			},
		},
	}
	err := iam.LoadS3ApiConfigurationFromBytes(mustMarshalJSON(testConfig))
	assert.NoError(t, err)

	// Active key should be found
	identity, cred, found := iam.LookupByAccessKey(UserAccessKeyPrefix + "ACTIVE123")
	assert.True(t, found)
	assert.NotNil(t, identity)
	assert.NotNil(t, cred)

	// Inactive key should NOT be found
	identity, cred, found = iam.LookupByAccessKey(UserAccessKeyPrefix + "INACTIVE1")
	assert.False(t, found)
	assert.Nil(t, identity)
	assert.Nil(t, cred)

	// Key with no status (default Active) should be found
	identity, cred, found = iam.LookupByAccessKey(UserAccessKeyPrefix + "DEFAULT12")
	assert.True(t, found)
	assert.NotNil(t, identity)
	assert.NotNil(t, cred)
}

// TestAuthIamAuthenticatesBeforeParseForm verifies that AuthIam authenticates the request
// BEFORE parsing the form. This is critical because ParseForm() consumes the request body,
// but IAM signature verification needs to hash the body.
// This test reproduces the bug described in GitHub issue #7802.
func TestAuthIamAuthenticatesBeforeParseForm(t *testing.T) {
	// Create IAM with test credentials
	iam := &IdentityAccessManagement{
		hashes:       make(map[string]*sync.Pool),
		hashCounters: make(map[string]*int32),
	}

	testConfig := &iam_pb.S3ApiConfiguration{
		Identities: []*iam_pb.Identity{
			{
				Name: "admin",
				Credentials: []*iam_pb.Credential{
					{
						AccessKey: "admin_access_key",
						SecretKey: "admin_secret_key",
						Status:    "Active",
					},
				},
				Actions: []string{"Admin"},
			},
		},
	}
	err := iam.loadS3ApiConfiguration(testConfig)
	assert.NoError(t, err)

	embeddedApi := &EmbeddedIamApi{
		iam: iam,
	}

	// Create a properly signed IAM request
	payload := "Action=CreateUser&Version=2010-05-08&UserName=bob"

	// Use current time to avoid clock skew
	now := time.Now().UTC()
	amzDate := now.Format(iso8601Format)
	dateStamp := now.Format(yyyymmdd)
	credentialScope := dateStamp + "/us-east-1/iam/aws4_request"

	req, err := http.NewRequest("POST", "http://localhost:8333/", strings.NewReader(payload))
	assert.NoError(t, err)

	req.Header.Set("Content-Type", "application/x-www-form-urlencoded; charset=utf-8")
	req.Header.Set("Host", "localhost:8333")
	req.Header.Set("X-Amz-Date", amzDate)

	// Calculate the correct signature using IAM service
	payloadHash := getSHA256Hash([]byte(payload))
	canonicalRequest := fmt.Sprintf("POST\n/\n\ncontent-type:application/x-www-form-urlencoded; charset=utf-8\nhost:localhost:8333\nx-amz-date:%s\n\ncontent-type;host;x-amz-date\n%s", amzDate, payloadHash)
	canonicalRequestHash := getSHA256Hash([]byte(canonicalRequest))
	stringToSign := fmt.Sprintf("AWS4-HMAC-SHA256\n%s\n%s\n%s", amzDate, credentialScope, canonicalRequestHash)
	signingKey := getSigningKey("admin_secret_key", dateStamp, "us-east-1", "iam")
	signature := getSignature(signingKey, stringToSign)

	authHeader := fmt.Sprintf("AWS4-HMAC-SHA256 Credential=admin_access_key/%s, SignedHeaders=content-type;host;x-amz-date, Signature=%s",
		credentialScope, signature)
	req.Header.Set("Authorization", authHeader)

	// Create a test handler that just returns OK
	handlerCalled := false
	testHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		handlerCalled = true
		w.WriteHeader(http.StatusOK)
	})

	// Wrap with AuthIam
	authHandler := embeddedApi.AuthIam(testHandler, ACTION_WRITE)

	// Execute the request
	rr := httptest.NewRecorder()
	authHandler.ServeHTTP(rr, req)

	// The handler should be called (authentication succeeded)
	// Before the fix, this would fail with SignatureDoesNotMatch because
	// ParseForm was called before authentication, consuming the body
	assert.True(t, handlerCalled, "Handler was not called - authentication failed")
	assert.Equal(t, http.StatusOK, rr.Code, "Expected OK status, got %d", rr.Code)
}

// TestOldCodeOrderWouldFail demonstrates why the old code order was broken.
// This test shows that ParseForm() before signature verification causes auth failure.
func TestOldCodeOrderWouldFail(t *testing.T) {
	// Create IAM with test credentials
	iam := &IdentityAccessManagement{
		hashes:       make(map[string]*sync.Pool),
		hashCounters: make(map[string]*int32),
	}

	testConfig := &iam_pb.S3ApiConfiguration{
		Identities: []*iam_pb.Identity{
			{
				Name: "admin",
				Credentials: []*iam_pb.Credential{
					{
						AccessKey: "admin_access_key",
						SecretKey: "admin_secret_key",
						Status:    "Active",
					},
				},
				Actions: []string{"Admin"},
			},
		},
	}
	err := iam.loadS3ApiConfiguration(testConfig)
	assert.NoError(t, err)

	// Create a properly signed IAM request
	payload := "Action=CreateUser&Version=2010-05-08&UserName=bob"

	now := time.Now().UTC()
	amzDate := now.Format(iso8601Format)
	dateStamp := now.Format(yyyymmdd)
	credentialScope := dateStamp + "/us-east-1/iam/aws4_request"

	req, err := http.NewRequest("POST", "http://localhost:8333/", strings.NewReader(payload))
	assert.NoError(t, err)

	req.Header.Set("Content-Type", "application/x-www-form-urlencoded; charset=utf-8")
	req.Header.Set("Host", "localhost:8333")
	req.Header.Set("X-Amz-Date", amzDate)

	// Calculate the correct signature using IAM service
	payloadHash := getSHA256Hash([]byte(payload))
	canonicalRequest := fmt.Sprintf("POST\n/\n\ncontent-type:application/x-www-form-urlencoded; charset=utf-8\nhost:localhost:8333\nx-amz-date:%s\n\ncontent-type;host;x-amz-date\n%s", amzDate, payloadHash)
	canonicalRequestHash := getSHA256Hash([]byte(canonicalRequest))
	stringToSign := fmt.Sprintf("AWS4-HMAC-SHA256\n%s\n%s\n%s", amzDate, credentialScope, canonicalRequestHash)
	signingKey := getSigningKey("admin_secret_key", dateStamp, "us-east-1", "iam")
	signature := getSignature(signingKey, stringToSign)

	authHeader := fmt.Sprintf("AWS4-HMAC-SHA256 Credential=admin_access_key/%s, SignedHeaders=content-type;host;x-amz-date, Signature=%s",
		credentialScope, signature)
	req.Header.Set("Authorization", authHeader)

	// Simulate OLD buggy code: ParseForm BEFORE authentication
	// This consumes the request body!
	err = req.ParseForm()
	assert.NoError(t, err)
	assert.Equal(t, "CreateUser", req.Form.Get("Action")) // Form parsing works

	// Now try to authenticate - this should FAIL because body is consumed
	identity, errCode := iam.AuthSignatureOnly(req)

	// With old code order, this would fail with SignatureDoesNotMatch
	// because the body is empty when signature verification tries to hash it
	assert.Equal(t, s3err.ErrSignatureDoesNotMatch, errCode,
		"Expected SignatureDoesNotMatch when ParseForm is called before auth")
	assert.Nil(t, identity)

	t.Log("This demonstrates the bug: ParseForm before auth causes SignatureDoesNotMatch")
}

// TestEmbeddedIamExecuteAction tests calling ExecuteAction directly
func TestEmbeddedIamExecuteAction(t *testing.T) {
	api := NewEmbeddedIamApiForTest()
	api.mockConfig = &iam_pb.S3ApiConfiguration{}

	// Explicitly set hook to debug panic
	api.EmbeddedIamApi.reloadConfigurationFunc = func() error {
		return nil
	}

	// Test case: CreateUser via ExecuteAction
	vals := url.Values{}
	vals.Set("Action", "CreateUser")
	vals.Set("UserName", "ExecuteActionUser")

	resp, iamErr := api.ExecuteAction(vals, false)
	assert.Nil(t, iamErr)

	// Verify response type
	createResp, ok := resp.(iamCreateUserResponse)
	assert.True(t, ok)
	assert.Equal(t, "ExecuteActionUser", *createResp.CreateUserResult.User.UserName)

	// Verify persistence
	assert.Len(t, api.mockConfig.Identities, 1)
	assert.Equal(t, "ExecuteActionUser", api.mockConfig.Identities[0].Name)
}
