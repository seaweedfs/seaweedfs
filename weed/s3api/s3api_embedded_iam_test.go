package s3api

import (
	"encoding/json"
	"encoding/xml"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/iam"
	"github.com/gorilla/mux"
	"github.com/seaweedfs/seaweedfs/weed/pb/iam_pb"
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
	xmlBytes, _ := xml.Marshal(response)
	w.Write(xmlBytes)
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
					{AccessKey: "AKIATEST12345", SecretKey: "secret"},
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
	assert.Equal(t, "AKIATEST12345", api.mockConfig.Identities[0].Credentials[0].AccessKey)

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
					{AccessKey: "AKIATEST12345", SecretKey: "secret"},
				},
			},
		},
	}

	// Use direct form post since AWS SDK may format differently
	form := url.Values{}
	form.Set("Action", "DeleteAccessKey")
	form.Set("UserName", "TestUser")
	form.Set("AccessKeyId", "AKIATEST12345")

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
	iam := &IdentityAccessManagement{}
	testConfig := &iam_pb.S3ApiConfiguration{
		Identities: []*iam_pb.Identity{
			{
				Name: "testuser1",
				Credentials: []*iam_pb.Credential{
					{AccessKey: "197FSAQ7HHTA48X64O3A", SecretKey: "testsecret"},
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
		{&http.Request{Header: http.Header{"Authorization": []string{"AWS4-HMAC-SHA256 Credential=197FSAQ7HHTA48X64O3A/20220420/us-east-1/iam/aws4_request, SignedHeaders=content-type;host;x-amz-date, Signature=6757dc6b3d7534d67e17842760310e99ee695408497f6edc4fdb84770c252dc8"}}}, url.Values{}, "testuser1"},
		// Malformed auth header (no Credential=) - should not set username
		{&http.Request{Header: http.Header{"Authorization": []string{"AWS4-HMAC-SHA256 =197FSAQ7HHTA48X64O3A/20220420/test1/iam/aws4_request, SignedHeaders=content-type;host;x-amz-date, Signature=6757dc6b3d7534d67e17842760310e99ee695408497f6edc4fdb84770c252dc8"}}}, url.Values{}, ""},
		// Unknown access key - should not set username
		{&http.Request{Header: http.Header{"Authorization": []string{"AWS4-HMAC-SHA256 Credential=UNKNOWNACCESSKEY12345/20220420/us-east-1/iam/aws4_request, SignedHeaders=content-type;host;x-amz-date, Signature=6757dc6b3d7534d67e17842760310e99ee695408497f6edc4fdb84770c252dc8"}}}, url.Values{}, ""},
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

