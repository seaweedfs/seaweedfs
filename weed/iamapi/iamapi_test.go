package iamapi

import (
	"context"
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
	"github.com/seaweedfs/seaweedfs/weed/credential"
	"github.com/seaweedfs/seaweedfs/weed/pb/iam_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api"
	"github.com/seaweedfs/seaweedfs/weed/s3api/policy_engine"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"github.com/stretchr/testify/assert"
)

var GetPolicies func(policies *Policies) (err error)
var PutPolicies func(policies *Policies) (err error)

var s3config = iam_pb.S3ApiConfiguration{}
var policiesFile = Policies{Policies: make(map[string]policy_engine.PolicyDocument)}

// Setup MockCredentialStore
type MockCredentialStore struct{}

func (m *MockCredentialStore) GetName() credential.CredentialStoreTypeName { return "mock" }
func (m *MockCredentialStore) Initialize(configuration util.Configuration, prefix string) error {
	return nil
}
func (m *MockCredentialStore) LoadConfiguration(ctx context.Context) (*iam_pb.S3ApiConfiguration, error) {
	var cfg iam_pb.S3ApiConfiguration
	_ = copier.Copy(&cfg, &s3config)
	return &cfg, nil
}
func (m *MockCredentialStore) SaveConfiguration(ctx context.Context, config *iam_pb.S3ApiConfiguration) error {
	_ = copier.Copy(&s3config, config)
	return nil
}
func (m *MockCredentialStore) CreateUser(ctx context.Context, identity *iam_pb.Identity) error {
	return nil
}
func (m *MockCredentialStore) GetUser(ctx context.Context, username string) (*iam_pb.Identity, error) {
	return nil, nil
}
func (m *MockCredentialStore) UpdateUser(ctx context.Context, username string, identity *iam_pb.Identity) error {
	return nil
}
func (m *MockCredentialStore) DeleteUser(ctx context.Context, username string) error { return nil }
func (m *MockCredentialStore) ListUsers(ctx context.Context) ([]string, error)       { return nil, nil }
func (m *MockCredentialStore) GetUserByAccessKey(ctx context.Context, accessKey string) (*iam_pb.Identity, error) {
	return nil, nil
}
func (m *MockCredentialStore) CreateAccessKey(ctx context.Context, username string, credential *iam_pb.Credential) error {
	return nil
}
func (m *MockCredentialStore) DeleteAccessKey(ctx context.Context, username string, accessKey string) error {
	return nil
}
func (m *MockCredentialStore) GetPolicies(ctx context.Context) (map[string]policy_engine.PolicyDocument, error) {
	return nil, nil
}
func (m *MockCredentialStore) PutPolicy(ctx context.Context, name string, document policy_engine.PolicyDocument) error {
	return nil
}
func (m *MockCredentialStore) DeletePolicy(ctx context.Context, name string) error { return nil }
func (m *MockCredentialStore) GetPolicy(ctx context.Context, name string) (*policy_engine.PolicyDocument, error) {
	return nil, nil
}
func (m *MockCredentialStore) CreateServiceAccount(ctx context.Context, sa *iam_pb.ServiceAccount) error {
	return nil
}
func (m *MockCredentialStore) UpdateServiceAccount(ctx context.Context, id string, sa *iam_pb.ServiceAccount) error {
	return nil
}
func (m *MockCredentialStore) DeleteServiceAccount(ctx context.Context, id string) error { return nil }
func (m *MockCredentialStore) GetServiceAccount(ctx context.Context, id string) (*iam_pb.ServiceAccount, error) {
	return nil, nil
}
func (m *MockCredentialStore) ListServiceAccounts(ctx context.Context) ([]*iam_pb.ServiceAccount, error) {
	return nil, nil
}
func (m *MockCredentialStore) GetServiceAccountByAccessKey(ctx context.Context, accessKey string) (*iam_pb.ServiceAccount, error) {
	return nil, nil
}
func (m *MockCredentialStore) Shutdown() {}

// Initialize ias
var ias IamApiServer

func init() {
	// Infect the credential manager with our mock store
	// Since NewIdentityAccessManagementWithStore creates a default one, we need to swap the store inside it
	// Accessing unexported field 'credentialManager' in 'IdentityAccessManagement' requires reflection or
	// standard setters if available.
	// However, GetCredentialManager returns *CredentialManager.
	// *CredentialManager has 'store' field which is unexported in credential package.
	// But we can create a NEW CredentialManager with our mock store and set it on iam if the field was exported.
	// It is NOT exported.

	// Wait, credential.NewCredentialManager is a function.
	// Check if is a way to set store.

	// Hack: register our mock store in credential.Stores?
	credential.Stores = append(credential.Stores, &MockCredentialStore{})

	// Initialize ias after registering store
	ias = IamApiServer{
		s3ApiConfig: iamS3ApiConfigureMock{},
		iam:         s3api.NewIdentityAccessManagementWithStore(&s3api.S3ApiServerOption{}, "mock"),
	}

	_, _ = credential.NewCredentialManager("mock", nil, "")

	// Now how to set this 'cm' into 'ias.iam'?
	// 'iam.credentialManager' is unexported in 's3api'.

	// Maybe I should use reflection to set it, since this is a test.
}

// Reflection is messy.
// Alternative: NewIdentityAccessManagementWithStore takes 'explicitStore' string.
// If I register my mock store with name "mock", I can pass "mock" to NewIdentityAccessManagementWithStore.

type iamS3ApiConfigureMock struct{}

func (iam iamS3ApiConfigureMock) GetPolicies(policies *Policies) (err error) {
	_ = copier.Copy(&policies, &policiesFile)
	return nil
}

func (iam iamS3ApiConfigureMock) PutPolicies(policies *Policies) (err error) {
	_ = copier.Copy(&policiesFile, &policies)
	return nil
}

func TestCreateUser(t *testing.T) {
	// Re-init ias with mock store properly
	credential.Stores = append(credential.Stores, &MockCredentialStore{})
	ias.iam = s3api.NewIdentityAccessManagementWithStore(&s3api.S3ApiServerOption{}, "mock")

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
