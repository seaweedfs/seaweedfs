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
	"github.com/seaweedfs/seaweedfs/weed/credential/memory"
	"github.com/seaweedfs/seaweedfs/weed/pb/iam_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api"
	"github.com/seaweedfs/seaweedfs/weed/s3api/policy_engine"
	"github.com/seaweedfs/seaweedfs/weed/util/request_id"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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

func TestListUsersRequestIdMatchesResponseHeader(t *testing.T) {
	params := &iam.ListUsersInput{}
	req, _ := iam.New(session.New()).ListUsersRequest(params)
	_ = req.Build()

	out := ListUsersResponse{}
	response, err := executeRequest(req.HTTPRequest, out)
	assert.Equal(t, nil, err)
	assert.Equal(t, http.StatusOK, response.Code)

	headerRequestID := response.Header().Get(request_id.AmzRequestIDHeader)
	assert.NotEmpty(t, headerRequestID)
	assert.Equal(t, headerRequestID, extractRequestID(response))
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

// isolatedIamConfig is a self-contained IamS3ApiConfig backing whose state lives
// on the instance (not the package globals that executeRequest shares), so
// policy-version tests stay order-independent under -shuffle.
type isolatedIamConfig struct {
	identities []*iam_pb.Identity
	groups     []*iam_pb.Group
	policies   Policies
}

func newIsolatedIamServer() *IamApiServer {
	return &IamApiServer{s3ApiConfig: &isolatedIamConfig{
		policies: Policies{Policies: make(map[string]policy_engine.PolicyDocument)},
	}}
}

func (m *isolatedIamConfig) GetS3ApiConfiguration(s3cfg *iam_pb.S3ApiConfiguration) error {
	_ = copier.Copy(&s3cfg.Identities, &m.identities)
	_ = copier.Copy(&s3cfg.Groups, &m.groups)
	return nil
}

func (m *isolatedIamConfig) PutS3ApiConfiguration(s3cfg *iam_pb.S3ApiConfiguration) error {
	_ = copier.Copy(&m.identities, &s3cfg.Identities)
	_ = copier.Copy(&m.groups, &s3cfg.Groups)
	return nil
}

func (m *isolatedIamConfig) GetPolicies(policies *Policies) error {
	_ = copier.Copy(&policies, &m.policies)
	return nil
}

func (m *isolatedIamConfig) PutPolicies(policies *Policies) error {
	_ = copier.Copy(&m.policies, &policies)
	return nil
}

// TestCreatePolicyVersion reproduces issue #9785: the AWS Terraform provider
// updates a managed policy in place via CreatePolicyVersion, which previously
// returned 501 NotImplemented. The whole read/update surface Terraform relies on
// is exercised here: GetPolicy (DefaultVersionId), CreatePolicyVersion,
// GetPolicyVersion and ListPolicyVersions.
func TestCreatePolicyVersion(t *testing.T) {
	srv := newIsolatedIamServer()
	svc := iam.New(session.New())
	policyName := "tf-managed-policy"
	policyArn := aws.String("arn:aws:iam:::policy/" + policyName)

	// Create the managed policy.
	createReq, _ := svc.CreatePolicyRequest(&iam.CreatePolicyInput{
		PolicyName:     aws.String(policyName),
		PolicyDocument: aws.String(`{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":["s3:Get*","s3:List*"],"Resource":["arn:aws:s3:::EXAMPLE-BUCKET"]}]}`),
	})
	_ = createReq.Build()
	resp, err := executeRequestWith(srv, createReq.HTTPRequest, nil)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.Code)

	// Update it in place (what Terraform does). This used to return 501.
	cpvReq, _ := svc.CreatePolicyVersionRequest(&iam.CreatePolicyVersionInput{
		PolicyArn:      policyArn,
		PolicyDocument: aws.String(`{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":["s3:Get*"],"Resource":["arn:aws:s3:::EXAMPLE-BUCKET"]}]}`),
		SetAsDefault:   aws.Bool(true),
	})
	_ = cpvReq.Build()
	resp, err = executeRequestWith(srv, cpvReq.HTTPRequest, nil)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.Code)
	var cpvResp CreatePolicyVersionResponse
	require.NoError(t, xml.Unmarshal(resp.Body.Bytes(), &cpvResp))
	require.NotNil(t, cpvResp.CreatePolicyVersionResult.PolicyVersion.VersionId)
	assert.Equal(t, "v1", *cpvResp.CreatePolicyVersionResult.PolicyVersion.VersionId)
	require.NotNil(t, cpvResp.CreatePolicyVersionResult.PolicyVersion.IsDefaultVersion)
	assert.True(t, *cpvResp.CreatePolicyVersionResult.PolicyVersion.IsDefaultVersion)

	// GetPolicy must advertise a default version so Terraform's read can chain
	// into GetPolicyVersion.
	gpReq, _ := svc.GetPolicyRequest(&iam.GetPolicyInput{PolicyArn: policyArn})
	_ = gpReq.Build()
	resp, err = executeRequestWith(srv, gpReq.HTTPRequest, nil)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.Code)
	var gpResp GetPolicyResponse
	require.NoError(t, xml.Unmarshal(resp.Body.Bytes(), &gpResp))
	require.NotNil(t, gpResp.GetPolicyResult.Policy.DefaultVersionId)
	assert.Equal(t, "v1", *gpResp.GetPolicyResult.Policy.DefaultVersionId)

	// GetPolicyVersion must return the updated document (Get* only, List* gone).
	gpvReq, _ := svc.GetPolicyVersionRequest(&iam.GetPolicyVersionInput{PolicyArn: policyArn, VersionId: aws.String("v1")})
	_ = gpvReq.Build()
	resp, err = executeRequestWith(srv, gpvReq.HTTPRequest, nil)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.Code)
	var gpvResp GetPolicyVersionResponse
	require.NoError(t, xml.Unmarshal(resp.Body.Bytes(), &gpvResp))
	require.NotNil(t, gpvResp.GetPolicyVersionResult.PolicyVersion.Document)
	assert.Contains(t, *gpvResp.GetPolicyVersionResult.PolicyVersion.Document, "s3:Get*")
	assert.NotContains(t, *gpvResp.GetPolicyVersionResult.PolicyVersion.Document, "s3:List*")

	// ListPolicyVersions must report exactly the single default version.
	lpvReq, _ := svc.ListPolicyVersionsRequest(&iam.ListPolicyVersionsInput{PolicyArn: policyArn})
	_ = lpvReq.Build()
	resp, err = executeRequestWith(srv, lpvReq.HTTPRequest, nil)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.Code)
	var lpvResp ListPolicyVersionsResponse
	require.NoError(t, xml.Unmarshal(resp.Body.Bytes(), &lpvResp))
	require.Len(t, lpvResp.ListPolicyVersionsResult.Versions, 1)
	assert.Equal(t, "v1", *lpvResp.ListPolicyVersionsResult.Versions[0].VersionId)
}

// TestCreatePolicyVersionMissingPolicy verifies a NoSuchEntity (404) when the
// target policy does not exist, matching AWS.
func TestCreatePolicyVersionMissingPolicy(t *testing.T) {
	srv := newIsolatedIamServer()
	req, _ := iam.New(session.New()).CreatePolicyVersionRequest(&iam.CreatePolicyVersionInput{
		PolicyArn:      aws.String("arn:aws:iam:::policy/does-not-exist"),
		PolicyDocument: aws.String(`{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":["s3:Get*"],"Resource":["arn:aws:s3:::EXAMPLE-BUCKET"]}]}`),
		SetAsDefault:   aws.Bool(true),
	})
	_ = req.Build()
	resp, err := executeRequestWith(srv, req.HTTPRequest, nil)
	require.NoError(t, err)
	assert.Equal(t, http.StatusNotFound, resp.Code)
	code, _ := extractErrorCodeAndMessage(resp)
	assert.Equal(t, "NoSuchEntity", code)
}

// TestCreatePolicyVersionRequiresSetAsDefault verifies that, given the
// single-version model, a request that does not set SetAsDefault=true is
// rejected rather than silently overwriting the active document.
func TestCreatePolicyVersionRequiresSetAsDefault(t *testing.T) {
	srv := newIsolatedIamServer()
	svc := iam.New(session.New())
	policyArn := aws.String("arn:aws:iam:::policy/tf-managed-policy")

	createReq, _ := svc.CreatePolicyRequest(&iam.CreatePolicyInput{
		PolicyName:     aws.String("tf-managed-policy"),
		PolicyDocument: aws.String(`{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":["s3:Get*"],"Resource":["arn:aws:s3:::EXAMPLE-BUCKET"]}]}`),
	})
	_ = createReq.Build()
	resp, err := executeRequestWith(srv, createReq.HTTPRequest, nil)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.Code)

	// SetAsDefault omitted (defaults to false) must be rejected.
	cpvReq, _ := svc.CreatePolicyVersionRequest(&iam.CreatePolicyVersionInput{
		PolicyArn:      policyArn,
		PolicyDocument: aws.String(`{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":["s3:Put*"],"Resource":["arn:aws:s3:::EXAMPLE-BUCKET"]}]}`),
	})
	_ = cpvReq.Build()
	resp, err = executeRequestWith(srv, cpvReq.HTTPRequest, nil)
	require.NoError(t, err)
	assert.Equal(t, http.StatusBadRequest, resp.Code)
	code, _ := extractErrorCodeAndMessage(resp)
	assert.Equal(t, "InvalidInput", code)

	// The live document must be untouched (still Get*, not Put*).
	gpvReq, _ := svc.GetPolicyVersionRequest(&iam.GetPolicyVersionInput{PolicyArn: policyArn, VersionId: aws.String("v1")})
	_ = gpvReq.Build()
	resp, err = executeRequestWith(srv, gpvReq.HTTPRequest, nil)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.Code)
	var gpvResp GetPolicyVersionResponse
	require.NoError(t, xml.Unmarshal(resp.Body.Bytes(), &gpvResp))
	require.NotNil(t, gpvResp.GetPolicyVersionResult.PolicyVersion.Document)
	assert.Contains(t, *gpvResp.GetPolicyVersionResult.PolicyVersion.Document, "s3:Get*")
	assert.NotContains(t, *gpvResp.GetPolicyVersionResult.PolicyVersion.Document, "s3:Put*")
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
	assert.Contains(t, response.Body.String(), "<RequestId>")
	assert.NotContains(t, response.Body.String(), "<ResponseMetadata>")
	assert.Equal(t, response.Header().Get(request_id.AmzRequestIDHeader), extractRequestID(response))
}

func extractErrorCodeAndMessage(response *httptest.ResponseRecorder) (string, string) {
	pattern := `<Error><Code>(.*)</Code><Message>(.*)</Message><Type>(.*)</Type></Error>`
	re := regexp.MustCompile(pattern)

	code := re.FindStringSubmatch(response.Body.String())[1]
	message := re.FindStringSubmatch(response.Body.String())[2]
	return code, message
}

func extractRequestID(response *httptest.ResponseRecorder) string {
	re := regexp.MustCompile(`<RequestId>([^<]+)</RequestId>`)
	matches := re.FindStringSubmatch(response.Body.String())
	if len(matches) < 2 {
		return ""
	}
	return matches[1]
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

// iamApiServerWithCredentialManager builds an IamApiServer backed by a real
// in-memory credential store. Used for tests that exercise the targeted
// credentialManager paths (e.g. CreateUser) instead of the legacy mock.
func iamApiServerWithCredentialManager() (*IamApiServer, *credential.CredentialManager, *countingConfigSaver) {
	store := &memory.MemoryStore{}
	store.Initialize(nil, "")
	cm := &credential.CredentialManager{Store: store}

	counter := &countingConfigSaver{cm: cm}
	iamInstance := &s3api.IdentityAccessManagement{}
	// Wire up the credential manager so GetCredentialManager() returns it.
	iamInstance.SetCredentialManagerForTest(cm)

	srv := &IamApiServer{
		s3ApiConfig: counter,
		iam:         iamInstance,
	}
	return srv, cm, counter
}

// countingConfigSaver wraps the credential manager and counts full-config saves.
type countingConfigSaver struct {
	cm        *credential.CredentialManager
	putCalled int
}

func (c *countingConfigSaver) GetS3ApiConfiguration(s3cfg *iam_pb.S3ApiConfiguration) error {
	config, err := c.cm.LoadConfiguration(context.Background())
	if err != nil {
		return err
	}
	s3cfg.Identities = config.Identities
	s3cfg.Policies = config.Policies
	return nil
}

func (c *countingConfigSaver) PutS3ApiConfiguration(s3cfg *iam_pb.S3ApiConfiguration) error {
	c.putCalled++
	return c.cm.SaveConfiguration(context.Background(), s3cfg)
}

func (c *countingConfigSaver) GetPolicies(policies *Policies) error {
	return nil
}

func (c *countingConfigSaver) PutPolicies(policies *Policies) error {
	return nil
}

func executeRequestWith(srv *IamApiServer, req *http.Request, v interface{}) (*httptest.ResponseRecorder, error) {
	rr := httptest.NewRecorder()
	apiRouter := mux.NewRouter().SkipClean(true)
	apiRouter.Path("/").Methods(http.MethodPost).HandlerFunc(srv.DoActions)
	apiRouter.ServeHTTP(rr, req)
	return rr, xml.Unmarshal(rr.Body.Bytes(), &v)
}

// TestCreateUserDoesNotSaveAllUsers is a regression test for the bug where
// creating a new user triggered a full SaveConfiguration over all existing
// identities (causing N file writes + reload cycles in the filer_etc store).
// The fix uses credentialManager.CreateUser for a targeted single-file write.
func TestCreateUserDoesNotSaveAllUsers(t *testing.T) {
	srv, cm, counter := iamApiServerWithCredentialManager()
	ctx := context.Background()

	// Pre-populate three existing users.
	for _, name := range []string{"existing-1", "existing-2", "existing-3"} {
		require.NoError(t, cm.CreateUser(ctx, &iam_pb.Identity{Name: name}))
	}

	// Create a new user via the HTTP API.
	params := &iam.CreateUserInput{UserName: aws.String("new-user")}
	req, _ := iam.New(session.New()).CreateUserRequest(params)
	_ = req.Build()
	out := CreateUserResponse{}
	resp, err := executeRequestWith(srv, req.HTTPRequest, &out)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.Code)

	// The new user must appear in the store.
	newUser, userErr := cm.GetUser(ctx, "new-user")
	require.NoError(t, userErr)
	require.Equal(t, "new-user", newUser.Name)

	// Critical: full SaveConfiguration (PutS3ApiConfiguration) must NOT have
	// been called. Before the fix it was called once per CreateUser, rewriting
	// every existing user file and triggering a cascade of reload events.
	assert.Equal(t, 0, counter.putCalled,
		"CreateUser must not trigger a full PutS3ApiConfiguration over all users")

	// All pre-existing users must still be intact.
	for _, name := range []string{"existing-1", "existing-2", "existing-3"} {
		u, err := cm.GetUser(ctx, name)
		require.NoError(t, err, "pre-existing user %s should still exist", name)
		assert.Equal(t, name, u.Name)
	}
}
