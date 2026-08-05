package iamapi

import (
	"context"
	"crypto/sha256"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	"github.com/gorilla/mux"
	"github.com/seaweedfs/seaweedfs/weed/credential"
	"github.com/seaweedfs/seaweedfs/weed/credential/memory"
	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/pb/iam_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// A data-plane identity holding nothing but the equivalent of AmazonS3FullAccess,
// and an admin, so the same request can be run from both sides. The admin key is
// obviously fake to keep secret scanners quiet.
const authzConfigJSON = `{
  "identities": [
    {
      "name": "power_user",
      "credentials": [{"accessKey": "AKIAIOSFODNN7EXAMPLE", "secretKey": "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"}],
      "policyNames": ["PowerUserPolicy"]
    },
    {
      "name": "iam_admin",
      "credentials": [{"accessKey": "AKIATESTFAKEADMIN0001", "secretKey": "testAdminSecretFake0000000000000000000000"}],
      "actions": ["Admin"]
    }
  ],
  "policies": [
    {
      "name": "PowerUserPolicy",
      "content": "{\"Version\":\"2012-10-17\",\"Statement\":[{\"Effect\":\"Allow\",\"Action\":\"s3:*\",\"Resource\":\"*\"}]}"
    }
  ]
}`

func newAuthzTestServer(t *testing.T) *httptest.Server {
	t.Helper()

	config := &iam_pb.S3ApiConfiguration{}
	require.NoError(t, filer.ParseS3ConfigurationFromBytes([]byte(authzConfigJSON), config))

	store := &memory.MemoryStore{}
	require.NoError(t, store.Initialize(nil, ""))
	cm := &credential.CredentialManager{Store: store}
	require.NoError(t, cm.SaveConfiguration(context.Background(), config))

	iam := &s3api.IdentityAccessManagement{}
	iam.SetCredentialManagerForTest(cm)
	require.NoError(t, iam.LoadS3ApiConfigurationFromBytes([]byte(authzConfigJSON)))

	iama := &IamApiServer{
		iam:         iam,
		s3ApiConfig: &countingConfigSaver{cm: cm},
	}
	router := mux.NewRouter().SkipClean(true)
	iama.registerRouter(router)

	server := httptest.NewServer(router)
	t.Cleanup(server.Close)
	return server
}

func postSignedIamAction(t *testing.T, serverURL, accessKey, secretKey, rawQuery, body string) *http.Response {
	t.Helper()

	url := serverURL + "/"
	if rawQuery != "" {
		url += "?" + rawQuery
	}
	req, err := http.NewRequest(http.MethodPost, url, strings.NewReader(body))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	payloadHash := fmt.Sprintf("%x", sha256.Sum256([]byte(body)))
	require.NoError(t, v4.NewSigner().SignHTTP(context.Background(),
		aws.Credentials{AccessKeyID: accessKey, SecretAccessKey: secretKey},
		req, payloadHash, "iam", "us-east-1", time.Now()))

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	t.Cleanup(func() { resp.Body.Close() })
	return resp
}

// An S3 data-plane policy grants no IAM management, however broad it is: the
// standalone server used to check these actions as a coarse S3 action, which
// such a policy matches.
func TestIamManagementDeniedForDataPlanePolicy(t *testing.T) {
	server := newAuthzTestServer(t)

	for _, action := range []string{"CreateUser", "CreateAccessKey", "PutUserPolicy", "AttachUserPolicy"} {
		t.Run(action, func(t *testing.T) {
			resp := postSignedIamAction(t, server.URL, "AKIAIOSFODNN7EXAMPLE", "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
				"", "Action="+action+"&UserName=victim&Version=2010-05-08")
			assert.Equal(t, http.StatusForbidden, resp.StatusCode)
		})
	}
}

// Same expectation when the request carries an S3 query parameter, which the
// action resolver used to read even for an IAM action.
func TestIamManagementDeniedWithS3QueryParameter(t *testing.T) {
	server := newAuthzTestServer(t)

	for _, rawQuery := range []string{"delete", "acl", "tagging", "policy"} {
		t.Run(rawQuery, func(t *testing.T) {
			resp := postSignedIamAction(t, server.URL, "AKIAIOSFODNN7EXAMPLE", "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
				rawQuery, "Action=CreateUser&UserName=victim&Version=2010-05-08")
			assert.Equal(t, http.StatusForbidden, resp.StatusCode)
		})
	}
}

// Self-service still works without an IAM grant, matching AWS and the embedded
// IAM surface: a user may rotate its own access keys.
func TestIamSelfServiceAllowedForDataPlanePolicy(t *testing.T) {
	server := newAuthzTestServer(t)

	resp := postSignedIamAction(t, server.URL, "AKIAIOSFODNN7EXAMPLE", "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
		"", "Action=ListAccessKeys&Version=2010-05-08")
	assert.Equal(t, http.StatusOK, resp.StatusCode)
}

func TestIamManagementAllowedForAdmin(t *testing.T) {
	server := newAuthzTestServer(t)

	resp := postSignedIamAction(t, server.URL, "AKIATESTFAKEADMIN0001", "testAdminSecretFake0000000000000000000000",
		"", "Action=CreateUser&UserName=new-user&Version=2010-05-08")
	assert.Equal(t, http.StatusOK, resp.StatusCode)
}
