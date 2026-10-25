package iamapi

import (
	"context"
	"crypto/hmac"
	"crypto/sha1"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
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

// The implicit UserName must come from the identity authentication verified,
// never from re-parsing the Authorization header: a request can carry presigned
// credentials for one user alongside a junk SigV4 header naming another.
func TestImplicitUserNameUsesVerifiedIdentity(t *testing.T) {
	server := newAuthzTestServer(t)
	expires := strconv.FormatInt(time.Now().Add(10*time.Minute).Unix(), 10)
	sts := "POST\n\napplication/x-www-form-urlencoded\n" + expires + "\n/"
	mac := hmac.New(sha1.New, []byte("wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"))
	mac.Write([]byte(sts))
	sig := base64.StdEncoding.EncodeToString(mac.Sum(nil))
	q := url.Values{"AWSAccessKeyId": {"AKIAIOSFODNN7EXAMPLE"}, "Expires": {expires}, "Signature": {sig}}

	post := func(t *testing.T, body string) (int, string) {
		t.Helper()
		req, err := http.NewRequest(http.MethodPost, server.URL+"/?"+q.Encode(), strings.NewReader(body))
		require.NoError(t, err)
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		req.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential=AKIATESTFAKEADMIN0001/20261021/us-east-1/iam/aws4_request, SignedHeaders=host, Signature=00")
		resp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		defer resp.Body.Close()
		b, err := io.ReadAll(resp.Body)
		require.NoError(t, err)
		t.Logf("%d %s", resp.StatusCode, b)
		return resp.StatusCode, string(b)
	}

	for _, action := range []string{"ListAccessKeys", "CreateAccessKey", "GetUser"} {
		t.Run(action, func(t *testing.T) {
			code, body := post(t, "Action="+action+"&Version=2010-05-08")
			assert.Equal(t, http.StatusOK, code)
			assert.NotContains(t, body, "iam_admin")
			assert.Contains(t, body, "power_user")
		})
	}

	// ListUserPolicies is not self-service: a caller without IAM rights is
	// denied whichever name the implicit resolution lands on.
	t.Run("ListUserPolicies", func(t *testing.T) {
		code, body := post(t, "Action=ListUserPolicies&Version=2010-05-08")
		assert.Equal(t, http.StatusForbidden, code)
		assert.NotContains(t, body, "iam_admin")
	})

	// A write aimed at the header-named admin's key resolves to the verified
	// caller instead: the key is not power_user's, so it is not found.
	t.Run("UpdateAccessKey", func(t *testing.T) {
		code, body := post(t, "Action=UpdateAccessKey&AccessKeyId=AKIATESTFAKEADMIN0001&Status=Inactive&Version=2010-05-08")
		assert.Equal(t, http.StatusNotFound, code)
		assert.Contains(t, body, "NoSuchEntity")
	})

	t.Run("DeleteAccessKey", func(t *testing.T) {
		code, _ := post(t, "Action=DeleteAccessKey&AccessKeyId=AKIATESTFAKEADMIN0001&Version=2010-05-08")
		assert.Equal(t, http.StatusOK, code)
	})

	// The admin key the junk header named is untouched: still present and active.
	resp := postSignedIamAction(t, server.URL, "AKIATESTFAKEADMIN0001", "testAdminSecretFake0000000000000000000000",
		"", "Action=ListAccessKeys&Version=2010-05-08")
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	assert.Contains(t, string(body), "AKIATESTFAKEADMIN0001")
	assert.Contains(t, string(body), "<Status>Active</Status>")
}
