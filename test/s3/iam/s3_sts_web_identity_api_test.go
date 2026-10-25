package iam

import (
	"crypto/rand"
	"crypto/rsa"
	"encoding/base64"
	"encoding/json"
	"encoding/xml"
	"io"
	"math/big"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/iam"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/golang-jwt/jwt/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	webIdentityAudience = "web-identity-api-test"
	webIdentitySubject  = "spiffe://example.org/ns/app/sa/app"
)

// webIdentityIssuer is an OIDC issuer served by the test: discovery document
// and JWKS for one RSA key, so the server under test fetches real keys.
type webIdentityIssuer struct {
	server *httptest.Server
	key    *rsa.PrivateKey
}

func newWebIdentityIssuer(t *testing.T) *webIdentityIssuer {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)
	issuer := &webIdentityIssuer{key: key}
	mux := http.NewServeMux()
	mux.HandleFunc("/.well-known/openid-configuration", func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"issuer":                                issuer.server.URL,
			"jwks_uri":                              issuer.server.URL + "/keys",
			"id_token_signing_alg_values_supported": []string{"RS256"},
		})
	})
	mux.HandleFunc("/keys", func(w http.ResponseWriter, _ *http.Request) {
		b64 := func(b []byte) string { return base64.RawURLEncoding.EncodeToString(b) }
		_ = json.NewEncoder(w).Encode(map[string]any{"keys": []any{map[string]any{
			"kty": "RSA", "kid": "k1", "use": "sig", "alg": "RS256",
			"n": b64(key.PublicKey.N.Bytes()), "e": b64(big.NewInt(int64(key.PublicKey.E)).Bytes()),
		}}})
	})
	issuer.server = httptest.NewServer(mux)
	t.Cleanup(issuer.server.Close)
	return issuer
}

func (i *webIdentityIssuer) claims(sub string) jwt.MapClaims {
	now := time.Now()
	return jwt.MapClaims{"iss": i.server.URL, "sub": sub, "aud": webIdentityAudience,
		"iat": now.Unix(), "exp": now.Add(10 * time.Minute).Unix()}
}

func (i *webIdentityIssuer) token(t *testing.T, sub string, key *rsa.PrivateKey) string {
	t.Helper()
	tok := jwt.NewWithClaims(jwt.SigningMethodRS256, i.claims(sub))
	tok.Header["kid"] = "k1"
	signed, err := tok.SignedString(key)
	require.NoError(t, err)
	return signed
}

func (i *webIdentityIssuer) tokenForAudience(t *testing.T, sub, aud string) string {
	t.Helper()
	claims := i.claims(sub)
	claims["aud"] = aud
	tok := jwt.NewWithClaims(jwt.SigningMethodRS256, claims)
	tok.Header["kid"] = "k1"
	signed, err := tok.SignedString(i.key)
	require.NoError(t, err)
	return signed
}

func (i *webIdentityIssuer) unsignedToken(t *testing.T, sub string) string {
	t.Helper()
	signed, err := jwt.NewWithClaims(jwt.SigningMethodNone, i.claims(sub)).SignedString(jwt.UnsafeAllowNoneSignatureType)
	require.NoError(t, err)
	return signed
}

func trustPolicyFor(issuerURL, sub string) string {
	doc, _ := json.Marshal(map[string]any{"Version": "2012-10-17", "Statement": []any{map[string]any{
		"Effect": "Allow", "Principal": map[string]any{"Federated": issuerURL},
		"Action":    []string{"sts:AssumeRoleWithWebIdentity"},
		"Condition": map[string]any{"StringEquals": map[string]any{"oidc:sub": sub}},
	}}})
	return string(doc)
}

// assumeWithWebIdentity returns session credentials, or nil and the error body.
func assumeWithWebIdentity(t *testing.T, roleArn, token string) (*credentials.Credentials, string) {
	t.Helper()
	resp, err := callSTSAPI(t, url.Values{
		"Action": {"AssumeRoleWithWebIdentity"}, "Version": {"2011-06-15"},
		"RoleArn": {roleArn}, "RoleSessionName": {"web-identity-api"}, "WebIdentityToken": {token},
	})
	require.NoError(t, err)
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	if resp.StatusCode != http.StatusOK {
		return nil, string(body)
	}
	var out AssumeRoleWithWebIdentityTestResponse
	require.NoError(t, xml.Unmarshal(body, &out), "body: %s", body)
	c := out.Result.Credentials
	return credentials.NewStaticCredentials(c.AccessKeyId, c.SecretAccessKey, c.SessionToken), ""
}

func s3ClientWith(t *testing.T, creds *credentials.Credentials) *s3.S3 {
	t.Helper()
	sess, err := session.NewSession(&aws.Config{
		Region: aws.String(TestRegion), Endpoint: aws.String(TestS3Endpoint),
		Credentials: creds, S3ForcePathStyle: aws.Bool(true), DisableSSL: aws.Bool(true),
	})
	require.NoError(t, err)
	return s3.New(sess)
}

// TestWebIdentityWithProviderAndRoleManagedThroughIAMAPI configures STS
// federation entirely at runtime — an OIDC provider, a managed policy and a role
// created through the IAM API, with no static configuration — and checks that
// the role admits exactly the subject its trust policy names, with exactly the
// permissions of its attached policy.
func TestWebIdentityWithProviderAndRoleManagedThroughIAMAPI(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}
	if !isSTSEndpointRunning(t) {
		t.Fatal("SeaweedFS STS endpoint is not running at", TestSTSEndpoint, "- please run 'make setup-all-tests' first")
	}

	framework := NewS3IAMTestFramework(t)
	defer framework.Cleanup()
	admin, err := framework.CreateIAMClientWithJWT("admin-user", "TestAdminRole")
	require.NoError(t, err)
	adminS3, err := framework.CreateS3ClientWithJWT("admin-user", "TestAdminRole")
	require.NoError(t, err)

	issuer := newWebIdentityIssuer(t)
	bucket := framework.GenerateUniqueBucketName("web-identity")
	require.NoError(t, framework.CreateBucketWithCleanup(adminS3, bucket))

	provider, err := admin.CreateOpenIDConnectProvider(&iam.CreateOpenIDConnectProviderInput{
		Url:          aws.String(issuer.server.URL),
		ClientIDList: []*string{aws.String(webIdentityAudience)},
		// Required by this SDK version; pinning only applies to a TLS issuer,
		// and the test issuer is plain HTTP.
		ThumbprintList: []*string{aws.String("0000000000000000000000000000000000000000")},
	})
	require.NoError(t, err)
	defer admin.DeleteOpenIDConnectProvider(&iam.DeleteOpenIDConnectProviderInput{OpenIDConnectProviderArn: provider.OpenIDConnectProviderArn})

	policyDoc, _ := json.Marshal(map[string]any{"Version": "2012-10-17", "Statement": []any{map[string]any{
		"Effect": "Allow", "Action": []string{"s3:*"},
		"Resource": []string{"arn:aws:s3:::" + bucket, "arn:aws:s3:::" + bucket + "/*"},
	}}})
	policy, err := admin.CreatePolicy(&iam.CreatePolicyInput{
		PolicyName: aws.String(bucket + "-rw"), PolicyDocument: aws.String(string(policyDoc)),
	})
	require.NoError(t, err)
	defer admin.DeletePolicy(&iam.DeletePolicyInput{PolicyArn: policy.Policy.Arn})

	// Role names are at most 64 characters; the bucket name is longer.
	roleName := "web-identity-" + strconv.FormatInt(time.Now().UnixNano(), 36)
	role, err := admin.CreateRole(&iam.CreateRoleInput{
		RoleName:                 aws.String(roleName),
		AssumeRolePolicyDocument: aws.String(trustPolicyFor(issuer.server.URL, webIdentitySubject)),
	})
	require.NoError(t, err)
	defer admin.DeleteRole(&iam.DeleteRoleInput{RoleName: aws.String(roleName)})
	_, err = admin.AttachRolePolicy(&iam.AttachRolePolicyInput{RoleName: aws.String(roleName), PolicyArn: policy.Policy.Arn})
	require.NoError(t, err)
	defer admin.DetachRolePolicy(&iam.DetachRolePolicyInput{RoleName: aws.String(roleName), PolicyArn: policy.Policy.Arn})
	roleArn := aws.StringValue(role.Role.Arn)

	t.Run("the trusted subject gets credentials scoped to the attached policy", func(t *testing.T) {
		creds, failure := assumeWithWebIdentity(t, roleArn, issuer.token(t, webIdentitySubject, issuer.key))
		require.NotNil(t, creds, "AssumeRoleWithWebIdentity refused the trusted subject: %s", failure)
		client := s3ClientWith(t, creds)
		_, err := client.PutObject(&s3.PutObjectInput{Bucket: aws.String(bucket), Key: aws.String("federated.txt")})
		assert.NoError(t, err, "the session cannot write the bucket its policy grants")
		_, err = client.CreateBucket(&s3.CreateBucketInput{Bucket: aws.String(bucket + "-other")})
		assert.Error(t, err, "the session created a bucket its policy does not grant")
	})

	refusals := map[string]string{
		"another subject": issuer.token(t, "spiffe://example.org/ns/other/sa/other", issuer.key),
		"a token signed by another key": func() string {
			other, err := rsa.GenerateKey(rand.Reader, 2048)
			require.NoError(t, err)
			return issuer.token(t, webIdentitySubject, other)
		}(),
		"an unsigned token": issuer.unsignedToken(t, webIdentitySubject),
		// The provider accepts only its registered client IDs as audiences.
		"a token issued for another audience": issuer.tokenForAudience(t, webIdentitySubject, "some-other-service"),
	}
	for name, token := range refusals {
		t.Run("refuses "+name, func(t *testing.T) {
			creds, _ := assumeWithWebIdentity(t, roleArn, token)
			assert.Nil(t, creds, "AssumeRoleWithWebIdentity issued credentials for %s", name)
		})
	}

	t.Run("an updated trust policy takes effect", func(t *testing.T) {
		moved := "spiffe://example.org/ns/app/sa/moved"
		_, err := admin.UpdateAssumeRolePolicy(&iam.UpdateAssumeRolePolicyInput{
			RoleName: aws.String(roleName), PolicyDocument: aws.String(trustPolicyFor(issuer.server.URL, moved)),
		})
		require.NoError(t, err)
		creds, _ := assumeWithWebIdentity(t, roleArn, issuer.token(t, webIdentitySubject, issuer.key))
		assert.Nil(t, creds, "the subject removed from the trust policy still assumes the role")
		creds, failure := assumeWithWebIdentity(t, roleArn, issuer.token(t, moved, issuer.key))
		assert.NotNil(t, creds, "the subject added to the trust policy was refused: %s", failure)
	})

	t.Run("a session does not survive its role being deleted and created again", func(t *testing.T) {
		trust := trustPolicyFor(issuer.server.URL, webIdentitySubject)
		_, err := admin.UpdateAssumeRolePolicy(&iam.UpdateAssumeRolePolicyInput{RoleName: aws.String(roleName), PolicyDocument: aws.String(trust)})
		require.NoError(t, err)
		before, failure := assumeWithWebIdentity(t, roleArn, issuer.token(t, webIdentitySubject, issuer.key))
		require.NotNil(t, before, "precondition: %s", failure)

		_, err = admin.DetachRolePolicy(&iam.DetachRolePolicyInput{RoleName: aws.String(roleName), PolicyArn: policy.Policy.Arn})
		require.NoError(t, err)
		_, err = admin.DeleteRole(&iam.DeleteRoleInput{RoleName: aws.String(roleName)})
		require.NoError(t, err)
		recreated, err := admin.CreateRole(&iam.CreateRoleInput{RoleName: aws.String(roleName), AssumeRolePolicyDocument: aws.String(trust)})
		require.NoError(t, err)
		_, err = admin.AttachRolePolicy(&iam.AttachRolePolicyInput{RoleName: aws.String(roleName), PolicyArn: policy.Policy.Arn})
		require.NoError(t, err)
		assert.NotEqual(t, aws.StringValue(role.Role.RoleId), aws.StringValue(recreated.Role.RoleId), "the recreated role reuses the deleted role's ID")

		_, err = s3ClientWith(t, before).PutObject(&s3.PutObjectInput{Bucket: aws.String(bucket), Key: aws.String("revived.txt")})
		assert.Error(t, err, "a session of the deleted role works again under the new role of the same name")
		after, failure := assumeWithWebIdentity(t, roleArn, issuer.token(t, webIdentitySubject, issuer.key))
		require.NotNil(t, after, "the new role refuses its trusted subject: %s", failure)
		_, err = s3ClientWith(t, after).PutObject(&s3.PutObjectInput{Bucket: aws.String(bucket), Key: aws.String("fresh.txt")})
		assert.NoError(t, err, "a session of the new role cannot use its policy")
	})
}
