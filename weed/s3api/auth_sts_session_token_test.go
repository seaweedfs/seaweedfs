package s3api

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/seaweedfs/seaweedfs/weed/iam/sts"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3err"
)

// sessionIdFromToken reads the session id out of a session token (or a
// presigned URL's X-Amz-Security-Token) without any key.
func sessionIdFromToken(t *testing.T, token string) string {
	t.Helper()
	parts := strings.Split(token, ".")
	require.Len(t, parts, 3)
	payload, err := base64.RawURLEncoding.DecodeString(parts[1])
	require.NoError(t, err)
	var claims map[string]interface{}
	require.NoError(t, json.Unmarshal(payload, &claims))
	sid, ok := claims["sid"].(string)
	require.True(t, ok, "session token should carry a sid claim")
	return sid
}

// issueTestSession mints an STS session the way AssumeRoleWithWebIdentity does.
func issueTestSession(t *testing.T, stsService *sts.STSService, config *sts.STSConfig) (*sts.SessionInfo, string) {
	t.Helper()
	sessionId, err := sts.GenerateSessionId()
	require.NoError(t, err)
	claims := sts.NewSTSSessionClaims(sessionId, config.Issuer, time.Now().Add(time.Hour)).
		WithSessionName("alice-session").
		WithRoleInfo("arn:aws:iam::role/AppRole",
			"arn:aws:sts::assumed-role/AppRole/alice-session",
			"arn:aws:sts::assumed-role/AppRole/alice-session")
	token, err := sts.NewTokenGenerator(config.SigningKey, config.Issuer).GenerateJWTWithClaims(claims)
	require.NoError(t, err)
	sessionInfo, err := stsService.ValidateSessionToken(context.Background(), token)
	require.NoError(t, err)
	return sessionInfo, token
}

// testIdentityFromSessionToken builds the identity for a validated STS session
// token. Session tokens are not bearer credentials; tests use this to reach
// the authorization layer the way a verified SigV4 request would.
func testIdentityFromSessionToken(t *testing.T, s3iam *S3IAMIntegration, sessionToken string) *IAMIdentity {
	t.Helper()
	sessionInfo, err := s3iam.stsService.ValidateSessionToken(context.Background(), sessionToken)
	require.NoError(t, err)
	claims := make(map[string]interface{}, len(sessionInfo.RequestContext)+4)
	for k, v := range sessionInfo.RequestContext {
		claims[k] = v
	}
	claims["sub"] = sessionInfo.Subject
	claims["role"] = sessionInfo.RoleArn
	claims["principal"] = sessionInfo.Principal
	claims["snam"] = sessionInfo.SessionName
	return &IAMIdentity{
		Name:         sessionInfo.Subject,
		Principal:    sessionInfo.Principal,
		SessionToken: sessionToken,
		Account: &Account{
			DisplayName:  sessionInfo.SessionName,
			EmailAddress: sessionInfo.Subject + "@seaweedfs.local",
			Id:           sessionInfo.Subject,
		},
		Claims: claims,
	}
}

// A presigned URL discloses the session token in X-Amz-Security-Token. Whoever
// holds it must not be able to reconstruct the temporary credential or turn it
// into a standalone credential.
func TestSTSSessionTokenDoesNotRevealCredential(t *testing.T) {
	stsService, config := setupTestSTSService(t)
	iam := NewIdentityAccessManagementWithStore(&S3ApiServerOption{}, nil, "memory")
	s3iam := &S3IAMIntegration{stsService: stsService, enabled: true}
	iam.SetIAMIntegration(s3iam)

	sessionInfo, sessionToken := issueTestSession(t, stsService, config)

	// control: the issued credential verifies
	req, err := newTestRequest(http.MethodGet, "https://example.com/reports/data.csv", 0, nil)
	require.NoError(t, err)
	req.Header.Set("X-Amz-Security-Token", sessionToken)
	require.NoError(t, signRequestV4(req, sessionInfo.Credentials.AccessKeyId, sessionInfo.Credentials.SecretAccessKey))
	_, errCode := iam.reqSignatureV4Verify(req)
	require.Equal(t, s3err.ErrNone, errCode)

	// attack: recompute the credential from the token's public claims
	sid := sessionIdFromToken(t, sessionToken)
	akHash := sha256.Sum256([]byte("access-key:" + sid))
	accessKey := "ASIA" + hex.EncodeToString(akHash[:8])
	skHash := sha256.Sum256([]byte("secret-key:" + sid))
	secretKey := base64.StdEncoding.EncodeToString(skHash[:])

	forged, err := newTestRequest(http.MethodDelete, "https://example.com/reports/payroll.csv", 0, nil)
	require.NoError(t, err)
	forged.Header.Set("X-Amz-Security-Token", sessionToken)
	require.NoError(t, signRequestV4(forged, accessKey, secretKey))
	_, errCode = iam.reqSignatureV4Verify(forged)
	require.NotEqual(t, s3err.ErrNone, errCode,
		"a request signed with a credential derived from the session token must not verify")

	// attack: replay the token itself as a bearer credential
	bearerReq := httptest.NewRequest(http.MethodDelete, "/reports/payroll.csv", http.NoBody)
	bearerReq.Header.Set("Authorization", "Bearer "+sessionToken)
	_, errCode = s3iam.AuthenticateJWT(context.Background(), bearerReq)
	require.NotEqual(t, s3err.ErrNone, errCode,
		"an STS session token must not authenticate as a bearer token")
}
