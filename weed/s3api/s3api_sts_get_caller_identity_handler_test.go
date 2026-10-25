package s3api

import (
	"context"
	"encoding/xml"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/iam/integration"
	"github.com/seaweedfs/seaweedfs/weed/iam/policy"
	"github.com/seaweedfs/seaweedfs/weed/pb/iam_pb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// GetCallerIdentity is what an AWS SDK calls first to discover who it is, but
// the handler had no test at all - only XML marshalling. Nothing pinned that a
// caller presenting session credentials is reported as the assumed role rather
// than the user who minted the session, which is the answer operators rely on
// to tell two sessions of the same role apart.
func TestGetCallerIdentityHandler(t *testing.T) {
	ctx := context.Background()
	manager := newTestSTSIntegrationManager(t)

	require.NoError(t, manager.CreatePolicy(ctx, "", "CallerPolicy", &policy.PolicyDocument{
		Version: "2012-10-17",
		Statement: []policy.Statement{{
			Effect:   "Allow",
			Action:   []string{"s3:*"},
			Resource: []string{"arn:aws:s3:::*", "arn:aws:s3:::*/*"},
		}},
	}))
	require.NoError(t, manager.CreateRole(ctx, "", "CallerRole", &integration.RoleDefinition{
		RoleName: "CallerRole",
		TrustPolicy: &policy.PolicyDocument{
			Version:   "2012-10-17",
			Statement: []policy.Statement{{Effect: "Allow", Principal: "*", Action: []string{"sts:AssumeRole"}}},
		},
		AttachedPolicies: []string{"CallerPolicy"},
	}))

	const accessKey, secretKey = "callerkey", "callersecret"
	iam := &IdentityAccessManagement{iamIntegration: NewS3IAMIntegration(manager, "")}
	require.NoError(t, iam.loadS3ApiConfiguration(&iam_pb.S3ApiConfiguration{
		Identities: []*iam_pb.Identity{{
			Name:        "alice",
			Credentials: []*iam_pb.Credential{{AccessKey: accessKey, SecretKey: secretKey}},
			Actions:     []string{"Admin"},
		}},
	}))
	handlers := NewSTSHandlers(manager.GetSTSService(), iam)

	// newSignedSTSRequest builds a form-encoded STS POST and signs it with the
	// given credentials, optionally carrying a session token.
	newSignedSTSRequest := func(t *testing.T, form url.Values, ak, sk, sessionToken string) *http.Request {
		t.Helper()
		body := form.Encode()
		req, err := newTestRequest(http.MethodPost, "http://sts.seaweedfs.test/", int64(len(body)), strings.NewReader(body))
		require.NoError(t, err)
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		if sessionToken != "" {
			req.Header.Set("X-Amz-Security-Token", sessionToken)
		}
		require.NoError(t, signRequestV4(req, ak, sk))
		return req
	}

	callerIdentityForm := url.Values{
		"Action":  {"GetCallerIdentity"},
		"Version": {"2011-06-15"},
	}

	t.Run("static credentials report the user ARN", func(t *testing.T) {
		req := newSignedSTSRequest(t, callerIdentityForm, accessKey, secretKey, "")
		rec := httptest.NewRecorder()
		handlers.HandleSTSRequest(rec, req)
		require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

		var resp GetCallerIdentityResponse
		require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
		assert.Equal(t, "arn:aws:iam::"+defaultAccountID+":user/alice", resp.Result.Arn)
		assert.Equal(t, "alice", resp.Result.UserId)
		assert.Equal(t, defaultAccountID, resp.Result.Account)
	})

	t.Run("session credentials report the assumed role, not the minting user", func(t *testing.T) {
		// Mint a session as alice, then ask who we are with the session creds.
		assumeReq := newSignedSTSRequest(t, url.Values{
			"Action":          {"AssumeRole"},
			"Version":         {"2011-06-15"},
			"RoleArn":         {"arn:aws:iam::" + defaultAccountID + ":role/CallerRole"},
			"RoleSessionName": {"caller-session"},
		}, accessKey, secretKey, "")
		assumeRec := httptest.NewRecorder()
		handlers.HandleSTSRequest(assumeRec, assumeReq)
		require.Equal(t, http.StatusOK, assumeRec.Code, assumeRec.Body.String())

		var assumed AssumeRoleResponse
		require.NoError(t, xml.Unmarshal(assumeRec.Body.Bytes(), &assumed))
		creds := assumed.Result.Credentials
		require.NotEmpty(t, creds.SessionToken)

		req := newSignedSTSRequest(t, callerIdentityForm,
			creds.AccessKeyId, creds.SecretAccessKey, creds.SessionToken)
		rec := httptest.NewRecorder()
		handlers.HandleSTSRequest(rec, req)
		require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

		var resp GetCallerIdentityResponse
		require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
		assert.Equal(t, "arn:aws:sts::"+defaultAccountID+":assumed-role/CallerRole/caller-session", resp.Result.Arn,
			"session credentials must identify the assumed role and session, not alice")
		assert.NotContains(t, resp.Result.Arn, "alice")
	})

	t.Run("bad signature is denied", func(t *testing.T) {
		req := newSignedSTSRequest(t, callerIdentityForm, accessKey, "wrong-secret", "")
		rec := httptest.NewRecorder()
		handlers.HandleSTSRequest(rec, req)
		assert.Equal(t, http.StatusForbidden, rec.Code)
	})

	t.Run("unsigned request is denied", func(t *testing.T) {
		body := callerIdentityForm.Encode()
		req := httptest.NewRequest(http.MethodPost, "http://sts.seaweedfs.test/", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		rec := httptest.NewRecorder()
		handlers.HandleSTSRequest(rec, req)
		assert.Equal(t, http.StatusForbidden, rec.Code)
	})
}
