package s3api

import (
	"context"
	"encoding/xml"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/seaweedfs/seaweedfs/weed/iam/integration"
	"github.com/seaweedfs/seaweedfs/weed/iam/policy"
	"github.com/seaweedfs/seaweedfs/weed/pb/iam_pb"
)

// An admin that assumes a named role gets the role's permissions, not its own:
// the is_admin claim bypasses base policy evaluation, so carrying it into a
// role session would silently ignore the role's attached policies.
func TestAssumeRole_AdminCallerScopedToRolePolicies(t *testing.T) {
	ctx := context.Background()
	manager := newTestSTSIntegrationManager(t)
	manager.SetSessionRevocationStore(integration.NewMemorySessionRevocationStore())

	require.NoError(t, manager.CreatePolicy(ctx, "", "ReadOnlyPolicy", &policy.PolicyDocument{
		Version: "2012-10-17",
		Statement: []policy.Statement{{
			Effect:   "Allow",
			Action:   []string{"s3:GetObject"},
			Resource: []string{"arn:aws:s3:::*/*"},
		}},
	}))
	require.NoError(t, manager.CreateRole(ctx, "", "ReadOnlyRole", &integration.RoleDefinition{
		RoleName: "ReadOnlyRole",
		TrustPolicy: &policy.PolicyDocument{
			Version:   "2012-10-17",
			Statement: []policy.Statement{{Effect: "Allow", Principal: "*", Action: []string{"sts:AssumeRole"}}},
		},
		AttachedPolicies: []string{"ReadOnlyPolicy"},
	}))

	const accessKey, secretKey = "adminkey", "adminsecret"
	iam := &IdentityAccessManagement{iamIntegration: NewS3IAMIntegration(manager, "")}
	require.NoError(t, iam.loadS3ApiConfiguration(&iam_pb.S3ApiConfiguration{
		Identities: []*iam_pb.Identity{{
			Name:        "admin",
			Credentials: []*iam_pb.Credential{{AccessKey: accessKey, SecretKey: secretKey}},
			Actions:     []string{"Admin"},
		}},
	}))

	body := url.Values{
		"Action":          {"AssumeRole"},
		"Version":         {"2011-06-15"},
		"RoleArn":         {"arn:aws:iam::" + defaultAccountID + ":role/ReadOnlyRole"},
		"RoleSessionName": {"admin-session"},
	}.Encode()
	req, err := newTestRequest(http.MethodPost, "http://sts.seaweedfs.test/", int64(len(body)), strings.NewReader(body))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	require.NoError(t, signRequestV4(req, accessKey, secretKey))

	rec := httptest.NewRecorder()
	NewSTSHandlers(manager.GetSTSService(), iam).handleAssumeRole(rec, req)
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

	var resp AssumeRoleResponse
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
	sessionToken := resp.Result.Credentials.SessionToken
	require.NotEmpty(t, sessionToken)

	session, err := manager.GetSTSService().ValidateSessionToken(ctx, sessionToken)
	require.NoError(t, err)
	assert.NotContains(t, session.RequestContext, "is_admin", "a role session must not inherit the caller's admin standing")

	allowed := func(action, resource string) bool {
		t.Helper()
		ok, err := manager.IsActionAllowed(ctx, &integration.ActionRequest{
			Principal:      session.Principal,
			Action:         action,
			Resource:       resource,
			SessionToken:   sessionToken,
			RequestContext: session.RequestContext,
			PolicyNames:    session.Policies,
		})
		require.NoError(t, err)
		return ok
	}

	assert.True(t, allowed("s3:GetObject", "arn:aws:s3:::bucket/key"), "the role's own policy still applies")
	assert.False(t, allowed("s3:DeleteBucket", "arn:aws:s3:::bucket"), "the role's policy bounds the session")
}
