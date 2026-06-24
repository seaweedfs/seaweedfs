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

// A role's trust policy is the authority on who may assume it, so a non-admin
// caller can assume a role its trust policy admits without holding the Admin
// action or an identity-side sts:AssumeRole grant (which legacy static
// identities cannot express).
func TestAssumeRole_NonAdminCallerAuthorizedByTrustPolicy(t *testing.T) {
	ctx := context.Background()
	manager := newTestSTSIntegrationManager(t)

	require.NoError(t, manager.CreatePolicy(ctx, "", "WarehouseAccess", &policy.PolicyDocument{
		Version: "2012-10-17",
		Statement: []policy.Statement{{
			Effect:   "Allow",
			Action:   []string{"s3:*"},
			Resource: []string{"arn:aws:s3:::*", "arn:aws:s3:::*/*"},
		}},
	}))

	const accessKey, secretKey = "lakekeeperkey", "lakekeepersecret"
	iam := &IdentityAccessManagement{iamIntegration: NewS3IAMIntegration(manager, "")}
	require.NoError(t, iam.loadS3ApiConfiguration(&iam_pb.S3ApiConfiguration{
		Identities: []*iam_pb.Identity{{
			Name:        "lakekeeper",
			Credentials: []*iam_pb.Credential{{AccessKey: accessKey, SecretKey: secretKey}},
			Actions:     []string{"Read", "Write", "List", "Tagging"},
		}},
	}))
	stsHandlers := NewSTSHandlers(manager.GetSTSService(), iam)

	assume := func(t *testing.T, roleName string) *httptest.ResponseRecorder {
		t.Helper()
		body := url.Values{
			"Action":          {"AssumeRole"},
			"Version":         {"2011-06-15"},
			"RoleArn":         {"arn:aws:iam::" + defaultAccountID + ":role/" + roleName},
			"RoleSessionName": {"lakekeeper-session"},
		}.Encode()
		req, err := newTestRequest(http.MethodPost, "http://sts.seaweedfs.test/", int64(len(body)), strings.NewReader(body))
		require.NoError(t, err)
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		require.NoError(t, signRequestV4(req, accessKey, secretKey))
		rec := httptest.NewRecorder()
		stsHandlers.handleAssumeRole(rec, req)
		return rec
	}

	t.Run("trust policy admits the caller", func(t *testing.T) {
		require.NoError(t, manager.CreateRole(ctx, "", "OpenWarehouse", &integration.RoleDefinition{
			RoleName: "OpenWarehouse",
			TrustPolicy: &policy.PolicyDocument{
				Version:   "2012-10-17",
				Statement: []policy.Statement{{Effect: "Allow", Principal: "*", Action: []string{"sts:AssumeRole"}}},
			},
			AttachedPolicies: []string{"WarehouseAccess"},
		}))

		rec := assume(t, "OpenWarehouse")
		require.Equal(t, http.StatusOK, rec.Code, "non-admin caller should assume a role its trust policy admits: %s", rec.Body.String())

		var resp AssumeRoleResponse
		require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
		require.NotEmpty(t, resp.Result.Credentials.SessionToken)

		session, err := manager.GetSTSService().ValidateSessionToken(ctx, resp.Result.Credentials.SessionToken)
		require.NoError(t, err)
		assert.Equal(t, []string{"WarehouseAccess"}, session.Policies, "session is scoped to the role, not the caller")
	})

	t.Run("trust policy rejects the caller", func(t *testing.T) {
		require.NoError(t, manager.CreateRole(ctx, "", "PrivateWarehouse", &integration.RoleDefinition{
			RoleName: "PrivateWarehouse",
			TrustPolicy: &policy.PolicyDocument{
				Version: "2012-10-17",
				Statement: []policy.Statement{{
					Effect:    "Allow",
					Principal: map[string]interface{}{"AWS": "arn:aws:iam::" + defaultAccountID + ":user/someone-else"},
					Action:    []string{"sts:AssumeRole"},
				}},
			},
			AttachedPolicies: []string{"WarehouseAccess"},
		}))

		rec := assume(t, "PrivateWarehouse")
		assert.Equal(t, http.StatusForbidden, rec.Code, "caller not named by the trust policy must be denied")
	})
}
