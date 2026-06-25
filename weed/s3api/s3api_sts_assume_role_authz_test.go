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
	manager.SetSessionRevocationStore(integration.NewMemorySessionRevocationStore())

	require.NoError(t, manager.CreatePolicy(ctx, "", "WarehouseAccess", &policy.PolicyDocument{
		Version: "2012-10-17",
		Statement: []policy.Statement{{
			Effect:   "Allow",
			Action:   []string{"s3:*"},
			Resource: []string{"arn:aws:s3:::*", "arn:aws:s3:::*/*"},
		}},
	}))
	require.NoError(t, manager.CreatePolicy(ctx, "", "DenyAssumeRole", &policy.PolicyDocument{
		Version: "2012-10-17",
		Statement: []policy.Statement{{
			Effect:   "Deny",
			Action:   []string{"sts:AssumeRole"},
			Resource: []string{"*"},
		}},
	}))

	const accessKey, secretKey = "lakekeeperkey", "lakekeepersecret"
	const denyAccessKey, denySecretKey = "deniedkey", "deniedsecret"
	iam := &IdentityAccessManagement{iamIntegration: NewS3IAMIntegration(manager, "")}
	require.NoError(t, iam.loadS3ApiConfiguration(&iam_pb.S3ApiConfiguration{
		Identities: []*iam_pb.Identity{
			{
				Name:        "lakekeeper",
				Credentials: []*iam_pb.Credential{{AccessKey: accessKey, SecretKey: secretKey}},
				Actions:     []string{"Read", "Write", "List", "Tagging"},
			},
			{
				Name:        "lakekeeper-denied",
				Credentials: []*iam_pb.Credential{{AccessKey: denyAccessKey, SecretKey: denySecretKey}},
				Actions:     []string{"Read", "Write", "List", "Tagging"},
				PolicyNames: []string{"DenyAssumeRole"},
			},
		},
	}))
	stsHandlers := NewSTSHandlers(manager.GetSTSService(), iam)

	assume := func(t *testing.T, ak, sk, roleName string) *httptest.ResponseRecorder {
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
		require.NoError(t, signRequestV4(req, ak, sk))
		rec := httptest.NewRecorder()
		stsHandlers.handleAssumeRole(rec, req)
		return rec
	}

	// assumeWithSessionCreds chains: it signs an AssumeRole request with temporary
	// session credentials and forwards the session token (role chaining).
	assumeWithSessionCreds := func(t *testing.T, creds STSCredentials, roleName string) *httptest.ResponseRecorder {
		t.Helper()
		body := url.Values{
			"Action":          {"AssumeRole"},
			"Version":         {"2011-06-15"},
			"RoleArn":         {"arn:aws:iam::" + defaultAccountID + ":role/" + roleName},
			"RoleSessionName": {"chained"},
		}.Encode()
		req, err := newTestRequest(http.MethodPost, "http://sts.seaweedfs.test/", int64(len(body)), strings.NewReader(body))
		require.NoError(t, err)
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		req.Header.Set("X-Amz-Security-Token", creds.SessionToken)
		require.NoError(t, signRequestV4(req, creds.AccessKeyId, creds.SecretAccessKey))
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

		rec := assume(t, accessKey, secretKey, "OpenWarehouse")
		require.Equal(t, http.StatusOK, rec.Code, "non-admin caller should assume a role its trust policy admits: %s", rec.Body.String())

		var resp AssumeRoleResponse
		require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
		require.NotEmpty(t, resp.Result.Credentials.SessionToken)

		session, err := manager.GetSTSService().ValidateSessionToken(ctx, resp.Result.Credentials.SessionToken)
		require.NoError(t, err)
		assert.Equal(t, []string{"WarehouseAccess"}, session.Policies, "session is scoped to the role, not the caller")
	})

	t.Run("trust policy admits a specific principal", func(t *testing.T) {
		require.NoError(t, manager.CreateRole(ctx, "", "NamedWarehouse", &integration.RoleDefinition{
			RoleName: "NamedWarehouse",
			TrustPolicy: &policy.PolicyDocument{
				Version: "2012-10-17",
				Statement: []policy.Statement{{
					Effect:    "Allow",
					Principal: map[string]interface{}{"AWS": "arn:aws:iam::" + defaultAccountID + ":user/lakekeeper"},
					Action:    []string{"sts:AssumeRole"},
				}},
			},
			AttachedPolicies: []string{"WarehouseAccess"},
		}))

		rec := assume(t, accessKey, secretKey, "NamedWarehouse")
		require.Equal(t, http.StatusOK, rec.Code, "caller named by the trust policy should be admitted: %s", rec.Body.String())
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

		rec := assume(t, accessKey, secretKey, "PrivateWarehouse")
		assert.Equal(t, http.StatusForbidden, rec.Code, "caller not named by the trust policy must be denied")
	})

	t.Run("identity policy explicit deny wins over trust policy", func(t *testing.T) {
		require.NoError(t, manager.CreateRole(ctx, "", "DenyTestWarehouse", &integration.RoleDefinition{
			RoleName: "DenyTestWarehouse",
			TrustPolicy: &policy.PolicyDocument{
				Version:   "2012-10-17",
				Statement: []policy.Statement{{Effect: "Allow", Principal: "*", Action: []string{"sts:AssumeRole"}}},
			},
			AttachedPolicies: []string{"WarehouseAccess"},
		}))

		// Caller is admitted by the trust policy but has an attached identity
		// policy that explicitly denies sts:AssumeRole; the deny must win.
		rec := assume(t, denyAccessKey, denySecretKey, "DenyTestWarehouse")
		assert.Equal(t, http.StatusForbidden, rec.Code, "explicit identity-side deny must block AssumeRole even when the trust policy admits the caller")
	})

	t.Run("session policy explicit deny blocks role chaining", func(t *testing.T) {
		require.NoError(t, manager.CreateRole(ctx, "", "ChainWarehouse", &integration.RoleDefinition{
			RoleName: "ChainWarehouse",
			TrustPolicy: &policy.PolicyDocument{
				Version:   "2012-10-17",
				Statement: []policy.Statement{{Effect: "Allow", Principal: "*", Action: []string{"sts:AssumeRole"}}},
			},
			AttachedPolicies: []string{"WarehouseAccess"},
		}))
		chainArn := "arn:aws:iam::" + defaultAccountID + ":role/ChainWarehouse"

		// First hop succeeds, with a session policy that denies sts:AssumeRole.
		denySession := `{"Version":"2012-10-17","Statement":[{"Effect":"Deny","Action":"sts:AssumeRole","Resource":"*"}]}`
		body := url.Values{
			"Action":          {"AssumeRole"},
			"Version":         {"2011-06-15"},
			"RoleArn":         {chainArn},
			"RoleSessionName": {"hop1"},
			"Policy":          {denySession},
		}.Encode()
		req, err := newTestRequest(http.MethodPost, "http://sts.seaweedfs.test/", int64(len(body)), strings.NewReader(body))
		require.NoError(t, err)
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		require.NoError(t, signRequestV4(req, accessKey, secretKey))
		rec := httptest.NewRecorder()
		stsHandlers.handleAssumeRole(rec, req)
		require.Equal(t, http.StatusOK, rec.Code, "first hop should succeed: %s", rec.Body.String())
		var hop1 AssumeRoleResponse
		require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &hop1))
		require.NotEmpty(t, hop1.Result.Credentials.SessionToken)

		// Second hop reuses the session credentials to chain-assume; the session
		// policy's explicit deny must block it even though the trust policy admits.
		rec2 := assumeWithSessionCreds(t, hop1.Result.Credentials, "ChainWarehouse")
		assert.Equal(t, http.StatusForbidden, rec2.Code, "session policy explicit deny must block role chaining: %s", rec2.Body.String())
	})

	t.Run("revoked chained session cannot assume", func(t *testing.T) {
		require.NoError(t, manager.CreateRole(ctx, "", "RevokeWarehouse", &integration.RoleDefinition{
			RoleName: "RevokeWarehouse",
			TrustPolicy: &policy.PolicyDocument{
				Version:   "2012-10-17",
				Statement: []policy.Statement{{Effect: "Allow", Principal: "*", Action: []string{"sts:AssumeRole"}}},
			},
			AttachedPolicies: []string{"WarehouseAccess"},
		}))

		rec := assume(t, accessKey, secretKey, "RevokeWarehouse")
		require.Equal(t, http.StatusOK, rec.Code, "first hop should succeed: %s", rec.Body.String())
		var hop1 AssumeRoleResponse
		require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &hop1))

		// Revoke the session, then chaining with it must be blocked.
		session, err := manager.GetSTSService().ValidateSessionToken(ctx, hop1.Result.Credentials.SessionToken)
		require.NoError(t, err)
		require.NotEmpty(t, session.SessionId)
		require.NoError(t, manager.RevokeSession(ctx, session.SessionId, session.ExpiresAt, "test"))

		rec2 := assumeWithSessionCreds(t, hop1.Result.Credentials, "RevokeWarehouse")
		assert.Equal(t, http.StatusForbidden, rec2.Code, "a revoked session must not be able to chain-assume")
	})
}

func TestCallerPrincipalArn(t *testing.T) {
	h := &STSHandlers{}
	assert.Equal(t, "arn:aws:iam::"+defaultAccountID+":user/lakekeeper",
		h.callerPrincipalArn(&Identity{Name: "lakekeeper"}),
		"synthesizes the canonical user ARN when one is not set")
	assert.Equal(t, "arn:aws:sts::111122223333:assumed-role/Warehouse/sess",
		h.callerPrincipalArn(&Identity{Name: "lakekeeper", PrincipalArn: "arn:aws:sts::111122223333:assumed-role/Warehouse/sess"}),
		"keeps an explicit principal ARN")
}
