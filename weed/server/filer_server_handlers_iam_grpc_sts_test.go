package weed_server

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/credential"
	"github.com/seaweedfs/seaweedfs/weed/iam/integration"
	"github.com/seaweedfs/seaweedfs/weed/pb/iam_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/policy_engine"
	"github.com/seaweedfs/seaweedfs/weed/security"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const stsTestTrust = `{"Version":"2012-10-17","Statement":[{"Effect":"Allow",` +
	`"Principal":{"Federated":"https://oidc.example"},"Action":["sts:AssumeRoleWithWebIdentity"],` +
	`"Condition":{"StringEquals":{"oidc:sub":"spiffe://example.org/ns/app/sa/app"}}}]}`

func newSTSTestServer(t *testing.T) (*IamGrpcServer, context.Context, *integration.MemoryOIDCProviderStore, *integration.MemoryRoleStore) {
	t.Helper()
	s := newTestIamGrpcServer(t)
	providers, roles := integration.NewMemoryOIDCProviderStore(), integration.NewMemoryRoleStore()
	s.SetSTSStores(providers, roles)
	doc := policy_engine.PolicyDocument{Version: "2012-10-17", Statement: []policy_engine.PolicyStatement{{
		Effect:   policy_engine.PolicyEffectAllow,
		Action:   policy_engine.NewStringOrStringSlice("s3:GetObject"),
		Resource: policy_engine.NewStringOrStringSlicePtr("arn:aws:s3:::bucket/*"),
	}}}
	require.NoError(t, s.credentialManager.CreatePolicy(context.Background(), "read-bucket", doc))
	ctx := ctxWithBearer(string(security.GenJwtForFilerAdmin(security.SigningKey(testIamSigningKey), 60)))
	return s, ctx, providers, roles
}

func requireCode(t *testing.T, err error, code codes.Code) {
	t.Helper()
	require.Error(t, err, "expected %s", code)
	assert.Equal(t, code, status.Code(err), "error: %v", err)
}

func TestIamGrpc_OIDCProviderPutGetListDelete(t *testing.T) {
	s, ctx, _, _ := newSTSTestServer(t)
	put, err := s.PutOIDCProvider(ctx, &iam_pb.PutOIDCProviderRequest{
		IssuerUrl: "https://oidc.example", ClientIds: []string{"aud"}, AccountId: "111122223333",
	})
	require.NoError(t, err)
	assert.Equal(t, "arn:aws:iam::111122223333:oidc-provider/oidc.example", put.Arn)

	got, err := s.GetOIDCProvider(ctx, &iam_pb.GetOIDCProviderRequest{IssuerUrl: "https://oidc.example", AccountId: "111122223333"})
	require.NoError(t, err)
	assert.Equal(t, []string{"aud"}, got.Provider.ClientIds)

	list, err := s.ListOIDCProviders(ctx, &iam_pb.ListOIDCProvidersRequest{})
	require.NoError(t, err)
	require.Len(t, list.Providers, 1)

	_, err = s.DeleteOIDCProvider(ctx, &iam_pb.DeleteOIDCProviderRequest{IssuerUrl: "https://oidc.example", AccountId: "111122223333"})
	require.NoError(t, err)
	_, err = s.GetOIDCProvider(ctx, &iam_pb.GetOIDCProviderRequest{IssuerUrl: "https://oidc.example", AccountId: "111122223333"})
	requireCode(t, err, codes.NotFound)
	_, err = s.DeleteOIDCProvider(ctx, &iam_pb.DeleteOIDCProviderRequest{IssuerUrl: "https://oidc.example", AccountId: "111122223333"})
	requireCode(t, err, codes.NotFound)
}

// Put replaces what the request carries and keeps what it cannot express.
func TestIamGrpc_PutOIDCProviderKeepsWhatTheRequestCannotCarry(t *testing.T) {
	s, ctx, providers, _ := newSTSTestServer(t)
	created := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	arn, err := integration.DeriveOIDCProviderARN("", "https://oidc.example")
	require.NoError(t, err)
	require.NoError(t, providers.StoreProvider(context.Background(), "", &integration.OIDCProviderRecord{
		ARN: arn, URL: "https://oidc.example", ClientIDs: []string{"old"},
		Tags: map[string]string{"team": "infra"}, PolicyClaim: "policy", CreatedAt: created,
	}))

	_, err = s.PutOIDCProvider(ctx, &iam_pb.PutOIDCProviderRequest{IssuerUrl: "https://oidc.example", ClientIds: []string{"new"}})
	require.NoError(t, err)
	rec, err := providers.GetProviderByARN(context.Background(), "", arn)
	require.NoError(t, err)
	assert.Equal(t, []string{"new"}, rec.ClientIDs)
	assert.Equal(t, map[string]string{"team": "infra"}, rec.Tags)
	assert.Equal(t, "policy", rec.PolicyClaim)
	assert.True(t, rec.CreatedAt.Equal(created))
}

func TestIamGrpc_RolePutGetListDelete(t *testing.T) {
	s, ctx, _, roles := newSTSTestServer(t)
	put, err := s.PutRole(ctx, &iam_pb.PutRoleRequest{Role: &iam_pb.Role{
		RoleName: "app", TrustPolicy: stsTestTrust, AttachedPolicies: []string{"read-bucket"}, MaxSessionDuration: 3600,
	}})
	require.NoError(t, err)
	assert.Equal(t, "arn:aws:iam::role/app", put.RoleArn)

	stored, err := roles.GetRole(context.Background(), "", "app")
	require.NoError(t, err)
	assert.Equal(t, []string{"read-bucket"}, stored.AttachedPolicies)
	assert.False(t, stored.CreatedAt.IsZero())

	got, err := s.GetRole(ctx, &iam_pb.GetRoleRequest{RoleName: "app"})
	require.NoError(t, err)
	assert.Contains(t, got.Role.TrustPolicy, "spiffe://example.org/ns/app/sa/app")

	// Replacing keeps CreatedAt.
	_, err = s.PutRole(ctx, &iam_pb.PutRoleRequest{Role: &iam_pb.Role{RoleName: "app", TrustPolicy: stsTestTrust}})
	require.NoError(t, err)
	replaced, err := roles.GetRole(context.Background(), "", "app")
	require.NoError(t, err)
	assert.True(t, replaced.CreatedAt.Equal(stored.CreatedAt))
	assert.Empty(t, replaced.AttachedPolicies)
	assert.NotEmpty(t, stored.RoleId, "a created role has an ID")
	assert.Equal(t, stored.RoleId, replaced.RoleId, "replacing a role changed its ID")

	list, err := s.ListRoles(ctx, &iam_pb.ListRolesRequest{})
	require.NoError(t, err)
	require.Len(t, list.Roles, 1)

	_, err = s.DeleteRole(ctx, &iam_pb.DeleteRoleRequest{RoleName: "app"})
	require.NoError(t, err)
	_, err = s.GetRole(ctx, &iam_pb.GetRoleRequest{RoleName: "app"})
	requireCode(t, err, codes.NotFound)
	_, err = s.DeleteRole(ctx, &iam_pb.DeleteRoleRequest{RoleName: "app"})
	requireCode(t, err, codes.NotFound)

	_, err = s.PutRole(ctx, &iam_pb.PutRoleRequest{Role: &iam_pb.Role{RoleName: "app", TrustPolicy: stsTestTrust}})
	require.NoError(t, err)
	recreated, err := roles.GetRole(context.Background(), "", "app")
	require.NoError(t, err)
	assert.NotEqual(t, stored.RoleId, recreated.RoleId, "a role created again under a deleted role's name reuses its ID")
}

func TestIamGrpc_STSRefusals(t *testing.T) {
	s, ctx, _, _ := newSTSTestServer(t)
	cases := []struct {
		name string
		call func() error
		code codes.Code
	}{
		{"provider without client IDs", func() error {
			_, err := s.PutOIDCProvider(ctx, &iam_pb.PutOIDCProviderRequest{IssuerUrl: "https://oidc.example"})
			return err
		}, codes.InvalidArgument},
		{"provider with bad thumbprint", func() error {
			_, err := s.PutOIDCProvider(ctx, &iam_pb.PutOIDCProviderRequest{IssuerUrl: "https://oidc.example", ClientIds: []string{"a"}, Thumbprints: []string{"nope"}})
			return err
		}, codes.InvalidArgument},
		{"role without trust policy", func() error {
			_, err := s.PutRole(ctx, &iam_pb.PutRoleRequest{Role: &iam_pb.Role{RoleName: "app"}})
			return err
		}, codes.InvalidArgument},
		{"role with malformed trust policy", func() error {
			_, err := s.PutRole(ctx, &iam_pb.PutRoleRequest{Role: &iam_pb.Role{RoleName: "app", TrustPolicy: "{"}})
			return err
		}, codes.InvalidArgument},
		{"role with session out of bounds", func() error {
			_, err := s.PutRole(ctx, &iam_pb.PutRoleRequest{Role: &iam_pb.Role{RoleName: "app", TrustPolicy: stsTestTrust, MaxSessionDuration: 60}})
			return err
		}, codes.InvalidArgument},
		{"role name leaving the role store", func() error {
			_, err := s.PutRole(ctx, &iam_pb.PutRoleRequest{Role: &iam_pb.Role{RoleName: "../identities/admin", TrustPolicy: stsTestTrust}})
			return err
		}, codes.InvalidArgument},
		{"role over the policy quota", func() error {
			policies := make([]string, integration.MaxManagedPoliciesPerRole+1)
			for i := range policies {
				policies[i] = "read-bucket"
			}
			_, err := s.PutRole(ctx, &iam_pb.PutRoleRequest{Role: &iam_pb.Role{RoleName: "app", TrustPolicy: stsTestTrust, AttachedPolicies: policies}})
			return err
		}, codes.InvalidArgument},
		{"role attaching a missing policy", func() error {
			_, err := s.PutRole(ctx, &iam_pb.PutRoleRequest{Role: &iam_pb.Role{RoleName: "app", TrustPolicy: stsTestTrust, AttachedPolicies: []string{"nope"}}})
			return err
		}, codes.NotFound},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) { requireCode(t, tc.call(), tc.code) })
	}
}

type unreadableProviders struct {
	*integration.MemoryOIDCProviderStore
}

func (unreadableProviders) GetProviderByARN(context.Context, string, string) (*integration.OIDCProviderRecord, error) {
	return nil, errors.New("lookup OIDC provider: filer unavailable")
}

type unreadableRoles struct{ *integration.MemoryRoleStore }

func (unreadableRoles) GetRole(context.Context, string, string) (*integration.RoleDefinition, error) {
	return nil, errors.New("lookup role: filer unavailable")
}

// An unreadable store is not an absent entry: Put must not write over what it
// could not see (a config-file entry included), and Delete must not report
// success.
func TestIamGrpc_UnreadableStoreIsUnavailableNotAbsent(t *testing.T) {
	s, ctx, _, _ := newSTSTestServer(t)
	providers, roles := unreadableProviders{integration.NewMemoryOIDCProviderStore()}, unreadableRoles{integration.NewMemoryRoleStore()}
	s.SetSTSStores(providers, roles)

	_, err := s.PutOIDCProvider(ctx, &iam_pb.PutOIDCProviderRequest{IssuerUrl: "https://oidc.example", ClientIds: []string{"aud"}})
	requireCode(t, err, codes.Unavailable)
	_, err = s.DeleteOIDCProvider(ctx, &iam_pb.DeleteOIDCProviderRequest{IssuerUrl: "https://oidc.example"})
	requireCode(t, err, codes.Unavailable)
	_, err = s.PutRole(ctx, &iam_pb.PutRoleRequest{Role: &iam_pb.Role{RoleName: "app", TrustPolicy: stsTestTrust}})
	requireCode(t, err, codes.Unavailable)
	_, err = s.DeleteRole(ctx, &iam_pb.DeleteRoleRequest{RoleName: "app"})
	requireCode(t, err, codes.Unavailable)

	recs, err := providers.ListProviders(context.Background(), "")
	require.NoError(t, err)
	assert.Empty(t, recs, "PutOIDCProvider wrote through a store it could not read")
	names, err := roles.ListRoles(context.Background(), "")
	require.NoError(t, err)
	assert.Empty(t, names, "PutRole wrote through a store it could not read")
}

func TestIamGrpc_STSRPCsWithoutStoresAreFailedPrecondition(t *testing.T) {
	s := newTestIamGrpcServer(t)
	ctx := ctxWithBearer(string(security.GenJwtForFilerAdmin(security.SigningKey(testIamSigningKey), 60)))
	_, err := s.ListOIDCProviders(ctx, &iam_pb.ListOIDCProvidersRequest{})
	requireCode(t, err, codes.FailedPrecondition)
	_, err = s.ListRoles(ctx, &iam_pb.ListRolesRequest{})
	requireCode(t, err, codes.FailedPrecondition)
}

func TestIamGrpc_STSRPCsRequireAuth(t *testing.T) {
	s, _, _, _ := newSTSTestServer(t)
	ctx := context.Background()
	calls := map[string]func() error{
		"PutOIDCProvider": func() error { _, err := s.PutOIDCProvider(ctx, &iam_pb.PutOIDCProviderRequest{}); return err },
		"GetOIDCProvider": func() error { _, err := s.GetOIDCProvider(ctx, &iam_pb.GetOIDCProviderRequest{}); return err },
		"DeleteOIDCProvider": func() error {
			_, err := s.DeleteOIDCProvider(ctx, &iam_pb.DeleteOIDCProviderRequest{})
			return err
		},
		"ListOIDCProviders": func() error { _, err := s.ListOIDCProviders(ctx, &iam_pb.ListOIDCProvidersRequest{}); return err },
		"PutRole":           func() error { _, err := s.PutRole(ctx, &iam_pb.PutRoleRequest{}); return err },
		"GetRole":           func() error { _, err := s.GetRole(ctx, &iam_pb.GetRoleRequest{}); return err },
		"DeleteRole":        func() error { _, err := s.DeleteRole(ctx, &iam_pb.DeleteRoleRequest{}); return err },
		"ListRoles":         func() error { _, err := s.ListRoles(ctx, &iam_pb.ListRolesRequest{}); return err },
	}
	for name, call := range calls {
		t.Run(name, func(t *testing.T) { requireCode(t, call(), codes.Unauthenticated) })
	}
}

func TestIamGrpc_DeletePolicyAttachedToARoleIsRefused(t *testing.T) {
	s, ctx, _, _ := newSTSTestServer(t)
	_, err := s.PutRole(ctx, &iam_pb.PutRoleRequest{Role: &iam_pb.Role{
		RoleName: "app", TrustPolicy: stsTestTrust, AttachedPolicies: []string{"read-bucket"},
	}})
	require.NoError(t, err)

	_, err = s.DeletePolicy(ctx, &iam_pb.DeletePolicyRequest{Name: "read-bucket"})
	requireCode(t, err, codes.FailedPrecondition)

	_, err = s.PutRole(ctx, &iam_pb.PutRoleRequest{Role: &iam_pb.Role{RoleName: "app", TrustPolicy: stsTestTrust}})
	require.NoError(t, err)
	_, err = s.DeletePolicy(ctx, &iam_pb.DeletePolicyRequest{Name: "read-bucket"})
	assert.NoError(t, err, "a policy no role attaches could not be deleted")
}

func TestIamGrpc_PutOIDCProviderRequiresHTTPSExceptOnLoopback(t *testing.T) {
	s, ctx, _, _ := newSTSTestServer(t)
	for issuer, ok := range map[string]bool{
		"https://oidc.example":              true,
		"http://localhost:8080":             true,
		"http://127.0.0.1:18999":            true,
		"http://[::1]:8080":                 true,
		"http://oidc.example":               false,
		"http://10.0.0.5":                   false,
		"ftp://oidc.example":                false,
		"http://localhost.attacker.example": false,
	} {
		_, err := s.PutOIDCProvider(ctx, &iam_pb.PutOIDCProviderRequest{IssuerUrl: issuer, ClientIds: []string{"aud"}})
		if ok {
			assert.NoError(t, err, issuer)
		} else {
			requireCode(t, err, codes.InvalidArgument)
		}
	}
}

// Without an admin signing key the IAM service accepts any caller. Users and
// policies keep that opt-in behaviour, but the OIDC provider and role RPCs
// grant STS access outright, so they refuse to run unauthenticated.
func TestIamGrpc_STSRPCsRefuseAnUnauthenticatedService(t *testing.T) {
	cm, err := credential.NewCredentialManager(credential.StoreTypeMemory, nil, "")
	require.NoError(t, err)
	s := NewIamGrpcServer(cm, nil)
	s.SetSTSStores(integration.NewMemoryOIDCProviderStore(), integration.NewMemoryRoleStore())
	ctx := context.Background()
	calls := map[string]func() error{
		"PutOIDCProvider": func() error {
			_, err := s.PutOIDCProvider(ctx, &iam_pb.PutOIDCProviderRequest{IssuerUrl: "https://oidc.example", ClientIds: []string{"aud"}})
			return err
		},
		"GetOIDCProvider": func() error {
			_, err := s.GetOIDCProvider(ctx, &iam_pb.GetOIDCProviderRequest{IssuerUrl: "https://oidc.example"})
			return err
		},
		"DeleteOIDCProvider": func() error {
			_, err := s.DeleteOIDCProvider(ctx, &iam_pb.DeleteOIDCProviderRequest{IssuerUrl: "https://oidc.example"})
			return err
		},
		"ListOIDCProviders": func() error { _, err := s.ListOIDCProviders(ctx, &iam_pb.ListOIDCProvidersRequest{}); return err },
		"PutRole": func() error {
			_, err := s.PutRole(ctx, &iam_pb.PutRoleRequest{Role: &iam_pb.Role{RoleName: "app", TrustPolicy: stsTestTrust}})
			return err
		},
		"GetRole":    func() error { _, err := s.GetRole(ctx, &iam_pb.GetRoleRequest{RoleName: "app"}); return err },
		"DeleteRole": func() error { _, err := s.DeleteRole(ctx, &iam_pb.DeleteRoleRequest{RoleName: "app"}); return err },
		"ListRoles":  func() error { _, err := s.ListRoles(ctx, &iam_pb.ListRolesRequest{}); return err },
	}
	for name, call := range calls {
		t.Run(name, func(t *testing.T) { requireCode(t, call(), codes.FailedPrecondition) })
	}

	// Users keep the service's opt-in auth.
	_, err = s.ListUsers(ctx, &iam_pb.ListUsersRequest{})
	assert.NoError(t, err)
}
