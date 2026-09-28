package s3api

import (
	"context"
	"encoding/xml"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go/service/iam"
	iamlib "github.com/seaweedfs/seaweedfs/weed/iam"
	"github.com/seaweedfs/seaweedfs/weed/iam/integration"
	"github.com/seaweedfs/seaweedfs/weed/pb/iam_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/policy_engine"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const spiffeTrustPolicy = `{"Version":"2012-10-17","Statement":[{"Effect":"Allow",` +
	`"Principal":{"Federated":"https://oidc.example"},"Action":["sts:AssumeRoleWithWebIdentity"],` +
	`"Condition":{"StringEquals":{"oidc:sub":"spiffe://example.org/ns/app/sa/app"}}}]}`

func roleAction(t *testing.T, api *EmbeddedIamApiForTest, params map[string]string) (iamlib.RequestIDSetter, *iamError) {
	t.Helper()
	values := url.Values{}
	for k, v := range params {
		values.Set(k, v)
	}
	return api.ExecuteAction(context.Background(), values, true, "role-test")
}

func requireIamCode(t *testing.T, iamErr *iamError, code string) {
	t.Helper()
	require.NotNil(t, iamErr, "expected %s", code)
	assert.Equal(t, code, iamErr.Code, "error: %v", iamErr.Error)
}

func newRoleTestAPI(t *testing.T) (*EmbeddedIamApiForTest, *integration.IAMManager) {
	t.Helper()
	api, mgr := newOIDCTestAPI(t)
	doc := policy_engine.PolicyDocument{Version: "2012-10-17", Statement: []policy_engine.PolicyStatement{{
		Effect:   policy_engine.PolicyEffectAllow,
		Action:   policy_engine.NewStringOrStringSlice("s3:GetObject"),
		Resource: policy_engine.NewStringOrStringSlicePtr("arn:aws:s3:::bucket/*"),
	}}}
	require.NoError(t, api.credentialManager.CreatePolicy(context.Background(), "read-bucket", doc))
	return api, mgr
}

func TestRoleLifecycle(t *testing.T) {
	api, mgr := newRoleTestAPI(t)

	resp, iamErr := roleAction(t, api, map[string]string{
		"Action": actionCreateRole, "RoleName": "app", "AssumeRolePolicyDocument": spiffeTrustPolicy,
		"Description": "app runtime", "MaxSessionDuration": "3600",
	})
	require.Nil(t, iamErr)
	created := resp.(*iamlib.CreateRoleResponse).CreateRoleResult.Role
	assert.Equal(t, "arn:aws:iam::role/app", created.Arn)
	assert.NotEmpty(t, created.RoleId)
	assert.NotEmpty(t, created.CreateDate)
	decoded, err := url.PathUnescape(created.AssumeRolePolicyDocument)
	require.NoError(t, err)
	assert.Contains(t, decoded, "spiffe://example.org/ns/app/sa/app")

	// The role is the one STS evaluates.
	stored, err := mgr.GetRole(context.Background(), "app")
	require.NoError(t, err)
	assert.Equal(t, "https://oidc.example", stored.TrustPolicy.Statement[0].Principal.(map[string]interface{})["Federated"])

	_, iamErr = roleAction(t, api, map[string]string{"Action": actionAttachRolePolicy, "RoleName": "app", "PolicyArn": "arn:aws:iam:::policy/read-bucket"})
	require.Nil(t, iamErr)
	resp, iamErr = roleAction(t, api, map[string]string{"Action": actionListAttachedRolePolicies, "RoleName": "app"})
	require.Nil(t, iamErr)
	attached := resp.(*iamlib.ListAttachedRolePoliciesResponse).ListAttachedRolePoliciesResult.AttachedPolicies
	require.Len(t, attached, 1)
	assert.Equal(t, "read-bucket", attached[0].PolicyName)

	_, iamErr = roleAction(t, api, map[string]string{"Action": actionDeleteRole, "RoleName": "app"})
	requireIamCode(t, iamErr, iam.ErrCodeDeleteConflictException)

	_, iamErr = roleAction(t, api, map[string]string{"Action": actionDetachRolePolicy, "RoleName": "app", "PolicyArn": "arn:aws:iam:::policy/read-bucket"})
	require.Nil(t, iamErr)
	_, iamErr = roleAction(t, api, map[string]string{"Action": actionDeleteRole, "RoleName": "app"})
	require.Nil(t, iamErr)
	_, iamErr = roleAction(t, api, map[string]string{"Action": actionGetRole, "RoleName": "app"})
	requireIamCode(t, iamErr, iam.ErrCodeNoSuchEntityException)
}

func TestUpdateAssumeRolePolicyReplacesTheTrustPolicy(t *testing.T) {
	api, mgr := newRoleTestAPI(t)
	_, iamErr := roleAction(t, api, map[string]string{"Action": actionCreateRole, "RoleName": "app", "AssumeRolePolicyDocument": spiffeTrustPolicy})
	require.Nil(t, iamErr)

	updated := `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Federated":"https://other.example"},"Action":["sts:AssumeRoleWithWebIdentity"]}]}`
	_, iamErr = roleAction(t, api, map[string]string{"Action": actionUpdateAssumeRolePolicy, "RoleName": "app", "PolicyDocument": updated})
	require.Nil(t, iamErr)
	stored, err := mgr.GetRole(context.Background(), "app")
	require.NoError(t, err)
	assert.Equal(t, "https://other.example", stored.TrustPolicy.Statement[0].Principal.(map[string]interface{})["Federated"])
}

func TestRoleActionsRefuseWhatTheyCannotHonour(t *testing.T) {
	api, _ := newRoleTestAPI(t)
	_, iamErr := roleAction(t, api, map[string]string{"Action": actionCreateRole, "RoleName": "app", "AssumeRolePolicyDocument": spiffeTrustPolicy})
	require.Nil(t, iamErr)

	cases := []struct {
		name   string
		params map[string]string
		code   string
	}{
		{"duplicate", map[string]string{"Action": actionCreateRole, "RoleName": "app", "AssumeRolePolicyDocument": spiffeTrustPolicy}, iam.ErrCodeEntityAlreadyExistsException},
		{"no trust policy", map[string]string{"Action": actionCreateRole, "RoleName": "b"}, iam.ErrCodeInvalidInputException},
		{"malformed trust policy", map[string]string{"Action": actionCreateRole, "RoleName": "b", "AssumeRolePolicyDocument": "{"}, iam.ErrCodeMalformedPolicyDocumentException},
		{"path", map[string]string{"Action": actionCreateRole, "RoleName": "b", "Path": "/team/", "AssumeRolePolicyDocument": spiffeTrustPolicy}, iam.ErrCodeInvalidInputException},
		{"tags", map[string]string{"Action": actionCreateRole, "RoleName": "b", "Tags.member.1.Key": "k", "Tags.member.1.Value": "v", "AssumeRolePolicyDocument": spiffeTrustPolicy}, iam.ErrCodeInvalidInputException},
		{"session out of bounds", map[string]string{"Action": actionCreateRole, "RoleName": "b", "MaxSessionDuration": "60", "AssumeRolePolicyDocument": spiffeTrustPolicy}, iam.ErrCodeInvalidInputException},
		{"attach missing policy", map[string]string{"Action": actionAttachRolePolicy, "RoleName": "app", "PolicyArn": "arn:aws:iam:::policy/nope"}, iam.ErrCodeNoSuchEntityException},
		{"detach unattached", map[string]string{"Action": actionDetachRolePolicy, "RoleName": "app", "PolicyArn": "arn:aws:iam:::policy/read-bucket"}, iam.ErrCodeNoSuchEntityException},
		{"missing role", map[string]string{"Action": actionDeleteRole, "RoleName": "nope"}, iam.ErrCodeNoSuchEntityException},
		{"name leaving the role store", map[string]string{"Action": actionCreateRole, "RoleName": "../identities/admin", "AssumeRolePolicyDocument": spiffeTrustPolicy}, iam.ErrCodeInvalidInputException},
		{"name too long", map[string]string{"Action": actionCreateRole, "RoleName": strings.Repeat("a", 65), "AssumeRolePolicyDocument": spiffeTrustPolicy}, iam.ErrCodeInvalidInputException},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, iamErr := roleAction(t, api, tc.params)
			requireIamCode(t, iamErr, tc.code)
		})
	}
}

// A role from the IAM config file is reloaded from it at every start, so an
// API change to it would be silently reverted; it is refused instead.
func TestConfigFileRolesAreUnmodifiable(t *testing.T) {
	api, mgr := newRoleTestAPI(t)
	trust, iamErr := parseTrustPolicy(spiffeTrustPolicy)
	require.Nil(t, iamErr)
	require.NoError(t, mgr.CreateRole(context.Background(), "", "from-file", &integration.RoleDefinition{
		RoleName: "from-file", TrustPolicy: trust, Source: integration.RoleSourceStaticConfig,
	}))

	for _, params := range []map[string]string{
		{"Action": actionDeleteRole, "RoleName": "from-file"},
		{"Action": actionUpdateAssumeRolePolicy, "RoleName": "from-file", "PolicyDocument": spiffeTrustPolicy},
		{"Action": actionAttachRolePolicy, "RoleName": "from-file", "PolicyArn": "arn:aws:iam:::policy/read-bucket"},
		{"Action": actionDetachRolePolicy, "RoleName": "from-file", "PolicyArn": "arn:aws:iam:::policy/read-bucket"},
	} {
		t.Run(params["Action"], func(t *testing.T) {
			_, iamErr := roleAction(t, api, params)
			requireIamCode(t, iamErr, iam.ErrCodeUnmodifiableEntityException)
		})
	}
	_, iamErr = roleAction(t, api, map[string]string{"Action": actionGetRole, "RoleName": "from-file"})
	assert.Nil(t, iamErr, "reading a config-file role is allowed")
}

func TestReadOnlyAllowsRoleReadsAndDeniesMutations(t *testing.T) {
	api, _ := newRoleTestAPI(t)
	_, iamErr := roleAction(t, api, map[string]string{"Action": actionCreateRole, "RoleName": "app", "AssumeRolePolicyDocument": spiffeTrustPolicy})
	require.Nil(t, iamErr)
	api.readOnly = true

	for _, action := range []string{actionGetRole, actionListRoles, actionListAttachedRolePolicies} {
		_, iamErr := roleAction(t, api, map[string]string{"Action": action, "RoleName": "app"})
		assert.Nil(t, iamErr, "%s must be allowed in read-only mode", action)
	}
	for _, action := range []string{actionCreateRole, actionDeleteRole, actionUpdateAssumeRolePolicy, actionAttachRolePolicy, actionDetachRolePolicy} {
		_, iamErr := roleAction(t, api, map[string]string{"Action": action, "RoleName": "app"})
		assert.NotNil(t, iamErr, "%s must be denied in read-only mode", action)
	}
}

// unreadableRoleStore fails every read the way an unreachable filer does.
type unreadableRoleStore struct{ *integration.MemoryRoleStore }

func (unreadableRoleStore) GetRole(context.Context, string, string) (*integration.RoleDefinition, error) {
	return nil, errors.New("lookup role: filer unavailable")
}

// An unreadable store is not an absent role: CreateRole must not write over a
// role it could not see, and the reads must not report NoSuchEntity.
func TestRoleActionsTreatAnUnreadableStoreAsAFailureNotAnAbsence(t *testing.T) {
	api, mgr := newRoleTestAPI(t)
	store := unreadableRoleStore{integration.NewMemoryRoleStore()}
	mgr.SetRoleStore(store)

	_, iamErr := roleAction(t, api, map[string]string{"Action": actionCreateRole, "RoleName": "app", "AssumeRolePolicyDocument": spiffeTrustPolicy})
	requireIamCode(t, iamErr, iam.ErrCodeServiceFailureException)
	names, err := store.ListRoles(context.Background(), "")
	require.NoError(t, err)
	assert.Empty(t, names, "CreateRole wrote through a store it could not read")

	_, iamErr = roleAction(t, api, map[string]string{"Action": actionGetRole, "RoleName": "app"})
	requireIamCode(t, iamErr, iam.ErrCodeServiceFailureException)
}

// A policy is attached to a role by name, so deleting it while attached would
// let a policy created later under that name take effect on the role. It is
// refused, as for users and groups.
func TestDeletePolicyAttachedToARoleIsAConflict(t *testing.T) {
	api, _ := newRoleTestAPI(t)
	// The test API replaces the credential store with mockConfig on the first
	// action that loads the configuration, so the policy has to be declared
	// there too.
	api.mockConfig.Policies = append(api.mockConfig.Policies, &iam_pb.Policy{
		Name:    "read-bucket",
		Content: `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":["s3:GetObject"],"Resource":["arn:aws:s3:::bucket/*"]}]}`,
	})
	_, iamErr := roleAction(t, api, map[string]string{"Action": actionCreateRole, "RoleName": "app", "AssumeRolePolicyDocument": spiffeTrustPolicy})
	require.Nil(t, iamErr)
	_, iamErr = roleAction(t, api, map[string]string{"Action": actionAttachRolePolicy, "RoleName": "app", "PolicyArn": "arn:aws:iam:::policy/read-bucket"})
	require.Nil(t, iamErr)

	_, iamErr = roleAction(t, api, map[string]string{"Action": "DeletePolicy", "PolicyArn": "arn:aws:iam:::policy/read-bucket"})
	requireIamCode(t, iamErr, iam.ErrCodeDeleteConflictException)

	_, iamErr = roleAction(t, api, map[string]string{"Action": actionDetachRolePolicy, "RoleName": "app", "PolicyArn": "arn:aws:iam:::policy/read-bucket"})
	require.Nil(t, iamErr)
	_, iamErr = roleAction(t, api, map[string]string{"Action": "DeletePolicy", "PolicyArn": "arn:aws:iam:::policy/read-bucket"})
	assert.Nil(t, iamErr, "a detached policy could not be deleted")
}

func TestAttachRolePolicyStopsAtTheRoleQuota(t *testing.T) {
	api, _ := newRoleTestAPI(t)
	_, iamErr := roleAction(t, api, map[string]string{"Action": actionCreateRole, "RoleName": "app", "AssumeRolePolicyDocument": spiffeTrustPolicy})
	require.Nil(t, iamErr)
	doc := policy_engine.PolicyDocument{Version: "2012-10-17", Statement: []policy_engine.PolicyStatement{{
		Effect: policy_engine.PolicyEffectAllow, Action: policy_engine.NewStringOrStringSlice("s3:GetObject"),
		Resource: policy_engine.NewStringOrStringSlicePtr("arn:aws:s3:::bucket/*"),
	}}}
	for i := 0; i <= integration.MaxManagedPoliciesPerRole; i++ {
		name := fmt.Sprintf("p%d", i)
		require.NoError(t, api.credentialManager.CreatePolicy(context.Background(), name, doc))
		_, iamErr = roleAction(t, api, map[string]string{"Action": actionAttachRolePolicy, "RoleName": "app", "PolicyArn": "arn:aws:iam:::policy/" + name})
		if i < integration.MaxManagedPoliciesPerRole {
			require.Nil(t, iamErr, "attach %d", i)
		}
	}
	requireIamCode(t, iamErr, iam.ErrCodeLimitExceededException)
}

// A role without a creation time omits CreateDate rather than sending it
// empty, which clients cannot parse.
func TestRoleWithoutACreationTimeOmitsCreateDate(t *testing.T) {
	out, err := xml.Marshal(toIAMRole(&integration.RoleDefinition{RoleName: "r", RoleArn: "arn:aws:iam::role/r"}))
	require.NoError(t, err)
	assert.NotContains(t, string(out), "CreateDate")
}
