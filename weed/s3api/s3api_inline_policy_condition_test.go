package s3api

import (
	"context"
	"net/http"
	"net/url"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/iam_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/policy_engine"
	s3_constants "github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3err"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// In-process reproducers for aws:SourceIp condition handling on inline policies.
//
// These exercise the full chain: PutUserPolicy/PutGroupPolicy → reload →
// VerifyActionPermission against a synthetic HTTP request whose RemoteAddr is
// 127.0.0.1. The whole point is to prove that the Condition block is honored
// (or, before the fix lands, that it is silently dropped).

const inlineCondTestBucket = "inline-cond-bucket"

func inlineCondPolicyDoc(cidr string) string {
	return `{
		"Version":"2012-10-17",
		"Statement":[{
			"Effect":"Allow",
			"Action":"s3:*",
			"Resource":["arn:aws:s3:::` + inlineCondTestBucket + `","arn:aws:s3:::` + inlineCondTestBucket + `/*"],
			"Condition":{"IpAddress":{"aws:SourceIp":"` + cidr + `"}}
		}]
	}`
}

// inlineCondRequest builds a minimal request that VerifyActionPermission can
// evaluate: RemoteAddr set so extractSourceIP returns 127.0.0.1, and the bucket
// path so GetBucketAndObject (if reached) is unambiguous.
func inlineCondRequest(t *testing.T, method string) *http.Request {
	t.Helper()
	req, err := http.NewRequest(method, "http://s3.amazonaws.com/"+inlineCondTestBucket+"/obj", nil)
	require.NoError(t, err)
	req.Host = "s3.amazonaws.com"
	req.RemoteAddr = "127.0.0.1:54321"
	return req
}

// seedInlineCondUser puts a user "alice" with a single access key into both
// api.mockConfig and the credential store, then reloads the in-memory IAM so
// identity lookups observe her. Returns the loaded Identity.
func seedInlineCondUser(t *testing.T, api *EmbeddedIamApiForTest) *Identity {
	t.Helper()
	if api.mockConfig == nil {
		api.mockConfig = &iam_pb.S3ApiConfiguration{}
	}
	api.mockConfig.Identities = append(api.mockConfig.Identities, &iam_pb.Identity{
		Name: "alice",
		Credentials: []*iam_pb.Credential{
			{AccessKey: "AKIAALICE", SecretKey: "alice-secret"},
		},
	})
	require.NoError(t, api.credentialManager.SaveConfiguration(context.Background(), api.mockConfig))
	require.NoError(t, api.iam.LoadS3ApiConfigurationFromCredentialManager())
	ident := api.iam.lookupByIdentityName("alice")
	require.NotNil(t, ident, "alice must be loaded after reload")
	return ident
}

// TestUserInlinePolicySourceIpCondition_Denies is the primary reproducer: a
// user inline policy with an aws:SourceIp condition that does NOT match the
// caller's source IP must result in AccessDenied. Today this passes (bug)
// because PutUserPolicy flattens the document into ident.Actions and the
// Condition block is dropped.
func TestUserInlinePolicySourceIpCondition_Denies(t *testing.T) {
	api := NewEmbeddedIamApiForTest()
	api.mockConfig = &iam_pb.S3ApiConfiguration{}
	seedInlineCondUser(t, api)

	// Allow s3:* on the bucket only when SourceIp is in 198.51.100.0/24 (TEST-NET-2).
	// 127.0.0.1 must not match, so the action must be denied.
	_, iamErr := api.PutUserPolicy(api.mockConfig, url.Values{
		"UserName":       {"alice"},
		"PolicyName":     {"OnlyFromTestNet"},
		"PolicyDocument": {inlineCondPolicyDoc("198.51.100.0/24")},
	})
	require.Nil(t, iamErr, "PutUserPolicy must succeed")
	require.NoError(t, api.PutS3ApiConfiguration(api.mockConfig))
	require.NoError(t, api.iam.LoadS3ApiConfigurationFromCredentialManager())

	ident := api.iam.lookupByIdentityName("alice")
	require.NotNil(t, ident)

	req := inlineCondRequest(t, http.MethodPut)
	got := api.iam.VerifyActionPermission(req, ident, s3_constants.ACTION_WRITE, inlineCondTestBucket, "obj")
	assert.Equal(t, s3err.ErrAccessDenied, got,
		"PutObject from 127.0.0.1 must be denied: the user inline policy's aws:SourceIp condition (198.51.100.0/24) does not match")
}

// TestUserInlinePolicySourceIpCondition_Allows is the companion: with a
// matching CIDR (127.0.0.0/8), the same call must succeed.
func TestUserInlinePolicySourceIpCondition_Allows(t *testing.T) {
	api := NewEmbeddedIamApiForTest()
	api.mockConfig = &iam_pb.S3ApiConfiguration{}
	seedInlineCondUser(t, api)

	_, iamErr := api.PutUserPolicy(api.mockConfig, url.Values{
		"UserName":       {"alice"},
		"PolicyName":     {"FromLoopback"},
		"PolicyDocument": {inlineCondPolicyDoc("127.0.0.0/8")},
	})
	require.Nil(t, iamErr)
	require.NoError(t, api.PutS3ApiConfiguration(api.mockConfig))
	require.NoError(t, api.iam.LoadS3ApiConfigurationFromCredentialManager())

	ident := api.iam.lookupByIdentityName("alice")
	require.NotNil(t, ident)

	req := inlineCondRequest(t, http.MethodPut)
	got := api.iam.VerifyActionPermission(req, ident, s3_constants.ACTION_WRITE, inlineCondTestBucket, "obj")
	assert.Equal(t, s3err.ErrNone, got,
		"PutObject from 127.0.0.1 must be allowed: the user inline policy's aws:SourceIp condition (127.0.0.0/8) matches")
}

// TestGroupInlinePolicy_PutAndEnforce verifies that PutGroupPolicy is supported
// (no longer returns NotImplemented) and that an aws:SourceIp condition on the
// resulting inline policy is honored for group members.
func TestGroupInlinePolicy_PutAndEnforce(t *testing.T) {
	api := NewEmbeddedIamApiForTest()
	api.mockConfig = &iam_pb.S3ApiConfiguration{
		Groups: []*iam_pb.Group{
			{Name: "devs", Members: []string{"alice"}},
		},
	}
	seedInlineCondUser(t, api)
	require.NoError(t, api.credentialManager.SaveConfiguration(context.Background(), api.mockConfig))
	require.NoError(t, api.iam.LoadS3ApiConfigurationFromCredentialManager())

	// PutGroupPolicy must succeed. This is the call that returns
	// NotImplemented today; this test will fail with that error.
	_, iamErr := api.PutGroupPolicy(api.mockConfig, url.Values{
		"GroupName":      {"devs"},
		"PolicyName":     {"DevsFromLoopback"},
		"PolicyDocument": {inlineCondPolicyDoc("198.51.100.0/24")},
	})
	require.Nil(t, iamErr, "PutGroupPolicy must succeed; got: %+v", iamErr)
	require.NoError(t, api.iam.LoadS3ApiConfigurationFromCredentialManager())

	ident := api.iam.lookupByIdentityName("alice")
	require.NotNil(t, ident)

	// Deny path: condition does not match the loopback caller.
	req := inlineCondRequest(t, http.MethodPut)
	got := api.iam.VerifyActionPermission(req, ident, s3_constants.ACTION_WRITE, inlineCondTestBucket, "obj")
	assert.Equal(t, s3err.ErrAccessDenied, got,
		"group member from 127.0.0.1 must be denied when the group inline policy's aws:SourceIp condition (198.51.100.0/24) does not match")

	// Round-trip via the store: list & get must observe the new inline policy.
	listResp, iamErr := api.ListGroupPolicies(api.mockConfig, url.Values{"GroupName": {"devs"}})
	require.Nil(t, iamErr)
	assert.Contains(t, listResp.ListGroupPoliciesResult.PolicyNames, "DevsFromLoopback")

	getResp, iamErr := api.GetGroupPolicy(api.mockConfig, url.Values{
		"GroupName": {"devs"}, "PolicyName": {"DevsFromLoopback"},
	})
	require.Nil(t, iamErr)
	assert.Contains(t, getResp.GetGroupPolicyResult.PolicyDocument, "aws:SourceIp",
		"GetGroupPolicy must round-trip the Condition block")

	// Flip the policy to the matching CIDR and verify the allow path.
	_, iamErr = api.PutGroupPolicy(api.mockConfig, url.Values{
		"GroupName":      {"devs"},
		"PolicyName":     {"DevsFromLoopback"},
		"PolicyDocument": {inlineCondPolicyDoc("127.0.0.0/8")},
	})
	require.Nil(t, iamErr)
	require.NoError(t, api.iam.LoadS3ApiConfigurationFromCredentialManager())

	ident = api.iam.lookupByIdentityName("alice")
	require.NotNil(t, ident)
	got = api.iam.VerifyActionPermission(inlineCondRequest(t, http.MethodPut), ident,
		s3_constants.ACTION_WRITE, inlineCondTestBucket, "obj")
	assert.Equal(t, s3err.ErrNone, got,
		"group member from 127.0.0.1 must be allowed when the group inline policy's aws:SourceIp condition (127.0.0.0/8) matches")

	// Cleanup: DeleteGroupPolicy must also be implemented and must drop the
	// engine registration so the action is no longer permitted via this group.
	_, iamErr = api.DeleteGroupPolicy(api.mockConfig, url.Values{
		"GroupName": {"devs"}, "PolicyName": {"DevsFromLoopback"},
	})
	require.Nil(t, iamErr, "DeleteGroupPolicy must succeed")
	require.NoError(t, api.iam.LoadS3ApiConfigurationFromCredentialManager())
	ident = api.iam.lookupByIdentityName("alice")
	require.NotNil(t, ident)
	got = api.iam.VerifyActionPermission(inlineCondRequest(t, http.MethodPut), ident,
		s3_constants.ACTION_WRITE, inlineCondTestBucket, "obj")
	assert.Equal(t, s3err.ErrAccessDenied, got,
		"after DeleteGroupPolicy, the group inline policy must no longer grant access")
}

// TestUserInlinePolicy_ConditionDiscriminatesAllowVsDeny pairs allow and deny
// against the same user with the same Actions list. If the engine path is not
// in use, both calls would hit the legacy Actions branch and both would be
// allowed; the discriminator is that switching the CIDR must flip the result.
func TestUserInlinePolicy_ConditionDiscriminatesAllowVsDeny(t *testing.T) {
	api := NewEmbeddedIamApiForTest()
	api.mockConfig = &iam_pb.S3ApiConfiguration{}
	seedInlineCondUser(t, api)

	// Allow from loopback first: must succeed.
	_, iamErr := api.PutUserPolicy(api.mockConfig, url.Values{
		"UserName":       {"alice"},
		"PolicyName":     {"P"},
		"PolicyDocument": {inlineCondPolicyDoc("127.0.0.0/8")},
	})
	require.Nil(t, iamErr)
	require.NoError(t, api.PutS3ApiConfiguration(api.mockConfig))
	require.NoError(t, api.iam.LoadS3ApiConfigurationFromCredentialManager())

	ident := api.iam.lookupByIdentityName("alice")
	require.NotNil(t, ident)
	got := api.iam.VerifyActionPermission(inlineCondRequest(t, http.MethodPut), ident,
		s3_constants.ACTION_WRITE, inlineCondTestBucket, "obj")
	require.Equal(t, s3err.ErrNone, got, "policy with matching CIDR must allow")

	// Replace with non-matching CIDR: must now deny. Same user, same action,
	// same Actions list (Admin:bucket). Only the Condition block changed.
	_, iamErr = api.PutUserPolicy(api.mockConfig, url.Values{
		"UserName":       {"alice"},
		"PolicyName":     {"P"},
		"PolicyDocument": {inlineCondPolicyDoc("198.51.100.0/24")},
	})
	require.Nil(t, iamErr)
	require.NoError(t, api.PutS3ApiConfiguration(api.mockConfig))
	require.NoError(t, api.iam.LoadS3ApiConfigurationFromCredentialManager())

	ident = api.iam.lookupByIdentityName("alice")
	require.NotNil(t, ident)
	got = api.iam.VerifyActionPermission(inlineCondRequest(t, http.MethodPut), ident,
		s3_constants.ACTION_WRITE, inlineCondTestBucket, "obj")
	require.Equal(t, s3err.ErrAccessDenied, got,
		"flipping aws:SourceIp to a non-matching CIDR must flip the decision; "+
			"this proves the engine (not the legacy Actions list) drives the outcome")
}

// TestUserInlinePolicy_ReloadFromStore verifies that after the IAM is reloaded
// (simulating a restart), the stored user inline policy is re-registered and
// its Condition block is still enforced. Catches a regression where the engine
// state is rebuilt only from managed policies, not inline ones.
func TestUserInlinePolicy_ReloadFromStore(t *testing.T) {
	api := NewEmbeddedIamApiForTest()
	api.mockConfig = &iam_pb.S3ApiConfiguration{}
	seedInlineCondUser(t, api)

	// Persist an inline policy via the credential store directly, bypassing
	// the API. This is the on-disk state a fresh process boots into.
	var doc policy_engine.PolicyDocument
	require.NoError(t, doc.UnmarshalJSON([]byte(inlineCondPolicyDoc("198.51.100.0/24"))))
	require.NoError(t, api.credentialManager.PutUserInlinePolicy(
		context.Background(), "alice", "OnlyFromTestNet", doc))

	// Simulate restart: full reload from credential store.
	require.NoError(t, api.iam.LoadS3ApiConfigurationFromCredentialManager())

	ident := api.iam.lookupByIdentityName("alice")
	require.NotNil(t, ident)

	req := inlineCondRequest(t, http.MethodPut)
	got := api.iam.VerifyActionPermission(req, ident, s3_constants.ACTION_WRITE, inlineCondTestBucket, "obj")
	assert.Equal(t, s3err.ErrAccessDenied, got,
		"after reload, the persisted user inline policy's aws:SourceIp condition must still be honored")
}
