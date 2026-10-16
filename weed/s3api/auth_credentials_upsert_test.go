package s3api

import (
	"context"
	"net/http"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/iam_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/policy_engine"
	s3_constants "github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3err"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// PutIdentity pushes carry the persisted identity proto, which records the
// inline policy flattened into Actions but not its runtime policy name. The
// upsert must keep the identity on the attached-policy route; otherwise its
// coarse "Write:bucket" action authorizes deletes the policy never granted.
func TestUpsertIdentityKeepsInlinePolicyAuthorization(t *testing.T) {
	s3a := newTestS3ApiServerWithMemoryIAM(t, []*iam_pb.Identity{{
		Name:        "writer",
		Credentials: []*iam_pb.Credential{{AccessKey: "AKW", SecretKey: "secret"}},
		Actions:     []string{"Read:bucket", "Write:bucket", "List:bucket"},
	}})
	ctx := context.Background()

	var doc policy_engine.PolicyDocument
	require.NoError(t, doc.UnmarshalJSON([]byte(`{
		"Version":"2012-10-17",
		"Statement":[{
			"Effect":"Allow",
			"Action":["s3:GetObject","s3:PutObject","s3:ListBucket"],
			"Resource":["arn:aws:s3:::bucket","arn:aws:s3:::bucket/*"]
		}]
	}`)))
	require.NoError(t, s3a.iam.credentialManager.PutUserInlinePolicy(ctx, "writer", "writer", doc))
	require.NoError(t, s3a.iam.LoadS3ApiConfigurationFromCredentialManager())

	pushed, err := s3a.iam.credentialManager.GetUser(ctx, "writer")
	require.NoError(t, err)
	require.NotContains(t, pushed.PolicyNames, inlinePolicyRuntimeName("writer", "writer"),
		"persisted protos do not carry runtime inline policy names")
	require.NoError(t, s3a.iam.UpsertIdentity(pushed))

	ident := s3a.iam.lookupByIdentityName("writer")
	require.NotNil(t, ident)
	require.Contains(t, ident.PolicyNames, inlinePolicyRuntimeName("writer", "writer"))

	req, err := http.NewRequest(http.MethodDelete, "http://s3/bucket/obj", nil)
	require.NoError(t, err)
	assert.Equal(t, s3err.ErrAccessDenied,
		s3a.iam.VerifyActionPermission(req, ident, s3_constants.ACTION_WRITE, "bucket", "obj"),
		"an identity pushed without its inline policy must not fall back to legacy Actions")
}

// The same applies to PutGroup pushes: the persisted group proto lacks the
// group's runtime inline policy names.
func TestPutGroupKeepsInlinePolicyAuthorization(t *testing.T) {
	s3a := newTestS3ApiServerWithMemoryIAM(t, []*iam_pb.Identity{{Name: "alice"}})
	ctx := context.Background()

	require.NoError(t, s3a.iam.credentialManager.CreateGroup(ctx,
		&iam_pb.Group{Name: "devs", Members: []string{"alice"}}))
	var doc policy_engine.PolicyDocument
	require.NoError(t, doc.UnmarshalJSON([]byte(`{
		"Version":"2012-10-17",
		"Statement":[{"Effect":"Allow","Action":"s3:GetObject","Resource":"*"}]
	}`)))
	require.NoError(t, s3a.iam.credentialManager.PutGroupInlinePolicy(ctx, "devs", "p", doc))
	require.NoError(t, s3a.iam.LoadS3ApiConfigurationFromCredentialManager())

	pushed, err := s3a.iam.credentialManager.GetGroup(ctx, "devs")
	require.NoError(t, err)
	require.NoError(t, s3a.iam.PutGroup(pushed))

	s3a.iam.m.RLock()
	defer s3a.iam.m.RUnlock()
	require.Contains(t, s3a.iam.groups["devs"].PolicyNames, inlineGroupPolicyRuntimeName("devs", "p"))
}
