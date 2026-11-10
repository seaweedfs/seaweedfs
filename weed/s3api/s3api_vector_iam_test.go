package s3api

import (
	"fmt"
	"net/http/httptest"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3tables"
	"github.com/stretchr/testify/require"
)

func TestVectorIAMPolicies(t *testing.T) {
	const resource = "arn:aws:s3vectors:us-east-1:000000000000:bucket/embeddings/index/docs"
	for _, action := range []string{"s3vectors:QueryVectors", "s3vectors:*"} {
		t.Run(action, func(t *testing.T) {
			iam := &IdentityAccessManagement{isAuthEnabled: true}
			require.NoError(t, iam.PutPolicy("vectors", fmt.Sprintf(`{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":%q,"Resource":%q,"Condition":{"StringEquals":{"aws:ResourceTag/team":"search"}}}]}`, action, resource)))
			identity := &Identity{Name: "reader", PolicyNames: []string{"vectors"}}
			r := httptest.NewRequest("POST", "/QueryVectors", nil)
			r = r.WithContext(s3_constants.SetIdentityInContext(r.Context(), identity))
			server := &S3ApiServer{iam: iam}
			allowed, denied := server.EvaluateIAMPolicy(r, "s3vectors:QueryVectors", resource, &s3tables.PolicyContext{ResourceTags: map[string]string{"team": "search"}})
			require.True(t, allowed)
			require.False(t, denied)
			allowed, denied = server.EvaluateIAMPolicy(r, "s3vectors:QueryVectors", resource, &s3tables.PolicyContext{ResourceTags: map[string]string{"team": "other"}})
			require.False(t, allowed)
			require.False(t, denied)
			allowed, _ = server.EvaluateIAMPolicy(r, "s3vectors:QueryVectors", resource+"-other", &s3tables.PolicyContext{ResourceTags: map[string]string{"team": "search"}})
			require.False(t, allowed)
			require.NoError(t, iam.PutPolicy("deny", `{"Version":"2012-10-17","Statement":[{"Effect":"Deny","Action":"s3vectors:QueryVectors","Resource":"*"}]}`))
			identity.PolicyNames = append(identity.PolicyNames, "deny")
			allowed, denied = server.EvaluateIAMPolicy(r, "s3vectors:QueryVectors", resource, &s3tables.PolicyContext{ResourceTags: map[string]string{"team": "search"}})
			require.False(t, allowed)
			require.True(t, denied)
		})
	}
}

func TestEmbeddedIAMAcceptsVectorPoliciesWithoutFlattening(t *testing.T) {
	api := &EmbeddedIamApi{}
	for _, effect := range []string{"Allow", "Deny"} {
		raw := fmt.Sprintf(`{"Statement":[{"Effect":%q,"Action":"s3vectors:*","Resource":"arn:aws:s3vectors:us-east-1:000000000000:bucket/embeddings/*"}]}`, effect)
		policy, err := api.GetPolicyDocument(&raw)
		require.NoError(t, err)
		actions, err := api.getActions(&policy)
		require.NoError(t, err)
		require.Empty(t, actions)
	}
}

func TestVectorIAMBucketTagDeny(t *testing.T) {
	iam := &IdentityAccessManagement{isAuthEnabled: true}
	require.NoError(t, iam.PutPolicy("vectors", `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"s3vectors:*","Resource":"*"},{"Effect":"Deny","Action":"s3vectors:QueryVectors","Resource":"*","Condition":{"StringEquals":{"s3vectors:VectorBucketTag/classification":"private"}}}]}`))
	identity := &Identity{Name: "reader", PolicyNames: []string{"vectors"}}
	r := httptest.NewRequest("POST", "/QueryVectors", nil)
	r = r.WithContext(s3_constants.SetIdentityInContext(r.Context(), identity))
	server := &S3ApiServer{iam: iam}
	allowed, denied := server.EvaluateIAMPolicy(r, "s3vectors:QueryVectors", "arn:aws:s3vectors:us-east-1:000000000000:bucket/embeddings/index/docs", &s3tables.PolicyContext{TableBucketTags: map[string]string{"classification": "private"}})
	require.False(t, allowed)
	require.True(t, denied)
}
