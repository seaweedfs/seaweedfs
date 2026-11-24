package s3tables

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/iam/integration"
	"github.com/seaweedfs/seaweedfs/weed/iam/policy"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/stretchr/testify/require"
)

type policyEngineAuthorizer struct {
	engine *policy.PolicyEngine
}

func (a policyEngineAuthorizer) IsActionAllowed(ctx context.Context, req *integration.ActionRequest) (bool, error) {
	result, err := a.engine.Evaluate(ctx, "", &policy.EvaluationContext{
		Principal:      req.Principal,
		Action:         req.Action,
		Resource:       req.Resource,
		RequestContext: req.RequestContext,
	}, req.PolicyNames)
	if err != nil {
		return false, err
	}
	return result.Effect == policy.EffectAllow, nil
}

// A role policy Deny keyed on aws:RequestTag must see the tags a
// CreateTableBucket request asks for.
func TestAuthorizeIAMActionSeesRequestTags(t *testing.T) {
	engine := policy.NewPolicyEngine()
	require.NoError(t, engine.Initialize(&policy.PolicyEngineConfig{DefaultEffect: "Deny", StoreType: "memory"}))
	var doc policy.PolicyDocument
	require.NoError(t, json.Unmarshal([]byte(`{"Version":"2012-10-17","Statement":[
		{"Effect":"Allow","Action":"s3tables:CreateTableBucket","Resource":"*"},
		{"Effect":"Deny","Action":"s3tables:CreateTableBucket","Resource":"*","Condition":{"StringEquals":{"aws:RequestTag/env":"prod"}}}]}`), &doc))
	require.NoError(t, engine.AddPolicy("", "tagged", &doc))

	h := &S3TablesHandler{}
	h.SetIAMAuthorizer(policyEngineAuthorizer{engine: engine})
	r := httptest.NewRequest("PUT", "/buckets", nil)
	r.Header.Set(s3_constants.SeaweedFSPrincipalHeader, "arn:aws:sts::000000000000:assumed-role/creator/s")
	bucketARN := h.generateTableBucketARN(DefaultAccountID, "tagged")

	allowed, err := h.authorizeIAMAction(r, []string{"tagged"}, "CreateTableBucket", map[string]string{"env": "dev"}, bucketARN)
	require.NoError(t, err)
	require.True(t, allowed)

	allowed, _ = h.authorizeIAMAction(r, []string{"tagged"}, "CreateTableBucket", map[string]string{"env": "prod"}, bucketARN)
	require.False(t, allowed)

	var paired policy.PolicyDocument
	require.NoError(t, json.Unmarshal([]byte(`{"Version":"2012-10-17","Statement":[
		{"Effect":"Allow","Action":"s3tables:CreateTableBucket","Resource":"*","Condition":{"StringEquals":{"aws:RequestTag/owner":"${aws:RequestTag/team}"}}}]}`), &paired))
	require.NoError(t, engine.AddPolicy("", "paired", &paired))
	allowed, err = h.authorizeIAMAction(r, []string{"paired"}, "CreateTableBucket", map[string]string{"owner": "data", "team": "data"}, bucketARN)
	require.NoError(t, err)
	require.True(t, allowed)
	allowed, _ = h.authorizeIAMAction(r, []string{"paired"}, "CreateTableBucket", map[string]string{"owner": "data", "team": "ops"}, bucketARN)
	require.False(t, allowed)
}
