package policy_engine

import (
	"fmt"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestOriginalGrantConditionsDeny verifies original positive denies without expanding allows or negative conditions.
func TestOriginalGrantConditionsDeny(t *testing.T) {
	const key = "s3:x-amz-grant-read"
	const canonical, original = `id="bucket-owner"`, `id = "bucket-\u006fwner"`
	tests := []struct {
		name, effect, conditions string
		result                   PolicyEvaluationResult
	}{
		{"original exact deny", "Deny", fmt.Sprintf(`{"StringEquals":{%q:%q}}`, key, original), PolicyResultDeny},
		{"original wildcard deny", "Deny", fmt.Sprintf(`{"StringLike":{%q:%q}}`, key, `id = *`), PolicyResultDeny},
		{"canonical deny unchanged", "Deny", fmt.Sprintf(`{"StringEquals":{%q:%q}}`, key, canonical), PolicyResultDeny},
		{"approved negative condition not denied", "Deny", fmt.Sprintf(`{"StringNotEquals":{%q:%q}}`, key, canonical), PolicyResultIndeterminate},
		{"negative deny unchanged", "Deny", fmt.Sprintf(`{"StringNotEquals":{%q:%q}}`, key, `id="other"`), PolicyResultDeny},
		{"original value cannot allow", "Allow", fmt.Sprintf(`{"StringEquals":{%q:%q}}`, key, original), PolicyResultIndeterminate},
		{"negative allow not expanded", "Allow", fmt.Sprintf(`{"StringNotEquals":{%q:%q}}`, key, canonical), PolicyResultIndeterminate},
		{"complete list allow unchanged", "Allow", fmt.Sprintf(`{"StringEquals":{%q:%q}}`, key, canonical), PolicyResultAllow},
		{"positive and negative conditions evaluated separately", "Deny", fmt.Sprintf(`{"StringEquals":{%q:%q},"StringNotEquals":{%q:%q}}`, key, original, key, canonical), PolicyResultIndeterminate},
		{"other conditions must still match", "Deny", fmt.Sprintf(`{"StringEquals":{%q:%q,"aws:username":"other"}}`, key, original), PolicyResultIndeterminate},
		{"variables remain canonical", "Deny", fmt.Sprintf(`{"StringEquals":{%q:"${s3:x-amz-grant-read}"}}`, key), PolicyResultDeny},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			engine := NewPolicyEngine()
			policy := fmt.Sprintf(`{"Version":"2012-10-17","Statement":[{"Effect":%q,"Principal":"*","Action":"s3:PutObjectAcl","Resource":"arn:aws:s3:::bucket/object","Condition":%s}]}`, tt.effect, tt.conditions)
			require.NoError(t, engine.SetBucketPolicy("bucket", policy))
			req := WithOriginalGrantConditions(httptest.NewRequest("PUT", "/bucket/object", nil), map[string][]string{key: {original}})
			args := &PolicyEvaluationArgs{
				Action: "s3:PutObjectAcl", Resource: "arn:aws:s3:::bucket/object", Principal: "upload-writer",
				Conditions:              map[string][]string{key: {canonical}, "aws:username": {"upload-writer"}},
				OriginalGrantConditions: OriginalGrantConditionsFromRequest(req),
			}
			require.Equal(t, tt.result, engine.EvaluatePolicy("bucket", args))
		})
	}
}

// TestOriginalGrantConditionsSnapshot verifies original values cannot be forged through headers or later mutations.
func TestOriginalGrantConditionsSnapshot(t *testing.T) {
	const key = "s3:x-amz-grant-read"
	values := map[string][]string{key: {`id = "bucket-owner"`}, "aws:username": {"forged"}}
	original := httptest.NewRequest("PUT", "/bucket/object", nil)
	original.Header.Set("X-Amz-Original-Grant-Read", `id="attacker"`)
	require.Nil(t, OriginalGrantConditionsFromRequest(original))
	require.Nil(t, OriginalGrantConditionsFromRequest(nil))
	req := WithOriginalGrantConditions(original, values)
	values[key][0] = `id="attacker"`
	delete(values, key)
	first := OriginalGrantConditionsFromRequest(req)
	require.Equal(t, `id = "bucket-owner"`, first[key][0])
	require.NotContains(t, first, "aws:username")
	first[key][0] = `id="attacker"`
	require.Equal(t, `id = "bucket-owner"`, OriginalGrantConditionsFromRequest(req)[key][0])
}
