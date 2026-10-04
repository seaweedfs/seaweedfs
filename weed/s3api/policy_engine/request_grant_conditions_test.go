package policy_engine

import (
	"fmt"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestOriginalGrantConditionsDeny 验证原始正条件拒绝生效，允许及负条件不扩张。
func TestOriginalGrantConditionsDeny(t *testing.T) {
	const key = "s3:x-amz-grant-read"
	const canonical, original = `id="bucket-owner"`, `id = "bucket-\u006fwner"`
	tests := []struct {
		name, effect, conditions string
		result                   PolicyEvaluationResult
	}{
		{"原始精确拒绝", "Deny", fmt.Sprintf(`{"StringEquals":{%q:%q}}`, key, original), PolicyResultDeny},
		{"原始通配拒绝", "Deny", fmt.Sprintf(`{"StringLike":{%q:%q}}`, key, `id = *`), PolicyResultDeny},
		{"规范化拒绝保持", "Deny", fmt.Sprintf(`{"StringEquals":{%q:%q}}`, key, canonical), PolicyResultDeny},
		{"获准负条件不误拒", "Deny", fmt.Sprintf(`{"StringNotEquals":{%q:%q}}`, key, canonical), PolicyResultIndeterminate},
		{"负条件拒绝保持", "Deny", fmt.Sprintf(`{"StringNotEquals":{%q:%q}}`, key, `id="other"`), PolicyResultDeny},
		{"原始值不产生允许", "Allow", fmt.Sprintf(`{"StringEquals":{%q:%q}}`, key, original), PolicyResultIndeterminate},
		{"负条件允许不扩大", "Allow", fmt.Sprintf(`{"StringNotEquals":{%q:%q}}`, key, canonical), PolicyResultIndeterminate},
		{"已有完整列表允许保持", "Allow", fmt.Sprintf(`{"StringEquals":{%q:%q}}`, key, canonical), PolicyResultAllow},
		{"同键正负条件分开求值", "Deny", fmt.Sprintf(`{"StringEquals":{%q:%q},"StringNotEquals":{%q:%q}}`, key, original, key, canonical), PolicyResultIndeterminate},
		{"其他条件仍需匹配", "Deny", fmt.Sprintf(`{"StringEquals":{%q:%q,"aws:username":"other"}}`, key, original), PolicyResultIndeterminate},
		{"变量仍使用规范化值", "Deny", fmt.Sprintf(`{"StringEquals":{%q:"${s3:x-amz-grant-read}"}}`, key), PolicyResultDeny},
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

// TestOriginalGrantConditionsSnapshot 验证原始值不能由请求头伪造或被后续修改。
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
