package policy_engine

import (
	"context"
	"net/http"
)

// originalGrantConditionsKey 避免客户端通过请求头伪造内部原始授权值。
type originalGrantConditionsKey struct{}

// isGrantConditionKey 把原始值补充检查限定为上传授权头的五种条件键。
func isGrantConditionKey(key string) bool {
	switch key {
	case "s3:x-amz-grant-read", "s3:x-amz-grant-write", "s3:x-amz-grant-read-acp", "s3:x-amz-grant-write-acp", "s3:x-amz-grant-full-control":
		return true
	}
	return false
}

// cloneGrantConditions 隔离输入和返回值，防止后续修改改变签名时的完整授权表示。
func cloneGrantConditions(values map[string][]string) map[string][]string {
	if len(values) == 0 {
		return nil
	}
	cloned := make(map[string][]string, len(values))
	for key, grants := range values {
		if isGrantConditionKey(key) {
			cloned[key] = append([]string(nil), grants...)
		}
	}
	return cloned
}

// WithOriginalGrantConditions 保存上传规范化前的完整列表，仅补充显式拒绝检查。
func WithOriginalGrantConditions(r *http.Request, values map[string][]string) *http.Request {
	return r.WithContext(context.WithValue(r.Context(), originalGrantConditionsKey{}, cloneGrantConditions(values)))
}

// OriginalGrantConditionsFromRequest 读取内部快照，其他操作没有此上下文。
func OriginalGrantConditionsFromRequest(r *http.Request) map[string][]string {
	if r == nil {
		return nil
	}
	values, _ := r.Context().Value(originalGrantConditionsKey{}).(map[string][]string)
	return cloneGrantConditions(values)
}
