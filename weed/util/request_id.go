package util

import "context"

const (
	RequestIdHttpHeader = "X-Request-ID"
	RequestIDKey        = "x-request-id"
)

func GetRequestID(ctx context.Context) string {
	if ctx == nil {
		return ""
	}
	id, _ := ctx.Value(RequestIDKey).(string)
	return id
}

func WithRequestID(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, RequestIDKey, id)
}
