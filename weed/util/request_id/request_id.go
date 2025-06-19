package request_id

import (
	"context"
	"net/http"
)

const (
	RequestIdHttpHeader = "X-Request-ID"
	RequestIDKey        = "x-request-id"
)

func Set(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, RequestIDKey, id)
}

func Get(ctx context.Context) string {
	if ctx == nil {
		return ""
	}
	id, _ := ctx.Value(RequestIDKey).(string)
	return id
}

func InjectToRequest(ctx context.Context, req *http.Request) {
	if req != nil {
		req.Header.Set(RequestIdHttpHeader, Get(ctx))
	}
}
