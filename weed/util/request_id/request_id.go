package request_id

import (
	"context"
	"net/http"
)

const AmzRequestIDHeader = "x-amz-request-id"

func Set(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, AmzRequestIDHeader, id)
}

func Get(ctx context.Context) string {
	if ctx == nil {
		return ""
	}
	id, _ := ctx.Value(AmzRequestIDHeader).(string)
	return id
}

func InjectToRequest(ctx context.Context, req *http.Request) {
	if req != nil {
		req.Header.Set(AmzRequestIDHeader, Get(ctx))
	}
}
