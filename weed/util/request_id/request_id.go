package request_id

import (
	"context"
	"net/http"

	"github.com/google/uuid"
)

const AmzRequestIDHeader = "x-amz-request-id"
const LegacyRequestIDHeader = "X-Request-ID"

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

func New() string {
	return uuid.New().String()
}

func GetFromRequest(r *http.Request) string {
	if r == nil {
		return ""
	}
	if id := Get(r.Context()); id != "" {
		return id
	}
	if id := r.Header.Get(AmzRequestIDHeader); id != "" {
		return id
	}
	return r.Header.Get(LegacyRequestIDHeader)
}

func Ensure(r *http.Request) (*http.Request, string) {
	if r == nil {
		return nil, ""
	}
	id := GetFromRequest(r)
	if id == "" {
		id = New()
	}
	r = r.WithContext(Set(r.Context(), id))
	r.Header.Set(AmzRequestIDHeader, id)
	return r, id
}

func Middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		r, reqID := Ensure(r)
		if w.Header().Get(AmzRequestIDHeader) == "" {
			w.Header().Set(AmzRequestIDHeader, reqID)
		}
		next.ServeHTTP(w, r)
	})
}
