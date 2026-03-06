package request_id

import (
	"context"
	"crypto/rand"
	"fmt"
	"net/http"
	"time"
)

const AmzRequestIDHeader = "x-amz-request-id"

type contextKey struct{}

func Set(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, contextKey{}, id)
}

func Get(ctx context.Context) string {
	if ctx == nil {
		return ""
	}
	id, _ := ctx.Value(contextKey{}).(string)
	return id
}

func InjectToRequest(ctx context.Context, req *http.Request) {
	if req != nil {
		req.Header.Set(AmzRequestIDHeader, Get(ctx))
	}
}

func New() string {
	var buf [4]byte
	rand.Read(buf[:])
	return fmt.Sprintf("%X%08X", time.Now().UTC().UnixNano(), buf)
}

// GetFromRequest returns the server-generated request ID from the context.
func GetFromRequest(r *http.Request) string {
	if r == nil {
		return ""
	}
	return Get(r.Context())
}

// Ensure guarantees a server-generated request ID exists in the context.
// It always generates a new ID if one is not already present in the context,
// ignoring any client-sent x-amz-request-id header to prevent spoofing.
func Ensure(r *http.Request) (*http.Request, string) {
	if r == nil {
		return nil, ""
	}
	if id := Get(r.Context()); id != "" {
		return r, id
	}
	id := New()
	r = r.WithContext(Set(r.Context(), id))
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
