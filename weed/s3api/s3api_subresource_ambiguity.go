package s3api

import (
	"net/http"

	"github.com/seaweedfs/seaweedfs/weed/s3api/s3err"
)

// rejectAmbiguousSubresource applies the same one-operation-per-request rule
// validateRequestPath enforces inside bucket routes to the routes outside them,
// so the check runs once for every request the router serves.
func rejectAmbiguousSubresource(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if hasAmbiguousSubresource(r.URL.Query()) {
			s3err.WriteErrorResponse(w, r, s3err.ErrInvalidRequest)
			return
		}
		next.ServeHTTP(w, r)
	})
}
