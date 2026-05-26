package s3api

import (
	"net/http"

	"github.com/gorilla/mux"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3err"
)

// validateRequestPath rejects requests whose captured {bucket}/{object} mux
// vars would normalize to a parent-directory traversal once joined into a
// filer path. The router runs with mux.NewRouter().SkipClean(true), so
// segments like `..` survive routing; the filer's util.JoinPath later collapses
// them via filepath.Join. Without this guard, `GET /bucket-A/../evil-bucket/k`
// matches as bucket=bucket-A, object=../evil-bucket/k, the filer resolves the
// read against evil-bucket, while IAM authorizes against bucket-A.
func validateRequestPath(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		vars := mux.Vars(r)
		if !s3_constants.IsValidBucketName(vars["bucket"]) {
			s3err.WriteErrorResponse(w, r, s3err.ErrInvalidRequest)
			return
		}
		if !s3_constants.IsValidObjectKey(vars["object"]) {
			s3err.WriteErrorResponse(w, r, s3err.ErrInvalidRequest)
			return
		}
		next.ServeHTTP(w, r)
	})
}
