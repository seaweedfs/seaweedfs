package s3api

import (
	"net/http"
	"net/url"
	"strings"

	"github.com/gorilla/mux"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3err"
)

func hasPathSegmentQuery(rawQuery string) bool {
	if strings.Contains(rawQuery, "versionId") || strings.Contains(rawQuery, "uploadId") {
		return true
	}
	if !strings.Contains(rawQuery, "%") {
		return false
	}

	for rawQuery != "" {
		field := rawQuery
		if i := strings.IndexByte(rawQuery, '&'); i >= 0 {
			field, rawQuery = rawQuery[:i], rawQuery[i+1:]
		} else {
			rawQuery = ""
		}
		if i := strings.IndexByte(field, '='); i >= 0 {
			field = field[:i]
		}
		if !strings.Contains(field, "%") {
			continue
		}
		key, err := url.QueryUnescape(field)
		if err == nil && (key == "versionId" || key == "uploadId") {
			return true
		}
	}
	return false
}

// operationSubresources are the query keys that select which operation a request
// is, so at most one may appear. The router matches them in registration order
// and the IAM action resolver in its own, so a request carrying two is
// authorized as one operation and served as another: `PUT /bucket?policy&tagging`
// authorizes as PutBucketTagging and runs PutBucketPolicy. Keys left out here
// (versionId, partNumber, prefix, ...) modify an operation instead of selecting
// one and may accompany any of these.
var operationSubresources = map[string]bool{
	"accelerate": true, "acl": true, "analytics": true, "attributes": true,
	"cors": true, "delete": true, "encryption": true, "intelligent-tiering": true,
	"inventory": true, "legal-hold": true, "lifecycle": true, "location": true,
	"logging": true, "metrics": true, "notification": true, "object-lock": true,
	"ownershipControls": true, "policy": true, "policyStatus": true,
	"publicAccessBlock": true, "renameObject": true, "replication": true,
	"requestPayment": true, "retention": true, "tagging": true, "uploadId": true,
	"uploads": true, "versioning": true, "versions": true, "website": true,
}

func hasAmbiguousSubresource(query url.Values) bool {
	seen := 0
	for key := range query {
		if !operationSubresources[key] {
			continue
		}
		if seen++; seen > 1 {
			return true
		}
	}
	return false
}

func hasInvalidPathSegment(values []string) bool {
	for _, value := range values {
		if value != "" && !s3_constants.IsValidPathSegment(value) {
			return true
		}
	}
	return false
}

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
		// When a var is in the matched route it must be non-empty: an empty
		// bucket would let downstream path.Join collapse it and let the object
		// key pick the bucket.
		if bucket, ok := vars["bucket"]; ok {
			if bucket == "" || !s3_constants.IsValidBucketName(bucket) {
				s3err.WriteErrorResponse(w, r, s3err.ErrInvalidRequest)
				return
			}
		}
		if object, ok := vars["object"]; ok {
			if object == "" || !s3_constants.IsValidObjectKey(object) {
				s3err.WriteErrorResponse(w, r, s3err.ErrInvalidRequest)
				return
			}
		}
		if r.URL.RawQuery != "" {
			query := r.URL.Query()
			if hasAmbiguousSubresource(query) {
				s3err.WriteErrorResponse(w, r, s3err.ErrInvalidRequest)
				return
			}
			// versionId and uploadId are later used as filer entry names, and
			// the encoded spelling of either still names one.
			if hasPathSegmentQuery(r.URL.RawQuery) {
				if hasInvalidPathSegment(query["versionId"]) || hasInvalidPathSegment(query["uploadId"]) {
					s3err.WriteErrorResponse(w, r, s3err.ErrInvalidRequest)
					return
				}
			}
		}
		next.ServeHTTP(w, r)
	})
}
