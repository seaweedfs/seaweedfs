package s3api

import (
	"net/http"

	"github.com/seaweedfs/seaweedfs/weed/s3api/s3err"
)

// s3SubresourceQueryParams are the query keys that select a routed S3
// subresource. Values such as versionId, partNumber, id, and the listing and
// presign parameters only modify an operation, so they are not here: a request
// may carry any number of them alongside its one subresource.
//
// A request naming two subresources is ambiguous: the router serves the first
// matching route while authorization resolves a single action, and the pair
// can name different operations (PUT ?tagging&policy is authorized as
// PutBucketTagging but served by PutBucketPolicyHandler). Rather than guess
// which one the caller meant, such requests are rejected.
var s3SubresourceQueryParams = map[string]bool{
	"accelerate": true, "acl": true, "analytics": true, "attributes": true,
	"cors": true, "delete": true, "encryption": true, "intelligent-tiering": true,
	"inventory": true, "legal-hold": true, "lifecycle": true, "list-type": true,
	"location": true, "logging": true, "metrics": true, "notification": true,
	"object-lock": true, "ownershipControls": true, "policy": true,
	"policyStatus": true, "publicAccessBlock": true, "renameObject": true,
	"replication": true, "requestPayment": true, "retention": true,
	"seaweedfs-quota": true, "tagging": true, "uploadId": true, "uploads": true,
	"versions": true, "website": true,
}

func rejectAmbiguousSubresource(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		subresources := 0
		for key := range r.URL.Query() {
			if s3SubresourceQueryParams[key] {
				subresources++
			}
		}
		if subresources > 1 {
			s3err.WriteErrorResponse(w, r, s3err.ErrInvalidRequest)
			return
		}
		next.ServeHTTP(w, r)
	})
}
