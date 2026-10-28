package s3api

import (
	"net/http"

	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3err"
)

func getAccountId(r *http.Request) string {
	id := r.Header.Get(s3_constants.AmzAccountId)
	if len(id) == 0 {
		return AccountAnonymous.Id
	} else {
		return id
	}
}

// requestAccountId returns the authenticated caller's account; with auth off
// the header is client-supplied, so the admin account stands in instead.
func (s3a *S3ApiServer) requestAccountId(r *http.Request) string {
	if s3a.iam != nil && !s3a.iam.isEnabled() {
		return AccountAdmin.Id
	}
	return r.Header.Get(s3_constants.AmzAccountId)
}

// ownerAccountId resolves the account that owns grants and ownerless buckets:
// the requester's account when one is trusted, else the admin account — with
// auth off the account header is client-supplied and cannot claim ownership.
func (s3a *S3ApiServer) ownerAccountId(r *http.Request) string {
	accountId := getAccountId(r)
	if accountId == AccountAnonymous.Id || (s3a.iam != nil && !s3a.iam.isEnabled()) {
		return AccountAdmin.Id
	}
	return accountId
}

func (s3a *S3ApiServer) checkAccessByOwnership(r *http.Request, bucket string) s3err.ErrorCode {
	metadata, errCode := s3a.bucketRegistry.GetBucketMetadata(bucket)
	if errCode != s3err.ErrNone {
		return errCode
	}
	// Without auth there is no owner to compare against.
	if s3a.iam != nil && !s3a.iam.isEnabled() {
		return s3err.ErrNone
	}
	// Admin by capability, not account id: account-less identities share the "admin" id.
	if s3a.isUserAdmin(r) {
		return s3err.ErrNone
	}
	accountId := getAccountId(r)
	if metadata.Owner != nil && metadata.Owner.ID != nil && accountId == *metadata.Owner.ID {
		return s3err.ErrNone
	}
	return s3err.ErrAccessDenied
}
