package s3api

import (
	"github.com/aws/aws-sdk-go/private/protocol/json/jsonutil"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3err"
	"net/http"
)

// GetObjectAclHandler Put object ACL
// https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetObjectAcl.html
func (s3a *S3ApiServer) GetObjectAclHandler(w http.ResponseWriter, r *http.Request) {
	bucket, object := s3_constants.GetBucketAndObject(r)
	errCode, acp := s3a.ObjectAclReadAccess(r, &s3_constants.PermissionReadAcp, &bucket, &object)
	if errCode != s3err.ErrNone {
		s3err.WriteErrorResponse(w, r, errCode)
		return
	}
	s3err.WriteAwsXMLResponse(w, r, http.StatusOK, &s3.PutObjectAclInput{
		AccessControlPolicy: acp,
	})
}

// PutObjectAclHandler Put object ACL
// https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutObjectAcl.html
func (s3a *S3ApiServer) PutObjectAclHandler(w http.ResponseWriter, r *http.Request) {
	bucket, object := s3_constants.GetBucketAndObject(r)

	errCode, bucketMetadata, entry, accountId, acp := s3a.ObjectAclWriteAccess(r, &bucket, &object)
	if errCode != s3err.ErrNone {
		s3err.WriteErrorResponse(w, r, errCode)
		return
	}

	acp, errCode = s3a.ParseAcp(r, bucketMetadata.ObjectOwnership, bucketMetadata.Owner.ID, acp.Owner.ID, accountId)
	if errCode != s3err.ErrNone {
		s3err.WriteErrorResponse(w, r, errCode)
		return
	}

	aclJson, _ := jsonutil.BuildJSON(acp)
	entry.Extended[s3_constants.ExtAcpKey] = aclJson

	_, err := s3a.updateEntry(bucket, entry)
	if err != nil {
		s3err.WriteErrorResponse(w, r, s3err.ErrInternalError)
		return
	}
	w.WriteHeader(http.StatusOK)

}

// PutObjectRetentionHandler Put object Retention
// https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutObjectRetention.html
func (s3a *S3ApiServer) PutObjectRetentionHandler(w http.ResponseWriter, r *http.Request) {

	w.WriteHeader(http.StatusNoContent)

}

// PutObjectLegalHoldHandler Put object Legal Hold
// https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutObjectLegalHold.html
func (s3a *S3ApiServer) PutObjectLegalHoldHandler(w http.ResponseWriter, r *http.Request) {

	w.WriteHeader(http.StatusNoContent)

}

// PutObjectLockConfigurationHandler Put object Lock configuration
// https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutObjectLockConfiguration.html
func (s3a *S3ApiServer) PutObjectLockConfigurationHandler(w http.ResponseWriter, r *http.Request) {

	w.WriteHeader(http.StatusNoContent)

}
