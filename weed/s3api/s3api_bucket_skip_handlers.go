package s3api

import (
	"github.com/aws/aws-sdk-go/private/protocol/json/jsonutil"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"net/http"

	"github.com/seaweedfs/seaweedfs/weed/s3api/s3err"
)

// GetBucketCorsHandler Get bucket CORS
// https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetBucketCors.html
func (s3a *S3ApiServer) GetBucketCorsHandler(w http.ResponseWriter, r *http.Request) {
	s3err.WriteErrorResponse(w, r, s3err.ErrNoSuchCORSConfiguration)
}

// PutBucketCorsHandler Put bucket CORS
// https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutBucketCors.html
func (s3a *S3ApiServer) PutBucketCorsHandler(w http.ResponseWriter, r *http.Request) {
	s3err.WriteErrorResponse(w, r, s3err.ErrNotImplemented)
}

// DeleteBucketCorsHandler Delete bucket CORS
// https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteBucketCors.html
func (s3a *S3ApiServer) DeleteBucketCorsHandler(w http.ResponseWriter, r *http.Request) {
	s3err.WriteErrorResponse(w, r, http.StatusNoContent)
}

// GetBucketPolicyHandler Get bucket Policy
// https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetBucketPolicy.html
func (s3a *S3ApiServer) GetBucketPolicyHandler(w http.ResponseWriter, r *http.Request) {
	s3err.WriteErrorResponse(w, r, s3err.ErrNoSuchBucketPolicy)
}

// PutBucketPolicyHandler Put bucket Policy
// https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutBucketPolicy.html
func (s3a *S3ApiServer) PutBucketPolicyHandler(w http.ResponseWriter, r *http.Request) {
	s3err.WriteErrorResponse(w, r, s3err.ErrNotImplemented)
}

// DeleteBucketPolicyHandler Delete bucket Policy
// https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteBucketPolicy.html
func (s3a *S3ApiServer) DeleteBucketPolicyHandler(w http.ResponseWriter, r *http.Request) {
	s3err.WriteErrorResponse(w, r, http.StatusNoContent)
}

// PutBucketAclHandler Put bucket ACL
// https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutBucketAcl.html
func (s3a *S3ApiServer) PutBucketAclHandler(w http.ResponseWriter, r *http.Request) {
	bucket, _ := s3_constants.GetBucketAndObject(r)
	glog.V(3).Infof("PutBucketAclHandler %s", bucket)

	errorCode, bucketMetaData, identAccountId := s3a.BucketAclWriteAccess(r, &bucket)
	if errorCode != s3err.ErrNone {
		s3err.WriteErrorResponse(w, r, errorCode)
		return
	}

	if identAccountId == nil {
		identAccountId = ensureAccountId(r)
	}
	bucketOwnerId := bucketMetaData.Owner.ID
	acp, errCode := s3a.ParseAcp(r, bucketMetaData.ObjectOwnership, bucketOwnerId, bucketOwnerId, identAccountId)
	if errCode != s3err.ErrNone {
		s3err.WriteErrorResponse(w, r, errCode)
		return
	}

	entry, err := s3a.getEntry(s3a.Option.BucketsPath, bucket)
	if err != nil {
		return
	}

	//cannedAcl doesn't include owner in request
	if acp.Owner == nil {
		acp.Owner = bucketMetaData.Owner
	}

	acpBytes, _ := jsonutil.BuildJSON(acp)
	entry.Extended[s3_constants.ExtAcpKey] = acpBytes
	_, err = s3a.updateEntry(s3a.Option.BucketsPath, entry)
	if err != nil {
		s3err.WriteErrorResponse(w, r, s3err.ErrInternalError)
		return
	}
	s3err.WriteEmptyResponse(w, r, http.StatusOK)
}
