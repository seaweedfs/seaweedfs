package s3api

import (
	"net/http"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3err"
)

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

// GetBucketEncryptionHandler Returns the default encryption configuration
// https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetBucketEncryption.html
func (s3a *S3ApiServer) GetBucketEncryptionHandler(w http.ResponseWriter, r *http.Request) {
	bucket, _ := s3_constants.GetBucketAndObject(r)
	glog.V(3).Infof("GetBucketEncryption %s", bucket)

	if err := s3a.checkBucket(r, bucket); err != s3err.ErrNone {
		s3err.WriteErrorResponse(w, r, err)
		return
	}

	s3err.WriteErrorResponse(w, r, s3err.ErrNotImplemented)
}

func (s3a *S3ApiServer) PutBucketEncryptionHandler(w http.ResponseWriter, r *http.Request) {
	s3err.WriteErrorResponse(w, r, s3err.ErrNotImplemented)
}

func (s3a *S3ApiServer) DeleteBucketEncryptionHandler(w http.ResponseWriter, r *http.Request) {
	s3err.WriteErrorResponse(w, r, s3err.ErrNotImplemented)
}

// GetPublicAccessBlockHandler Retrieves the PublicAccessBlock configuration for an S3 bucket
// https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetPublicAccessBlock.html
func (s3a *S3ApiServer) GetPublicAccessBlockHandler(w http.ResponseWriter, r *http.Request) {
	s3err.WriteErrorResponse(w, r, s3err.ErrNotImplemented)
}

func (s3a *S3ApiServer) PutPublicAccessBlockHandler(w http.ResponseWriter, r *http.Request) {
	s3err.WriteErrorResponse(w, r, s3err.ErrNotImplemented)
}

func (s3a *S3ApiServer) DeletePublicAccessBlockHandler(w http.ResponseWriter, r *http.Request) {
	s3err.WriteErrorResponse(w, r, s3err.ErrNotImplemented)
}
