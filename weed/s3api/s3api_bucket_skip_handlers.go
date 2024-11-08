package s3api

import (
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

// PutBucketVersioningHandler Put bucket Versionin
// https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutBucketVersioning.html
func (s3a *S3ApiServer) PutBucketVersioningHandler(w http.ResponseWriter, r *http.Request) {
	s3err.WriteErrorResponse(w, r, s3err.ErrNotImplemented)
}

// GetBucketTaggingHandler Returns the tag set associated with the bucket
// https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetBucketTagging.html
func (s3a *S3ApiServer) GetBucketTaggingHandler(w http.ResponseWriter, r *http.Request) {
	bucket, _ := s3_constants.GetBucketAndObject(r)
	glog.V(3).Infof("GetBucketTagging %s", bucket)

	if err := s3a.checkBucket(r, bucket); err != s3err.ErrNone {
		s3err.WriteErrorResponse(w, r, err)
		return
	}

	s3err.WriteErrorResponse(w, r, s3err.ErrNoSuchTagSet)
}

func (s3a *S3ApiServer) PutBucketTaggingHandler(w http.ResponseWriter, r *http.Request) {
	s3err.WriteErrorResponse(w, r, s3err.ErrNotImplemented)
}

func (s3a *S3ApiServer) DeleteBucketTaggingHandler(w http.ResponseWriter, r *http.Request) {
	s3err.WriteErrorResponse(w, r, s3err.ErrNotImplemented)
}

// GetBucketEncryptionHandler Returns the default encryption configuration
// https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetBucketEncryption.html
func (s3a *S3ApiServer) GetBucketEncryptionHandler(w http.ResponseWriter, r *http.Request) {
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
