package s3api

import (
	"net/http"

	"github.com/chrislusf/seaweedfs/weed/s3api/s3err"
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
	s3err.WriteErrorResponse(w, r, s3err.ErrNotImplemented)
}
