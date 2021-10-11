package s3api

import (
	"net/http"
)

// GetObjectAclHandler Put object ACL
// https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetObjectAcl.html
func (s3a *S3ApiServer) GetObjectAclHandler(w http.ResponseWriter, r *http.Request) {

	w.WriteHeader(http.StatusNoContent)

}

// PutObjectAclHandler Put object ACL
// https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutObjectAcl.html
func (s3a *S3ApiServer) PutObjectAclHandler(w http.ResponseWriter, r *http.Request) {

	w.WriteHeader(http.StatusNoContent)

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
