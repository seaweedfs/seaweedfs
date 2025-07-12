package s3api

import (
	"net/http"
)

// GetObjectAclHandler Get object ACL
// https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetObjectAcl.html
func (s3a *S3ApiServer) GetObjectAclHandler(w http.ResponseWriter, r *http.Request) {

	w.WriteHeader(http.StatusNoContent)

}

// PutObjectAclHandler Put object ACL
// https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutObjectAcl.html
func (s3a *S3ApiServer) PutObjectAclHandler(w http.ResponseWriter, r *http.Request) {

	w.WriteHeader(http.StatusNoContent)

}
