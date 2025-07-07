package s3api

import (
	"encoding/xml"
	"net/http"

	"github.com/aws/aws-sdk-go/private/protocol/xml/xmlutil"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3err"
	util_http "github.com/seaweedfs/seaweedfs/weed/util/http"
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

// PutBucketVersioningHandler Put bucket Versioning
// https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutBucketVersioning.html
func (s3a *S3ApiServer) PutBucketVersioningHandler(w http.ResponseWriter, r *http.Request) {
	bucket, _ := s3_constants.GetBucketAndObject(r)
	glog.V(3).Infof("PutBucketVersioning %s", bucket)

	if err := s3a.checkBucket(r, bucket); err != s3err.ErrNone {
		s3err.WriteErrorResponse(w, r, err)
		return
	}

	if r.Body == nil || r.Body == http.NoBody {
		s3err.WriteErrorResponse(w, r, s3err.ErrInvalidRequest)
		return
	}

	var versioningConfig s3.VersioningConfiguration
	defer util_http.CloseRequest(r)

	err := xmlutil.UnmarshalXML(&versioningConfig, xml.NewDecoder(r.Body), "")
	if err != nil {
		glog.Warningf("PutBucketVersioningHandler xml decode: %s", err)
		s3err.WriteErrorResponse(w, r, s3err.ErrMalformedXML)
		return
	}

	if versioningConfig.Status == nil {
		s3err.WriteErrorResponse(w, r, s3err.ErrInvalidRequest)
		return
	}

	status := *versioningConfig.Status
	if status != "Enabled" && status != "Suspended" {
		s3err.WriteErrorResponse(w, r, s3err.ErrInvalidRequest)
		return
	}

	// Update bucket metadata with versioning configuration
	bucketEntry, err := s3a.getEntry(s3a.option.BucketsPath, bucket)
	if err != nil {
		if err == filer_pb.ErrNotFound {
			s3err.WriteErrorResponse(w, r, s3err.ErrNoSuchBucket)
			return
		}
		s3err.WriteErrorResponse(w, r, s3err.ErrInternalError)
		return
	}

	if bucketEntry.Extended == nil {
		bucketEntry.Extended = make(map[string][]byte)
	}
	bucketEntry.Extended[s3_constants.ExtVersioningKey] = []byte(status)

	err = s3a.updateEntry(s3a.option.BucketsPath, bucketEntry)
	if err != nil {
		glog.Errorf("PutBucketVersioningHandler save config: %s", err)
		s3err.WriteErrorResponse(w, r, s3err.ErrInternalError)
		return
	}

	writeSuccessResponseEmpty(w, r)
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
