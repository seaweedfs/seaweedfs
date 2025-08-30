package s3api

import (
	"encoding/xml"
	"io"
	"net/http"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3err"
)

// GetBucketTaggingHandler Returns the tag set associated with the bucket
// https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetBucketTagging.html
func (s3a *S3ApiServer) GetBucketTaggingHandler(w http.ResponseWriter, r *http.Request) {
	bucket, _ := s3_constants.GetBucketAndObject(r)
	glog.V(3).Infof("GetBucketTagging %s", bucket)

	if err := s3a.checkBucket(r, bucket); err != s3err.ErrNone {
		s3err.WriteErrorResponse(w, r, err)
		return
	}

	// Load bucket metadata and extract tags
	metadata, err := s3a.GetBucketMetadata(bucket)
	if err != nil {
		glog.V(3).Infof("GetBucketTagging: failed to get bucket metadata for %s: %v", bucket, err)
		s3err.WriteErrorResponse(w, r, s3err.ErrInternalError)
		return
	}

	if len(metadata.Tags) == 0 {
		glog.V(3).Infof("GetBucketTagging: no tags found for bucket %s", bucket)
		s3err.WriteErrorResponse(w, r, s3err.ErrNoSuchTagSet)
		return
	}

	tags := metadata.Tags

	// Convert tags to XML response format
	tagging := FromTags(tags)
	writeSuccessResponseXML(w, r, tagging)
}

// PutBucketTaggingHandler Put bucket tagging
// https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutBucketTagging.html
func (s3a *S3ApiServer) PutBucketTaggingHandler(w http.ResponseWriter, r *http.Request) {
	bucket, _ := s3_constants.GetBucketAndObject(r)
	glog.V(3).Infof("PutBucketTagging %s", bucket)

	if err := s3a.checkBucket(r, bucket); err != s3err.ErrNone {
		s3err.WriteErrorResponse(w, r, err)
		return
	}

	// Parse tagging configuration from request body
	tagging := &Tagging{}
	input, err := io.ReadAll(io.LimitReader(r.Body, r.ContentLength))
	if err != nil {
		glog.Errorf("PutBucketTagging read input %s: %v", r.URL, err)
		s3err.WriteErrorResponse(w, r, s3err.ErrInternalError)
		return
	}
	if err = xml.Unmarshal(input, tagging); err != nil {
		glog.Errorf("PutBucketTagging Unmarshal %s: %v", r.URL, err)
		s3err.WriteErrorResponse(w, r, s3err.ErrMalformedXML)
		return
	}

	tags := tagging.ToTags()

	// Validate tags using existing validation
	err = ValidateTags(tags)
	if err != nil {
		glog.Errorf("PutBucketTagging ValidateTags error %s: %v", r.URL, err)
		s3err.WriteErrorResponse(w, r, s3err.ErrInvalidTag)
		return
	}

	// Store bucket tags in metadata
	if err = s3a.UpdateBucketTags(bucket, tags); err != nil {
		glog.Errorf("PutBucketTagging UpdateBucketTags %s: %v", r.URL, err)
		s3err.WriteErrorResponse(w, r, s3err.ErrInternalError)
		return
	}

	writeSuccessResponseEmpty(w, r)
}

// DeleteBucketTaggingHandler Delete bucket tagging
// https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteBucketTagging.html
func (s3a *S3ApiServer) DeleteBucketTaggingHandler(w http.ResponseWriter, r *http.Request) {
	bucket, _ := s3_constants.GetBucketAndObject(r)
	glog.V(3).Infof("DeleteBucketTagging %s", bucket)

	if err := s3a.checkBucket(r, bucket); err != s3err.ErrNone {
		s3err.WriteErrorResponse(w, r, err)
		return
	}

	// Remove bucket tags from metadata
	if err := s3a.ClearBucketTags(bucket); err != nil {
		glog.Errorf("DeleteBucketTagging ClearBucketTags %s: %v", r.URL, err)
		s3err.WriteErrorResponse(w, r, s3err.ErrInternalError)
		return
	}

	w.WriteHeader(http.StatusNoContent)
	s3err.PostLog(r, http.StatusNoContent, s3err.ErrNone)
}
