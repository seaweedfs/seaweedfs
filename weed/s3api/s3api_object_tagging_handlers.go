package s3api

import (
	"encoding/xml"
	"fmt"
	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/pb/filer_pb"
	"github.com/chrislusf/seaweedfs/weed/s3api/s3err"
	"github.com/chrislusf/seaweedfs/weed/util"
	"io"
	"io/ioutil"
	"net/http"
)

// GetObjectTaggingHandler - GET object tagging
// API reference: https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetObjectTagging.html
func (s3a *S3ApiServer) GetObjectTaggingHandler(w http.ResponseWriter, r *http.Request) {

	bucket, object := getBucketAndObject(r)

	target := util.FullPath(fmt.Sprintf("%s/%s%s", s3a.option.BucketsPath, bucket, object))
	dir, name := target.DirAndName()

	tags, err := s3a.getTags(dir, name)
	if err != nil {
		if err == filer_pb.ErrNotFound {
			glog.Errorf("GetObjectTaggingHandler %s: %v", r.URL, err)
			writeErrorResponse(w, s3err.ErrNoSuchKey, r.URL)
		} else {
			glog.Errorf("GetObjectTaggingHandler %s: %v", r.URL, err)
			writeErrorResponse(w, s3err.ErrInternalError, r.URL)
		}
		return
	}

	writeSuccessResponseXML(w, encodeResponse(FromTags(tags)))

}

// PutObjectTaggingHandler Put object tagging
// API reference: https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutObjectTagging.html
func (s3a *S3ApiServer) PutObjectTaggingHandler(w http.ResponseWriter, r *http.Request) {

	bucket, object := getBucketAndObject(r)

	target := util.FullPath(fmt.Sprintf("%s/%s%s", s3a.option.BucketsPath, bucket, object))
	dir, name := target.DirAndName()

	tagging := &Tagging{}
	input, err := ioutil.ReadAll(io.LimitReader(r.Body, r.ContentLength))
	if err != nil {
		glog.Errorf("PutObjectTaggingHandler read input %s: %v", r.URL, err)
		writeErrorResponse(w, s3err.ErrInternalError, r.URL)
		return
	}
	if err = xml.Unmarshal(input, tagging); err != nil {
		glog.Errorf("PutObjectTaggingHandler Unmarshal %s: %v", r.URL, err)
		writeErrorResponse(w, s3err.ErrMalformedXML, r.URL)
		return
	}
	tags := tagging.ToTags()
	if len(tags) > 10 {
		glog.Errorf("PutObjectTaggingHandler tags %s: %d tags more than 10", r.URL, len(tags))
		writeErrorResponse(w, s3err.ErrInvalidTag, r.URL)
		return
	}
	for k, v := range tags {
		if len(k) > 128 {
			glog.Errorf("PutObjectTaggingHandler tags %s: tag key %s longer than 128", r.URL, k)
			writeErrorResponse(w, s3err.ErrInvalidTag, r.URL)
			return
		}
		if len(v) > 256 {
			glog.Errorf("PutObjectTaggingHandler tags %s: tag value %s longer than 256", r.URL, v)
			writeErrorResponse(w, s3err.ErrInvalidTag, r.URL)
			return
		}
	}

	if err = s3a.setTags(dir, name, tagging.ToTags()); err != nil {
		if err == filer_pb.ErrNotFound {
			glog.Errorf("PutObjectTaggingHandler setTags %s: %v", r.URL, err)
			writeErrorResponse(w, s3err.ErrNoSuchKey, r.URL)
		} else {
			glog.Errorf("PutObjectTaggingHandler setTags %s: %v", r.URL, err)
			writeErrorResponse(w, s3err.ErrInternalError, r.URL)
		}
		return
	}

	w.WriteHeader(http.StatusNoContent)

}

// DeleteObjectTaggingHandler Delete object tagging
// API reference: https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteObjectTagging.html
func (s3a *S3ApiServer) DeleteObjectTaggingHandler(w http.ResponseWriter, r *http.Request) {

	bucket, object := getBucketAndObject(r)

	target := util.FullPath(fmt.Sprintf("%s/%s%s", s3a.option.BucketsPath, bucket, object))
	dir, name := target.DirAndName()

	err := s3a.rmTags(dir, name)
	if err != nil {
		if err == filer_pb.ErrNotFound {
			glog.Errorf("DeleteObjectTaggingHandler %s: %v", r.URL, err)
			writeErrorResponse(w, s3err.ErrNoSuchKey, r.URL)
		} else {
			glog.Errorf("DeleteObjectTaggingHandler %s: %v", r.URL, err)
			writeErrorResponse(w, s3err.ErrInternalError, r.URL)
		}
		return
	}

	w.WriteHeader(http.StatusNoContent)
}
