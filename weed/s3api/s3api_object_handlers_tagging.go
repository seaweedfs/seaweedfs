package s3api

import (
	"context"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"

	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3err"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

// GetObjectTaggingHandler - GET object tagging
// API reference: https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetObjectTagging.html
func (s3a *S3ApiServer) GetObjectTaggingHandler(w http.ResponseWriter, r *http.Request) {

	bucket, object := s3_constants.GetBucketAndObject(r)
	glog.V(3).Infof("GetObjectTaggingHandler %s %s", bucket, object)

	// Check for specific version ID in query parameters
	versionId := r.URL.Query().Get("versionId")

	// Check if versioning is configured for the bucket (Enabled or Suspended)
	versioningConfigured, err := s3a.isVersioningConfigured(bucket)
	if err != nil {
		if err == filer_pb.ErrNotFound {
			s3err.WriteErrorResponse(w, r, s3err.ErrNoSuchBucket)
			return
		}
		glog.Errorf("GetObjectTaggingHandler: Error checking versioning status for bucket %s: %v", bucket, err)
		s3err.WriteErrorResponse(w, r, s3err.ErrInternalError)
		return
	}

	var entry *filer_pb.Entry

	if versioningConfigured {
		// Handle versioned object tagging retrieval
		if versionId != "" {
			// Request for specific version
			glog.V(2).Infof("GetObjectTaggingHandler: requesting tags for specific version %s of %s%s", versionId, bucket, object)
			entry, err = s3a.getSpecificObjectVersion(bucket, object, versionId)
		} else {
			// Request for latest version
			glog.V(2).Infof("GetObjectTaggingHandler: requesting tags for latest version of %s%s", bucket, object)
			entry, err = s3a.getLatestObjectVersion(bucket, object)
		}

		if err != nil {
			glog.Errorf("GetObjectTaggingHandler: Failed to get object version %s for %s%s: %v", versionId, bucket, object, err)
			s3err.WriteErrorResponse(w, r, s3err.ErrNoSuchKey)
			return
		}

		// Check if this is a delete marker
		if entry.Extended != nil {
			if deleteMarker, exists := entry.Extended[s3_constants.ExtDeleteMarkerKey]; exists && string(deleteMarker) == "true" {
				s3err.WriteErrorResponse(w, r, s3err.ErrNoSuchKey)
				return
			}
		}
	} else {
		// Handle regular (non-versioned) object tagging retrieval
		target := util.FullPath(fmt.Sprintf("%s/%s%s", s3a.option.BucketsPath, bucket, object))
		dir, name := target.DirAndName()

		tags, err := s3a.getTags(dir, name)
		if err != nil {
			if err == filer_pb.ErrNotFound {
				glog.Errorf("GetObjectTaggingHandler %s: %v", r.URL, err)
				s3err.WriteErrorResponse(w, r, s3err.ErrNoSuchKey)
			} else {
				glog.Errorf("GetObjectTaggingHandler %s: %v", r.URL, err)
				s3err.WriteErrorResponse(w, r, s3err.ErrInternalError)
			}
			return
		}

		writeSuccessResponseXML(w, r, FromTags(tags))
		return
	}

	// Extract tags from the entry's extended attributes
	tags := make(map[string]string)
	if entry.Extended != nil {
		for k, v := range entry.Extended {
			if len(k) > len(S3TAG_PREFIX) && k[:len(S3TAG_PREFIX)] == S3TAG_PREFIX {
				tags[k[len(S3TAG_PREFIX):]] = string(v)
			}
		}
	}

	writeSuccessResponseXML(w, r, FromTags(tags))

}

// PutObjectTaggingHandler Put object tagging
// API reference: https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutObjectTagging.html
func (s3a *S3ApiServer) PutObjectTaggingHandler(w http.ResponseWriter, r *http.Request) {

	bucket, object := s3_constants.GetBucketAndObject(r)
	glog.V(3).Infof("PutObjectTaggingHandler %s %s", bucket, object)

	tagging := &Tagging{}
	input, err := io.ReadAll(io.LimitReader(r.Body, r.ContentLength))
	if err != nil {
		glog.Errorf("PutObjectTaggingHandler read input %s: %v", r.URL, err)
		s3err.WriteErrorResponse(w, r, s3err.ErrInternalError)
		return
	}
	if err = xml.Unmarshal(input, tagging); err != nil {
		glog.Errorf("PutObjectTaggingHandler Unmarshal %s: %v", r.URL, err)
		s3err.WriteErrorResponse(w, r, s3err.ErrMalformedXML)
		return
	}
	tags := tagging.ToTags()
	err = ValidateTags(tags)
	if err != nil {
		glog.Errorf("PutObjectTaggingHandler ValidateTags error %s: %v", r.URL, err)
		s3err.WriteErrorResponse(w, r, s3err.ErrInvalidTag)
		return
	}

	// Check for specific version ID in query parameters
	versionId := r.URL.Query().Get("versionId")

	// Check if versioning is configured for the bucket (Enabled or Suspended)
	versioningConfigured, err := s3a.isVersioningConfigured(bucket)
	if err != nil {
		if err == filer_pb.ErrNotFound {
			s3err.WriteErrorResponse(w, r, s3err.ErrNoSuchBucket)
			return
		}
		glog.Errorf("PutObjectTaggingHandler: Error checking versioning status for bucket %s: %v", bucket, err)
		s3err.WriteErrorResponse(w, r, s3err.ErrInternalError)
		return
	}

	var entry *filer_pb.Entry

	if versioningConfigured {
		// Handle versioned object tagging modification
		if versionId != "" {
			// Request for specific version
			glog.V(2).Infof("PutObjectTaggingHandler: modifying tags for specific version %s of %s%s", versionId, bucket, object)
			entry, err = s3a.getSpecificObjectVersion(bucket, object, versionId)
		} else {
			// Request for latest version
			glog.V(2).Infof("PutObjectTaggingHandler: modifying tags for latest version of %s%s", bucket, object)
			entry, err = s3a.getLatestObjectVersion(bucket, object)
		}

		if err != nil {
			glog.Errorf("PutObjectTaggingHandler: Failed to get object version %s for %s%s: %v", versionId, bucket, object, err)
			s3err.WriteErrorResponse(w, r, s3err.ErrNoSuchKey)
			return
		}

		// Check if this is a delete marker
		if entry.Extended != nil {
			if deleteMarker, exists := entry.Extended[s3_constants.ExtDeleteMarkerKey]; exists && string(deleteMarker) == "true" {
				s3err.WriteErrorResponse(w, r, s3err.ErrNoSuchKey)
				return
			}
		}
	} else {
		// Handle regular (non-versioned) object tagging modification
		target := util.FullPath(fmt.Sprintf("%s/%s%s", s3a.option.BucketsPath, bucket, object))
		dir, name := target.DirAndName()

		if err = s3a.setTags(dir, name, tags); err != nil {
			if err == filer_pb.ErrNotFound {
				glog.Errorf("PutObjectTaggingHandler setTags %s: %v", r.URL, err)
				s3err.WriteErrorResponse(w, r, s3err.ErrNoSuchKey)
			} else {
				glog.Errorf("PutObjectTaggingHandler setTags %s: %v", r.URL, err)
				s3err.WriteErrorResponse(w, r, s3err.ErrInternalError)
			}
			return
		}

		w.WriteHeader(http.StatusOK)
		s3err.PostLog(r, http.StatusOK, s3err.ErrNone)
		return
	}

	// For versioned objects, determine the correct directory based on the version
	var updateDirectory string
	if versionId != "" {
		// Specific version requested
		if versionId == "null" {
			// Null version (pre-versioning object) - stored as regular file
			updateDirectory = s3a.option.BucketsPath + "/" + bucket
		} else {
			// Versioned object - stored in .versions directory
			updateDirectory = s3a.option.BucketsPath + "/" + bucket + "/" + object + s3_constants.VersionsFolder
		}
	} else {
		// Latest version in versioned bucket - could be null version or versioned object
		// Extract version ID from the entry to determine where it's stored
		var actualVersionId string
		if entry.Extended != nil {
			if versionIdBytes, exists := entry.Extended[s3_constants.ExtVersionIdKey]; exists {
				actualVersionId = string(versionIdBytes)
			}
		}

		if actualVersionId == "null" || actualVersionId == "" {
			// Null version (pre-versioning object) - stored as regular file
			updateDirectory = s3a.option.BucketsPath + "/" + bucket
		} else {
			// Versioned object - stored in .versions directory
			updateDirectory = s3a.option.BucketsPath + "/" + bucket + "/" + object + s3_constants.VersionsFolder
		}
	}

	// Remove old tags and add new ones
	for k := range entry.Extended {
		if len(k) > len(S3TAG_PREFIX) && k[:len(S3TAG_PREFIX)] == S3TAG_PREFIX {
			delete(entry.Extended, k)
		}
	}

	if entry.Extended == nil {
		entry.Extended = make(map[string][]byte)
	}
	for k, v := range tags {
		entry.Extended[S3TAG_PREFIX+k] = []byte(v)
	}

	// Update the entry with tags
	err = s3a.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		request := &filer_pb.UpdateEntryRequest{
			Directory: updateDirectory,
			Entry:     entry,
		}

		if _, err := client.UpdateEntry(context.Background(), request); err != nil {
			return err
		}
		return nil
	})

	if err != nil {
		glog.Errorf("PutObjectTaggingHandler: failed to update entry: %v", err)
		s3err.WriteErrorResponse(w, r, s3err.ErrInternalError)
		return
	}

	glog.V(3).Infof("PutObjectTaggingHandler: Successfully updated tags for %s/%s", bucket, object)
	w.WriteHeader(http.StatusOK)
	s3err.PostLog(r, http.StatusOK, s3err.ErrNone)
}

// DeleteObjectTaggingHandler Delete object tagging
// API reference: https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteObjectTagging.html
func (s3a *S3ApiServer) DeleteObjectTaggingHandler(w http.ResponseWriter, r *http.Request) {

	bucket, object := s3_constants.GetBucketAndObject(r)
	glog.V(3).Infof("DeleteObjectTaggingHandler %s %s", bucket, object)

	// Check for specific version ID in query parameters
	versionId := r.URL.Query().Get("versionId")

	// Check if versioning is configured for the bucket (Enabled or Suspended)
	versioningConfigured, err := s3a.isVersioningConfigured(bucket)
	if err != nil {
		if err == filer_pb.ErrNotFound {
			s3err.WriteErrorResponse(w, r, s3err.ErrNoSuchBucket)
			return
		}
		glog.Errorf("DeleteObjectTaggingHandler: Error checking versioning status for bucket %s: %v", bucket, err)
		s3err.WriteErrorResponse(w, r, s3err.ErrInternalError)
		return
	}

	var entry *filer_pb.Entry

	if versioningConfigured {
		// Handle versioned object tagging deletion
		if versionId != "" {
			// Request for specific version
			glog.V(2).Infof("DeleteObjectTaggingHandler: deleting tags for specific version %s of %s%s", versionId, bucket, object)
			entry, err = s3a.getSpecificObjectVersion(bucket, object, versionId)
		} else {
			// Request for latest version
			glog.V(2).Infof("DeleteObjectTaggingHandler: deleting tags for latest version of %s%s", bucket, object)
			entry, err = s3a.getLatestObjectVersion(bucket, object)
		}

		if err != nil {
			glog.Errorf("DeleteObjectTaggingHandler: Failed to get object version %s for %s%s: %v", versionId, bucket, object, err)
			s3err.WriteErrorResponse(w, r, s3err.ErrNoSuchKey)
			return
		}

		// Check if this is a delete marker
		if entry.Extended != nil {
			if deleteMarker, exists := entry.Extended[s3_constants.ExtDeleteMarkerKey]; exists && string(deleteMarker) == "true" {
				s3err.WriteErrorResponse(w, r, s3err.ErrNoSuchKey)
				return
			}
		}
	} else {
		// Handle regular (non-versioned) object tagging deletion
		target := util.FullPath(fmt.Sprintf("%s/%s%s", s3a.option.BucketsPath, bucket, object))
		dir, name := target.DirAndName()

		err := s3a.rmTags(dir, name)
		if err != nil {
			if err == filer_pb.ErrNotFound {
				glog.Errorf("DeleteObjectTaggingHandler %s: %v", r.URL, err)
				s3err.WriteErrorResponse(w, r, s3err.ErrNoSuchKey)
			} else {
				glog.Errorf("DeleteObjectTaggingHandler %s: %v", r.URL, err)
				s3err.WriteErrorResponse(w, r, s3err.ErrInternalError)
			}
			return
		}

		w.WriteHeader(http.StatusNoContent)
		s3err.PostLog(r, http.StatusNoContent, s3err.ErrNone)
		return
	}

	// For versioned objects, determine the correct directory based on the version
	var updateDirectory string
	if versionId != "" {
		// Specific version requested
		if versionId == "null" {
			// Null version (pre-versioning object) - stored as regular file
			updateDirectory = s3a.option.BucketsPath + "/" + bucket
		} else {
			// Versioned object - stored in .versions directory
			updateDirectory = s3a.option.BucketsPath + "/" + bucket + "/" + object + s3_constants.VersionsFolder
		}
	} else {
		// Latest version in versioned bucket - could be null version or versioned object
		// Extract version ID from the entry to determine where it's stored
		var actualVersionId string
		if entry.Extended != nil {
			if versionIdBytes, exists := entry.Extended[s3_constants.ExtVersionIdKey]; exists {
				actualVersionId = string(versionIdBytes)
			}
		}

		if actualVersionId == "null" || actualVersionId == "" {
			// Null version (pre-versioning object) - stored as regular file
			updateDirectory = s3a.option.BucketsPath + "/" + bucket
		} else {
			// Versioned object - stored in .versions directory
			updateDirectory = s3a.option.BucketsPath + "/" + bucket + "/" + object + s3_constants.VersionsFolder
		}
	}

	// Remove all tags
	hasDeletion := false
	for k := range entry.Extended {
		if len(k) > len(S3TAG_PREFIX) && k[:len(S3TAG_PREFIX)] == S3TAG_PREFIX {
			delete(entry.Extended, k)
			hasDeletion = true
		}
	}

	if !hasDeletion {
		// No tags to delete - success
		w.WriteHeader(http.StatusNoContent)
		s3err.PostLog(r, http.StatusNoContent, s3err.ErrNone)
		return
	}

	// Update the entry
	err = s3a.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		request := &filer_pb.UpdateEntryRequest{
			Directory: updateDirectory,
			Entry:     entry,
		}

		if _, err := client.UpdateEntry(context.Background(), request); err != nil {
			return err
		}
		return nil
	})

	if err != nil {
		glog.Errorf("DeleteObjectTaggingHandler: failed to update entry: %v", err)
		s3err.WriteErrorResponse(w, r, s3err.ErrInternalError)
		return
	}

	glog.V(3).Infof("DeleteObjectTaggingHandler: Successfully deleted tags for %s/%s", bucket, object)
	w.WriteHeader(http.StatusNoContent)
	s3err.PostLog(r, http.StatusNoContent, s3err.ErrNone)
}
