package s3api

import (
	"encoding/xml"
	"errors"
	"net/http"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3err"
	stats_collect "github.com/seaweedfs/seaweedfs/weed/stats"
)

// PutObjectLockConfigurationHandler Put object Lock configuration
// https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutObjectLockConfiguration.html
func (s3a *S3ApiServer) PutObjectLockConfigurationHandler(w http.ResponseWriter, r *http.Request) {
	bucket, _ := s3_constants.GetBucketAndObject(r)
	glog.V(3).Infof("PutObjectLockConfigurationHandler %s", bucket)

	// Check if Object Lock is available for this bucket (requires versioning)
	// For bucket-level operations, return InvalidBucketState (409) when object lock is not available
	if err := s3a.isObjectLockAvailable(bucket); err != nil {
		glog.Errorf("PutObjectLockConfigurationHandler: object lock not available for bucket %s: %v", bucket, err)
		if errors.Is(err, ErrBucketNotFound) {
			s3err.WriteErrorResponse(w, r, s3err.ErrNoSuchBucket)
		} else {
			// Return InvalidBucketState for bucket-level object lock operations on buckets without object lock enabled
			// This matches AWS S3 behavior and s3-tests expectations (409 Conflict)
			s3err.WriteErrorResponse(w, r, s3err.ErrInvalidBucketState)
		}
		return
	}

	// Parse object lock configuration from request body
	config, err := parseObjectLockConfiguration(r)
	if err != nil {
		glog.Errorf("PutObjectLockConfigurationHandler: failed to parse object lock config: %v", err)
		s3err.WriteErrorResponse(w, r, s3err.ErrMalformedXML)
		return
	}

	// Validate object lock configuration
	if err := ValidateObjectLockConfiguration(config); err != nil {
		glog.Errorf("PutObjectLockConfigurationHandler: invalid object lock config: %v", err)
		s3err.WriteErrorResponse(w, r, mapValidationErrorToS3Error(err))
		return
	}

	// Set object lock configuration on the bucket
	errCode := s3a.updateBucketConfig(bucket, func(bucketConfig *BucketConfig) error {
		// Set the cached Object Lock configuration
		bucketConfig.ObjectLockConfig = config
		return nil
	})

	if errCode != s3err.ErrNone {
		glog.Errorf("PutObjectLockConfigurationHandler: failed to set object lock config: %v", errCode)
		s3err.WriteErrorResponse(w, r, errCode)
		return
	}

	// Record metrics
	stats_collect.RecordBucketActiveTime(bucket)

	// Return success (HTTP 200 with no body)
	w.WriteHeader(http.StatusOK)
	glog.V(3).Infof("PutObjectLockConfigurationHandler: successfully set object lock config for %s", bucket)
}

// GetObjectLockConfigurationHandler Get object Lock configuration
// https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetObjectLockConfiguration.html
func (s3a *S3ApiServer) GetObjectLockConfigurationHandler(w http.ResponseWriter, r *http.Request) {
	bucket, _ := s3_constants.GetBucketAndObject(r)
	glog.V(3).Infof("GetObjectLockConfigurationHandler %s", bucket)

	// Get bucket configuration
	bucketConfig, errCode := s3a.getBucketConfig(bucket)
	if errCode != s3err.ErrNone {
		glog.Errorf("GetObjectLockConfigurationHandler: failed to get bucket config: %v", errCode)
		s3err.WriteErrorResponse(w, r, errCode)
		return
	}

	glog.V(3).Infof("GetObjectLockConfigurationHandler: retrieved bucket config for %s, ObjectLockConfig=%+v", bucket, bucketConfig.ObjectLockConfig)

	// Check if we have cached Object Lock configuration
	if bucketConfig.ObjectLockConfig != nil {
		// Set namespace for S3 compatibility
		bucketConfig.ObjectLockConfig.XMLNS = s3_constants.S3Namespace

		// Use cached configuration and marshal it to XML for response
		marshaledXML, err := xml.Marshal(bucketConfig.ObjectLockConfig)
		if err != nil {
			glog.Errorf("GetObjectLockConfigurationHandler: failed to marshal cached Object Lock config: %v", err)
			s3err.WriteErrorResponse(w, r, s3err.ErrInternalError)
			return
		}

		// Write XML response
		w.Header().Set("Content-Type", "application/xml")
		w.WriteHeader(http.StatusOK)
		if _, err := w.Write([]byte(xml.Header)); err != nil {
			glog.Errorf("GetObjectLockConfigurationHandler: failed to write XML header: %v", err)
			return
		}
		if _, err := w.Write(marshaledXML); err != nil {
			glog.Errorf("GetObjectLockConfigurationHandler: failed to write config XML: %v", err)
			return
		}

		// Record metrics
		stats_collect.RecordBucketActiveTime(bucket)

		glog.V(3).Infof("GetObjectLockConfigurationHandler: successfully retrieved cached object lock config for %s", bucket)
		return
	}

	// If no cached Object Lock configuration, reload entry from filer to get the latest extended attributes
	// This handles cases where the cache might have a stale entry due to timing issues with metadata updates
	glog.V(3).Infof("GetObjectLockConfigurationHandler: no cached ObjectLockConfig, reloading entry from filer for %s", bucket)
	freshEntry, err := s3a.getEntry(s3a.option.BucketsPath, bucket)
	if err != nil {
		if errors.Is(err, filer_pb.ErrNotFound) {
			glog.V(1).Infof("GetObjectLockConfigurationHandler: bucket %s not found while reloading entry", bucket)
			s3err.WriteErrorResponse(w, r, s3err.ErrNoSuchBucket)
			return
		}
		glog.Errorf("GetObjectLockConfigurationHandler: failed to reload bucket entry: %v", err)
		s3err.WriteErrorResponse(w, r, s3err.ErrInternalError)
		return
	}

	// Try to load Object Lock configuration from the fresh entry
	// LoadObjectLockConfigurationFromExtended already checks ExtObjectLockEnabledKey and returns
	// a basic configuration even when there's no default retention policy
	if objectLockConfig, found := LoadObjectLockConfigurationFromExtended(freshEntry); found {
		glog.V(3).Infof("GetObjectLockConfigurationHandler: loaded Object Lock config from fresh entry for %s: %+v", bucket, objectLockConfig)

		// Rebuild the entire cached config from the fresh entry to maintain cache coherence
		// This ensures all fields (Versioning, Owner, ACL, IsPublicRead, CORS, etc.) are up-to-date,
		// not just ObjectLockConfig, before resetting the TTL
		s3a.updateBucketConfigCacheFromEntry(freshEntry)

		// Set namespace for S3 compatibility
		objectLockConfig.XMLNS = s3_constants.S3Namespace

		// Marshal and return the configuration
		marshaledXML, err := xml.Marshal(objectLockConfig)
		if err != nil {
			glog.Errorf("GetObjectLockConfigurationHandler: failed to marshal Object Lock config: %v", err)
			s3err.WriteErrorResponse(w, r, s3err.ErrInternalError)
			return
		}

		w.Header().Set("Content-Type", "application/xml")
		w.WriteHeader(http.StatusOK)
		if _, err := w.Write([]byte(xml.Header)); err != nil {
			glog.Errorf("GetObjectLockConfigurationHandler: failed to write XML header: %v", err)
			return
		}
		if _, err := w.Write(marshaledXML); err != nil {
			glog.Errorf("GetObjectLockConfigurationHandler: failed to write config XML: %v", err)
			return
		}

		// Record metrics
		stats_collect.RecordBucketActiveTime(bucket)

		glog.V(3).Infof("GetObjectLockConfigurationHandler: successfully retrieved object lock config from fresh entry for %s", bucket)
		return
	}

	// No Object Lock configuration found
	glog.V(3).Infof("GetObjectLockConfigurationHandler: no Object Lock configuration found for %s", bucket)
	s3err.WriteErrorResponse(w, r, s3err.ErrObjectLockConfigurationNotFoundError)
}
