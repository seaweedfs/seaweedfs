package s3api

import (
	"encoding/xml"
	"net/http"

	"errors"

	"github.com/seaweedfs/seaweedfs/weed/glog"
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

	var configXML []byte

	// Check if we have cached Object Lock configuration
	if bucketConfig.ObjectLockConfig != nil {
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
		glog.V(3).Infof("GetObjectLockConfigurationHandler: successfully retrieved cached object lock config for %s", bucket)
		return
	}

	// Fallback: check for legacy storage in extended attributes
	if bucketConfig.Entry.Extended != nil {
		// Check if Object Lock is enabled via boolean flag
		if enabledBytes, exists := bucketConfig.Entry.Extended[s3_constants.ExtObjectLockEnabledKey]; exists {
			enabled := string(enabledBytes)
			if enabled == s3_constants.ObjectLockEnabled || enabled == "true" {
				// Generate minimal XML configuration for enabled Object Lock without retention policies
				minimalConfig := `<ObjectLockConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><ObjectLockEnabled>Enabled</ObjectLockEnabled></ObjectLockConfiguration>`
				configXML = []byte(minimalConfig)
			}
		}
	}

	// If no Object Lock configuration found, return error
	if len(configXML) == 0 {
		s3err.WriteErrorResponse(w, r, s3err.ErrObjectLockConfigurationNotFoundError)
		return
	}

	// Set response headers
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(http.StatusOK)

	// Write XML response
	if _, err := w.Write([]byte(xml.Header)); err != nil {
		glog.Errorf("GetObjectLockConfigurationHandler: failed to write XML header: %v", err)
		return
	}

	if _, err := w.Write(configXML); err != nil {
		glog.Errorf("GetObjectLockConfigurationHandler: failed to write config XML: %v", err)
		return
	}

	// Record metrics
	stats_collect.RecordBucketActiveTime(bucket)

	glog.V(3).Infof("GetObjectLockConfigurationHandler: successfully retrieved object lock config for %s", bucket)
}
