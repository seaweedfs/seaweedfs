package s3api

import (
	"encoding/xml"
	"errors"
	"net/http"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3err"
	stats_collect "github.com/seaweedfs/seaweedfs/weed/stats"
)

// PutObjectRetentionHandler Put object Retention
// https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutObjectRetention.html
func (s3a *S3ApiServer) PutObjectRetentionHandler(w http.ResponseWriter, r *http.Request) {
	bucket, object := s3_constants.GetBucketAndObject(r)
	glog.V(3).Infof("PutObjectRetentionHandler %s %s", bucket, object)

	// Check if Object Lock is available for this bucket (requires versioning)
	if !s3a.handleObjectLockAvailabilityCheck(w, r, bucket, "PutObjectRetentionHandler") {
		return
	}

	// Get version ID from query parameters
	versionId := r.URL.Query().Get("versionId")

	// Check for bypass governance retention header
	bypassGovernance := r.Header.Get("x-amz-bypass-governance-retention") == "true"

	// Parse retention configuration from request body
	retention, err := parseObjectRetention(r)
	if err != nil {
		glog.Errorf("PutObjectRetentionHandler: failed to parse retention config: %v", err)
		s3err.WriteErrorResponse(w, r, s3err.ErrMalformedXML)
		return
	}

	// Validate retention configuration
	if err := validateRetention(retention); err != nil {
		glog.Errorf("PutObjectRetentionHandler: invalid retention config: %v", err)
		s3err.WriteErrorResponse(w, r, s3err.ErrInvalidRequest)
		return
	}

	// Set retention on the object
	if err := s3a.setObjectRetention(bucket, object, versionId, retention, bypassGovernance); err != nil {
		glog.Errorf("PutObjectRetentionHandler: failed to set retention: %v", err)

		// Handle specific error cases
		if errors.Is(err, ErrObjectNotFound) || errors.Is(err, ErrVersionNotFound) || errors.Is(err, ErrLatestVersionNotFound) {
			s3err.WriteErrorResponse(w, r, s3err.ErrNoSuchKey)
			return
		}

		if errors.Is(err, ErrComplianceModeActive) || errors.Is(err, ErrGovernanceModeActive) {
			s3err.WriteErrorResponse(w, r, s3err.ErrAccessDenied)
			return
		}

		s3err.WriteErrorResponse(w, r, s3err.ErrInternalError)
		return
	}

	// Record metrics
	stats_collect.RecordBucketActiveTime(bucket)

	// Return success (HTTP 200 with no body)
	w.WriteHeader(http.StatusOK)
	glog.V(3).Infof("PutObjectRetentionHandler: successfully set retention for %s/%s", bucket, object)
}

// GetObjectRetentionHandler Get object Retention
// https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetObjectRetention.html
func (s3a *S3ApiServer) GetObjectRetentionHandler(w http.ResponseWriter, r *http.Request) {
	bucket, object := s3_constants.GetBucketAndObject(r)
	glog.V(3).Infof("GetObjectRetentionHandler %s %s", bucket, object)

	// Check if Object Lock is available for this bucket (requires versioning)
	if !s3a.handleObjectLockAvailabilityCheck(w, r, bucket, "GetObjectRetentionHandler") {
		return
	}

	// Get version ID from query parameters
	versionId := r.URL.Query().Get("versionId")

	// Get retention configuration for the object
	retention, err := s3a.getObjectRetention(bucket, object, versionId)
	if err != nil {
		glog.Errorf("GetObjectRetentionHandler: failed to get retention: %v", err)

		// Handle specific error cases
		if errors.Is(err, ErrObjectNotFound) || errors.Is(err, ErrVersionNotFound) {
			s3err.WriteErrorResponse(w, r, s3err.ErrNoSuchKey)
			return
		}

		if errors.Is(err, ErrNoRetentionConfiguration) {
			s3err.WriteErrorResponse(w, r, s3err.ErrNoSuchObjectLockConfiguration)
			return
		}

		s3err.WriteErrorResponse(w, r, s3err.ErrInternalError)
		return
	}

	// Marshal retention configuration to XML
	retentionXML, err := xml.Marshal(retention)
	if err != nil {
		glog.Errorf("GetObjectRetentionHandler: failed to marshal retention: %v", err)
		s3err.WriteErrorResponse(w, r, s3err.ErrInternalError)
		return
	}

	// Set response headers
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(http.StatusOK)

	// Write XML response
	if _, err := w.Write([]byte(xml.Header)); err != nil {
		glog.Errorf("GetObjectRetentionHandler: failed to write XML header: %v", err)
		return
	}

	if _, err := w.Write(retentionXML); err != nil {
		glog.Errorf("GetObjectRetentionHandler: failed to write retention XML: %v", err)
		return
	}

	// Record metrics
	stats_collect.RecordBucketActiveTime(bucket)

	glog.V(3).Infof("GetObjectRetentionHandler: successfully retrieved retention for %s/%s", bucket, object)
}

// PutObjectLegalHoldHandler Put object Legal Hold
// https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutObjectLegalHold.html
func (s3a *S3ApiServer) PutObjectLegalHoldHandler(w http.ResponseWriter, r *http.Request) {
	bucket, object := s3_constants.GetBucketAndObject(r)
	glog.V(3).Infof("PutObjectLegalHoldHandler %s %s", bucket, object)

	// Check if Object Lock is available for this bucket (requires versioning)
	if !s3a.handleObjectLockAvailabilityCheck(w, r, bucket, "PutObjectLegalHoldHandler") {
		return
	}

	// Get version ID from query parameters
	versionId := r.URL.Query().Get("versionId")

	// Parse legal hold configuration from request body
	legalHold, err := parseObjectLegalHold(r)
	if err != nil {
		glog.Errorf("PutObjectLegalHoldHandler: failed to parse legal hold config: %v", err)
		s3err.WriteErrorResponse(w, r, s3err.ErrMalformedXML)
		return
	}

	// Validate legal hold configuration
	if err := validateLegalHold(legalHold); err != nil {
		glog.Errorf("PutObjectLegalHoldHandler: invalid legal hold config: %v", err)
		s3err.WriteErrorResponse(w, r, s3err.ErrInvalidRequest)
		return
	}

	// Set legal hold on the object
	if err := s3a.setObjectLegalHold(bucket, object, versionId, legalHold); err != nil {
		glog.Errorf("PutObjectLegalHoldHandler: failed to set legal hold: %v", err)

		// Handle specific error cases
		if errors.Is(err, ErrObjectNotFound) || errors.Is(err, ErrVersionNotFound) {
			s3err.WriteErrorResponse(w, r, s3err.ErrNoSuchKey)
			return
		}

		s3err.WriteErrorResponse(w, r, s3err.ErrInternalError)
		return
	}

	// Record metrics
	stats_collect.RecordBucketActiveTime(bucket)

	// Return success (HTTP 200 with no body)
	w.WriteHeader(http.StatusOK)
	glog.V(3).Infof("PutObjectLegalHoldHandler: successfully set legal hold for %s/%s", bucket, object)
}

// GetObjectLegalHoldHandler Get object Legal Hold
// https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetObjectLegalHold.html
func (s3a *S3ApiServer) GetObjectLegalHoldHandler(w http.ResponseWriter, r *http.Request) {
	bucket, object := s3_constants.GetBucketAndObject(r)
	glog.V(3).Infof("GetObjectLegalHoldHandler %s %s", bucket, object)

	// Check if Object Lock is available for this bucket (requires versioning)
	if !s3a.handleObjectLockAvailabilityCheck(w, r, bucket, "GetObjectLegalHoldHandler") {
		return
	}

	// Get version ID from query parameters
	versionId := r.URL.Query().Get("versionId")

	// Get legal hold configuration for the object
	legalHold, err := s3a.getObjectLegalHold(bucket, object, versionId)
	if err != nil {
		glog.Errorf("GetObjectLegalHoldHandler: failed to get legal hold: %v", err)

		// Handle specific error cases
		if errors.Is(err, ErrObjectNotFound) || errors.Is(err, ErrVersionNotFound) {
			s3err.WriteErrorResponse(w, r, s3err.ErrNoSuchKey)
			return
		}

		if errors.Is(err, ErrNoLegalHoldConfiguration) {
			s3err.WriteErrorResponse(w, r, s3err.ErrNoSuchObjectLegalHold)
			return
		}

		s3err.WriteErrorResponse(w, r, s3err.ErrInternalError)
		return
	}

	// Marshal legal hold configuration to XML
	legalHoldXML, err := xml.Marshal(legalHold)
	if err != nil {
		glog.Errorf("GetObjectLegalHoldHandler: failed to marshal legal hold: %v", err)
		s3err.WriteErrorResponse(w, r, s3err.ErrInternalError)
		return
	}

	// Set response headers
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(http.StatusOK)

	// Write XML response
	if _, err := w.Write([]byte(xml.Header)); err != nil {
		glog.Errorf("GetObjectLegalHoldHandler: failed to write XML header: %v", err)
		return
	}

	if _, err := w.Write(legalHoldXML); err != nil {
		glog.Errorf("GetObjectLegalHoldHandler: failed to write legal hold XML: %v", err)
		return
	}

	// Record metrics
	stats_collect.RecordBucketActiveTime(bucket)

	glog.V(3).Infof("GetObjectLegalHoldHandler: successfully retrieved legal hold for %s/%s", bucket, object)
}

// PutObjectLockConfigurationHandler Put object Lock configuration
// https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutObjectLockConfiguration.html
func (s3a *S3ApiServer) PutObjectLockConfigurationHandler(w http.ResponseWriter, r *http.Request) {
	bucket, _ := s3_constants.GetBucketAndObject(r)
	glog.V(3).Infof("PutObjectLockConfigurationHandler %s", bucket)

	// Check if Object Lock is available for this bucket (requires versioning)
	if !s3a.handleObjectLockAvailabilityCheck(w, r, bucket, "PutObjectLockConfigurationHandler") {
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
	if err := validateObjectLockConfiguration(config); err != nil {
		glog.Errorf("PutObjectLockConfigurationHandler: invalid object lock config: %v", err)
		s3err.WriteErrorResponse(w, r, s3err.ErrInvalidRequest)
		return
	}

	// Set object lock configuration on the bucket
	errCode := s3a.updateBucketConfig(bucket, func(bucketConfig *BucketConfig) error {
		if bucketConfig.Entry.Extended == nil {
			bucketConfig.Entry.Extended = make(map[string][]byte)
		}

		// Store the configuration as JSON in extended attributes
		configXML, err := xml.Marshal(config)
		if err != nil {
			return err
		}

		bucketConfig.Entry.Extended[s3_constants.ExtObjectLockConfigKey] = configXML

		if config.ObjectLockEnabled != "" {
			bucketConfig.Entry.Extended[s3_constants.ExtObjectLockEnabledKey] = []byte(config.ObjectLockEnabled)
		}

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

	// Check if object lock configuration exists
	if bucketConfig.Entry.Extended == nil {
		s3err.WriteErrorResponse(w, r, s3err.ErrNoSuchObjectLockConfiguration)
		return
	}

	configXML, exists := bucketConfig.Entry.Extended[s3_constants.ExtObjectLockConfigKey]
	if !exists {
		s3err.WriteErrorResponse(w, r, s3err.ErrNoSuchObjectLockConfiguration)
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
