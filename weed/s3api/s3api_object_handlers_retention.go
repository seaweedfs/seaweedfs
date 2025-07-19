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

	// Evaluate governance bypass request (header + permission validation)
	governanceBypassAllowed := s3a.evaluateGovernanceBypassRequest(r, bucket, object)

	// Parse retention configuration from request body
	retention, err := parseObjectRetention(r)
	if err != nil {
		glog.Errorf("PutObjectRetentionHandler: failed to parse retention config: %v", err)
		s3err.WriteErrorResponse(w, r, s3err.ErrMalformedXML)
		return
	}

	// Validate retention configuration
	if err := ValidateRetention(retention); err != nil {
		glog.Errorf("PutObjectRetentionHandler: invalid retention config: %v", err)
		s3err.WriteErrorResponse(w, r, mapValidationErrorToS3Error(err))
		return
	}

	// Set retention on the object
	if err := s3a.setObjectRetention(bucket, object, versionId, retention, governanceBypassAllowed); err != nil {
		glog.Errorf("PutObjectRetentionHandler: failed to set retention: %v", err)

		// Handle specific error cases
		if errors.Is(err, ErrObjectNotFound) || errors.Is(err, ErrVersionNotFound) || errors.Is(err, ErrLatestVersionNotFound) {
			s3err.WriteErrorResponse(w, r, s3err.ErrNoSuchKey)
			return
		}

		if errors.Is(err, ErrComplianceModeActive) || errors.Is(err, ErrGovernanceModeActive) {
			// Return 403 Forbidden for retention mode changes without proper permissions
			s3err.WriteErrorResponse(w, r, s3err.ErrAccessDenied)
			return
		}

		s3err.WriteErrorResponse(w, r, s3err.ErrInternalError)
		return
	}

	// Add VersionId to response headers if available (expected by s3-tests)
	if versionId != "" {
		w.Header().Set("x-amz-version-id", versionId)
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
			s3err.WriteErrorResponse(w, r, s3err.ErrObjectLockConfigurationNotFoundError)
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
