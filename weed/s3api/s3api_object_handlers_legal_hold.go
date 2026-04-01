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
	if err := ValidateLegalHold(legalHold); err != nil {
		glog.Errorf("PutObjectLegalHoldHandler: invalid legal hold config: %v", err)
		s3err.WriteErrorResponse(w, r, mapValidationErrorToS3Error(err))
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

	// Add VersionId to response headers if available (expected by s3-tests)
	if versionId != "" {
		w.Header().Set("x-amz-version-id", versionId)
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
		// Handle specific error cases
		if errors.Is(err, ErrObjectNotFound) || errors.Is(err, ErrVersionNotFound) {
			s3err.WriteErrorResponse(w, r, s3err.ErrNoSuchKey)
			return
		}

		if errors.Is(err, ErrNoLegalHoldConfiguration) {
			s3err.WriteErrorResponse(w, r, s3err.ErrNoSuchObjectLegalHold)
			return
		}

		glog.Errorf("GetObjectLegalHoldHandler: failed to get legal hold: %v", err)
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
