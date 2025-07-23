package s3api

import (
	"encoding/xml"
	"net/http"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/s3api/cors"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3err"
)

// S3BucketChecker implements cors.BucketChecker interface
type S3BucketChecker struct {
	server *S3ApiServer
}

func (c *S3BucketChecker) CheckBucket(r *http.Request, bucket string) s3err.ErrorCode {
	return c.server.checkBucket(r, bucket)
}

// S3CORSConfigGetter implements cors.CORSConfigGetter interface
type S3CORSConfigGetter struct {
	server *S3ApiServer
}

func (g *S3CORSConfigGetter) GetCORSConfiguration(bucket string) (*cors.CORSConfiguration, s3err.ErrorCode) {
	return g.server.getCORSConfiguration(bucket)
}

// getCORSMiddleware returns a CORS middleware instance with caching
func (s3a *S3ApiServer) getCORSMiddleware() *cors.Middleware {
	bucketChecker := &S3BucketChecker{server: s3a}
	corsConfigGetter := &S3CORSConfigGetter{server: s3a}

	return cors.NewMiddleware(bucketChecker, corsConfigGetter)
}

// GetBucketCorsHandler handles Get bucket CORS configuration
// https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetBucketCors.html
func (s3a *S3ApiServer) GetBucketCorsHandler(w http.ResponseWriter, r *http.Request) {
	bucket, _ := s3_constants.GetBucketAndObject(r)
	glog.V(3).Infof("GetBucketCorsHandler %s", bucket)

	if err := s3a.checkBucket(r, bucket); err != s3err.ErrNone {
		s3err.WriteErrorResponse(w, r, err)
		return
	}

	// Load CORS configuration from cache
	config, errCode := s3a.getCORSConfiguration(bucket)
	if errCode != s3err.ErrNone {
		if errCode == s3err.ErrNoSuchBucket {
			s3err.WriteErrorResponse(w, r, s3err.ErrNoSuchBucket)
		} else {
			s3err.WriteErrorResponse(w, r, s3err.ErrInternalError)
		}
		return
	}

	if config == nil {
		s3err.WriteErrorResponse(w, r, s3err.ErrNoSuchCORSConfiguration)
		return
	}

	// Return CORS configuration as XML
	writeSuccessResponseXML(w, r, config)
}

// PutBucketCorsHandler handles Put bucket CORS configuration
// https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutBucketCors.html
func (s3a *S3ApiServer) PutBucketCorsHandler(w http.ResponseWriter, r *http.Request) {
	bucket, _ := s3_constants.GetBucketAndObject(r)
	glog.V(3).Infof("PutBucketCorsHandler %s", bucket)

	if err := s3a.checkBucket(r, bucket); err != s3err.ErrNone {
		s3err.WriteErrorResponse(w, r, err)
		return
	}

	// Parse CORS configuration from request body
	var config cors.CORSConfiguration
	if err := xml.NewDecoder(r.Body).Decode(&config); err != nil {
		glog.V(1).Infof("Failed to parse CORS configuration: %v", err)
		s3err.WriteErrorResponse(w, r, s3err.ErrMalformedXML)
		return
	}

	// Validate CORS configuration
	if err := cors.ValidateConfiguration(&config); err != nil {
		glog.V(1).Infof("Invalid CORS configuration: %v", err)
		s3err.WriteErrorResponse(w, r, s3err.ErrInvalidRequest)
		return
	}

	// Store CORS configuration and update cache
	// This handles both cache update and persistent storage through the unified bucket config system
	if err := s3a.updateCORSConfiguration(bucket, &config); err != s3err.ErrNone {
		glog.Errorf("Failed to update CORS configuration: %v", err)
		s3err.WriteErrorResponse(w, r, s3err.ErrInternalError)
		return
	}

	// Return success
	writeSuccessResponseEmpty(w, r)
}

// DeleteBucketCorsHandler handles Delete bucket CORS configuration
// https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteBucketCors.html
func (s3a *S3ApiServer) DeleteBucketCorsHandler(w http.ResponseWriter, r *http.Request) {
	bucket, _ := s3_constants.GetBucketAndObject(r)
	glog.V(3).Infof("DeleteBucketCorsHandler %s", bucket)

	if err := s3a.checkBucket(r, bucket); err != s3err.ErrNone {
		s3err.WriteErrorResponse(w, r, err)
		return
	}

	// Remove CORS configuration from cache and persistent storage
	// This handles both cache invalidation and persistent storage cleanup through the unified bucket config system
	if err := s3a.removeCORSConfiguration(bucket); err != s3err.ErrNone {
		glog.Errorf("Failed to remove CORS configuration: %v", err)
		s3err.WriteErrorResponse(w, r, s3err.ErrInternalError)
		return
	}

	// Return success (204 No Content)
	w.WriteHeader(http.StatusNoContent)
}
