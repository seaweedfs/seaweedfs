package cors

import (
	"net/http"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3err"
)

// BucketChecker interface for checking bucket existence
type BucketChecker interface {
	CheckBucket(r *http.Request, bucket string) s3err.ErrorCode
}

// CORSConfigGetter interface for getting CORS configuration
type CORSConfigGetter interface {
	GetCORSConfiguration(bucket string) (*CORSConfiguration, s3err.ErrorCode)
}

// Middleware handles CORS evaluation for all S3 API requests
type Middleware struct {
	bucketChecker    BucketChecker
	corsConfigGetter CORSConfigGetter
}

// NewMiddleware creates a new CORS middleware instance
func NewMiddleware(bucketChecker BucketChecker, corsConfigGetter CORSConfigGetter) *Middleware {
	return &Middleware{
		bucketChecker:    bucketChecker,
		corsConfigGetter: corsConfigGetter,
	}
}

// Handler returns the CORS middleware handler
func (m *Middleware) Handler(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Parse CORS request
		corsReq := ParseRequest(r)

		// If not a CORS request, continue normally
		if corsReq.Origin == "" {
			next.ServeHTTP(w, r)
			return
		}

		// Extract bucket from request
		bucket, _ := s3_constants.GetBucketAndObject(r)
		if bucket == "" {
			next.ServeHTTP(w, r)
			return
		}

		// Check if bucket exists
		if err := m.bucketChecker.CheckBucket(r, bucket); err != s3err.ErrNone {
			// For non-existent buckets, let the normal handler deal with it
			next.ServeHTTP(w, r)
			return
		}

		// Load CORS configuration from cache
		config, errCode := m.corsConfigGetter.GetCORSConfiguration(bucket)
		if errCode != s3err.ErrNone || config == nil {
			// No CORS configuration, handle based on request type
			if corsReq.IsPreflightRequest {
				// Preflight request without CORS config should fail
				s3err.WriteErrorResponse(w, r, s3err.ErrAccessDenied)
				return
			}
			// Non-preflight request, continue normally
			next.ServeHTTP(w, r)
			return
		}

		// Evaluate CORS request
		corsResp, err := EvaluateRequest(config, corsReq)
		if err != nil {
			glog.V(3).Infof("CORS evaluation failed for bucket %s: %v", bucket, err)
			if corsReq.IsPreflightRequest {
				// Preflight request that doesn't match CORS rules should fail
				s3err.WriteErrorResponse(w, r, s3err.ErrAccessDenied)
				return
			}
			// Non-preflight request, continue normally but without CORS headers
			next.ServeHTTP(w, r)
			return
		}

		// Apply CORS headers
		ApplyHeaders(w, corsResp)

		// Handle preflight requests
		if corsReq.IsPreflightRequest {
			// Preflight request should return 200 OK with just CORS headers
			w.WriteHeader(http.StatusOK)
			return
		}

		// For actual requests, continue with normal processing
		next.ServeHTTP(w, r)
	})
}

// HandleOptionsRequest handles OPTIONS requests for CORS preflight
func (m *Middleware) HandleOptionsRequest(w http.ResponseWriter, r *http.Request) {
	// Parse CORS request
	corsReq := ParseRequest(r)

	// If not a CORS request, return OK
	if corsReq.Origin == "" {
		w.WriteHeader(http.StatusOK)
		return
	}

	// Extract bucket from request
	bucket, _ := s3_constants.GetBucketAndObject(r)
	if bucket == "" {
		w.WriteHeader(http.StatusOK)
		return
	}

	// Check if bucket exists
	if err := m.bucketChecker.CheckBucket(r, bucket); err != s3err.ErrNone {
		// For non-existent buckets, return OK (let other handlers deal with bucket existence)
		w.WriteHeader(http.StatusOK)
		return
	}

	// Load CORS configuration from cache
	config, errCode := m.corsConfigGetter.GetCORSConfiguration(bucket)
	if errCode != s3err.ErrNone || config == nil {
		// No CORS configuration for OPTIONS request should return access denied
		if corsReq.IsPreflightRequest {
			s3err.WriteErrorResponse(w, r, s3err.ErrAccessDenied)
			return
		}
		w.WriteHeader(http.StatusOK)
		return
	}

	// Evaluate CORS request
	corsResp, err := EvaluateRequest(config, corsReq)
	if err != nil {
		glog.V(3).Infof("CORS evaluation failed for bucket %s: %v", bucket, err)
		if corsReq.IsPreflightRequest {
			s3err.WriteErrorResponse(w, r, s3err.ErrAccessDenied)
			return
		}
		w.WriteHeader(http.StatusOK)
		return
	}

	// Apply CORS headers and return success
	ApplyHeaders(w, corsResp)
	w.WriteHeader(http.StatusOK)
}
