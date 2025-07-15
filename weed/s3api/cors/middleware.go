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
	storage          *Storage
	bucketChecker    BucketChecker
	corsConfigGetter CORSConfigGetter
}

// NewMiddleware creates a new CORS middleware instance
func NewMiddleware(storage *Storage, bucketChecker BucketChecker, corsConfigGetter CORSConfigGetter) *Middleware {
	return &Middleware{
		storage:          storage,
		bucketChecker:    bucketChecker,
		corsConfigGetter: corsConfigGetter,
	}
}

// evaluateCORSRequest performs the common CORS request evaluation logic
// Returns: (corsResponse, responseWritten, shouldContinue)
// - corsResponse: the CORS response if evaluation succeeded
// - responseWritten: true if an error response was already written
// - shouldContinue: true if the request should continue to the next handler
func (m *Middleware) evaluateCORSRequest(w http.ResponseWriter, r *http.Request) (*CORSResponse, bool, bool) {
	// Parse CORS request
	corsReq := ParseRequest(r)
	if corsReq.Origin == "" {
		// Not a CORS request
		return nil, false, true
	}

	// Extract bucket from request
	bucket, _ := s3_constants.GetBucketAndObject(r)
	if bucket == "" {
		return nil, false, true
	}

	// Check if bucket exists
	if err := m.bucketChecker.CheckBucket(r, bucket); err != s3err.ErrNone {
		// For non-existent buckets, let the normal handler deal with it
		return nil, false, true
	}

	// Load CORS configuration from cache
	config, errCode := m.corsConfigGetter.GetCORSConfiguration(bucket)
	if errCode != s3err.ErrNone || config == nil {
		// No CORS configuration, handle based on request type
		if corsReq.IsPreflightRequest {
			// Preflight request without CORS config should fail
			s3err.WriteErrorResponse(w, r, s3err.ErrAccessDenied)
			return nil, true, false // Response written, don't continue
		}
		// Non-preflight request, continue normally
		return nil, false, true
	}

	// Evaluate CORS request
	corsResp, err := EvaluateRequest(config, corsReq)
	if err != nil {
		glog.V(3).Infof("CORS evaluation failed for bucket %s: %v", bucket, err)
		if corsReq.IsPreflightRequest {
			// Preflight request that doesn't match CORS rules should fail
			s3err.WriteErrorResponse(w, r, s3err.ErrAccessDenied)
			return nil, true, false // Response written, don't continue
		}
		// Non-preflight request, continue normally but without CORS headers
		return nil, false, true
	}

	return corsResp, false, false
}

// Handler returns the CORS middleware handler
func (m *Middleware) Handler(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Use the common evaluation logic
		corsResp, responseWritten, shouldContinue := m.evaluateCORSRequest(w, r)
		if responseWritten {
			// Response was already written (error case)
			return
		}

		if shouldContinue {
			// Continue with normal request processing
			next.ServeHTTP(w, r)
			return
		}

		// Parse request to check if it's a preflight request
		corsReq := ParseRequest(r)

		// Apply CORS headers to response
		ApplyHeaders(w, corsResp)

		// Handle preflight requests
		if corsReq.IsPreflightRequest {
			// Preflight request should return 200 OK with just CORS headers
			w.WriteHeader(http.StatusOK)
			return
		}

		// Continue with normal request processing
		next.ServeHTTP(w, r)
	})
}

// HandleOptionsRequest handles OPTIONS requests for CORS preflight
func (m *Middleware) HandleOptionsRequest(w http.ResponseWriter, r *http.Request) {
	// Use the common evaluation logic
	corsResp, responseWritten, shouldContinue := m.evaluateCORSRequest(w, r)
	if responseWritten {
		// Response was already written (error case)
		return
	}

	if shouldContinue || corsResp == nil {
		// Not a CORS request or should continue normally
		w.WriteHeader(http.StatusOK)
		return
	}

	// Apply CORS headers and return success
	ApplyHeaders(w, corsResp)
	w.WriteHeader(http.StatusOK)
}
