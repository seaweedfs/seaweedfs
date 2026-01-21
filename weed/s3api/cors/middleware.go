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
	fallbackConfig   *CORSConfiguration // Global CORS configuration as fallback
}

// NewMiddleware creates a new CORS middleware instance with optional global fallback config
func NewMiddleware(bucketChecker BucketChecker, corsConfigGetter CORSConfigGetter, fallbackConfig *CORSConfiguration) *Middleware {
	return &Middleware{
		bucketChecker:    bucketChecker,
		corsConfigGetter: corsConfigGetter,
		fallbackConfig:   fallbackConfig,
	}
}

// getCORSConfig retrieves the applicable CORS configuration, trying bucket-specific first, then fallback.
// Returns the configuration and a boolean indicating if any configuration was found.
// Only falls back to global config when there's explicitly no bucket-level config.
// For other errors (e.g., access denied), returns false to let the handler deny the request.
func (m *Middleware) getCORSConfig(bucket string) (*CORSConfiguration, bool) {
	config, errCode := m.corsConfigGetter.GetCORSConfiguration(bucket)

	switch errCode {
	case s3err.ErrNone:
		if config != nil {
			// Found a bucket-specific config, use it.
			return config, true
		}
		// No bucket config, proceed to fallback.
	case s3err.ErrNoSuchCORSConfiguration:
		// No bucket config, proceed to fallback.
	default:
		// Any other error means we should not proceed.
		return nil, false
	}

	// No bucket-level config found, try global fallback
	if m.fallbackConfig != nil {
		return m.fallbackConfig, true
	}

	return nil, false
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

		// Get CORS configuration (bucket-specific or fallback) BEFORE checking bucket existence
		// This ensures CORS headers are applied consistently regardless of bucket existence,
		// preventing information disclosure about whether a bucket exists
		config, found := m.getCORSConfig(bucket)
		if !found {
			// No CORS configuration at all, handle based on request type
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

		// Apply CORS headers early, before bucket existence check
		// This ensures consistent CORS behavior and prevents information disclosure
		ApplyHeaders(w, corsResp)

		// Handle preflight requests - return success immediately without checking bucket existence
		// This matches AWS S3 behavior where preflight requests succeed even for non-existent buckets
		if corsReq.IsPreflightRequest {
			// Preflight request should return 200 OK with just CORS headers
			w.WriteHeader(http.StatusOK)
			return
		}

		// For actual requests, continue with normal processing
		// The handler will check bucket existence and return appropriate errors (e.g., NoSuchBucket)
		// but CORS headers have already been applied above
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

	// Get CORS configuration (bucket-specific or fallback) BEFORE checking bucket existence
	// This ensures CORS headers are applied consistently regardless of bucket existence
	config, found := m.getCORSConfig(bucket)
	if !found {
		// No CORS configuration at all for OPTIONS request should return access denied
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
