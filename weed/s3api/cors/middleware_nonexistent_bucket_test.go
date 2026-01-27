package cors

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gorilla/mux"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3err"
)

// TestMiddlewareNonExistentBucket tests that CORS headers are applied even for non-existent buckets
// This prevents information disclosure about bucket existence and ensures consistent CORS behavior
func TestMiddlewareNonExistentBucket(t *testing.T) {
	tests := []struct {
		name                 string
		fallbackConfig       *CORSConfiguration
		requestOrigin        string
		requestMethod        string
		isOptions            bool
		expectedStatus       int
		expectedOriginHeader string
		description          string
	}{
		{
			name: "Preflight request to non-existent bucket with global CORS config",
			fallbackConfig: &CORSConfiguration{
				CORSRules: []CORSRule{
					{
						AllowedOrigins: []string{"*"},
						AllowedMethods: []string{"GET", "POST", "PUT", "DELETE", "HEAD"},
						AllowedHeaders: []string{"*"},
					},
				},
			},
			requestOrigin:        "https://example.com",
			requestMethod:        "OPTIONS",
			isOptions:            true,
			expectedStatus:       http.StatusOK,
			expectedOriginHeader: "https://example.com",
			description:          "Preflight to non-existent bucket should succeed with CORS headers",
		},
		{
			name: "Actual request to non-existent bucket with global CORS config",
			fallbackConfig: &CORSConfiguration{
				CORSRules: []CORSRule{
					{
						AllowedOrigins: []string{"*"},
						AllowedMethods: []string{"GET", "POST", "PUT", "DELETE", "HEAD"},
						AllowedHeaders: []string{"*"},
					},
				},
			},
			requestOrigin:        "https://example.com",
			requestMethod:        "GET",
			isOptions:            false,
			expectedStatus:       http.StatusNotFound, // Handler returns NoSuchBucket
			expectedOriginHeader: "https://example.com",
			description:          "GET to non-existent bucket should have CORS headers even with NoSuchBucket error",
		},
		{
			name: "Preflight to non-existent bucket with specific origin",
			fallbackConfig: &CORSConfiguration{
				CORSRules: []CORSRule{
					{
						AllowedOrigins: []string{"https://allowed.com"},
						AllowedMethods: []string{"GET", "POST"},
						AllowedHeaders: []string{"*"},
					},
				},
			},
			requestOrigin:        "https://allowed.com",
			requestMethod:        "OPTIONS",
			isOptions:            true,
			expectedStatus:       http.StatusOK,
			expectedOriginHeader: "https://allowed.com",
			description:          "Preflight to non-existent bucket with matching origin should succeed",
		},
		{
			name: "Preflight to non-existent bucket with non-matching origin",
			fallbackConfig: &CORSConfiguration{
				CORSRules: []CORSRule{
					{
						AllowedOrigins: []string{"https://allowed.com"},
						AllowedMethods: []string{"GET", "POST"},
						AllowedHeaders: []string{"*"},
					},
				},
			},
			requestOrigin:        "https://notallowed.com",
			requestMethod:        "OPTIONS",
			isOptions:            true,
			expectedStatus:       http.StatusForbidden,
			expectedOriginHeader: "",
			description:          "Preflight to non-existent bucket with non-matching origin should fail",
		},
		{
			name:                 "Preflight to non-existent bucket without CORS config",
			fallbackConfig:       nil,
			requestOrigin:        "https://example.com",
			requestMethod:        "OPTIONS",
			isOptions:            true,
			expectedStatus:       http.StatusForbidden,
			expectedOriginHeader: "",
			description:          "Preflight to non-existent bucket without CORS config should fail",
		},
		{
			name: "Bucket listing request to non-existent bucket with CORS",
			fallbackConfig: &CORSConfiguration{
				CORSRules: []CORSRule{
					{
						AllowedOrigins: []string{"*"},
						AllowedMethods: []string{"GET", "POST", "PUT", "DELETE", "HEAD"},
						AllowedHeaders: []string{"*"},
					},
				},
			},
			requestOrigin:        "https://mydomain.com",
			requestMethod:        "OPTIONS",
			isOptions:            true,
			expectedStatus:       http.StatusOK,
			expectedOriginHeader: "https://mydomain.com",
			description:          "Bucket listing preflight to non-existent bucket should have CORS headers (issue #8065)",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Setup mocks - bucket does NOT exist
			bucketChecker := &mockBucketChecker{bucketExists: false}
			configGetter := &mockCORSConfigGetter{
				config:  nil, // No bucket-specific config
				errCode: s3err.ErrNone,
			}

			// Create middleware with fallback config
			middleware := NewMiddleware(bucketChecker, configGetter, tt.fallbackConfig)

			// Create request with mux variables
			req := httptest.NewRequest(tt.requestMethod, "/nonexistent-bucket/testobject", nil)
			req = mux.SetURLVars(req, map[string]string{
				"bucket": "nonexistent-bucket",
				"object": "testobject",
			})
			if tt.requestOrigin != "" {
				req.Header.Set("Origin", tt.requestOrigin)
			}
			if tt.isOptions {
				req.Header.Set("Access-Control-Request-Method", "GET")
			}

			// Create response recorder
			w := httptest.NewRecorder()

			// Create a handler that returns 404 for non-existent buckets
			nextHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				// Simulate NoSuchBucket error
				w.WriteHeader(http.StatusNotFound)
			})

			// Execute middleware
			if tt.isOptions {
				middleware.HandleOptionsRequest(w, req)
			} else {
				middleware.Handler(nextHandler).ServeHTTP(w, req)
			}

			// Check status code
			if w.Code != tt.expectedStatus {
				t.Errorf("%s: expected status %d, got %d", tt.description, tt.expectedStatus, w.Code)
			}

			// Check CORS header
			actualOrigin := w.Header().Get("Access-Control-Allow-Origin")
			if actualOrigin != tt.expectedOriginHeader {
				t.Errorf("%s: expected Access-Control-Allow-Origin='%s', got '%s'",
					tt.description, tt.expectedOriginHeader, actualOrigin)
			}
		})
	}
}

// TestMiddlewareConsistentBehavior tests that CORS headers are consistent regardless of bucket existence
// This is a security test to ensure bucket existence cannot be inferred from CORS header presence
func TestMiddlewareConsistentBehavior(t *testing.T) {
	fallbackConfig := &CORSConfiguration{
		CORSRules: []CORSRule{
			{
				AllowedOrigins: []string{"*"},
				AllowedMethods: []string{"GET", "POST", "PUT", "DELETE", "HEAD"},
				AllowedHeaders: []string{"*"},
			},
		},
	}

	testCases := []struct {
		bucketExists bool
		description  string
	}{
		{bucketExists: true, description: "existing bucket"},
		{bucketExists: false, description: "non-existent bucket"},
	}

	for _, tc := range testCases {
		t.Run(tc.description, func(t *testing.T) {
			// Setup mocks
			bucketChecker := &mockBucketChecker{bucketExists: tc.bucketExists}
			configGetter := &mockCORSConfigGetter{
				config:  nil,
				errCode: s3err.ErrNone,
			}

			middleware := NewMiddleware(bucketChecker, configGetter, fallbackConfig)

			// Create preflight request
			req := httptest.NewRequest("OPTIONS", "/testbucket/testobject", nil)
			req = mux.SetURLVars(req, map[string]string{
				"bucket": "testbucket",
				"object": "testobject",
			})
			req.Header.Set("Origin", "https://example.com")
			req.Header.Set("Access-Control-Request-Method", "GET")

			w := httptest.NewRecorder()
			middleware.HandleOptionsRequest(w, req)

			// Both existing and non-existent buckets should return same CORS headers
			actualOrigin := w.Header().Get("Access-Control-Allow-Origin")
			if actualOrigin != "https://example.com" {
				t.Errorf("%s: expected Access-Control-Allow-Origin='https://example.com', got '%s'",
					tc.description, actualOrigin)
			}

			// Both should return 200 OK for preflight
			if w.Code != http.StatusOK {
				t.Errorf("%s: expected status 200, got %d", tc.description, w.Code)
			}
		})
	}
}
