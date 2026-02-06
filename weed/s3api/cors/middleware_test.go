package cors

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gorilla/mux"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3err"
)

// Mock implementations for testing

type mockBucketChecker struct {
	bucketExists bool
}

func (m *mockBucketChecker) CheckBucket(r *http.Request, bucket string) s3err.ErrorCode {
	if m.bucketExists {
		return s3err.ErrNone
	}
	return s3err.ErrNoSuchBucket
}

type mockCORSConfigGetter struct {
	config  *CORSConfiguration
	errCode s3err.ErrorCode
}

func (m *mockCORSConfigGetter) GetCORSConfiguration(bucket string) (*CORSConfiguration, s3err.ErrorCode) {
	return m.config, m.errCode
}

// TestMiddlewareFallbackConfig tests that the middleware uses fallback config when bucket-level config is not available
func TestMiddlewareFallbackConfig(t *testing.T) {
	tests := []struct {
		name                 string
		bucketConfig         *CORSConfiguration
		fallbackConfig       *CORSConfiguration
		requestOrigin        string
		requestMethod        string
		isOptions            bool
		expectedStatus       int
		expectedOriginHeader string
		description          string
	}{
		{
			name:         "No bucket config, fallback to global config with wildcard",
			bucketConfig: nil,
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
			expectedStatus:       http.StatusOK,
			expectedOriginHeader: "https://example.com",
			description:          "Should use fallback global config when no bucket config exists",
		},
		{
			name:         "No bucket config, fallback to global config with specific origin",
			bucketConfig: nil,
			fallbackConfig: &CORSConfiguration{
				CORSRules: []CORSRule{
					{
						AllowedOrigins: []string{"https://example.com"},
						AllowedMethods: []string{"GET", "POST"},
						AllowedHeaders: []string{"*"},
					},
				},
			},
			requestOrigin:        "https://example.com",
			requestMethod:        "GET",
			isOptions:            false,
			expectedStatus:       http.StatusOK,
			expectedOriginHeader: "https://example.com",
			description:          "Should use fallback config with specific origin match",
		},
		{
			name:         "No bucket config, fallback rejects non-matching origin",
			bucketConfig: nil,
			fallbackConfig: &CORSConfiguration{
				CORSRules: []CORSRule{
					{
						AllowedOrigins: []string{"https://allowed.com"},
						AllowedMethods: []string{"GET"},
						AllowedHeaders: []string{"*"},
					},
				},
			},
			requestOrigin:        "https://notallowed.com",
			requestMethod:        "GET",
			isOptions:            false,
			expectedStatus:       http.StatusOK,
			expectedOriginHeader: "",
			description:          "Should not apply CORS headers when origin doesn't match fallback config",
		},
		{
			name: "Bucket config takes precedence over fallback",
			bucketConfig: &CORSConfiguration{
				CORSRules: []CORSRule{
					{
						AllowedOrigins: []string{"https://bucket-specific.com"},
						AllowedMethods: []string{"GET"},
						AllowedHeaders: []string{"*"},
					},
				},
			},
			fallbackConfig: &CORSConfiguration{
				CORSRules: []CORSRule{
					{
						AllowedOrigins: []string{"*"},
						AllowedMethods: []string{"GET", "POST"},
						AllowedHeaders: []string{"*"},
					},
				},
			},
			requestOrigin:        "https://bucket-specific.com",
			requestMethod:        "GET",
			isOptions:            false,
			expectedStatus:       http.StatusOK,
			expectedOriginHeader: "https://bucket-specific.com",
			description:          "Bucket-level config should be used instead of fallback",
		},
		{
			name: "Bucket config rejects, even though fallback would allow",
			bucketConfig: &CORSConfiguration{
				CORSRules: []CORSRule{
					{
						AllowedOrigins: []string{"https://restricted.com"},
						AllowedMethods: []string{"GET"},
						AllowedHeaders: []string{"*"},
					},
				},
			},
			fallbackConfig: &CORSConfiguration{
				CORSRules: []CORSRule{
					{
						AllowedOrigins: []string{"*"},
						AllowedMethods: []string{"GET", "POST"},
						AllowedHeaders: []string{"*"},
					},
				},
			},
			requestOrigin:        "https://example.com",
			requestMethod:        "GET",
			isOptions:            false,
			expectedStatus:       http.StatusOK,
			expectedOriginHeader: "",
			description:          "Bucket-level config is authoritative, fallback should not apply",
		},
		{
			name:                 "No config at all, no CORS headers",
			bucketConfig:         nil,
			fallbackConfig:       nil,
			requestOrigin:        "https://example.com",
			requestMethod:        "GET",
			isOptions:            false,
			expectedStatus:       http.StatusOK,
			expectedOriginHeader: "",
			description:          "Without any config, no CORS headers should be applied",
		},
		{
			name:         "OPTIONS preflight with fallback config",
			bucketConfig: nil,
			fallbackConfig: &CORSConfiguration{
				CORSRules: []CORSRule{
					{
						AllowedOrigins: []string{"https://example.com"},
						AllowedMethods: []string{"GET", "POST"},
						AllowedHeaders: []string{"*"},
					},
				},
			},
			requestOrigin:        "https://example.com",
			requestMethod:        "OPTIONS",
			isOptions:            true,
			expectedStatus:       http.StatusOK,
			expectedOriginHeader: "https://example.com",
			description:          "OPTIONS preflight should work with fallback config",
		},
		{
			name:                 "OPTIONS preflight without any config should fail",
			bucketConfig:         nil,
			fallbackConfig:       nil,
			requestOrigin:        "https://example.com",
			requestMethod:        "OPTIONS",
			isOptions:            true,
			expectedStatus:       http.StatusForbidden,
			expectedOriginHeader: "",
			description:          "OPTIONS preflight without config should return 403",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Setup mocks
			bucketChecker := &mockBucketChecker{bucketExists: true}
			configGetter := &mockCORSConfigGetter{
				config:  tt.bucketConfig,
				errCode: s3err.ErrNone,
			}

			// Create middleware with optional fallback
			middleware := NewMiddleware(bucketChecker, configGetter, tt.fallbackConfig)

			// Create request with mux variables
			req := httptest.NewRequest(tt.requestMethod, "/testbucket/testobject", nil)
			req = mux.SetURLVars(req, map[string]string{
				"bucket": "testbucket",
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

			// Create a simple handler that returns 200 OK
			nextHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusOK)
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

// TestMiddlewareFallbackConfigWithMultipleOrigins tests fallback with multiple allowed origins
func TestMiddlewareFallbackConfigWithMultipleOrigins(t *testing.T) {
	fallbackConfig := &CORSConfiguration{
		CORSRules: []CORSRule{
			{
				AllowedOrigins: []string{"https://example1.com", "https://example2.com"},
				AllowedMethods: []string{"GET", "POST"},
				AllowedHeaders: []string{"*"},
			},
		},
	}

	bucketChecker := &mockBucketChecker{bucketExists: true}
	configGetter := &mockCORSConfigGetter{
		config:  nil, // No bucket config
		errCode: s3err.ErrNone,
	}

	middleware := NewMiddleware(bucketChecker, configGetter, fallbackConfig)

	tests := []struct {
		origin      string
		shouldMatch bool
		description string
	}{
		{
			origin:      "https://example1.com",
			shouldMatch: true,
			description: "First allowed origin should match",
		},
		{
			origin:      "https://example2.com",
			shouldMatch: true,
			description: "Second allowed origin should match",
		},
		{
			origin:      "https://example3.com",
			shouldMatch: false,
			description: "Non-allowed origin should not match",
		},
	}

	for _, tt := range tests {
		t.Run(tt.description, func(t *testing.T) {
			req := httptest.NewRequest("GET", "/testbucket/testobject", nil)
			req = mux.SetURLVars(req, map[string]string{
				"bucket": "testbucket",
				"object": "testobject",
			})
			req.Header.Set("Origin", tt.origin)

			w := httptest.NewRecorder()
			nextHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusOK)
			})

			middleware.Handler(nextHandler).ServeHTTP(w, req)

			actualOrigin := w.Header().Get("Access-Control-Allow-Origin")
			if tt.shouldMatch {
				if actualOrigin != tt.origin {
					t.Errorf("%s: expected Access-Control-Allow-Origin='%s', got '%s'",
						tt.description, tt.origin, actualOrigin)
				}
			} else {
				if actualOrigin != "" {
					t.Errorf("%s: expected no Access-Control-Allow-Origin header, got '%s'",
						tt.description, actualOrigin)
				}
			}
		})
	}
}

// TestMiddlewareFallbackWithError tests that real errors (not "no config") don't trigger fallback
func TestMiddlewareFallbackWithError(t *testing.T) {
	fallbackConfig := &CORSConfiguration{
		CORSRules: []CORSRule{
			{
				AllowedOrigins: []string{"*"},
				AllowedMethods: []string{"GET", "POST"},
				AllowedHeaders: []string{"*"},
			},
		},
	}

	tests := []struct {
		name                 string
		errCode              s3err.ErrorCode
		expectedOriginHeader string
		description          string
	}{
		{
			name:                 "ErrAccessDenied should not trigger fallback",
			errCode:              s3err.ErrAccessDenied,
			expectedOriginHeader: "",
			description:          "Access denied errors should not expose CORS headers",
		},
		{
			name:                 "ErrInternalError should not trigger fallback",
			errCode:              s3err.ErrInternalError,
			expectedOriginHeader: "",
			description:          "Internal errors should not expose CORS headers",
		},
		{
			name:                 "ErrNoSuchBucket should trigger fallback",
			errCode:              s3err.ErrNoSuchBucket,
			expectedOriginHeader: "https://example.com",
			description:          "Bucket not found errors should expose CORS headers to prevent information disclosure",
		},
		{
			name:                 "ErrNoSuchCORSConfiguration should trigger fallback",
			errCode:              s3err.ErrNoSuchCORSConfiguration,
			expectedOriginHeader: "https://example.com",
			description:          "Explicit no CORS config should use fallback",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			bucketChecker := &mockBucketChecker{bucketExists: true}
			configGetter := &mockCORSConfigGetter{
				config:  nil,
				errCode: tt.errCode,
			}

			middleware := NewMiddleware(bucketChecker, configGetter, fallbackConfig)

			req := httptest.NewRequest("GET", "/testbucket/testobject", nil)
			req = mux.SetURLVars(req, map[string]string{
				"bucket": "testbucket",
				"object": "testobject",
			})
			req.Header.Set("Origin", "https://example.com")

			w := httptest.NewRecorder()
			nextHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusOK)
			})

			middleware.Handler(nextHandler).ServeHTTP(w, req)

			actualOrigin := w.Header().Get("Access-Control-Allow-Origin")
			if actualOrigin != tt.expectedOriginHeader {
				t.Errorf("%s: expected Access-Control-Allow-Origin='%s', got '%s'",
					tt.description, tt.expectedOriginHeader, actualOrigin)
			}
		})
	}
}

// TestMiddlewareVaryHeader tests that the Vary: Origin header is correctly applied in various scenarios
func TestMiddlewareVaryHeader(t *testing.T) {
	tests := []struct {
		name                 string
		bucketConfig         *CORSConfiguration
		shouldHaveVaryHeader bool
		description          string
	}{
		{
			name: "Specific allowed origin",
			bucketConfig: &CORSConfiguration{
				CORSRules: []CORSRule{
					{
						AllowedOrigins: []string{"https://example.com"},
						AllowedMethods: []string{"GET"},
						AllowedHeaders: []string{"*"},
					},
				},
			},
			shouldHaveVaryHeader: true,
			description:          "Should have Vary: Origin header when CORS config exists with specific origin",
		},
		{
			name: "Wildcard allowed origin",
			bucketConfig: &CORSConfiguration{
				CORSRules: []CORSRule{
					{
						AllowedOrigins: []string{"*"},
						AllowedMethods: []string{"GET"},
						AllowedHeaders: []string{"*"},
					},
				},
			},
			shouldHaveVaryHeader: true,
			description:          "Should have Vary: Origin header even when CORS config has wildcard origin",
		},
		{
			name:                 "No CORS configuration",
			bucketConfig:         nil,
			shouldHaveVaryHeader: false,
			description:          "Should NOT have Vary: Origin header when no CORS config exists",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Setup mocks
			bucketChecker := &mockBucketChecker{bucketExists: true}

			var errCode s3err.ErrorCode
			if tt.bucketConfig == nil {
				errCode = s3err.ErrNoSuchCORSConfiguration
			} else {
				errCode = s3err.ErrNone
			}

			configGetter := &mockCORSConfigGetter{
				config:  tt.bucketConfig,
				errCode: errCode,
			}

			// Create middleware
			middleware := NewMiddleware(bucketChecker, configGetter, nil)

			// Create request WITHOUT Origin header
			req := httptest.NewRequest("GET", "/testbucket/testobject", nil)
			req = mux.SetURLVars(req, map[string]string{
				"bucket": "testbucket",
				"object": "testobject",
			})

			// Create response recorder
			w := httptest.NewRecorder()

			// Create a simple handler that returns 200 OK
			nextHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusOK)
			})

			// Execute middleware
			middleware.Handler(nextHandler).ServeHTTP(w, req)

			// Check Vary header
			hasVaryOrigin := hasVaryOrigin(w.Header())

			if tt.shouldHaveVaryHeader && !hasVaryOrigin {
				t.Errorf("%s: expected Vary: Origin header, but got %v", tt.description, w.Header()["Vary"])
			} else if !tt.shouldHaveVaryHeader && hasVaryOrigin {
				t.Errorf("%s: expected NO Vary: Origin header, but got it", tt.description)
			}
		})
	}
}

// TestHandleOptionsRequestVaryHeader reproduces the issue where HandleOptionsRequest misses Vary: Origin
func TestHandleOptionsRequestVaryHeader(t *testing.T) {
	// Setup mocks
	bucketChecker := &mockBucketChecker{bucketExists: true}

	config := &CORSConfiguration{
		CORSRules: []CORSRule{
			{
				AllowedOrigins: []string{"https://example.com"},
				AllowedMethods: []string{"GET", "POST"},
				AllowedHeaders: []string{"*"},
			},
		},
	}

	configGetter := &mockCORSConfigGetter{
		config:  config,
		errCode: s3err.ErrNone,
	}

	// Create middleware
	middleware := NewMiddleware(bucketChecker, configGetter, nil)

	// Create OPTIONS request
	req := httptest.NewRequest("OPTIONS", "/testbucket/testobject", nil)
	req = mux.SetURLVars(req, map[string]string{
		"bucket": "testbucket",
		"object": "testobject",
	})

	// Set valid CORS headers
	req.Header.Set("Origin", "https://example.com")
	req.Header.Set("Access-Control-Request-Method", "GET")

	// Create response recorder
	w := httptest.NewRecorder()

	// Execute HandleOptionsRequest
	middleware.HandleOptionsRequest(w, req)

	// Check Response Status
	if w.Code != http.StatusOK {
		t.Errorf("Expected status 200, got %d", w.Code)
	}

	// Check Vary header - verified to FAIL without fix
	if !hasVaryOrigin(w.Header()) {
		t.Errorf("Expected Vary: Origin header in OPTIONS response, but got %v", w.Header()["Vary"])
	}
}

// hasVaryOrigin checks if the "Vary" header contains "Origin"
func hasVaryOrigin(header http.Header) bool {
	varyHeaders := header["Vary"]
	for _, h := range varyHeaders {
		for _, v := range strings.Split(h, ",") {
			if strings.TrimSpace(v) == "Origin" {
				return true
			}
		}
	}
	return false
}
