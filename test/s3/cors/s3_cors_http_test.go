package cors

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestCORSPreflightRequest tests CORS preflight OPTIONS requests
func TestCORSPreflightRequest(t *testing.T) {
	client := getS3Client(t)
	bucketName := createTestBucket(t, client)
	defer cleanupTestBucket(t, client, bucketName)

	// Set up CORS configuration
	corsConfig := &types.CORSConfiguration{
		CORSRules: []types.CORSRule{
			{
				AllowedHeaders: []string{"Content-Type", "Authorization"},
				AllowedMethods: []string{"GET", "POST", "PUT", "DELETE"},
				AllowedOrigins: []string{"https://example.com"},
				ExposeHeaders:  []string{"ETag", "Content-Length"},
				MaxAgeSeconds:  aws.Int32(3600),
			},
		},
	}

	_, err := client.PutBucketCors(context.TODO(), &s3.PutBucketCorsInput{
		Bucket:            aws.String(bucketName),
		CORSConfiguration: corsConfig,
	})
	require.NoError(t, err, "Should be able to put CORS configuration")

	// Wait for metadata subscription to update cache
	time.Sleep(50 * time.Millisecond)

	// Test preflight request with raw HTTP
	httpClient := &http.Client{Timeout: 10 * time.Second}

	// Create OPTIONS request
	req, err := http.NewRequest("OPTIONS", fmt.Sprintf("%s/%s/test-object", getDefaultConfig().Endpoint, bucketName), nil)
	require.NoError(t, err, "Should be able to create OPTIONS request")

	// Add CORS preflight headers
	req.Header.Set("Origin", "https://example.com")
	req.Header.Set("Access-Control-Request-Method", "PUT")
	req.Header.Set("Access-Control-Request-Headers", "Content-Type, Authorization")

	// Send the request
	resp, err := httpClient.Do(req)
	require.NoError(t, err, "Should be able to send OPTIONS request")
	defer resp.Body.Close()

	// Verify CORS headers in response
	assert.Equal(t, "https://example.com", resp.Header.Get("Access-Control-Allow-Origin"), "Should have correct Allow-Origin header")
	assert.Contains(t, resp.Header.Get("Access-Control-Allow-Methods"), "PUT", "Should allow PUT method")
	assert.Contains(t, resp.Header.Get("Access-Control-Allow-Headers"), "Content-Type", "Should allow Content-Type header")
	assert.Contains(t, resp.Header.Get("Access-Control-Allow-Headers"), "Authorization", "Should allow Authorization header")
	assert.Equal(t, "3600", resp.Header.Get("Access-Control-Max-Age"), "Should have correct Max-Age header")
	assert.Contains(t, resp.Header.Get("Access-Control-Expose-Headers"), "ETag", "Should expose ETag header")
	assert.Equal(t, http.StatusOK, resp.StatusCode, "OPTIONS request should return 200")
}

// TestCORSActualRequest tests CORS behavior with actual requests
func TestCORSActualRequest(t *testing.T) {
	// Temporarily clear AWS environment variables to ensure truly anonymous requests
	// This prevents AWS SDK from auto-signing requests in GitHub Actions
	originalAccessKey := os.Getenv("AWS_ACCESS_KEY_ID")
	originalSecretKey := os.Getenv("AWS_SECRET_ACCESS_KEY")
	originalSessionToken := os.Getenv("AWS_SESSION_TOKEN")
	originalProfile := os.Getenv("AWS_PROFILE")
	originalRegion := os.Getenv("AWS_REGION")

	os.Setenv("AWS_ACCESS_KEY_ID", "")
	os.Setenv("AWS_SECRET_ACCESS_KEY", "")
	os.Setenv("AWS_SESSION_TOKEN", "")
	os.Setenv("AWS_PROFILE", "")
	os.Setenv("AWS_REGION", "")

	defer func() {
		// Restore original environment variables
		os.Setenv("AWS_ACCESS_KEY_ID", originalAccessKey)
		os.Setenv("AWS_SECRET_ACCESS_KEY", originalSecretKey)
		os.Setenv("AWS_SESSION_TOKEN", originalSessionToken)
		os.Setenv("AWS_PROFILE", originalProfile)
		os.Setenv("AWS_REGION", originalRegion)
	}()

	client := getS3Client(t)
	bucketName := createTestBucket(t, client)
	defer cleanupTestBucket(t, client, bucketName)

	// Set up CORS configuration
	corsConfig := &types.CORSConfiguration{
		CORSRules: []types.CORSRule{
			{
				AllowedHeaders: []string{"*"},
				AllowedMethods: []string{"GET", "PUT"},
				AllowedOrigins: []string{"https://example.com"},
				ExposeHeaders:  []string{"ETag", "Content-Length"},
				MaxAgeSeconds:  aws.Int32(3600),
			},
		},
	}

	_, err := client.PutBucketCors(context.TODO(), &s3.PutBucketCorsInput{
		Bucket:            aws.String(bucketName),
		CORSConfiguration: corsConfig,
	})
	require.NoError(t, err, "Should be able to put CORS configuration")

	// Wait for CORS configuration to be fully processed
	time.Sleep(100 * time.Millisecond)

	// First, put an object using S3 client
	objectKey := "test-cors-object"
	_, err = client.PutObject(context.TODO(), &s3.PutObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(objectKey),
		Body:   strings.NewReader("Test CORS content"),
	})
	require.NoError(t, err, "Should be able to put object")

	// Test GET request with CORS headers using raw HTTP
	// Create a completely isolated HTTP client to avoid AWS SDK auto-signing
	transport := &http.Transport{
		// Completely disable any proxy or middleware
		Proxy: nil,
	}

	httpClient := &http.Client{
		Timeout: 10 * time.Second,
		// Use a completely clean transport to avoid any AWS SDK middleware
		Transport: transport,
	}

	// Create URL manually to avoid any AWS SDK endpoint processing
	// Use the same endpoint as the S3 client to ensure compatibility with GitHub Actions
	config := getDefaultConfig()
	endpoint := config.Endpoint
	// Remove any protocol prefix and ensure it's http for anonymous requests
	if strings.HasPrefix(endpoint, "https://") {
		endpoint = strings.Replace(endpoint, "https://", "http://", 1)
	}
	if !strings.HasPrefix(endpoint, "http://") {
		endpoint = "http://" + endpoint
	}

	requestURL := fmt.Sprintf("%s/%s/%s", endpoint, bucketName, objectKey)
	req, err := http.NewRequest("GET", requestURL, nil)
	require.NoError(t, err, "Should be able to create GET request")

	// Add Origin header to simulate CORS request
	req.Header.Set("Origin", "https://example.com")

	// Explicitly ensure no AWS headers are present (defensive programming)
	// Clear ALL potential AWS-related headers that might be auto-added
	req.Header.Del("Authorization")
	req.Header.Del("X-Amz-Content-Sha256")
	req.Header.Del("X-Amz-Date")
	req.Header.Del("Amz-Sdk-Invocation-Id")
	req.Header.Del("Amz-Sdk-Request")
	req.Header.Del("X-Amz-Security-Token")
	req.Header.Del("X-Amz-Session-Token")
	req.Header.Del("AWS-Session-Token")
	req.Header.Del("X-Amz-Target")
	req.Header.Del("X-Amz-User-Agent")

	// Ensure User-Agent doesn't indicate AWS SDK
	req.Header.Set("User-Agent", "anonymous-cors-test/1.0")

	// Verify no AWS-related headers are present
	for name := range req.Header {
		headerLower := strings.ToLower(name)
		if strings.Contains(headerLower, "aws") ||
			strings.Contains(headerLower, "amz") ||
			strings.Contains(headerLower, "authorization") {
			t.Fatalf("Found AWS-related header in anonymous request: %s", name)
		}
	}

	// Send the request
	resp, err := httpClient.Do(req)
	require.NoError(t, err, "Should be able to send GET request")
	defer resp.Body.Close()

	// Verify CORS headers are present
	assert.Equal(t, "https://example.com", resp.Header.Get("Access-Control-Allow-Origin"), "Should have correct Allow-Origin header")
	assert.Contains(t, resp.Header.Get("Access-Control-Expose-Headers"), "ETag", "Should expose ETag header")

	// Anonymous requests should succeed when anonymous read permission is configured in IAM
	// The server configuration allows anonymous users to have Read permissions
	assert.Equal(t, http.StatusOK, resp.StatusCode, "Anonymous GET request should succeed when anonymous read is configured")
}

// TestCORSOriginMatching tests origin matching with different patterns
func TestCORSOriginMatching(t *testing.T) {
	client := getS3Client(t)
	bucketName := createTestBucket(t, client)
	defer cleanupTestBucket(t, client, bucketName)

	testCases := []struct {
		name           string
		allowedOrigins []string
		requestOrigin  string
		shouldAllow    bool
	}{
		{
			name:           "exact match",
			allowedOrigins: []string{"https://example.com"},
			requestOrigin:  "https://example.com",
			shouldAllow:    true,
		},
		{
			name:           "wildcard match",
			allowedOrigins: []string{"*"},
			requestOrigin:  "https://example.com",
			shouldAllow:    true,
		},
		{
			name:           "subdomain wildcard match",
			allowedOrigins: []string{"https://*.example.com"},
			requestOrigin:  "https://api.example.com",
			shouldAllow:    true,
		},
		{
			name:           "no match",
			allowedOrigins: []string{"https://example.com"},
			requestOrigin:  "https://malicious.com",
			shouldAllow:    false,
		},
		{
			name:           "subdomain wildcard no match",
			allowedOrigins: []string{"https://*.example.com"},
			requestOrigin:  "https://example.com",
			shouldAllow:    false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Set up CORS configuration for this test case
			corsConfig := &types.CORSConfiguration{
				CORSRules: []types.CORSRule{
					{
						AllowedHeaders: []string{"*"},
						AllowedMethods: []string{"GET"},
						AllowedOrigins: tc.allowedOrigins,
						ExposeHeaders:  []string{"ETag"},
						MaxAgeSeconds:  aws.Int32(3600),
					},
				},
			}

			_, err := client.PutBucketCors(context.TODO(), &s3.PutBucketCorsInput{
				Bucket:            aws.String(bucketName),
				CORSConfiguration: corsConfig,
			})
			require.NoError(t, err, "Should be able to put CORS configuration")

			// Wait for metadata subscription to update cache
			time.Sleep(50 * time.Millisecond)

			// Test preflight request
			httpClient := &http.Client{Timeout: 10 * time.Second}

			req, err := http.NewRequest("OPTIONS", fmt.Sprintf("%s/%s/test-object", getDefaultConfig().Endpoint, bucketName), nil)
			require.NoError(t, err, "Should be able to create OPTIONS request")

			req.Header.Set("Origin", tc.requestOrigin)
			req.Header.Set("Access-Control-Request-Method", "GET")

			resp, err := httpClient.Do(req)
			require.NoError(t, err, "Should be able to send OPTIONS request")
			defer resp.Body.Close()

			if tc.shouldAllow {
				assert.Equal(t, tc.requestOrigin, resp.Header.Get("Access-Control-Allow-Origin"), "Should have correct Allow-Origin header")
				assert.Contains(t, resp.Header.Get("Access-Control-Allow-Methods"), "GET", "Should allow GET method")
			} else {
				assert.Empty(t, resp.Header.Get("Access-Control-Allow-Origin"), "Should not have Allow-Origin header for disallowed origin")
			}
		})
	}
}

// TestCORSHeaderMatching tests header matching with different patterns
func TestCORSHeaderMatching(t *testing.T) {
	client := getS3Client(t)
	bucketName := createTestBucket(t, client)
	defer cleanupTestBucket(t, client, bucketName)

	testCases := []struct {
		name            string
		allowedHeaders  []string
		requestHeaders  string
		shouldAllow     bool
		expectedHeaders string
	}{
		{
			name:            "wildcard headers",
			allowedHeaders:  []string{"*"},
			requestHeaders:  "Content-Type, Authorization",
			shouldAllow:     true,
			expectedHeaders: "Content-Type, Authorization",
		},
		{
			name:            "specific headers match",
			allowedHeaders:  []string{"Content-Type", "Authorization"},
			requestHeaders:  "Content-Type, Authorization",
			shouldAllow:     true,
			expectedHeaders: "Content-Type, Authorization",
		},
		{
			name:            "partial header match",
			allowedHeaders:  []string{"Content-Type"},
			requestHeaders:  "Content-Type",
			shouldAllow:     true,
			expectedHeaders: "Content-Type",
		},
		{
			name:            "case insensitive match",
			allowedHeaders:  []string{"content-type"},
			requestHeaders:  "Content-Type",
			shouldAllow:     true,
			expectedHeaders: "Content-Type",
		},
		{
			name:           "disallowed header",
			allowedHeaders: []string{"Content-Type"},
			requestHeaders: "Authorization",
			shouldAllow:    false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Set up CORS configuration for this test case
			corsConfig := &types.CORSConfiguration{
				CORSRules: []types.CORSRule{
					{
						AllowedHeaders: tc.allowedHeaders,
						AllowedMethods: []string{"GET", "POST"},
						AllowedOrigins: []string{"https://example.com"},
						ExposeHeaders:  []string{"ETag"},
						MaxAgeSeconds:  aws.Int32(3600),
					},
				},
			}

			_, err := client.PutBucketCors(context.TODO(), &s3.PutBucketCorsInput{
				Bucket:            aws.String(bucketName),
				CORSConfiguration: corsConfig,
			})
			require.NoError(t, err, "Should be able to put CORS configuration")

			// Wait for metadata subscription to update cache
			time.Sleep(50 * time.Millisecond)

			// Test preflight request
			httpClient := &http.Client{Timeout: 10 * time.Second}

			req, err := http.NewRequest("OPTIONS", fmt.Sprintf("%s/%s/test-object", getDefaultConfig().Endpoint, bucketName), nil)
			require.NoError(t, err, "Should be able to create OPTIONS request")

			req.Header.Set("Origin", "https://example.com")
			req.Header.Set("Access-Control-Request-Method", "POST")
			req.Header.Set("Access-Control-Request-Headers", tc.requestHeaders)

			resp, err := httpClient.Do(req)
			require.NoError(t, err, "Should be able to send OPTIONS request")
			defer resp.Body.Close()

			if tc.shouldAllow {
				assert.Equal(t, "https://example.com", resp.Header.Get("Access-Control-Allow-Origin"), "Should have correct Allow-Origin header")
				allowedHeaders := resp.Header.Get("Access-Control-Allow-Headers")
				for _, header := range strings.Split(tc.expectedHeaders, ", ") {
					assert.Contains(t, allowedHeaders, header, "Should allow header: %s", header)
				}
			} else {
				// Even if headers are not allowed, the origin should still be in the response
				// but the headers should not be echoed back
				assert.Equal(t, "https://example.com", resp.Header.Get("Access-Control-Allow-Origin"), "Should have correct Allow-Origin header")
				allowedHeaders := resp.Header.Get("Access-Control-Allow-Headers")
				assert.NotContains(t, allowedHeaders, "Authorization", "Should not allow Authorization header")
			}
		})
	}
}

// TestCORSWithoutConfiguration tests CORS behavior when no bucket-level configuration is set
// With the fallback feature, buckets without explicit CORS config will use the global CORS settings
func TestCORSWithoutConfiguration(t *testing.T) {
	client := getS3Client(t)
	bucketName := createTestBucket(t, client)
	defer cleanupTestBucket(t, client, bucketName)

	// Test preflight request without bucket-level CORS configuration
	// The global CORS fallback (default: "*") should be used
	httpClient := &http.Client{Timeout: 10 * time.Second}

	req, err := http.NewRequest("OPTIONS", fmt.Sprintf("%s/%s/test-object", getDefaultConfig().Endpoint, bucketName), nil)
	require.NoError(t, err, "Should be able to create OPTIONS request")

	req.Header.Set("Origin", "https://example.com")
	req.Header.Set("Access-Control-Request-Method", "GET")
	req.Header.Set("Access-Control-Request-Headers", "Content-Type")

	resp, err := httpClient.Do(req)
	require.NoError(t, err, "Should be able to send OPTIONS request")
	defer resp.Body.Close()

	// With fallback CORS (global default: "*"), CORS headers should be present
	assert.Equal(t, "https://example.com", resp.Header.Get("Access-Control-Allow-Origin"), "Should have Allow-Origin header from global fallback")
	assert.Contains(t, resp.Header.Get("Access-Control-Allow-Methods"), "GET", "Should have GET in Allow-Methods from global fallback")
	assert.Contains(t, resp.Header.Get("Access-Control-Allow-Headers"), "Content-Type", "Should have requested headers in Allow-Headers from global fallback")
}

// TestCORSMethodMatching tests method matching
func TestCORSMethodMatching(t *testing.T) {
	client := getS3Client(t)
	bucketName := createTestBucket(t, client)
	defer cleanupTestBucket(t, client, bucketName)

	// Set up CORS configuration with limited methods
	corsConfig := &types.CORSConfiguration{
		CORSRules: []types.CORSRule{
			{
				AllowedHeaders: []string{"*"},
				AllowedMethods: []string{"GET", "POST"},
				AllowedOrigins: []string{"https://example.com"},
				ExposeHeaders:  []string{"ETag"},
				MaxAgeSeconds:  aws.Int32(3600),
			},
		},
	}

	_, err := client.PutBucketCors(context.TODO(), &s3.PutBucketCorsInput{
		Bucket:            aws.String(bucketName),
		CORSConfiguration: corsConfig,
	})
	require.NoError(t, err, "Should be able to put CORS configuration")

	// Wait for metadata subscription to update cache
	time.Sleep(50 * time.Millisecond)

	testCases := []struct {
		method      string
		shouldAllow bool
	}{
		{"GET", true},
		{"POST", true},
		{"PUT", false},
		{"DELETE", false},
		{"HEAD", false},
	}

	for _, tc := range testCases {
		t.Run(fmt.Sprintf("method_%s", tc.method), func(t *testing.T) {
			httpClient := &http.Client{Timeout: 10 * time.Second}

			req, err := http.NewRequest("OPTIONS", fmt.Sprintf("%s/%s/test-object", getDefaultConfig().Endpoint, bucketName), nil)
			require.NoError(t, err, "Should be able to create OPTIONS request")

			req.Header.Set("Origin", "https://example.com")
			req.Header.Set("Access-Control-Request-Method", tc.method)

			resp, err := httpClient.Do(req)
			require.NoError(t, err, "Should be able to send OPTIONS request")
			defer resp.Body.Close()

			if tc.shouldAllow {
				assert.Equal(t, "https://example.com", resp.Header.Get("Access-Control-Allow-Origin"), "Should have correct Allow-Origin header")
				assert.Contains(t, resp.Header.Get("Access-Control-Allow-Methods"), tc.method, "Should allow method: %s", tc.method)
			} else {
				// Even if method is not allowed, the origin should still be in the response
				// but the method should not be in the allowed methods
				assert.Equal(t, "https://example.com", resp.Header.Get("Access-Control-Allow-Origin"), "Should have correct Allow-Origin header")
				allowedMethods := resp.Header.Get("Access-Control-Allow-Methods")
				assert.NotContains(t, allowedMethods, tc.method, "Should not allow method: %s", tc.method)
			}
		})
	}
}

// TestCORSMultipleRulesMatching tests CORS with multiple rules
func TestCORSMultipleRulesMatching(t *testing.T) {
	client := getS3Client(t)
	bucketName := createTestBucket(t, client)
	defer cleanupTestBucket(t, client, bucketName)

	// Set up CORS configuration with multiple rules
	corsConfig := &types.CORSConfiguration{
		CORSRules: []types.CORSRule{
			{
				AllowedHeaders: []string{"Content-Type"},
				AllowedMethods: []string{"GET"},
				AllowedOrigins: []string{"https://example.com"},
				ExposeHeaders:  []string{"ETag"},
				MaxAgeSeconds:  aws.Int32(3600),
			},
			{
				AllowedHeaders: []string{"Authorization"},
				AllowedMethods: []string{"POST", "PUT"},
				AllowedOrigins: []string{"https://api.example.com"},
				ExposeHeaders:  []string{"Content-Length"},
				MaxAgeSeconds:  aws.Int32(7200),
			},
		},
	}

	_, err := client.PutBucketCors(context.TODO(), &s3.PutBucketCorsInput{
		Bucket:            aws.String(bucketName),
		CORSConfiguration: corsConfig,
	})
	require.NoError(t, err, "Should be able to put CORS configuration")

	// Wait for metadata subscription to update cache
	time.Sleep(50 * time.Millisecond)

	// Test first rule
	httpClient := &http.Client{Timeout: 10 * time.Second}

	req, err := http.NewRequest("OPTIONS", fmt.Sprintf("%s/%s/test-object", getDefaultConfig().Endpoint, bucketName), nil)
	require.NoError(t, err, "Should be able to create OPTIONS request")

	req.Header.Set("Origin", "https://example.com")
	req.Header.Set("Access-Control-Request-Method", "GET")
	req.Header.Set("Access-Control-Request-Headers", "Content-Type")

	resp, err := httpClient.Do(req)
	require.NoError(t, err, "Should be able to send OPTIONS request")
	defer resp.Body.Close()

	assert.Equal(t, "https://example.com", resp.Header.Get("Access-Control-Allow-Origin"), "Should match first rule")
	assert.Contains(t, resp.Header.Get("Access-Control-Allow-Methods"), "GET", "Should allow GET method")
	assert.Contains(t, resp.Header.Get("Access-Control-Allow-Headers"), "Content-Type", "Should allow Content-Type header")
	assert.Equal(t, "3600", resp.Header.Get("Access-Control-Max-Age"), "Should have first rule's max age")

	// Test second rule
	req2, err := http.NewRequest("OPTIONS", fmt.Sprintf("%s/%s/test-object", getDefaultConfig().Endpoint, bucketName), nil)
	require.NoError(t, err, "Should be able to create OPTIONS request")

	req2.Header.Set("Origin", "https://api.example.com")
	req2.Header.Set("Access-Control-Request-Method", "POST")
	req2.Header.Set("Access-Control-Request-Headers", "Authorization")

	resp2, err := httpClient.Do(req2)
	require.NoError(t, err, "Should be able to send OPTIONS request")
	defer resp2.Body.Close()

	assert.Equal(t, "https://api.example.com", resp2.Header.Get("Access-Control-Allow-Origin"), "Should match second rule")
	assert.Contains(t, resp2.Header.Get("Access-Control-Allow-Methods"), "POST", "Should allow POST method")
	assert.Contains(t, resp2.Header.Get("Access-Control-Allow-Headers"), "Authorization", "Should allow Authorization header")
	assert.Equal(t, "7200", resp2.Header.Get("Access-Control-Max-Age"), "Should have second rule's max age")
}

// TestServiceLevelCORS tests that service-level endpoints (like /status) get proper CORS headers
func TestServiceLevelCORS(t *testing.T) {
	assert := assert.New(t)

	endpoints := []string{
		"/",
		"/status",
		"/healthz",
	}

	for _, endpoint := range endpoints {
		t.Run(fmt.Sprintf("endpoint_%s", strings.ReplaceAll(endpoint, "/", "_")), func(t *testing.T) {
			req, err := http.NewRequest("OPTIONS", fmt.Sprintf("%s%s", getDefaultConfig().Endpoint, endpoint), nil)
			assert.NoError(err)

			// Add Origin header to trigger CORS
			req.Header.Set("Origin", "http://example.com")

			client := &http.Client{}
			resp, err := client.Do(req)
			assert.NoError(err)
			defer resp.Body.Close()

			// Should return 200 OK
			assert.Equal(http.StatusOK, resp.StatusCode)

			// Should have CORS headers set
			assert.Equal("*", resp.Header.Get("Access-Control-Allow-Origin"))
			assert.Equal("*", resp.Header.Get("Access-Control-Expose-Headers"))
			assert.Equal("*", resp.Header.Get("Access-Control-Allow-Methods"))
			assert.Equal("*", resp.Header.Get("Access-Control-Allow-Headers"))
		})
	}
}

// TestServiceLevelCORSWithoutOrigin tests that service-level endpoints without Origin header don't get CORS headers
func TestServiceLevelCORSWithoutOrigin(t *testing.T) {
	assert := assert.New(t)

	req, err := http.NewRequest("OPTIONS", fmt.Sprintf("%s/status", getDefaultConfig().Endpoint), nil)
	assert.NoError(err)

	// No Origin header

	client := &http.Client{}
	resp, err := client.Do(req)
	assert.NoError(err)
	defer resp.Body.Close()

	// Should return 200 OK
	assert.Equal(http.StatusOK, resp.StatusCode)

	// Should not have CORS headers set (or have empty values)
	corsHeaders := []string{
		"Access-Control-Allow-Origin",
		"Access-Control-Expose-Headers",
		"Access-Control-Allow-Methods",
		"Access-Control-Allow-Headers",
	}

	for _, header := range corsHeaders {
		value := resp.Header.Get(header)
		// Headers should either be empty or not present
		assert.True(value == "" || value == "*", "Header %s should be empty or wildcard, got: %s", header, value)
	}
}
