package cors

import (
	"context"
	"fmt"
	"net/http"
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
				MaxAgeSeconds:  3600,
			},
		},
	}

	_, err := client.PutBucketCors(context.TODO(), &s3.PutBucketCorsInput{
		Bucket:            aws.String(bucketName),
		CORSConfiguration: corsConfig,
	})
	require.NoError(t, err, "Should be able to put CORS configuration")

	// Test preflight request with raw HTTP
	httpClient := &http.Client{Timeout: 10 * time.Second}

	// Create OPTIONS request
	req, err := http.NewRequest("OPTIONS", fmt.Sprintf("%s/%s/test-object", defaultConfig.Endpoint, bucketName), nil)
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
				MaxAgeSeconds:  3600,
			},
		},
	}

	_, err := client.PutBucketCors(context.TODO(), &s3.PutBucketCorsInput{
		Bucket:            aws.String(bucketName),
		CORSConfiguration: corsConfig,
	})
	require.NoError(t, err, "Should be able to put CORS configuration")

	// First, put an object using S3 client
	objectKey := "test-cors-object"
	_, err = client.PutObject(context.TODO(), &s3.PutObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(objectKey),
		Body:   strings.NewReader("Test CORS content"),
	})
	require.NoError(t, err, "Should be able to put object")

	// Test GET request with CORS headers using raw HTTP
	httpClient := &http.Client{Timeout: 10 * time.Second}

	req, err := http.NewRequest("GET", fmt.Sprintf("%s/%s/%s", defaultConfig.Endpoint, bucketName, objectKey), nil)
	require.NoError(t, err, "Should be able to create GET request")

	// Add Origin header to simulate CORS request
	req.Header.Set("Origin", "https://example.com")

	// Send the request
	resp, err := httpClient.Do(req)
	require.NoError(t, err, "Should be able to send GET request")
	defer resp.Body.Close()

	// Verify CORS headers in response
	assert.Equal(t, "https://example.com", resp.Header.Get("Access-Control-Allow-Origin"), "Should have correct Allow-Origin header")
	assert.Contains(t, resp.Header.Get("Access-Control-Expose-Headers"), "ETag", "Should expose ETag header")
	assert.Equal(t, http.StatusOK, resp.StatusCode, "GET request should return 200")
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
						MaxAgeSeconds:  3600,
					},
				},
			}

			_, err := client.PutBucketCors(context.TODO(), &s3.PutBucketCorsInput{
				Bucket:            aws.String(bucketName),
				CORSConfiguration: corsConfig,
			})
			require.NoError(t, err, "Should be able to put CORS configuration")

			// Test preflight request
			httpClient := &http.Client{Timeout: 10 * time.Second}

			req, err := http.NewRequest("OPTIONS", fmt.Sprintf("%s/%s/test-object", defaultConfig.Endpoint, bucketName), nil)
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
						MaxAgeSeconds:  3600,
					},
				},
			}

			_, err := client.PutBucketCors(context.TODO(), &s3.PutBucketCorsInput{
				Bucket:            aws.String(bucketName),
				CORSConfiguration: corsConfig,
			})
			require.NoError(t, err, "Should be able to put CORS configuration")

			// Test preflight request
			httpClient := &http.Client{Timeout: 10 * time.Second}

			req, err := http.NewRequest("OPTIONS", fmt.Sprintf("%s/%s/test-object", defaultConfig.Endpoint, bucketName), nil)
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

// TestCORSWithoutConfiguration tests CORS behavior when no configuration is set
func TestCORSWithoutConfiguration(t *testing.T) {
	client := getS3Client(t)
	bucketName := createTestBucket(t, client)
	defer cleanupTestBucket(t, client, bucketName)

	// Test preflight request without CORS configuration
	httpClient := &http.Client{Timeout: 10 * time.Second}

	req, err := http.NewRequest("OPTIONS", fmt.Sprintf("%s/%s/test-object", defaultConfig.Endpoint, bucketName), nil)
	require.NoError(t, err, "Should be able to create OPTIONS request")

	req.Header.Set("Origin", "https://example.com")
	req.Header.Set("Access-Control-Request-Method", "GET")

	resp, err := httpClient.Do(req)
	require.NoError(t, err, "Should be able to send OPTIONS request")
	defer resp.Body.Close()

	// Without CORS configuration, CORS headers should not be present
	assert.Empty(t, resp.Header.Get("Access-Control-Allow-Origin"), "Should not have Allow-Origin header without CORS config")
	assert.Empty(t, resp.Header.Get("Access-Control-Allow-Methods"), "Should not have Allow-Methods header without CORS config")
	assert.Empty(t, resp.Header.Get("Access-Control-Allow-Headers"), "Should not have Allow-Headers header without CORS config")
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
				MaxAgeSeconds:  3600,
			},
		},
	}

	_, err := client.PutBucketCors(context.TODO(), &s3.PutBucketCorsInput{
		Bucket:            aws.String(bucketName),
		CORSConfiguration: corsConfig,
	})
	require.NoError(t, err, "Should be able to put CORS configuration")

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

			req, err := http.NewRequest("OPTIONS", fmt.Sprintf("%s/%s/test-object", defaultConfig.Endpoint, bucketName), nil)
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
				MaxAgeSeconds:  3600,
			},
			{
				AllowedHeaders: []string{"Authorization"},
				AllowedMethods: []string{"POST", "PUT"},
				AllowedOrigins: []string{"https://api.example.com"},
				ExposeHeaders:  []string{"Content-Length"},
				MaxAgeSeconds:  7200,
			},
		},
	}

	_, err := client.PutBucketCors(context.TODO(), &s3.PutBucketCorsInput{
		Bucket:            aws.String(bucketName),
		CORSConfiguration: corsConfig,
	})
	require.NoError(t, err, "Should be able to put CORS configuration")

	// Test first rule
	httpClient := &http.Client{Timeout: 10 * time.Second}

	req, err := http.NewRequest("OPTIONS", fmt.Sprintf("%s/%s/test-object", defaultConfig.Endpoint, bucketName), nil)
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
	req2, err := http.NewRequest("OPTIONS", fmt.Sprintf("%s/%s/test-object", defaultConfig.Endpoint, bucketName), nil)
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
