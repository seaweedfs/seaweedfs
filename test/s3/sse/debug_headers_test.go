package sse_test

import (
	"context"
	"crypto/md5"
	"encoding/base64"
	"net/http"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stretchr/testify/require"
)

// RoundTripperWithLogging wraps an http.RoundTripper and logs requests/responses
type RoundTripperWithLogging struct {
	transport http.RoundTripper
	t         *testing.T
}

func (rt *RoundTripperWithLogging) RoundTrip(req *http.Request) (*http.Response, error) {
	rt.t.Logf("REQUEST: %s %s", req.Method, req.URL.String())
	for k, v := range req.Header {
		if strings.Contains(strings.ToLower(k), "encrypt") || strings.Contains(strings.ToLower(k), "sse") {
			rt.t.Logf("  Header %s: %v", k, v)
		}
	}
	
	resp, err := rt.transport.RoundTrip(req)
	if resp != nil {
		rt.t.Logf("RESPONSE: %d %s", resp.StatusCode, resp.Status)
	}
	return resp, err
}

// TestSSECHeaderDebugging tests SSE-C with detailed header logging
func TestSSECHeaderDebugging(t *testing.T) {
	ctx := context.Background()
	
	// Create a known SSE-C key for testing
	key := make([]byte, 32)
	copy(key, []byte("test-key-for-debugging-purposes!"))
	
	keyB64 := base64.StdEncoding.EncodeToString(key)
	keyMD5Hash := md5.Sum(key)
	keyMD5 := base64.StdEncoding.EncodeToString(keyMD5Hash[:])
	
	t.Logf("Using test key: %x", key)
	t.Logf("Key base64: %s", keyB64)
	t.Logf("Key MD5: %s", keyMD5)
	
	// Create HTTP client with logging
	httpClient := &http.Client{
		Transport: &RoundTripperWithLogging{
			transport: http.DefaultTransport,
			t:         t,
		},
	}
	
	// Create S3 client with custom HTTP client
	customResolver := aws.EndpointResolverWithOptionsFunc(func(service, region string, options ...interface{}) (aws.Endpoint, error) {
		return aws.Endpoint{
			URL:               "http://127.0.0.1:8333",
			HostnameImmutable: true,
		}, nil
	})

	awsCfg, err := config.LoadDefaultConfig(ctx,
		config.WithRegion("us-east-1"),
		config.WithEndpointResolverWithOptions(customResolver),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(
			"some_access_key1",
			"some_secret_key1",
			"",
		)),
		config.WithHTTPClient(httpClient),
	)
	require.NoError(t, err)

	client := s3.NewFromConfig(awsCfg, func(o *s3.Options) {
		o.UsePathStyle = true
	})
	
	bucketName := "test-debug-bucket"
	objectKey := "test-debug-object"
	
	// Try to create bucket first
	t.Log("Creating test bucket...")
	_, err = client.CreateBucket(ctx, &s3.CreateBucketInput{
		Bucket: aws.String(bucketName),
	})
	if err != nil {
		t.Logf("Bucket creation error (might be OK if exists): %v", err)
	}
	
	// Try PUT with SSE-C
	t.Log("Attempting PUT with SSE-C...")
	_, err = client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(objectKey),
		Body:   strings.NewReader("test data"),
		SSECustomerAlgorithm: aws.String("AES256"),
		SSECustomerKey:       aws.String(keyB64),
		SSECustomerKeyMD5:    aws.String(keyMD5),
	})
	
	if err != nil {
		t.Logf("PUT failed as expected: %v", err)
	} else {
		t.Log("PUT succeeded!")
	}
}
