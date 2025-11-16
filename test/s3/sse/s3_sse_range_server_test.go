package sse_test

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestSSECRangeRequestsServerBehavior tests that the server correctly handles Range requests
// for SSE-C encrypted objects by checking actual HTTP response (not SDK-processed response)
func TestSSECRangeRequestsServerBehavior(t *testing.T) {
	ctx := context.Background()
	client, err := createS3Client(ctx, defaultConfig)
	require.NoError(t, err, "Failed to create S3 client")

	bucketName, err := createTestBucket(ctx, client, defaultConfig.BucketPrefix+"ssec-range-server-")
	require.NoError(t, err, "Failed to create test bucket")
	defer cleanupTestBucket(ctx, client, bucketName)

	sseKey := generateSSECKey()
	testData := generateTestData(2048) // 2KB test file
	objectKey := "test-range-server-validation"

	// Upload with SSE-C
	_, err = client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:               aws.String(bucketName),
		Key:                  aws.String(objectKey),
		Body:                 bytes.NewReader(testData),
		SSECustomerAlgorithm: aws.String("AES256"),
		SSECustomerKey:       aws.String(sseKey.KeyB64),
		SSECustomerKeyMD5:    aws.String(sseKey.KeyMD5),
	})
	require.NoError(t, err, "Failed to upload SSE-C object")

	// Test cases for range requests
	testCases := []struct {
		name          string
		rangeHeader   string
		expectedStart int64
		expectedEnd   int64
		expectedTotal int64
	}{
		{
			name:          "First 100 bytes",
			rangeHeader:   "bytes=0-99",
			expectedStart: 0,
			expectedEnd:   99,
			expectedTotal: 2048,
		},
		{
			name:          "Middle range",
			rangeHeader:   "bytes=500-699",
			expectedStart: 500,
			expectedEnd:   699,
			expectedTotal: 2048,
		},
		{
			name:          "Last 100 bytes",
			rangeHeader:   "bytes=1948-2047",
			expectedStart: 1948,
			expectedEnd:   2047,
			expectedTotal: 2048,
		},
		{
			name:          "Single byte",
			rangeHeader:   "bytes=1000-1000",
			expectedStart: 1000,
			expectedEnd:   1000,
			expectedTotal: 2048,
		},
		{
			name:          "AES block boundary crossing",
			rangeHeader:   "bytes=15-17",
			expectedStart: 15,
			expectedEnd:   17,
			expectedTotal: 2048,
		},
		{
			name:          "Open-ended range",
			rangeHeader:   "bytes=2000-",
			expectedStart: 2000,
			expectedEnd:   2047,
			expectedTotal: 2048,
		},
		{
			name:          "Suffix range (last 100 bytes)",
			rangeHeader:   "bytes=-100",
			expectedStart: 1948,
			expectedEnd:   2047,
			expectedTotal: 2048,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Build object URL
			objectURL := fmt.Sprintf("http://%s/%s/%s",
				defaultConfig.Endpoint,
				bucketName,
				objectKey,
			)

			// Create raw HTTP request
			req, err := http.NewRequest("GET", objectURL, nil)
			require.NoError(t, err, "Failed to create HTTP request")

			// Add Range header
			req.Header.Set("Range", tc.rangeHeader)

			// Add SSE-C headers
			req.Header.Set("x-amz-server-side-encryption-customer-algorithm", "AES256")
			req.Header.Set("x-amz-server-side-encryption-customer-key", sseKey.KeyB64)
			req.Header.Set("x-amz-server-side-encryption-customer-key-MD5", sseKey.KeyMD5)

			// Make request with raw HTTP client
			httpClient := &http.Client{}
			resp, err := httpClient.Do(req)
			require.NoError(t, err, "Failed to execute range request")
			defer resp.Body.Close()

			// CRITICAL CHECK 1: Status code must be 206 Partial Content
			assert.Equal(t, http.StatusPartialContent, resp.StatusCode,
				"Server must return 206 Partial Content for range request, got %d", resp.StatusCode)

			// CRITICAL CHECK 2: Content-Range header must be present and correct
			expectedContentRange := fmt.Sprintf("bytes %d-%d/%d",
				tc.expectedStart, tc.expectedEnd, tc.expectedTotal)
			actualContentRange := resp.Header.Get("Content-Range")
			assert.Equal(t, expectedContentRange, actualContentRange,
				"Content-Range header mismatch")

			// CRITICAL CHECK 3: Content-Length must match requested range size
			expectedLength := tc.expectedEnd - tc.expectedStart + 1
			actualLength := resp.ContentLength
			assert.Equal(t, expectedLength, actualLength,
				"Content-Length mismatch: expected %d, got %d", expectedLength, actualLength)

			// CRITICAL CHECK 4: Actual bytes received from network
			bodyBytes, err := io.ReadAll(resp.Body)
			require.NoError(t, err, "Failed to read response body")
			assert.Equal(t, int(expectedLength), len(bodyBytes),
				"Actual bytes received from server mismatch: expected %d, got %d",
				expectedLength, len(bodyBytes))

			// CRITICAL CHECK 5: Verify decrypted content matches expected range
			expectedData := testData[tc.expectedStart : tc.expectedEnd+1]
			assert.Equal(t, expectedData, bodyBytes,
				"Decrypted range content doesn't match expected data")

			// Verify SSE-C headers are present in response
			assert.Equal(t, "AES256", resp.Header.Get("x-amz-server-side-encryption-customer-algorithm"),
				"SSE-C algorithm header missing in range response")
			assert.Equal(t, sseKey.KeyMD5, resp.Header.Get("x-amz-server-side-encryption-customer-key-MD5"),
				"SSE-C key MD5 header missing in range response")
		})
	}
}

// TestSSEKMSRangeRequestsServerBehavior tests server-side Range handling for SSE-KMS
func TestSSEKMSRangeRequestsServerBehavior(t *testing.T) {
	ctx := context.Background()
	client, err := createS3Client(ctx, defaultConfig)
	require.NoError(t, err, "Failed to create S3 client")

	bucketName, err := createTestBucket(ctx, client, defaultConfig.BucketPrefix+"ssekms-range-server-")
	require.NoError(t, err, "Failed to create test bucket")
	defer cleanupTestBucket(ctx, client, bucketName)

	kmsKeyID := "test-range-key"
	testData := generateTestData(4096) // 4KB test file
	objectKey := "test-kms-range-server-validation"

	// Upload with SSE-KMS
	_, err = client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:               aws.String(bucketName),
		Key:                  aws.String(objectKey),
		Body:                 bytes.NewReader(testData),
		ServerSideEncryption: "aws:kms",
		SSEKMSKeyId:          aws.String(kmsKeyID),
	})
	require.NoError(t, err, "Failed to upload SSE-KMS object")

	// Test various ranges
	testCases := []struct {
		name        string
		rangeHeader string
		start       int64
		end         int64
	}{
		{"First KB", "bytes=0-1023", 0, 1023},
		{"Second KB", "bytes=1024-2047", 1024, 2047},
		{"Last KB", "bytes=3072-4095", 3072, 4095},
		{"Unaligned range", "bytes=100-299", 100, 299},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			objectURL := fmt.Sprintf("http://%s/%s/%s",
				defaultConfig.Endpoint,
				bucketName,
				objectKey,
			)

			req, err := http.NewRequest("GET", objectURL, nil)
			require.NoError(t, err)
			req.Header.Set("Range", tc.rangeHeader)

			httpClient := &http.Client{}
			resp, err := httpClient.Do(req)
			require.NoError(t, err)
			defer resp.Body.Close()

			// Verify 206 status
			assert.Equal(t, http.StatusPartialContent, resp.StatusCode,
				"SSE-KMS range request must return 206, got %d", resp.StatusCode)

			// Verify Content-Range
			expectedContentRange := fmt.Sprintf("bytes %d-%d/%d", tc.start, tc.end, int64(len(testData)))
			assert.Equal(t, expectedContentRange, resp.Header.Get("Content-Range"))

			// Verify actual bytes received
			bodyBytes, err := io.ReadAll(resp.Body)
			require.NoError(t, err)
			expectedLength := tc.end - tc.start + 1
			assert.Equal(t, int(expectedLength), len(bodyBytes),
				"Actual network bytes mismatch")

			// Verify content
			expectedData := testData[tc.start : tc.end+1]
			assert.Equal(t, expectedData, bodyBytes)
		})
	}
}

// TestSSES3RangeRequestsServerBehavior tests server-side Range handling for SSE-S3
func TestSSES3RangeRequestsServerBehavior(t *testing.T) {
	ctx := context.Background()
	client, err := createS3Client(ctx, defaultConfig)
	require.NoError(t, err, "Failed to create S3 client")

	bucketName, err := createTestBucket(ctx, client, "sses3-range-server")
	require.NoError(t, err, "Failed to create test bucket")
	defer cleanupTestBucket(ctx, client, bucketName)

	testData := generateTestData(8192) // 8KB test file
	objectKey := "test-s3-range-server-validation"

	// Upload with SSE-S3
	_, err = client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:               aws.String(bucketName),
		Key:                  aws.String(objectKey),
		Body:                 bytes.NewReader(testData),
		ServerSideEncryption: "AES256",
	})
	require.NoError(t, err, "Failed to upload SSE-S3 object")

	// Test range request
	objectURL := fmt.Sprintf("http://%s/%s/%s",
		defaultConfig.Endpoint,
		bucketName,
		objectKey,
	)

	req, err := http.NewRequest("GET", objectURL, nil)
	require.NoError(t, err)
	req.Header.Set("Range", "bytes=1000-1999")

	httpClient := &http.Client{}
	resp, err := httpClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	// Verify server response
	assert.Equal(t, http.StatusPartialContent, resp.StatusCode)
	assert.Equal(t, "bytes 1000-1999/8192", resp.Header.Get("Content-Range"))
	assert.Equal(t, int64(1000), resp.ContentLength)

	bodyBytes, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	assert.Equal(t, 1000, len(bodyBytes))
	assert.Equal(t, testData[1000:2000], bodyBytes)
}

// TestSSEMultipartRangeRequestsServerBehavior tests Range requests on multipart encrypted objects
func TestSSEMultipartRangeRequestsServerBehavior(t *testing.T) {
	ctx := context.Background()
	client, err := createS3Client(ctx, defaultConfig)
	require.NoError(t, err)

	bucketName, err := createTestBucket(ctx, client, defaultConfig.BucketPrefix+"ssec-mp-range-")
	require.NoError(t, err)
	defer cleanupTestBucket(ctx, client, bucketName)

	sseKey := generateSSECKey()
	objectKey := "test-multipart-range-server"

	// Create 10MB test data (2 parts of 5MB each)
	partSize := 5 * 1024 * 1024
	part1Data := generateTestData(partSize)
	part2Data := generateTestData(partSize)
	fullData := append(part1Data, part2Data...)

	// Initiate multipart upload
	createResp, err := client.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{
		Bucket:               aws.String(bucketName),
		Key:                  aws.String(objectKey),
		SSECustomerAlgorithm: aws.String("AES256"),
		SSECustomerKey:       aws.String(sseKey.KeyB64),
		SSECustomerKeyMD5:    aws.String(sseKey.KeyMD5),
	})
	require.NoError(t, err)
	uploadID := aws.ToString(createResp.UploadId)

	// Upload part 1
	part1Resp, err := client.UploadPart(ctx, &s3.UploadPartInput{
		Bucket:               aws.String(bucketName),
		Key:                  aws.String(objectKey),
		UploadId:             aws.String(uploadID),
		PartNumber:           aws.Int32(1),
		Body:                 bytes.NewReader(part1Data),
		SSECustomerAlgorithm: aws.String("AES256"),
		SSECustomerKey:       aws.String(sseKey.KeyB64),
		SSECustomerKeyMD5:    aws.String(sseKey.KeyMD5),
	})
	require.NoError(t, err)

	// Upload part 2
	part2Resp, err := client.UploadPart(ctx, &s3.UploadPartInput{
		Bucket:               aws.String(bucketName),
		Key:                  aws.String(objectKey),
		UploadId:             aws.String(uploadID),
		PartNumber:           aws.Int32(2),
		Body:                 bytes.NewReader(part2Data),
		SSECustomerAlgorithm: aws.String("AES256"),
		SSECustomerKey:       aws.String(sseKey.KeyB64),
		SSECustomerKeyMD5:    aws.String(sseKey.KeyMD5),
	})
	require.NoError(t, err)

	// Complete multipart upload
	_, err = client.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{
		Bucket:   aws.String(bucketName),
		Key:      aws.String(objectKey),
		UploadId: aws.String(uploadID),
		MultipartUpload: &s3types.CompletedMultipartUpload{
			Parts: []s3types.CompletedPart{
				{PartNumber: aws.Int32(1), ETag: part1Resp.ETag},
				{PartNumber: aws.Int32(2), ETag: part2Resp.ETag},
			},
		},
	})
	require.NoError(t, err)

	// Test range that crosses part boundary
	objectURL := fmt.Sprintf("http://%s/%s/%s",
		defaultConfig.Endpoint,
		bucketName,
		objectKey,
	)

	// Range spanning across the part boundary
	start := int64(partSize - 1000)
	end := int64(partSize + 1000)

	req, err := http.NewRequest("GET", objectURL, nil)
	require.NoError(t, err)
	req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", start, end))
	req.Header.Set("x-amz-server-side-encryption-customer-algorithm", "AES256")
	req.Header.Set("x-amz-server-side-encryption-customer-key", sseKey.KeyB64)
	req.Header.Set("x-amz-server-side-encryption-customer-key-MD5", sseKey.KeyMD5)

	httpClient := &http.Client{}
	resp, err := httpClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	// Verify server behavior for cross-part range
	assert.Equal(t, http.StatusPartialContent, resp.StatusCode,
		"Multipart range request must return 206")

	expectedLength := end - start + 1
	assert.Equal(t, expectedLength, resp.ContentLength,
		"Content-Length for cross-part range")

	bodyBytes, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	assert.Equal(t, int(expectedLength), len(bodyBytes),
		"Actual bytes for cross-part range")

	// Verify content spans the part boundary correctly
	expectedData := fullData[start : end+1]
	assert.Equal(t, expectedData, bodyBytes,
		"Cross-part range content must be correctly decrypted and assembled")
}

