package encryption

import (
	"context"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSSECCopyOperations(t *testing.T) {
	client := getS3Client(t)
	bucketName := createTestBucket(t, client)

	// Test data
	testData := "This is test data for SSE-C copy operations"

	// Upload source object with SSE-C
	sourceKey := "test-source-sse-c"
	_, err := client.PutObject(context.TODO(), &s3.PutObjectInput{
		Bucket:               aws.String(bucketName),
		Key:                  aws.String(sourceKey),
		Body:                 strings.NewReader(testData),
		SSECustomerAlgorithm: aws.String("AES256"),
		SSECustomerKey:       aws.String(testSSEKey),
		SSECustomerKeyMD5:    aws.String(testSSEKeyMD5),
	})
	require.NoError(t, err, "Failed to upload source object with SSE-C")

	testCases := []struct {
		name           string
		description    string
		sourceKey      string
		sourceKeyMD5   string
		destKey        string
		destKeyMD5     string
		expectedResult string
		shouldSucceed  bool
	}{
		{
			name:           "same_key_copy",
			description:    "Copy with same SSE-C key (fast path)",
			sourceKey:      testSSEKey,
			sourceKeyMD5:   testSSEKeyMD5,
			destKey:        testSSEKey,
			destKeyMD5:     testSSEKeyMD5,
			expectedResult: testData,
			shouldSucceed:  true,
		},
		{
			name:           "different_key_copy",
			description:    "Copy with different SSE-C key (slow path)",
			sourceKey:      testSSEKey,
			sourceKeyMD5:   testSSEKeyMD5,
			destKey:        wrongSSEKey,
			destKeyMD5:     wrongSSEKeyMD5,
			expectedResult: testData,
			shouldSucceed:  true,
		},
		{
			name:           "sse_to_unencrypted",
			description:    "Copy from SSE-C to unencrypted (slow path)",
			sourceKey:      testSSEKey,
			sourceKeyMD5:   testSSEKeyMD5,
			destKey:        "",
			destKeyMD5:     "",
			expectedResult: testData,
			shouldSucceed:  true,
		},
		{
			name:           "wrong_source_key",
			description:    "Copy with wrong source key should fail",
			sourceKey:      wrongSSEKey,
			sourceKeyMD5:   wrongSSEKeyMD5,
			destKey:        testSSEKey,
			destKeyMD5:     testSSEKeyMD5,
			expectedResult: "",
			shouldSucceed:  false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			destKey := "test-dest-" + tc.name

			// Prepare copy request
			copyInput := &s3.CopyObjectInput{
				Bucket:     aws.String(bucketName),
				Key:        aws.String(destKey),
				CopySource: aws.String(bucketName + "/" + sourceKey),
			}

			// Add copy source SSE-C headers
			if tc.sourceKey != "" {
				copyInput.CopySourceSSECustomerAlgorithm = aws.String("AES256")
				copyInput.CopySourceSSECustomerKey = aws.String(tc.sourceKey)
				copyInput.CopySourceSSECustomerKeyMD5 = aws.String(tc.sourceKeyMD5)
			}

			// Add destination SSE-C headers
			if tc.destKey != "" {
				copyInput.SSECustomerAlgorithm = aws.String("AES256")
				copyInput.SSECustomerKey = aws.String(tc.destKey)
				copyInput.SSECustomerKeyMD5 = aws.String(tc.destKeyMD5)
			}

			// Perform copy
			_, err := client.CopyObject(context.TODO(), copyInput)

			if tc.shouldSucceed {
				require.NoError(t, err, "Copy operation should succeed for %s", tc.description)

				// Verify the copied object
				getInput := &s3.GetObjectInput{
					Bucket: aws.String(bucketName),
					Key:    aws.String(destKey),
				}

				// Add SSE-C headers for reading if destination is encrypted
				if tc.destKey != "" {
					getInput.SSECustomerAlgorithm = aws.String("AES256")
					getInput.SSECustomerKey = aws.String(tc.destKey)
					getInput.SSECustomerKeyMD5 = aws.String(tc.destKeyMD5)
				}

				getResp, err := client.GetObject(context.TODO(), getInput)
				require.NoError(t, err, "Failed to read copied object")

				// Verify content
				content := make([]byte, len(tc.expectedResult))
				_, err = getResp.Body.Read(content)
				require.NoError(t, err, "Failed to read content")
				getResp.Body.Close()

				assert.Equal(t, tc.expectedResult, string(content), "Copied content doesn't match for %s", tc.description)

			} else {
				assert.Error(t, err, "Copy operation should fail for %s", tc.description)
			}
		})
	}
}

func TestSSECCopyUnencryptedToSSEC(t *testing.T) {
	client := getS3Client(t)
	bucketName := createTestBucket(t, client)

	testData := "This is unencrypted test data to be copied to SSE-C"
	sourceKey := "test-unencrypted-source"

	// Upload unencrypted source object
	_, err := client.PutObject(context.TODO(), &s3.PutObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(sourceKey),
		Body:   strings.NewReader(testData),
	})
	require.NoError(t, err, "Failed to upload unencrypted source object")

	// Copy to SSE-C encrypted destination
	destKey := "test-encrypted-dest"
	_, err = client.CopyObject(context.TODO(), &s3.CopyObjectInput{
		Bucket:               aws.String(bucketName),
		Key:                  aws.String(destKey),
		CopySource:           aws.String(bucketName + "/" + sourceKey),
		SSECustomerAlgorithm: aws.String("AES256"),
		SSECustomerKey:       aws.String(testSSEKey),
		SSECustomerKeyMD5:    aws.String(testSSEKeyMD5),
	})
	require.NoError(t, err, "Failed to copy unencrypted to SSE-C")

	// Verify the copied encrypted object
	getResp, err := client.GetObject(context.TODO(), &s3.GetObjectInput{
		Bucket:               aws.String(bucketName),
		Key:                  aws.String(destKey),
		SSECustomerAlgorithm: aws.String("AES256"),
		SSECustomerKey:       aws.String(testSSEKey),
		SSECustomerKeyMD5:    aws.String(testSSEKeyMD5),
	})
	require.NoError(t, err, "Failed to read copied encrypted object")

	content := make([]byte, len(testData))
	_, err = getResp.Body.Read(content)
	require.NoError(t, err, "Failed to read content")
	getResp.Body.Close()

	assert.Equal(t, testData, string(content), "Copied content doesn't match")

	// Verify SSE-C headers in response
	assert.Equal(t, "AES256", aws.ToString(getResp.SSECustomerAlgorithm))
	assert.Equal(t, testSSEKeyMD5, aws.ToString(getResp.SSECustomerKeyMD5))
}

func TestSSECCopyLargeObject(t *testing.T) {
	client := getS3Client(t)
	bucketName := createTestBucket(t, client)

	// Create larger test data (100KB)
	largeData := strings.Repeat("SeaweedFS SSE-C large object copy test data! ", 2000)
	sourceKey := "test-large-source"

	// Upload large source object with SSE-C
	_, err := client.PutObject(context.TODO(), &s3.PutObjectInput{
		Bucket:               aws.String(bucketName),
		Key:                  aws.String(sourceKey),
		Body:                 strings.NewReader(largeData),
		SSECustomerAlgorithm: aws.String("AES256"),
		SSECustomerKey:       aws.String(testSSEKey),
		SSECustomerKeyMD5:    aws.String(testSSEKeyMD5),
	})
	require.NoError(t, err, "Failed to upload large source object")

	// Test both fast and slow copy paths
	testCases := []struct {
		name     string
		destKey  string
		destMD5  string
		fastPath bool
	}{
		{"same_key_large", testSSEKey, testSSEKeyMD5, true},
		{"different_key_large", wrongSSEKey, wrongSSEKeyMD5, false},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			destKey := "test-large-dest-" + tc.name

			// Copy with potentially different key
			_, err := client.CopyObject(context.TODO(), &s3.CopyObjectInput{
				Bucket:                         aws.String(bucketName),
				Key:                            aws.String(destKey),
				CopySource:                     aws.String(bucketName + "/" + sourceKey),
				CopySourceSSECustomerAlgorithm: aws.String("AES256"),
				CopySourceSSECustomerKey:       aws.String(testSSEKey),
				CopySourceSSECustomerKeyMD5:    aws.String(testSSEKeyMD5),
				SSECustomerAlgorithm:           aws.String("AES256"),
				SSECustomerKey:                 aws.String(tc.destKey),
				SSECustomerKeyMD5:              aws.String(tc.destMD5),
			})
			require.NoError(t, err, "Failed to copy large object")

			// Verify the copied object
			getResp, err := client.GetObject(context.TODO(), &s3.GetObjectInput{
				Bucket:               aws.String(bucketName),
				Key:                  aws.String(destKey),
				SSECustomerAlgorithm: aws.String("AES256"),
				SSECustomerKey:       aws.String(tc.destKey),
				SSECustomerKeyMD5:    aws.String(tc.destMD5),
			})
			require.NoError(t, err, "Failed to read copied large object")

			content := make([]byte, len(largeData))
			n, err := getResp.Body.Read(content)
			require.NoError(t, err, "Failed to read content")
			getResp.Body.Close()

			assert.Equal(t, len(largeData), n, "Content length mismatch")
			assert.Equal(t, largeData, string(content), "Large object content doesn't match")
		})
	}
}

func TestSSECCopyErrorScenarios(t *testing.T) {
	client := getS3Client(t)
	bucketName := createTestBucket(t, client)

	testData := "Test data for error scenarios"
	sourceKey := "test-error-source"

	// Upload source object with SSE-C
	_, err := client.PutObject(context.TODO(), &s3.PutObjectInput{
		Bucket:               aws.String(bucketName),
		Key:                  aws.String(sourceKey),
		Body:                 strings.NewReader(testData),
		SSECustomerAlgorithm: aws.String("AES256"),
		SSECustomerKey:       aws.String(testSSEKey),
		SSECustomerKeyMD5:    aws.String(testSSEKeyMD5),
	})
	require.NoError(t, err, "Failed to upload source object")

	errorCases := []struct {
		name          string
		copySourceKey string
		copySourceMD5 string
		destKey       string
		destMD5       string
		expectError   bool
		description   string
	}{
		{
			name:          "missing_source_key",
			copySourceKey: "",
			copySourceMD5: "",
			destKey:       testSSEKey,
			destMD5:       testSSEKeyMD5,
			expectError:   true,
			description:   "Missing copy source key for encrypted object",
		},
		{
			name:          "wrong_source_key_md5",
			copySourceKey: testSSEKey,
			copySourceMD5: wrongSSEKeyMD5,
			destKey:       testSSEKey,
			destMD5:       testSSEKeyMD5,
			expectError:   true,
			description:   "Wrong MD5 for copy source key",
		},
	}

	for _, tc := range errorCases {
		t.Run(tc.name, func(t *testing.T) {
			destKey := "test-error-dest-" + tc.name

			copyInput := &s3.CopyObjectInput{
				Bucket:     aws.String(bucketName),
				Key:        aws.String(destKey),
				CopySource: aws.String(bucketName + "/" + sourceKey),
			}

			// Add copy source headers if provided
			if tc.copySourceKey != "" {
				copyInput.CopySourceSSECustomerAlgorithm = aws.String("AES256")
				copyInput.CopySourceSSECustomerKey = aws.String(tc.copySourceKey)
				copyInput.CopySourceSSECustomerKeyMD5 = aws.String(tc.copySourceMD5)
			}

			// Add destination headers if provided
			if tc.destKey != "" {
				copyInput.SSECustomerAlgorithm = aws.String("AES256")
				copyInput.SSECustomerKey = aws.String(tc.destKey)
				copyInput.SSECustomerKeyMD5 = aws.String(tc.destMD5)
			}

			_, err := client.CopyObject(context.TODO(), copyInput)

			if tc.expectError {
				assert.Error(t, err, "Expected error for %s", tc.description)
			} else {
				assert.NoError(t, err, "Unexpected error for %s: %v", tc.description, err)
			}
		})
	}
}
