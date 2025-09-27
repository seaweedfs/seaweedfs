package sse_test

import (
	"bytes"
	"context"
	"crypto/md5"
	"crypto/rand"
	"encoding/base64"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestSimpleSSECIntegration tests basic SSE-C with a fixed bucket name
func TestSimpleSSECIntegration(t *testing.T) {
	ctx := context.Background()

	// Create S3 client
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
	)
	require.NoError(t, err)

	client := s3.NewFromConfig(awsCfg, func(o *s3.Options) {
		o.UsePathStyle = true
	})

	bucketName := "test-debug-bucket"
	objectKey := fmt.Sprintf("test-object-prefixed-%d", time.Now().UnixNano())

	// Generate SSE-C key
	key := make([]byte, 32)
	rand.Read(key)
	keyB64 := base64.StdEncoding.EncodeToString(key)
	keyMD5Hash := md5.Sum(key)
	keyMD5 := base64.StdEncoding.EncodeToString(keyMD5Hash[:])

	testData := []byte("Hello, simple SSE-C integration test!")

	// Ensure bucket exists
	_, err = client.CreateBucket(ctx, &s3.CreateBucketInput{
		Bucket: aws.String(bucketName),
	})
	if err != nil {
		t.Logf("Bucket creation result: %v (might be OK if exists)", err)
	}

	// Wait a moment for bucket to be ready
	time.Sleep(1 * time.Second)

	t.Run("PUT with SSE-C", func(t *testing.T) {
		_, err := client.PutObject(ctx, &s3.PutObjectInput{
			Bucket:               aws.String(bucketName),
			Key:                  aws.String(objectKey),
			Body:                 bytes.NewReader(testData),
			SSECustomerAlgorithm: aws.String("AES256"),
			SSECustomerKey:       aws.String(keyB64),
			SSECustomerKeyMD5:    aws.String(keyMD5),
		})
		require.NoError(t, err, "Failed to upload SSE-C object")
		t.Log("SSE-C PUT succeeded!")
	})

	t.Run("GET with SSE-C", func(t *testing.T) {
		resp, err := client.GetObject(ctx, &s3.GetObjectInput{
			Bucket:               aws.String(bucketName),
			Key:                  aws.String(objectKey),
			SSECustomerAlgorithm: aws.String("AES256"),
			SSECustomerKey:       aws.String(keyB64),
			SSECustomerKeyMD5:    aws.String(keyMD5),
		})
		require.NoError(t, err, "Failed to retrieve SSE-C object")
		defer resp.Body.Close()

		retrievedData, err := io.ReadAll(resp.Body)
		require.NoError(t, err, "Failed to read retrieved data")
		assert.Equal(t, testData, retrievedData, "Retrieved data doesn't match original")

		// Verify SSE-C headers
		assert.Equal(t, "AES256", aws.ToString(resp.SSECustomerAlgorithm))
		assert.Equal(t, keyMD5, aws.ToString(resp.SSECustomerKeyMD5))

		t.Log("SSE-C GET succeeded and data matches!")
	})

	t.Run("GET without key should fail", func(t *testing.T) {
		_, err := client.GetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(objectKey),
		})
		assert.Error(t, err, "Should fail to retrieve SSE-C object without key")
		t.Log("GET without key correctly failed")
	})
}
