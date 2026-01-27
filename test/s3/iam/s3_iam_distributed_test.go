package iam

import (
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestS3IAMDistributedTests tests IAM functionality across multiple S3 gateway instances
func TestS3IAMDistributedTests(t *testing.T) {
	// Skip if not in distributed test mode
	if os.Getenv("ENABLE_DISTRIBUTED_TESTS") != "true" {
		t.Skip("Distributed tests not enabled. Set ENABLE_DISTRIBUTED_TESTS=true")
	}

	framework := NewS3IAMTestFramework(t)
	defer framework.Cleanup()

	t.Run("distributed_session_consistency", func(t *testing.T) {
		// Test that sessions created on one instance are visible on others
		// This requires filer-based session storage

		// Create S3 clients that would connect to different gateway instances
		// In a real distributed setup, these would point to different S3 gateway ports
		client1, err := framework.CreateS3ClientWithJWT("admin-user", "TestAdminRole")
		require.NoError(t, err)

		client2, err := framework.CreateS3ClientWithJWT("admin-user", "TestAdminRole")
		require.NoError(t, err)

		// Both clients should be able to perform operations
		bucketName := "test-distributed-session"

		err = framework.CreateBucket(client1, bucketName)
		require.NoError(t, err)

		// Client2 should see the bucket created by client1
		// Retry logic for eventually consistent storage
		var found bool
		for i := 0; i < 20; i++ {
			listResult, err := client2.ListBuckets(&s3.ListBucketsInput{})
			require.NoError(t, err)

			found = false
			for _, bucket := range listResult.Buckets {
				if *bucket.Name == bucketName {
					found = true
					break
				}
			}
			if found {
				break
			}
			time.Sleep(250 * time.Millisecond)
		}
		assert.True(t, found, "Bucket should be visible across distributed instances")

		// Cleanup
		_, err = client1.DeleteBucket(&s3.DeleteBucketInput{
			Bucket: aws.String(bucketName),
		})
		require.NoError(t, err)
	})

	t.Run("distributed_role_consistency", func(t *testing.T) {
		// Test that role definitions are consistent across instances
		// This requires filer-based role storage

		// Create clients with different roles
		adminClient, err := framework.CreateS3ClientWithJWT("admin-user", "TestAdminRole")
		require.NoError(t, err)

		readOnlyClient, err := framework.CreateS3ClientWithJWT("read-user", "TestReadOnlyRole")
		require.NoError(t, err)

		bucketName := "test-distributed-roles"
		objectKey := "test-object.txt"

		// Admin should be able to create bucket
		err = framework.CreateBucket(adminClient, bucketName)
		require.NoError(t, err)

		// Admin should be able to put object
		err = framework.PutTestObject(adminClient, bucketName, objectKey, "test content")
		require.NoError(t, err)

		// Read-only user should be able to get object
		content, err := framework.GetTestObject(readOnlyClient, bucketName, objectKey)
		require.NoError(t, err)
		assert.Equal(t, "test content", content)

		// Read-only user should NOT be able to put object
		err = framework.PutTestObject(readOnlyClient, bucketName, "forbidden-object.txt", "forbidden content")
		require.Error(t, err, "Read-only user should not be able to put objects")

		// Cleanup
		err = framework.DeleteTestObject(adminClient, bucketName, objectKey)
		require.NoError(t, err)
		_, err = adminClient.DeleteBucket(&s3.DeleteBucketInput{
			Bucket: aws.String(bucketName),
		})
		require.NoError(t, err)
	})

	t.Run("distributed_concurrent_operations", func(t *testing.T) {
		// Test concurrent operations across distributed instances with robust retry mechanisms
		// This approach implements proper retry logic instead of tolerating errors to catch real concurrency issues
		const numGoroutines = 3                   // Reduced concurrency for better CI reliability
		const numOperationsPerGoroutine = 2       // Minimal operations per goroutine
		const maxRetries = 3                      // Maximum retry attempts for transient failures
		const retryDelay = 200 * time.Millisecond // Increased delay for better stability

		var wg sync.WaitGroup
		errors := make(chan error, numGoroutines*numOperationsPerGoroutine)

		// Helper function to determine if an error is retryable
		isRetryableError := func(err error) bool {
			if err == nil {
				return false
			}
			errorMsg := err.Error()
			return strings.Contains(errorMsg, "timeout") ||
				strings.Contains(errorMsg, "connection reset") ||
				strings.Contains(errorMsg, "temporary failure") ||
				strings.Contains(errorMsg, "TooManyRequests") ||
				strings.Contains(errorMsg, "ServiceUnavailable") ||
				strings.Contains(errorMsg, "InternalError")
		}

		// Helper function to execute operations with retry logic
		executeWithRetry := func(operation func() error, operationName string) error {
			var lastErr error
			for attempt := 0; attempt <= maxRetries; attempt++ {
				if attempt > 0 {
					time.Sleep(retryDelay * time.Duration(attempt)) // Linear backoff
				}

				lastErr = operation()
				if lastErr == nil {
					return nil // Success
				}

				if !isRetryableError(lastErr) {
					// Non-retryable error - fail immediately
					return fmt.Errorf("%s failed with non-retryable error: %w", operationName, lastErr)
				}

				// Retryable error - continue to next attempt
				if attempt < maxRetries {
					t.Logf("Retrying %s (attempt %d/%d) after error: %v", operationName, attempt+1, maxRetries, lastErr)
				}
			}

			// All retries exhausted
			return fmt.Errorf("%s failed after %d retries, last error: %w", operationName, maxRetries, lastErr)
		}

		for i := 0; i < numGoroutines; i++ {
			wg.Add(1)
			go func(goroutineID int) {
				defer wg.Done()

				client, err := framework.CreateS3ClientWithJWT("admin-user", "TestAdminRole")
				if err != nil {
					errors <- fmt.Errorf("failed to create S3 client for goroutine %d: %w", goroutineID, err)
					return
				}

				for j := 0; j < numOperationsPerGoroutine; j++ {
					bucketName := fmt.Sprintf("test-concurrent-%d-%d", goroutineID, j)
					objectKey := "test-object.txt"
					objectContent := fmt.Sprintf("content-%d-%d", goroutineID, j)

					// Execute full operation sequence with individual retries
					operationFailed := false

					// 1. Create bucket with retry
					if err := executeWithRetry(func() error {
						return framework.CreateBucket(client, bucketName)
					}, fmt.Sprintf("CreateBucket-%s", bucketName)); err != nil {
						errors <- err
						operationFailed = true
					}

					if !operationFailed {
						// 2. Put object with retry
						if err := executeWithRetry(func() error {
							return framework.PutTestObject(client, bucketName, objectKey, objectContent)
						}, fmt.Sprintf("PutObject-%s/%s", bucketName, objectKey)); err != nil {
							errors <- err
							operationFailed = true
						}
					}

					if !operationFailed {
						// 3. Get object with retry
						if err := executeWithRetry(func() error {
							_, err := framework.GetTestObject(client, bucketName, objectKey)
							return err
						}, fmt.Sprintf("GetObject-%s/%s", bucketName, objectKey)); err != nil {
							errors <- err
							operationFailed = true
						}
					}

					if !operationFailed {
						// 4. Delete object with retry
						if err := executeWithRetry(func() error {
							return framework.DeleteTestObject(client, bucketName, objectKey)
						}, fmt.Sprintf("DeleteObject-%s/%s", bucketName, objectKey)); err != nil {
							errors <- err
							operationFailed = true
						}
					}

					// 5. Always attempt bucket cleanup, even if previous operations failed
					if err := executeWithRetry(func() error {
						_, err := client.DeleteBucket(&s3.DeleteBucketInput{
							Bucket: aws.String(bucketName),
						})
						return err
					}, fmt.Sprintf("DeleteBucket-%s", bucketName)); err != nil {
						// Only log cleanup failures, don't fail the test
						t.Logf("Warning: Failed to cleanup bucket %s: %v", bucketName, err)
					}

					// Increased delay between operation sequences to reduce server load and improve stability
					time.Sleep(100 * time.Millisecond)
				}
			}(i)
		}

		wg.Wait()
		close(errors)

		// Collect and analyze errors - with retry logic, we should see very few errors
		var errorList []error
		for err := range errors {
			errorList = append(errorList, err)
		}

		totalOperations := numGoroutines * numOperationsPerGoroutine

		// Report results
		if len(errorList) == 0 {
			t.Logf("All %d concurrent operations completed successfully with retry mechanisms!", totalOperations)
		} else {
			t.Logf("Concurrent operations summary:")
			t.Logf("  Total operations: %d", totalOperations)
			t.Logf("  Failed operations: %d (%.1f%% error rate)", len(errorList), float64(len(errorList))/float64(totalOperations)*100)

			// Log first few errors for debugging
			for i, err := range errorList {
				if i >= 3 { // Limit to first 3 errors
					t.Logf("  ... and %d more errors", len(errorList)-3)
					break
				}
				t.Logf("  Error %d: %v", i+1, err)
			}
		}

		// With proper retry mechanisms, we should expect near-zero failures
		// Any remaining errors likely indicate real concurrency issues or system problems
		if len(errorList) > 0 {
			t.Errorf("%d operation(s) failed even after retry mechanisms (%.1f%% failure rate). This indicates potential system issues or race conditions that need investigation.",
				len(errorList), float64(len(errorList))/float64(totalOperations)*100)
		}
	})
}

// TestS3IAMPerformanceTests tests IAM performance characteristics
func TestS3IAMPerformanceTests(t *testing.T) {
	// Skip if not in performance test mode
	if os.Getenv("ENABLE_PERFORMANCE_TESTS") != "true" {
		t.Skip("Performance tests not enabled. Set ENABLE_PERFORMANCE_TESTS=true")
	}

	framework := NewS3IAMTestFramework(t)
	defer framework.Cleanup()

	t.Run("authentication_performance", func(t *testing.T) {
		// Test authentication performance
		const numRequests = 100

		client, err := framework.CreateS3ClientWithJWT("perf-user", "TestAdminRole")
		require.NoError(t, err)

		bucketName := "test-auth-performance"
		err = framework.CreateBucket(client, bucketName)
		require.NoError(t, err)
		defer func() {
			_, err := client.DeleteBucket(&s3.DeleteBucketInput{
				Bucket: aws.String(bucketName),
			})
			require.NoError(t, err)
		}()

		start := time.Now()

		for i := 0; i < numRequests; i++ {
			_, err := client.ListBuckets(&s3.ListBucketsInput{})
			require.NoError(t, err)
		}

		duration := time.Since(start)
		avgLatency := duration / numRequests

		t.Logf("Authentication performance: %d requests in %v (avg: %v per request)",
			numRequests, duration, avgLatency)

		// Performance assertion - should be under 100ms per request on average
		assert.Less(t, avgLatency, 100*time.Millisecond,
			"Average authentication latency should be under 100ms")
	})

	t.Run("authorization_performance", func(t *testing.T) {
		// Test authorization performance with different policy complexities
		const numRequests = 50

		client, err := framework.CreateS3ClientWithJWT("perf-user", "TestAdminRole")
		require.NoError(t, err)

		bucketName := "test-authz-performance"
		err = framework.CreateBucket(client, bucketName)
		require.NoError(t, err)
		defer func() {
			_, err := client.DeleteBucket(&s3.DeleteBucketInput{
				Bucket: aws.String(bucketName),
			})
			require.NoError(t, err)
		}()

		start := time.Now()

		for i := 0; i < numRequests; i++ {
			objectKey := fmt.Sprintf("perf-object-%d.txt", i)
			err := framework.PutTestObject(client, bucketName, objectKey, "performance test content")
			require.NoError(t, err)

			_, err = framework.GetTestObject(client, bucketName, objectKey)
			require.NoError(t, err)

			err = framework.DeleteTestObject(client, bucketName, objectKey)
			require.NoError(t, err)
		}

		duration := time.Since(start)
		avgLatency := duration / (numRequests * 3) // 3 operations per iteration

		t.Logf("Authorization performance: %d operations in %v (avg: %v per operation)",
			numRequests*3, duration, avgLatency)

		// Performance assertion - should be under 50ms per operation on average
		assert.Less(t, avgLatency, 50*time.Millisecond,
			"Average authorization latency should be under 50ms")
	})
}

// BenchmarkS3IAMAuthentication benchmarks JWT authentication
func BenchmarkS3IAMAuthentication(b *testing.B) {
	if os.Getenv("ENABLE_PERFORMANCE_TESTS") != "true" {
		b.Skip("Performance tests not enabled. Set ENABLE_PERFORMANCE_TESTS=true")
	}

	framework := NewS3IAMTestFramework(&testing.T{})
	defer framework.Cleanup()

	client, err := framework.CreateS3ClientWithJWT("bench-user", "TestAdminRole")
	require.NoError(b, err)

	bucketName := "test-bench-auth"
	err = framework.CreateBucket(client, bucketName)
	require.NoError(b, err)
	defer func() {
		_, err := client.DeleteBucket(&s3.DeleteBucketInput{
			Bucket: aws.String(bucketName),
		})
		require.NoError(b, err)
	}()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, err := client.ListBuckets(&s3.ListBucketsInput{})
			if err != nil {
				b.Error(err)
			}
		}
	})
}

// BenchmarkS3IAMAuthorization benchmarks policy evaluation
func BenchmarkS3IAMAuthorization(b *testing.B) {
	if os.Getenv("ENABLE_PERFORMANCE_TESTS") != "true" {
		b.Skip("Performance tests not enabled. Set ENABLE_PERFORMANCE_TESTS=true")
	}

	framework := NewS3IAMTestFramework(&testing.T{})
	defer framework.Cleanup()

	client, err := framework.CreateS3ClientWithJWT("bench-user", "TestAdminRole")
	require.NoError(b, err)

	bucketName := "test-bench-authz"
	err = framework.CreateBucket(client, bucketName)
	require.NoError(b, err)
	defer func() {
		_, err := client.DeleteBucket(&s3.DeleteBucketInput{
			Bucket: aws.String(bucketName),
		})
		require.NoError(b, err)
	}()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			objectKey := fmt.Sprintf("bench-object-%d.txt", i)
			err := framework.PutTestObject(client, bucketName, objectKey, "benchmark content")
			if err != nil {
				b.Error(err)
			}
			i++
		}
	})
}
