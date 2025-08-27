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
		client1, err := framework.CreateS3ClientWithJWT("test-user", "TestAdminRole")
		require.NoError(t, err)

		client2, err := framework.CreateS3ClientWithJWT("test-user", "TestAdminRole")
		require.NoError(t, err)

		// Both clients should be able to perform operations
		bucketName := "test-distributed-session"

		err = framework.CreateBucket(client1, bucketName)
		require.NoError(t, err)

		// Client2 should see the bucket created by client1
		listResult, err := client2.ListBuckets(&s3.ListBucketsInput{})
		require.NoError(t, err)

		found := false
		for _, bucket := range listResult.Buckets {
			if *bucket.Name == bucketName {
				found = true
				break
			}
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

		readOnlyClient, err := framework.CreateS3ClientWithJWT("readonly-user", "TestReadOnlyRole")
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
		// Test concurrent operations across distributed instances
		// Reduced concurrency (3x2=6 total ops) for CI stability
		const numGoroutines = 3
		const numOperationsPerGoroutine = 2

		var wg sync.WaitGroup
		errors := make(chan error, numGoroutines*numOperationsPerGoroutine)

		for i := 0; i < numGoroutines; i++ {
			wg.Add(1)
			go func(goroutineID int) {
				defer wg.Done()

				client, err := framework.CreateS3ClientWithJWT(fmt.Sprintf("user-%d", goroutineID), "TestAdminRole")
				if err != nil {
					errors <- err
					return
				}

				for j := 0; j < numOperationsPerGoroutine; j++ {
					bucketName := fmt.Sprintf("test-concurrent-%d-%d", goroutineID, j)

					// Create bucket
					if err := framework.CreateBucket(client, bucketName); err != nil {
						errors <- err
						continue
					}
					// Small delay to reduce server load
					time.Sleep(100 * time.Millisecond)

					// Put object
					objectKey := "test-object.txt"
					if err := framework.PutTestObject(client, bucketName, objectKey, fmt.Sprintf("content-%d-%d", goroutineID, j)); err != nil {
						errors <- err
						continue
					}
					// Small delay to reduce server load
					time.Sleep(100 * time.Millisecond)

					// Get object
					if _, err := framework.GetTestObject(client, bucketName, objectKey); err != nil {
						errors <- err
						continue
					}
					// Small delay to reduce server load
					time.Sleep(100 * time.Millisecond)

					// Delete object
					if err := framework.DeleteTestObject(client, bucketName, objectKey); err != nil {
						errors <- err
						continue
					}
					// Small delay to reduce server load
					time.Sleep(100 * time.Millisecond)

					// Delete bucket
					if _, err := client.DeleteBucket(&s3.DeleteBucketInput{
						Bucket: aws.String(bucketName),
					}); err != nil {
						errors <- err
						continue
					}
					// Small delay to reduce server load
					time.Sleep(100 * time.Millisecond)
				}
			}(i)
		}

		wg.Wait()
		close(errors)

		// Analyze errors with categorization for better diagnostics
		var errorList []error
		var transientErrors []error
		var seriousErrors []error

		for err := range errors {
			errorList = append(errorList, err)
			errorMsg := err.Error()

			// Categorize errors: transient vs serious
			if strings.Contains(errorMsg, "timeout") ||
				strings.Contains(errorMsg, "connection reset") ||
				strings.Contains(errorMsg, "temporary failure") ||
				strings.Contains(errorMsg, "TooManyRequests") {
				transientErrors = append(transientErrors, err)
			} else {
				seriousErrors = append(seriousErrors, err)
			}
		}

		totalOperations := numGoroutines * numOperationsPerGoroutine
		errorRate := float64(len(errorList)) / float64(totalOperations)
		seriousErrorRate := float64(len(seriousErrors)) / float64(totalOperations)
		transientErrorRate := float64(len(transientErrors)) / float64(totalOperations)

		// Detailed error reporting
		if len(errorList) > 0 {
			t.Logf("Concurrent operations summary:")
			t.Logf("  Total operations: %d", totalOperations)
			t.Logf("  Failed operations: %d (%.1f%% error rate)", len(errorList), errorRate*100)
			t.Logf("  Serious errors: %d (%.1f%% rate)", len(seriousErrors), seriousErrorRate*100)
			t.Logf("  Transient errors: %d (%.1f%% rate)", len(transientErrors), transientErrorRate*100)

			if len(seriousErrors) > 0 {
				t.Logf("  First serious error: %v", seriousErrors[0])
			}
			if len(transientErrors) > 0 {
				t.Logf("  First transient error: %v", transientErrors[0])
			}
		}

		// STRICT CONCURRENCY TESTING: Use appropriate thresholds for the operation count
		// For totalOperations=6, error thresholds need to be adjusted to avoid unreachable conditions

		// Serious errors (race conditions, deadlocks) should be zero for reliable testing
		maxSeriousErrors := 0 // Allow 0 serious errors max - any serious error indicates system issues
		if len(seriousErrors) > maxSeriousErrors {
			t.Errorf("❌ %d serious error(s) detected, indicating potential concurrency bugs. First error: %v",
				len(seriousErrors), seriousErrors[0])
		}

		// For total errors, allow more flexibility with small operation counts
		maxTotalErrors := 2 // Allow 2 total errors max (33.3%) for small test size
		if len(errorList) > maxTotalErrors {
			t.Errorf("❌ Too many total errors: %d (%.1f%%) - max allowed: %d. This suggests system instability under concurrent load.",
				len(errorList), errorRate*100, maxTotalErrors)
		} else if len(errorList) > 0 {
			t.Logf("⚠️  Concurrent operations completed with %d errors (%.1f%%) - within acceptable range for testing", len(errorList), errorRate*100)
		} else {
			t.Logf("✅ All concurrent operations completed successfully - excellent concurrency handling!")
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
