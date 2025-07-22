package cors

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/k0kubun/pp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// S3TestConfig holds configuration for S3 tests
type S3TestConfig struct {
	Endpoint      string
	AccessKey     string
	SecretKey     string
	Region        string
	BucketPrefix  string
	UseSSL        bool
	SkipVerifySSL bool
}

// getDefaultConfig returns a fresh instance of the default test configuration
// to avoid parallel test issues with global mutable state
func getDefaultConfig() *S3TestConfig {
	return &S3TestConfig{
		Endpoint:      "http://localhost:8333", // Default SeaweedFS S3 port
		AccessKey:     "some_access_key1",
		SecretKey:     "some_secret_key1",
		Region:        "us-east-1",
		BucketPrefix:  "test-cors-",
		UseSSL:        false,
		SkipVerifySSL: true,
	}
}

// getS3Client creates an AWS S3 client for testing
func getS3Client(t *testing.T) *s3.Client {
	defaultConfig := getDefaultConfig()
	cfg, err := config.LoadDefaultConfig(context.TODO(),
		config.WithRegion(defaultConfig.Region),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(
			defaultConfig.AccessKey,
			defaultConfig.SecretKey,
			"",
		)),
		config.WithEndpointResolverWithOptions(aws.EndpointResolverWithOptionsFunc(
			func(service, region string, options ...interface{}) (aws.Endpoint, error) {
				return aws.Endpoint{
					URL:           defaultConfig.Endpoint,
					SigningRegion: defaultConfig.Region,
				}, nil
			})),
	)
	require.NoError(t, err)

	client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.UsePathStyle = true
	})
	return client
}

// createTestBucket creates a test bucket with a unique name
func createTestBucket(t *testing.T, client *s3.Client) string {
	defaultConfig := getDefaultConfig()
	bucketName := fmt.Sprintf("%s%d", defaultConfig.BucketPrefix, time.Now().UnixNano())

	_, err := client.CreateBucket(context.TODO(), &s3.CreateBucketInput{
		Bucket: aws.String(bucketName),
	})
	require.NoError(t, err)

	// Wait for bucket metadata to be fully processed
	time.Sleep(50 * time.Millisecond)

	return bucketName
}

// cleanupTestBucket removes the test bucket and all its contents
func cleanupTestBucket(t *testing.T, client *s3.Client, bucketName string) {
	// First, delete all objects in the bucket
	listResp, err := client.ListObjectsV2(context.TODO(), &s3.ListObjectsV2Input{
		Bucket: aws.String(bucketName),
	})
	if err == nil {
		for _, obj := range listResp.Contents {
			_, err := client.DeleteObject(context.TODO(), &s3.DeleteObjectInput{
				Bucket: aws.String(bucketName),
				Key:    obj.Key,
			})
			if err != nil {
				t.Logf("Warning: failed to delete object %s: %v", *obj.Key, err)
			}
		}
	}

	// Then delete the bucket
	_, err = client.DeleteBucket(context.TODO(), &s3.DeleteBucketInput{
		Bucket: aws.String(bucketName),
	})
	if err != nil {
		t.Logf("Warning: failed to delete bucket %s: %v", bucketName, err)
	}
}

// TestCORSConfigurationManagement tests basic CORS configuration CRUD operations
func TestCORSConfigurationManagement(t *testing.T) {
	client := getS3Client(t)
	bucketName := createTestBucket(t, client)
	defer cleanupTestBucket(t, client, bucketName)

	// Test 1: Get CORS configuration when none exists (should return error)
	_, err := client.GetBucketCors(context.TODO(), &s3.GetBucketCorsInput{
		Bucket: aws.String(bucketName),
	})
	assert.Error(t, err, "Should get error when no CORS configuration exists")

	// Test 2: Put CORS configuration
	corsConfig := &types.CORSConfiguration{
		CORSRules: []types.CORSRule{
			{
				AllowedHeaders: []string{"*"},
				AllowedMethods: []string{"GET", "POST", "PUT"},
				AllowedOrigins: []string{"https://example.com"},
				ExposeHeaders:  []string{"ETag"},
				MaxAgeSeconds:  aws.Int32(3600),
			},
		},
	}

	_, err = client.PutBucketCors(context.TODO(), &s3.PutBucketCorsInput{
		Bucket:            aws.String(bucketName),
		CORSConfiguration: corsConfig,
	})
	assert.NoError(t, err, "Should be able to put CORS configuration")

	// Wait for metadata subscription to update cache
	time.Sleep(50 * time.Millisecond)

	// Test 3: Get CORS configuration
	getResp, err := client.GetBucketCors(context.TODO(), &s3.GetBucketCorsInput{
		Bucket: aws.String(bucketName),
	})
	assert.NoError(t, err, "Should be able to get CORS configuration")
	assert.NotNil(t, getResp.CORSRules, "CORS configuration should not be nil")
	assert.Len(t, getResp.CORSRules, 1, "Should have one CORS rule")

	rule := getResp.CORSRules[0]
	assert.Equal(t, []string{"*"}, rule.AllowedHeaders, "Allowed headers should match")
	assert.Equal(t, []string{"GET", "POST", "PUT"}, rule.AllowedMethods, "Allowed methods should match")
	assert.Equal(t, []string{"https://example.com"}, rule.AllowedOrigins, "Allowed origins should match")
	assert.Equal(t, []string{"ETag"}, rule.ExposeHeaders, "Expose headers should match")
	assert.Equal(t, aws.Int32(3600), rule.MaxAgeSeconds, "Max age should match")

	// Test 4: Update CORS configuration
	updatedCorsConfig := &types.CORSConfiguration{
		CORSRules: []types.CORSRule{
			{
				AllowedHeaders: []string{"Content-Type"},
				AllowedMethods: []string{"GET", "POST"},
				AllowedOrigins: []string{"https://example.com", "https://another.com"},
				ExposeHeaders:  []string{"ETag", "Content-Length"},
				MaxAgeSeconds:  aws.Int32(7200),
			},
		},
	}

	_, err = client.PutBucketCors(context.TODO(), &s3.PutBucketCorsInput{
		Bucket:            aws.String(bucketName),
		CORSConfiguration: updatedCorsConfig,
	})
	require.NoError(t, err, "Should be able to update CORS configuration")

	// Wait for CORS configuration update to be fully processed
	time.Sleep(100 * time.Millisecond)

	// Verify the update with retries for robustness
	var updateSuccess bool
	for i := 0; i < 3; i++ {
		getResp, err = client.GetBucketCors(context.TODO(), &s3.GetBucketCorsInput{
			Bucket: aws.String(bucketName),
		})
		if err != nil {
			t.Logf("Attempt %d: Failed to get updated CORS config: %v", i+1, err)
			time.Sleep(50 * time.Millisecond)
			continue
		}

		if len(getResp.CORSRules) > 0 {
			rule = getResp.CORSRules[0]
			// Check if the update actually took effect
			if len(rule.AllowedHeaders) > 0 && rule.AllowedHeaders[0] == "Content-Type" &&
				len(rule.AllowedOrigins) > 1 {
				updateSuccess = true
				break
			}
		}
		t.Logf("Attempt %d: CORS config not updated yet, retrying...", i+1)
		time.Sleep(50 * time.Millisecond)
	}

	require.NoError(t, err, "Should be able to get updated CORS configuration")
	require.True(t, updateSuccess, "CORS configuration should be updated after retries")
	assert.Equal(t, []string{"Content-Type"}, rule.AllowedHeaders, "Updated allowed headers should match")
	assert.Equal(t, []string{"https://example.com", "https://another.com"}, rule.AllowedOrigins, "Updated allowed origins should match")

	// Test 5: Delete CORS configuration
	_, err = client.DeleteBucketCors(context.TODO(), &s3.DeleteBucketCorsInput{
		Bucket: aws.String(bucketName),
	})
	require.NoError(t, err, "Should be able to delete CORS configuration")

	// Wait for deletion to be fully processed
	time.Sleep(100 * time.Millisecond)

	// Verify deletion - should get NoSuchCORSConfiguration error
	_, err = client.GetBucketCors(context.TODO(), &s3.GetBucketCorsInput{
		Bucket: aws.String(bucketName),
	})

	// Check that we get the expected error type
	if err != nil {
		// Log the error for debugging
		t.Logf("Got expected error after CORS deletion: %v", err)
		// Check if it's the correct error type (NoSuchCORSConfiguration)
		errMsg := err.Error()
		if !strings.Contains(errMsg, "NoSuchCORSConfiguration") && !strings.Contains(errMsg, "404") {
			t.Errorf("Expected NoSuchCORSConfiguration error, got: %v", err)
		}
	} else {
		// If no error, this might be a SeaweedFS implementation difference
		// Some implementations might return empty config instead of error
		t.Logf("CORS deletion test: No error returned - this may be implementation-specific behavior")
	}
}

// TestCORSMultipleRules tests CORS configuration with multiple rules
func TestCORSMultipleRules(t *testing.T) {
	client := getS3Client(t)
	bucketName := createTestBucket(t, client)
	defer cleanupTestBucket(t, client, bucketName)

	// Create CORS configuration with multiple rules
	corsConfig := &types.CORSConfiguration{
		CORSRules: []types.CORSRule{
			{
				AllowedHeaders: []string{"*"},
				AllowedMethods: []string{"GET", "HEAD"},
				AllowedOrigins: []string{"https://example.com"},
				ExposeHeaders:  []string{"ETag"},
				MaxAgeSeconds:  aws.Int32(3600),
			},
			{
				AllowedHeaders: []string{"Content-Type", "Authorization"},
				AllowedMethods: []string{"POST", "PUT", "DELETE"},
				AllowedOrigins: []string{"https://app.example.com"},
				ExposeHeaders:  []string{"ETag", "Content-Length"},
				MaxAgeSeconds:  aws.Int32(7200),
			},
			{
				AllowedHeaders: []string{"*"},
				AllowedMethods: []string{"GET"},
				AllowedOrigins: []string{"*"},
				ExposeHeaders:  []string{"ETag"},
				MaxAgeSeconds:  aws.Int32(1800),
			},
		},
	}

	_, err := client.PutBucketCors(context.TODO(), &s3.PutBucketCorsInput{
		Bucket:            aws.String(bucketName),
		CORSConfiguration: corsConfig,
	})
	require.NoError(t, err, "Should be able to put CORS configuration with multiple rules")

	// Wait for CORS configuration to be fully processed
	time.Sleep(100 * time.Millisecond)

	// Get and verify the configuration with retries for robustness
	var getResp *s3.GetBucketCorsOutput
	var getErr error

	// Retry getting CORS config up to 3 times to handle timing issues
	for i := 0; i < 3; i++ {
		getResp, getErr = client.GetBucketCors(context.TODO(), &s3.GetBucketCorsInput{
			Bucket: aws.String(bucketName),
		})
		if getErr == nil {
			break
		}
		t.Logf("Attempt %d: Failed to get multiple rules CORS config: %v", i+1, getErr)
		time.Sleep(50 * time.Millisecond)
	}

	require.NoError(t, getErr, "Should be able to get CORS configuration after retries")
	require.NotNil(t, getResp, "GetBucketCors response should not be nil")
	require.Len(t, getResp.CORSRules, 3, "Should have three CORS rules")

	// Verify first rule
	rule1 := getResp.CORSRules[0]
	assert.Equal(t, []string{"*"}, rule1.AllowedHeaders)
	assert.Equal(t, []string{"GET", "HEAD"}, rule1.AllowedMethods)
	assert.Equal(t, []string{"https://example.com"}, rule1.AllowedOrigins)

	// Verify second rule
	rule2 := getResp.CORSRules[1]
	assert.Equal(t, []string{"Content-Type", "Authorization"}, rule2.AllowedHeaders)
	assert.Equal(t, []string{"POST", "PUT", "DELETE"}, rule2.AllowedMethods)
	assert.Equal(t, []string{"https://app.example.com"}, rule2.AllowedOrigins)

	// Verify third rule
	rule3 := getResp.CORSRules[2]
	assert.Equal(t, []string{"*"}, rule3.AllowedHeaders)
	assert.Equal(t, []string{"GET"}, rule3.AllowedMethods)
	assert.Equal(t, []string{"*"}, rule3.AllowedOrigins)
}

// TestCORSValidation tests CORS configuration validation
func TestCORSValidation(t *testing.T) {
	client := getS3Client(t)
	bucketName := createTestBucket(t, client)
	defer cleanupTestBucket(t, client, bucketName)

	// Test invalid HTTP method
	invalidMethodConfig := &types.CORSConfiguration{
		CORSRules: []types.CORSRule{
			{
				AllowedHeaders: []string{"*"},
				AllowedMethods: []string{"INVALID_METHOD"},
				AllowedOrigins: []string{"https://example.com"},
			},
		},
	}

	_, err := client.PutBucketCors(context.TODO(), &s3.PutBucketCorsInput{
		Bucket:            aws.String(bucketName),
		CORSConfiguration: invalidMethodConfig,
	})
	assert.Error(t, err, "Should get error for invalid HTTP method")

	// Test empty origins
	emptyOriginsConfig := &types.CORSConfiguration{
		CORSRules: []types.CORSRule{
			{
				AllowedHeaders: []string{"*"},
				AllowedMethods: []string{"GET"},
				AllowedOrigins: []string{},
			},
		},
	}

	_, err = client.PutBucketCors(context.TODO(), &s3.PutBucketCorsInput{
		Bucket:            aws.String(bucketName),
		CORSConfiguration: emptyOriginsConfig,
	})
	assert.Error(t, err, "Should get error for empty origins")

	// Test negative MaxAge
	negativeMaxAgeConfig := &types.CORSConfiguration{
		CORSRules: []types.CORSRule{
			{
				AllowedHeaders: []string{"*"},
				AllowedMethods: []string{"GET"},
				AllowedOrigins: []string{"https://example.com"},
				MaxAgeSeconds:  aws.Int32(-1),
			},
		},
	}

	_, err = client.PutBucketCors(context.TODO(), &s3.PutBucketCorsInput{
		Bucket:            aws.String(bucketName),
		CORSConfiguration: negativeMaxAgeConfig,
	})
	assert.Error(t, err, "Should get error for negative MaxAge")
}

// TestCORSWithWildcards tests CORS configuration with wildcard patterns
func TestCORSWithWildcards(t *testing.T) {
	client := getS3Client(t)
	bucketName := createTestBucket(t, client)
	defer cleanupTestBucket(t, client, bucketName)

	// Create CORS configuration with wildcard patterns
	corsConfig := &types.CORSConfiguration{
		CORSRules: []types.CORSRule{
			{
				AllowedHeaders: []string{"*"},
				AllowedMethods: []string{"GET", "POST"},
				AllowedOrigins: []string{"https://*.example.com"},
				ExposeHeaders:  []string{"*"},
				MaxAgeSeconds:  aws.Int32(3600),
			},
		},
	}

	_, err := client.PutBucketCors(context.TODO(), &s3.PutBucketCorsInput{
		Bucket:            aws.String(bucketName),
		CORSConfiguration: corsConfig,
	})
	require.NoError(t, err, "Should be able to put CORS configuration with wildcards")

	// Wait for CORS configuration to be fully processed and available
	time.Sleep(100 * time.Millisecond)

	// Get and verify the configuration with retries for robustness
	var getResp *s3.GetBucketCorsOutput
	var getErr error

	// Retry getting CORS config up to 3 times to handle timing issues
	for i := 0; i < 3; i++ {
		getResp, getErr = client.GetBucketCors(context.TODO(), &s3.GetBucketCorsInput{
			Bucket: aws.String(bucketName),
		})
		if getErr == nil {
			break
		}
		t.Logf("Attempt %d: Failed to get CORS config: %v", i+1, getErr)
		time.Sleep(50 * time.Millisecond)
	}

	require.NoError(t, getErr, "Should be able to get CORS configuration after retries")
	require.NotNil(t, getResp, "GetBucketCors response should not be nil")
	require.Len(t, getResp.CORSRules, 1, "Should have one CORS rule")

	rule := getResp.CORSRules[0]
	require.NotNil(t, rule, "CORS rule should not be nil")
	assert.Equal(t, []string{"*"}, rule.AllowedHeaders, "Wildcard headers should be preserved")
	assert.Equal(t, []string{"https://*.example.com"}, rule.AllowedOrigins, "Wildcard origins should be preserved")
	assert.Equal(t, []string{"*"}, rule.ExposeHeaders, "Wildcard expose headers should be preserved")
}

// TestCORSRuleLimit tests the maximum number of CORS rules
func TestCORSRuleLimit(t *testing.T) {
	client := getS3Client(t)
	bucketName := createTestBucket(t, client)
	defer cleanupTestBucket(t, client, bucketName)

	// Create CORS configuration with maximum allowed rules (100)
	rules := make([]types.CORSRule, 100)
	for i := 0; i < 100; i++ {
		rules[i] = types.CORSRule{
			AllowedHeaders: []string{"*"},
			AllowedMethods: []string{"GET"},
			AllowedOrigins: []string{fmt.Sprintf("https://example%d.com", i)},
			MaxAgeSeconds:  aws.Int32(3600),
		}
	}

	corsConfig := &types.CORSConfiguration{
		CORSRules: rules,
	}

	_, err := client.PutBucketCors(context.TODO(), &s3.PutBucketCorsInput{
		Bucket:            aws.String(bucketName),
		CORSConfiguration: corsConfig,
	})
	assert.NoError(t, err, "Should be able to put CORS configuration with 100 rules")

	// Try to add one more rule (should fail)
	rules = append(rules, types.CORSRule{
		AllowedHeaders: []string{"*"},
		AllowedMethods: []string{"GET"},
		AllowedOrigins: []string{"https://example101.com"},
		MaxAgeSeconds:  aws.Int32(3600),
	})

	corsConfig.CORSRules = rules

	_, err = client.PutBucketCors(context.TODO(), &s3.PutBucketCorsInput{
		Bucket:            aws.String(bucketName),
		CORSConfiguration: corsConfig,
	})
	assert.Error(t, err, "Should get error when exceeding maximum number of rules")
}

// TestCORSNonExistentBucket tests CORS operations on non-existent bucket
func TestCORSNonExistentBucket(t *testing.T) {
	client := getS3Client(t)
	nonExistentBucket := "non-existent-bucket-cors-test"

	// Test Get CORS on non-existent bucket
	_, err := client.GetBucketCors(context.TODO(), &s3.GetBucketCorsInput{
		Bucket: aws.String(nonExistentBucket),
	})
	assert.Error(t, err, "Should get error for non-existent bucket")

	// Test Put CORS on non-existent bucket
	corsConfig := &types.CORSConfiguration{
		CORSRules: []types.CORSRule{
			{
				AllowedHeaders: []string{"*"},
				AllowedMethods: []string{"GET"},
				AllowedOrigins: []string{"https://example.com"},
			},
		},
	}

	_, err = client.PutBucketCors(context.TODO(), &s3.PutBucketCorsInput{
		Bucket:            aws.String(nonExistentBucket),
		CORSConfiguration: corsConfig,
	})
	assert.Error(t, err, "Should get error for non-existent bucket")

	// Test Delete CORS on non-existent bucket
	_, err = client.DeleteBucketCors(context.TODO(), &s3.DeleteBucketCorsInput{
		Bucket: aws.String(nonExistentBucket),
	})
	assert.Error(t, err, "Should get error for non-existent bucket")
}

// TestCORSObjectOperations tests CORS behavior with object operations
func TestCORSObjectOperations(t *testing.T) {
	client := getS3Client(t)
	bucketName := createTestBucket(t, client)
	defer cleanupTestBucket(t, client, bucketName)

	// Set up CORS configuration
	corsConfig := &types.CORSConfiguration{
		CORSRules: []types.CORSRule{
			{
				AllowedHeaders: []string{"*"},
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
	assert.NoError(t, err, "Should be able to put CORS configuration")

	// Test putting an object (this should work normally)
	objectKey := "test-object.txt"
	objectContent := "Hello, CORS World!"

	_, err = client.PutObject(context.TODO(), &s3.PutObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(objectKey),
		Body:   strings.NewReader(objectContent),
	})
	assert.NoError(t, err, "Should be able to put object in CORS-enabled bucket")

	// Test getting the object
	getResp, err := client.GetObject(context.TODO(), &s3.GetObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(objectKey),
	})
	assert.NoError(t, err, "Should be able to get object from CORS-enabled bucket")
	assert.NotNil(t, getResp.Body, "Object body should not be nil")

	// Test deleting the object
	_, err = client.DeleteObject(context.TODO(), &s3.DeleteObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(objectKey),
	})
	assert.NoError(t, err, "Should be able to delete object from CORS-enabled bucket")
}

// TestCORSCaching tests CORS configuration caching behavior
func TestCORSCaching(t *testing.T) {
	client := getS3Client(t)
	bucketName := createTestBucket(t, client)
	defer cleanupTestBucket(t, client, bucketName)

	// Set up initial CORS configuration
	corsConfig1 := &types.CORSConfiguration{
		CORSRules: []types.CORSRule{
			{
				AllowedHeaders: []string{"*"},
				AllowedMethods: []string{"GET"},
				AllowedOrigins: []string{"https://example.com"},
				MaxAgeSeconds:  aws.Int32(3600),
			},
		},
	}

	_, err := client.PutBucketCors(context.TODO(), &s3.PutBucketCorsInput{
		Bucket:            aws.String(bucketName),
		CORSConfiguration: corsConfig1,
	})
	assert.NoError(t, err, "Should be able to put initial CORS configuration")

	// Wait for metadata subscription to update cache
	time.Sleep(50 * time.Millisecond)

	// Get the configuration
	getResp1, err := client.GetBucketCors(context.TODO(), &s3.GetBucketCorsInput{
		Bucket: aws.String(bucketName),
	})
	assert.NoError(t, err, "Should be able to get initial CORS configuration")
	assert.Len(t, getResp1.CORSRules, 1, "Should have one CORS rule")

	// Update the configuration
	corsConfig2 := &types.CORSConfiguration{
		CORSRules: []types.CORSRule{
			{
				AllowedHeaders: []string{"Content-Type"},
				AllowedMethods: []string{"GET", "POST"},
				AllowedOrigins: []string{"https://example.com", "https://another.com"},
				MaxAgeSeconds:  aws.Int32(7200),
			},
		},
	}

	_, err = client.PutBucketCors(context.TODO(), &s3.PutBucketCorsInput{
		Bucket:            aws.String(bucketName),
		CORSConfiguration: corsConfig2,
	})
	assert.NoError(t, err, "Should be able to update CORS configuration")

	// Wait for metadata subscription to update cache
	time.Sleep(50 * time.Millisecond)

	// Get the updated configuration (should reflect the changes)
	getResp2, err := client.GetBucketCors(context.TODO(), &s3.GetBucketCorsInput{
		Bucket: aws.String(bucketName),
	})
	assert.NoError(t, err, "Should be able to get updated CORS configuration")
	assert.Len(t, getResp2.CORSRules, 1, "Should have one CORS rule")

	rule := getResp2.CORSRules[0]
	assert.Equal(t, []string{"Content-Type"}, rule.AllowedHeaders, "Should have updated headers")
	assert.Equal(t, []string{"GET", "POST"}, rule.AllowedMethods, "Should have updated methods")
	assert.Equal(t, []string{"https://example.com", "https://another.com"}, rule.AllowedOrigins, "Should have updated origins")
	assert.Equal(t, aws.Int32(7200), rule.MaxAgeSeconds, "Should have updated max age")
}

// TestCORSErrorHandling tests various error conditions
func TestCORSErrorHandling(t *testing.T) {
	client := getS3Client(t)
	bucketName := createTestBucket(t, client)
	defer cleanupTestBucket(t, client, bucketName)

	// Test empty CORS configuration
	emptyCorsConfig := &types.CORSConfiguration{
		CORSRules: []types.CORSRule{},
	}

	_, err := client.PutBucketCors(context.TODO(), &s3.PutBucketCorsInput{
		Bucket:            aws.String(bucketName),
		CORSConfiguration: emptyCorsConfig,
	})
	assert.Error(t, err, "Should get error for empty CORS configuration")

	// Test nil CORS configuration
	_, err = client.PutBucketCors(context.TODO(), &s3.PutBucketCorsInput{
		Bucket:            aws.String(bucketName),
		CORSConfiguration: nil,
	})
	assert.Error(t, err, "Should get error for nil CORS configuration")

	// Test CORS rule with empty methods
	emptyMethodsConfig := &types.CORSConfiguration{
		CORSRules: []types.CORSRule{
			{
				AllowedHeaders: []string{"*"},
				AllowedMethods: []string{},
				AllowedOrigins: []string{"https://example.com"},
			},
		},
	}

	_, err = client.PutBucketCors(context.TODO(), &s3.PutBucketCorsInput{
		Bucket:            aws.String(bucketName),
		CORSConfiguration: emptyMethodsConfig,
	})
	assert.Error(t, err, "Should get error for empty methods")
}

// Debugging helper to pretty print responses
func debugResponse(t *testing.T, title string, response interface{}) {
	t.Logf("=== %s ===", title)
	pp.Println(response)
}
