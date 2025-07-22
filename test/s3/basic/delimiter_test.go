package basic

import (
	"fmt"
	"math/rand"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestS3ListDelimiterWithDirectoryKeyObjects tests the specific scenario from
// test_bucket_list_delimiter_not_skip_special where directory key objects
// should be properly grouped into common prefixes when using delimiters
func TestS3ListDelimiterWithDirectoryKeyObjects(t *testing.T) {
	bucketName := fmt.Sprintf("test-delimiter-dir-key-%d", rand.Int31())

	// Create bucket
	_, err := svc.CreateBucket(&s3.CreateBucketInput{
		Bucket: aws.String(bucketName),
	})
	require.NoError(t, err)
	defer cleanupBucket(t, bucketName)

	// Create objects matching the failing test scenario:
	// ['0/'] + ['0/1000', '0/1001', '0/1002'] + ['1999', '1999#', '1999+', '2000']
	objects := []string{
		"0/",     // Directory key object
		"0/1000", // Objects under 0/ prefix
		"0/1001",
		"0/1002",
		"1999", // Objects without delimiter
		"1999#",
		"1999+",
		"2000",
	}

	// Create all objects
	for _, key := range objects {
		_, err := svc.PutObject(&s3.PutObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(key),
			Body:   strings.NewReader(fmt.Sprintf("content for %s", key)),
		})
		require.NoError(t, err, "Failed to create object %s", key)
	}

	// Test with delimiter='/'
	resp, err := svc.ListObjects(&s3.ListObjectsInput{
		Bucket:    aws.String(bucketName),
		Delimiter: aws.String("/"),
	})
	require.NoError(t, err)

	// Extract keys and prefixes
	var keys []string
	for _, content := range resp.Contents {
		keys = append(keys, *content.Key)
	}

	var prefixes []string
	for _, prefix := range resp.CommonPrefixes {
		prefixes = append(prefixes, *prefix.Prefix)
	}

	// Expected results:
	// Keys should be: ['1999', '1999#', '1999+', '2000'] (objects without delimiters)
	// Prefixes should be: ['0/'] (grouping '0/' and all '0/xxxx' objects)

	expectedKeys := []string{"1999", "1999#", "1999+", "2000"}
	expectedPrefixes := []string{"0/"}

	t.Logf("Actual keys: %v", keys)
	t.Logf("Actual prefixes: %v", prefixes)

	assert.ElementsMatch(t, expectedKeys, keys, "Keys should only include objects without delimiters")
	assert.ElementsMatch(t, expectedPrefixes, prefixes, "CommonPrefixes should group directory key object with other objects sharing prefix")

	// Additional validation
	assert.Equal(t, "/", *resp.Delimiter, "Delimiter should be set correctly")
	assert.Contains(t, prefixes, "0/", "Directory key object '0/' should be grouped into common prefix '0/'")
	assert.NotContains(t, keys, "0/", "Directory key object '0/' should NOT appear as individual key when delimiter is used")

	// Verify none of the '0/xxxx' objects appear as individual keys
	for _, key := range keys {
		assert.False(t, strings.HasPrefix(key, "0/"), "No object with '0/' prefix should appear as individual key, found: %s", key)
	}
}

// TestS3ListWithoutDelimiter tests that directory key objects appear as individual keys when no delimiter is used
func TestS3ListWithoutDelimiter(t *testing.T) {
	bucketName := fmt.Sprintf("test-no-delimiter-%d", rand.Int31())

	// Create bucket
	_, err := svc.CreateBucket(&s3.CreateBucketInput{
		Bucket: aws.String(bucketName),
	})
	require.NoError(t, err)
	defer cleanupBucket(t, bucketName)

	// Create objects
	objects := []string{"0/", "0/1000", "1999"}

	for _, key := range objects {
		_, err := svc.PutObject(&s3.PutObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(key),
			Body:   strings.NewReader(fmt.Sprintf("content for %s", key)),
		})
		require.NoError(t, err)
	}

	// Test without delimiter
	resp, err := svc.ListObjects(&s3.ListObjectsInput{
		Bucket: aws.String(bucketName),
		// No delimiter specified
	})
	require.NoError(t, err)

	// Extract keys
	var keys []string
	for _, content := range resp.Contents {
		keys = append(keys, *content.Key)
	}

	// When no delimiter is used, all objects should be returned as individual keys
	expectedKeys := []string{"0/", "0/1000", "1999"}
	assert.ElementsMatch(t, expectedKeys, keys, "All objects should be individual keys when no delimiter is used")

	// No common prefixes should be present
	assert.Empty(t, resp.CommonPrefixes, "No common prefixes should be present when no delimiter is used")
	assert.Contains(t, keys, "0/", "Directory key object '0/' should appear as individual key when no delimiter is used")
}

func cleanupBucket(t *testing.T, bucketName string) {
	// Delete all objects
	resp, err := svc.ListObjects(&s3.ListObjectsInput{
		Bucket: aws.String(bucketName),
	})
	if err != nil {
		t.Logf("Failed to list objects for cleanup: %v", err)
		return
	}

	for _, obj := range resp.Contents {
		_, err := svc.DeleteObject(&s3.DeleteObjectInput{
			Bucket: aws.String(bucketName),
			Key:    obj.Key,
		})
		if err != nil {
			t.Logf("Failed to delete object %s: %v", *obj.Key, err)
		}
	}

	// Give some time for eventual consistency
	time.Sleep(100 * time.Millisecond)

	// Delete bucket
	_, err = svc.DeleteBucket(&s3.DeleteBucketInput{
		Bucket: aws.String(bucketName),
	})
	if err != nil {
		t.Logf("Failed to delete bucket %s: %v", bucketName, err)
	}
}
