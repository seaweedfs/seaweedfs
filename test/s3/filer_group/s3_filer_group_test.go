package filer_group

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
)

// TestConfig holds configuration for filer group S3 tests
type TestConfig struct {
	S3Endpoint    string `json:"s3_endpoint"`
	MasterAddress string `json:"master_address"`
	AccessKey     string `json:"access_key"`
	SecretKey     string `json:"secret_key"`
	Region        string `json:"region"`
	FilerGroup    string `json:"filer_group"`
}

var testConfig = &TestConfig{
	S3Endpoint:    "http://localhost:8333",
	MasterAddress: "localhost:19333", // gRPC port = 10000 + master HTTP port (9333)
	AccessKey:     "some_access_key1",
	SecretKey:     "some_secret_key1",
	Region:        "us-east-1",
	FilerGroup:    "testgroup", // Expected filer group for these tests
}

func init() {
	// Load config from file if exists
	if data, err := os.ReadFile("test_config.json"); err == nil {
		if err := json.Unmarshal(data, testConfig); err != nil {
			// Log but don't fail - env vars can still override
			fmt.Fprintf(os.Stderr, "Warning: failed to parse test_config.json: %v\n", err)
		}
	}

	// Override from environment variables
	if v := os.Getenv("S3_ENDPOINT"); v != "" {
		testConfig.S3Endpoint = v
	}
	if v := os.Getenv("MASTER_ADDRESS"); v != "" {
		testConfig.MasterAddress = v
	}
	if v := os.Getenv("FILER_GROUP"); v != "" {
		testConfig.FilerGroup = v
	}
}

func getS3Client(t *testing.T) *s3.Client {
	cfg, err := config.LoadDefaultConfig(context.TODO(),
		config.WithRegion(testConfig.Region),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(
			testConfig.AccessKey,
			testConfig.SecretKey,
			"",
		)),
	)
	require.NoError(t, err)

	return s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(testConfig.S3Endpoint)
		o.UsePathStyle = true
	})
}

func getMasterClient(t *testing.T) master_pb.SeaweedClient {
	conn, err := grpc.NewClient(testConfig.MasterAddress, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { conn.Close() })
	return master_pb.NewSeaweedClient(conn)
}

func getNewBucketName() string {
	return fmt.Sprintf("filergroup-test-%d", time.Now().UnixNano())
}

// getExpectedCollectionName returns the expected collection name for a bucket
// When filer group is configured, collections are named: {filerGroup}_{bucketName}
func getExpectedCollectionName(bucketName string) string {
	if testConfig.FilerGroup != "" {
		return fmt.Sprintf("%s_%s", testConfig.FilerGroup, bucketName)
	}
	return bucketName
}

// listAllCollections returns a list of all collection names from the master
func listAllCollections(t *testing.T, masterClient master_pb.SeaweedClient) []string {
	collectionResp, err := masterClient.CollectionList(context.Background(), &master_pb.CollectionListRequest{
		IncludeNormalVolumes: true,
		IncludeEcVolumes:     true,
	})
	if err != nil {
		t.Logf("Warning: failed to list collections: %v", err)
		return nil
	}
	var names []string
	for _, c := range collectionResp.Collections {
		names = append(names, c.Name)
	}
	return names
}

// collectionExists checks if a collection exists in the master
func collectionExists(t *testing.T, masterClient master_pb.SeaweedClient, collectionName string) bool {
	for _, name := range listAllCollections(t, masterClient) {
		if name == collectionName {
			return true
		}
	}
	return false
}

// waitForCollectionExists waits for a collection to exist using polling
func waitForCollectionExists(t *testing.T, masterClient master_pb.SeaweedClient, collectionName string) {
	var lastCollections []string
	success := assert.Eventually(t, func() bool {
		lastCollections = listAllCollections(t, masterClient)
		for _, name := range lastCollections {
			if name == collectionName {
				return true
			}
		}
		return false
	}, 10*time.Second, 200*time.Millisecond)
	if !success {
		t.Fatalf("collection %s should be created; existing collections: %v", collectionName, lastCollections)
	}
}

// waitForCollectionDeleted waits for a collection to be deleted using polling
func waitForCollectionDeleted(t *testing.T, masterClient master_pb.SeaweedClient, collectionName string) {
	require.Eventually(t, func() bool {
		return !collectionExists(t, masterClient, collectionName)
	}, 10*time.Second, 200*time.Millisecond, "collection %s should be deleted", collectionName)
}

// TestFilerGroupCollectionNaming verifies that when a filer group is configured,
// collections are created with the correct prefix ({filerGroup}_{bucketName})
func TestFilerGroupCollectionNaming(t *testing.T) {
	if testConfig.FilerGroup == "" {
		t.Skip("Skipping test: FILER_GROUP not configured. Set FILER_GROUP environment variable or configure in test_config.json")
	}

	s3Client := getS3Client(t)
	masterClient := getMasterClient(t)
	bucketName := getNewBucketName()
	expectedCollection := getExpectedCollectionName(bucketName)

	t.Logf("Testing with filer group: %s", testConfig.FilerGroup)
	t.Logf("Bucket name: %s", bucketName)
	t.Logf("Expected collection name: %s", expectedCollection)

	// Create bucket
	_, err := s3Client.CreateBucket(context.Background(), &s3.CreateBucketInput{
		Bucket: aws.String(bucketName),
	})
	require.NoError(t, err, "CreateBucket should succeed")

	// Upload an object to trigger collection creation
	_, err = s3Client.PutObject(context.Background(), &s3.PutObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String("test-object"),
		Body:   strings.NewReader("test content"),
	})
	require.NoError(t, err, "PutObject should succeed")

	// Wait for collection to be visible using polling
	waitForCollectionExists(t, masterClient, expectedCollection)

	// Verify collection exists with correct name
	require.True(t, collectionExists(t, masterClient, expectedCollection),
		"Collection %s should exist (filer group prefix applied)", expectedCollection)

	// Cleanup: delete object and bucket
	_, err = s3Client.DeleteObject(context.Background(), &s3.DeleteObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String("test-object"),
	})
	require.NoError(t, err, "DeleteObject should succeed")

	_, err = s3Client.DeleteBucket(context.Background(), &s3.DeleteBucketInput{
		Bucket: aws.String(bucketName),
	})
	require.NoError(t, err, "DeleteBucket should succeed")

	// Wait for collection to be deleted using polling
	waitForCollectionDeleted(t, masterClient, expectedCollection)

	t.Log("SUCCESS: Collection naming with filer group works correctly")
}

// TestBucketDeletionWithFilerGroup verifies that bucket deletion correctly
// deletes the collection when filer group is configured
func TestBucketDeletionWithFilerGroup(t *testing.T) {
	if testConfig.FilerGroup == "" {
		t.Skip("Skipping test: FILER_GROUP not configured")
	}

	s3Client := getS3Client(t)
	masterClient := getMasterClient(t)
	bucketName := getNewBucketName()
	expectedCollection := getExpectedCollectionName(bucketName)

	// Create bucket and add an object
	_, err := s3Client.CreateBucket(context.Background(), &s3.CreateBucketInput{
		Bucket: aws.String(bucketName),
	})
	require.NoError(t, err)

	_, err = s3Client.PutObject(context.Background(), &s3.PutObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String("test-object"),
		Body:   strings.NewReader("test content"),
	})
	require.NoError(t, err)

	// Wait for collection to be created using polling
	waitForCollectionExists(t, masterClient, expectedCollection)

	// Verify collection exists before deletion
	require.True(t, collectionExists(t, masterClient, expectedCollection),
		"Collection should exist before bucket deletion")

	// Delete object first
	_, err = s3Client.DeleteObject(context.Background(), &s3.DeleteObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String("test-object"),
	})
	require.NoError(t, err)

	// Delete bucket
	_, err = s3Client.DeleteBucket(context.Background(), &s3.DeleteBucketInput{
		Bucket: aws.String(bucketName),
	})
	require.NoError(t, err, "DeleteBucket should succeed")

	// Wait for collection to be deleted using polling
	waitForCollectionDeleted(t, masterClient, expectedCollection)

	// Verify collection was deleted
	require.False(t, collectionExists(t, masterClient, expectedCollection),
		"Collection %s should be deleted after bucket deletion", expectedCollection)

	t.Log("SUCCESS: Bucket deletion with filer group correctly deletes collection")
}

// TestMultipleBucketsWithFilerGroup tests creating and deleting multiple buckets
func TestMultipleBucketsWithFilerGroup(t *testing.T) {
	if testConfig.FilerGroup == "" {
		t.Skip("Skipping test: FILER_GROUP not configured")
	}

	s3Client := getS3Client(t)
	masterClient := getMasterClient(t)

	buckets := make([]string, 3)
	for i := 0; i < 3; i++ {
		buckets[i] = getNewBucketName()
	}

	// Create all buckets and add objects
	for _, bucket := range buckets {
		_, err := s3Client.CreateBucket(context.Background(), &s3.CreateBucketInput{
			Bucket: aws.String(bucket),
		})
		require.NoError(t, err)

		_, err = s3Client.PutObject(context.Background(), &s3.PutObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String("test-object"),
			Body:   strings.NewReader("test content"),
		})
		require.NoError(t, err)
	}

	// Wait for all collections to be created using polling
	for _, bucket := range buckets {
		expectedCollection := getExpectedCollectionName(bucket)
		waitForCollectionExists(t, masterClient, expectedCollection)
	}

	// Verify all collections exist with correct naming
	for _, bucket := range buckets {
		expectedCollection := getExpectedCollectionName(bucket)
		require.True(t, collectionExists(t, masterClient, expectedCollection),
			"Collection %s should exist for bucket %s", expectedCollection, bucket)
	}

	// Delete all buckets
	for _, bucket := range buckets {
		_, err := s3Client.DeleteObject(context.Background(), &s3.DeleteObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String("test-object"),
		})
		require.NoError(t, err)

		_, err = s3Client.DeleteBucket(context.Background(), &s3.DeleteBucketInput{
			Bucket: aws.String(bucket),
		})
		require.NoError(t, err)
	}

	// Wait for all collections to be deleted using polling
	for _, bucket := range buckets {
		expectedCollection := getExpectedCollectionName(bucket)
		waitForCollectionDeleted(t, masterClient, expectedCollection)
	}

	// Verify all collections are deleted
	for _, bucket := range buckets {
		expectedCollection := getExpectedCollectionName(bucket)
		require.False(t, collectionExists(t, masterClient, expectedCollection),
			"Collection %s should be deleted for bucket %s", expectedCollection, bucket)
	}

	t.Log("SUCCESS: Multiple bucket operations with filer group work correctly")
}
