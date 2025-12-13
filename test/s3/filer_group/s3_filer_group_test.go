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
	MasterAddress: "localhost:9333",
	AccessKey:     "some_access_key1",
	SecretKey:     "some_secret_key1",
	Region:        "us-east-1",
	FilerGroup:    "testgroup", // Expected filer group for these tests
}

func init() {
	// Load config from file if exists
	if data, err := os.ReadFile("test_config.json"); err == nil {
		json.Unmarshal(data, testConfig)
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
		config.WithEndpointResolverWithOptions(aws.EndpointResolverWithOptionsFunc(
			func(service, region string, options ...interface{}) (aws.Endpoint, error) {
				return aws.Endpoint{
					URL:               testConfig.S3Endpoint,
					SigningRegion:     testConfig.Region,
					HostnameImmutable: true,
				}, nil
			})),
	)
	require.NoError(t, err)

	return s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.UsePathStyle = true
	})
}

func getMasterClient(t *testing.T) master_pb.SeaweedClient {
	conn, err := grpc.Dial(testConfig.MasterAddress, grpc.WithTransportCredentials(insecure.NewCredentials()))
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

	// Wait for collection to be visible
	time.Sleep(time.Second)

	// List collections and verify the expected collection exists
	collectionResp, err := masterClient.CollectionList(context.Background(), &master_pb.CollectionListRequest{
		IncludeNormalVolumes: true,
		IncludeEcVolumes:     true,
	})
	require.NoError(t, err, "CollectionList should succeed")

	found := false
	for _, c := range collectionResp.Collections {
		t.Logf("Found collection: %s", c.Name)
		if c.Name == expectedCollection {
			found = true
			break
		}
	}
	require.True(t, found, "Collection %s should exist (filer group prefix applied)", expectedCollection)

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

	// Verify collection was deleted
	time.Sleep(time.Second)
	collectionResp, err = masterClient.CollectionList(context.Background(), &master_pb.CollectionListRequest{
		IncludeNormalVolumes: true,
		IncludeEcVolumes:     true,
	})
	require.NoError(t, err)

	for _, c := range collectionResp.Collections {
		require.NotEqual(t, expectedCollection, c.Name, "Collection %s should be deleted", expectedCollection)
	}

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

	// Wait for collection to be created
	time.Sleep(time.Second)

	// Verify collection exists before deletion
	collectionResp, err := masterClient.CollectionList(context.Background(), &master_pb.CollectionListRequest{
		IncludeNormalVolumes: true,
		IncludeEcVolumes:     true,
	})
	require.NoError(t, err)

	collectionExistsBefore := false
	for _, c := range collectionResp.Collections {
		if c.Name == expectedCollection {
			collectionExistsBefore = true
			break
		}
	}
	require.True(t, collectionExistsBefore, "Collection should exist before bucket deletion")

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

	// Wait for collection deletion to propagate
	time.Sleep(time.Second)

	// Verify collection was deleted
	collectionResp, err = masterClient.CollectionList(context.Background(), &master_pb.CollectionListRequest{
		IncludeNormalVolumes: true,
		IncludeEcVolumes:     true,
	})
	require.NoError(t, err)

	collectionExistsAfter := false
	for _, c := range collectionResp.Collections {
		if c.Name == expectedCollection {
			collectionExistsAfter = true
			break
		}
	}
	require.False(t, collectionExistsAfter, "Collection %s should be deleted after bucket deletion", expectedCollection)

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

	time.Sleep(time.Second)

	// Verify all collections exist with correct naming
	collectionResp, err := masterClient.CollectionList(context.Background(), &master_pb.CollectionListRequest{
		IncludeNormalVolumes: true,
		IncludeEcVolumes:     true,
	})
	require.NoError(t, err)

	collectionSet := make(map[string]bool)
	for _, c := range collectionResp.Collections {
		collectionSet[c.Name] = true
	}

	for _, bucket := range buckets {
		expectedCollection := getExpectedCollectionName(bucket)
		require.True(t, collectionSet[expectedCollection],
			"Collection %s should exist for bucket %s", expectedCollection, bucket)
	}

	// Delete all buckets
	for _, bucket := range buckets {
		_, err = s3Client.DeleteObject(context.Background(), &s3.DeleteObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String("test-object"),
		})
		require.NoError(t, err)

		_, err = s3Client.DeleteBucket(context.Background(), &s3.DeleteBucketInput{
			Bucket: aws.String(bucket),
		})
		require.NoError(t, err)
	}

	time.Sleep(time.Second)

	// Verify all collections are deleted
	collectionResp, err = masterClient.CollectionList(context.Background(), &master_pb.CollectionListRequest{
		IncludeNormalVolumes: true,
		IncludeEcVolumes:     true,
	})
	require.NoError(t, err)

	collectionSet = make(map[string]bool)
	for _, c := range collectionResp.Collections {
		collectionSet[c.Name] = true
	}

	for _, bucket := range buckets {
		expectedCollection := getExpectedCollectionName(bucket)
		require.False(t, collectionSet[expectedCollection],
			"Collection %s should be deleted for bucket %s", expectedCollection, bucket)
	}

	t.Log("SUCCESS: Multiple bucket operations with filer group work correctly")
}

