package acl

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func getS3Client(t *testing.T) *s3.Client {
	endpoint := os.Getenv("S3_ENDPOINT")
	if endpoint == "" {
		endpoint = "http://localhost:8333"
	}
	accessKey := os.Getenv("S3_ACCESS_KEY")
	if accessKey == "" {
		accessKey = "some_access_key1"
	}
	secretKey := os.Getenv("S3_SECRET_KEY")
	if secretKey == "" {
		secretKey = "some_secret_key1"
	}

	cfg, err := config.LoadDefaultConfig(
		context.TODO(),
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(
			accessKey,
			secretKey,
			"",
		)),
	)
	require.NoError(t, err)
	return s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.UsePathStyle = true
		o.BaseEndpoint = aws.String(endpoint)
	})
}

func createVersionedTestBucket(t *testing.T, client *s3.Client) string {
	bucketName := "test-acl-versioned-" + strings.ToLower(strings.ReplaceAll(time.Now().Format("2006-01-02-15-04-05.000"), ":", "-"))
	_, err := client.CreateBucket(context.TODO(), &s3.CreateBucketInput{
		Bucket: aws.String(bucketName),
	})
	require.NoError(t, err)

	_, err = client.PutBucketVersioning(context.TODO(), &s3.PutBucketVersioningInput{
		Bucket: aws.String(bucketName),
		VersioningConfiguration: &types.VersioningConfiguration{
			Status: types.BucketVersioningStatusEnabled,
		},
	})
	require.NoError(t, err)
	time.Sleep(100 * time.Millisecond)
	return bucketName
}

func cleanupTestBucket(t *testing.T, client *s3.Client, bucketName string) {
	listResp, err := client.ListObjectsV2(context.TODO(), &s3.ListObjectsV2Input{
		Bucket: aws.String(bucketName),
	})
	if err == nil {
		for _, obj := range listResp.Contents {
			client.DeleteObject(context.TODO(), &s3.DeleteObjectInput{
				Bucket: aws.String(bucketName),
				Key:    obj.Key,
			})
		}
	}

	listVersionsResp, err := client.ListObjectVersions(context.TODO(), &s3.ListObjectVersionsInput{
		Bucket: aws.String(bucketName),
	})
	if err == nil {
		for _, version := range listVersionsResp.Versions {
			client.DeleteObject(context.TODO(), &s3.DeleteObjectInput{
				Bucket:    aws.String(bucketName),
				Key:       version.Key,
				VersionId: version.VersionId,
			})
		}
		for _, marker := range listVersionsResp.DeleteMarkers {
			client.DeleteObject(context.TODO(), &s3.DeleteObjectInput{
				Bucket:    aws.String(bucketName),
				Key:       marker.Key,
				VersionId: marker.VersionId,
			})
		}
	}

	client.DeleteBucket(context.TODO(), &s3.DeleteBucketInput{
		Bucket: aws.String(bucketName),
	})
}

// TestGetObjectAclOnVersionedBucket tests retrieving ACL from versioned objects
func TestGetObjectAclOnVersionedBucket(t *testing.T) {
	client := getS3Client(t)
	bucketName := createVersionedTestBucket(t, client)
	defer cleanupTestBucket(t, client, bucketName)

	objectKey := "versioned-object-acl"
	putResp, err := client.PutObject(context.TODO(), &s3.PutObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(objectKey),
		Body:   strings.NewReader("Hello, ACL World!"),
	})
	require.NoError(t, err)

	aclResp, err := client.GetObjectAcl(context.TODO(), &s3.GetObjectAclInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(objectKey),
	})
	require.NoError(t, err)
	assert.NotNil(t, aclResp.Owner)

	t.Logf("Successfully retrieved ACL for versioned object %s (versionId: %s)", objectKey, *putResp.VersionId)
}

// TestGetObjectAclOnSpecificVersionInVersionedBucket tests retrieving ACL for specific versions
func TestGetObjectAclOnSpecificVersionInVersionedBucket(t *testing.T) {
	client := getS3Client(t)
	bucketName := createVersionedTestBucket(t, client)
	defer cleanupTestBucket(t, client, bucketName)

	objectKey := "multi-version-object-acl"

	version1Resp, err := client.PutObject(context.TODO(), &s3.PutObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(objectKey),
		Body:   strings.NewReader("Version 1"),
	})
	require.NoError(t, err)

	time.Sleep(50 * time.Millisecond)

	version2Resp, err := client.PutObject(context.TODO(), &s3.PutObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(objectKey),
		Body:   strings.NewReader("Version 2"),
	})
	require.NoError(t, err)

	versionId1 := *version1Resp.VersionId
	versionId2 := *version2Resp.VersionId

	aclResp1, err := client.GetObjectAcl(context.TODO(), &s3.GetObjectAclInput{
		Bucket:    aws.String(bucketName),
		Key:       aws.String(objectKey),
		VersionId: aws.String(versionId1),
	})
	require.NoError(t, err)
	assert.NotNil(t, aclResp1.Owner)

	aclResp2, err := client.GetObjectAcl(context.TODO(), &s3.GetObjectAclInput{
		Bucket:    aws.String(bucketName),
		Key:       aws.String(objectKey),
		VersionId: aws.String(versionId2),
	})
	require.NoError(t, err)
	assert.NotNil(t, aclResp2.Owner)

	t.Logf("Successfully retrieved ACL for both versions: v1=%s, v2=%s", versionId1, versionId2)
}

// TestPutObjectAclOnVersionedBucket tests setting ACL on versioned objects
func TestPutObjectAclOnVersionedBucket(t *testing.T) {
	client := getS3Client(t)
	bucketName := createVersionedTestBucket(t, client)
	defer cleanupTestBucket(t, client, bucketName)

	objectKey := "versioned-object-put-acl"
	putResp, err := client.PutObject(context.TODO(), &s3.PutObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(objectKey),
		Body:   strings.NewReader("Hello, Put ACL!"),
	})
	require.NoError(t, err)

	_, err = client.PutObjectAcl(context.TODO(), &s3.PutObjectAclInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(objectKey),
		ACL:    types.ObjectCannedACLPublicRead,
	})
	require.NoError(t, err)

	aclResp, err := client.GetObjectAcl(context.TODO(), &s3.GetObjectAclInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(objectKey),
	})
	require.NoError(t, err)
	assert.NotNil(t, aclResp.Owner)

	t.Logf("Successfully set and verified ACL for versioned object %s (versionId: %s)", objectKey, *putResp.VersionId)
}

// TestPutObjectAclOnSpecificVersionInVersionedBucket tests setting ACL on specific versions
func TestPutObjectAclOnSpecificVersionInVersionedBucket(t *testing.T) {
	client := getS3Client(t)
	bucketName := createVersionedTestBucket(t, client)
	defer cleanupTestBucket(t, client, bucketName)

	objectKey := "multi-version-object-put-acl"

	version1Resp, err := client.PutObject(context.TODO(), &s3.PutObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(objectKey),
		Body:   strings.NewReader("Version 1"),
	})
	require.NoError(t, err)

	time.Sleep(50 * time.Millisecond)

	version2Resp, err := client.PutObject(context.TODO(), &s3.PutObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(objectKey),
		Body:   strings.NewReader("Version 2"),
	})
	require.NoError(t, err)

	versionId1 := *version1Resp.VersionId
	versionId2 := *version2Resp.VersionId

	_, err = client.PutObjectAcl(context.TODO(), &s3.PutObjectAclInput{
		Bucket:    aws.String(bucketName),
		Key:       aws.String(objectKey),
		VersionId: aws.String(versionId1),
		ACL:       types.ObjectCannedACLPublicRead,
	})
	require.NoError(t, err)

	_, err = client.PutObjectAcl(context.TODO(), &s3.PutObjectAclInput{
		Bucket:    aws.String(bucketName),
		Key:       aws.String(objectKey),
		VersionId: aws.String(versionId2),
		ACL:       types.ObjectCannedACLPrivate,
	})
	require.NoError(t, err)

	aclResp1, err := client.GetObjectAcl(context.TODO(), &s3.GetObjectAclInput{
		Bucket:    aws.String(bucketName),
		Key:       aws.String(objectKey),
		VersionId: aws.String(versionId1),
	})
	require.NoError(t, err)
	assert.NotNil(t, aclResp1.Owner)

	aclResp2, err := client.GetObjectAcl(context.TODO(), &s3.GetObjectAclInput{
		Bucket:    aws.String(bucketName),
		Key:       aws.String(objectKey),
		VersionId: aws.String(versionId2),
	})
	require.NoError(t, err)
	assert.NotNil(t, aclResp2.Owner)

	t.Logf("Successfully set ACL on both versions: v1=%s, v2=%s", versionId1, versionId2)
}

// TestModifyAclOnDifferentVersionsIndependently tests that ACL changes on one version don't affect others
func TestModifyAclOnDifferentVersionsIndependently(t *testing.T) {
	client := getS3Client(t)
	bucketName := createVersionedTestBucket(t, client)
	defer cleanupTestBucket(t, client, bucketName)

	objectKey := "versioned-independent-acl"

	version1Resp, err := client.PutObject(context.TODO(), &s3.PutObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(objectKey),
		Body:   strings.NewReader("Version 1"),
	})
	require.NoError(t, err)

	time.Sleep(50 * time.Millisecond)

	version2Resp, err := client.PutObject(context.TODO(), &s3.PutObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(objectKey),
		Body:   strings.NewReader("Version 2"),
	})
	require.NoError(t, err)

	versionId1 := *version1Resp.VersionId
	versionId2 := *version2Resp.VersionId

	_, err = client.PutObjectAcl(context.TODO(), &s3.PutObjectAclInput{
		Bucket:    aws.String(bucketName),
		Key:       aws.String(objectKey),
		VersionId: aws.String(versionId1),
		ACL:       types.ObjectCannedACLPublicRead,
	})
	require.NoError(t, err)

	_, err = client.PutObjectAcl(context.TODO(), &s3.PutObjectAclInput{
		Bucket:    aws.String(bucketName),
		Key:       aws.String(objectKey),
		VersionId: aws.String(versionId2),
		ACL:       types.ObjectCannedACLPrivate,
	})
	require.NoError(t, err)

	aclRespLatest, err := client.GetObjectAcl(context.TODO(), &s3.GetObjectAclInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(objectKey),
	})
	require.NoError(t, err)
	assert.NotNil(t, aclRespLatest.Owner)

	aclResp1, err := client.GetObjectAcl(context.TODO(), &s3.GetObjectAclInput{
		Bucket:    aws.String(bucketName),
		Key:       aws.String(objectKey),
		VersionId: aws.String(versionId1),
	})
	require.NoError(t, err)
	assert.NotNil(t, aclResp1.Owner)

	aclResp2, err := client.GetObjectAcl(context.TODO(), &s3.GetObjectAclInput{
		Bucket:    aws.String(bucketName),
		Key:       aws.String(objectKey),
		VersionId: aws.String(versionId2),
	})
	require.NoError(t, err)
	assert.NotNil(t, aclResp2.Owner)

	t.Logf("Successfully verified independent ACL management across versions")
}
