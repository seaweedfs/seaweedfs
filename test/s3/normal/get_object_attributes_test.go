package example

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	v1credentials "github.com/aws/aws-sdk-go/aws/credentials"
	v1signer "github.com/aws/aws-sdk-go/aws/signer/v4"
	v1s3 "github.com/aws/aws-sdk-go/service/s3"
	v2aws "github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	v2s3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// newS3V2Client creates an AWS SDK v2 S3 client from the test cluster.
func newS3V2Client(cluster *TestCluster) *v2s3.Client {
	return v2s3.New(v2s3.Options{
		Region:       testRegion,
		BaseEndpoint: v2aws.String(cluster.s3Endpoint),
		Credentials:  v2aws.NewCredentialsCache(credentials.NewStaticCredentialsProvider(testAccessKey, testSecretKey, "")),
		UsePathStyle: true,
	})
}

func TestGetObjectAttributes(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	cluster, err := startMiniCluster(t)
	require.NoError(t, err)
	defer cluster.Stop()

	t.Run("Basic", func(t *testing.T) {
		testGetObjectAttributesBasic(t, cluster)
	})
	t.Run("MultipartObject", func(t *testing.T) {
		testGetObjectAttributesMultipart(t, cluster)
	})
	t.Run("SelectiveAttributes", func(t *testing.T) {
		testGetObjectAttributesSelective(t, cluster)
	})
	t.Run("InvalidAttribute", func(t *testing.T) {
		testGetObjectAttributesInvalid(t, cluster)
	})
	t.Run("NonExistentObject", func(t *testing.T) {
		testGetObjectAttributesNotFound(t, cluster)
	})
}

func testGetObjectAttributesBasic(t *testing.T, cluster *TestCluster) {
	bucketName := createTestBucket(t, cluster, "test-goa-basic-")
	objectKey := "test-object.txt"
	objectData := "Hello, GetObjectAttributes!"

	_, err := cluster.s3Client.PutObject(&v1s3.PutObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(objectKey),
		Body:   bytes.NewReader([]byte(objectData)),
	})
	require.NoError(t, err)

	client := newS3V2Client(cluster)
	resp, err := client.GetObjectAttributes(context.Background(), &v2s3.GetObjectAttributesInput{
		Bucket: v2aws.String(bucketName),
		Key:    v2aws.String(objectKey),
		ObjectAttributes: []types.ObjectAttributes{
			types.ObjectAttributesEtag,
			types.ObjectAttributesStorageClass,
			types.ObjectAttributesObjectSize,
			types.ObjectAttributesObjectParts,
		},
	})
	require.NoError(t, err)

	// ETag should be present and non-empty
	require.NotNil(t, resp.ETag)
	assert.NotEmpty(t, *resp.ETag)
	assert.False(t, strings.Contains(*resp.ETag, `"`), "ETag in XML body should not have quotes")

	// ObjectSize should match
	require.NotNil(t, resp.ObjectSize)
	assert.Equal(t, int64(len(objectData)), *resp.ObjectSize)

	// StorageClass should be STANDARD (default)
	assert.Equal(t, "STANDARD", string(resp.StorageClass))

	// ObjectParts should be nil for non-multipart objects
	assert.Nil(t, resp.ObjectParts)

	// LastModified header should be present
	assert.NotNil(t, resp.LastModified)

	t.Logf("Basic GetObjectAttributes passed: ETag=%s, Size=%d, StorageClass=%s",
		*resp.ETag, *resp.ObjectSize, resp.StorageClass)
}

func testGetObjectAttributesMultipart(t *testing.T, cluster *TestCluster) {
	bucketName := createTestBucket(t, cluster, "test-goa-mp-")
	objectKey := "test-multipart.bin"

	// Create a 2-part multipart upload
	part1Data := bytes.Repeat([]byte("A"), 5*1024*1024) // 5MB (minimum part size)
	part2Data := bytes.Repeat([]byte("B"), 3*1024*1024) // 3MB

	initResp, err := cluster.s3Client.CreateMultipartUpload(&v1s3.CreateMultipartUploadInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(objectKey),
	})
	require.NoError(t, err)
	uploadID := initResp.UploadId

	part1Resp, err := cluster.s3Client.UploadPart(&v1s3.UploadPartInput{
		Bucket:     aws.String(bucketName),
		Key:        aws.String(objectKey),
		PartNumber: aws.Int64(1),
		UploadId:   uploadID,
		Body:       bytes.NewReader(part1Data),
	})
	require.NoError(t, err)

	part2Resp, err := cluster.s3Client.UploadPart(&v1s3.UploadPartInput{
		Bucket:     aws.String(bucketName),
		Key:        aws.String(objectKey),
		PartNumber: aws.Int64(2),
		UploadId:   uploadID,
		Body:       bytes.NewReader(part2Data),
	})
	require.NoError(t, err)

	_, err = cluster.s3Client.CompleteMultipartUpload(&v1s3.CompleteMultipartUploadInput{
		Bucket:   aws.String(bucketName),
		Key:      aws.String(objectKey),
		UploadId: uploadID,
		MultipartUpload: &v1s3.CompletedMultipartUpload{
			Parts: []*v1s3.CompletedPart{
				{ETag: part1Resp.ETag, PartNumber: aws.Int64(1)},
				{ETag: part2Resp.ETag, PartNumber: aws.Int64(2)},
			},
		},
	})
	require.NoError(t, err)

	// Wait briefly for metadata to settle
	time.Sleep(200 * time.Millisecond)

	client := newS3V2Client(cluster)
	resp, err := client.GetObjectAttributes(context.Background(), &v2s3.GetObjectAttributesInput{
		Bucket: v2aws.String(bucketName),
		Key:    v2aws.String(objectKey),
		ObjectAttributes: []types.ObjectAttributes{
			types.ObjectAttributesObjectParts,
			types.ObjectAttributesObjectSize,
		},
	})
	require.NoError(t, err)

	require.NotNil(t, resp.ObjectSize)
	assert.Equal(t, int64(len(part1Data)+len(part2Data)), *resp.ObjectSize)

	require.NotNil(t, resp.ObjectParts, "ObjectParts should be present for multipart objects")
	assert.Equal(t, int32(2), *resp.ObjectParts.TotalPartsCount)
	require.Len(t, resp.ObjectParts.Parts, 2)
	assert.Equal(t, int32(1), *resp.ObjectParts.Parts[0].PartNumber)
	assert.Equal(t, int64(len(part1Data)), *resp.ObjectParts.Parts[0].Size)
	assert.Equal(t, int32(2), *resp.ObjectParts.Parts[1].PartNumber)
	assert.Equal(t, int64(len(part2Data)), *resp.ObjectParts.Parts[1].Size)

	// Test pagination: MaxParts=1
	resp2, err := client.GetObjectAttributes(context.Background(), &v2s3.GetObjectAttributesInput{
		Bucket:  v2aws.String(bucketName),
		Key:     v2aws.String(objectKey),
		MaxParts: v2aws.Int32(1),
		ObjectAttributes: []types.ObjectAttributes{
			types.ObjectAttributesObjectParts,
		},
	})
	require.NoError(t, err)
	require.NotNil(t, resp2.ObjectParts)
	assert.Len(t, resp2.ObjectParts.Parts, 1)
	assert.True(t, *resp2.ObjectParts.IsTruncated)
	assert.Equal(t, int32(2), *resp2.ObjectParts.TotalPartsCount)

	t.Logf("Multipart GetObjectAttributes passed: %d parts, total size %d",
		*resp.ObjectParts.TotalPartsCount, *resp.ObjectSize)
}

func testGetObjectAttributesSelective(t *testing.T, cluster *TestCluster) {
	bucketName := createTestBucket(t, cluster, "test-goa-sel-")
	objectKey := "test-selective.txt"
	objectData := "Selective attributes test"

	_, err := cluster.s3Client.PutObject(&v1s3.PutObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(objectKey),
		Body:   bytes.NewReader([]byte(objectData)),
	})
	require.NoError(t, err)

	client := newS3V2Client(cluster)

	// Request only ETag
	resp, err := client.GetObjectAttributes(context.Background(), &v2s3.GetObjectAttributesInput{
		Bucket: v2aws.String(bucketName),
		Key:    v2aws.String(objectKey),
		ObjectAttributes: []types.ObjectAttributes{
			types.ObjectAttributesEtag,
		},
	})
	require.NoError(t, err)
	require.NotNil(t, resp.ETag)
	assert.NotEmpty(t, *resp.ETag)
	assert.Nil(t, resp.ObjectSize, "ObjectSize should not be present when not requested")
	assert.Empty(t, string(resp.StorageClass), "StorageClass should not be present when not requested")
	assert.Nil(t, resp.ObjectParts, "ObjectParts should not be present when not requested")

	t.Logf("Selective GetObjectAttributes passed: ETag=%s", *resp.ETag)
}

func testGetObjectAttributesInvalid(t *testing.T, cluster *TestCluster) {
	bucketName := createTestBucket(t, cluster, "test-goa-inv-")
	objectKey := "test-object.txt"

	_, err := cluster.s3Client.PutObject(&v1s3.PutObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(objectKey),
		Body:   bytes.NewReader([]byte("test")),
	})
	require.NoError(t, err)

	// Use raw HTTP to send an invalid attribute name since the SDK validates
	reqURL := fmt.Sprintf("%s/%s/%s?attributes", cluster.s3Endpoint, bucketName, objectKey)
	req, err := http.NewRequest("GET", reqURL, nil)
	require.NoError(t, err)
	req.Header.Set("X-Amz-Object-Attributes", "InvalidAttr")

	signer := v1signer.NewSigner(v1credentials.NewStaticCredentials(testAccessKey, testSecretKey, ""))
	_, err = signer.Sign(req, nil, "s3", testRegion, time.Now())
	require.NoError(t, err)

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	io.Copy(io.Discard, resp.Body)

	assert.Equal(t, 400, resp.StatusCode)
	t.Logf("Invalid attribute test passed: got %d", resp.StatusCode)
}

func testGetObjectAttributesNotFound(t *testing.T, cluster *TestCluster) {
	bucketName := createTestBucket(t, cluster, "test-goa-nf-")

	client := newS3V2Client(cluster)
	_, err := client.GetObjectAttributes(context.Background(), &v2s3.GetObjectAttributesInput{
		Bucket: v2aws.String(bucketName),
		Key:    v2aws.String("nonexistent-key"),
		ObjectAttributes: []types.ObjectAttributes{
			types.ObjectAttributesEtag,
		},
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "NoSuchKey")

	t.Logf("NotFound GetObjectAttributes passed")
}
