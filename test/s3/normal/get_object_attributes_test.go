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
	t.Run("VersionedObject", func(t *testing.T) {
		testGetObjectAttributesVersioned(t, cluster)
	})
	t.Run("ConditionalHeaders", func(t *testing.T) {
		testGetObjectAttributesConditionalHeaders(t, cluster)
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

	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Do(req)
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

func testGetObjectAttributesVersioned(t *testing.T, cluster *TestCluster) {
	client := newS3V2Client(cluster)
	bucketName := createTestBucket(t, cluster, "test-goa-ver-")

	// Enable versioning
	_, err := client.PutBucketVersioning(context.Background(), &v2s3.PutBucketVersioningInput{
		Bucket: v2aws.String(bucketName),
		VersioningConfiguration: &types.VersioningConfiguration{
			Status: types.BucketVersioningStatusEnabled,
		},
	})
	require.NoError(t, err)
	time.Sleep(200 * time.Millisecond)

	// Put two versions of the same object
	v1Data := "version 1 content"
	putResp1, err := client.PutObject(context.Background(), &v2s3.PutObjectInput{
		Bucket: v2aws.String(bucketName),
		Key:    v2aws.String("versioned-key"),
		Body:   strings.NewReader(v1Data),
	})
	require.NoError(t, err)
	require.NotNil(t, putResp1.VersionId)
	versionId1 := *putResp1.VersionId

	v2Data := "version 2 content - longer"
	putResp2, err := client.PutObject(context.Background(), &v2s3.PutObjectInput{
		Bucket: v2aws.String(bucketName),
		Key:    v2aws.String("versioned-key"),
		Body:   strings.NewReader(v2Data),
	})
	require.NoError(t, err)
	require.NotNil(t, putResp2.VersionId)
	versionId2 := *putResp2.VersionId

	assert.NotEqual(t, versionId1, versionId2, "versions should differ")

	// GetObjectAttributes for latest version (v2)
	resp, err := client.GetObjectAttributes(context.Background(), &v2s3.GetObjectAttributesInput{
		Bucket: v2aws.String(bucketName),
		Key:    v2aws.String("versioned-key"),
		ObjectAttributes: []types.ObjectAttributes{
			types.ObjectAttributesObjectSize,
			types.ObjectAttributesEtag,
		},
	})
	require.NoError(t, err)
	require.NotNil(t, resp.ObjectSize)
	assert.Equal(t, int64(len(v2Data)), *resp.ObjectSize)
	require.NotNil(t, resp.VersionId)
	assert.Equal(t, versionId2, *resp.VersionId)

	// GetObjectAttributes for specific older version (v1)
	resp1, err := client.GetObjectAttributes(context.Background(), &v2s3.GetObjectAttributesInput{
		Bucket:    v2aws.String(bucketName),
		Key:       v2aws.String("versioned-key"),
		VersionId: v2aws.String(versionId1),
		ObjectAttributes: []types.ObjectAttributes{
			types.ObjectAttributesObjectSize,
			types.ObjectAttributesEtag,
		},
	})
	require.NoError(t, err)
	require.NotNil(t, resp1.ObjectSize)
	assert.Equal(t, int64(len(v1Data)), *resp1.ObjectSize)
	require.NotNil(t, resp1.VersionId)
	assert.Equal(t, versionId1, *resp1.VersionId)

	t.Logf("Versioned GetObjectAttributes passed: v1 size=%d (id=%s), v2 size=%d (id=%s)",
		*resp1.ObjectSize, versionId1, *resp.ObjectSize, versionId2)
}

// signedGetObjectAttributes creates a signed GET request for ?attributes with custom headers.
func signedGetObjectAttributes(t *testing.T, cluster *TestCluster, bucketName, objectKey string, extraHeaders map[string]string) *http.Response {
	reqURL := fmt.Sprintf("%s/%s/%s?attributes", cluster.s3Endpoint, bucketName, objectKey)
	req, err := http.NewRequest("GET", reqURL, nil)
	require.NoError(t, err)
	req.Header.Set("X-Amz-Object-Attributes", "ETag,ObjectSize")
	for k, v := range extraHeaders {
		req.Header.Set(k, v)
	}
	signer := v1signer.NewSigner(v1credentials.NewStaticCredentials(testAccessKey, testSecretKey, ""))
	_, err = signer.Sign(req, nil, "s3", testRegion, time.Now())
	require.NoError(t, err)
	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Do(req)
	require.NoError(t, err)
	return resp
}

func testGetObjectAttributesConditionalHeaders(t *testing.T, cluster *TestCluster) {
	bucketName := createTestBucket(t, cluster, "test-goa-cond-")
	objectKey := "cond-test.txt"

	_, err := cluster.s3Client.PutObject(&v1s3.PutObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(objectKey),
		Body:   bytes.NewReader([]byte("conditional headers test")),
	})
	require.NoError(t, err)

	// Get the ETag and Last-Modified for the object
	headResp, err := cluster.s3Client.HeadObject(&v1s3.HeadObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(objectKey),
	})
	require.NoError(t, err)
	etag := aws.StringValue(headResp.ETag)
	lastModified := headResp.LastModified
	require.NotNil(t, lastModified)

	pastDate := lastModified.Add(-1 * time.Hour).UTC().Format(http.TimeFormat)
	futureDate := lastModified.Add(1 * time.Hour).UTC().Format(http.TimeFormat)

	// RFC 7232: If-Match true + If-Unmodified-Since false => 200 OK
	// If-Unmodified-Since is ignored when If-Match is present
	t.Run("IfMatch_true_IfUnmodifiedSince_false", func(t *testing.T) {
		resp := signedGetObjectAttributes(t, cluster, bucketName, objectKey, map[string]string{
			"If-Match":            etag,
			"If-Unmodified-Since": pastDate, // object was modified after this => false
		})
		defer resp.Body.Close()
		io.Copy(io.Discard, resp.Body)
		assert.Equal(t, 200, resp.StatusCode,
			"If-Match=true should return 200 even when If-Unmodified-Since=false (RFC 7232 Section 3.4)")
	})

	// RFC 7232: If-None-Match false + If-Modified-Since true => 304 Not Modified
	// If-Modified-Since is ignored when If-None-Match is present
	t.Run("IfNoneMatch_false_IfModifiedSince_true", func(t *testing.T) {
		resp := signedGetObjectAttributes(t, cluster, bucketName, objectKey, map[string]string{
			"If-None-Match":     etag,
			"If-Modified-Since": pastDate, // object was modified after this => true
		})
		defer resp.Body.Close()
		io.Copy(io.Discard, resp.Body)
		assert.Equal(t, 304, resp.StatusCode,
			"If-None-Match=false (ETag match) should return 304 even when If-Modified-Since=true (RFC 7232 Section 3.3)")
	})

	// If-Match succeeds, If-Unmodified-Since also succeeds => 200
	t.Run("IfMatch_true_IfUnmodifiedSince_true", func(t *testing.T) {
		resp := signedGetObjectAttributes(t, cluster, bucketName, objectKey, map[string]string{
			"If-Match":            etag,
			"If-Unmodified-Since": futureDate,
		})
		defer resp.Body.Close()
		io.Copy(io.Discard, resp.Body)
		assert.Equal(t, 200, resp.StatusCode)
	})

	// If-None-Match passes (ETag differs), If-Modified-Since ignored => 200
	// Per RFC 7232, If-Modified-Since is ignored when If-None-Match is present
	t.Run("IfNoneMatch_true_IfModifiedSince_ignored", func(t *testing.T) {
		resp := signedGetObjectAttributes(t, cluster, bucketName, objectKey, map[string]string{
			"If-None-Match":     `"nonexistent-etag"`,
			"If-Modified-Since": futureDate, // would fail alone, but is ignored
		})
		defer resp.Body.Close()
		io.Copy(io.Discard, resp.Body)
		assert.Equal(t, 200, resp.StatusCode,
			"If-None-Match=true means If-Modified-Since is ignored, should return 200 (RFC 7232 Section 3.3)")
	})

	// If-Match fails => 412 regardless of If-Unmodified-Since
	t.Run("IfMatch_false", func(t *testing.T) {
		resp := signedGetObjectAttributes(t, cluster, bucketName, objectKey, map[string]string{
			"If-Match":            `"wrong-etag"`,
			"If-Unmodified-Since": futureDate,
		})
		defer resp.Body.Close()
		io.Copy(io.Discard, resp.Body)
		assert.Equal(t, 412, resp.StatusCode)
	})

	t.Logf("Conditional headers tests passed")
}
