package checksum_test

import (
	"bytes"
	"context"
	"crypto/sha1"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"io"
	"net/http"
	"os"
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

// Integration test for https://github.com/seaweedfs/seaweedfs/issues/9075:
// presigned PUT URLs that request a flexible-checksum algorithm (SHA256, SHA1, ...)
// must cause the server to compute and persist that checksum, and HEAD/GET must
// return the x-amz-checksum-* header when the caller asks for it.
//
// AWS SDK presigners hoist headers like x-amz-sdk-checksum-algorithm into the
// signed URL's query string, so we deliberately upload the body with a plain
// http.Client to ensure the server sees the checksum algorithm via the query
// string — which is exactly what a browser / curl / non-SDK client does.

type s3TestConfig struct {
	Endpoint     string
	AccessKey    string
	SecretKey    string
	Region       string
	BucketPrefix string
}

var defaultConfig = &s3TestConfig{
	Endpoint:     "http://localhost:8333",
	AccessKey:    "some_access_key1",
	SecretKey:    "some_secret_key1",
	Region:       "us-east-1",
	BucketPrefix: "test-checksum-",
}

func init() {
	if v := os.Getenv("S3_ENDPOINT"); v != "" {
		defaultConfig.Endpoint = v
	}
}

func getS3Client(t *testing.T) *s3.Client {
	cfg, err := config.LoadDefaultConfig(context.TODO(),
		config.WithRegion(defaultConfig.Region),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(
			defaultConfig.AccessKey, defaultConfig.SecretKey, "")),
		config.WithEndpointResolverWithOptions(aws.EndpointResolverWithOptionsFunc(
			func(service, region string, _ ...interface{}) (aws.Endpoint, error) {
				return aws.Endpoint{
					URL:               defaultConfig.Endpoint,
					SigningRegion:     defaultConfig.Region,
					HostnameImmutable: true,
				}, nil
			})),
	)
	require.NoError(t, err)
	return s3.NewFromConfig(cfg, func(o *s3.Options) { o.UsePathStyle = true })
}

func uniqueBucket() string {
	return fmt.Sprintf("%s%d", defaultConfig.BucketPrefix, time.Now().UnixNano())
}

func createBucket(t *testing.T, client *s3.Client, name string) {
	_, err := client.CreateBucket(context.TODO(), &s3.CreateBucketInput{Bucket: aws.String(name)})
	require.NoError(t, err)
}

func cleanupBucket(t *testing.T, client *s3.Client, name string) {
	t.Helper()
	objs, err := client.ListObjectsV2(context.TODO(), &s3.ListObjectsV2Input{Bucket: aws.String(name)})
	if err == nil {
		for _, o := range objs.Contents {
			_, _ = client.DeleteObject(context.TODO(), &s3.DeleteObjectInput{
				Bucket: aws.String(name), Key: o.Key,
			})
		}
	}
	_, _ = client.DeleteBucket(context.TODO(), &s3.DeleteBucketInput{Bucket: aws.String(name)})
}

// uploadViaPresignedURL PUTs body to the given presigned URL using a plain
// http.Client, mirroring what a browser / curl / non-SDK client would do.
// Crucially this path does NOT run any AWS SDK middleware, so the server must
// compute and store the checksum based on the algorithm parameter that was
// hoisted into the query string by the presigner.
func uploadViaPresignedURL(t *testing.T, url string, body []byte) {
	t.Helper()
	req, err := http.NewRequest(http.MethodPut, url, bytes.NewReader(body))
	require.NoError(t, err)
	req.ContentLength = int64(len(body))
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	respBody, _ := io.ReadAll(resp.Body)
	require.Equalf(t, http.StatusOK, resp.StatusCode,
		"presigned PUT failed: %d %s", resp.StatusCode, string(respBody))
}

// TestPresignedPutWithChecksumSHA256 reproduces issue #9075: a presigned PUT
// URL generated with ChecksumAlgorithm=SHA256 must result in the object being
// stored with an x-amz-checksum-sha256 attribute, visible via HEAD when the
// caller requests ChecksumMode=ENABLED.
func TestPresignedPutWithChecksumSHA256(t *testing.T) {
	client := getS3Client(t)
	bucket := uniqueBucket()
	createBucket(t, client, bucket)
	defer cleanupBucket(t, client, bucket)

	key := "presigned-sha256.txt"
	body := []byte("hello seaweedfs checksum")
	expected := base64.StdEncoding.EncodeToString(func() []byte {
		h := sha256.Sum256(body)
		return h[:]
	}())

	presignClient := s3.NewPresignClient(client)
	req, err := presignClient.PresignPutObject(context.TODO(), &s3.PutObjectInput{
		Bucket:            aws.String(bucket),
		Key:               aws.String(key),
		ChecksumAlgorithm: types.ChecksumAlgorithmSha256,
	}, func(o *s3.PresignOptions) { o.Expires = 10 * time.Minute })
	require.NoError(t, err)

	uploadViaPresignedURL(t, req.URL, body)

	head, err := client.HeadObject(context.TODO(), &s3.HeadObjectInput{
		Bucket:       aws.String(bucket),
		Key:          aws.String(key),
		ChecksumMode: types.ChecksumModeEnabled,
	})
	require.NoError(t, err)
	require.NotNil(t, head.ChecksumSHA256, "x-amz-checksum-sha256 missing from HEAD response")
	assert.Equal(t, expected, aws.ToString(head.ChecksumSHA256),
		"stored SHA256 does not match body")

	// GET with ChecksumMode=ENABLED should also return the header and the body should match.
	getOut, err := client.GetObject(context.TODO(), &s3.GetObjectInput{
		Bucket:       aws.String(bucket),
		Key:          aws.String(key),
		ChecksumMode: types.ChecksumModeEnabled,
	})
	require.NoError(t, err)
	defer getOut.Body.Close()
	require.NotNil(t, getOut.ChecksumSHA256, "x-amz-checksum-sha256 missing from GET response")
	assert.Equal(t, expected, aws.ToString(getOut.ChecksumSHA256))
	got, err := io.ReadAll(getOut.Body)
	require.NoError(t, err)
	assert.Equal(t, body, got)
}

// TestPresignedPutWithChecksumSHA1 is the SHA1 variant — catches regressions
// where the fix was wired only for a single algorithm.
func TestPresignedPutWithChecksumSHA1(t *testing.T) {
	client := getS3Client(t)
	bucket := uniqueBucket()
	createBucket(t, client, bucket)
	defer cleanupBucket(t, client, bucket)

	key := "presigned-sha1.txt"
	body := []byte("another checksum payload")
	sum := sha1.Sum(body)
	expected := base64.StdEncoding.EncodeToString(sum[:])

	presignClient := s3.NewPresignClient(client)
	req, err := presignClient.PresignPutObject(context.TODO(), &s3.PutObjectInput{
		Bucket:            aws.String(bucket),
		Key:               aws.String(key),
		ChecksumAlgorithm: types.ChecksumAlgorithmSha1,
	}, func(o *s3.PresignOptions) { o.Expires = 10 * time.Minute })
	require.NoError(t, err)

	uploadViaPresignedURL(t, req.URL, body)

	head, err := client.HeadObject(context.TODO(), &s3.HeadObjectInput{
		Bucket:       aws.String(bucket),
		Key:          aws.String(key),
		ChecksumMode: types.ChecksumModeEnabled,
	})
	require.NoError(t, err)
	require.NotNil(t, head.ChecksumSHA1, "x-amz-checksum-sha1 missing from HEAD response")
	assert.Equal(t, expected, aws.ToString(head.ChecksumSHA1))
}

// TestPresignedPutWithoutChecksumAlgorithm is a negative control: when the
// caller doesn't request a checksum algorithm, HEAD should not return one.
func TestPresignedPutWithoutChecksumAlgorithm(t *testing.T) {
	client := getS3Client(t)
	bucket := uniqueBucket()
	createBucket(t, client, bucket)
	defer cleanupBucket(t, client, bucket)

	key := "presigned-nosum.txt"
	body := []byte("no checksum requested")

	presignClient := s3.NewPresignClient(client)
	req, err := presignClient.PresignPutObject(context.TODO(), &s3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	}, func(o *s3.PresignOptions) { o.Expires = 10 * time.Minute })
	require.NoError(t, err)

	uploadViaPresignedURL(t, req.URL, body)

	head, err := client.HeadObject(context.TODO(), &s3.HeadObjectInput{
		Bucket:       aws.String(bucket),
		Key:          aws.String(key),
		ChecksumMode: types.ChecksumModeEnabled,
	})
	require.NoError(t, err)
	assert.Nil(t, head.ChecksumSHA256)
	assert.Nil(t, head.ChecksumSHA1)
	assert.Nil(t, head.ChecksumCRC32)
	assert.Nil(t, head.ChecksumCRC32C)
}
