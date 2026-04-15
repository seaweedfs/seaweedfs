package checksum_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
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

func getenvAny(keys ...string) string {
	for _, k := range keys {
		if v := os.Getenv(k); v != "" {
			return v
		}
	}
	return ""
}

func init() {
	if v := getenvAny("S3_ENDPOINT"); v != "" {
		defaultConfig.Endpoint = v
	}
	if v := getenvAny("S3_ACCESS_KEY", "AWS_ACCESS_KEY_ID"); v != "" {
		defaultConfig.AccessKey = v
	}
	if v := getenvAny("S3_SECRET_KEY", "AWS_SECRET_ACCESS_KEY"); v != "" {
		defaultConfig.SecretKey = v
	}
	if v := getenvAny("AWS_REGION", "AWS_DEFAULT_REGION"); v != "" {
		defaultConfig.Region = v
	}
}

// presignedHTTPClient is used for non-SDK PUTs to a presigned URL. A fixed
// timeout keeps tests from hanging forever if the server stalls.
var presignedHTTPClient = &http.Client{Timeout: 30 * time.Second}

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
//
// extraHeaders are additional HTTP headers the presigner signed (e.g.
// Content-MD5). Their values must exactly match what was signed or SigV4
// verification will return SignatureDoesNotMatch.
func uploadViaPresignedURL(t *testing.T, url string, body []byte, extraHeaders map[string]string) {
	t.Helper()
	req, err := http.NewRequest(http.MethodPut, url, bytes.NewReader(body))
	require.NoError(t, err)
	req.ContentLength = int64(len(body))
	for k, v := range extraHeaders {
		req.Header.Set(k, v)
	}
	resp, err := presignedHTTPClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	respBody, err := io.ReadAll(resp.Body)
	require.NoError(t, err, "read presigned PUT response body")
	require.Equalf(t, http.StatusOK, resp.StatusCode,
		"presigned PUT failed: %d %s", resp.StatusCode, string(respBody))
}

// presignPutURL builds a presigned S3 PutObject URL using the low-level
// SigV4 presigner, with an optional x-amz-sdk-checksum-algorithm query
// parameter baked into the signed canonical query string.
//
// We use the raw signer instead of s3.PresignClient because the SDK's
// flexible-checksum middleware tries to inject a Content-MD5 header for
// flexible-checksum PutObject calls, and at presign time (no body) it seeds
// MD5-of-empty, which then mismatches any real body uploaded through a plain
// http.Client. The raw signer has no such middleware and produces exactly
// the kind of URL a browser, curl caller, or custom client would receive.
func presignPutURL(t *testing.T, bucket, key, checksumAlgorithm string) string {
	t.Helper()
	putURL, err := url.Parse(fmt.Sprintf("%s/%s/%s", strings.TrimRight(defaultConfig.Endpoint, "/"), bucket, key))
	require.NoError(t, err)
	q := putURL.Query()
	if checksumAlgorithm != "" {
		q.Set("x-amz-sdk-checksum-algorithm", checksumAlgorithm)
	}
	putURL.RawQuery = q.Encode()

	req, err := http.NewRequest(http.MethodPut, putURL.String(), nil)
	require.NoError(t, err)

	signer := v4.NewSigner()
	creds := aws.Credentials{
		AccessKeyID:     defaultConfig.AccessKey,
		SecretAccessKey: defaultConfig.SecretKey,
	}
	// For presigned URLs the AWS convention is UNSIGNED-PAYLOAD so the
	// signer doesn't require a body-hash up front.
	signedURL, _, err := signer.PresignHTTP(context.TODO(), creds, req,
		"UNSIGNED-PAYLOAD", "s3", defaultConfig.Region, time.Now(),
		func(o *v4.SignerOptions) {
			o.DisableURIPathEscaping = true
		})
	require.NoError(t, err)
	return signedURL
}

// TestPresignedPutWithChecksumSHA256 reproduces issue #9075: a presigned PUT
// URL that carries x-amz-sdk-checksum-algorithm=SHA256 in its query string
// must cause the object to be stored with an x-amz-checksum-sha256 attribute,
// visible via HEAD when the caller requests ChecksumMode=ENABLED.
func TestPresignedPutWithChecksumSHA256(t *testing.T) {
	client := getS3Client(t)
	bucket := uniqueBucket()
	createBucket(t, client, bucket)
	defer cleanupBucket(t, client, bucket)

	key := "presigned-sha256.txt"
	body := []byte("hello seaweedfs checksum")
	sha256Sum := sha256.Sum256(body)
	expected := base64.StdEncoding.EncodeToString(sha256Sum[:])

	signedURL := presignPutURL(t, bucket, key, "SHA256")
	uploadViaPresignedURL(t, signedURL, body, nil)

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

// TestPresignedPutWithoutChecksumAlgorithm is a negative control: when the
// caller doesn't request a checksum algorithm, HEAD should not return one.
func TestPresignedPutWithoutChecksumAlgorithm(t *testing.T) {
	client := getS3Client(t)
	bucket := uniqueBucket()
	createBucket(t, client, bucket)
	defer cleanupBucket(t, client, bucket)

	key := "presigned-nosum.txt"
	body := []byte("no checksum requested")

	signedURL := presignPutURL(t, bucket, key, "")
	uploadViaPresignedURL(t, signedURL, body, nil)

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
