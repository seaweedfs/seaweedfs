// Package sdkv2routing_test exercises route disambiguation between the
// regular S3 API and the S3 Tables REST API on top-level paths the two
// share (/buckets, /get-table). The bug it pins:
//
// When a user has an S3 bucket named "buckets" (or "get-table"), a
// path-style ListObjectsV2 request sent by AWS SDK V2 / Hadoop s3a /
// Spark would be routed to the S3 Tables ListTableBuckets handler and
// receive a JSON body. AWS SDK V2 then fails XML parsing with
// "Unexpected character '{' (code 123) in prolog".
//
// These tests use the real AWS SDK V2 for Go, so the SDK's own
// deserializer is the assertion: if the server returns the wrong
// content type, the SDK errors out before any test assertion runs.
package sdkv2routing_test

import (
	"context"
	"io"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stretchr/testify/require"
)

const (
	defaultEndpoint  = "http://127.0.0.1:8333"
	defaultAccessKey = "some_access_key1"
	defaultSecretKey = "some_secret_key1"
	defaultRegion    = "us-east-1"
)

func getS3Client(t *testing.T) *s3.Client {
	t.Helper()

	endpoint := os.Getenv("S3_ENDPOINT")
	if endpoint == "" {
		endpoint = defaultEndpoint
	}
	accessKey := os.Getenv("AWS_ACCESS_KEY_ID")
	if accessKey == "" {
		accessKey = defaultAccessKey
	}
	secretKey := os.Getenv("AWS_SECRET_ACCESS_KEY")
	if secretKey == "" {
		secretKey = defaultSecretKey
	}
	region := os.Getenv("AWS_REGION")
	if region == "" {
		region = defaultRegion
	}

	cfg, err := config.LoadDefaultConfig(context.TODO(),
		config.WithRegion(region),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(accessKey, secretKey, "")),
		config.WithEndpointResolverWithOptions(aws.EndpointResolverWithOptionsFunc(
			func(service, region string, options ...interface{}) (aws.Endpoint, error) {
				return aws.Endpoint{
					URL:               endpoint,
					SigningRegion:     defaultRegion,
					HostnameImmutable: true,
				}, nil
			})),
	)
	require.NoError(t, err)

	return s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.UsePathStyle = true
	})
}

// ensureBucket creates bucket if it doesn't already exist. It tolerates
// BucketAlreadyOwnedByYou / BucketAlreadyExists so the tests are
// idempotent across local re-runs.
func ensureBucket(t *testing.T, ctx context.Context, client *s3.Client, bucket string) {
	t.Helper()
	_, err := client.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(bucket)})
	if err == nil {
		return
	}
	msg := err.Error()
	if strings.Contains(msg, "BucketAlreadyOwnedByYou") || strings.Contains(msg, "BucketAlreadyExists") {
		return
	}
	t.Fatalf("CreateBucket(%q) failed: %v", bucket, err)
}

// deleteBucket best-effort cleans up a bucket and any objects in it.
// Test does not fail if cleanup fails — the next run is idempotent.
func deleteBucket(ctx context.Context, client *s3.Client, bucket string) {
	paginator := s3.NewListObjectsV2Paginator(client, &s3.ListObjectsV2Input{Bucket: aws.String(bucket)})
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			break
		}
		for _, obj := range page.Contents {
			client.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: aws.String(bucket), Key: obj.Key})
		}
	}
	client.DeleteBucket(ctx, &s3.DeleteBucketInput{Bucket: aws.String(bucket)})
}

// TestListObjectsV2_OnBucketNamedBuckets is the direct reproducer for
// issue #9559: Spark / Hadoop s3a does a ListObjectsV2 against bucket
// "buckets" via AWS SDK V2, which fails with
// "Could not parse XML response. ... Unexpected character '{' (code 123)
// in prolog" when SeaweedFS routes the request to the JSON-returning
// ListTableBuckets handler. The SDK's response deserializer is the
// real assertion here — a JSON body produces an SDK error before we
// reach require.NoError.
func TestListObjectsV2_OnBucketNamedBuckets(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	client := getS3Client(t)

	const bucket = "buckets"
	ensureBucket(t, ctx, client, bucket)
	t.Cleanup(func() { deleteBucket(ctx, client, bucket) })

	out, err := client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
		Bucket: aws.String(bucket),
		Prefix: aws.String("logs/"),
	})
	require.NoError(t, err, "AWS SDK V2 must parse the response as XML")
	require.NotNil(t, out)
}

// TestPutGetObject_OnBucketNamedBuckets exercises the full read/write
// round-trip on the colliding bucket name. PutObject and GetObject go
// through different routes than ListObjectsV2, and verifying them
// guards against future regressions that re-route only some verbs.
func TestPutGetObject_OnBucketNamedBuckets(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	client := getS3Client(t)

	const bucket = "buckets"
	const key = "logs/hello.txt"
	const body = "hello from issue 9559"

	ensureBucket(t, ctx, client, bucket)
	t.Cleanup(func() { deleteBucket(ctx, client, bucket) })

	_, err := client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
		Body:   strings.NewReader(body),
	})
	require.NoError(t, err)

	out, err := client.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)})
	require.NoError(t, err)
	defer out.Body.Close()

	got, err := io.ReadAll(out.Body)
	require.NoError(t, err)
	require.Equal(t, body, string(got))
}

// TestListObjectsV2_OnBucketNamedGetTable covers the second colliding
// path. The S3 Tables GET /get-table endpoint shares its path with a
// bucket literally named "get-table", which is a legal S3 bucket name.
func TestListObjectsV2_OnBucketNamedGetTable(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	client := getS3Client(t)

	const bucket = "get-table"
	ensureBucket(t, ctx, client, bucket)
	t.Cleanup(func() { deleteBucket(ctx, client, bucket) })

	out, err := client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{Bucket: aws.String(bucket)})
	require.NoError(t, err)
	require.NotNil(t, out)
}

// TestCreateAndListBuckets_ServiceLevel verifies the SDK's service-level
// ListBuckets still parses as XML when a bucket named "buckets" exists.
// ListBuckets goes through GET / (root) which is unaffected by the
// /buckets route collision — this is a guard against the matcher
// accidentally widening to top-level paths.
func TestCreateAndListBuckets_ServiceLevel(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	client := getS3Client(t)

	const bucket = "buckets"
	ensureBucket(t, ctx, client, bucket)
	t.Cleanup(func() { deleteBucket(ctx, client, bucket) })

	out, err := client.ListBuckets(ctx, &s3.ListBucketsInput{})
	require.NoError(t, err)
	require.NotNil(t, out)

	found := false
	for _, b := range out.Buckets {
		if aws.ToString(b.Name) == bucket {
			found = true
			break
		}
	}
	require.True(t, found, "bucket %q must appear in service-level ListBuckets", bucket)
}

