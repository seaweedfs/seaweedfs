package s3tables

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newObjectClient(t *testing.T) *s3.Client {
	t.Helper()
	cfg, err := config.LoadDefaultConfig(context.Background(),
		config.WithRegion(testRegion),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(testAccessKey, testSecretKey, "")),
	)
	require.NoError(t, err)
	return s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(sharedCluster.s3Endpoint)
		o.UsePathStyle = true
	})
}

func apiErrorCode(err error) string {
	var apiErr smithy.APIError
	if errors.As(err, &apiErr) {
		return apiErr.ErrorCode()
	}
	return ""
}

// The external-volume verification object round-trips at a table bucket's
// root the way the check uses it: PUT, HEAD, GET, list, DELETE. Any other key
// at the root is still refused.
func TestTableBucketExternalVolumeVerificationRoundTrip(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}
	ctx := context.Background()
	tables := NewS3TablesClient(sharedCluster.s3Endpoint, testRegion, testAccessKey, testSecretKey)
	bucket := "verify-" + randomString(8)
	created, err := tables.CreateTableBucket(bucket, nil)
	require.NoError(t, err)
	defer tables.DeleteTableBucket(created.ARN)
	client := newObjectClient(t)

	body := []byte("verification payload!") // 21 bytes
	key := fmt.Sprintf("verify_%d_%d", time.Now().UnixMilli(), 1081846483)

	// the control: an ordinary key at the root is refused
	_, err = client.PutObject(ctx, &s3.PutObjectInput{Bucket: aws.String(bucket), Key: aws.String("notes.parquet"), Body: bytes.NewReader(body)})
	require.Error(t, err)
	assert.Equal(t, "AccessDenied", apiErrorCode(err))

	_, err = client.PutObject(ctx, &s3.PutObjectInput{Bucket: aws.String(bucket), Key: aws.String(key), Body: bytes.NewReader(body), ContentLength: aws.Int64(int64(len(body)))})
	require.NoError(t, err)

	head, err := client.HeadObject(ctx, &s3.HeadObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)})
	require.NoError(t, err)
	assert.Equal(t, int64(len(body)), aws.ToInt64(head.ContentLength))

	got, err := client.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)})
	require.NoError(t, err)
	data, err := io.ReadAll(got.Body)
	got.Body.Close()
	require.NoError(t, err)
	assert.Equal(t, body, data)

	list, err := client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{Bucket: aws.String(bucket), Prefix: aws.String("verify_")})
	require.NoError(t, err)
	var keys []string
	for _, o := range list.Contents {
		keys = append(keys, aws.ToString(o.Key))
	}
	assert.Equal(t, []string{key}, keys)

	_, err = client.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)})
	require.NoError(t, err)
	_, err = client.HeadObject(ctx, &s3.HeadObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)})
	require.Error(t, err, "the verification object must be gone after DELETE")
}

// A namespace can carry a name of the verification key's shape. The key
// then names the namespace's directory: a PUT or DELETE through it must not
// replace or remove the namespace.
func TestTableBucketVerificationKeyLeavesNamespaceAlone(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}
	ctx := context.Background()
	tables := NewS3TablesClient(sharedCluster.s3Endpoint, testRegion, testAccessKey, testSecretKey)
	bucket := "verify-ns-" + randomString(8)
	created, err := tables.CreateTableBucket(bucket, nil)
	require.NoError(t, err)
	defer tables.DeleteTableBucket(created.ARN)
	client := newObjectClient(t)
	body := []byte("verification payload!")

	listed := func(name string) bool {
		resp, err := tables.ListNamespaces(created.ARN, "", "", 0)
		require.NoError(t, err)
		for _, ns := range resp.Namespaces {
			if len(ns.Namespace) == 1 && ns.Namespace[0] == name {
				return true
			}
		}
		return false
	}
	for i, op := range []string{"PUT", "DELETE"} {
		t.Run(op, func(t *testing.T) {
			name := fmt.Sprintf("verify_%d_%d", time.Now().UnixMilli(), 7+i)
			_, err := tables.CreateNamespace(created.ARN, []string{name})
			require.NoError(t, err)
			defer tables.DeleteNamespace(created.ARN, []string{name})
			require.True(t, listed(name), "setup: the namespace is listed")

			if op == "PUT" {
				_, err = client.PutObject(ctx, &s3.PutObjectInput{Bucket: aws.String(bucket), Key: aws.String(name), Body: bytes.NewReader(body), ContentLength: aws.Int64(int64(len(body)))})
				assert.Error(t, err, "a PUT over a namespace must be refused")
			} else {
				_, err = client.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: aws.String(bucket), Key: aws.String(name)})
				assert.Error(t, err, "a DELETE of a namespace must be refused")
			}
			_, err = tables.GetNamespace(created.ARN, []string{name})
			assert.NoError(t, err, "the namespace survives the %s", op)
			assert.True(t, listed(name), "the namespace is still listed after the %s", op)
		})
	}
}
