package s3tables

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/seaweedfs/seaweedfs/weed/s3api/s3tables"
)

// emptyObjectBucket deletes every object in bucket.
func emptyObjectBucket(t *testing.T, ctx context.Context, client *s3.Client, bucket string) {
	t.Helper()
	pages := s3.NewListObjectsV2Paginator(client, &s3.ListObjectsV2Input{Bucket: aws.String(bucket)})
	for pages.HasMorePages() {
		page, err := pages.NextPage(ctx)
		require.NoError(t, err)
		for _, o := range page.Contents {
			_, err := client.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: aws.String(bucket), Key: o.Key})
			require.NoError(t, err)
		}
	}
}

// The AWS SDK sends RenameTable as PUT /tables/<arn>/<ns>/<name>/rename. With
// no route for it the gateway ran it as a PutObject into an S3 bucket named
// "tables" and answered 2xx, so the rename reported success while the table
// kept its old name. The bucket is created up front so that failure shows as
// an object landing in it.
func TestRenameTableREST(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}
	ctx := context.Background()
	tables := NewS3TablesClient(sharedCluster.s3Endpoint, testRegion, testAccessKey, testSecretKey)
	objects := newObjectClient(t)

	const decoyBucket = "tables"
	if _, err := objects.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(decoyBucket)}); err != nil {
		require.Equal(t, "BucketAlreadyOwnedByYou", apiErrorCode(err), "create the %q bucket: %v", decoyBucket, err)
	}
	emptyObjectBucket(t, ctx, objects, decoyBucket)
	defer func() {
		emptyObjectBucket(t, ctx, objects, decoyBucket)
		_, _ = objects.DeleteBucket(ctx, &s3.DeleteBucketInput{Bucket: aws.String(decoyBucket)})
	}()

	created, err := tables.CreateTableBucket("rename-rest-"+randomString(8), nil)
	require.NoError(t, err)
	defer tables.DeleteTableBucket(created.ARN)
	bucketARN := created.ARN

	const namespace, otherNamespace = "rename_ns", "rename_other"
	for _, ns := range []string{namespace, otherNamespace} {
		_, err := tables.CreateNamespace(bucketARN, []string{ns})
		require.NoError(t, err)
		defer tables.DeleteNamespace(bucketARN, []string{ns})
	}

	// Each subtest owns its source table, so any one of them runs alone.
	createSource := func(t *testing.T, name string) string {
		resp, err := tables.CreateTable(bucketARN, []string{namespace}, name, "ICEBERG", nil, nil)
		require.NoError(t, err)
		return resp.VersionToken
	}

	t.Run("newName only", func(t *testing.T) {
		createSource(t, "src")
		defer tables.DeleteTable(bucketARN, []string{namespace}, "src")
		defer tables.DeleteTable(bucketARN, []string{namespace}, "dst")
		require.NoError(t, tables.RenameTable(bucketARN, []string{namespace}, "src", "", "dst", ""))

		got, err := tables.GetTable(bucketARN, []string{namespace}, "dst")
		require.NoError(t, err, "the renamed table must resolve under its new name")
		assert.Equal(t, "dst", got.Name)

		_, err = tables.GetTable(bucketARN, []string{namespace}, "src")
		require.Error(t, err)
		assert.Contains(t, err.Error(), s3tables.ErrCodeNoSuchTable)

		list, err := objects.ListObjectsV2(ctx, &s3.ListObjectsV2Input{Bucket: aws.String(decoyBucket)})
		require.NoError(t, err)
		assert.Empty(t, list.Contents, "the rename must not store anything in the %q bucket", decoyBucket)
	})

	t.Run("stale versionToken", func(t *testing.T) {
		token := createSource(t, "stale_src")
		defer tables.DeleteTable(bucketARN, []string{namespace}, "stale_src")
		defer tables.DeleteTable(bucketARN, []string{otherNamespace}, "stale_moved")
		err := tables.RenameTable(bucketARN, []string{namespace}, "stale_src", otherNamespace, "stale_moved", token+"-stale")
		require.Error(t, err)
		assert.Contains(t, err.Error(), s3tables.ErrCodeConflict)

		_, err = tables.GetTable(bucketARN, []string{namespace}, "stale_src")
		require.NoError(t, err, "a refused rename must leave the source in place")
	})

	t.Run("newNamespaceName and newName with the current versionToken", func(t *testing.T) {
		token := createSource(t, "tok_src")
		defer tables.DeleteTable(bucketARN, []string{namespace}, "tok_src")
		defer tables.DeleteTable(bucketARN, []string{otherNamespace}, "moved")
		require.NoError(t, tables.RenameTable(bucketARN, []string{namespace}, "tok_src", otherNamespace, "moved", token))

		got, err := tables.GetTable(bucketARN, []string{otherNamespace}, "moved")
		require.NoError(t, err)
		assert.Equal(t, "moved", got.Name)

		_, err = tables.GetTable(bucketARN, []string{namespace}, "tok_src")
		require.Error(t, err)
		assert.Contains(t, err.Error(), s3tables.ErrCodeNoSuchTable)
	})
}
