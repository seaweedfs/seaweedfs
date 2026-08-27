package retention

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// A key ending in "/" is stored as the filer directory rather than as an object
// beside it, and is deleted the unversioned way. Object Lock still covers it: the
// gateway lists it as an object and serves retention set on it.
func TestObjectLockDirectoryMarker(t *testing.T) {
	client := getS3Client(t)
	bucketName := getNewBucketName()

	createBucketWithObjectLock(t, client, bucketName)
	defer deleteBucket(t, client, bucketName)

	retainUntil := time.Now().Add(24 * time.Hour)

	t.Run("retention headers are honored, not dropped", func(t *testing.T) {
		key := "records/evidence/"
		_, err := client.PutObject(context.TODO(), &s3.PutObjectInput{
			Bucket:                    aws.String(bucketName),
			Key:                       aws.String(key),
			Body:                      strings.NewReader("marker"),
			ObjectLockMode:            types.ObjectLockModeCompliance,
			ObjectLockRetainUntilDate: aws.Time(retainUntil),
		})
		require.NoError(t, err)

		_, err = client.DeleteObject(context.TODO(), &s3.DeleteObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(key),
		})
		require.Error(t, err, "a retained marker must not be deletable")

		_, err = client.HeadObject(context.TODO(), &s3.HeadObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(key),
		})
		assert.NoError(t, err, "the marker must still be there after the refused delete")
	})

	t.Run("retention set through PutObjectRetention is honored", func(t *testing.T) {
		key := "records/ledger/"
		_, err := client.PutObject(context.TODO(), &s3.PutObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(key),
			Body:   strings.NewReader("marker"),
		})
		require.NoError(t, err)

		_, err = client.PutObjectRetention(context.TODO(), &s3.PutObjectRetentionInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(key),
			Retention: &types.ObjectLockRetention{
				Mode:            types.ObjectLockRetentionModeCompliance,
				RetainUntilDate: aws.Time(retainUntil),
			},
		})
		require.NoError(t, err)

		_, err = client.DeleteObject(context.TODO(), &s3.DeleteObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(key),
		})
		require.Error(t, err, "retention the gateway serves back must also block the delete")
	})

	t.Run("multi-object delete is refused too", func(t *testing.T) {
		key := "records/batch/"
		_, err := client.PutObject(context.TODO(), &s3.PutObjectInput{
			Bucket:                    aws.String(bucketName),
			Key:                       aws.String(key),
			Body:                      strings.NewReader("marker"),
			ObjectLockMode:            types.ObjectLockModeCompliance,
			ObjectLockRetainUntilDate: aws.Time(retainUntil),
		})
		require.NoError(t, err)

		resp, err := client.DeleteObjects(context.TODO(), &s3.DeleteObjectsInput{
			Bucket: aws.String(bucketName),
			Delete: &types.Delete{Objects: []types.ObjectIdentifier{{Key: aws.String(key)}}},
		})
		require.NoError(t, err)
		assert.Empty(t, resp.Deleted, "a retained marker must not be reported deleted")
		assert.Len(t, resp.Errors, 1)

		_, err = client.HeadObject(context.TODO(), &s3.HeadObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(key),
		})
		assert.NoError(t, err, "the marker must survive the batch delete")
	})

	t.Run("invalid lock headers are rejected, as on a regular key", func(t *testing.T) {
		_, err := client.PutObject(context.TODO(), &s3.PutObjectInput{
			Bucket:                    aws.String(bucketName),
			Key:                       aws.String("records/bad-mode/"),
			Body:                      strings.NewReader("marker"),
			ObjectLockMode:            "INVALID_MODE",
			ObjectLockRetainUntilDate: aws.Time(retainUntil),
		})
		require.Error(t, err)

		_, err = client.PutObject(context.TODO(), &s3.PutObjectInput{
			Bucket:         aws.String(bucketName),
			Key:            aws.String("records/no-date/"),
			Body:           strings.NewReader("marker"),
			ObjectLockMode: types.ObjectLockModeGovernance,
		})
		require.Error(t, err)
	})

	t.Run("an unretained marker still deletes", func(t *testing.T) {
		key := "records/plain/"
		_, err := client.PutObject(context.TODO(), &s3.PutObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(key),
			Body:   strings.NewReader("marker"),
		})
		require.NoError(t, err)

		_, err = client.DeleteObject(context.TODO(), &s3.DeleteObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(key),
		})
		require.NoError(t, err)
	})
}
