package delete

import (
	"bytes"
	"context"
	"fmt"
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

const (
	testEndpoint  = "http://localhost:8333"
	testAccessKey = "admin"
	testSecretKey = "admin"
	testRegion    = "us-east-1"
)

func getTestClient(t *testing.T) *s3.Client {
	cfg, err := config.LoadDefaultConfig(context.TODO(),
		config.WithRegion(testRegion),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(
			testAccessKey,
			testSecretKey,
			"",
		)),
	)
	require.NoError(t, err)

	client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(testEndpoint)
		o.UsePathStyle = true
	})

	return client
}

func createTestBucket(t *testing.T, client *s3.Client) string {
	bucketName := fmt.Sprintf("test-multi-delete-%d", time.Now().UnixNano())
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

	return bucketName
}

func cleanupBucket(t *testing.T, client *s3.Client, bucket string) {
	listResp, _ := client.ListObjectVersions(context.TODO(), &s3.ListObjectVersionsInput{
		Bucket: aws.String(bucket),
	})

	if listResp != nil {
		var objectsToDelete []types.ObjectIdentifier

		for _, version := range listResp.Versions {
			objectsToDelete = append(objectsToDelete, types.ObjectIdentifier{
				Key:       version.Key,
				VersionId: version.VersionId,
			})
		}

		for _, marker := range listResp.DeleteMarkers {
			objectsToDelete = append(objectsToDelete, types.ObjectIdentifier{
				Key:       marker.Key,
				VersionId: marker.VersionId,
			})
		}

		if len(objectsToDelete) > 0 {
			_, _ = client.DeleteObjects(context.TODO(), &s3.DeleteObjectsInput{
				Bucket: aws.String(bucket),
				Delete: &types.Delete{
					Objects: objectsToDelete,
					Quiet:   aws.Bool(false),
				},
			})
		}
	}

	_, _ = client.DeleteBucket(context.TODO(), &s3.DeleteBucketInput{
		Bucket: aws.String(bucket),
	})
}

func TestVersioningMultiObjectDelete(t *testing.T) {
	client := getTestClient(t)
	bucket := createTestBucket(t, client)
	defer cleanupBucket(t, client, bucket)

	key := "key"
	numVersions := 2
	var versionIds []string

	for i := 0; i < numVersions; i++ {
		content := fmt.Sprintf("content-%d", i)
		putResp, err := client.PutObject(context.TODO(), &s3.PutObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
			Body:   bytes.NewReader([]byte(content)),
		})
		require.NoError(t, err)
		require.NotNil(t, putResp.VersionId)
		versionIds = append(versionIds, *putResp.VersionId)
	}

	assert.Len(t, versionIds, 2)

	var objectsToDelete []types.ObjectIdentifier
	for _, vid := range versionIds {
		objectsToDelete = append(objectsToDelete, types.ObjectIdentifier{
			Key:       aws.String(key),
			VersionId: aws.String(vid),
		})
	}

	deleteResp, err := client.DeleteObjects(context.TODO(), &s3.DeleteObjectsInput{
		Bucket: aws.String(bucket),
		Delete: &types.Delete{
			Objects: objectsToDelete,
		},
	})
	require.NoError(t, err)
	t.Logf("Delete response: Deleted=%d, Errors=%d", len(deleteResp.Deleted), len(deleteResp.Errors))

	listResp, err := client.ListObjectVersions(context.TODO(), &s3.ListObjectVersionsInput{
		Bucket: aws.String(bucket),
	})
	require.NoError(t, err)

	if listResp.Versions != nil && len(listResp.Versions) > 0 {
		t.Errorf("❌ FAIL: Expected no versions, but found %d versions:", len(listResp.Versions))
		for _, v := range listResp.Versions {
			t.Logf("  - Key=%s, VersionId=%s, IsLatest=%v", *v.Key, *v.VersionId, v.IsLatest)
		}
	} else {
		t.Logf("✓ PASS: Versions correctly deleted")
	}
	assert.Nil(t, listResp.Versions, "Expected no versions after deletion")

	deleteResp2, err := client.DeleteObjects(context.TODO(), &s3.DeleteObjectsInput{
		Bucket: aws.String(bucket),
		Delete: &types.Delete{
			Objects: objectsToDelete,
		},
	})
	require.NoError(t, err)
	assert.Empty(t, deleteResp2.Errors, "Idempotent delete should not return errors")
}
