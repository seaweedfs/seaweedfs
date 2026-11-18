package acl

import (
	"context"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsmiddleware "github.com/aws/aws-sdk-go-v2/aws/middleware"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	smithyhttp "github.com/aws/smithy-go/transport/http"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func createOwnershipTestBucket(t *testing.T, client *s3.Client) string {
	bucketName := "test-ownership-" + strings.ToLower(strings.ReplaceAll(time.Now().Format("2006-01-02-15-04-05.000"), ":", "-"))
	_, err := client.CreateBucket(context.TODO(), &s3.CreateBucketInput{
		Bucket: aws.String(bucketName),
	})
	require.NoError(t, err)
	return bucketName
}

func getObjectOwnership(t *testing.T, client *s3.Client, bucketName string) types.ObjectOwnership {
	t.Helper()
	resp, err := client.GetBucketOwnershipControls(context.TODO(), &s3.GetBucketOwnershipControlsInput{
		Bucket: aws.String(bucketName),
	})
	require.NoError(t, err)
	require.NotNil(t, resp.OwnershipControls)
	require.Len(t, resp.OwnershipControls.Rules, 1)
	return resp.OwnershipControls.Rules[0].ObjectOwnership
}

func putObjectOwnership(t *testing.T, client *s3.Client, bucketName string, ownership types.ObjectOwnership) {
	t.Helper()
	_, err := client.PutBucketOwnershipControls(context.TODO(), &s3.PutBucketOwnershipControlsInput{
		Bucket: aws.String(bucketName),
		OwnershipControls: &types.OwnershipControls{
			Rules: []types.OwnershipControlsRule{{ObjectOwnership: ownership}},
		},
	})
	require.NoError(t, err)
}

func TestGetBucketOwnershipControlsDefault(t *testing.T) {
	client := getS3Client(t)
	bucketName := createOwnershipTestBucket(t, client)
	defer cleanupTestBucket(t, client, bucketName)

	assert.Equal(t, types.ObjectOwnershipBucketOwnerEnforced, getObjectOwnership(t, client, bucketName))
}

func TestBucketOwnershipControlsLifecycle(t *testing.T) {
	client := getS3Client(t)
	bucketName := createOwnershipTestBucket(t, client)
	defer cleanupTestBucket(t, client, bucketName)

	putObjectOwnership(t, client, bucketName, types.ObjectOwnershipObjectWriter)
	assert.Equal(t, types.ObjectOwnershipObjectWriter, getObjectOwnership(t, client, bucketName))

	putObjectOwnership(t, client, bucketName, types.ObjectOwnershipBucketOwnerEnforced)
	assert.Equal(t, types.ObjectOwnershipBucketOwnerEnforced, getObjectOwnership(t, client, bucketName))

	deleted, err := client.DeleteBucketOwnershipControls(context.TODO(), &s3.DeleteBucketOwnershipControlsInput{
		Bucket: aws.String(bucketName),
	})
	require.NoError(t, err)
	raw, ok := awsmiddleware.GetRawResponse(deleted.ResultMetadata).(*smithyhttp.Response)
	require.True(t, ok)
	assert.Equal(t, http.StatusNoContent, raw.StatusCode)

	_, err = client.GetBucketOwnershipControls(context.TODO(), &s3.GetBucketOwnershipControlsInput{
		Bucket: aws.String(bucketName),
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "OwnershipControlsNotFoundError")

	putObjectOwnership(t, client, bucketName, types.ObjectOwnershipBucketOwnerPreferred)
	assert.Equal(t, types.ObjectOwnershipBucketOwnerPreferred, getObjectOwnership(t, client, bucketName))
}

// A bucket with nothing stored already reports BucketOwnerEnforced, so this put has
// to persist anyway or the delete below finds nothing to remove.
func TestPutBucketOwnershipControlsDefaultOnNewBucket(t *testing.T) {
	client := getS3Client(t)
	bucketName := createOwnershipTestBucket(t, client)
	defer cleanupTestBucket(t, client, bucketName)

	putObjectOwnership(t, client, bucketName, types.ObjectOwnershipBucketOwnerEnforced)

	_, err := client.DeleteBucketOwnershipControls(context.TODO(), &s3.DeleteBucketOwnershipControlsInput{
		Bucket: aws.String(bucketName),
	})
	require.NoError(t, err)

	_, err = client.DeleteBucketOwnershipControls(context.TODO(), &s3.DeleteBucketOwnershipControlsInput{
		Bucket: aws.String(bucketName),
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "OwnershipControlsNotFound")
}
