package delete

import (
	"bytes"
	"context"
	"errors"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConditionalDeleteIfMatchOnLatestVersion(t *testing.T) {
	client := getTestClient(t)
	bucket := createTestBucket(t, client)
	defer cleanupBucket(t, client, bucket)

	key := "conditional-delete.txt"
	putResp, err := client.PutObject(context.TODO(), &s3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
		Body:   bytes.NewReader([]byte("versioned body")),
	})
	require.NoError(t, err)
	require.NotNil(t, putResp.ETag)

	_, err = client.DeleteObject(context.TODO(), &s3.DeleteObjectInput{
		Bucket:  aws.String(bucket),
		Key:     aws.String(key),
		IfMatch: aws.String(`"not-the-current-etag"`),
	})
	require.Error(t, err, "DeleteObject should reject a mismatched If-Match header")

	var apiErr smithy.APIError
	if assert.True(t, errors.As(err, &apiErr), "Expected smithy API error for conditional delete") {
		assert.Equal(t, "PreconditionFailed", apiErr.ErrorCode())
	}

	_, err = client.HeadObject(context.TODO(), &s3.HeadObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	require.NoError(t, err, "Object should remain current after a failed conditional delete")

	deleteResp, err := client.DeleteObject(context.TODO(), &s3.DeleteObjectInput{
		Bucket:  aws.String(bucket),
		Key:     aws.String(key),
		IfMatch: putResp.ETag,
	})
	require.NoError(t, err)
	require.NotNil(t, deleteResp.DeleteMarker)
	assert.True(t, *deleteResp.DeleteMarker, "Successful conditional delete on a versioned bucket should create a delete marker")
	require.NotNil(t, deleteResp.VersionId)

	_, err = client.HeadObject(context.TODO(), &s3.HeadObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	require.Error(t, err, "Delete marker should hide the current object after a successful conditional delete")
}

func TestConditionalMultiDeletePerObjectETag(t *testing.T) {
	client := getTestClient(t)
	bucket := createTestBucket(t, client)
	defer cleanupBucket(t, client, bucket)

	okKey := "delete-ok.txt"
	failKey := "delete-fail.txt"

	okPutResp, err := client.PutObject(context.TODO(), &s3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(okKey),
		Body:   bytes.NewReader([]byte("delete me")),
	})
	require.NoError(t, err)
	require.NotNil(t, okPutResp.ETag)

	_, err = client.PutObject(context.TODO(), &s3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(failKey),
		Body:   bytes.NewReader([]byte("keep me")),
	})
	require.NoError(t, err)

	deleteResp, err := client.DeleteObjects(context.TODO(), &s3.DeleteObjectsInput{
		Bucket: aws.String(bucket),
		Delete: &types.Delete{
			Objects: []types.ObjectIdentifier{
				{
					Key:  aws.String(okKey),
					ETag: okPutResp.ETag,
				},
				{
					Key:  aws.String(failKey),
					ETag: aws.String(`"mismatched-etag"`),
				},
			},
		},
	})
	require.NoError(t, err)
	require.Len(t, deleteResp.Deleted, 1, "One object should satisfy its ETag precondition")
	require.Len(t, deleteResp.Errors, 1, "One object should report a precondition failure")
	deletedKeys := make([]string, 0, len(deleteResp.Deleted))
	for _, deleted := range deleteResp.Deleted {
		deletedKeys = append(deletedKeys, aws.ToString(deleted.Key))
	}
	assert.Contains(t, deletedKeys, okKey)

	var matchedError *types.Error
	for i := range deleteResp.Errors {
		if aws.ToString(deleteResp.Errors[i].Key) == failKey {
			matchedError = &deleteResp.Errors[i]
			break
		}
	}
	if assert.NotNil(t, matchedError, "Expected error entry for failed key") {
		assert.Equal(t, "PreconditionFailed", aws.ToString(matchedError.Code))
	}

	_, err = client.HeadObject(context.TODO(), &s3.HeadObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(okKey),
	})
	require.Error(t, err, "Successfully deleted key should no longer be current")

	_, err = client.HeadObject(context.TODO(), &s3.HeadObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(failKey),
	})
	require.NoError(t, err, "Object with mismatched ETag should remain untouched")
}
