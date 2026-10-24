package example

import (
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestListMultipartUploadsInitiated verifies that ListMultipartUploads
// returns the Initiated timestamp for each in-progress upload, and that
// the timestamp is preserved across repeated listings rather than
// reflecting the time of the listing.
func TestListMultipartUploadsInitiated(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	cluster, err := startMiniCluster(t)
	require.NoError(t, err)
	defer cluster.Stop()

	bucket := createTestBucket(t, cluster, "test-list-mpu-initiated-")

	beforeInit := time.Now().UTC()
	createOut, err := cluster.s3Client.CreateMultipartUpload(&s3.CreateMultipartUploadInput{
		Bucket: aws.String(bucket),
		Key:    aws.String("unfinished.bin"),
	})
	require.NoError(t, err)
	afterInit := time.Now().UTC()

	listOut, err := cluster.s3Client.ListMultipartUploads(&s3.ListMultipartUploadsInput{
		Bucket: aws.String(bucket),
	})
	require.NoError(t, err)
	require.Len(t, listOut.Uploads, 1)

	upload := listOut.Uploads[0]
	assert.Equal(t, "unfinished.bin", aws.StringValue(upload.Key))
	assert.Equal(t, aws.StringValue(createOut.UploadId), aws.StringValue(upload.UploadId))
	require.NotNil(t, upload.Initiated, "Initiated timestamp must be populated")

	initiated := upload.Initiated.UTC()
	assert.False(t, initiated.Before(beforeInit.Add(-time.Second)), "Initiated %v is before upload creation %v", initiated, beforeInit)
	assert.False(t, initiated.After(afterInit.Add(time.Second)), "Initiated %v is after upload creation %v", initiated, afterInit)

	time.Sleep(2 * time.Second)
	listOut2, err := cluster.s3Client.ListMultipartUploads(&s3.ListMultipartUploadsInput{
		Bucket: aws.String(bucket),
	})
	require.NoError(t, err)
	require.Len(t, listOut2.Uploads, 1)
	require.NotNil(t, listOut2.Uploads[0].Initiated, "Initiated timestamp must be populated on repeated listing")
	assert.True(t, listOut2.Uploads[0].Initiated.Equal(*upload.Initiated), "Initiated must be preserved across listings, got %v then %v", *upload.Initiated, *listOut2.Uploads[0].Initiated)

	_, err = cluster.s3Client.AbortMultipartUpload(&s3.AbortMultipartUploadInput{
		Bucket:   aws.String(bucket),
		Key:      aws.String("unfinished.bin"),
		UploadId: createOut.UploadId,
	})
	require.NoError(t, err)
}
