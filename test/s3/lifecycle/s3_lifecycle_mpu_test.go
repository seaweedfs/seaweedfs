// AbortIncompleteMultipartUpload integration scenario.
package lifecycle

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/stretchr/testify/require"
)

// putAbortMPULifecycle wires AbortIncompleteMultipartUpload on a prefix.
// AWS rejects DaysAfterInitiation < 1, so the test backdates the upload
// directory to age it past the threshold.
func putAbortMPULifecycle(t *testing.T, c *s3.Client, bucket, prefix string, days int32) {
	t.Helper()
	_, err := c.PutBucketLifecycleConfiguration(context.Background(), &s3.PutBucketLifecycleConfigurationInput{
		Bucket: aws.String(bucket),
		LifecycleConfiguration: &types.BucketLifecycleConfiguration{
			Rules: []types.LifecycleRule{
				{
					ID:     aws.String("abort-stale-mpu"),
					Status: types.ExpirationStatusEnabled,
					Filter: &types.LifecycleRuleFilter{Prefix: aws.String(prefix)},
					AbortIncompleteMultipartUpload: &types.AbortIncompleteMultipartUpload{
						DaysAfterInitiation: aws.Int32(days),
					},
				},
			},
		},
	})
	require.NoError(t, err)
}

// backdateUploadDir ages the .uploads/<uploadID>/ directory entry, which is
// what the lifecycle worker keys ABORT_MPU off of (the upload's init-time
// directory carries the destination key in Extended).
func backdateUploadDir(t *testing.T, fc filer_pb.SeaweedFilerClient, bucket, uploadID string, daysOld int) {
	t.Helper()
	dir := bucketsPath + "/" + bucket + "/.uploads"
	resp, err := fc.LookupDirectoryEntry(context.Background(), &filer_pb.LookupDirectoryEntryRequest{
		Directory: dir, Name: uploadID,
	})
	require.NoError(t, err, "lookup .uploads/%s", uploadID)
	require.NotNil(t, resp.Entry)
	require.NotNil(t, resp.Entry.Attributes)
	require.True(t, resp.Entry.IsDirectory, ".uploads/%s should be a directory", uploadID)

	resp.Entry.Attributes.Mtime = time.Now().Add(-time.Duration(daysOld) * 24 * time.Hour).Unix()
	resp.Entry.Attributes.MtimeNs = 0
	_, err = fc.UpdateEntry(context.Background(), &filer_pb.UpdateEntryRequest{
		Directory: dir, Entry: resp.Entry,
	})
	require.NoError(t, err)
}

// TestLifecycleAbortIncompleteMultipartUpload: an MPU left incomplete past
// the AbortIncompleteMultipartUpload.DaysAfterInitiation threshold must be
// aborted by the lifecycle worker. Exercises the lifecycleAbortMPU handler
// path which is otherwise unreachable from the prefix-based expiration
// tests (the routing keys off of `.uploads/<id>/` directory events, not
// regular object events).
func TestLifecycleAbortIncompleteMultipartUpload(t *testing.T) {
	c := s3Client(t)
	fc, fcClose := filerClient(t)
	defer fcClose()

	bucket := uniqueBucket("abort-mpu")
	mustCreateBucket(t, c, bucket)
	putAbortMPULifecycle(t, c, bucket, "uploads/", 1)

	// Initiate an MPU, upload a single part, then leave it. CompleteMultipart
	// would consume the upload; AbortMultipart would explicitly abort. We do
	// neither so the lifecycle worker is the path that ends it.
	const key = "uploads/big.bin"
	createOut, err := c.CreateMultipartUpload(context.Background(), &s3.CreateMultipartUploadInput{
		Bucket: aws.String(bucket), Key: aws.String(key),
	})
	require.NoError(t, err)
	uploadID := aws.ToString(createOut.UploadId)
	require.NotEmpty(t, uploadID)

	// Upload one part so the directory carries the right shape.
	_, err = c.UploadPart(context.Background(), &s3.UploadPartInput{
		Bucket:     aws.String(bucket),
		Key:        aws.String(key),
		UploadId:   aws.String(uploadID),
		PartNumber: aws.Int32(1),
		Body:       strings.NewReader("part 1 contents"),
	})
	require.NoError(t, err)

	// Sanity: ListMultipartUploads sees our pending upload before the
	// worker runs.
	listOut, err := c.ListMultipartUploads(context.Background(), &s3.ListMultipartUploadsInput{
		Bucket: aws.String(bucket),
	})
	require.NoError(t, err)
	var foundBefore bool
	for _, u := range listOut.Uploads {
		if aws.ToString(u.UploadId) == uploadID {
			foundBefore = true
			break
		}
	}
	require.True(t, foundBefore, "upload %s should be visible before the worker runs", uploadID)

	// Backdate the upload directory entry so the rule's DaysAfterInitiation
	// threshold is satisfied. The router walks .uploads/ and emits an
	// MPU-init event for each entry.
	backdateUploadDir(t, fc, bucket, uploadID, 30)

	out := runLifecycleShard(t)
	t.Logf("shell output:\n%s", out)

	// The upload must no longer appear in ListMultipartUploads.
	require.Eventuallyf(t, func() bool {
		listOut, err := c.ListMultipartUploads(context.Background(), &s3.ListMultipartUploadsInput{
			Bucket: aws.String(bucket),
		})
		if err != nil {
			return false
		}
		for _, u := range listOut.Uploads {
			if aws.ToString(u.UploadId) == uploadID {
				return false
			}
		}
		return true
	}, 30*time.Second, 500*time.Millisecond, "upload %s must be aborted by the lifecycle worker", uploadID)
}
