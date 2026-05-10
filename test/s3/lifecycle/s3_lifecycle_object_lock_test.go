// Object Lock + lifecycle integration scenario.
package lifecycle

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/stretchr/testify/require"
)

// TestLifecycleSkipsObjectLockedObjects: an object under Object Lock
// retention must NOT be expired by the lifecycle worker. The handler's
// enforceObjectLockProtections check (s3api_internal_lifecycle.go:47)
// returns an error which the dispatcher classifies as
// SKIPPED_OBJECT_LOCK. Pinning the integration end-to-end so a
// regression in either the lock state lookup or the dispatcher's
// outcome handling is caught.
//
// Companion positive control: a second object in the same bucket
// without retention is expired by the same sweep — proves the rule
// is firing AND that the lock check is selective, not a blanket skip.
func TestLifecycleSkipsObjectLockedObjects(t *testing.T) {
	c := s3Client(t)
	fc, fcClose := filerClient(t)
	defer fcClose()

	bucket := uniqueBucket("objlock")
	// Object Lock requires the bucket to be created with the flag set;
	// it can't be enabled retroactively on an existing bucket. Use the
	// raw API rather than mustCreateBucket here so we can pass the
	// ObjectLockEnabledForBucket option.
	_, err := c.CreateBucket(context.Background(), &s3.CreateBucketInput{
		Bucket:                     aws.String(bucket),
		ObjectLockEnabledForBucket: aws.Bool(true),
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		// Best-effort cleanup mirrors mustCreateBucket; locked objects
		// may resist deletion but the test process continues regardless.
		c.DeleteBucketLifecycle(context.Background(), &s3.DeleteBucketLifecycleInput{Bucket: aws.String(bucket)})
		listOut, _ := c.ListObjectVersions(context.Background(), &s3.ListObjectVersionsInput{Bucket: aws.String(bucket)})
		if listOut != nil {
			for _, v := range listOut.Versions {
				// Bypass governance retention to free the bucket for delete.
				c.DeleteObject(context.Background(), &s3.DeleteObjectInput{
					Bucket:                    aws.String(bucket),
					Key:                       v.Key,
					VersionId:                 v.VersionId,
					BypassGovernanceRetention: aws.Bool(true),
				})
			}
			for _, m := range listOut.DeleteMarkers {
				c.DeleteObject(context.Background(), &s3.DeleteObjectInput{
					Bucket: aws.String(bucket), Key: m.Key, VersionId: m.VersionId,
				})
			}
		}
		c.DeleteBucket(context.Background(), &s3.DeleteBucketInput{Bucket: aws.String(bucket)})
	})

	putExpirationLifecycle(t, c, bucket, "lock/", 1)

	const lockedKey = "lock/protected.txt"
	const freeKey = "lock/free.txt"

	// Locked object: PUT with GOVERNANCE retention until 1h from now.
	lockedPut, err := c.PutObject(context.Background(), &s3.PutObjectInput{
		Bucket:                    aws.String(bucket),
		Key:                       aws.String(lockedKey),
		Body:                      strings.NewReader("locked"),
		ObjectLockMode:            types.ObjectLockModeGovernance,
		ObjectLockRetainUntilDate: aws.Time(time.Now().Add(time.Hour)),
	})
	require.NoError(t, err, "PUT with retention must succeed on a lock-enabled bucket")
	require.NotEmpty(t, aws.ToString(lockedPut.VersionId))

	// Free object: PUT without retention.
	putObject(t, c, bucket, freeKey, "free")

	// Backdate both so they would otherwise both expire under the
	// 1-day rule. The lock check is what distinguishes them.
	backdateMtime(t, fc, bucket, lockedKey, 30)
	backdateMtime(t, fc, bucket, freeKey, 30)

	out := runLifecycleShard(t)
	t.Logf("shell output:\n%s", out)

	// Free object expires (positive control).
	require.Eventuallyf(t, func() bool {
		_, err := c.HeadObject(context.Background(), &s3.HeadObjectInput{
			Bucket: aws.String(bucket), Key: aws.String(freeKey),
		})
		return isS3NotFound(err)
	}, 30*time.Second, 500*time.Millisecond, "unlocked object must expire (control)")

	// Locked object survives — Object Lock retention overrides the
	// lifecycle rule. HEAD without versionId returns the still-current
	// object data, not a 404.
	_, err = c.HeadObject(context.Background(), &s3.HeadObjectInput{
		Bucket: aws.String(bucket), Key: aws.String(lockedKey),
	})
	require.NoError(t, err, "object under GOVERNANCE retention must survive the sweep")
}

