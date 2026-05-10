// Multiple-bucket integration scenario.
package lifecycle

import (
	"context"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stretchr/testify/require"
)

// TestLifecycleMultipleBucketsInOneSweep: a single shell-driven shard
// sweep must process every bucket carrying lifecycle config, not just
// the first one alphabetically. Pinned because the scheduler iterates
// the buckets directory and a regression that returns early after the
// first match would silently disable lifecycle for every later bucket.
//
// Two buckets, each with its own 1-day prefix-expiration rule and one
// backdated object. After the worker runs, both objects must be gone.
func TestLifecycleMultipleBucketsInOneSweep(t *testing.T) {
	c := s3Client(t)
	fc, fcClose := filerClient(t)
	defer fcClose()

	bucketA := uniqueBucket("multi-a")
	bucketB := uniqueBucket("multi-b")
	mustCreateBucket(t, c, bucketA)
	mustCreateBucket(t, c, bucketB)

	putExpirationLifecycle(t, c, bucketA, "exp/", 1)
	putExpirationLifecycle(t, c, bucketB, "exp/", 1)

	const keyA = "exp/a.txt"
	const keyB = "exp/b.txt"
	putObject(t, c, bucketA, keyA, "a")
	putObject(t, c, bucketB, keyB, "b")
	backdateMtime(t, fc, bucketA, keyA, 30)
	backdateMtime(t, fc, bucketB, keyB, 30)

	out := runLifecycleShard(t)
	t.Logf("shell output:\n%s", out)

	// Both buckets must have their objects expired in this single sweep.
	for _, c2 := range []struct {
		bucket, key string
	}{
		{bucketA, keyA},
		{bucketB, keyB},
	} {
		c2 := c2
		require.Eventuallyf(t, func() bool {
			_, err := c.HeadObject(context.Background(), &s3.HeadObjectInput{
				Bucket: aws.String(c2.bucket), Key: aws.String(c2.key),
			})
			return err != nil
		}, 30*time.Second, 500*time.Millisecond,
			"%s/%s must be expired by the multi-bucket sweep", c2.bucket, c2.key)
	}
}
