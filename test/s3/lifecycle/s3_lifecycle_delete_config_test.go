// Operator-removes-lifecycle-config integration scenario.
package lifecycle

import (
	"context"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stretchr/testify/require"
)

// TestLifecycleDeleteBucketLifecycleStopsDispatching: when an operator
// runs DeleteBucketLifecycle, subsequent sweeps must NOT continue
// expiring objects under the now-removed rule. The worker reads each
// bucket's XML config from filer state on every run; if it cached the
// previous config across PutBucketLifecycleConfiguration → DeleteBucket
// Lifecycle the bucket would keep losing objects.
//
// Setup: positive control (rule active, backdated obj expires), then
// DeleteBucketLifecycle, then PUT + backdate a fresh object, run the
// worker again. The fresh object must remain — no rule, no dispatch.
func TestLifecycleDeleteBucketLifecycleStopsDispatching(t *testing.T) {
	c := s3Client(t)
	fc, fcClose := filerClient(t)
	defer fcClose()

	bucket := uniqueBucket("delete-config")
	mustCreateBucket(t, c, bucket)
	putExpirationLifecycle(t, c, bucket, "x/", 1)

	const firstKey = "x/initial.txt"
	putObject(t, c, bucket, firstKey, "first")
	backdateMtime(t, fc, bucket, firstKey, 30)

	out := runLifecycleShard(t)
	t.Logf("sweep 1 output (rule active):\n%s", out)
	require.Eventuallyf(t, func() bool {
		_, err := c.HeadObject(context.Background(), &s3.HeadObjectInput{
			Bucket: aws.String(bucket), Key: aws.String(firstKey),
		})
		return isS3NotFound(err)
	}, 30*time.Second, 500*time.Millisecond, "positive control: %s should expire while rule is active", firstKey)

	// Now remove the rule. The worker must observe the removal on the
	// next sweep.
	_, err := c.DeleteBucketLifecycle(context.Background(), &s3.DeleteBucketLifecycleInput{
		Bucket: aws.String(bucket),
	})
	require.NoError(t, err)

	const survivorKey = "x/post-delete.txt"
	putObject(t, c, bucket, survivorKey, "should-stay")
	backdateMtime(t, fc, bucket, survivorKey, 30)

	out = runLifecycleShard(t)
	t.Logf("sweep 2 output (rule deleted):\n%s", out)

	// Wait the same window; survivor MUST still exist after the worker
	// has had every opportunity to (incorrectly) act on it.
	time.Sleep(2 * time.Second)
	_, err = c.HeadObject(context.Background(), &s3.HeadObjectInput{
		Bucket: aws.String(bucket), Key: aws.String(survivorKey),
	})
	require.NoError(t, err, "after DeleteBucketLifecycle, sweeps must not expire %s", survivorKey)
}
