// Bootstrap-walk integration scenario: lifecycle config added AFTER the
// objects exist must still expire them on the first sweep.
package lifecycle

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stretchr/testify/require"
)

// TestLifecycleBootstrapWalkOnExistingObjects: PUT a batch of objects
// FIRST, backdate them, THEN configure the lifecycle rule, THEN run
// the worker. The worker must discover the pre-existing objects via
// the bootstrap walk (BucketBootstrapper) — there were no meta-log
// events the reader could have observed because the objects predate
// the rule.
//
// Production scenario: an operator enables lifecycle on a bucket that
// already holds a million objects from before the policy. Without a
// bootstrap walk, only NEW writes would ever match; the existing
// content would never expire.
func TestLifecycleBootstrapWalkOnExistingObjects(t *testing.T) {
	c := s3Client(t)
	fc, fcClose := filerClient(t)
	defer fcClose()

	bucket := uniqueBucket("bootstrap")
	mustCreateBucket(t, c, bucket)

	// 5 objects so the test exercises the walker iterating multiple
	// entries in the same bucket directory. PUT FIRST, before any
	// lifecycle config is set, so the meta-log doesn't carry events
	// that match any rule.
	const total = 5
	keys := make([]string, total)
	for i := 0; i < total; i++ {
		keys[i] = fmt.Sprintf("preexisting/obj-%02d.txt", i)
		putObject(t, c, bucket, keys[i], "content")
		backdateMtime(t, fc, bucket, keys[i], 30)
	}

	// One out-of-prefix object that must NOT be expired. Same
	// pre-existing PUT timing.
	const survivor = "other/keep.txt"
	putObject(t, c, bucket, survivor, "keep")
	backdateMtime(t, fc, bucket, survivor, 30)

	// NOW set the rule. Up to this point the worker (if it were
	// running event-driven only) would have nothing matching to do.
	putExpirationLifecycle(t, c, bucket, "preexisting/", 1)

	out := runLifecycleShard(t)
	t.Logf("shell output:\n%s", out)

	// Every pre-existing in-prefix object must be expired by the
	// bootstrap walk's discovery + dispatch.
	for _, k := range keys {
		k := k
		require.Eventuallyf(t, func() bool {
			_, err := c.HeadObject(context.Background(), &s3.HeadObjectInput{
				Bucket: aws.String(bucket), Key: aws.String(k),
			})
			return isS3NotFound(err)
		}, 30*time.Second, 500*time.Millisecond,
			"pre-existing %s/%s must be discovered + expired by bootstrap walk", bucket, k)
	}

	// Out-of-prefix object stays — pin that the bootstrap walk
	// doesn't ignore the rule's prefix filter.
	_, err := c.HeadObject(context.Background(), &s3.HeadObjectInput{
		Bucket: aws.String(bucket), Key: aws.String(survivor),
	})
	require.NoError(t, err, "out-of-prefix object must survive the bootstrap walk")
}
