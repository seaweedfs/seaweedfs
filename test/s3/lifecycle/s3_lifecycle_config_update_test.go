// Lifecycle config update across sweeps.
package lifecycle

import (
	"context"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stretchr/testify/require"
)

// TestLifecycleConfigUpdateBetweenSweeps: an operator changes the
// lifecycle rule between two shell-driven sweeps. The second sweep
// must respect the NEW rule, not a cached version of the old one.
//
// Each `runLifecycleShard` invocation spawns a fresh `weed shell`
// subprocess, so cached engine state from a previous sweep doesn't
// persist across runs. This test pins that the freshly-loaded config
// actually changes routing — under the new prefix only matching
// objects expire, even if there are still backdated objects sitting
// under the old prefix.
func TestLifecycleConfigUpdateBetweenSweeps(t *testing.T) {
	c := s3Client(t)
	fc, fcClose := filerClient(t)
	defer fcClose()

	bucket := uniqueBucket("config-update")
	mustCreateBucket(t, c, bucket)

	// Sweep 1: rule expires anything under "first/".
	putExpirationLifecycle(t, c, bucket, "first/", 1)

	const firstKey = "first/initial.txt"
	putObject(t, c, bucket, firstKey, "first")
	backdateMtime(t, fc, bucket, firstKey, 30)

	out := runLifecycleShard(t)
	t.Logf("sweep 1 output:\n%s", out)

	require.Eventuallyf(t, func() bool {
		_, err := c.HeadObject(context.Background(), &s3.HeadObjectInput{
			Bucket: aws.String(bucket), Key: aws.String(firstKey),
		})
		return isS3NotFound(err)
	}, 30*time.Second, 500*time.Millisecond, "sweep 1 must expire %s", firstKey)

	// Update the rule to a different prefix. The old "first/" prefix
	// is no longer covered by any rule; objects under it must NOT be
	// expired by sweep 2 even when backdated.
	putExpirationLifecycle(t, c, bucket, "second/", 1)

	const secondKey = "second/new.txt"
	const oldPrefixKey = "first/post-update.txt"
	putObject(t, c, bucket, secondKey, "second")
	putObject(t, c, bucket, oldPrefixKey, "stale rule")
	backdateMtime(t, fc, bucket, secondKey, 30)
	backdateMtime(t, fc, bucket, oldPrefixKey, 30)

	out = runLifecycleShard(t)
	t.Logf("sweep 2 output:\n%s", out)

	// Sweep 2 expires the new-prefix object.
	require.Eventuallyf(t, func() bool {
		_, err := c.HeadObject(context.Background(), &s3.HeadObjectInput{
			Bucket: aws.String(bucket), Key: aws.String(secondKey),
		})
		return isS3NotFound(err)
	}, 30*time.Second, 500*time.Millisecond, "sweep 2 must expire %s under the new rule", secondKey)

	// Sweep 2 must NOT expire the old-prefix object — the rule was
	// replaced, not merged. A regression that caches old rules across
	// PutBucketLifecycleConfiguration calls would fail here.
	_, err := c.HeadObject(context.Background(), &s3.HeadObjectInput{
		Bucket: aws.String(bucket), Key: aws.String(oldPrefixKey),
	})
	require.NoError(t, err, "old-prefix object must survive after rule update — config replacement, not merge")
}
