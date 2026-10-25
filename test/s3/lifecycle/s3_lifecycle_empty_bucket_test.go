// Empty bucket sweep — no-op safety integration scenario.
package lifecycle

import (
	"context"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stretchr/testify/require"
)

// TestLifecycleEmptyBucketSweepIsNoOp: a bucket carrying lifecycle
// config but no objects must produce a successful sweep — no hangs,
// no errors, no dispatches. Pinned because the bootstrap walker
// iterates bucket directories, and an empty directory is a corner of
// that traversal that's easy to break (e.g. a slice-bounds bug on
// the first listing returning zero entries).
func TestLifecycleEmptyBucketSweepIsNoOp(t *testing.T) {
	c := s3Client(t)
	_, fcClose := filerClient(t)
	defer fcClose()

	bucket := uniqueBucket("empty")
	mustCreateBucket(t, c, bucket)
	putExpirationLifecycle(t, c, bucket, "anywhere/", 1)

	// No PUTs. The bucket is empty.

	out := runLifecycleShard(t)
	t.Logf("sweep output:\n%s", out)

	// Sanity: the worker must report it loaded the bucket's config and
	// completed without errors. The runLifecycleShard helper already
	// fails on FATAL; pin the success markers here too so a regression
	// that produces a half-shaped output (no completion) is caught.
	require.Contains(t, out, "loaded lifecycle for", "worker must report config load")
	require.Contains(t, out, "shards 0-15 complete", "worker must complete all shards")
	require.False(t, strings.Contains(out, "FATAL"), "worker must not produce FATAL output")

	// Bucket still exists after the sweep — the worker doesn't drop
	// empty buckets as a side effect.
	_, err := c.HeadBucket(context.Background(), &s3.HeadBucketInput{Bucket: aws.String(bucket)})
	require.NoError(t, err, "empty bucket must still exist after sweep")
}
