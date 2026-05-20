// NewerNoncurrentVersions integration scenario.
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

// putNewerNoncurrentLifecycle keeps the N most recent noncurrent versions
// and expires the rest. NoncurrentDays is required by the AWS XML schema
// alongside NewerNoncurrentVersions; keeping it at 1 means the rule fires
// as soon as a version moves past the keep-count and is older than 1 day.
func putNewerNoncurrentLifecycle(t *testing.T, c *s3.Client, bucket, prefix string, keep int32) {
	t.Helper()
	_, err := c.PutBucketLifecycleConfiguration(context.Background(), &s3.PutBucketLifecycleConfigurationInput{
		Bucket: aws.String(bucket),
		LifecycleConfiguration: &types.BucketLifecycleConfiguration{
			Rules: []types.LifecycleRule{
				{
					ID:     aws.String("keep-newest-noncurrent"),
					Status: types.ExpirationStatusEnabled,
					Filter: &types.LifecycleRuleFilter{Prefix: aws.String(prefix)},
					NoncurrentVersionExpiration: &types.NoncurrentVersionExpiration{
						NewerNoncurrentVersions: aws.Int32(keep),
						NoncurrentDays:          aws.Int32(1),
					},
				},
			},
		},
	})
	require.NoError(t, err)
}

// TestLifecycleNewerNoncurrentVersions: NewerNoncurrentVersions=1 keeps
// only the most recent noncurrent version; older noncurrent versions
// must be deleted. PUT v1, v2, v3, v4 (v4 current; v1-v3 noncurrent),
// backdate v1, v2, v3, run the worker, expect v3 (newest noncurrent)
// and v4 (current) to remain, v1 and v2 to be removed.
//
// This routes through routePointerTransition's "needs full expansion"
// path — distinct from the per-version NoncurrentDays path because
// the rule depends on per-version rank, not just per-version age.
func TestLifecycleNewerNoncurrentVersions(t *testing.T) {
	c := s3Client(t)
	fc, fcClose := filerClient(t)
	defer fcClose()

	bucket := uniqueBucket("newer-nc")
	mustCreateBucket(t, c, bucket)
	putVersioningEnabled(t, c, bucket)
	putNewerNoncurrentLifecycle(t, c, bucket, "n/", 1)

	const key = "n/obj.txt"
	puts := make([]string, 4) // versionIds in PUT order
	for i := range puts {
		out, err := c.PutObject(context.Background(), &s3.PutObjectInput{
			Bucket: aws.String(bucket), Key: aws.String(key),
			Body: strings.NewReader(time.Now().Format(time.RFC3339Nano) + "-" + string(rune('a'+i))),
		})
		require.NoError(t, err)
		puts[i] = aws.ToString(out.VersionId)
		require.NotEmpty(t, puts[i])
	}
	v1, v2, v3, v4 := puts[0], puts[1], puts[2], puts[3]
	require.NotEqual(t, v1, v2)
	require.NotEqual(t, v3, v4)

	// Backdate every noncurrent (v1, v2, v3) so all three satisfy the
	// NoncurrentDays>=1 floor; the keep-newest-1 logic then chooses
	// which to drop.
	for _, vid := range []string{v1, v2, v3} {
		backdateVersionedMtime(t, fc, bucket, key, vid, 30)
	}

	out := runLifecycleShard(t)
	t.Logf("shell output:\n%s", out)

	// v1 and v2 (older noncurrent) must be expired.
	for _, vid := range []string{v1, v2} {
		vid := vid
		require.Eventuallyf(t, func() bool {
			_, err := c.HeadObject(context.Background(), &s3.HeadObjectInput{
				Bucket: aws.String(bucket), Key: aws.String(key), VersionId: aws.String(vid),
			})
			return err != nil
		}, 30*time.Second, 500*time.Millisecond, "older noncurrent version %s must be expired", vid)
	}

	// v3 (newest noncurrent within the keep=1 window) must remain.
	_, err := c.HeadObject(context.Background(), &s3.HeadObjectInput{
		Bucket: aws.String(bucket), Key: aws.String(key), VersionId: aws.String(v3),
	})
	require.NoError(t, err, "v3 (newest noncurrent within keep=1) must survive")

	// v4 (current) must remain — current versions are immune to
	// NoncurrentVersionExpiration regardless of how the keep-count
	// arithmetic plays out.
	currentHead, err := c.HeadObject(context.Background(), &s3.HeadObjectInput{
		Bucket: aws.String(bucket), Key: aws.String(key),
	})
	require.NoError(t, err, "current version v4 must remain")
	require.Equal(t, v4, aws.ToString(currentHead.VersionId))
}
