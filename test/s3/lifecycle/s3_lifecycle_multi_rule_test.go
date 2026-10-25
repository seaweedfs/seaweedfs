// Multiple-rules-per-bucket integration scenario.
package lifecycle

import (
	"context"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/stretchr/testify/require"
)

// TestLifecycleMultipleRulesInOneBucket: a single bucket carries two
// independent Expiration rules with disjoint prefix filters and
// different Days thresholds. Each rule must fire only on its prefix
// and ignore the other; objects outside both prefixes must survive.
//
// Pinned because compile.Compile builds one CompiledAction per rule
// per kind, all sharing the same bucket index. A bug that lets one
// rule's prefix or threshold leak into the other (e.g. last-write-
// wins on a shared map) would silently expire wrong objects.
func TestLifecycleMultipleRulesInOneBucket(t *testing.T) {
	c := s3Client(t)
	fc, fcClose := filerClient(t)
	defer fcClose()

	bucket := uniqueBucket("multi-rule")
	mustCreateBucket(t, c, bucket)

	_, err := c.PutBucketLifecycleConfiguration(context.Background(), &s3.PutBucketLifecycleConfigurationInput{
		Bucket: aws.String(bucket),
		LifecycleConfiguration: &types.BucketLifecycleConfiguration{
			Rules: []types.LifecycleRule{
				{
					ID:         aws.String("logs-1d"),
					Status:     types.ExpirationStatusEnabled,
					Filter:     &types.LifecycleRuleFilter{Prefix: aws.String("logs/")},
					Expiration: &types.LifecycleExpiration{Days: aws.Int32(1)},
				},
				{
					ID:         aws.String("tmp-7d"),
					Status:     types.ExpirationStatusEnabled,
					Filter:     &types.LifecycleRuleFilter{Prefix: aws.String("tmp/")},
					Expiration: &types.LifecycleExpiration{Days: aws.Int32(7)},
				},
			},
		},
	})
	require.NoError(t, err)

	const logsKey = "logs/access.log"
	const tmpKey = "tmp/scratch.bin"
	const otherKey = "data/keep.bin"

	putObject(t, c, bucket, logsKey, "log entry")
	putObject(t, c, bucket, tmpKey, "scratch")
	putObject(t, c, bucket, otherKey, "important")

	// Backdate logs/ past the 1d rule, tmp/ past the 7d rule, data/
	// past both thresholds — but data/ matches no rule, so it must
	// survive regardless of age.
	backdateMtime(t, fc, bucket, logsKey, 30)
	backdateMtime(t, fc, bucket, tmpKey, 30)
	backdateMtime(t, fc, bucket, otherKey, 30)

	out := runLifecycleShard(t)
	t.Logf("shell output:\n%s", out)

	// logs/ rule fires.
	require.Eventuallyf(t, func() bool {
		_, err := c.HeadObject(context.Background(), &s3.HeadObjectInput{
			Bucket: aws.String(bucket), Key: aws.String(logsKey),
		})
		return isS3NotFound(err)
	}, 30*time.Second, 500*time.Millisecond, "logs/ rule must expire %s", logsKey)

	// tmp/ rule fires.
	require.Eventuallyf(t, func() bool {
		_, err := c.HeadObject(context.Background(), &s3.HeadObjectInput{
			Bucket: aws.String(bucket), Key: aws.String(tmpKey),
		})
		return isS3NotFound(err)
	}, 30*time.Second, 500*time.Millisecond, "tmp/ rule must expire %s", tmpKey)

	// data/ matches NEITHER rule and must survive — pinning that the
	// per-rule prefix gate is independent and additive, not a global
	// "any rule's prefix" check.
	_, err = c.HeadObject(context.Background(), &s3.HeadObjectInput{
		Bucket: aws.String(bucket), Key: aws.String(otherKey),
	})
	require.NoError(t, err, "object outside both rule prefixes must survive")
}
