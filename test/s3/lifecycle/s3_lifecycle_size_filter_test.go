// Size-filter integration scenario.
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

// TestLifecycleSizeFilterGreaterThan: ObjectSizeGreaterThan is a strict >
// gate (filterAllows in match.go uses ev.Size <= rule.FilterSizeGreaterThan
// to reject). Two backdated objects on the same prefix — only the one
// strictly larger than the threshold is expired.
func TestLifecycleSizeFilterGreaterThan(t *testing.T) {
	c := s3Client(t)
	fc, fcClose := filerClient(t)
	defer fcClose()

	bucket := uniqueBucket("size-filter")
	mustCreateBucket(t, c, bucket)

	const threshold = 100
	_, err := c.PutBucketLifecycleConfiguration(context.Background(), &s3.PutBucketLifecycleConfigurationInput{
		Bucket: aws.String(bucket),
		LifecycleConfiguration: &types.BucketLifecycleConfiguration{
			Rules: []types.LifecycleRule{{
				ID:     aws.String("size-gt"),
				Status: types.ExpirationStatusEnabled,
				Filter: &types.LifecycleRuleFilter{
					And: &types.LifecycleRuleAndOperator{
						Prefix:                aws.String("sz/"),
						ObjectSizeGreaterThan: aws.Int64(threshold),
					},
				},
				Expiration: &types.LifecycleExpiration{Days: aws.Int32(1)},
			}},
		},
	})
	require.NoError(t, err)

	const smallKey = "sz/small.txt"                                   // size <= threshold → must remain
	const largeKey = "sz/large.txt"                                   // size > threshold → must expire
	putObject(t, c, bucket, smallKey, strings.Repeat("a", threshold)) // exactly at boundary; gate is strictly >, so rejects
	putObject(t, c, bucket, largeKey, strings.Repeat("a", threshold+50))

	backdateMtime(t, fc, bucket, smallKey, 30)
	backdateMtime(t, fc, bucket, largeKey, 30)

	out := runLifecycleShard(t)
	t.Logf("shell output:\n%s", out)

	// Larger-than-threshold object expired.
	require.Eventuallyf(t, func() bool {
		_, err := c.HeadObject(context.Background(), &s3.HeadObjectInput{
			Bucket: aws.String(bucket), Key: aws.String(largeKey),
		})
		return err != nil
	}, 30*time.Second, 500*time.Millisecond, "object larger than threshold must expire")

	// Boundary object (size == threshold) survives — strictly-greater gate.
	_, err = c.HeadObject(context.Background(), &s3.HeadObjectInput{
		Bucket: aws.String(bucket), Key: aws.String(smallKey),
	})
	require.NoError(t, err, "object at exact threshold must remain (gate is strictly >)")
}
