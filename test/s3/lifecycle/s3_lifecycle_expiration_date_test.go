// ExpirationDate (date-based, not Days) integration scenario.
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

// TestLifecycleExpirationDateInThePast: an Expiration{Date: <past>} rule
// routes through the engine's ScanAtDate mode rather than the
// EventDriven delay-group path. The worker's dispatcher must still fire
// when the date has already passed at the time the worker runs. Pinning
// this path because most tests use Days-based rules; ScanAtDate is a
// separate compile + dispatch branch (engine.decideMode case
// ActionKindExpirationDate) that wouldn't be exercised otherwise.
func TestLifecycleExpirationDateInThePast(t *testing.T) {
	// SCAN_AT_DATE is a documented mode in engine.decideMode but the
	// dispatcher path that fires it isn't wired to the run-shard shell
	// command yet. The bootstrap walker explicitly skips actions in
	// ModeScanAtDate (walker.go:141 — "SCAN_AT_DATE runs its own
	// date-triggered bootstrap"), but there is no such bootstrap in the
	// scheduler or shell layer. Until that lands, this test would
	// always time out. Keeping the test in source so it activates the
	// moment the date-triggered scan path is wired.
	t.Skip("ScanAtDate dispatch path not yet wired to run-shard; activate when the date-bootstrap lands")
	c := s3Client(t)
	fc, fcClose := filerClient(t)
	defer fcClose()

	bucket := uniqueBucket("expdate")
	mustCreateBucket(t, c, bucket)

	// Date in the past: AWS rejects ExpirationDate in the future from
	// being processed early but a past date is the natural integration
	// test — every object hit by the rule is immediately eligible.
	pastDate := time.Now().Add(-7 * 24 * time.Hour).UTC().Truncate(24 * time.Hour)
	_, err := c.PutBucketLifecycleConfiguration(context.Background(), &s3.PutBucketLifecycleConfigurationInput{
		Bucket: aws.String(bucket),
		LifecycleConfiguration: &types.BucketLifecycleConfiguration{
			Rules: []types.LifecycleRule{{
				ID:     aws.String("expire-by-date"),
				Status: types.ExpirationStatusEnabled,
				Filter: &types.LifecycleRuleFilter{Prefix: aws.String("d/")},
				Expiration: &types.LifecycleExpiration{
					Date: aws.Time(pastDate),
				},
			}},
		},
	})
	require.NoError(t, err)

	const oldKey = "d/in-prefix.txt"
	const otherKey = "other/skip.txt"
	putObject(t, c, bucket, oldKey, "old")
	putObject(t, c, bucket, otherKey, "other")

	// Backdate oldKey too — defense-in-depth so the test doesn't
	// depend on whether the worker also considers MinTriggerAge for
	// date kinds; a fresh object with a past date should still expire,
	// but aging it locks the assertion either way.
	backdateMtime(t, fc, bucket, oldKey, 30)

	out := runLifecycleShard(t)
	t.Logf("shell output:\n%s", out)

	require.Eventuallyf(t, func() bool {
		_, err := c.HeadObject(context.Background(), &s3.HeadObjectInput{
			Bucket: aws.String(bucket), Key: aws.String(oldKey),
		})
		return isS3NotFound(err)
	}, 30*time.Second, 500*time.Millisecond, "%s/%s must be expired by past-date rule", bucket, oldKey)

	// Out-of-prefix object stays — the rule's Filter.Prefix gates this.
	_, err = c.HeadObject(context.Background(), &s3.HeadObjectInput{
		Bucket: aws.String(bucket), Key: aws.String(otherKey),
	})
	require.NoError(t, err, "object outside the rule's prefix must remain")
}
