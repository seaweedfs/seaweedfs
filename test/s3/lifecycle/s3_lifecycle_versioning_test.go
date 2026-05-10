// Versioning + delete-marker + noncurrent integration scenarios for the
// event-driven lifecycle worker. Same backdating trick as the base test:
// AWS rejects Days < 1, so the test ages the object via the filer's
// UpdateEntry RPC and runs the shell command once with -runtime 10s.
package lifecycle

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/stretchr/testify/require"
)

// putVersioningEnabled flips Status=Enabled on the bucket so subsequent
// PUTs produce versioned siblings rather than overwriting.
func putVersioningEnabled(t *testing.T, c *s3.Client, bucket string) {
	t.Helper()
	_, err := c.PutBucketVersioning(context.Background(), &s3.PutBucketVersioningInput{
		Bucket:                  aws.String(bucket),
		VersioningConfiguration: &types.VersioningConfiguration{Status: types.BucketVersioningStatusEnabled},
	})
	require.NoError(t, err)
}

// putNoncurrentExpirationLifecycle sets a NoncurrentVersionExpiration rule
// targeting a prefix. Days=1 because AWS rejects 0; the test backdates
// the noncurrent version.
func putNoncurrentExpirationLifecycle(t *testing.T, c *s3.Client, bucket, prefix string, days int32) {
	t.Helper()
	_, err := c.PutBucketLifecycleConfiguration(context.Background(), &s3.PutBucketLifecycleConfigurationInput{
		Bucket: aws.String(bucket),
		LifecycleConfiguration: &types.BucketLifecycleConfiguration{
			Rules: []types.LifecycleRule{
				{
					ID:     aws.String("expire-noncurrent"),
					Status: types.ExpirationStatusEnabled,
					Filter: &types.LifecycleRuleFilter{Prefix: aws.String(prefix)},
					NoncurrentVersionExpiration: &types.NoncurrentVersionExpiration{
						NoncurrentDays: aws.Int32(days),
					},
				},
			},
		},
	})
	require.NoError(t, err)
}

// putExpiredDeleteMarkerLifecycle: Expiration{ExpiredObjectDeleteMarker:
// true} cleans up sole-survivor delete markers.
func putExpiredDeleteMarkerLifecycle(t *testing.T, c *s3.Client, bucket, prefix string) {
	t.Helper()
	_, err := c.PutBucketLifecycleConfiguration(context.Background(), &s3.PutBucketLifecycleConfigurationInput{
		Bucket: aws.String(bucket),
		LifecycleConfiguration: &types.BucketLifecycleConfiguration{
			Rules: []types.LifecycleRule{
				{
					ID:     aws.String("expire-marker"),
					Status: types.ExpirationStatusEnabled,
					Filter: &types.LifecycleRuleFilter{Prefix: aws.String(prefix)},
					Expiration: &types.LifecycleExpiration{
						ExpiredObjectDeleteMarker: aws.Bool(true),
					},
				},
			},
		},
	})
	require.NoError(t, err)
}

// backdateVersionedMtime ages a specific .versions/v_<id> entry. The
// version files live under <buckets>/<bucket>/<key>.versions/v_<versionId>.
func backdateVersionedMtime(t *testing.T, fc filer_pb.SeaweedFilerClient, bucket, key, versionID string, daysOld int) {
	t.Helper()
	dir := bucketsPath + "/" + bucket + "/" + key + ".versions"
	name := "v_" + versionID
	resp, err := fc.LookupDirectoryEntry(context.Background(), &filer_pb.LookupDirectoryEntryRequest{
		Directory: dir, Name: name,
	})
	require.NoError(t, err, "lookup version %s/%s", dir, name)
	require.NotNil(t, resp.Entry)
	require.NotNil(t, resp.Entry.Attributes)

	resp.Entry.Attributes.Mtime = time.Now().Add(-time.Duration(daysOld) * 24 * time.Hour).Unix()
	resp.Entry.Attributes.MtimeNs = 0
	_, err = fc.UpdateEntry(context.Background(), &filer_pb.UpdateEntryRequest{
		Directory: dir, Entry: resp.Entry,
	})
	require.NoError(t, err)
}

func runLifecycleShard(t *testing.T) string {
	t.Helper()
	out := runShellCommand(t, fmt.Sprintf(
		"s3.lifecycle.run-shard -shards 0-15 -s3 %s -events 0 -dispatch 200ms -checkpoint 5s -runtime 10s",
		envOr("S3_GRPC_ENDPOINT", defaultS3GrpcEndpoint),
	))
	require.NotContains(t, out, "FATAL", "shell output:\n%s", out)
	return out
}

// TestLifecycleVersionedBucketCreatesDeleteMarker: an Expiration rule on a
// versioned bucket must produce a delete marker (latest version after
// expiration) rather than removing the version itself. Pre-fix this would
// have unconditionally deleted the live version.
func TestLifecycleVersionedBucketCreatesDeleteMarker(t *testing.T) {
	c := s3Client(t)
	fc, fcClose := filerClient(t)
	defer fcClose()

	bucket := uniqueBucket("vexp")
	mustCreateBucket(t, c, bucket)
	putVersioningEnabled(t, c, bucket)
	putExpirationLifecycle(t, c, bucket, "expire/", 1)

	const key = "expire/old.txt"
	putObject(t, c, bucket, key, "v1")

	// Look up the version we just created so we can backdate it. The bare
	// filer entry under <bucket>/<key> is the latest-version pointer in
	// SeaweedFS's versioned layout.
	headOut, err := c.HeadObject(context.Background(), &s3.HeadObjectInput{
		Bucket: aws.String(bucket), Key: aws.String(key),
	})
	require.NoError(t, err)
	require.NotNil(t, headOut.VersionId)
	versionID := aws.ToString(headOut.VersionId)
	require.NotEmpty(t, versionID)
	backdateVersionedMtime(t, fc, bucket, key, versionID, 30)

	out := runLifecycleShard(t)
	t.Logf("shell output:\n%s", out)

	// HEAD without versionId returns the latest version. After the worker
	// runs, the latest must be a delete marker (NoSuchKey on plain HEAD).
	require.Eventuallyf(t, func() bool {
		_, err := c.HeadObject(context.Background(), &s3.HeadObjectInput{
			Bucket: aws.String(bucket), Key: aws.String(key),
		})
		return err != nil
	}, 30*time.Second, 500*time.Millisecond, "expected delete marker to become latest for %s/%s", bucket, key)

	// The original version must still be addressable directly — Expiration
	// on a versioned bucket creates a marker, it doesn't drop the version.
	directHead, err := c.HeadObject(context.Background(), &s3.HeadObjectInput{
		Bucket: aws.String(bucket), Key: aws.String(key), VersionId: aws.String(versionID),
	})
	require.NoError(t, err, "original version must still exist")
	require.NotNil(t, directHead)

	// ListObjectVersions should now show a delete marker dominating the
	// version. The IsLatest=true marker proves the worker created a fresh
	// marker rather than aging the existing version.
	listOut, err := c.ListObjectVersions(context.Background(), &s3.ListObjectVersionsInput{
		Bucket: aws.String(bucket), Prefix: aws.String(key),
	})
	require.NoError(t, err)
	var sawMarker bool
	for _, m := range listOut.DeleteMarkers {
		if aws.ToString(m.Key) == key && aws.ToBool(m.IsLatest) {
			sawMarker = true
			break
		}
	}
	require.True(t, sawMarker, "ListObjectVersions must show a delete marker as latest for %s", key)
}

// TestLifecycleNoncurrentVersionExpiration: NoncurrentVersionExpiration
// only fires on noncurrent versions. PUT v1, PUT v2 (so v1 → noncurrent),
// backdate v1, run worker. v1 must be removed; v2 stays current.
func TestLifecycleNoncurrentVersionExpiration(t *testing.T) {
	c := s3Client(t)
	fc, fcClose := filerClient(t)
	defer fcClose()

	bucket := uniqueBucket("noncurrent")
	mustCreateBucket(t, c, bucket)
	putVersioningEnabled(t, c, bucket)
	putNoncurrentExpirationLifecycle(t, c, bucket, "v/", 1)

	const key = "v/obj.txt"
	// First PUT.
	put1, err := c.PutObject(context.Background(), &s3.PutObjectInput{
		Bucket: aws.String(bucket), Key: aws.String(key), Body: strings.NewReader("v1"),
	})
	require.NoError(t, err)
	v1 := aws.ToString(put1.VersionId)

	// Second PUT promotes v1 to noncurrent.
	put2, err := c.PutObject(context.Background(), &s3.PutObjectInput{
		Bucket: aws.String(bucket), Key: aws.String(key), Body: strings.NewReader("v2"),
	})
	require.NoError(t, err)
	v2 := aws.ToString(put2.VersionId)
	require.NotEqual(t, v1, v2)

	// Age the noncurrent (v1) past the 1-day threshold.
	backdateVersionedMtime(t, fc, bucket, key, v1, 30)

	out := runLifecycleShard(t)
	t.Logf("shell output:\n%s", out)

	// v1 must be gone.
	require.Eventuallyf(t, func() bool {
		_, err := c.HeadObject(context.Background(), &s3.HeadObjectInput{
			Bucket: aws.String(bucket), Key: aws.String(key), VersionId: aws.String(v1),
		})
		return err != nil
	}, 30*time.Second, 500*time.Millisecond, "noncurrent v1 must be expired")

	// v2 must still be addressable as the current version.
	currentHead, err := c.HeadObject(context.Background(), &s3.HeadObjectInput{
		Bucket: aws.String(bucket), Key: aws.String(key),
	})
	require.NoError(t, err, "current version (v2) must remain")
	require.Equal(t, v2, aws.ToString(currentHead.VersionId))
}

// TestLifecycleExpiredDeleteMarkerCleanup: a sole-survivor delete marker
// must be removed by an ExpiredObjectDeleteMarker=true rule. Setup: PUT v1,
// DELETE (creates a marker that becomes latest, with v1 as noncurrent),
// expire v1 via NoncurrentVersionExpiration so the marker is alone, then
// the marker rule fires.
//
// We exercise this in two passes: pass 1 (Noncurrent rule on a sibling
// prefix) cleans v1; pass 2 (ExpiredObjectDeleteMarker rule, also on the
// same prefix) removes the now-orphaned marker. Since the lifecycle XML
// only carries one set of rules per bucket, we wire both.
func TestLifecycleExpiredDeleteMarkerCleanup(t *testing.T) {
	c := s3Client(t)
	fc, fcClose := filerClient(t)
	defer fcClose()

	bucket := uniqueBucket("marker")
	mustCreateBucket(t, c, bucket)
	putVersioningEnabled(t, c, bucket)

	// Combined rule: noncurrent expiration + delete-marker cleanup, both
	// on the same prefix.
	_, err := c.PutBucketLifecycleConfiguration(context.Background(), &s3.PutBucketLifecycleConfigurationInput{
		Bucket: aws.String(bucket),
		LifecycleConfiguration: &types.BucketLifecycleConfiguration{
			Rules: []types.LifecycleRule{
				{
					ID:     aws.String("expire-noncurrent"),
					Status: types.ExpirationStatusEnabled,
					Filter: &types.LifecycleRuleFilter{Prefix: aws.String("m/")},
					NoncurrentVersionExpiration: &types.NoncurrentVersionExpiration{
						NoncurrentDays: aws.Int32(1),
					},
				},
				{
					ID:     aws.String("expire-marker"),
					Status: types.ExpirationStatusEnabled,
					Filter: &types.LifecycleRuleFilter{Prefix: aws.String("m/")},
					Expiration: &types.LifecycleExpiration{
						ExpiredObjectDeleteMarker: aws.Bool(true),
					},
				},
			},
		},
	})
	require.NoError(t, err)
	_ = putExpiredDeleteMarkerLifecycle // keep the helper referenced; it's used by other tests in this file

	const key = "m/obj.txt"
	put1, err := c.PutObject(context.Background(), &s3.PutObjectInput{
		Bucket: aws.String(bucket), Key: aws.String(key), Body: strings.NewReader("v1"),
	})
	require.NoError(t, err)
	v1 := aws.ToString(put1.VersionId)

	// Plain DELETE with versioning on creates a marker; the marker becomes
	// latest, v1 demotes to noncurrent.
	_, err = c.DeleteObject(context.Background(), &s3.DeleteObjectInput{
		Bucket: aws.String(bucket), Key: aws.String(key),
	})
	require.NoError(t, err)

	// Backdate v1 so the noncurrent rule expires it.
	backdateVersionedMtime(t, fc, bucket, key, v1, 30)

	// Backdate the delete marker too. ListObjectVersions tells us the
	// marker's versionId.
	listOut, err := c.ListObjectVersions(context.Background(), &s3.ListObjectVersionsInput{
		Bucket: aws.String(bucket), Prefix: aws.String(key),
	})
	require.NoError(t, err)
	var markerID string
	for _, m := range listOut.DeleteMarkers {
		if aws.ToString(m.Key) == key {
			markerID = aws.ToString(m.VersionId)
			break
		}
	}
	require.NotEmpty(t, markerID, "expected to find the delete marker we just created")
	backdateVersionedMtime(t, fc, bucket, key, markerID, 30)

	out := runLifecycleShard(t)
	t.Logf("shell output:\n%s", out)

	// Both v1 and the marker must be gone — the .versions/<key>/ folder
	// should hold nothing for this key.
	require.Eventuallyf(t, func() bool {
		listOut, err := c.ListObjectVersions(context.Background(), &s3.ListObjectVersionsInput{
			Bucket: aws.String(bucket), Prefix: aws.String(key),
		})
		if err != nil {
			return false
		}
		for _, v := range listOut.Versions {
			if aws.ToString(v.Key) == key {
				return false
			}
		}
		for _, m := range listOut.DeleteMarkers {
			if aws.ToString(m.Key) == key {
				return false
			}
		}
		return true
	}, 30*time.Second, 500*time.Millisecond, "every version and marker for %s must be gone", key)
}

// TestLifecycleDisabledRuleSkipsObject: a rule with Status=Disabled must
// not produce dispatches, even on a backdated object that would otherwise
// match. Negative test for the engine's enabled-status gate.
func TestLifecycleDisabledRuleSkipsObject(t *testing.T) {
	c := s3Client(t)
	fc, fcClose := filerClient(t)
	defer fcClose()

	bucket := uniqueBucket("disabled")
	mustCreateBucket(t, c, bucket)
	_, err := c.PutBucketLifecycleConfiguration(context.Background(), &s3.PutBucketLifecycleConfigurationInput{
		Bucket: aws.String(bucket),
		LifecycleConfiguration: &types.BucketLifecycleConfiguration{
			Rules: []types.LifecycleRule{{
				ID:         aws.String("disabled-rule"),
				Status:     types.ExpirationStatusDisabled,
				Filter:     &types.LifecycleRuleFilter{Prefix: aws.String("d/")},
				Expiration: &types.LifecycleExpiration{Days: aws.Int32(1)},
			}},
		},
	})
	require.NoError(t, err)

	const key = "d/obj.txt"
	putObject(t, c, bucket, key, "still-here")
	backdateMtime(t, fc, bucket, key, 30)

	out := runLifecycleShard(t)
	t.Logf("shell output:\n%s", out)

	// Wait the same window; the object MUST still exist after.
	time.Sleep(2 * time.Second) // give the worker a chance to (incorrectly) act
	_, err = c.HeadObject(context.Background(), &s3.HeadObjectInput{
		Bucket: aws.String(bucket), Key: aws.String(key),
	})
	require.NoError(t, err, "Disabled rule must not delete the object")
}

// TestLifecycleTagFilter: a rule with a tag filter must only match objects
// carrying that tag. Two backdated objects, one tagged, one not — only the
// tagged one is removed.
func TestLifecycleTagFilter(t *testing.T) {
	c := s3Client(t)
	fc, fcClose := filerClient(t)
	defer fcClose()

	bucket := uniqueBucket("tagfilter")
	mustCreateBucket(t, c, bucket)
	_, err := c.PutBucketLifecycleConfiguration(context.Background(), &s3.PutBucketLifecycleConfigurationInput{
		Bucket: aws.String(bucket),
		LifecycleConfiguration: &types.BucketLifecycleConfiguration{
			Rules: []types.LifecycleRule{{
				ID:     aws.String("tag-only"),
				Status: types.ExpirationStatusEnabled,
				Filter: &types.LifecycleRuleFilter{
					And: &types.LifecycleRuleAndOperator{
						Prefix: aws.String("t/"),
						Tags: []types.Tag{
							{Key: aws.String("env"), Value: aws.String("temp")},
						},
					},
				},
				Expiration: &types.LifecycleExpiration{Days: aws.Int32(1)},
			}},
		},
	})
	require.NoError(t, err)

	const taggedKey = "t/tagged.txt"
	const untaggedKey = "t/untagged.txt"
	putObject(t, c, bucket, taggedKey, "tagged")
	putObject(t, c, bucket, untaggedKey, "untagged")

	// Stamp the tag on one of them.
	_, err = c.PutObjectTagging(context.Background(), &s3.PutObjectTaggingInput{
		Bucket: aws.String(bucket), Key: aws.String(taggedKey),
		Tagging: &types.Tagging{TagSet: []types.Tag{
			{Key: aws.String("env"), Value: aws.String("temp")},
		}},
	})
	require.NoError(t, err)

	backdateMtime(t, fc, bucket, taggedKey, 30)
	backdateMtime(t, fc, bucket, untaggedKey, 30)

	out := runLifecycleShard(t)
	t.Logf("shell output:\n%s", out)

	require.Eventuallyf(t, func() bool {
		_, err := c.HeadObject(context.Background(), &s3.HeadObjectInput{
			Bucket: aws.String(bucket), Key: aws.String(taggedKey),
		})
		return err != nil
	}, 30*time.Second, 500*time.Millisecond, "tagged object must be expired")

	_, err = c.HeadObject(context.Background(), &s3.HeadObjectInput{
		Bucket: aws.String(bucket), Key: aws.String(untaggedKey),
	})
	require.NoError(t, err, "untagged object must remain")
}
