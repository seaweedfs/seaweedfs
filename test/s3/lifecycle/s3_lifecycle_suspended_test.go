// Suspended-versioning Expiration integration scenario.
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

// putVersioningSuspended flips Status=Suspended. PUTs after this point
// overwrite the null version in place rather than creating new versionIds.
func putVersioningSuspended(t *testing.T, c *s3.Client, bucket string) {
	t.Helper()
	_, err := c.PutBucketVersioning(context.Background(), &s3.PutBucketVersioningInput{
		Bucket:                  aws.String(bucket),
		VersioningConfiguration: &types.VersioningConfiguration{Status: types.BucketVersioningStatusSuspended},
	})
	require.NoError(t, err)
}

// TestLifecycleSuspendedVersioningExpiration: on a suspended-versioning
// bucket, Expiration takes a different code path than the Enabled and
// Off cases. lifecycleDispatch's VersioningSuspended branch first
// deletes the null version (via deleteSpecificObjectVersion(versionId="null"))
// and then writes a fresh delete marker on top.
//
// Setup: enable then suspend versioning so any noncurrent versions can
// exist alongside the null one; PUT once-with-versioning + once-after-
// suspend (so we have a noncurrent version + a null version), backdate
// the null, run the worker. The null must be deleted and replaced by a
// delete marker; the prior noncurrent version stays addressable.
func TestLifecycleSuspendedVersioningExpiration(t *testing.T) {
	c := s3Client(t)
	fc, fcClose := filerClient(t)
	defer fcClose()

	bucket := uniqueBucket("suspended")
	mustCreateBucket(t, c, bucket)
	putVersioningEnabled(t, c, bucket)

	const key = "s/obj.txt"
	// First PUT under Enabled: produces a real versionId.
	putObject(t, c, bucket, key, "v1")
	headEnabled, err := c.HeadObject(context.Background(), &s3.HeadObjectInput{
		Bucket: aws.String(bucket), Key: aws.String(key),
	})
	require.NoError(t, err)
	v1 := aws.ToString(headEnabled.VersionId)
	require.NotEmpty(t, v1)
	require.NotEqual(t, "null", v1)

	// Suspend versioning, then PUT again. The new write becomes the
	// "null" version that overwrites the bare-key entry; v1 demotes to
	// a noncurrent version.
	putVersioningSuspended(t, c, bucket)
	putObject(t, c, bucket, key, "null-version")

	// Set the Expiration rule AFTER the writes so the rule
	// configuration race against PUTs is deterministic. Days=1 for
	// AWS-spec compliance; backdating the null entry makes the worker
	// fire immediately.
	putExpirationLifecycle(t, c, bucket, "s/", 1)

	// Backdate the bare-key entry (the null version): on suspended
	// buckets the null lives at the bare path, NOT under .versions/.
	backdateMtime(t, fc, bucket, key, 30)

	out := runLifecycleShard(t)
	t.Logf("shell output:\n%s", out)

	// HEAD without versionId hits the latest, which after the worker
	// runs should be a delete marker (404 NoSuchKey).
	require.Eventuallyf(t, func() bool {
		_, err := c.HeadObject(context.Background(), &s3.HeadObjectInput{
			Bucket: aws.String(bucket), Key: aws.String(key),
		})
		return err != nil
	}, 30*time.Second, 500*time.Millisecond, "delete marker must dominate latest after suspended-versioning expiration")

	// The noncurrent v1 (created under Enabled) must remain — Expiration
	// in the suspended branch only touches the null version, not other
	// versions in the .versions/ folder.
	directHead, err := c.HeadObject(context.Background(), &s3.HeadObjectInput{
		Bucket: aws.String(bucket), Key: aws.String(key), VersionId: aws.String(v1),
	})
	require.NoError(t, err, "noncurrent v1 must survive — suspended Expiration only touches the null")
	require.NotNil(t, directHead)

	// ListObjectVersions must show: v1 still present, a fresh delete
	// marker as latest, and no more "null" version.
	listOut, err := c.ListObjectVersions(context.Background(), &s3.ListObjectVersionsInput{
		Bucket: aws.String(bucket), Prefix: aws.String(key),
	})
	require.NoError(t, err)
	var sawV1, sawMarker bool
	for _, v := range listOut.Versions {
		if aws.ToString(v.Key) == key {
			vid := aws.ToString(v.VersionId)
			if vid == v1 {
				sawV1 = true
			}
			require.NotEqual(t, "null", vid, "null version must have been removed")
		}
	}
	for _, m := range listOut.DeleteMarkers {
		if aws.ToString(m.Key) == key && aws.ToBool(m.IsLatest) {
			sawMarker = true
		}
	}
	require.True(t, sawV1, "v1 must still appear in ListObjectVersions")
	require.True(t, sawMarker, "fresh delete marker must dominate latest")
}
