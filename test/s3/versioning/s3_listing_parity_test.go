package s3api

import (
	"bytes"
	"context"
	"sort"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ListObjects and ListObjectVersions walk the same namespace and must describe it
// the same way. They are separate code paths, so a client that navigates by
// versioned listings can see a different tree than one navigating by plain
// listings — and concludes keys are missing that are plainly there. Testing the
// two paths independently never catches that; only comparing them does.

// listingParityTree is shaped after a backup repository: nested prefixes, a file
// beside a prefix at the same level, and a key that is both an object and the
// parent of other keys.
var listingParityTree = []string{
	"backup/archive/001/data.blk",
	"backup/archive/002/data.blk",
	"backup/archive/summary.xml",
	"backup/meta",
	"backup/meta/nested.txt",
	"backup/index",
}

func currentKeysAndPrefixes(t *testing.T, client *s3.Client, bucket, prefix, delimiter string) ([]string, []string) {
	t.Helper()
	in := &s3.ListObjectsV2Input{Bucket: aws.String(bucket)}
	if prefix != "" {
		in.Prefix = aws.String(prefix)
	}
	if delimiter != "" {
		in.Delimiter = aws.String(delimiter)
	}
	resp, err := client.ListObjectsV2(context.TODO(), in)
	require.NoError(t, err)
	// One page only. A truncated response would silently compare partial results
	// against partial results and report parity that was never checked.
	require.False(t, aws.ToBool(resp.IsTruncated), "test data must fit one page")

	var keys, prefixes []string
	for _, o := range resp.Contents {
		require.NotNil(t, o.Key)
		keys = append(keys, *o.Key)
	}
	for _, p := range resp.CommonPrefixes {
		require.NotNil(t, p.Prefix)
		prefixes = append(prefixes, *p.Prefix)
	}
	sort.Strings(keys)
	sort.Strings(prefixes)
	return keys, prefixes
}

func currentKeysAndPrefixesFromVersions(t *testing.T, client *s3.Client, bucket, prefix, delimiter string) ([]string, []string) {
	t.Helper()
	in := &s3.ListObjectVersionsInput{Bucket: aws.String(bucket)}
	if prefix != "" {
		in.Prefix = aws.String(prefix)
	}
	if delimiter != "" {
		in.Delimiter = aws.String(delimiter)
	}
	resp, err := client.ListObjectVersions(context.TODO(), in)
	require.NoError(t, err)
	require.False(t, aws.ToBool(resp.IsTruncated), "test data must fit one page")

	// Reduce the version view to the same thing ListObjects reports: keys whose
	// current version is real content, not a delete marker.
	//
	// Delete markers are only visible here as individual entries. Under a
	// delimiter, keys grouped into a common prefix do not appear individually, so
	// this cannot tell that every key beneath a prefix is delete-marked — such a
	// prefix stays in the returned list. Comparisons that need that case must
	// query the prefix directly rather than relying on the grouped view.
	deleted := make(map[string]bool)
	for _, m := range resp.DeleteMarkers {
		if m.Key != nil && m.IsLatest != nil && *m.IsLatest {
			deleted[*m.Key] = true
		}
	}
	var keys, prefixes []string
	for _, v := range resp.Versions {
		if v.Key == nil || v.IsLatest == nil || !*v.IsLatest {
			continue
		}
		if deleted[*v.Key] {
			continue
		}
		keys = append(keys, *v.Key)
	}
	for _, p := range resp.CommonPrefixes {
		require.NotNil(t, p.Prefix)
		prefixes = append(prefixes, *p.Prefix)
	}
	sort.Strings(keys)
	sort.Strings(prefixes)
	return keys, prefixes
}

func TestListingParityAcrossPrefixesAndDelimiter(t *testing.T) {
	client := getS3Client(t)
	bucketName := getNewBucketName()

	createBucket(t, client, bucketName)
	defer deleteBucket(t, client, bucketName)
	enableVersioning(t, client, bucketName)

	for _, key := range listingParityTree {
		_, err := client.PutObject(context.TODO(), &s3.PutObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(key),
			Body:   bytes.NewReader([]byte("x")),
		})
		require.NoError(t, err)
	}

	cases := []struct {
		name      string
		prefix    string
		delimiter string
	}{
		{"root delimited", "", "/"},
		{"root undelimited", "", ""},
		{"one level delimited", "backup/", "/"},
		{"one level undelimited", "backup/", ""},
		{"two levels delimited", "backup/archive/", "/"},
		{"leaf prefix delimited", "backup/archive/001/", "/"},
		// The prefix names an object exactly. Both listings must return that
		// object and must not descend into anything below it.
		{"prefix is exactly an object", "backup/archive/summary.xml", "/"},
		{"prefix is exactly an object undelimited", "backup/archive/summary.xml", ""},
		// A key that is also a parent: "backup/meta" is an object and
		// "backup/meta/" is a prefix. The two listings must not disagree about
		// which of those the client is looking at.
		{"object that is also a prefix", "backup/meta", "/"},
		{"prefix form of that object", "backup/meta/", "/"},
		{"partial key fragment", "backup/ind", "/"},
		{"prefix matching nothing", "backup/nothing/", "/"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			plainKeys, plainPrefixes := currentKeysAndPrefixes(t, client, bucketName, tc.prefix, tc.delimiter)
			versionKeys, versionPrefixes := currentKeysAndPrefixesFromVersions(t, client, bucketName, tc.prefix, tc.delimiter)

			assert.Equal(t, plainKeys, versionKeys,
				"prefix=%q delimiter=%q: ListObjectVersions reports a different set of current keys than ListObjects", tc.prefix, tc.delimiter)
			assert.Equal(t, plainPrefixes, versionPrefixes,
				"prefix=%q delimiter=%q: ListObjectVersions reports different common prefixes than ListObjects", tc.prefix, tc.delimiter)
		})
	}
}

// Deleting every key under a prefix must retire the prefix from both listings
// together. A prefix that lingers in one view sends a client looking for keys the
// other view says are gone.
func TestListingParityAfterPrefixEmptied(t *testing.T) {
	client := getS3Client(t)
	bucketName := getNewBucketName()

	createBucket(t, client, bucketName)
	defer deleteBucket(t, client, bucketName)
	enableVersioning(t, client, bucketName)

	keys := []string{"backup/archive/001/data.blk", "backup/archive/002/data.blk"}
	versionIDs := make([]string, 0, len(keys))
	for _, key := range keys {
		put, err := client.PutObject(context.TODO(), &s3.PutObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(key),
			Body:   bytes.NewReader([]byte("x")),
		})
		require.NoError(t, err)
		require.NotNil(t, put.VersionId)
		versionIDs = append(versionIDs, *put.VersionId)
	}

	plainKeys, plainPrefixes := currentKeysAndPrefixes(t, client, bucketName, "backup/archive/", "/")
	versionKeys, versionPrefixes := currentKeysAndPrefixesFromVersions(t, client, bucketName, "backup/archive/", "/")
	require.Equal(t, plainKeys, versionKeys)
	require.Equal(t, plainPrefixes, versionPrefixes)
	require.Len(t, plainPrefixes, 2, "both leaf prefixes should be visible while populated")

	// Remove the versions outright rather than layering delete markers, so the
	// containers are left genuinely empty.
	for i, key := range keys {
		_, err := client.DeleteObject(context.TODO(), &s3.DeleteObjectInput{
			Bucket:    aws.String(bucketName),
			Key:       aws.String(key),
			VersionId: aws.String(versionIDs[i]),
		})
		require.NoError(t, err)
	}

	plainKeys, plainPrefixes = currentKeysAndPrefixes(t, client, bucketName, "backup/archive/", "/")
	versionKeys, versionPrefixes = currentKeysAndPrefixesFromVersions(t, client, bucketName, "backup/archive/", "/")

	// Both views must still describe the same namespace. Whether an emptied
	// prefix is retired promptly is a separate question from whether the two
	// listings agree about it, and only the latter is under test here.
	assert.Equal(t, plainKeys, versionKeys, "listings disagree on remaining keys after the prefix was emptied")
	assert.Equal(t, plainPrefixes, versionPrefixes, "listings disagree on remaining common prefixes after the prefix was emptied")
	// Asserted on both sides rather than leaning on the equality above to catch a
	// divergence in whichever direction it happens.
	assert.Empty(t, plainKeys, "no current keys should remain once every version is gone")
	assert.Empty(t, versionKeys, "no current keys should remain once every version is gone")
}

// The other way a key stops being current: a delete marker rather than removal of
// the version. Both listings must drop the key while its history stays intact.
func TestListingParityAfterDeleteMarker(t *testing.T) {
	client := getS3Client(t)
	bucketName := getNewBucketName()

	createBucket(t, client, bucketName)
	defer deleteBucket(t, client, bucketName)
	enableVersioning(t, client, bucketName)

	const marked = "backup/archive/001/data.blk"
	const kept = "backup/archive/002/data.blk"
	for _, key := range []string{marked, kept} {
		_, err := client.PutObject(context.TODO(), &s3.PutObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(key),
			Body:   bytes.NewReader([]byte("x")),
		})
		require.NoError(t, err)
	}

	// No version id: this layers a delete marker instead of removing the version.
	_, err := client.DeleteObject(context.TODO(), &s3.DeleteObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(marked),
	})
	require.NoError(t, err)

	for _, tc := range []struct{ prefix, delimiter string }{
		{"backup/archive/", ""},
		{"backup/archive/001/", "/"},
		{"backup/archive/001/", ""},
	} {
		t.Run(tc.prefix+"|"+tc.delimiter, func(t *testing.T) {
			plainKeys, plainPrefixes := currentKeysAndPrefixes(t, client, bucketName, tc.prefix, tc.delimiter)
			versionKeys, versionPrefixes := currentKeysAndPrefixesFromVersions(t, client, bucketName, tc.prefix, tc.delimiter)

			assert.Equal(t, plainKeys, versionKeys,
				"prefix=%q delimiter=%q: listings disagree on current keys after a delete marker", tc.prefix, tc.delimiter)
			assert.Equal(t, plainPrefixes, versionPrefixes,
				"prefix=%q delimiter=%q: listings disagree on common prefixes after a delete marker", tc.prefix, tc.delimiter)
			assert.NotContains(t, plainKeys, marked, "a delete-marked key is not current")
		})
	}

	// The history the marker hides must still be there.
	versions, err := client.ListObjectVersions(context.TODO(), &s3.ListObjectVersionsInput{
		Bucket: aws.String(bucketName),
		Prefix: aws.String(marked),
	})
	require.NoError(t, err)
	assert.Len(t, versions.Versions, 1, "the superseded version must survive its delete marker")
	assert.Len(t, versions.DeleteMarkers, 1)
}
