package example

import (
	"bytes"
	"io"
	"net/http"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	v1credentials "github.com/aws/aws-sdk-go/aws/credentials"
	v1signer "github.com/aws/aws-sdk-go/aws/signer/v4"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestS3PrefixObjectKeys covers keys that are a strict prefix of other keys: S3's
// namespace is flat, so "collision/foo" and "collision/foo/bar" are independent
// objects that coexist in either write order.
func TestS3PrefixObjectKeys(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	cluster, err := startMiniCluster(t)
	require.NoError(t, err)
	defer cluster.Stop()

	put := func(t *testing.T, bucket, key string, body []byte) {
		t.Helper()
		_, err := cluster.s3Client.PutObject(&s3.PutObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
			Body:   bytes.NewReader(body),
		})
		require.NoError(t, err, "put %s", key)
	}
	// read checks both paths a client reaches an object by, since a directory entry
	// carrying an object is served by neither the directory nor the plain object path
	// alone.
	read := func(t *testing.T, bucket, key string, want []byte) {
		t.Helper()
		head, err := cluster.s3Client.HeadObject(&s3.HeadObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
		})
		require.NoError(t, err, "head %s", key)
		assert.Equal(t, int64(len(want)), aws.Int64Value(head.ContentLength), "head %s", key)

		get, err := cluster.s3Client.GetObject(&s3.GetObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
		})
		require.NoError(t, err, "get %s", key)
		defer get.Body.Close()
		got, err := io.ReadAll(get.Body)
		require.NoError(t, err, "read %s", key)
		assert.Equal(t, want, got, "get %s", key)
	}
	// gone checks the key answers as absent rather than lingering on a directory
	// entry that outlived the object.
	gone := func(t *testing.T, bucket, key string) {
		t.Helper()
		_, err := cluster.s3Client.GetObject(&s3.GetObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
		})
		var missing awserr.RequestFailure
		require.ErrorAs(t, err, &missing, "get %s", key)
		assert.Equal(t, http.StatusNotFound, missing.StatusCode(), "get %s", key)

		_, err = cluster.s3Client.HeadObject(&s3.HeadObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
		})
		require.ErrorAs(t, err, &missing, "head %s", key)
		assert.Equal(t, http.StatusNotFound, missing.StatusCode(), "head %s", key)
	}
	listKeys := func(t *testing.T, bucket string) []string {
		t.Helper()
		resp, err := cluster.s3Client.ListObjectsV2(&s3.ListObjectsV2Input{Bucket: aws.String(bucket)})
		require.NoError(t, err)
		keys := collectKeys(resp.Contents)
		sort.Strings(keys)
		return keys
	}

	body := []byte("prefix object")
	// Distinct bodies, so a read that resolves to the wrong entry cannot pass.
	nested := []byte("nested under the prefix object")

	// The reported order: the nested key is written first, so the prefix key has to
	// land on a path the filer already holds a directory at.
	t.Run("ChildFirst", func(t *testing.T) {
		bucket := createTestBucket(t, cluster, "test-prefix-child-first-")
		put(t, bucket, "collision/foo/bar", nested)
		put(t, bucket, "collision/foo", body)

		assert.Equal(t, []string{"collision/foo", "collision/foo/bar"}, listKeys(t, bucket))
		read(t, bucket, "collision/foo", body)
		read(t, bucket, "collision/foo/bar", nested)
	})

	// The opposite order used to keep the prefix key's data but hide the key.
	t.Run("PrefixFirst", func(t *testing.T) {
		bucket := createTestBucket(t, cluster, "test-prefix-first-")
		put(t, bucket, "collision/foo", body)
		put(t, bucket, "collision/foo/bar", nested)

		assert.Equal(t, []string{"collision/foo", "collision/foo/bar"}, listKeys(t, bucket))
		read(t, bucket, "collision/foo", body)
		read(t, bucket, "collision/foo/bar", nested)
	})

	// An empty object leaves no chunks, content or mime behind, so it is the case a
	// promoted directory carries no other trace of.
	t.Run("EmptyObject", func(t *testing.T) {
		bucket := createTestBucket(t, cluster, "test-prefix-empty-")
		put(t, bucket, "a/foo/bar", nil)
		put(t, bucket, "a/foo", nil)
		put(t, bucket, "b/foo", nil)
		put(t, bucket, "b/foo/bar", nil)

		assert.Equal(t, []string{"a/foo", "a/foo/bar", "b/foo", "b/foo/bar"}, listKeys(t, bucket))
		read(t, bucket, "a/foo", []byte{})
		read(t, bucket, "b/foo", []byte{})
	})

	// The key has no trailing slash, and the keys nested under it still roll up into
	// their own CommonPrefix.
	t.Run("Delimiter", func(t *testing.T) {
		bucket := createTestBucket(t, cluster, "test-prefix-delimiter-")
		put(t, bucket, "collision/foo/bar", nested)
		put(t, bucket, "collision/foo", body)
		put(t, bucket, "collision/other", body)

		resp, err := cluster.s3Client.ListObjectsV2(&s3.ListObjectsV2Input{
			Bucket:    aws.String(bucket),
			Prefix:    aws.String("collision/"),
			Delimiter: aws.String("/"),
		})
		require.NoError(t, err)

		keys := collectKeys(resp.Contents)
		sort.Strings(keys)
		assert.Equal(t, []string{"collision/foo", "collision/other"}, keys)
		assert.Equal(t, []string{"collision/foo/"}, collectPrefixes(resp.CommonPrefixes))

		// Listing the prefix itself names only what is under it.
		resp, err = cluster.s3Client.ListObjectsV2(&s3.ListObjectsV2Input{
			Bucket:    aws.String(bucket),
			Prefix:    aws.String("collision/foo/"),
			Delimiter: aws.String("/"),
		})
		require.NoError(t, err)
		assert.Equal(t, []string{"collision/foo/bar"}, collectKeys(resp.Contents))
		assert.Empty(t, collectPrefixes(resp.CommonPrefixes))
	})

	// The key and the CommonPrefix its nested keys fold into come off one filer
	// entry, so a page boundary must not drop either of them.
	t.Run("Paged", func(t *testing.T) {
		bucket := createTestBucket(t, cluster, "test-prefix-paged-")
		for _, key := range []string{"foo", "foo/bar", "foobar", "other", "zed", "zed/a"} {
			put(t, bucket, key, body)
		}

		for _, maxKeys := range []int64{1, 2, 3, 4, 5} {
			var keys, prefixes []string
			var token *string
			for page := 0; page < 12; page++ {
				resp, err := cluster.s3Client.ListObjectsV2(&s3.ListObjectsV2Input{
					Bucket:            aws.String(bucket),
					Delimiter:         aws.String("/"),
					MaxKeys:           aws.Int64(maxKeys),
					ContinuationToken: token,
				})
				require.NoError(t, err)
				// One entry over the budget is the documented same-entry exception;
				// anything more means the unsigned budget wrapped.
				assert.LessOrEqual(t, int64(len(resp.Contents)+len(resp.CommonPrefixes)), maxKeys+1,
					"maxKeys=%d page %d", maxKeys, page)
				keys = append(keys, collectKeys(resp.Contents)...)
				prefixes = append(prefixes, collectPrefixes(resp.CommonPrefixes)...)
				if !aws.BoolValue(resp.IsTruncated) {
					token = nil
					break
				}
				token = resp.NextContinuationToken
				require.NotNil(t, token, "a truncated page must name where to resume")
			}
			require.Nil(t, token, "maxKeys=%d did not finish", maxKeys)
			assert.Equal(t, []string{"foo", "foobar", "other", "zed"}, keys, "maxKeys=%d", maxKeys)
			assert.Equal(t, []string{"foo/", "zed/"}, prefixes, "maxKeys=%d", maxKeys)
		}
	})

	// Versioning reaches a prefix object from two directions: a suspended bucket
	// writes the null version at the key's own path, and a bucket versioned later
	// finds one already sitting there. Both leave a key that is a directory with
	// version history beside it.
	t.Run("Versioned", func(t *testing.T) {
		setVersioning := func(t *testing.T, bucket, status string) {
			t.Helper()
			_, err := cluster.s3Client.PutBucketVersioning(&s3.PutBucketVersioningInput{
				Bucket:                  aws.String(bucket),
				VersioningConfiguration: &s3.VersioningConfiguration{Status: aws.String(status)},
			})
			require.NoError(t, err)
		}

		// Both write orders, in a bucket that is versioned and in one where versioning
		// was suspended - the suspended one is the case that writes at the key's path.
		for _, state := range []string{"Enabled", "Suspended"} {
			bucket := createTestBucket(t, cluster, "test-prefix-"+strings.ToLower(state)+"-")
			setVersioning(t, bucket, "Enabled")
			if state == "Suspended" {
				setVersioning(t, bucket, "Suspended")
			}
			put(t, bucket, "child/foo/bar", nested)
			put(t, bucket, "child/foo", body)
			put(t, bucket, "prefix/foo", body)
			put(t, bucket, "prefix/foo/bar", nested)

			assert.Equal(t, []string{"child/foo", "child/foo/bar", "prefix/foo", "prefix/foo/bar"},
				listKeys(t, bucket), state)
			for _, key := range []string{"child/foo", "prefix/foo"} {
				read(t, bucket, key, body)
			}
			for _, key := range []string{"child/foo/bar", "prefix/foo/bar"} {
				read(t, bucket, key, nested)
			}
		}

		// A prefix object written before versioning is the key's null version. Removing
		// that version by id must not take the keys nested under it with it.
		bucket := createTestBucket(t, cluster, "test-prefix-nullversion-")
		put(t, bucket, "collision/foo/bar", nested)
		put(t, bucket, "collision/foo", body)
		setVersioning(t, bucket, "Enabled")
		newer := []byte("written after versioning was enabled")
		versioned, err := cluster.s3Client.PutObject(&s3.PutObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String("collision/foo"),
			Body:   bytes.NewReader(newer),
		})
		require.NoError(t, err)

		for _, v := range []struct {
			id   string
			want []byte
		}{{"null", body}, {aws.StringValue(versioned.VersionId), newer}} {
			got, err := cluster.s3Client.GetObject(&s3.GetObjectInput{
				Bucket:    aws.String(bucket),
				Key:       aws.String("collision/foo"),
				VersionId: aws.String(v.id),
			})
			require.NoError(t, err, "get version %s", v.id)
			body, err := io.ReadAll(got.Body)
			require.NoError(t, err)
			got.Body.Close()
			assert.Equal(t, v.want, body, "get version %s", v.id)
		}

		_, err = cluster.s3Client.DeleteObject(&s3.DeleteObjectInput{
			Bucket:    aws.String(bucket),
			Key:       aws.String("collision/foo"),
			VersionId: aws.String("null"),
		})
		require.NoError(t, err, "the null version sits on a directory other keys live in")

		read(t, bucket, "collision/foo", newer)
		read(t, bucket, "collision/foo/bar", nested)
		remaining, err := cluster.s3Client.ListObjectVersions(&s3.ListObjectVersionsInput{
			Bucket: aws.String(bucket),
			Prefix: aws.String("collision/foo"),
		})
		require.NoError(t, err)
		for _, v := range remaining.Versions {
			if aws.StringValue(v.Key) != "collision/foo" {
				// collision/foo/bar predates versioning too, and keeps its null version.
				continue
			}
			assert.NotEqual(t, "null", aws.StringValue(v.VersionId), "the null version was deleted")
		}
	})

	// The two listings walk the tree differently, and a prefix object is the entry
	// they disagree about: it is a directory the version listing descends through and
	// a key at the same time. They have to name the same keys and the same prefixes.
	t.Run("VersionListingMatchesObjectListing", func(t *testing.T) {
		bucket := createTestBucket(t, cluster, "test-prefix-parity-")
		for _, key := range []string{"foo", "foo/bar", "other", "a/foo", "a/foo/bar", "a/z"} {
			put(t, bucket, key, body)
		}

		for _, q := range []struct{ prefix, delimiter string }{
			{"", ""},
			{"foo/", ""},
			{"a/", ""},
			{"a/foo/", ""},
			{"", "/"},
			{"a/", "/"},
		} {
			name := "prefix=" + q.prefix + " delimiter=" + q.delimiter

			objects, err := cluster.s3Client.ListObjectsV2(&s3.ListObjectsV2Input{
				Bucket:    aws.String(bucket),
				Prefix:    aws.String(q.prefix),
				Delimiter: aws.String(q.delimiter),
			})
			require.NoError(t, err, name)

			versions, err := cluster.s3Client.ListObjectVersions(&s3.ListObjectVersionsInput{
				Bucket:    aws.String(bucket),
				Prefix:    aws.String(q.prefix),
				Delimiter: aws.String(q.delimiter),
			})
			require.NoError(t, err, name)

			versionKeys := make([]string, 0, len(versions.Versions))
			for _, v := range versions.Versions {
				versionKeys = append(versionKeys, aws.StringValue(v.Key))
			}
			versionPrefixes := make([]string, 0, len(versions.CommonPrefixes))
			for _, p := range versions.CommonPrefixes {
				versionPrefixes = append(versionPrefixes, aws.StringValue(p.Prefix))
			}
			sort.Strings(versionKeys)
			sort.Strings(versionPrefixes)

			objectKeys := collectKeys(objects.Contents)
			objectPrefixes := collectPrefixes(objects.CommonPrefixes)
			sort.Strings(objectKeys)
			sort.Strings(objectPrefixes)

			assert.Equal(t, objectKeys, versionKeys, "keys, %s", name)
			assert.Equal(t, objectPrefixes, versionPrefixes, "prefixes, %s", name)
		}
	})

	// A prefix object written before versioning is the key's null version, so the
	// version written after it has to take the latest flag off it.
	t.Run("VersionedAfterPrefixObject", func(t *testing.T) {
		bucket := createTestBucket(t, cluster, "test-prefix-versioned-")
		put(t, bucket, "collision/foo/bar", nested)
		put(t, bucket, "collision/foo", body)
		_, err := cluster.s3Client.PutBucketVersioning(&s3.PutBucketVersioningInput{
			Bucket:                  aws.String(bucket),
			VersioningConfiguration: &s3.VersioningConfiguration{Status: aws.String("Enabled")},
		})
		require.NoError(t, err)
		newer := []byte("written after versioning was enabled")
		put(t, bucket, "collision/foo", newer)

		resp, err := cluster.s3Client.ListObjectVersions(&s3.ListObjectVersionsInput{Bucket: aws.String(bucket)})
		require.NoError(t, err)

		latest := map[string]int{}
		var nullSize int64 = -1
		for _, v := range resp.Versions {
			if aws.BoolValue(v.IsLatest) {
				latest[aws.StringValue(v.Key)]++
			}
			if aws.StringValue(v.Key) == "collision/foo" && aws.StringValue(v.VersionId) == "null" {
				nullSize = aws.Int64Value(v.Size)
				assert.False(t, aws.BoolValue(v.IsLatest), "the newer version is the latest one")
			}
		}
		assert.Equal(t, 1, latest["collision/foo"], "exactly one version of a key is the latest")
		assert.Equal(t, int64(len(body)), nullSize, "the null version keeps the prefix object's size")
		read(t, bucket, "collision/foo", newer)
		read(t, bucket, "collision/foo/bar", nested)
	})

	// A key that other keys are nested under is a copy source and a copy destination
	// like any other. The keys nested under either end are not part of the copy.
	t.Run("Copy", func(t *testing.T) {
		bucket := createTestBucket(t, cluster, "test-prefix-copy-")
		copyObject := func(t *testing.T, src, dst string) {
			t.Helper()
			_, err := cluster.s3Client.CopyObject(&s3.CopyObjectInput{
				Bucket:     aws.String(bucket),
				Key:        aws.String(dst),
				CopySource: aws.String(bucket + "/" + src),
			})
			require.NoError(t, err, "copy %s to %s", src, dst)
		}

		put(t, bucket, "collision/foo", body)
		put(t, bucket, "collision/foo/bar", nested)

		// Out of a prefix object, into a key of its own.
		copyObject(t, "collision/foo", "plain")
		read(t, bucket, "plain", body)
		read(t, bucket, "collision/foo", body)
		read(t, bucket, "collision/foo/bar", nested)

		// Into a key that other keys are nested under.
		put(t, bucket, "target/child", nested)
		copyObject(t, "plain", "target")
		read(t, bucket, "target", body)
		read(t, bucket, "target/child", nested)

		// And between two of them.
		put(t, bucket, "other", []byte("copied between prefix keys"))
		copyObject(t, "other", "collision/foo")
		read(t, bucket, "collision/foo", []byte("copied between prefix keys"))
		read(t, bucket, "collision/foo/bar", nested)

		assert.Equal(t, []string{"collision/foo", "collision/foo/bar", "other", "plain", "target", "target/child"},
			listKeys(t, bucket))
	})

	// Rename moves the object off the key without moving the keys nested under it,
	// which is not what the filer's atomic rename of a directory would do.
	t.Run("Rename", func(t *testing.T) {
		bucket := createTestBucket(t, cluster, "test-prefix-rename-")
		renameObject := func(t *testing.T, src, dst string) {
			t.Helper()
			req, _ := http.NewRequest(http.MethodPut, cluster.s3Endpoint+"/"+bucket+"/"+dst+"?renameObject=", nil)
			req.Header.Set("x-amz-rename-source", "/"+bucket+"/"+src)
			signer := v1signer.NewSigner(v1credentials.NewStaticCredentials(testAccessKey, testSecretKey, ""))
			_, err := signer.Sign(req, nil, "s3", testRegion, time.Now())
			require.NoError(t, err)

			resp, err := (&http.Client{Timeout: 20 * time.Second}).Do(req)
			require.NoError(t, err, "rename %s to %s", src, dst)
			defer resp.Body.Close()
			io.Copy(io.Discard, resp.Body)
			require.Equal(t, http.StatusOK, resp.StatusCode, "rename %s to %s", src, dst)
		}

		put(t, bucket, "collision/foo", body)
		put(t, bucket, "collision/foo/bar", nested)

		// Off a prefix object: the key goes, the keys under it stay.
		renameObject(t, "collision/foo", "moved")
		read(t, bucket, "moved", body)
		read(t, bucket, "collision/foo/bar", nested)
		gone(t, bucket, "collision/foo")

		// Onto a key other keys are nested under.
		put(t, bucket, "target/child", nested)
		renameObject(t, "moved", "target")
		read(t, bucket, "target", body)
		read(t, bucket, "target/child", nested)
		gone(t, bucket, "moved")

		assert.Equal(t, []string{"collision/foo/bar", "target", "target/child"}, listKeys(t, bucket))
	})

	// A directory SeaweedFS keeps its own state in is not a prefix a key can be
	// stored on: the object would replace that state with its own.
	t.Run("ReservedDirectory", func(t *testing.T) {
		bucket := createTestBucket(t, cluster, "test-prefix-reserved-")
		_, err := cluster.s3Client.PutBucketVersioning(&s3.PutBucketVersioningInput{
			Bucket:                  aws.String(bucket),
			VersioningConfiguration: &s3.VersioningConfiguration{Status: aws.String("Enabled")},
		})
		require.NoError(t, err)
		put(t, bucket, "foo", body)
		_, err = cluster.s3Client.PutBucketVersioning(&s3.PutBucketVersioningInput{
			Bucket:                  aws.String(bucket),
			VersioningConfiguration: &s3.VersioningConfiguration{Status: aws.String("Suspended")},
		})
		require.NoError(t, err)

		_, err = cluster.s3Client.PutObject(&s3.PutObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String("foo.versions"),
			Body:   bytes.NewReader(body),
		})
		// Not just any error: a permanently impossible write must not come back as a
		// 500 the SDK retries.
		var refused awserr.RequestFailure
		require.ErrorAs(t, err, &refused, "the version history of foo is not a prefix of foo.versions")
		assert.Equal(t, http.StatusConflict, refused.StatusCode())
		assert.Equal(t, "ExistingObjectIsDirectory", refused.Code())

		versions, err := cluster.s3Client.ListObjectVersions(&s3.ListObjectVersionsInput{Bucket: aws.String(bucket)})
		require.NoError(t, err)
		require.Len(t, versions.Versions, 1)
		assert.Equal(t, "foo", aws.StringValue(versions.Versions[0].Key))
		read(t, bucket, "foo", body)

		// The multipart staging folder is the other one, and an in-flight upload has
		// to survive the attempt.
		staging := createTestBucket(t, cluster, "test-prefix-uploads-")
		created, err := cluster.s3Client.CreateMultipartUpload(&s3.CreateMultipartUploadInput{
			Bucket: aws.String(staging),
			Key:    aws.String("mp.bin"),
		})
		require.NoError(t, err)

		_, err = cluster.s3Client.PutObject(&s3.PutObjectInput{
			Bucket: aws.String(staging),
			Key:    aws.String(".uploads"),
			Body:   bytes.NewReader(body),
		})
		require.ErrorAs(t, err, &refused, "the multipart staging folder is not a prefix of .uploads")
		assert.Equal(t, http.StatusConflict, refused.StatusCode())
		assert.Equal(t, "ExistingObjectIsDirectory", refused.Code())

		uploads, err := cluster.s3Client.ListMultipartUploads(&s3.ListMultipartUploadsInput{Bucket: aws.String(staging)})
		require.NoError(t, err)
		require.Len(t, uploads.Uploads, 1)
		assert.Equal(t, "mp.bin", aws.StringValue(uploads.Uploads[0].Key))
		_, err = cluster.s3Client.AbortMultipartUpload(&s3.AbortMultipartUploadInput{
			Bucket:   aws.String(staging),
			Key:      aws.String("mp.bin"),
			UploadId: created.UploadId,
		})
		require.NoError(t, err)
	})

	// Either key can be deleted without touching the other.
	t.Run("Delete", func(t *testing.T) {
		bucket := createTestBucket(t, cluster, "test-prefix-delete-")
		put(t, bucket, "collision/foo/bar", nested)
		put(t, bucket, "collision/foo", body)

		_, err := cluster.s3Client.DeleteObject(&s3.DeleteObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String("collision/foo"),
		})
		require.NoError(t, err)
		assert.Equal(t, []string{"collision/foo/bar"}, listKeys(t, bucket))
		read(t, bucket, "collision/foo/bar", nested)
		gone(t, bucket, "collision/foo")

		put(t, bucket, "collision/foo", body)
		_, err = cluster.s3Client.DeleteObject(&s3.DeleteObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String("collision/foo/bar"),
		})
		require.NoError(t, err)
		assert.Equal(t, []string{"collision/foo"}, listKeys(t, bucket))
		read(t, bucket, "collision/foo", body)
		gone(t, bucket, "collision/foo/bar")
	})
}
