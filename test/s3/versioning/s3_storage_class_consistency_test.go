package s3api

import (
	"bytes"
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// A listing on a versioned bucket is served from metadata cached on the .versions
// directory entry so the whole listing is one scan. Anything the cache does not
// carry falls back to a default, so a field missing from the cache makes the
// listing disagree with HEAD about the same object. Storage class is one such
// field, and clients that filter or tier on it act on the listing.

func TestStorageClassConsistentBetweenHeadAndListings(t *testing.T) {
	client := getS3Client(t)
	bucketName := getNewBucketName()

	createBucket(t, client, bucketName)
	defer deleteBucket(t, client, bucketName)
	enableVersioning(t, client, bucketName)

	cases := []struct {
		key   string
		class types.StorageClass
		want  string
	}{
		{"default.blk", "", "STANDARD"},
		{"standard.blk", types.StorageClassStandard, "STANDARD"},
		{"reduced.blk", types.StorageClassReducedRedundancy, "REDUCED_REDUNDANCY"},
		{"glacier.blk", types.StorageClassGlacier, "GLACIER"},
	}

	for _, tc := range cases {
		in := &s3.PutObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(tc.key),
			Body:   bytes.NewReader([]byte("x")),
		}
		if tc.class != "" {
			in.StorageClass = tc.class
		}
		_, err := client.PutObject(context.TODO(), in)
		require.NoError(t, err, "PUT %s with class %q", tc.key, tc.class)
	}

	listed, err := client.ListObjectsV2(context.TODO(), &s3.ListObjectsV2Input{Bucket: aws.String(bucketName)})
	require.NoError(t, err)
	byKeyV2 := make(map[string]string, len(listed.Contents))
	for _, o := range listed.Contents {
		require.NotNil(t, o.Key)
		byKeyV2[*o.Key] = string(o.StorageClass)
	}

	versions, err := client.ListObjectVersions(context.TODO(), &s3.ListObjectVersionsInput{Bucket: aws.String(bucketName)})
	require.NoError(t, err)
	byKeyVersions := make(map[string]string, len(versions.Versions))
	for _, v := range versions.Versions {
		require.NotNil(t, v.Key)
		if v.IsLatest != nil && *v.IsLatest {
			byKeyVersions[*v.Key] = string(v.StorageClass)
		}
	}

	for _, tc := range cases {
		t.Run(tc.key, func(t *testing.T) {
			assert.Equal(t, tc.want, byKeyV2[tc.key],
				"ListObjectsV2 must report the class the object was stored with")
			assert.Equal(t, tc.want, byKeyVersions[tc.key],
				"ListObjectVersions must report the class the object was stored with")

			head, err := client.HeadObject(context.TODO(), &s3.HeadObjectInput{
				Bucket: aws.String(bucketName), Key: aws.String(tc.key),
			})
			require.NoError(t, err)
			// S3 omits the header for STANDARD and sends it otherwise; either way
			// it must not contradict the listing.
			if head.StorageClass != "" {
				assert.Equal(t, tc.want, string(head.StorageClass),
					"HEAD and the listings must agree on storage class")
			} else {
				assert.Equal(t, "STANDARD", tc.want,
					"HEAD omits the header only for STANDARD")
			}
		})
	}
}

// Overwriting refreshes the cached listing metadata; the class must follow the
// new current version rather than stay pinned to the one it replaced.
func TestStorageClassFollowsLatestVersion(t *testing.T) {
	client := getS3Client(t)
	bucketName := getNewBucketName()

	createBucket(t, client, bucketName)
	defer deleteBucket(t, client, bucketName)
	enableVersioning(t, client, bucketName)

	key := "rewritten.blk"
	_, err := client.PutObject(context.TODO(), &s3.PutObjectInput{
		Bucket: aws.String(bucketName), Key: aws.String(key),
		Body:         bytes.NewReader([]byte("first")),
		StorageClass: types.StorageClassGlacier,
	})
	require.NoError(t, err)

	_, err = client.PutObject(context.TODO(), &s3.PutObjectInput{
		Bucket: aws.String(bucketName), Key: aws.String(key),
		Body:         bytes.NewReader([]byte("second")),
		StorageClass: types.StorageClassReducedRedundancy,
	})
	require.NoError(t, err)

	listed, err := client.ListObjectsV2(context.TODO(), &s3.ListObjectsV2Input{
		Bucket: aws.String(bucketName), Prefix: aws.String(key),
	})
	require.NoError(t, err)
	require.Len(t, listed.Contents, 1)
	assert.Equal(t, "REDUCED_REDUNDANCY", string(listed.Contents[0].StorageClass),
		"the listing must report the current version's class, not the one it replaced")
}
