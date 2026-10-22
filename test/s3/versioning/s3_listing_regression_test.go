package s3api

import (
	"bytes"
	"context"
	"fmt"
	"sort"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Check both wire APIs with small pages. The directory walker alone does
// not exercise prefix normalization, delimiter-marker adjustment, or tokens.
func TestListingPrefixAndDelimiterPagination(t *testing.T) {
	client := getS3Client(t)
	ctx := context.Background()
	for _, versioning := range []string{"", "Enabled", "Suspended"} {
		t.Run("versioning="+versioning, func(t *testing.T) {
			bucket := getNewBucketName()
			createBucket(t, client, bucket)
			defer deleteBucket(t, client, bucket)
			if versioning != "" {
				enableVersioning(t, client, bucket)
				if versioning == "Suspended" {
					_, err := client.PutBucketVersioning(ctx, &s3.PutBucketVersioningInput{
						Bucket: aws.String(bucket), VersioningConfiguration: &types.VersioningConfiguration{Status: types.BucketVersioningStatusSuspended},
					})
					require.NoError(t, err)
				}
			}
			for _, key := range []string{"a", "a-c", "a.txt", "a/b", "foo_bar", "foo_bar_baz", "foo_baz"} {
				_, err := client.PutObject(ctx, &s3.PutObjectInput{Bucket: aws.String(bucket), Key: aws.String(key), Body: bytes.NewReader([]byte("x"))})
				require.NoError(t, err)
			}
			for _, tc := range []struct {
				name, prefix, delimiter, startAfter string
				keys, prefixes                      []string
			}{
				{"trailing slash", "a/", "", "", []string{"a/b"}, nil},
				{"trailing slash custom delimiter", "a/", "_", "", []string{"a/b"}, nil},
				{"custom delimiter", "foo", "_", "", nil, []string{"foo_"}},
				{"custom delimiter nested group", "foo_", "_", "", []string{"foo_bar", "foo_baz"}, []string{"foo_bar_"}},
				{"marker names directory", "a", "", "a", []string{"a-c", "a.txt", "a/b"}, nil},
				{"marker between name and slash", "a", "", "a.txt", []string{"a/b"}, nil},
			} {
				for _, v2 := range []bool{false, true} {
					for _, pageSize := range []int32{1, 2, 1000} {
						t.Run(fmt.Sprintf("%s/v2=%t/max=%d", tc.name, v2, pageSize), func(t *testing.T) {
							var keys, prefixes []string
							marker, token := tc.startAfter, ""
							for page := 0; ; page++ {
								require.Less(t, page, 10, "listing must terminate without repeating a prefix")
								var objects []types.Object
								var commonPrefixes []types.CommonPrefix
								var truncated bool
								if v2 {
									in := &s3.ListObjectsV2Input{Bucket: aws.String(bucket), Prefix: aws.String(tc.prefix), Delimiter: aws.String(tc.delimiter), MaxKeys: aws.Int32(pageSize)}
									if token != "" {
										in.ContinuationToken = aws.String(token)
									} else if marker != "" {
										in.StartAfter = aws.String(marker)
									}
									out, err := client.ListObjectsV2(ctx, in)
									require.NoError(t, err)
									objects, commonPrefixes, truncated = out.Contents, out.CommonPrefixes, aws.ToBool(out.IsTruncated)
									next := aws.ToString(out.NextContinuationToken)
									if truncated {
										require.NotEmpty(t, next)
										require.NotEqual(t, token, next, "token must advance")
									}
									token = next
								} else {
									out, err := client.ListObjects(ctx, &s3.ListObjectsInput{
										Bucket: aws.String(bucket), Prefix: aws.String(tc.prefix), Delimiter: aws.String(tc.delimiter), MaxKeys: aws.Int32(pageSize), Marker: aws.String(marker),
									})
									require.NoError(t, err)
									objects, commonPrefixes, truncated = out.Contents, out.CommonPrefixes, aws.ToBool(out.IsTruncated)
									next := aws.ToString(out.NextMarker)
									if truncated {
										if next == "" && len(objects) > 0 {
											next = aws.ToString(objects[len(objects)-1].Key)
										}
										require.Greater(t, next, marker, "marker must advance")
									}
									marker = next
								}
								for _, object := range objects {
									keys = append(keys, aws.ToString(object.Key))
								}
								for _, prefix := range commonPrefixes {
									prefixes = append(prefixes, aws.ToString(prefix.Prefix))
								}
								if !truncated {
									break
								}
							}
							sort.Strings(keys)
							sort.Strings(prefixes)
							assert.Equal(t, tc.keys, keys)
							assert.Equal(t, tc.prefixes, prefixes)
						})
					}
				}
			}
		})
	}
}
